package proxy

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mattn/go-isatty"
)

// ANSI 色码——语义对齐 YesMem proxy.go 色板。
const (
	colorReset      = "\033[0m"
	colorDim        = "\033[2m"  // 暗灰——passthrough / 高频噪音
	colorGreen      = "\033[32m" // 折叠/压缩完成
	colorLightGreen = "\033[92m" // 无需压缩
	colorBlue       = "\033[34m" // archive 注入
	colorYellow     = "\033[33m" // 警告/skip/retry
	colorRed        = "\033[31m" // 错误
)

// logColorKey 语义色 attr 的 key——Handle 消费该 attr 用于选色，不输出到文本。
const logColorKey = "logColor"

// 语义色常量——调用点追加到 slog.Info 参数列表末尾为级别标签着色。
// attr 值直接存 ANSI 色码，Handle 消费后原样使用。
var (
	LogGreen      = slog.String(logColorKey, colorGreen)      // 折叠/压缩完成
	LogLightGreen = slog.String(logColorKey, colorLightGreen) // 无需压缩
	LogBlue       = slog.String(logColorKey, colorBlue)       // archive 注入
	LogDim        = slog.String(logColorKey, colorDim)        // passthrough 高频噪音
)

// LogHandler 是对齐 YesMem 日志体验的 slog.Handler：
// 行格式 `2006/01/02 15:04:05 [LEVEL] 消息 k=v`，仅级别标签按级别/语义着色，
// 时间戳、消息和 attrs 永远保持终端默认颜色；非 TTY 输出零转义码。
type LogHandler struct {
	w         io.Writer
	level     slog.Level  // 最低输出级别
	color     bool        // TTY 检测结果；同包测试可直接置 true 强制开色
	preAttrs  string      // WithAttrs 预格式化段（形如 " k=v"），置于 record attrs 之前
	groups    string      // WithGroup 累积的 key 前缀（形如 "g." / "g1.g2."）
	withColor string      // WithAttrs 收到的语义色——record 无语义色时的默认覆盖
	mu        *sync.Mutex // 指针——跨 WithAttrs/WithGroup 克隆共享同一把锁
}

// NewLogHandler 构造 LogHandler。w 为 *os.File 且是终端时开启颜色，
// 并尝试启用 Windows conhost 的 VT 处理；启用失败回退无色（防 ←[32m 乱码）。
// bytes.Buffer 等非 *os.File writer 自然走无色路径。
func NewLogHandler(w io.Writer, level slog.Level) *LogHandler {
	h := &LogHandler{w: w, level: level, mu: &sync.Mutex{}}
	if f, ok := w.(*os.File); ok {
		if isatty.IsTerminal(f.Fd()) || isatty.IsCygwinTerminal(f.Fd()) {
			if enableVT(f) == nil {
				h.color = true
			}
		}
	}
	return h
}

// Enabled 按构造时的最低级别过滤。
func (h *LogHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.level
}

// Handle 输出一行：`时间戳 ` + [色码] + `[LEVEL]` + [复位] + 消息 + attrs + 换行。
// 在本地 buffer 拼完整行后持锁单次 Write，保证并发下行不交错。
func (h *LogHandler) Handle(_ context.Context, r slog.Record) error {
	var buf bytes.Buffer

	// 时间戳段永远无色。
	buf.WriteString(r.Time.Format("2006/01/02 15:04:05"))
	buf.WriteByte(' ')

	// 遍历 record attrs：消费语义色 attr，其余格式化为 " k=v"
	semColor := ""
	var attrBuf bytes.Buffer
	r.Attrs(func(a slog.Attr) bool {
		if a.Key == logColorKey {
			semColor = a.Value.String()
			return true
		}
		// LOG-03：session 身份只进文件路由，不进终端渲染。
		if isSessionIdentityLogAttr(a.Key) {
			return true
		}
		attrBuf.WriteByte(' ')
		attrBuf.WriteString(h.groups)
		attrBuf.WriteString(a.Key)
		attrBuf.WriteByte('=')
		attrBuf.WriteString(a.Value.String())
		return true
	})

	// 选色优先级：record 语义色 > WithAttrs 语义色 > level 默认色。
	c := semColor
	if c == "" {
		c = h.withColor
	}
	if c == "" {
		switch {
		case r.Level >= slog.LevelError:
			c = colorRed
		case r.Level >= slog.LevelWarn:
			c = colorYellow
		case r.Level < slog.LevelInfo:
			c = colorDim
		default:
			c = colorGreen
		}
	}

	label, padding := logLevelLabel(r.Level)
	if h.color && c != "" {
		buf.WriteString(c)
	}
	buf.WriteString(label)
	if h.color && c != "" {
		buf.WriteString(colorReset)
	}
	buf.WriteString(padding)
	buf.WriteString(r.Message)
	buf.WriteString(h.preAttrs) // WithAttrs 预置段在 record attrs 之前
	buf.Write(attrBuf.Bytes())
	buf.WriteByte('\n')

	h.mu.Lock()
	defer h.mu.Unlock()
	_, err := h.w.Write(buf.Bytes())
	return err
}

// logLevelLabel 返回固定宽度的级别标签和标签后的对齐空格。
// label + padding 始终占 8 列，使不同级别的消息正文从同一列开始。
func logLevelLabel(level slog.Level) (label, padding string) {
	switch {
	case level >= slog.LevelError:
		return "[ERROR]", " "
	case level >= slog.LevelWarn:
		return "[WARN]", "  "
	case level < slog.LevelInfo:
		return "[DEBUG]", " "
	default:
		return "[INFO]", "  "
	}
}

// WithAttrs 克隆 handler 并预格式化 attrs 追加存储；
// 遇语义色 attr 则存为克隆的默认色覆盖。空切片返回自身。
func (h *LogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	if len(attrs) == 0 {
		return h
	}
	h2 := *h
	for _, a := range attrs {
		if a.Key == logColorKey {
			h2.withColor = a.Value.String()
			continue
		}
		// meta.Logger 的 request_session_id 走的正是这条路径。
		if isSessionIdentityLogAttr(a.Key) {
			continue
		}
		h2.preAttrs += " " + h2.groups + a.Key + "=" + a.Value.String()
	}
	return &h2
}

// WithGroup 克隆 handler 并累积 group 前缀，
// 后续 attr key 变为 `名.key`（嵌套 group 用点连接）。空名返回自身。
func (h *LogHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	h2 := *h
	h2.groups += name + "."
	return &h2
}

// ---- request outcome 与健康提示的终端专用投影 ----
//
// 这两条路径都不经过 slog.Record：它们由 authoritative outcome 与 typed health
// transition 直接驱动，只复用 LogHandler 的 writer 和共享单行锁。普通日志格式
// 因此完全不受影响，健康提示也无法回流到任何 file-capable handler。

// writeTerminalLine 在共享单行锁下一次写出整行，避免并发请求交错。
func writeTerminalLine(w io.Writer, mu *sync.Mutex, line string) error {
	if w == nil || mu == nil || line == "" {
		return nil
	}
	buf := make([]byte, 0, len(line)+1)
	buf = append(buf, line...)
	buf = append(buf, '\n')
	mu.Lock()
	defer mu.Unlock()
	_, err := w.Write(buf)
	return err
}

// ProjectTerminal 让 LogHandler 直接充当 outcome dispatcher 的 terminal projector。
// 它只消费 immutable snapshot 与 session admission 结果，不访问 session sink 状态。
func (h *LogHandler) ProjectTerminal(snapshot requestOutcomeSnapshot, admission outcomeDispatchResult) error {
	if h == nil {
		return nil
	}
	return writeTerminalLine(h.w, h.mu, renderRequestOutcome(snapshot, admission))
}

// renderRequestOutcome 把 typed outcome 渲染成一条面向老大的自然中文结果。
// terminal-ineligible 返回空串——session 投影资格与此无关，由 dispatcher 独立决定。
func renderRequestOutcome(snapshot requestOutcomeSnapshot, admission outcomeDispatchResult) string {
	snapshot = snapshot.normalized()
	if snapshot.TerminalEligibility != outcomeEligibilityTerminalEligible {
		return ""
	}
	trigger, reason := terminalTriggerPhrase(snapshot)
	var builder strings.Builder
	builder.WriteString(outcomeDisplayTime(snapshot).Format("15:04:05"))
	builder.WriteString(" ST ")
	builder.WriteString(trigger)
	builder.WriteString("｜原因：")
	builder.WriteString(reason)
	builder.WriteString("｜动作：")
	builder.WriteString(terminalActionPhrase(snapshot.Action))
	builder.WriteString("｜结果：")
	builder.WriteString(terminalResultPhrase(snapshot))
	builder.WriteString("｜会话=")
	builder.WriteString(snapshot.SessionHash)
	builder.WriteString("｜请求=")
	builder.WriteString(strconv.FormatUint(snapshot.RequestID, 10))
	// 只有提交时就被拒绝才追加缺失说明；accepted 之后的文件故障由独立健康提示报告。
	if admission.SessionRejected {
		builder.WriteString("｜详细记录未保存")
	}
	return builder.String()
}

// outcomeDisplayTime 优先使用结束时间；终端只显示本地 HH:MM:SS，不显示年月日和毫秒。
func outcomeDisplayTime(snapshot requestOutcomeSnapshot) time.Time {
	if !snapshot.FinishedAt.IsZero() {
		return snapshot.FinishedAt.Local()
	}
	if !snapshot.StartedAt.IsZero() {
		return snapshot.StartedAt.Local()
	}
	return time.Now()
}

// terminalTriggerPhrase 返回“是否触发”与自然中文原因；未完成判定时不假装已评估。
func terminalTriggerPhrase(snapshot requestOutcomeSnapshot) (string, string) {
	switch snapshot.Eligibility {
	case outcomeEligibilityUnknown, outcomeEligibilityNotEvaluable:
		return "未评估", "本次请求未完成整理判定"
	}
	switch snapshot.TriggerReason {
	case TriggerNone:
		return "未触发", "上下文仍有余量"
	case TriggerTokens:
		return "已触发", "上下文接近上限"
	case TriggerPause:
		return "已触发", "对话长时间未继续"
	case TriggerEmergency:
		return "已触发", "上下文即将超出上限"
	default:
		return "触发情况未确认", "判定信息不完整"
	}
}

func terminalActionPhrase(action outcomeAction) string {
	switch action {
	case outcomeActionPassthrough:
		return "原样转发"
	case outcomeActionDirect:
		return "直接发送"
	case outcomeActionLight:
		return "轻度整理"
	case outcomeActionCollapse:
		return "整理历史消息"
	case outcomeActionFallback:
		return "改用备用方式整理"
	case outcomeActionCompact:
		return "压缩历史消息"
	case outcomeActionFailClosed:
		return "保守转发，未改动历史"
	case outcomeActionUnavailable:
		return "未能执行整理"
	default:
		return "处理方式未记录"
	}
}

// terminalResultPhrase 合成“处理结果 + 是否需要干预”；等待阈值和内部实现名不出现。
func terminalResultPhrase(snapshot requestOutcomeSnapshot) string {
	var builder strings.Builder
	builder.WriteString(terminalUpstreamPhrase(snapshot.UpstreamState))
	if persistence := terminalPersistencePhrase(snapshot); persistence != "" {
		builder.WriteString("，")
		builder.WriteString(persistence)
	}
	builder.WriteString("，")
	builder.WriteString(terminalInterventionPhrase(snapshot.Intervention))
	return builder.String()
}

func terminalUpstreamPhrase(state upstreamState) string {
	switch state {
	case upstreamStateSuccess:
		return "请求已完成"
	case upstreamStateNotStarted:
		return "请求未发出"
	case upstreamStateNon2xx:
		return "上游返回错误"
	case upstreamStateTransportFailure:
		return "未能连接上游"
	case upstreamStateHeaderTimeout:
		return "等待上游响应超时"
	case upstreamStateHardTimeout:
		return "上游响应超时"
	case upstreamStateIdleTimeout:
		return "上游响应中断超时"
	case upstreamStateBodyReadFailure:
		return "上游响应读取失败"
	case upstreamStateCommittedFailure:
		return "响应中途中断"
	default:
		return "处理结果未确认"
	}
}

func terminalPersistencePhrase(snapshot requestOutcomeSnapshot) string {
	switch {
	case snapshot.MemoryState == persistenceStateFailed:
		return "本地状态更新失败"
	case snapshot.DiskState == persistenceStateFailed:
		return "本地状态未能保存到磁盘"
	case snapshot.DiskState == persistenceStateUnavailable:
		return "本地状态暂时无法保存"
	default:
		return ""
	}
}

func terminalInterventionPhrase(state interventionState) string {
	switch state {
	case interventionRequired:
		return "需要人工检查"
	case interventionUnknown:
		return "是否需要人工处理待确认"
	default:
		return "无需人工处理"
	}
}

// TerminalHealthReporter 是四个 health scope 唯一的终端出口。它只持有终端
// writer 与共享单行锁，因此 session/SQLite 故障的提示不可能再次进入正在失败
// 的文件 sink，也不会经过任何组合 handler 递归放大。
type TerminalHealthReporter struct {
	w   io.Writer
	mu  *sync.Mutex
	now func() time.Time
}

// NewTerminalHealthReporter 只接受终端 handler；参数类型本身即排除 file-capable sink。
func NewTerminalHealthReporter(terminal *LogHandler) *TerminalHealthReporter {
	reporter := &TerminalHealthReporter{now: time.Now}
	if terminal != nil {
		reporter.w = terminal.w
		reporter.mu = terminal.mu
	}
	return reporter
}

// ReportHealthTransition 只渲染 entered/recovered；ongoing/unchanged 与未知 scope 输出零行。
func (r *TerminalHealthReporter) ReportHealthTransition(transition HealthTransition) {
	if r == nil {
		return
	}
	at := time.Now()
	if r.now != nil {
		at = r.now()
	}
	_ = writeTerminalLine(r.w, r.mu, renderHealthTransition(transition, at))
}

func renderHealthTransition(transition HealthTransition, at time.Time) string {
	text := terminalHealthText(transition.Scope, transition.Kind)
	if text == "" {
		return ""
	}
	return at.Format("15:04:05") + " " + text
}

// terminalHealthText 是四 scope × entered/recovered 的稳定人类影响文案。
// 它不输出 scope 名、failure class、generation、等待阈值或 raw error。
func terminalHealthText(scope HealthScope, kind HealthTransitionKind) string {
	var entered, recovered string
	switch scope {
	case HealthScopeSQLiteState:
		entered, recovered = "本地状态暂时无法保存到磁盘，重启后可能丢失最近进度", "本地状态已恢复正常保存"
	case HealthScopeSQLiteArchive:
		entered, recovered = "归档暂时无法保存，相关原文会保持原样发送", "归档已恢复正常保存"
	case HealthScopeSessionQueueFull:
		entered, recovered = "详细记录暂时来不及保存，这段时间的详细记录会缺失", "详细记录已恢复保存，缺失范围已补记"
	case HealthScopeSessionLogSink:
		entered, recovered = "详细记录暂时写不进文件，这段时间的详细记录会缺失", "详细记录文件写入已恢复，缺失范围已补记"
	default:
		return ""
	}
	switch kind {
	case HealthTransitionKindEntered:
		return entered
	case HealthTransitionKindRecovered:
		return recovered
	default:
		return ""
	}
}

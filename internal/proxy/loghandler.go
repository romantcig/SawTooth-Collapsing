package proxy

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"os"
	"sync"

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

// 语义色常量——调用点追加到 slog.Info 参数列表末尾为整行消息着色。
// attr 值直接存 ANSI 色码，Handle 消费后原样使用。
var (
	LogGreen      = slog.String(logColorKey, colorGreen)      // 折叠/压缩完成
	LogLightGreen = slog.String(logColorKey, colorLightGreen) // 无需压缩
	LogBlue       = slog.String(logColorKey, colorBlue)       // archive 注入
	LogDim        = slog.String(logColorKey, colorDim)        // passthrough 高频噪音
)

// LogHandler 是对齐 YesMem 日志体验的 slog.Handler：
// 行格式 `[proxy] 2006/01/02 15:04:05 消息 k=v`，前缀与时间戳永远无色，
// 消息+attrs 段按级别/语义着色；非 TTY 输出零转义码并补 WARN/ERROR 文本前缀。
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

// Handle 输出一行：`[proxy] 时间戳 ` + [色码] + 消息 + attrs + [复位] + 换行。
// 在本地 buffer 拼完整行后持锁单次 Write，保证并发下行不交错。
func (h *LogHandler) Handle(_ context.Context, r slog.Record) error {
	var buf bytes.Buffer

	// 前缀段（永远无色）：[proxy] 2006/01/02 15:04:05␣
	buf.WriteString("[proxy] ")
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
		attrBuf.WriteByte(' ')
		attrBuf.WriteString(h.groups)
		attrBuf.WriteString(a.Key)
		attrBuf.WriteByte('=')
		attrBuf.WriteString(a.Value.String())
		return true
	})

	// 选色优先级：record 语义色 > WithAttrs 语义色 > level 默认色（Info 无色）
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
		}
	}

	if h.color {
		if c != "" {
			buf.WriteString(c)
		}
	} else {
		// 非 TTY：零转义码，Warn/Error 补文本级别前缀防丢失级别信息
		switch {
		case r.Level >= slog.LevelError:
			buf.WriteString("ERROR ")
		case r.Level >= slog.LevelWarn:
			buf.WriteString("WARN ")
		}
	}

	buf.WriteString(r.Message)
	buf.WriteString(h.preAttrs) // WithAttrs 预置段在 record attrs 之前
	buf.Write(attrBuf.Bytes())

	if h.color && c != "" {
		buf.WriteString(colorReset)
	}
	buf.WriteByte('\n')

	h.mu.Lock()
	defer h.mu.Unlock()
	_, err := h.w.Write(buf.Bytes())
	return err
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

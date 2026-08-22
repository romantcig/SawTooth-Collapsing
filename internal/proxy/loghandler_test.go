package proxy

import (
	"bytes"
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// logTestTime 测试用固定时间戳——对应输出 2026/07/06 15:04:05。
var logTestTime = time.Date(2026, 7, 6, 15, 4, 5, 0, time.Local)

// logTestPrefix 固定时间戳对应的无色前缀。
const logTestPrefix = "2026/07/06 15:04:05 "

// emitLogRecord 用固定时间手工构造 record 并直接调 Handle，返回输出内容。
func emitLogRecord(t *testing.T, h *LogHandler, buf *bytes.Buffer, level slog.Level, msg string, attrs ...slog.Attr) string {
	t.Helper()
	buf.Reset()
	r := slog.NewRecord(logTestTime, level, msg, 0)
	r.AddAttrs(attrs...)
	if err := h.Handle(context.Background(), r); err != nil {
		t.Fatalf("Handle 出错: %v", err)
	}
	return buf.String()
}

// Test 1: 时间戳格式——行以 "2006/01/02 15:04:05 " 格式前缀开头，
// 且时间戳段无任何 ANSI 码（开色时色码只包裹级别标签）。
func TestLogHandlerTimestampFormat(t *testing.T) {
	var buf bytes.Buffer
	h := NewLogHandler(&buf, slog.LevelInfo)

	// 非 TTY 路径
	got := emitLogRecord(t, h, &buf, slog.LevelInfo, "hello")
	if !strings.HasPrefix(got, logTestPrefix) {
		t.Errorf("输出 = %q, want 前缀 %q", got, logTestPrefix)
	}

	// 强制开色后前缀段依旧无色
	h.color = true
	got = emitLogRecord(t, h, &buf, slog.LevelError, "boom")
	if !strings.HasPrefix(got, logTestPrefix) {
		t.Errorf("开色输出 = %q, want 前缀 %q", got, logTestPrefix)
	}
	if strings.Contains(got[:len(logTestPrefix)], "\033") {
		t.Errorf("前缀段含 ANSI 码: %q", got[:len(logTestPrefix)])
	}
}

// Test 2: 非 TTY 去色+级别标签——bytes.Buffer writer 下零转义码，
// 所有级别均保留方括号标签且消息列对齐。
func TestLogHandlerNonTTY(t *testing.T) {
	var buf bytes.Buffer
	h := NewLogHandler(&buf, slog.LevelInfo)
	logger := slog.New(h)

	logger.Info("info msg")
	out := buf.String()
	if strings.Contains(out, "\033") {
		t.Errorf("Info 行含 ANSI 码: %q", out)
	}
	if !strings.Contains(out, "[INFO]  info msg") {
		t.Errorf("Info 行级别标签错误: %q", out)
	}

	buf.Reset()
	logger.Warn("warn msg")
	out = buf.String()
	if strings.Contains(out, "\033") {
		t.Errorf("Warn 行含 ANSI 码: %q", out)
	}
	if !strings.Contains(out, "[WARN]  warn msg") {
		t.Errorf("Warn 行级别标签错误: %q", out)
	}

	buf.Reset()
	logger.Error("error msg")
	out = buf.String()
	if strings.Contains(out, "\033") {
		t.Errorf("Error 行含 ANSI 码: %q", out)
	}
	if !strings.Contains(out, "[ERROR] error msg") {
		t.Errorf("Error 行级别标签错误: %q", out)
	}
}

// Test 3: level 默认色（强制开色）——Info 绿、Error 红、Warn 黄、Debug 暗灰，
// 色码只包裹级别标签，消息正文不着色。
func TestLogHandlerLevelColors(t *testing.T) {
	var buf bytes.Buffer
	h := NewLogHandler(&buf, slog.LevelDebug)
	h.color = true

	cases := []struct {
		name  string
		level slog.Level
		code  string
		label string
	}{
		{"Info绿", slog.LevelInfo, "\033[32m", "[INFO]"},
		{"Error红", slog.LevelError, "\033[31m", "[ERROR]"},
		{"Warn黄", slog.LevelWarn, "\033[33m", "[WARN]"},
		{"Debug暗灰", slog.LevelDebug, "\033[2m", "[DEBUG]"},
	}
	for _, c := range cases {
		got := emitLogRecord(t, h, &buf, c.level, "msg")
		rest := strings.TrimPrefix(got, logTestPrefix)
		wantColoredLabel := c.code + c.label + colorReset
		if !strings.HasPrefix(rest, wantColoredLabel) {
			t.Errorf("%s: 级别段 = %q, want 以 %q 开头", c.name, rest, wantColoredLabel)
		}
		if strings.Contains(strings.TrimPrefix(rest, wantColoredLabel), "\033") {
			t.Errorf("%s: 级别标签后的消息仍含 ANSI: %q", c.name, rest)
		}
	}
}

// Test 4: 语义色消费（强制开色）——语义色 attr 决定级别标签颜色，
// 且该 attr 本身（key）不出现在输出文本中。
func TestLogHandlerSemanticColors(t *testing.T) {
	var buf bytes.Buffer
	h := NewLogHandler(&buf, slog.LevelInfo)
	h.color = true

	cases := []struct {
		name string
		attr slog.Attr
		code string
	}{
		{"LogGreen", LogGreen, "\033[32m"},
		{"LogLightGreen", LogLightGreen, "\033[92m"},
		{"LogBlue", LogBlue, "\033[34m"},
		{"LogDim", LogDim, "\033[2m"},
	}
	for _, c := range cases {
		got := emitLogRecord(t, h, &buf, slog.LevelInfo, "msg", c.attr)
		rest := strings.TrimPrefix(got, logTestPrefix)
		wantColoredLabel := c.code + "[INFO]" + colorReset
		if !strings.HasPrefix(rest, wantColoredLabel) {
			t.Errorf("%s: 级别段 = %q, want 以 %q 开头", c.name, rest, wantColoredLabel)
		}
		if strings.Contains(got, logColorKey) {
			t.Errorf("%s: 语义色 attr key 泄漏到输出: %q", c.name, got)
		}
	}
}

// Test 5: attrs 格式——k-v 对输出为 "消息 k=v k2=v2"，msg 裸输出不加引号。
func TestLogHandlerAttrFormat(t *testing.T) {
	var buf bytes.Buffer
	h := NewLogHandler(&buf, slog.LevelInfo)

	got := emitLogRecord(t, h, &buf, slog.LevelInfo, "msg",
		slog.String("k", "v"), slog.Int("k2", 42))
	want := logTestPrefix + "[INFO]  msg k=v k2=42\n"
	if got != want {
		t.Errorf("输出 = %q, want %q", got, want)
	}
}

// Test 6: Handler 契约——WithAttrs 预置 attr 出现在后续每行、
// WithGroup 给 record attr key 加组前缀、Enabled 按构造 level 过滤，均不 panic。
func TestLogHandlerContract(t *testing.T) {
	var buf bytes.Buffer
	h := NewLogHandler(&buf, slog.LevelInfo)
	ctx := context.Background()

	// Enabled 过滤
	if h.Enabled(ctx, slog.LevelDebug) {
		t.Error("LevelInfo 构造时 Enabled(Debug) 应返回 false")
	}
	if !h.Enabled(ctx, slog.LevelInfo) {
		t.Error("LevelInfo 构造时 Enabled(Info) 应返回 true")
	}

	// WithAttrs 预置 attr 出现在后续每一行
	h2 := h.WithAttrs([]slog.Attr{slog.String("req", "abc")})
	for i := 0; i < 2; i++ {
		buf.Reset()
		r := slog.NewRecord(logTestTime, slog.LevelInfo, "msg", 0)
		if err := h2.Handle(ctx, r); err != nil {
			t.Fatalf("WithAttrs 后 Handle 出错: %v", err)
		}
		if got := buf.String(); !strings.Contains(got, "req=abc") {
			t.Errorf("第 %d 行缺预置 attr: %q", i+1, got)
		}
	}

	// WithGroup 后 record attr key 变为 g.key
	h3 := h.WithGroup("g")
	buf.Reset()
	r := slog.NewRecord(logTestTime, slog.LevelInfo, "msg", 0)
	r.AddAttrs(slog.String("key", "v"))
	if err := h3.Handle(ctx, r); err != nil {
		t.Fatalf("WithGroup 后 Handle 出错: %v", err)
	}
	if got := buf.String(); !strings.Contains(got, "g.key=v") {
		t.Errorf("WithGroup 输出 = %q, want 含 g.key=v", got)
	}

	// 空参数返回自身且不 panic
	if got := h.WithAttrs(nil); got != slog.Handler(h) {
		t.Error("WithAttrs(nil) 应返回自身")
	}
	if got := h.WithGroup(""); got != slog.Handler(h) {
		t.Error("WithGroup(\"\") 应返回自身")
	}
}

// terminalOutcomeSnapshot 构造一个 terminal-eligible 的健康基线 snapshot；
// 各用例只改动自己关心的字段，避免每个 fixture 重复完整结构体。
func terminalOutcomeSnapshot(mutate func(*requestOutcomeSnapshot)) requestOutcomeSnapshot {
	snapshot := requestOutcomeSnapshot{
		RequestID:           75,
		SessionHash:         "0123456789abcdef",
		StartedAt:           logTestTime,
		FinishedAt:          logTestTime,
		Eligibility:         outcomeEligibilityEvaluable,
		TerminalEligibility: outcomeEligibilityTerminalEligible,
		TriggerReason:       TriggerTokens,
		PressureSource:      pressureSourceActualPlusDelta,
		SelectedPressureTokens:    206628,
		PressureThresholdTokens:   150000,
		RequiredWait:        4 * time.Minute,
		ActualWait:          5 * time.Minute,
		ActualWaitKnown:     true,
		Action:              outcomeActionCollapse,
		BeforeMessages:      120,
		AfterMessages:       40,
		BeforeTokens:        90000,
		AfterTokens:         30000,
		UpstreamState:       upstreamStateSuccess,
		UpstreamStatus:      200,
		MemoryState:         persistenceStateSaved,
		DiskState:           persistenceStateSaved,
		FailureClass:        persistenceFailureNone,
		Intervention:        interventionNotRequired,
	}
	if mutate != nil {
		mutate(&snapshot)
	}
	return snapshot
}

func projectTerminalOutcome(t *testing.T, snapshot requestOutcomeSnapshot, admission outcomeDispatchResult) string {
	t.Helper()
	var buf bytes.Buffer
	handler := NewLogHandler(&buf, slog.LevelInfo)
	if err := handler.ProjectTerminal(snapshot, admission); err != nil {
		t.Fatalf("ProjectTerminal 出错: %v", err)
	}
	return buf.String()
}

// terminalOutcomeForbidden 是终端结果行绝不允许出现的实现术语与技术片段。
var terminalOutcomeForbidden = []string{
	"SawtoothTrigger", "frozen", "Frozen", "baseline", "actual_plus_delta",
	"collapse", "fallback", "passthrough", "fail_closed", "non_2xx",
	"sqlite", "stub", "trigger_reason", "pressure_source", "terminal_eligibility",
	"event=", "request_id", "session_hash", "unknown", "unavailable",
	"[INFO]", "[WARN]", "[ERROR]", "[DEBUG]", "\033",
	"秒", "分钟", "毫秒", "ms",
}

func assertTerminalOutcomeSafe(t *testing.T, line string) {
	t.Helper()
	for _, forbidden := range terminalOutcomeForbidden {
		if strings.Contains(line, forbidden) {
			t.Fatalf("终端结果泄漏禁止片段 %q: %q", forbidden, line)
		}
	}
}

func TestTerminalOutcomeRendererMatrix(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(*requestOutcomeSnapshot)
		want    []string
		notWant []string
	}{
		{
			name: "API 完整输入已超阈值时预告整理",
			mutate: func(snapshot *requestOutcomeSnapshot) {
				snapshot.TriggerReason = TriggerNone
				snapshot.Action = outcomeActionDirect
				snapshot.PressureSource = pressureSourceLocalFull
				snapshot.SelectedPressureTokens = 126403
				snapshot.PressureThresholdTokens = 150000
				snapshot.RequiredWait, snapshot.ActualWait, snapshot.ActualWaitKnown = 0, 0, false
				snapshot.BeforeMessages, snapshot.AfterMessages = 0, 0
				snapshot.BeforeTokens, snapshot.AfterTokens = 0, 0
				snapshot.APIUsageKnown = true
				snapshot.APIInputTokens = 2
				snapshot.APICacheCreationTokens = 205506
				snapshot.APITotalInputTokens = 205508
			},
			want: []string{"上下文", "判定 126403/150000", "API 完整输入 205508 已超阈值", "下一次连续主请求将整理"},
			notWant: []string{"未触发", "上下文仍有余量"},
		},
		{
			name:   "令牌触发整理",
			mutate: nil,
			want:   []string{"已触发", "判定 206628/150000", "整理前 90000 → 整理后 30000", "动作：整理历史消息", "结果：请求已完成"},
		},
		{
			name: "长时间未继续触发",
			mutate: func(snapshot *requestOutcomeSnapshot) {
				snapshot.TriggerReason = TriggerPause
			},
			want: []string{"已触发", "原因：对话长时间未继续", "动作：整理历史消息"},
		},
		{
			name: "紧急触发",
			mutate: func(snapshot *requestOutcomeSnapshot) {
				snapshot.TriggerReason = TriggerEmergency
			},
			want: []string{"已触发", "原因：上下文即将超出上限"},
		},
		{
			name: "备用整理方式",
			mutate: func(snapshot *requestOutcomeSnapshot) {
				snapshot.Action = outcomeActionFallback
			},
			want: []string{"动作：改用备用方式整理"},
		},
		{
			name: "历史判定失败保守转发",
			mutate: func(snapshot *requestOutcomeSnapshot) {
				snapshot.Eligibility = outcomeEligibilityNotEvaluable
				snapshot.TriggerReason = TriggerUnknown
				snapshot.Action = outcomeActionFailClosed
				snapshot.Intervention = interventionNotRequired
			},
			want: []string{"未评估", "原因：本次请求未完成整理判定", "动作：保守转发，未改动历史"},
		},
		{
			name: "上游失败",
			mutate: func(snapshot *requestOutcomeSnapshot) {
				snapshot.UpstreamState = upstreamStateTransportFailure
				snapshot.UpstreamStatus = 0
			},
			want: []string{"结果：未能连接上游"},
		},
		{
			name: "磁盘保存失败需要人工检查",
			mutate: func(snapshot *requestOutcomeSnapshot) {
				snapshot.DiskState = persistenceStateFailed
				snapshot.FailureClass = persistenceFailureSQLite
				snapshot.Intervention = interventionRequired
			},
			want: []string{"结果：请求已完成", "本地状态未能保存到磁盘", "需要人工检查"},
		},
		{
			name: "内存与磁盘均正常时不需要干预",
			mutate: func(snapshot *requestOutcomeSnapshot) {
				snapshot.Intervention = interventionNone
			},
			want:    []string{"无需人工处理"},
			notWant: []string{"需要人工检查"},
		},
	}

	for _, testCase := range cases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			out := projectTerminalOutcome(t, terminalOutcomeSnapshot(testCase.mutate), outcomeDispatchResult{SessionAccepted: true, TerminalAccepted: true})
			if strings.Count(out, "\n") != 1 {
				t.Fatalf("终端结果不是恰好一行: %q", out)
			}
			line := strings.TrimSuffix(out, "\n")
			for _, want := range testCase.want {
				if !strings.Contains(line, want) {
					t.Fatalf("终端结果缺少 %q: %q", want, line)
				}
			}
			for _, notWant := range testCase.notWant {
				if strings.Contains(line, notWant) {
					t.Fatalf("终端结果含多余 %q: %q", notWant, line)
				}
			}
			assertTerminalOutcomeSafe(t, line)
			if !strings.Contains(line, "会话=0123456789abcdef") || !strings.Contains(line, "请求=75") {
				t.Fatalf("终端结果缺少短 hash/request 关联: %q", line)
			}
		})
	}
}

// 普通未折叠请求必须零输出（Terminal Policy 2026-08-22 修订）：
// 数值闭环由响应侧“上下文总Tokens”行承担，ST 行只服务折叠与异常；
// “请求已完成/无需人工处理”不得出现在正常高频流程里。
func TestTerminalOutcomeQuietForNormalDirectRequests(t *testing.T) {
	quiet := []struct {
		name   string
		mutate func(*requestOutcomeSnapshot)
	}{
		{name: "普通直通无数值", mutate: func(snapshot *requestOutcomeSnapshot) {
			snapshot.TriggerReason = TriggerNone
			snapshot.Action = outcomeActionPassthrough
			snapshot.RequiredWait, snapshot.ActualWait, snapshot.ActualWaitKnown = 0, 0, false
			snapshot.BeforeMessages, snapshot.AfterMessages = 0, 0
			snapshot.BeforeTokens, snapshot.AfterTokens = 0, 0
			snapshot.SelectedPressureTokens, snapshot.PressureThresholdTokens = 0, 0
			snapshot.APIUsageKnown = false
		}},
		{name: "直接发送且API在阈值内", mutate: func(snapshot *requestOutcomeSnapshot) {
			snapshot.TriggerReason = TriggerNone
			snapshot.Action = outcomeActionDirect
			snapshot.PressureSource = pressureSourceLocalFull
			snapshot.SelectedPressureTokens = 126403
			snapshot.PressureThresholdTokens = 150000
			snapshot.RequiredWait, snapshot.ActualWait, snapshot.ActualWaitKnown = 0, 0, false
			snapshot.BeforeMessages, snapshot.AfterMessages = 0, 0
			snapshot.BeforeTokens, snapshot.AfterTokens = 0, 0
			snapshot.APIUsageKnown = true
			snapshot.APIInputTokens = 2
			snapshot.APICacheReadTokens = 88043
			snapshot.APITotalInputTokens = 88045
		}},
		{name: "复用已有整理结果", mutate: func(snapshot *requestOutcomeSnapshot) {
			snapshot.TriggerReason = TriggerNone
			snapshot.Action = outcomeActionDirect
			snapshot.BeforeMessages, snapshot.AfterMessages = 300, 42
		}},
		{name: "usage未知但其余正常", mutate: func(snapshot *requestOutcomeSnapshot) {
			snapshot.TriggerReason = TriggerNone
			snapshot.Action = outcomeActionDirect
			snapshot.APIUsageKnown = false
			snapshot.APITotalInputTokens = 0
		}},
	}
	for _, testCase := range quiet {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			out := projectTerminalOutcome(t, terminalOutcomeSnapshot(testCase.mutate), outcomeDispatchResult{SessionAccepted: true, TerminalAccepted: true})
			if out != "" {
				t.Fatalf("普通未折叠请求不应输出 ST 行: %q", out)
			}
		})
	}
}

func TestTerminalOutcomeFormatAndPrivacy(t *testing.T) {
	snapshot := terminalOutcomeSnapshot(nil)
	out := projectTerminalOutcome(t, snapshot, outcomeDispatchResult{SessionAccepted: true, TerminalAccepted: true})
	line := strings.TrimSuffix(out, "\n")

	if !strings.HasPrefix(line, logTestTime.Local().Format("15:04:05")+" ") {
		t.Fatalf("终端结果前缀不是 HH:MM:SS: %q", line)
	}
	if strings.Contains(line, "2026/") || strings.Contains(line, logTestTime.Format("2006/01/02")) {
		t.Fatalf("终端结果含年月日: %q", line)
	}
	assertTerminalOutcomeSafe(t, line)
	for _, sentinel := range []string{"4m0s", "5m0s", "240", "200"} {
		if strings.Contains(line, sentinel) {
			t.Fatalf("终端结果泄漏内部数值 %q: %q", sentinel, line)
		}
	}
	full := "session-secret-full-identity"
	leaky := terminalOutcomeSnapshot(func(mutable *requestOutcomeSnapshot) { mutable.SessionHash = full })
	leakyLine := projectTerminalOutcome(t, leaky, outcomeDispatchResult{SessionAccepted: true})
	if strings.Contains(leakyLine, full) {
		t.Fatalf("终端结果泄漏完整 session: %q", leakyLine)
	}
	if !strings.Contains(leakyLine, stableSessionHash(full)) {
		t.Fatalf("终端结果未归一为短 hash: %q", leakyLine)
	}
}

func TestTerminalOutcomeImmediateSessionReject(t *testing.T) {
	rejected := projectTerminalOutcome(t, terminalOutcomeSnapshot(nil), outcomeDispatchResult{SessionRejected: true, TerminalAccepted: true})
	if !strings.Contains(rejected, "详细记录未保存") {
		t.Fatalf("session 立即拒绝未追加缺失说明: %q", rejected)
	}
	if strings.Count(rejected, "\n") != 1 {
		t.Fatalf("session 立即拒绝产生多行: %q", rejected)
	}
	assertTerminalOutcomeSafe(t, strings.TrimSuffix(rejected, "\n"))

	accepted := projectTerminalOutcome(t, terminalOutcomeSnapshot(nil), outcomeDispatchResult{SessionAccepted: true, TerminalAccepted: true})
	if strings.Contains(accepted, "详细记录未保存") {
		t.Fatalf("session 已接受却提示未保存: %q", accepted)
	}
}

func TestTerminalIneligibleHasNoResultLine(t *testing.T) {
	for _, eligibility := range []outcomeEligibility{
		outcomeEligibilityTerminalIneligible,
		outcomeEligibilityNotApplicable,
		outcomeEligibilityUnknown,
	} {
		snapshot := terminalOutcomeSnapshot(func(mutable *requestOutcomeSnapshot) {
			mutable.TerminalEligibility = eligibility
		})
		if out := projectTerminalOutcome(t, snapshot, outcomeDispatchResult{SessionAccepted: true}); out != "" {
			t.Fatalf("terminal-ineligible=%s 仍输出结果行: %q", eligibility, out)
		}
	}
}

// terminalHealthLine 用固定时间渲染一次 typed transition，返回终端输出。
func terminalHealthLine(t *testing.T, transition HealthTransition) string {
	t.Helper()
	var buf bytes.Buffer
	reporter := NewTerminalHealthReporter(NewLogHandler(&buf, slog.LevelInfo))
	reporter.now = func() time.Time { return logTestTime }
	reporter.ReportHealthTransition(transition)
	return buf.String()
}

func TestTerminalHealthReporterAllScopes(t *testing.T) {
	cases := []struct {
		scope HealthScope
		kind  HealthTransitionKind
		want  string
	}{
		{HealthScopeSQLiteState, HealthTransitionEntered, "本地状态暂时无法保存到磁盘"},
		{HealthScopeSQLiteState, HealthTransitionRecovered, "本地状态已恢复正常保存"},
		{HealthScopeSQLiteArchive, HealthTransitionEntered, "归档暂时无法保存"},
		{HealthScopeSQLiteArchive, HealthTransitionRecovered, "归档已恢复正常保存"},
		{HealthScopeSessionQueueFull, HealthTransitionEntered, "详细记录暂时来不及保存"},
		{HealthScopeSessionQueueFull, HealthTransitionRecovered, "详细记录已恢复保存"},
		{HealthScopeSessionLogSink, HealthTransitionEntered, "详细记录暂时写不进文件"},
		{HealthScopeSessionLogSink, HealthTransitionRecovered, "详细记录文件写入已恢复"},
	}
	for _, testCase := range cases {
		testCase := testCase
		t.Run(string(testCase.scope)+"-"+string(testCase.kind), func(t *testing.T) {
			out := terminalHealthLine(t, makeHealthTransition(testCase.scope, HealthFailureClass(testCase.scope), testCase.kind, 7, 7))
			if strings.Count(out, "\n") != 1 {
				t.Fatalf("健康提示不是恰好一行: %q", out)
			}
			line := strings.TrimSuffix(out, "\n")
			if !strings.HasPrefix(line, logTestTime.Format("15:04:05")+" ") {
				t.Fatalf("健康提示前缀不是 HH:MM:SS: %q", line)
			}
			if !strings.Contains(line, testCase.want) {
				t.Fatalf("健康提示缺少人类影响文案 %q: %q", testCase.want, line)
			}
			for _, forbidden := range []string{
				string(testCase.scope), "generation", "7", "[INFO]", "[WARN]", "[ERROR]", "\033", "秒", "分钟",
			} {
				if strings.Contains(line, forbidden) {
					t.Fatalf("健康提示泄漏内部片段 %q: %q", forbidden, line)
				}
			}
		})
	}
}

func TestTerminalHealthReporterSkipsOngoing(t *testing.T) {
	for _, kind := range []HealthTransitionKind{HealthTransitionOngoing, HealthTransitionUnchanged} {
		for _, scope := range HealthScopes() {
			if out := terminalHealthLine(t, makeHealthTransition(scope, HealthFailureClass(scope), kind, 3, 0)); out != "" {
				t.Fatalf("scope=%s kind=%s 仍写终端: %q", scope, kind, out)
			}
		}
	}
	if out := terminalHealthLine(t, makeHealthTransition("other_scope", "other_scope", HealthTransitionEntered, 1, 0)); out != "" {
		t.Fatalf("未知 scope 仍写终端: %q", out)
	}
}

// countingHandler 统计任何 file-capable logger 是否被健康路径调用。
type countingHandler struct {
	calls int
}

func (h *countingHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *countingHandler) Handle(context.Context, slog.Record) error {
	h.calls++
	return nil
}

func (h *countingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }

func (h *countingHandler) WithGroup(string) slog.Handler { return h }

func TestTerminalHealthReporterHasNoFileCapableDependency(t *testing.T) {
	var terminal bytes.Buffer
	dataDir := t.TempDir()
	fileHandler := NewSessionLogHandler(dataDir, slog.LevelDebug, nil)
	combinedProbe := &countingHandler{}
	defaultProbe := &countingHandler{}
	previous := slog.Default()
	slog.SetDefault(slog.New(defaultProbe))
	t.Cleanup(func() { slog.SetDefault(previous) })
	_ = NewCombinedLogHandler(combinedProbe, fileHandler)

	reporter := NewTerminalHealthReporter(NewLogHandler(&terminal, slog.LevelInfo))
	tracker := NewHealthTracker(reporter)
	for _, scope := range HealthScopes() {
		tracker.ObserveFailure(scope, HealthFailureClass(scope), uint64(5))
		tracker.ObserveFailure(scope, HealthFailureClass(scope), uint64(6))
	}
	if lines := strings.Count(terminal.String(), "\n"); lines != len(HealthScopes()) {
		t.Fatalf("四 scope entered 行数=%d，want %d: %q", lines, len(HealthScopes()), terminal.String())
	}
	if combinedProbe.calls != 0 || defaultProbe.calls != 0 {
		t.Fatalf("健康提示流经 file-capable logger: combined=%d default=%d", combinedProbe.calls, defaultProbe.calls)
	}
	if entries, err := os.ReadDir(filepath.Join(dataDir, "logs")); err == nil && len(entries) != 0 {
		t.Fatalf("健康提示写入了文件日志: %v", entries)
	}

	source, err := os.ReadFile("loghandler.go")
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := parser.ParseFile(token.NewFileSet(), "loghandler.go", source, 0)
	if err != nil {
		t.Fatal(err)
	}
	forbidden := map[string]bool{
		"SessionLogHandler": true, "CombinedLogHandler": true, "slog": true, "Logger": true,
	}
	var leaked string
	inspectReporter := func(node ast.Node) {
		ast.Inspect(node, func(inner ast.Node) bool {
			ident, ok := inner.(*ast.Ident)
			if ok && forbidden[ident.Name] {
				leaked = ident.Name
			}
			if selector, ok := inner.(*ast.SelectorExpr); ok && selector.Sel.Name == "Handle" {
				leaked = "Handle"
			}
			return true
		})
	}
	for _, decl := range parsed.Decls {
		switch typed := decl.(type) {
		case *ast.FuncDecl:
			if typed.Name.Name == "NewTerminalHealthReporter" || receiverTypeName(typed) == "TerminalHealthReporter" {
				inspectReporter(typed)
			}
		case *ast.GenDecl:
			for _, spec := range typed.Specs {
				typeSpec, ok := spec.(*ast.TypeSpec)
				if ok && typeSpec.Name.Name == "TerminalHealthReporter" {
					inspectReporter(typeSpec)
				}
			}
		}
	}
	if leaked != "" {
		t.Fatalf("TerminalHealthReporter 依赖了 file-capable 资源: %s", leaked)
	}
}

func receiverTypeName(decl *ast.FuncDecl) string {
	if decl.Recv == nil || len(decl.Recv.List) == 0 {
		return ""
	}
	switch typed := decl.Recv.List[0].Type.(type) {
	case *ast.StarExpr:
		if ident, ok := typed.X.(*ast.Ident); ok {
			return ident.Name
		}
	case *ast.Ident:
		return typed.Name
	}
	return ""
}

func TestLogHandlerRequestAttrsSharedAcrossLines(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(NewLogHandler(&buf, slog.LevelInfo)).With(
		"request_id", uint64(9),
		"request_session_id", "current-session",
	)
	logger.Info("请求进入")
	logger.Info("上游请求发送")

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("日志行数=%d，期望 2: %q", len(lines), buf.String())
	}
	for _, line := range lines {
		// LOG-03：request_session_id 只用于文件路由，不再渲染到终端；
		// 贯穿各行的固定关联字段因此只剩 request_id。
		if !strings.Contains(line, "request_id=9") {
			t.Fatalf("请求固定字段未贯穿日志行: %q", line)
		}
		if strings.Contains(line, "current-session") || strings.Contains(line, "request_session_id") {
			t.Fatalf("终端日志行泄漏 session 身份: %q", line)
		}
	}
}

// ── Plan 10 Task 3：终端 sink 的 session 身份脱敏（LOG-03 Gap 3） ──

// TestTerminalSinkRedactsSessionIdentityAttrs 同时覆盖 WithAttrs 与 record attrs
// 两条路径：meta.Logger 的 request_session_id 是经 WithAttrs 进来的，只过滤
// record attrs 会漏掉真实生产形态。
func TestTerminalSinkRedactsSessionIdentityAttrs(t *testing.T) {
	const (
		secretRequestSession = "SECRET-REQ-SESSION-9f1c"
		secretSession        = "SECRET-SESSION-2b7e"
		secretSourceSession  = "SECRET-SOURCE-SESSION-4d0a"
		secretGroupedSession = "SECRET-GROUPED-SESSION-77aa"
	)

	t.Run("with_attrs_path", func(t *testing.T) {
		var buf bytes.Buffer
		logger := slog.New(NewLogHandler(&buf, slog.LevelInfo)).With(
			"request_id", uint64(11),
			"request_session_id", secretRequestSession,
			"session_id", secretSession,
			"source_session_id", secretSourceSession,
			"model", "deepseek-v4-pro",
		)
		logger.WithGroup("recall").Info("上游请求发送", "origin_session_id", secretGroupedSession, "candidates", 3)
		got := buf.String()

		assertNoSessionIdentityInTerminal(t, got, secretRequestSession, secretSession, secretSourceSession, secretGroupedSession)
		for _, want := range []string{"request_id=11", "model=deepseek-v4-pro", "recall.candidates=3"} {
			if !strings.Contains(got, want) {
				t.Fatalf("脱敏误伤普通属性，缺少 %q: %q", want, got)
			}
		}
	})

	t.Run("record_attrs_path", func(t *testing.T) {
		var buf bytes.Buffer
		h := NewLogHandler(&buf, slog.LevelInfo)
		got := emitLogRecord(t, h, &buf, slog.LevelInfo, "frozen prefix 已存储",
			slog.String("request_session_id", secretRequestSession),
			slog.String("session_id", secretSession),
			slog.String("source_session_id", secretSourceSession),
			slog.String("upstream_session_id", secretGroupedSession),
			slog.Int("original_message_count", 80),
			slog.Int("cutoff", 40),
			slog.Int("tokens", 10903),
		)

		assertNoSessionIdentityInTerminal(t, got, secretRequestSession, secretSession, secretSourceSession, secretGroupedSession)
		for _, want := range []string{"original_message_count=80", "cutoff=40", "tokens=10903"} {
			if !strings.Contains(got, want) {
				t.Fatalf("脱敏误伤普通属性，缺少 %q: %q", want, got)
			}
		}
		// 终端时间格式未被本次改动波及。
		if !strings.HasPrefix(got, logTestPrefix) {
			t.Fatalf("终端时间格式被改动: %q", got)
		}
	})

	// 两个 sink 必须共用同一判定——各写一份正是 Gap 3 的根因。
	t.Run("both_sinks_agree", func(t *testing.T) {
		for _, key := range []string{
			requestSessionIDAttr, sessionIDAttr, sourceSessionIDAttr,
			"origin_session_id", "upstream_session_id", "Request_Session_ID",
		} {
			if !isSessionIdentityLogAttr(key) {
				t.Fatalf("%q 未被判为 session 身份属性", key)
			}
			if isVisibleFileLogAttr(key) {
				t.Fatalf("%q 被终端判为 session 身份，却在文件侧可见（两个 sink 判定漂移）", key)
			}
		}
		for _, key := range []string{
			"request_id", "model", "cutoff", "tokens", "original_message_count", "session_hash",
		} {
			if isSessionIdentityLogAttr(key) {
				t.Fatalf("%q 被误判为 session 身份属性", key)
			}
			if !isVisibleFileLogAttr(key) {
				t.Fatalf("%q 在文件侧的既有可见性被改动", key)
			}
		}
	})
}

// assertNoSessionIdentityInTerminal 同时否定属性键与属性值：只删值不删键仍会
// 暴露「这条日志属于某个 session」的结构，且与文件侧行为不一致。
func assertNoSessionIdentityInTerminal(t *testing.T, output string, secrets ...string) {
	t.Helper()
	for _, key := range []string{"request_session_id", "session_id", "source_session_id", "_session_id"} {
		if strings.Contains(output, key) {
			t.Fatalf("终端输出含 session 身份属性键 %q: %q", key, output)
		}
	}
	for _, secret := range secrets {
		if strings.Contains(output, secret) {
			t.Fatalf("终端输出含完整 session ID %q: %q", secret, output)
		}
	}
}

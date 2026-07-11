package proxy

import (
	"bytes"
	"context"
	"log/slog"
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
		if !strings.Contains(line, "request_id=9") || !strings.Contains(line, "request_session_id=current-session") {
			t.Fatalf("请求固定字段未贯穿日志行: %q", line)
		}
	}
}

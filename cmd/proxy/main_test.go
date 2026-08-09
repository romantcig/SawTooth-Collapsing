package main

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode"

	"github.com/romantcig/sawtooth-collapsing/internal/proxy"
)

type startupMessageCapture struct {
	messages []string
	attrs    []map[string]string
}

func (*startupMessageCapture) Enabled(context.Context, slog.Level) bool { return true }

func (h *startupMessageCapture) Handle(_ context.Context, record slog.Record) error {
	h.messages = append(h.messages, record.Message)
	attrs := make(map[string]string)
	record.Attrs(func(attr slog.Attr) bool {
		attrs[attr.Key] = attr.Value.Resolve().String()
		return true
	})
	h.attrs = append(h.attrs, attrs)
	return nil
}

func (h *startupMessageCapture) WithAttrs([]slog.Attr) slog.Handler { return h }

func (h *startupMessageCapture) WithGroup(string) slog.Handler { return h }

func TestStartupTerminalOmitsWaitDetailsForEphemeralAndOneHour(t *testing.T) {
	for _, cacheTTL := range []string{"ephemeral", "1h"} {
		t.Run(cacheTTL, func(t *testing.T) {
			previous := slog.Default()
			capture := &startupMessageCapture{}
			var terminal bytes.Buffer
			combined := proxy.NewCombinedLogHandler(
				proxy.NewLogHandler(&terminal, slog.LevelInfo),
				capture,
			)
			slog.SetDefault(slog.New(combined))
			t.Cleanup(func() { slog.SetDefault(previous) })

			cfg := proxy.DefaultConfig()
			cfg.Cache.CacheTTL = cacheTTL
			trigger := newSawtoothTrigger(cfg)
			wait := proxy.CacheGapForTTL(cacheTTL)
			if evaluation := trigger.Evaluate("startup", 0, time.Now()); evaluation.RequiredWait != wait {
				t.Fatalf("startup trigger RequiredWait=%s, want %s", evaluation.RequiredWait, wait)
			}

			logSawtoothStartup(cfg.Frozen.Enabled)

			runtimeTokens := []string{
				wait.String(),
				strconv.FormatFloat(wait.Seconds(), 'f', -1, 64),
				strconv.FormatFloat(wait.Seconds(), 'f', -1, 64) + "s",
				strconv.FormatInt(int64(wait/time.Minute), 10) + "m",
			}
			for index, message := range capture.messages {
				assertStartupTextOmitsWaitDetails(t, "message["+strconv.Itoa(index)+"]", message, runtimeTokens)
			}
			for index, attrs := range capture.attrs {
				for key, value := range attrs {
					if startupWaitDetailKey(key) {
						t.Fatalf("attrs[%d] 泄露等待字段 %q=%q", index, key, value)
					}
					for _, token := range runtimeTokens {
						if containsCompleteStartupToken(value, token) {
							t.Fatalf("attrs[%d] %q 泄露运行时等待值 %q", index, key, token)
						}
					}
				}
			}
			for index, line := range strings.Split(strings.TrimSpace(terminal.String()), "\n") {
				payload := startupTerminalPayload(line)
				assertStartupTextOmitsWaitDetails(t, "terminal["+strconv.Itoa(index)+"]", payload, runtimeTokens)
			}

			source, err := os.ReadFile("main.go")
			if err != nil {
				t.Fatalf("读取 main.go: %v", err)
			}
			for _, literal := range []string{
				`"pause_threshold"`,
				`"token_threshold"`,
				`"token_minimum"`,
				`"ttl_minutes"`,
				`"required_wait"`,
				`"duration"`,
				`"seconds"`,
			} {
				if bytes.Contains(source, []byte(literal)) {
					t.Fatalf("main.go 仍含 terminal-visible 启动字段 %s", literal)
				}
			}
		})
	}
}

func TestStartupDoesNotEnableAsyncObservability(t *testing.T) {
	source, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatalf("读取 main.go: %v", err)
	}
	text := string(source)
	for _, forbidden := range []string{"NewPersistenceWriter(", "NewOutcomeDispatcher("} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("Task 2 不得提前启用异步组件: %s", forbidden)
		}
	}
	if !strings.Contains(text, "proxy.CacheGapForTTL(cfg.Cache.CacheTTL)") {
		t.Fatal("main 必须继续用 CacheGapForTTL 初始化真实 trigger")
	}
	signalIndex := strings.Index(text, "signal.Notify(sigCh, os.Interrupt)")
	storeCloseIndex := strings.Index(text, "srv.Store.Close()")
	if signalIndex < 0 || storeCloseIndex < 0 || storeCloseIndex < signalIndex {
		t.Fatal("既有 signal/Store.Close shutdown 顺序被改动")
	}
}

func assertStartupTextOmitsWaitDetails(t *testing.T, source, text string, runtimeTokens []string) {
	t.Helper()
	lower := strings.ToLower(text)
	for _, forbidden := range []string{
		"threshold",
		"token_minimum",
		"required_wait",
		"required wait",
		"ttl_minutes",
		"duration",
		"seconds",
		"330s",
		"等待阈值",
	} {
		if strings.Contains(lower, forbidden) {
			t.Fatalf("%s 泄露等待细节 %q: %q", source, forbidden, text)
		}
	}
	for _, token := range runtimeTokens {
		if containsCompleteStartupToken(text, token) {
			t.Fatalf("%s 泄露运行时等待值 %q: %q", source, token, text)
		}
	}
}

func startupWaitDetailKey(key string) bool {
	lower := strings.ToLower(key)
	return strings.Contains(lower, "threshold") ||
		strings.Contains(lower, "minimum") ||
		strings.Contains(lower, "wait") ||
		strings.Contains(lower, "ttl") ||
		strings.Contains(lower, "duration") ||
		strings.Contains(lower, "seconds") ||
		strings.Contains(lower, "minutes")
}

func containsCompleteStartupToken(text, token string) bool {
	if token == "" {
		return false
	}
	for _, field := range strings.FieldsFunc(text, func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '_' && r != '.'
	}) {
		if field == token {
			return true
		}
	}
	return false
}

func startupTerminalPayload(line string) string {
	if labelEnd := strings.Index(line, "]"); labelEnd >= 0 {
		return strings.TrimSpace(line[labelEnd+1:])
	}
	return strings.TrimSpace(line)
}

func TestFullBodyDebugStartupNotice(t *testing.T) {
	for _, tc := range []struct {
		name     string
		enabled  bool
		fullBody bool
		want     []string
	}{
		{name: "debug disabled", enabled: false, fullBody: true},
		{name: "full body disabled", enabled: true, fullBody: false},
		{name: "explicit opt in", enabled: true, fullBody: true, want: []string{"完整正文 Debug 已开启"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			previous := slog.Default()
			capture := &startupMessageCapture{}
			slog.SetDefault(slog.New(capture))
			t.Cleanup(func() { slog.SetDefault(previous) })

			cfg := proxy.DefaultConfig()
			cfg.Debug.Enabled = tc.enabled
			cfg.Debug.FullBody = tc.fullBody
			logFullBodyDebugNotice(cfg)

			if len(capture.messages) != len(tc.want) {
				t.Fatalf("启动提示=%v，want %v", capture.messages, tc.want)
			}
			for index := range tc.want {
				if capture.messages[index] != tc.want[index] {
					t.Fatalf("启动提示[%d]=%q，want %q", index, capture.messages[index], tc.want[index])
				}
			}
		})
	}
}

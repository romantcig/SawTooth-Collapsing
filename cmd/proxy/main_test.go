package main

import (
	"bytes"
	"context"
	"errors"
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

func TestProductionPersistenceOutcomeWiring(t *testing.T) {
	text := readMainSource(t)

	for literal, want := range map[string]int{
		"proxy.NewHealthTracker(":            1,
		"proxy.NewTerminalHealthReporter(":   1,
		"proxy.NewPersistenceWriterChecked(": 1,
		"proxy.NewOutcomeDispatcherChecked(": 1,
		"proxy.NewOutcomeGapAccumulator(":    1,
		"proxy.NewSessionOutcomeWriter(":     1,
		"srv.SetOutcomeDispatcher(":          1,
		"srv.SetArchiveHealthObserver(":      1,
	} {
		if got := strings.Count(text, literal); got != want {
			t.Errorf("main.go 中 %s 出现 %d 次, want %d", literal, got, want)
		}
	}
	// 同一个 tracker/reporter 实例必须同时注入 writer 与 dispatcher。
	if !strings.Contains(text, "healthTracker, healthReporter") {
		t.Error("writer 未收到与 dispatcher 相同的 tracker/reporter 实例")
	}
	if !strings.Contains(text, "HealthTracker:     healthTracker") ||
		!strings.Contains(text, "Reporter:          healthReporter") {
		t.Error("dispatcher 未收到共享 tracker/reporter")
	}
	// History transition 与 Archive committer 均不得进入普通 writer。
	if strings.Contains(text, "SetStateSubmitter") && !strings.Contains(text, "persistenceWriter") {
		t.Error("state submitter 未接到全局 writer")
	}
	if strings.Contains(text, "HistoryEpoch.SetStateSubmitter") {
		t.Error("history transition 被放进了普通 writer")
	}
	if !strings.Contains(text, "SetTransitionFunc(store.CommitHistoryTransition)") {
		t.Error("history transition 未保持同步 Store 直连")
	}
	for _, forbidden := range []string{"nil, nil, nil", "no-op reporter"} {
		if strings.Contains(text, forbidden) {
			t.Errorf("main.go 出现 nil/no-op wiring: %s", forbidden)
		}
	}
}

func TestProductionHealthReporterIsTerminalOnly(t *testing.T) {
	text := readMainSource(t)
	index := strings.Index(text, "proxy.NewTerminalHealthReporter(")
	if index < 0 {
		t.Fatal("main.go 未构造 terminal health reporter")
	}
	call := text[index : index+strings.Index(text[index:], ")")+1]
	if !strings.Contains(call, "terminalHandler") {
		t.Fatalf("health reporter 未直接绑定终端 handler: %s", call)
	}
	for _, forbidden := range []string{"fileHandler", "combined", "Combined", "SessionLog", "slog.Default"} {
		if strings.Contains(call, forbidden) {
			t.Fatalf("health reporter 被 file-capable logger 适配: %s", call)
		}
	}
}

func TestProductionUsesErrorAwareLoadersOnly(t *testing.T) {
	text := readMainSource(t)
	if got := strings.Count(text, "SetStateLoader(store)"); got != 4 {
		t.Fatalf("error-aware loader 接线=%d 处, want 4", got)
	}
	for _, forbidden := range []string{"SetLoadFunc(store.LoadState)", "store.LoadState)", ".LoadState("} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("main.go 仍引用 bool-only loader: %s", forbidden)
		}
	}
}

func TestProductionDoesNotDiscardStoreErrors(t *testing.T) {
	text := readMainSource(t)
	for _, forbidden := range []string{
		"_ = store.PersistState(",
		"_ = store.DeleteState(",
		"_ = store.SaveArchive",
		"_ = srv.Store.PersistState(",
	} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("main.go 用忽略返回值的闭包包装 Store 写入: %s", forbidden)
		}
	}
	if strings.Contains(text, "os.Exit(0)") {
		t.Fatal("signal 路径仍直接 os.Exit")
	}
}

func TestShutdownStrictDrainOrder(t *testing.T) {
	var recorded []lifecycleStage
	record := func(stage lifecycleStage) { recorded = append(recorded, stage) }

	steps := productionShutdownSteps(
		func() error { return errors.New("http shutdown failed") },
		func() error { return nil },
		func() error { return nil },
		func() error { return nil },
	)
	errs := runShutdown(steps, record)

	want := []lifecycleStage{stageHTTPStopped, stagePersistenceDrained, stageOutcomeDrained, stageStoreClosed}
	if len(recorded) != len(want) {
		t.Fatalf("关闭阶段=%v, want %v", recorded, want)
	}
	for index := range want {
		if recorded[index] != want[index] {
			t.Fatalf("关闭顺序=%v, want %v", recorded, want)
		}
	}
	if len(errs) != 1 {
		t.Fatalf("阶段错误数=%d, want 1（前置失败不得中断后续安全阶段）", len(errs))
	}
	if !strings.Contains(errs[0].Error(), string(stageHTTPStopped)) {
		t.Fatalf("阶段错误未标注来源: %v", errs[0])
	}
}

func readMainSource(t *testing.T) string {
	t.Helper()
	source, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatalf("读取 main.go: %v", err)
	}
	return string(source)
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

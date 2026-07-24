package main

import (
	"context"
	"log/slog"
	"testing"

	"github.com/romantcig/sawtooth-collapsing/internal/proxy"
)

type startupMessageCapture struct {
	messages []string
}

func (*startupMessageCapture) Enabled(context.Context, slog.Level) bool { return true }

func (h *startupMessageCapture) Handle(_ context.Context, record slog.Record) error {
	h.messages = append(h.messages, record.Message)
	return nil
}

func (h *startupMessageCapture) WithAttrs([]slog.Attr) slog.Handler { return h }

func (h *startupMessageCapture) WithGroup(string) slog.Handler { return h }

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

package proxy

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestForwardRawTargetTrailingSlash(t *testing.T) {
	receivedURI := make(chan string, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedURI <- r.URL.RequestURI()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"type":"message","usage":{"input_tokens":1,"output_tokens":1}}`))
	}))
	defer upstream.Close()

	s := NewServer(Config{
		Proxy: ProxyConfig{
			Target:    upstream.URL + "/",
			Deflation: 1,
		},
	})

	req := httptest.NewRequest(http.MethodPost, "/v1/messages?beta=true", strings.NewReader(`{"model":"test","messages":[]}`))
	recorder := httptest.NewRecorder()
	s.forwardRaw(recorder, req, newRequestMeta(1, "test-session"))

	if got := <-receivedURI; got != "/v1/messages?beta=true" {
		t.Fatalf("上游请求 URI = %q，期望 %q", got, "/v1/messages?beta=true")
	}
	if recorder.Code != http.StatusOK {
		t.Fatalf("代理响应状态码 = %d，期望 %d", recorder.Code, http.StatusOK)
	}
}

func TestForwardRawRequestLogFields(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"type":"message","usage":{"input_tokens":1,"output_tokens":1}}`))
	}))
	defer upstream.Close()

	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(NewLogHandler(&logs, slog.LevelDebug)))
	t.Cleanup(func() { slog.SetDefault(previous) })

	s := NewServer(Config{Proxy: ProxyConfig{Target: upstream.URL, Deflation: 1}})
	meta := newRequestMeta(17, "request-session")
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"model":"test-model","messages":[{"role":"user","content":"one"},{"role":"assistant","content":"two"}]}`))
	recorder := httptest.NewRecorder()
	s.forwardRaw(recorder, req, meta)

	output := logs.String()
	for _, want := range []string{
		"请求进入 request_id=17 request_session_id=request-session original_message_count=2 model=test-model",
		"上游请求发送 request_id=17 request_session_id=request-session forwarded_message_count=2 model=test-model",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("日志缺少 %q:\n%s", want, output)
		}
	}
	if strings.Contains(output, "请求已接收") || strings.Contains(output, "Authorization") {
		t.Fatalf("日志包含旧事件名或敏感 header:\n%s", output)
	}
}

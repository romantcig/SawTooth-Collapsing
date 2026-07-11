package proxy

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestWriteDebugFileRedactsCredentialHeaders(t *testing.T) {
	dataDir := t.TempDir()
	s := NewServer(Config{Debug: DebugConfig{DataDir: dataDir}})
	headers := http.Header{
		"Authorization":       {"Bearer auth-secret"},
		"Proxy-Authorization": {"Basic proxy-secret"},
		"X-Api-Key":           {"api-secret"},
		"Anthropic-Api-Key":   {"anthropic-secret"},
		"Cookie":              {"session=cookie-secret"},
		"X-Diagnostic":        {"safe-value"},
	}
	timestamp := time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC)
	s.writeDebugFile("session", timestamp, "req", []byte(`{"ok":true}`), headers, "model", 1)

	debugDir, ok := safeDebugSessionDir(dataDir, "session")
	if !ok {
		t.Fatal("合法 session debug 目录校验失败")
	}
	data, err := os.ReadFile(filepath.Join(debugDir, "2026-07-11T120000.000-req.json"))
	if err != nil {
		t.Fatalf("读取 debug 文件: %v", err)
	}
	for _, secret := range []string{"auth-secret", "proxy-secret", "api-secret", "anthropic-secret", "cookie-secret"} {
		if bytes.Contains(data, []byte(secret)) {
			t.Fatalf("debug 文件泄漏凭证 %q: %s", secret, data)
		}
	}
	var entry debugEntry
	if err := json.Unmarshal(data, &entry); err != nil {
		t.Fatalf("解析 debug 文件: %v", err)
	}
	if !bytes.Contains(entry.Headers, []byte("safe-value")) {
		t.Fatalf("诊断 header 未保留: %s", entry.Headers)
	}
}

func TestWriteDebugFileSessionPathCannotEscapeDebugRoot(t *testing.T) {
	for _, sessionID := range []string{"../escape", `..\\escape`, `C:\\escape`, `\\\\server\\share\\escape`} {
		t.Run(sessionID, func(t *testing.T) {
			dataDir := t.TempDir()
			s := NewServer(Config{Debug: DebugConfig{DataDir: dataDir}})
			s.writeDebugFile(sessionID, time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC), "req", []byte(`{}`), nil, "model", 0)

			debugDir, ok := safeDebugSessionDir(dataDir, sessionID)
			if !ok {
				t.Fatal("哈希后的 session 目录应通过根目录校验")
			}
			root, _ := filepath.Abs(filepath.Join(dataDir, "debug"))
			rel, err := filepath.Rel(root, debugDir)
			if err != nil || strings.HasPrefix(rel, "..") || filepath.IsAbs(rel) {
				t.Fatalf("debug 目录逃逸: root=%s dir=%s rel=%s err=%v", root, debugDir, rel, err)
			}
			if _, err := os.Stat(filepath.Join(debugDir, "2026-07-11T120000.000-req.json")); err != nil {
				t.Fatalf("debug 文件未写入哈希目录: %v", err)
			}
		})
	}
}

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

package proxy

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestTotalInputTokens(t *testing.T) {
	tests := []struct {
		name  string
		usage map[string]any
		want  int
	}{
		{name: "真实 cache hit 25", usage: map[string]any{"input_tokens": 196, "cache_creation_input_tokens": 0, "cache_read_input_tokens": 93056}, want: 93252},
		{name: "真实 cache hit 26", usage: map[string]any{"input_tokens": json.Number("5559"), "cache_read_input_tokens": float64(15744)}, want: 21303},
		{name: "缺失字段", usage: map[string]any{"input_tokens": int64(7)}, want: 7},
		{name: "负数与非数字忽略", usage: map[string]any{"input_tokens": -1, "cache_creation_input_tokens": "secret", "cache_read_input_tokens": 4}, want: 4},
		{name: "NaN Inf 忽略", usage: map[string]any{"input_tokens": math.NaN(), "cache_creation_input_tokens": math.Inf(1), "cache_read_input_tokens": 3}, want: 3},
		{name: "单字段饱和", usage: map[string]any{"input_tokens": json.Number("9223372036854775807")}, want: math.MaxInt},
		{name: "求和饱和", usage: map[string]any{"input_tokens": math.MaxInt, "cache_creation_input_tokens": math.MaxInt, "cache_read_input_tokens": math.MaxInt}, want: math.MaxInt},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := totalInputTokens(tt.usage); got != tt.want {
				t.Fatalf("totalInputTokens() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestHandleSSECacheUsagePersistsTotalBeforeDeflation(t *testing.T) {
	trigger := NewSawtoothTrigger(time.Hour, 50000, 1000)
	var persisted string
	trigger.SetPersistFunc(func(_ string, value string) { persisted = value })
	s := NewServer(Config{Proxy: ProxyConfig{Deflation: 0.5}})
	s.Sawtooth = trigger
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": {"text/event-stream"}},
		Body: io.NopCloser(strings.NewReader("event: message_start\n" +
			"data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":196,\"cache_creation_input_tokens\":0,\"cache_read_input_tokens\":93056,\"output_tokens\":20}}}\n\n")),
	}
	recorder := httptest.NewRecorder()
	s.handleSSE(recorder, resp, newRequestMeta(1, "sse-cache"), time.Now(), "model", 25)

	var state persistedState
	if err := json.Unmarshal([]byte(persisted), &state); err != nil {
		t.Fatalf("解析持久状态: %v; raw=%q", err, persisted)
	}
	if state.Tokens != 93252 || state.MsgCount != 25 {
		t.Fatalf("SSE 持久状态=%+v, want tokens=93252 msg_count=25", state)
	}
	if strings.Contains(recorder.Body.String(), `"input_tokens":196`) || !strings.Contains(recorder.Body.String(), `"input_tokens":98`) {
		t.Fatalf("客户端 deflation 行为变化: %s", recorder.Body.String())
	}
}

func TestHandleJSONCacheUsagePersistsTotalAndColdStartTriggers(t *testing.T) {
	trigger := NewSawtoothTrigger(time.Hour, 50000, 1000)
	var persisted string
	trigger.SetPersistFunc(func(_ string, value string) { persisted = value })
	s := NewServer(Config{Proxy: ProxyConfig{Deflation: 0.5}})
	s.Sawtooth = trigger
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": {"application/json"}},
		Body:       io.NopCloser(strings.NewReader(`{"type":"message","usage":{"input_tokens":196,"cache_creation_input_tokens":0,"cache_read_input_tokens":93056,"output_tokens":20}}`)),
	}
	recorder := httptest.NewRecorder()
	s.handleJSON(recorder, resp, newRequestMeta(2, "json-cache"), time.Now(), "model", 25)

	var state persistedState
	if err := json.Unmarshal([]byte(persisted), &state); err != nil {
		t.Fatalf("解析持久状态: %v; raw=%q", err, persisted)
	}
	if state.Tokens != 93252 || state.MsgCount != 25 {
		t.Fatalf("JSON 持久状态=%+v, want tokens=93252 msg_count=25", state)
	}
	if strings.Contains(recorder.Body.String(), `"input_tokens":196`) || !strings.Contains(recorder.Body.String(), `"input_tokens":98`) {
		t.Fatalf("客户端 deflation 行为变化: %s", recorder.Body.String())
	}

	restored := NewSawtoothTrigger(time.Hour, 50000, 1000)
	restored.SetLoadFunc(func(key string) (string, bool) {
		return persisted, key == "sawtooth:json-cache"
	})
	if got := restored.ShouldTrigger("json-cache", 1); got != TriggerTokens {
		t.Fatalf("冷启动 trigger=%q, want %q", got, TriggerTokens)
	}
}

type failingDebugFile struct {
	file     *os.File
	writeErr error
	closeErr error
}

func (f *failingDebugFile) Write(data []byte) (int, error) {
	if f.writeErr != nil {
		n, _ := f.file.Write(data[:len(data)/2])
		return n, f.writeErr
	}
	return f.file.Write(data)
}

func (f *failingDebugFile) Close() error {
	err := f.file.Close()
	if f.closeErr != nil {
		return f.closeErr
	}
	return err
}

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
	s.writeDebugFile("session", 1, timestamp, "req", []byte(`{"ok":true}`), headers, "model", 1)

	debugDir, ok := safeDebugSessionDir(dataDir, "session")
	if !ok {
		t.Fatal("合法 session debug 目录校验失败")
	}
	data, err := os.ReadFile(filepath.Join(debugDir, "2026-07-11T120000.000000000-1-req.json"))
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
			s.writeDebugFile(sessionID, 1, time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC), "req", []byte(`{}`), nil, "model", 0)

			debugDir, ok := safeDebugSessionDir(dataDir, sessionID)
			if !ok {
				t.Fatal("哈希后的 session 目录应通过根目录校验")
			}
			root, _ := filepath.Abs(filepath.Join(dataDir, "debug"))
			rel, err := filepath.Rel(root, debugDir)
			if err != nil || strings.HasPrefix(rel, "..") || filepath.IsAbs(rel) {
				t.Fatalf("debug 目录逃逸: root=%s dir=%s rel=%s err=%v", root, debugDir, rel, err)
			}
			if _, err := os.Stat(filepath.Join(debugDir, "2026-07-11T120000.000000000-1-req.json")); err != nil {
				t.Fatalf("debug 文件未写入哈希目录: %v", err)
			}
		})
	}
}

func TestWriteDebugFileUsesRequestIDToPreventCollisions(t *testing.T) {
	dataDir := t.TempDir()
	s := NewServer(Config{Debug: DebugConfig{DataDir: dataDir}})
	timestamp := time.Date(2026, 7, 11, 12, 0, 0, 123, time.UTC)
	var wg sync.WaitGroup
	for _, requestID := range []uint64{41, 42} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.writeDebugFile("session", requestID, timestamp, "req", []byte(`{"request":true}`), nil, "model", 1)
		}()
	}
	wg.Wait()
	debugDir, _ := safeDebugSessionDir(dataDir, "session")
	entries, err := os.ReadDir(debugDir)
	if err != nil {
		t.Fatalf("读取 debug 目录: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("并发 debug 文件数=%d, want 2", len(entries))
	}
	if entries[0].Name() == entries[1].Name() {
		t.Fatalf("并发请求文件名冲突: %s", entries[0].Name())
	}
}

func TestWriteDebugEntryFileRemovesPartialFileOnFailure(t *testing.T) {
	for _, tt := range []struct {
		name     string
		writeErr error
		closeErr error
	}{
		{name: "write error", writeErr: errors.New("injected write error")},
		{name: "close error", closeErr: errors.New("injected close error")},
	} {
		t.Run(tt.name, func(t *testing.T) {
			filePath := filepath.Join(t.TempDir(), "entry.json")
			err := writeDebugEntryFile(filePath, []byte(`{"complete":true}`), func(name string, flag int, perm os.FileMode) (debugWriteCloser, error) {
				file, err := os.OpenFile(name, flag, perm)
				if err != nil {
					return nil, err
				}
				return &failingDebugFile{file: file, writeErr: tt.writeErr, closeErr: tt.closeErr}, nil
			})
			if err == nil {
				t.Fatal("注入失败后应返回错误")
			}
			if _, statErr := os.Stat(filePath); !os.IsNotExist(statErr) {
				t.Fatalf("失败后残留截断 debug 文件: %v", statErr)
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

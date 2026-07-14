package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
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

func TestHandleSSEPressureBaseline(t *testing.T) {
	trigger := NewSawtoothTrigger(time.Hour, 50000, 1000)
	var persisted string
	trigger.SetPersistFunc(func(_ string, value string) { persisted = value })
	s := NewServer(Config{Proxy: ProxyConfig{Deflation: 0.5}})
	s.Sawtooth = trigger
	system := json.RawMessage(`[{"type":"text","text":"sse system"}]`)
	tools := json.RawMessage(`[{"name":"sse_tool","input_schema":{"type":"object"}}]`)
	meta := newRequestMeta(1, "sse-cache")
	meta.PressureDecision = pressureDecision{
		Available:         true,
		MessageCount:      37,
		SystemFingerprint: fingerprintTopLevelJSON(system),
		ToolsFingerprint:  fingerprintTopLevelJSON(tools),
	}
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": {"text/event-stream"}},
		Body: io.NopCloser(strings.NewReader("event: message_start\n" +
			"data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":196,\"cache_creation_input_tokens\":0,\"cache_read_input_tokens\":93056,\"output_tokens\":20}}}\n\n")),
	}
	recorder := httptest.NewRecorder()
	s.handleSSE(recorder, resp, meta, time.Now(), "model", 4)

	var state persistedState
	if err := json.Unmarshal([]byte(persisted), &state); err != nil {
		t.Fatalf("解析持久状态: %v; raw=%q", err, persisted)
	}
	if state.Tokens != 93252 || state.MsgCount != meta.PressureDecision.MessageCount || state.SystemFingerprint != meta.PressureDecision.SystemFingerprint || state.ToolsFingerprint != meta.PressureDecision.ToolsFingerprint {
		t.Fatalf("SSE 持久状态=%+v, want actual=93252 original_count=%d fingerprints=%q/%q", state, meta.PressureDecision.MessageCount, meta.PressureDecision.SystemFingerprint, meta.PressureDecision.ToolsFingerprint)
	}
	if strings.Contains(recorder.Body.String(), `"input_tokens":196`) || !strings.Contains(recorder.Body.String(), `"input_tokens":98`) {
		t.Fatalf("客户端 deflation 行为变化: %s", recorder.Body.String())
	}
}

func TestHandleJSONPressureBaseline(t *testing.T) {
	trigger := NewSawtoothTrigger(time.Hour, 50000, 1000)
	var persisted string
	trigger.SetPersistFunc(func(_ string, value string) { persisted = value })
	s := NewServer(Config{Proxy: ProxyConfig{Deflation: 0.5}})
	s.Sawtooth = trigger
	system := json.RawMessage(`[{"type":"text","text":"json system"}]`)
	tools := json.RawMessage(`[{"name":"json_tool","input_schema":{"type":"object"}}]`)
	meta := newRequestMeta(2, "json-cache")
	meta.PressureDecision = pressureDecision{
		Available:         true,
		MessageCount:      41,
		SystemFingerprint: fingerprintTopLevelJSON(system),
		ToolsFingerprint:  fingerprintTopLevelJSON(tools),
	}
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": {"application/json"}},
		Body:       io.NopCloser(strings.NewReader(`{"type":"message","usage":{"input_tokens":196,"cache_creation_input_tokens":0,"cache_read_input_tokens":93056,"output_tokens":20}}`)),
	}
	recorder := httptest.NewRecorder()
	s.handleJSON(recorder, resp, meta, time.Now(), "model", 3)

	var state persistedState
	if err := json.Unmarshal([]byte(persisted), &state); err != nil {
		t.Fatalf("解析持久状态: %v; raw=%q", err, persisted)
	}
	if state.Tokens != 93252 || state.MsgCount != meta.PressureDecision.MessageCount || state.SystemFingerprint != meta.PressureDecision.SystemFingerprint || state.ToolsFingerprint != meta.PressureDecision.ToolsFingerprint {
		t.Fatalf("JSON 持久状态=%+v, want actual=93252 original_count=%d fingerprints=%q/%q", state, meta.PressureDecision.MessageCount, meta.PressureDecision.SystemFingerprint, meta.PressureDecision.ToolsFingerprint)
	}
	if strings.Contains(recorder.Body.String(), `"input_tokens":196`) || !strings.Contains(recorder.Body.String(), `"input_tokens":98`) {
		t.Fatalf("客户端 deflation 行为变化: %s", recorder.Body.String())
	}

	restored := NewSawtoothTrigger(time.Hour, 50000, 1000)
	restored.SetLoadFunc(func(key string) (string, bool) {
		return persisted, key == "sawtooth:json-cache"
	})
	restoredBaseline := restored.PressureBaseline("json-cache")
	if got := restored.ShouldTrigger("json-cache", restoredBaseline.ActualTokens); got != TriggerEmergency {
		t.Fatalf("冷启动 trigger=%q, want %q", got, TriggerEmergency)
	}
}

func TestHandleJSONAuxiliaryDoesNotUpdate(t *testing.T) {
	testHandleAuxiliaryDoesNotUpdate(t, false)
}

func TestHandleSSEAuxiliaryDoesNotUpdate(t *testing.T) {
	testHandleAuxiliaryDoesNotUpdate(t, true)
}

func testHandleAuxiliaryDoesNotUpdate(t *testing.T, sse bool) {
	t.Helper()
	for _, tc := range []struct {
		name string
		meta *requestMeta
	}{
		{name: "session title", meta: &requestMeta{RequestKind: requestKindSessionTitle}},
		{name: "subagent", meta: &requestMeta{AgentRole: agentRoleSubagent}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const sessionID = "auxiliary-baseline"
			trigger := NewSawtoothTrigger(time.Hour, 50000, 1000)
			oldSystem := fingerprintTopLevelJSON(json.RawMessage(`"old system"`))
			oldTools := fingerprintTopLevelJSON(json.RawMessage(`[]`))
			trigger.UpdatePressureBaseline(sessionID, 777, 9, oldSystem, oldTools)
			before := trigger.PressureBaseline(sessionID)
			persistCalls := 0
			trigger.SetPersistFunc(func(_ string, _ string) { persistCalls++ })
			s := NewServer(Config{Proxy: ProxyConfig{Deflation: 0.5}})
			s.Sawtooth = trigger
			meta := newRequestMeta(10, sessionID)
			meta.RequestKind = tc.meta.RequestKind
			meta.AgentRole = tc.meta.AgentRole
			meta.PressureDecision = pressureDecision{
				Available:         true,
				MessageCount:      20,
				SystemFingerprint: fingerprintTopLevelJSON(json.RawMessage(`"new system"`)),
				ToolsFingerprint:  fingerprintTopLevelJSON(json.RawMessage(`[{"name":"new"}]`)),
			}
			recorder := httptest.NewRecorder()
			if sse {
				resp := &http.Response{
					StatusCode: http.StatusOK,
					Header:     http.Header{"Content-Type": {"text/event-stream"}},
					Body: io.NopCloser(strings.NewReader("event: message_start\n" +
						"data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":196,\"cache_read_input_tokens\":93056}}}\n\n")),
				}
				s.handleSSE(recorder, resp, meta, time.Now(), "model", 2)
			} else {
				resp := &http.Response{
					StatusCode: http.StatusOK,
					Header:     http.Header{"Content-Type": {"application/json"}},
					Body:       io.NopCloser(strings.NewReader(`{"usage":{"input_tokens":196,"cache_read_input_tokens":93056}}`)),
				}
				s.handleJSON(recorder, resp, meta, time.Now(), "model", 2)
			}
			if persistCalls != 0 {
				t.Fatalf("auxiliary persisted baseline %d times", persistCalls)
			}
			if after := trigger.PressureBaseline(sessionID); after != before {
				t.Fatalf("auxiliary changed baseline\nbefore=%+v\nafter=%+v", before, after)
			}
			if strings.Contains(recorder.Body.String(), `"input_tokens":196`) || !strings.Contains(recorder.Body.String(), `"input_tokens":98`) {
				t.Fatalf("auxiliary deflation changed: %s", recorder.Body.String())
			}
		})
	}
}

func TestHandleJSONFailureDoesNotUpdate(t *testing.T) {
	testHandleFailureDoesNotUpdate(t, false)
	t.Run("upstream transport", func(t *testing.T) {
		const sessionID = "upstream-failure-baseline"
		trigger := NewSawtoothTrigger(time.Hour, 50000, 1000)
		fingerprint := fingerprintTopLevelJSON(nil)
		trigger.UpdatePressureBaseline(sessionID, 777, 9, fingerprint, fingerprint)
		before := trigger.PressureBaseline(sessionID)
		persistCalls := 0
		trigger.SetPersistFunc(func(_ string, _ string) { persistCalls++ })
		s := NewServer(Config{Proxy: ProxyConfig{Target: "https://upstream.invalid", Deflation: 0.5}})
		s.Sawtooth = trigger
		s.HTTPClient = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return nil, io.ErrUnexpectedEOF
		})}
		meta := newRequestMeta(21, sessionID)
		meta.PressureDecision = pressureDecision{Available: true, MessageCount: 10, SystemFingerprint: fingerprint, ToolsFingerprint: fingerprint}
		req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"model":"test","messages":[]}`))
		recorder := httptest.NewRecorder()
		s.forwardRaw(recorder, req, meta)
		if persistCalls != 0 {
			t.Fatalf("upstream failure persisted baseline %d times", persistCalls)
		}
		if after := trigger.PressureBaseline(sessionID); after != before {
			t.Fatalf("upstream failure changed baseline\nbefore=%+v\nafter=%+v", before, after)
		}
	})
}

func TestHandleSSEFailureDoesNotUpdate(t *testing.T) {
	testHandleFailureDoesNotUpdate(t, true)
}

func testHandleFailureDoesNotUpdate(t *testing.T, sse bool) {
	t.Helper()
	cases := []struct {
		name       string
		statusCode int
		body       string
	}{
		{name: "non-2xx", statusCode: http.StatusBadGateway, body: `{"usage":{"input_tokens":99999}}`},
		{name: "parse failure", statusCode: http.StatusOK, body: `{not-json`},
		{name: "no usage", statusCode: http.StatusOK, body: `{"type":"message"}`},
		{name: "empty usage", statusCode: http.StatusOK, body: `{"usage":{}}`},
	}
	if sse {
		cases = []struct {
			name       string
			statusCode int
			body       string
		}{
			{name: "parse failure", statusCode: http.StatusOK, body: "event: message_start\ndata: {not-json\n\n"},
			{name: "no message start", statusCode: http.StatusOK, body: "event: message_delta\ndata: {\"type\":\"message_delta\",\"usage\":{\"input_tokens\":99999}}\n\n"},
			{name: "message start without usage", statusCode: http.StatusOK, body: "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{}}\n\n"},
			{name: "empty usage", statusCode: http.StatusOK, body: "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"usage\":{}}}\n\n"},
		}
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			const sessionID = "failure-baseline"
			trigger := NewSawtoothTrigger(time.Hour, 50000, 1000)
			fingerprint := fingerprintTopLevelJSON(nil)
			trigger.UpdatePressureBaseline(sessionID, 777, 9, fingerprint, fingerprint)
			before := trigger.PressureBaseline(sessionID)
			persistCalls := 0
			trigger.SetPersistFunc(func(_ string, _ string) { persistCalls++ })
			s := NewServer(Config{Proxy: ProxyConfig{Deflation: 0.5}})
			s.Sawtooth = trigger
			meta := newRequestMeta(20, sessionID)
			meta.PressureDecision = pressureDecision{Available: true, MessageCount: 10, SystemFingerprint: fingerprint, ToolsFingerprint: fingerprint}
			resp := &http.Response{
				StatusCode: tc.statusCode,
				Header:     http.Header{"Content-Type": {"application/json"}},
				Body:       io.NopCloser(strings.NewReader(tc.body)),
			}
			recorder := httptest.NewRecorder()
			if sse {
				resp.Header.Set("Content-Type", "text/event-stream")
				s.handleSSE(recorder, resp, meta, time.Now(), "model", 2)
			} else {
				s.handleJSON(recorder, resp, meta, time.Now(), "model", 2)
			}
			if persistCalls != 0 {
				t.Fatalf("failure path persisted baseline %d times", persistCalls)
			}
			if after := trigger.PressureBaseline(sessionID); after != before {
				t.Fatalf("failure path changed baseline\nbefore=%+v\nafter=%+v", before, after)
			}
		})
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
	s.writeDebugFile("session", 1, timestamp, debugBodyStageForwarded, []byte(`{"ok":true}`), headers, "model", 1)

	debugDir, ok := safeDebugSessionDir(dataDir, "session")
	if !ok {
		t.Fatal("合法 session debug 目录校验失败")
	}
	data, err := os.ReadFile(filepath.Join(debugDir, "2026-07-11T120000.000000000-1-forwarded.json"))
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
	if entry.RequestID != 1 || entry.Stage != debugBodyStageForwarded {
		t.Fatalf("debug 条目缺少关联字段: %+v", entry)
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
			s.writeDebugFile(sessionID, 1, time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC), debugBodyStageForwarded, []byte(`{}`), nil, "model", 0)

			debugDir, ok := safeDebugSessionDir(dataDir, sessionID)
			if !ok {
				t.Fatal("哈希后的 session 目录应通过根目录校验")
			}
			root, _ := filepath.Abs(filepath.Join(dataDir, "debug"))
			rel, err := filepath.Rel(root, debugDir)
			if err != nil || strings.HasPrefix(rel, "..") || filepath.IsAbs(rel) {
				t.Fatalf("debug 目录逃逸: root=%s dir=%s rel=%s err=%v", root, debugDir, rel, err)
			}
			if _, err := os.Stat(filepath.Join(debugDir, "2026-07-11T120000.000000000-1-forwarded.json")); err != nil {
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
			s.writeDebugFile("session", requestID, timestamp, debugBodyStageForwarded, []byte(`{"request":true}`), nil, "model", 1)
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

func TestForwardRawClaudeCodeCancelDoesNotWriteGatewayError(t *testing.T) {
	started := make(chan struct{})
	canceled := make(chan struct{})
	s := NewServer(Config{Proxy: ProxyConfig{Target: "https://upstream.invalid"}})
	s.HTTPClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		close(started)
		<-req.Context().Done()
		close(canceled)
		return nil, req.Context().Err()
	})}

	logs := captureForwardLogs(t)
	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"stream":true,"messages":[]}`)).WithContext(ctx)
	w := newCountingResponseWriter()
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.forwardRaw(w, req, newRequestMeta(100, "cancel-session"))
	}()
	<-started
	cancel()

	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("Claude Code 取消未传播到上游")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("取消后 forwardRaw 未结束")
	}
	status, headerCalls, body := w.snapshot()
	if status != 0 || headerCalls != 0 || len(body) != 0 {
		t.Fatalf("下游取消后写入了伪造网关响应: status=%d calls=%d body=%q", status, headerCalls, body)
	}
	assertLogFields(t, logs.String(), "timeout_source=downstream_context", "stream=true", "elapsed_ms=", "phase=")
}

func TestForwardRawHeaderTimeoutClassification(t *testing.T) {
	logs := captureForwardLogs(t)
	for _, tc := range []struct {
		name   string
		stream bool
		source string
	}{
		{name: "stream", stream: true, source: "stream_header_timeout"},
		{name: "non-stream", stream: false, source: "non_stream_header_timeout"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			received := make(chan struct{}, 1)
			release := make(chan struct{})
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				received <- struct{}{}
				<-release
				w.WriteHeader(http.StatusNoContent)
			}))

			cfg := Config{
				Proxy: ProxyConfig{Target: upstream.URL, Deflation: 1},
				Transport: TransportConfig{
					StreamHeaderTimeout:    30 * time.Millisecond,
					NonStreamHeaderTimeout: 30 * time.Millisecond,
				},
			}
			s := NewServer(cfg)
			req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(fmt.Sprintf(`{"stream":%t,"messages":[]}`, tc.stream)))
			w := newCountingResponseWriter()
			done := make(chan struct{})
			go func() {
				defer close(done)
				s.forwardRaw(w, req, newRequestMeta(101, tc.name))
			}()
			<-received
			select {
			case <-done:
			case <-time.After(time.Second):
				close(release)
				upstream.Close()
				t.Fatal("响应头超时未结束请求")
			}
			close(release)
			upstream.Close()

			assertSingleGatewayResponse(t, w, http.StatusGatewayTimeout, "Gateway Timeout")
			assertLogFields(t, logs.String(), "timeout_source="+tc.source, "phase=awaiting_headers")
		})
	}
}

func TestForwardRawHardTimeoutClassification(t *testing.T) {
	logs := captureForwardLogs(t)
	s := NewServer(Config{
		Proxy:     ProxyConfig{Target: "https://upstream.invalid"},
		Transport: TransportConfig{HardTimeout: 35 * time.Millisecond},
	})
	s.HTTPClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		select {
		case <-req.Context().Done():
			return nil, req.Context().Err()
		case <-time.After(200 * time.Millisecond):
			return nil, timeoutError{"fallback timeout"}
		}
	})}

	req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"messages":[]}`))
	w := newCountingResponseWriter()
	s.forwardRaw(w, req, newRequestMeta(102, "hard-timeout"))

	assertSingleGatewayResponse(t, w, http.StatusGatewayTimeout, "Gateway Timeout")
	assertLogFields(t, logs.String(), "timeout_source=proxy_hard_limit", "stream=false")
}

func TestForwardRawResponseIdleTimeoutClassification(t *testing.T) {
	logs := captureForwardLogs(t)
	var body *stagedReadCloser
	s := NewServer(Config{
		Proxy:     ProxyConfig{Target: "https://upstream.invalid", Deflation: 1},
		Transport: TransportConfig{ResponseIdleTimeout: 35 * time.Millisecond},
	})
	s.HTTPClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		body = newStagedReadCloser([]byte(`{"type":"message"`), req.Context(), 200*time.Millisecond)
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": {"application/json"}},
			Body:       body,
		}, nil
	})}

	req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"messages":[]}`))
	w := newCountingResponseWriter()
	s.forwardRaw(w, req, newRequestMeta(103, "idle-timeout"))
	if body != nil {
		_ = body.Close()
	}

	assertSingleGatewayResponse(t, w, http.StatusGatewayTimeout, "Gateway Timeout")
	assertLogFields(t, logs.String(), "timeout_source=response_idle_timeout", "phase=reading_body")
}

func TestForwardRawUnexpectedEOFRemainsBadGateway(t *testing.T) {
	logs := captureForwardLogs(t)
	s := NewServer(Config{Proxy: ProxyConfig{Target: "https://upstream.invalid"}})
	s.HTTPClient = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, io.ErrUnexpectedEOF
	})}

	req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"messages":[]}`))
	w := newCountingResponseWriter()
	s.forwardRaw(w, req, newRequestMeta(104, "unexpected-eof"))

	assertSingleGatewayResponse(t, w, http.StatusBadGateway, "Bad Gateway")
	assertLogFields(t, logs.String(), "timeout_source=upstream_transport")
	if strings.Contains(logs.String(), "proxy_hard_limit") || strings.Contains(logs.String(), "header_timeout") {
		t.Fatalf("unexpected EOF 被误判为代理超时:\n%s", logs.String())
	}
}

func TestForwardRawDoesNotRetryAmbiguousPost(t *testing.T) {
	var calls atomic.Int32
	s := NewServer(Config{Proxy: ProxyConfig{Target: "https://upstream.invalid"}})
	s.HTTPClient = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		return nil, io.ErrUnexpectedEOF
	})}

	req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"messages":[]}`))
	w := newCountingResponseWriter()
	s.forwardRaw(w, req, newRequestMeta(105, "single-post"))

	if got := calls.Load(); got != 1 {
		t.Fatalf("含糊 POST 调用次数=%d, want 1", got)
	}
	assertSingleGatewayResponse(t, w, http.StatusBadGateway, "Bad Gateway")
	if got := bytes.Count(w.bodyBytes(), []byte(`"error"`)); got != 1 {
		t.Fatalf("错误 JSON 数量=%d, body=%s", got, w.bodyBytes())
	}
}

func TestForwardRawConnectOrTLSFailureRemainsBadGateway(t *testing.T) {
	for _, tc := range []struct {
		name  string
		trace func(*httptrace.ClientTrace)
	}{
		{name: "connect", trace: func(trace *httptrace.ClientTrace) { trace.GetConn("upstream.invalid") }},
		{name: "tls", trace: func(trace *httptrace.ClientTrace) { trace.TLSHandshakeStart() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logs := captureForwardLogs(t)
			s := NewServer(Config{Proxy: ProxyConfig{Target: "https://upstream.invalid"}})
			s.HTTPClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				trace := httptrace.ContextClientTrace(req.Context())
				if trace == nil {
					t.Fatal("请求 context 缺少 httptrace")
				}
				tc.trace(trace)
				return nil, timeoutError{tc.name + " timeout"}
			})}

			req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"messages":[]}`))
			w := newCountingResponseWriter()
			s.forwardRaw(w, req, newRequestMeta(106, tc.name))

			assertSingleGatewayResponse(t, w, http.StatusBadGateway, "Bad Gateway")
			assertLogFields(t, logs.String(), "phase="+map[string]string{"connect": "connect", "tls": "tls_handshake"}[tc.name], "timeout_source=upstream_transport")
		})
	}
}

func TestForwardRawFailureLogSensitiveBoundary(t *testing.T) {
	logs := captureForwardLogs(t)
	const (
		userinfoSecret = "userinfo-secret"
		authSecret     = "authorization-secret"
		apiKeySecret   = "api-key-secret"
		bodySecret     = "request-body-secret"
	)
	s := NewServer(Config{Proxy: ProxyConfig{Target: "https://ordinary-target.invalid"}})
	s.HTTPClient = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, fmt.Errorf("transport wrapper: %w", &url.Error{
			Op:  "Post",
			URL: "https://" + userinfoSecret + ":password@diagnostic-host.invalid/diagnostic-path?diagnostic-query=1",
			Err: io.ErrUnexpectedEOF,
		})
	})}

	req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"messages":[],"sentinel":"`+bodySecret+`"}`))
	req.Header.Set("Authorization", "Bearer "+authSecret)
	req.Header.Set("X-Api-Key", apiKeySecret)
	w := newCountingResponseWriter()
	s.forwardRaw(w, req, newRequestMeta(107, "safe-log"))

	output := logs.String()
	assertLogFields(t, output, "diagnostic-host.invalid", "/diagnostic-path", "diagnostic-query=1")
	for _, secret := range []string{userinfoSecret, "password", authSecret, apiKeySecret, bodySecret} {
		if strings.Contains(output, secret) {
			t.Fatalf("失败日志泄漏 %q:\n%s", secret, output)
		}
	}
}

func TestForwardRawStripsConnectionAndSetsContentLength(t *testing.T) {
	const requestBody = `{"model":"test","messages":[{"role":"user","content":"完整正文"}]}`
	var seenConnection string
	var seenHeaderLength string
	var seenContentLength int64
	var seenAuthorization string
	var seenBody []byte

	s := NewServer(Config{Proxy: ProxyConfig{Target: "https://upstream.invalid", Deflation: 1}})
	s.HTTPClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		seenConnection = req.Header.Get("Connection")
		seenHeaderLength = req.Header.Get("Content-Length")
		seenContentLength = req.ContentLength
		seenAuthorization = req.Header.Get("Authorization")
		seenBody, _ = io.ReadAll(req.Body)
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": {"application/json"}},
			Body:       io.NopCloser(strings.NewReader(`{"type":"message","usage":{"input_tokens":1,"output_tokens":1}}`)),
		}, nil
	})}

	req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(requestBody))
	req.Header.Set("Connection", "keep-alive")
	req.Header.Set("Content-Length", "999999")
	req.Header.Set("Authorization", "Bearer preserved")
	w := newCountingResponseWriter()
	s.forwardRaw(w, req, newRequestMeta(108, "wire"))

	if seenConnection != "" || seenHeaderLength != "" {
		t.Fatalf("逐跳/旧长度 header 未清理: Connection=%q Content-Length=%q", seenConnection, seenHeaderLength)
	}
	if seenContentLength != int64(len(requestBody)) || string(seenBody) != requestBody {
		t.Fatalf("上游 body 长度不一致: ContentLength=%d len=%d body=%q", seenContentLength, len(seenBody), seenBody)
	}
	if seenAuthorization != "Bearer preserved" {
		t.Fatalf("必要认证 header 未透传: %q", seenAuthorization)
	}
}

func TestForwardRawNon2xxBodyTimeoutBeforeCommit(t *testing.T) {
	for _, tc := range []struct {
		name       string
		status     int
		transport  TransportConfig
		wantSource string
	}{
		{name: "idle", status: http.StatusTooManyRequests, transport: TransportConfig{ResponseIdleTimeout: 35 * time.Millisecond}, wantSource: "response_idle_timeout"},
		{name: "hard", status: http.StatusServiceUnavailable, transport: TransportConfig{HardTimeout: 35 * time.Millisecond}, wantSource: "proxy_hard_limit"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logs := captureForwardLogs(t)
			var body *stagedReadCloser
			s := NewServer(Config{Proxy: ProxyConfig{Target: "https://upstream.invalid"}, Transport: tc.transport})
			s.HTTPClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				body = newStagedReadCloser([]byte("partial-upstream-body"), req.Context(), 200*time.Millisecond)
				return &http.Response{StatusCode: tc.status, Header: http.Header{"Content-Type": {"application/json"}}, Body: body}, nil
			})}

			req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"messages":[]}`))
			w := newCountingResponseWriter()
			s.forwardRaw(w, req, newRequestMeta(109, tc.name))
			if body != nil {
				_ = body.Close()
			}

			assertSingleGatewayResponse(t, w, http.StatusGatewayTimeout, "Gateway Timeout")
			if bytes.Contains(w.bodyBytes(), []byte("partial-upstream-body")) {
				t.Fatalf("504 混入上游部分正文: %s", w.bodyBytes())
			}
			assertLogFields(t, logs.String(), "timeout_source="+tc.wantSource, "phase=reading_body")
		})
	}
}

func TestForwardRawLongSSEProgressOutlivesLegacyLimit(t *testing.T) {
	const legacyBoundary = 50 * time.Millisecond
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flusher := w.(http.Flusher)
		for index := 0; index < 6; index++ {
			_, _ = fmt.Fprintf(w, "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":%d}\n\n", index)
			flusher.Flush()
			time.Sleep(15 * time.Millisecond)
		}
	}))
	defer upstream.Close()

	s := NewServer(Config{
		Proxy: ProxyConfig{Target: upstream.URL, Deflation: 1},
		Transport: TransportConfig{
			StreamHeaderTimeout: 200 * time.Millisecond,
			ResponseIdleTimeout: 100 * time.Millisecond,
			HardTimeout:         time.Second,
		},
	})
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"stream":true,"messages":[]}`))
	w := newCountingResponseWriter()
	started := time.Now()
	s.forwardRaw(w, req, newRequestMeta(110, "long-sse"))
	elapsed := time.Since(started)

	status, _, body := w.snapshot()
	if status != http.StatusOK || bytes.Contains(body, []byte("Gateway")) {
		t.Fatalf("长 SSE 被错误截断: status=%d body=%s", status, body)
	}
	if elapsed <= legacyBoundary {
		t.Fatalf("测试总时长=%s，未超过概念旧界限=%s", elapsed, legacyBoundary)
	}
	if got := bytes.Count(body, []byte("content_block_delta")); got != 12 {
		t.Fatalf("SSE 事件未完整到达: marker count=%d body=%s", got, body)
	}
}

func TestForwardRawSSETimeoutAfterCommitTerminatesWithoutForgedJSON(t *testing.T) {
	for _, tc := range []struct {
		name       string
		transport  TransportConfig
		wantSource string
	}{
		{name: "idle", transport: TransportConfig{ResponseIdleTimeout: 40 * time.Millisecond}, wantSource: "response_idle_timeout"},
		{name: "hard", transport: TransportConfig{HardTimeout: 40 * time.Millisecond}, wantSource: "proxy_hard_limit"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logs := captureForwardLogs(t)
			var body *stagedReadCloser
			s := NewServer(Config{Proxy: ProxyConfig{Target: "https://upstream.invalid", Deflation: 1}, Transport: tc.transport})
			s.HTTPClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				body = newStagedReadCloser([]byte("event: content_block_delta\ndata: {\"type\":\"content_block_delta\"}\n\n"), req.Context(), 200*time.Millisecond)
				return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: body}, nil
			})}

			req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"stream":true,"messages":[]}`))
			w := newCountingResponseWriter()
			done := make(chan struct{})
			go func() {
				defer close(done)
				s.forwardRaw(w, req, newRequestMeta(111, tc.name))
			}()

			select {
			case <-w.written:
			case <-time.After(time.Second):
				if body != nil {
					_ = body.Close()
				}
				t.Fatal("首个 SSE 事件未提交")
			}
			select {
			case <-done:
			case <-time.After(time.Second):
				if body != nil {
					_ = body.Close()
				}
				<-done
				t.Fatal("SSE timeout 后 handler 未结束")
			}

			status, headerCalls, responseBody := w.snapshot()
			if status != http.StatusOK || headerCalls != 1 {
				t.Fatalf("已提交 SSE 状态被改写: status=%d calls=%d", status, headerCalls)
			}
			if bytes.Contains(responseBody, []byte("Bad Gateway")) || bytes.Contains(responseBody, []byte("Gateway Timeout")) {
				t.Fatalf("已提交 SSE 被追加伪造 JSON: %s", responseBody)
			}
			assertLogFields(t, logs.String(), "timeout_source="+tc.wantSource, "response_committed=true", "stream=true")
		})
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return fn(req)
}

type timeoutError struct{ message string }

func (err timeoutError) Error() string    { return err.message }
func (timeoutError) Timeout() bool        { return true }
func (timeoutError) Temporary() bool      { return true }
func (timeoutError) Unwrap() error        { return nil }
func (timeoutError) Network() net.Error   { return timeoutError{} }
func (timeoutError) Is(target error) bool { return false }

type countingResponseWriter struct {
	mu          sync.Mutex
	header      http.Header
	status      int
	headerCalls int
	body        bytes.Buffer
	written     chan struct{}
	writtenOnce sync.Once
}

func newCountingResponseWriter() *countingResponseWriter {
	return &countingResponseWriter{header: make(http.Header), written: make(chan struct{})}
}

func (w *countingResponseWriter) Header() http.Header {
	return w.header
}

func (w *countingResponseWriter) WriteHeader(status int) {
	w.mu.Lock()
	w.headerCalls++
	if w.status == 0 {
		w.status = status
	}
	w.mu.Unlock()
	w.signalWritten()
}

func (w *countingResponseWriter) Write(data []byte) (int, error) {
	w.mu.Lock()
	if w.status == 0 {
		w.status = http.StatusOK
		w.headerCalls++
	}
	n, err := w.body.Write(data)
	w.mu.Unlock()
	w.signalWritten()
	return n, err
}

func (w *countingResponseWriter) Flush() {}

func (w *countingResponseWriter) signalWritten() {
	w.writtenOnce.Do(func() { close(w.written) })
}

func (w *countingResponseWriter) snapshot() (int, int, []byte) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.status, w.headerCalls, append([]byte(nil), w.body.Bytes()...)
}

func (w *countingResponseWriter) bodyBytes() []byte {
	_, _, body := w.snapshot()
	return body
}

type stagedReadCloser struct {
	mu       sync.Mutex
	first    []byte
	ctx      context.Context
	fallback time.Duration
	closed   chan struct{}
	close    sync.Once
}

func newStagedReadCloser(first []byte, ctx context.Context, fallback time.Duration) *stagedReadCloser {
	return &stagedReadCloser{first: append([]byte(nil), first...), ctx: ctx, fallback: fallback, closed: make(chan struct{})}
}

func (body *stagedReadCloser) Read(buffer []byte) (int, error) {
	body.mu.Lock()
	if len(body.first) > 0 {
		n := copy(buffer, body.first)
		body.first = body.first[n:]
		body.mu.Unlock()
		return n, nil
	}
	body.mu.Unlock()

	timer := time.NewTimer(body.fallback)
	defer timer.Stop()
	select {
	case <-body.closed:
		return 0, io.ErrClosedPipe
	case <-body.ctx.Done():
		return 0, body.ctx.Err()
	case <-timer.C:
		return 0, timeoutError{"staged body fallback timeout"}
	}
}

func (body *stagedReadCloser) Close() error {
	body.close.Do(func() { close(body.closed) })
	return nil
}

func captureForwardLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(NewLogHandler(&logs, slog.LevelDebug)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return &logs
}

func assertSingleGatewayResponse(t *testing.T, w *countingResponseWriter, wantStatus int, wantBody string) {
	t.Helper()
	status, headerCalls, body := w.snapshot()
	if status != wantStatus || headerCalls != 1 || !bytes.Contains(body, []byte(wantBody)) {
		t.Fatalf("网关响应 = status=%d calls=%d body=%q, want status=%d single body containing %q", status, headerCalls, body, wantStatus, wantBody)
	}
}

func assertLogFields(t *testing.T, output string, fields ...string) {
	t.Helper()
	for _, field := range fields {
		if !strings.Contains(output, field) {
			t.Fatalf("日志缺少 %q:\n%s", field, output)
		}
	}
}

package proxy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestHandleMessagesCollapseFreezeLifecycle(t *testing.T) {
	var forwarded []Message
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Messages []Message `json:"messages"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode upstream request: %v", err)
		}
		forwarded = deepCopyMessages(body.Messages)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"usage":{"input_tokens":100,"output_tokens":1}}`))
	}))
	defer upstream.Close()

	server := newPipelineTestServer(t, upstream.URL)
	raw := pipelineMessages(300, 80)
	servePipelineRequest(t, server, "thread-freeze", raw)

	if len(forwarded) >= len(raw) {
		t.Fatalf("forwarded message count = %d, want shorter than raw count %d", len(forwarded), len(raw))
	}
	if got := countBreakpoints(forwarded); got != 1 {
		t.Fatalf("freeze request breakpoint count = %d, want 1", got)
	}

	result := server.Frozen.Get("thread-freeze", raw)
	if result == nil {
		t.Fatal("expected frozen result to validate against the raw request boundary")
	}
	if result.Cutoff != len(raw) {
		t.Fatalf("raw cutoff = %d, want %d", result.Cutoff, len(raw))
	}
	if got := server.Frozen.LengthFor("thread-freeze"); got != len(forwarded) {
		t.Fatalf("frozen prefix length = %d, want forwarded prefix length %d", got, len(forwarded))
	}
	gotBytes, err := json.Marshal(result.Messages)
	if err != nil {
		t.Fatalf("marshal stored frozen prefix: %v", err)
	}
	wantBytes, err := json.Marshal(forwarded)
	if err != nil {
		t.Fatalf("marshal forwarded frozen prefix: %v", err)
	}
	if !bytes.Equal(gotBytes, wantBytes) {
		t.Fatalf("stored and forwarded frozen prefix bytes differ\nstored:    %s\nforwarded: %s", gotBytes, wantBytes)
	}
}

func newPipelineTestServer(t *testing.T, upstreamURL string) *Server {
	t.Helper()
	cfg := DefaultConfig()
	cfg.Proxy.Target = upstreamURL
	cfg.Proxy.Deflation = 1
	cfg.Stubify.TokenThreshold = 12000
	cfg.Stubify.KeepRecent = 8
	cfg.Debug.Enabled = false

	tokenCounter, err := NewTokenCounter()
	if err != nil {
		t.Fatalf("NewTokenCounter: %v", err)
	}
	store, err := NewSQLiteStore(filepath.Join(t.TempDir(), "pipeline.db"))
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	server := NewServer(cfg)
	server.TokenCounter = tokenCounter
	server.DecayTracker = NewDecayTracker()
	server.Store = store
	server.Frozen = NewFrozenStubs()
	server.Sawtooth = NewSawtoothTrigger(time.Minute, cfg.Stubify.TokenThreshold, cfg.Stubify.TokenThreshold/2)
	server.EagerStub = NewEagerStubMemory()
	return server
}

func servePipelineRequest(t *testing.T, server *Server, sessionID string, messages []Message) {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"model":    "deepseek-v4-pro",
		"thinking": map[string]any{"type": "enabled"},
		"messages": messages,
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Claude-Code-Session-Id", sessionID)
	recorder := httptest.NewRecorder()
	server.HandleMessages(recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("HandleMessages status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
}

func pipelineMessages(count, words int) []Message {
	messages := make([]Message, count)
	for i := range messages {
		role := "user"
		if i%2 == 1 {
			role = "assistant"
		}
		text := fmt.Sprintf("pipeline-message-%03d %s", i, strings.Repeat("context ", words))
		messages[i] = Message{Role: role, Content: mustMarshal(text)}
	}
	return messages
}

package proxy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestRequestMetaConcurrentIDsUnique(t *testing.T) {
	server := NewServer(DefaultConfig())
	const requestCount = 64
	ids := make(chan uint64, requestCount)
	var wg sync.WaitGroup
	for range requestCount {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ids <- server.nextRequestMeta("session").ID
		}()
	}
	wg.Wait()
	close(ids)

	seen := make(map[uint64]bool, requestCount)
	for id := range ids {
		if id == 0 || seen[id] {
			t.Fatalf("request_id 非法或重复: %d", id)
		}
		seen[id] = true
	}
	if len(seen) != requestCount {
		t.Fatalf("唯一 request_id 数=%d，期望 %d", len(seen), requestCount)
	}
}

func TestConcurrentRequestLogsReconstructable(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"usage":{"input_tokens":100,"output_tokens":1}}`))
	}))
	defer upstream.Close()

	server := newPipelineTestServer(t, upstream.URL)
	seedRecallArchive(t, server.Store)
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(NewLogHandler(&logs, slog.LevelDebug)))
	t.Cleanup(func() { slog.SetDefault(previous) })

	start := make(chan struct{})
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for _, sessionID := range []string{"concurrent-a", "concurrent-b"} {
		sessionID := sessionID
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			body, err := json.Marshal(map[string]any{
				"model":    "deepseek-v4-pro",
				"thinking": map[string]any{"type": "enabled"},
				"messages": []Message{{Role: "user", Content: mustMarshal("restore archive about flimflam details parser")}},
			})
			if err != nil {
				errs <- err
				return
			}
			req := httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-Claude-Code-Session-Id", sessionID)
			recorder := httptest.NewRecorder()
			server.HandleMessages(recorder, req)
			if recorder.Code != http.StatusOK {
				errs <- fmt.Errorf("%s status=%d body=%s", sessionID, recorder.Code, recorder.Body.String())
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}

	lines := strings.Split(strings.TrimSpace(logs.String()), "\n")
	requestIDs := make(map[string]string)
	for _, line := range lines {
		if !strings.Contains(line, "请求进入") {
			continue
		}
		for _, sessionID := range []string{"concurrent-a", "concurrent-b"} {
			if !strings.Contains(line, "request_session_id="+sessionID) {
				continue
			}
			for _, field := range strings.Fields(line) {
				if strings.HasPrefix(field, "request_id=") {
					requestIDs[sessionID] = strings.TrimPrefix(field, "request_id=")
				}
			}
		}
	}
	if len(requestIDs) != 2 || requestIDs["concurrent-a"] == requestIDs["concurrent-b"] {
		t.Fatalf("无法从入口日志还原唯一 request_id: %v\n%s", requestIDs, logs.String())
	}
	for sessionID, requestID := range requestIDs {
		var chain strings.Builder
		for _, line := range lines {
			if strings.Contains(line, "request_id="+requestID+" ") || strings.HasSuffix(line, "request_id="+requestID) {
				chain.WriteString(line)
				chain.WriteByte('\n')
			}
		}
		got := chain.String()
		for _, event := range []string{"请求进入", "Archive 召回汇总", "上游请求发送"} {
			if !strings.Contains(got, event) {
				t.Fatalf("%s(request_id=%s) 缺少 %s:\n%s", sessionID, requestID, event, got)
			}
		}
		if !strings.Contains(got, "request_session_id="+sessionID) {
			t.Fatalf("request_id=%s 混入其他 session:\n%s", requestID, got)
		}
	}
}

func TestHandleMessagesSubagentNoSideEffects(t *testing.T) {
	testHandleMessagesDirectAgentBypass(t, "subagent", `"deepseek-v4-pro"`, `{"type":"enabled"}`, `[{"type":"text","text":"cc_entrypoint=sdk-ts"}]`)
}

func TestHandleMessagesAgentUnknownNoSideEffects(t *testing.T) {
	testHandleMessagesDirectAgentBypass(t, "unknown", `"unverified-model"`, `{"type":"enabled"}`, `[{"type":"text","text":"ordinary system"}]`)
}

func testHandleMessagesDirectAgentBypass(t *testing.T, name, model, thinking, system string) {
	t.Helper()
	var forwardedBodies [][]byte
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read upstream request: %v", err)
		}
		forwardedBodies = append(forwardedBodies, body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"usage":{"input_tokens":100,"output_tokens":1}}`))
	}))
	defer upstream.Close()

	server := newPipelineTestServer(t, upstream.URL)
	searchCalls := 0
	server.searchAndExpandFn = func(messages []Message, _ *SQLiteStore, _ int, _ *TokenCounter, _ *Budget, _ *requestMeta) RecallOutcome {
		searchCalls++
		return RecallOutcome{Messages: messages}
	}
	messagesJSON, err := json.Marshal(pipelineMessages(300, 80))
	if err != nil {
		t.Fatalf("marshal messages: %v", err)
	}
	body := []byte(fmt.Sprintf("{ \"model\" : %s, \"thinking\" : %s, \"system\" : %s, \"messages\" : %s }", model, thinking, system, messagesJSON))
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Claude-Code-Session-Id", "thread-agent-"+name)
	recorder := httptest.NewRecorder()
	server.HandleMessages(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("HandleMessages status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	if searchCalls != 0 {
		t.Fatalf("SearchAndExpand calls = %d, want 0", searchCalls)
	}
	if got := archiveCount(t, server.Store); got != 0 {
		t.Fatalf("archive rows = %d, want 0", got)
	}
	if len(forwardedBodies) != 1 {
		t.Fatalf("forward calls = %d, want 1", len(forwardedBodies))
	}
	if !bytes.Equal(forwardedBodies[0], body) {
		t.Fatalf("direct-forward body changed\ngot:  %s\nwant: %s", forwardedBodies[0], body)
	}
}

func TestHandleMessagesParentFrozenRequiresExplicitRelation(t *testing.T) {
	const (
		childID  = "11111111-1111-4111-8111-111111111111"
		parentID = "22222222-2222-4222-8222-222222222222"
	)
	tests := []struct {
		name          string
		parentHeader  string
		wantFirstText string
		wantLog       string
	}{
		{name: "explicit parent", parentHeader: parentID, wantFirstText: "parent frozen"},
		{name: "parent unavailable", wantFirstText: "raw child", wantLog: "parent_frozen_unavailable"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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
			raw := []Message{
				{Role: "user", Content: mustMarshal("raw child")},
				{Role: "assistant", Content: mustMarshal("fresh tail")},
			}
			parentPrefix := []Message{{Role: "user", Content: mustMarshal("parent frozen")}}
			server.Frozen.Store(parentID, parentPrefix, 1, raw[0], 10, 20)
			childPrefix := []Message{{Role: "user", Content: mustMarshal("child frozen must not be used")}}
			server.Frozen.Store(childID, childPrefix, 1, raw[0], 10, 20)

			var logs bytes.Buffer
			previous := slog.Default()
			slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
			t.Cleanup(func() { slog.SetDefault(previous) })

			body, err := json.Marshal(map[string]any{
				"model":    "deepseek-v4-pro",
				"thinking": map[string]any{"type": "enabled"},
				"system":   []map[string]string{{"type": "text", "text": "cc_entrypoint=sdk-ts"}},
				"messages": raw,
			})
			if err != nil {
				t.Fatalf("marshal request: %v", err)
			}
			req := httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-Claude-Code-Session-Id", childID)
			if tt.parentHeader != "" {
				req.Header.Set(parentSessionHeader, tt.parentHeader)
			}
			recorder := httptest.NewRecorder()
			server.HandleMessages(recorder, req)
			if recorder.Code != http.StatusOK {
				t.Fatalf("HandleMessages status = %d, body = %s", recorder.Code, recorder.Body.String())
			}
			if len(forwarded) != 2 {
				t.Fatalf("forwarded message count = %d, want 2", len(forwarded))
			}
			var firstText string
			if err := json.Unmarshal(forwarded[0].Content, &firstText); err != nil {
				t.Fatalf("decode first content: %v", err)
			}
			if firstText != tt.wantFirstText {
				t.Fatalf("first message = %q, want %q", firstText, tt.wantFirstText)
			}
			if tt.wantLog != "" && !strings.Contains(logs.String(), tt.wantLog) {
				t.Fatalf("logs missing %q: %s", tt.wantLog, logs.String())
			}
		})
	}
}

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

func TestHandleMessagesCollapseThenRestore(t *testing.T) {
	var forwarded [][]Message
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Messages []Message `json:"messages"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode upstream request: %v", err)
		}
		forwarded = append(forwarded, deepCopyMessages(body.Messages))
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"usage":{"input_tokens":100,"output_tokens":1}}`))
	}))
	defer upstream.Close()

	server := newPipelineTestServer(t, upstream.URL)
	raw := pipelineMessages(300, 80)
	servePipelineRequest(t, server, "thread-restore", raw)
	archivesAfterFreeze := archiveCount(t, server.Store)

	tail := pipelineMessages(2, 10)
	tail[0].Content = mustMarshal("fresh-tail-0")
	tail[1].Content = mustMarshal("fresh-tail-1")
	secondRaw := append(deepCopyMessages(raw), tail...)
	servePipelineRequest(t, server, "thread-restore", secondRaw)

	if len(forwarded) != 2 {
		t.Fatalf("upstream request count = %d, want 2", len(forwarded))
	}
	if got, want := len(forwarded[1]), len(forwarded[0])+len(tail); got != want {
		t.Fatalf("restored message count = %d, want frozen prefix %d + tail %d = %d", got, len(forwarded[0]), len(tail), want)
	}
	for i := range tail {
		got, err := json.Marshal(forwarded[1][len(forwarded[0])+i])
		if err != nil {
			t.Fatalf("marshal forwarded tail %d: %v", i, err)
		}
		want, err := json.Marshal(tail[i])
		if err != nil {
			t.Fatalf("marshal expected tail %d: %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("fresh tail %d changed\ngot:  %s\nwant: %s", i, got, want)
		}
	}
	if got := archiveCount(t, server.Store); got != archivesAfterFreeze {
		t.Fatalf("archive rows after restore = %d, want unchanged %d", got, archivesAfterFreeze)
	}
}

func TestHandleMessagesFrozenBoundaryEdit(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"usage":{"input_tokens":100,"output_tokens":1}}`))
	}))
	defer upstream.Close()

	server := newPipelineTestServer(t, upstream.URL)
	raw := pipelineMessages(300, 80)
	servePipelineRequest(t, server, "thread-boundary-edit", raw)
	archivesAfterFreeze := archiveCount(t, server.Store)

	edited := deepCopyMessages(raw)
	edited[299].Content = mustMarshal("edited raw boundary")
	edited = append(edited, pipelineMessages(2, 10)...)
	servePipelineRequest(t, server, "thread-boundary-edit", edited)

	if got := archiveCount(t, server.Store); got < archivesAfterFreeze {
		t.Fatalf("archive rows after edited boundary = %d, want at least %d", got, archivesAfterFreeze)
	}
	server.Frozen.mu.RLock()
	refreshedCutoff := server.Frozen.cutoff["thread-boundary-edit"]
	server.Frozen.mu.RUnlock()
	if refreshedCutoff != len(edited) {
		t.Fatalf("refreshed cutoff=%d, want %d", refreshedCutoff, len(edited))
	}
}

func TestHandleMessagesSearchOnceAcrossFrozenPaths(t *testing.T) {
	tests := []struct {
		name        string
		setupFrozen func(t *testing.T, server *Server, raw []Message)
	}{
		{name: "no frozen"},
		{
			name: "valid frozen",
			setupFrozen: func(t *testing.T, server *Server, raw []Message) {
				t.Helper()
				stripped := StripReminders(raw)
				prefix := deepCopyMessages(stripped[:1])
				server.Frozen.Store("thread-search-once", prefix, 1, stripped[0], server.TokenCounter.CountMessagesTokens(prefix), server.TokenCounter.CountMessagesTokens(raw))
			},
		},
		{
			name: "invalidated frozen",
			setupFrozen: func(t *testing.T, server *Server, raw []Message) {
				t.Helper()
				stripped := StripReminders(raw)
				prefix := []Message{{Role: "user", Content: mustMarshal(strings.Repeat("oversized frozen context ", 20000))}}
				server.Frozen.Store("thread-search-once", prefix, 1, stripped[0], server.TokenCounter.CountMessagesTokens(prefix), server.TokenCounter.CountMessagesTokens(raw))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
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
			seedRecallArchive(t, server.Store)
			raw := pipelineMessages(3, 10)
			raw[2].Content = mustMarshal("restore archive about flimflam details parser")
			if tc.setupFrozen != nil {
				tc.setupFrozen(t, server, raw)
			}

			searchCalls := 0
			var outcomes []RecallOutcome
			server.searchAndExpandFn = func(messages []Message, store *SQLiteStore, threshold int, counter *TokenCounter, budget *Budget, meta *requestMeta) RecallOutcome {
				searchCalls++
				outcome := searchAndExpandWithMeta(messages, store, threshold, counter, budget, meta)
				outcomes = append(outcomes, outcome)
				return outcome
			}

			servePipelineRequest(t, server, "thread-search-once", raw)

			if searchCalls != 1 {
				t.Fatalf("SearchAndExpand calls = %d, want 1", searchCalls)
			}
			if len(outcomes) != 1 {
				t.Fatalf("outcome count = %d, want 1", len(outcomes))
			}
			outcome := outcomes[0]
			if outcome.Injected != 1 || outcome.Discarded != 0 {
				t.Fatalf("injected/discarded = %d/%d, want 1/0", outcome.Injected, outcome.Discarded)
			}
			if outcome.TokenCost > outcome.BudgetLimit || outcome.BudgetRemaining < 0 {
				t.Fatalf("budget cost/limit/remaining = %d/%d/%d", outcome.TokenCost, outcome.BudgetLimit, outcome.BudgetRemaining)
			}
			gotArchives := retrievedArchiveTexts(forwarded)
			wantArchives := retrievedArchiveTexts(outcome.Messages)
			if len(gotArchives) != outcome.Injected || len(wantArchives) != outcome.Injected {
				t.Fatalf("forwarded/outcome archive count = %d/%d, want %d", len(gotArchives), len(wantArchives), outcome.Injected)
			}
			for i := range wantArchives {
				if gotArchives[i] != wantArchives[i] {
					t.Fatalf("forwarded archive %d differs from outcome\ngot:  %q\nwant: %q", i, gotArchives[i], wantArchives[i])
				}
			}
		})
	}
}

func seedRecallArchive(t *testing.T, store *SQLiteStore) {
	t.Helper()
	block := ArchiveBlock{
		ID: "pipeline-recall", SessionID: "archive-session",
		BlockRangeStart: 1, BlockRangeEnd: 2,
		MessageCount: 2, EstimatedTokens: 80,
		SummaryText: "flimflam archive details",
		Messages:    []Message{{Role: "user", Content: mustMarshal("pipeline recall content")}},
		Keywords: []KeywordEntry{
			{Word: "flimflam", Source: "user_message"},
			{Word: "archive", Source: "user_message"},
			{Word: "details", Source: "user_message"},
			{Word: "parser", Source: "user_message"},
		},
	}
	if err := store.SaveArchive(block); err != nil {
		t.Fatalf("SaveArchive: %v", err)
	}
}

func retrievedArchiveTexts(messages []Message) []string {
	var archives []string
	for _, message := range messages {
		blocks, _ := parseContent(message.Content)
		for _, block := range blocks {
			if block.Type == "text" && strings.Contains(block.Text, "[Retrieved archive #") {
				archives = append(archives, block.Text)
			}
		}
	}
	return archives
}

func newPipelineTestServer(t *testing.T, upstreamURL string) *Server {
	t.Helper()
	cfg := DefaultConfig()
	cfg.Proxy.Target = upstreamURL
	cfg.Proxy.Deflation = 1
	cfg.Stubify.TokenThreshold = 16000
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

func archiveCount(t *testing.T, store *SQLiteStore) int {
	t.Helper()
	var count int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM archive_blocks`).Scan(&count); err != nil {
		t.Fatalf("count archive rows: %v", err)
	}
	return count
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

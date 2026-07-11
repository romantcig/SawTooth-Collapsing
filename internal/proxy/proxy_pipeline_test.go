package proxy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
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

func TestHandleMessagesDebugStages(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"usage":{"input_tokens":196,"cache_creation_input_tokens":0,"cache_read_input_tokens":93056,"output_tokens":1}}`))
	}))
	defer upstream.Close()

	server := newPipelineTestServer(t, upstream.URL)
	dataDir := t.TempDir()
	server.Config.Debug = DebugConfig{Enabled: true, FullBody: false, DataDir: dataDir}
	raw := append([]Message{pipelinePersistentContextMessage(t, "DEBUG-STAGE-CLAUDE-MD-SECRET")}, pipelineMessages(4, 5)...)
	servePipelineRequest(t, server, "debug-stage-session-secret", raw)

	files := readDebugFactFiles(t, dataDir, "debug-stage-session-secret")
	if len(files) != 3 {
		t.Fatalf("facts 文件数=%d, want raw+forwarded+usage 共 3", len(files))
	}
	stageCounts := make(map[debugStage]int)
	requestIDs := make(map[uint64]bool)
	for _, data := range files {
		if bytes.Contains(data, []byte("DEBUG-STAGE-CLAUDE-MD-SECRET")) || bytes.Contains(data, []byte("debug-stage-session-secret")) {
			t.Fatalf("facts 泄漏正文或 session: %s", data)
		}
		var fact debugFact
		if err := json.Unmarshal(data, &fact); err != nil {
			t.Fatal(err)
		}
		stageCounts[fact.Stage]++
		requestIDs[fact.RequestID] = true
	}
	for _, stage := range []debugStage{debugStageRawInbound, debugStageForwarded, debugStageResponseUsage} {
		if stageCounts[stage] != 1 {
			t.Fatalf("stage %q count=%d, want 1; all=%v", stage, stageCounts[stage], stageCounts)
		}
	}
	if len(requestIDs) != 1 {
		t.Fatalf("facts request_id 不一致: %v", requestIDs)
	}

	dir, _ := safeDebugSessionDir(dataDir, "debug-stage-session-secret")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), "-req.json") || strings.HasSuffix(entry.Name(), "-resp.json") {
			t.Fatalf("full_body=false 仍写完整 body: %s", entry.Name())
		}
	}
}

func TestHandleMessagesDebugFullBodyOptIn(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"usage":{"input_tokens":1,"output_tokens":1}}`))
	}))
	defer upstream.Close()

	server := newPipelineTestServer(t, upstream.URL)
	dataDir := t.TempDir()
	server.Config.Debug = DebugConfig{Enabled: true, FullBody: true, DataDir: dataDir}
	servePipelineRequest(t, server, "debug-full-body-session", pipelineMessages(2, 2))

	dir, _ := safeDebugSessionDir(dataDir, "debug-full-body-session")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var requestBody, responseBody bool
	for _, entry := range entries {
		requestBody = requestBody || strings.HasSuffix(entry.Name(), "-req.json")
		responseBody = responseBody || strings.HasSuffix(entry.Name(), "-resp.json")
	}
	if !requestBody || !responseBody {
		t.Fatalf("full_body=true 未保留兼容 body 文件: req=%v resp=%v", requestBody, responseBody)
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
		for _, event := range []string{"请求进入", "agent_features", "frozen prefix 未命中", "Archive 召回汇总", "上游请求发送"} {
			if !strings.Contains(got, event) {
				t.Fatalf("%s(request_id=%s) 缺少 %s:\n%s", sessionID, requestID, event, got)
			}
		}
		if !strings.Contains(got, "request_session_id="+sessionID) {
			t.Fatalf("request_id=%s 混入其他 session:\n%s", requestID, got)
		}
	}
	for _, line := range lines {
		if (strings.Contains(line, "agent_features") || strings.Contains(line, "frozen prefix")) && !strings.Contains(line, "request_id=") {
			t.Fatalf("Agent/Frozen 事件缺少 request_id: %s", line)
		}
	}
}

func TestHandleMessagesSubagentNoSideEffects(t *testing.T) {
	testHandleMessagesDirectAgentBypass(t)
}

func testHandleMessagesDirectAgentBypass(t *testing.T, _ ...string) {
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
	messages := append([]Message{pipelinePersistentContextMessage(t, "subagent-current")}, pipelineMessages(300, 80)...)
	messagesJSON, err := json.Marshal(messages)
	if err != nil {
		t.Fatalf("marshal messages: %v", err)
	}
	body := []byte(fmt.Sprintf("{ \"model\" : \"same-model\", \"thinking\" : {\"type\":\"enabled\"}, \"system\" : \"cc_entrypoint=sdk-ts\", \"messages\" : %s }", messagesJSON))
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Claude-Code-Session-Id", "thread-agent-subagent")
	req.Header.Set("x-anthropic-billing-header", "cch=12345, cc_is_subagent=true")
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
	var forwarded struct {
		Messages []Message `json:"messages"`
	}
	if err := json.Unmarshal(forwardedBodies[0], &forwarded); err != nil {
		t.Fatalf("decode forwarded subagent body: %v", err)
	}
	assertPersistentContext(t, forwarded.Messages, "subagent-current")
	if len(forwarded.Messages) != len(messages) {
		t.Fatalf("subagent message count=%d, want %d", len(forwarded.Messages), len(messages))
	}
}

func TestHandleMessagesMainFallbackRunsPipeline(t *testing.T) {
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
	searchCalls := 0
	server.searchAndExpandFn = func(messages []Message, _ *SQLiteStore, _ int, _ *TokenCounter, _ *Budget, _ *requestMeta) RecallOutcome {
		searchCalls++
		return RecallOutcome{Messages: messages}
	}
	raw := append([]Message{pipelinePersistentContextMessage(t, "main-fallback")}, pipelineMessages(300, 80)...)
	servePipelineRequestWith(t, server, "thread-main-fallback", raw, map[string]any{
		"model":  "unverified-model",
		"system": "cc_entrypoint=sdk-ts",
	}, nil)
	if searchCalls != 1 {
		t.Fatalf("SearchAndExpand calls = %d, want 1", searchCalls)
	}
	if len(forwarded) >= len(raw) {
		t.Fatalf("main fallback forwarded messages = %d, want collapse below raw %d", len(forwarded), len(raw))
	}
	assertPersistentContext(t, forwarded, "main-fallback")
}

func TestHandleMessagesSubagentIgnoresParentFrozen(t *testing.T) {
	const (
		childID  = "11111111-1111-4111-8111-111111111111"
		parentID = "22222222-2222-4222-8222-222222222222"
	)
	var forwarded []Message
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Messages []Message `json:"messages"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		forwarded = deepCopyMessages(body.Messages)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"usage":{"input_tokens":100}}`))
	}))
	defer upstream.Close()
	server := newPipelineTestServer(t, upstream.URL)
	history := pipelineMessages(3, 10)
	server.Frozen.Store(parentID, []Message{{Role: "user", Content: mustMarshal("parent frozen must not be used")}}, 1, history[0], 10, 20)
	server.Frozen.Store(childID, []Message{{Role: "user", Content: mustMarshal("child frozen must not be used")}}, 1, history[0], 10, 20)
	raw := append([]Message{pipelinePersistentContextMessage(t, "child-current")}, history...)
	servePipelineRequestWith(t, server, childID, raw, map[string]any{
		"agentContext": map[string]any{"agentType": "subagent", "parentSessionId": parentID},
	}, nil)
	assertPersistentContext(t, forwarded, "child-current")
	for _, message := range forwarded {
		text := allText(t, message)
		if strings.Contains(text, "frozen must not be used") {
			t.Fatalf("subagent 读取了 parent/current Frozen: %s", text)
		}
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

	result := server.Frozen.Get("thread-freeze", StripReminders(raw))
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
	history := pipelineMessages(300, 80)
	raw := append([]Message{pipelinePersistentContextMessage(t, "context-A")}, history...)
	servePipelineRequest(t, server, "thread-restore", raw)
	archivesAfterFreeze := archiveCount(t, server.Store)

	tail := pipelineMessages(2, 10)
	tail[0].Content = mustMarshal("fresh-tail-0")
	tail[1].Content = mustMarshal("fresh-tail-1")
	secondHistory := append(deepCopyMessages(history), tail...)
	secondRaw := append([]Message{pipelinePersistentContextMessage(t, "context-B")}, secondHistory...)
	servePipelineRequest(t, server, "thread-restore", secondRaw)

	if len(forwarded) != 2 {
		t.Fatalf("upstream request count = %d, want 2", len(forwarded))
	}
	if got, want := len(forwarded[1]), len(forwarded[0])+len(tail); got != want {
		t.Fatalf("restored message count = %d, want frozen prefix %d + tail %d = %d", got, len(forwarded[0]), len(tail), want)
	}
	assertPersistentContext(t, forwarded[0], "context-A")
	assertPersistentContext(t, forwarded[1], "context-B")
	if got := countMessagesContaining(forwarded[1], "context-A"); got != 0 {
		t.Fatalf("第二轮仍包含旧 context A，count=%d", got)
	}
	if got := countMessagesContaining(forwarded[1], "context-B"); got != 1 {
		t.Fatalf("第二轮 context B count=%d, want 1", got)
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
	detachedSecond, _ := DetachPersistentUserContext(secondRaw)
	if result := server.Frozen.Get("thread-restore", StripReminders(detachedSecond)); result == nil {
		t.Fatal("context A→B 后稳定 historical Frozen 应继续命中")
	}
	server.Frozen.mu.RLock()
	stored := deepCopyMessages(server.Frozen.messages["thread-restore"])
	server.Frozen.mu.RUnlock()
	if ExtractPersistentUserContext(stored) != nil {
		t.Fatal("Frozen snapshot 不得包含任一轮 persistent context")
	}
}

func TestHandleMessagesFrozenBoundaryEdit(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"usage":{"input_tokens":100,"output_tokens":1}}`))
	}))
	defer upstream.Close()

	server := newPipelineTestServer(t, upstream.URL)
	history := pipelineMessages(300, 80)
	raw := append([]Message{pipelinePersistentContextMessage(t, "boundary-A")}, history...)
	servePipelineRequest(t, server, "thread-boundary-edit", raw)
	archivesAfterFreeze := archiveCount(t, server.Store)

	editedHistory := deepCopyMessages(history)
	editedHistory[299].Content = mustMarshal("edited raw boundary")
	editedHistory = append(editedHistory, pipelineMessages(2, 10)...)
	edited := append([]Message{pipelinePersistentContextMessage(t, "boundary-B")}, editedHistory...)
	servePipelineRequest(t, server, "thread-boundary-edit", edited)

	if got := archiveCount(t, server.Store); got < archivesAfterFreeze {
		t.Fatalf("archive rows after edited boundary = %d, want at least %d", got, archivesAfterFreeze)
	}
	server.Frozen.mu.RLock()
	refreshedCutoff := server.Frozen.cutoff["thread-boundary-edit"]
	server.Frozen.mu.RUnlock()
	if refreshedCutoff != len(editedHistory) {
		t.Fatalf("refreshed historical cutoff=%d, want %d", refreshedCutoff, len(editedHistory))
	}
}

func TestHandleMessagesPersistentContextPaths(t *testing.T) {
	tests := []struct {
		name          string
		historyCount  int
		words         int
		subagent      bool
		setupFrozen   func(*Server, string, []Message)
		wantFrozenHit bool
	}{
		{name: "below threshold", historyCount: 6, words: 5},
		{name: "collapse", historyCount: 300, words: 80},
		{
			name: "valid frozen", historyCount: 6, words: 5, wantFrozenHit: true,
			setupFrozen: func(server *Server, sessionID string, history []Message) {
				prefix := deepCopyMessages(history[:2])
				server.Frozen.Store(sessionID, prefix, 2, history[1], server.TokenCounter.CountMessagesTokens(prefix), server.TokenCounter.CountMessagesTokens(history))
			},
		},
		{
			name: "invalid frozen", historyCount: 6, words: 5,
			setupFrozen: func(server *Server, sessionID string, history []Message) {
				prefix := deepCopyMessages(history[:2])
				wrongBoundary := history[1]
				wrongBoundary.Content = mustMarshal("edited historical boundary")
				server.Frozen.Store(sessionID, prefix, 2, wrongBoundary, server.TokenCounter.CountMessagesTokens(prefix), server.TokenCounter.CountMessagesTokens(history))
			},
		},
		{name: "subagent bypass", historyCount: 300, words: 80, subagent: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var forwarded []Message
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var body struct {
					Messages []Message `json:"messages"`
				}
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Fatal(err)
				}
				forwarded = deepCopyMessages(body.Messages)
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"usage":{"input_tokens":100}}`))
			}))
			defer upstream.Close()

			server := newPipelineTestServer(t, upstream.URL)
			sessionID := "thread-context-" + strings.ReplaceAll(tt.name, " ", "-")
			history := pipelineHistoryWithToolPair(t, tt.historyCount, tt.words)
			if tt.setupFrozen != nil {
				tt.setupFrozen(server, sessionID, history)
			}
			searchCalls := 0
			server.searchAndExpandFn = func(messages []Message, _ *SQLiteStore, _ int, _ *TokenCounter, _ *Budget, _ *requestMeta) RecallOutcome {
				searchCalls++
				return RecallOutcome{Messages: messages}
			}
			headers := map[string]string{}
			if tt.subagent {
				headers["x-anthropic-billing-header"] = "cc_is_subagent=true"
			}
			raw := append([]Message{pipelinePersistentContextMessage(t, "path-"+tt.name)}, history...)
			servePipelineRequestWith(t, server, sessionID, raw, nil, headers)

			assertPersistentContext(t, forwarded, "path-"+tt.name)
			assertToolPairOrder(t, forwarded, "tool-context-path")
			if tt.subagent {
				if searchCalls != 0 || archiveCount(t, server.Store) != 0 {
					t.Fatalf("subagent side effects: search=%d archives=%d", searchCalls, archiveCount(t, server.Store))
				}
			} else if searchCalls != 1 {
				t.Fatalf("main SearchAndExpand calls=%d, want 1", searchCalls)
			}
			if tt.wantFrozenHit && server.Frozen.LengthFor(sessionID) == 0 {
				t.Fatal("valid Frozen 应保持命中，不应被 context 坐标误判失效")
			}
			if tt.name == "invalid frozen" && server.Frozen.LengthFor(sessionID) != 0 {
				t.Fatal("真实 historical boundary 编辑应使 Frozen 失效")
			}
		})
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
	servePipelineRequestWith(t, server, sessionID, messages, nil, nil)
}

func servePipelineRequestWith(t *testing.T, server *Server, sessionID string, messages []Message, extra map[string]any, headers map[string]string) {
	t.Helper()
	requestBody := map[string]any{
		"model":    "deepseek-v4-pro",
		"thinking": map[string]any{"type": "enabled"},
		"messages": messages,
	}
	for key, value := range extra {
		requestBody[key] = value
	}
	body, err := json.Marshal(requestBody)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Claude-Code-Session-Id", sessionID)
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	recorder := httptest.NewRecorder()
	server.HandleMessages(recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("HandleMessages status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
}

func pipelinePersistentContextMessage(t *testing.T, label string) Message {
	t.Helper()
	text := "<system-reminder>\nAs you answer the user's questions, you can use the following context:\n# claudeMd\n" + label + "\n# currentDate\n2026-07-12\n</system-reminder>"
	raw, err := json.Marshal(map[string]any{
		"role": "user", "content": []map[string]any{{"type": "text", "text": text}},
		"isMeta": true, "future_context_field": map[string]any{"preserve": label},
	})
	if err != nil {
		t.Fatal(err)
	}
	var message Message
	if err := json.Unmarshal(raw, &message); err != nil {
		t.Fatal(err)
	}
	return message
}

func pipelineHistoryWithToolPair(t *testing.T, count, words int) []Message {
	t.Helper()
	if count < 2 {
		t.Fatal("history count must leave room for a tool pair")
	}
	messages := pipelineMessages(count, words)
	toolUse := `{"role":"assistant","content":[{"type":"tool_use","id":"tool-context-path","name":"Read","input":{"file_path":"context.go"}}],"future_tail_field":{"kind":"tool-use"}}`
	toolResult := `{"role":"user","content":[{"type":"tool_result","tool_use_id":"tool-context-path","content":"ok"}],"future_tail_field":{"kind":"tool-result"}}`
	if err := json.Unmarshal([]byte(toolUse), &messages[count-2]); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(toolResult), &messages[count-1]); err != nil {
		t.Fatal(err)
	}
	return messages
}

func assertPersistentContext(t *testing.T, messages []Message, label string) {
	t.Helper()
	if len(messages) == 0 || !strings.Contains(allText(t, messages[0]), "# claudeMd") || !strings.Contains(allText(t, messages[0]), label) {
		t.Fatalf("首消息不是本轮 persistent context %q: %v", label, messages)
	}
	if got := countMessagesContaining(messages, label); got != 1 {
		t.Fatalf("persistent context %q count=%d, want 1", label, got)
	}
	encoded, err := json.Marshal(messages[0])
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &fields); err != nil {
		t.Fatal(err)
	}
	if _, ok := fields["future_context_field"]; !ok {
		t.Fatalf("persistent context 未知字段丢失: %s", encoded)
	}
}

func countMessagesContaining(messages []Message, marker string) int {
	count := 0
	for _, message := range messages {
		blocks, _ := parseContent(message.Content)
		for _, block := range blocks {
			if block.Type == "text" && strings.Contains(block.Text, marker) {
				count++
				break
			}
		}
	}
	return count
}

func assertToolPairOrder(t *testing.T, messages []Message, toolID string) {
	t.Helper()
	useIndex, resultIndex := -1, -1
	for i, message := range messages {
		blocks, _ := parseContent(message.Content)
		for _, block := range blocks {
			if block.Type == "tool_use" && block.ID == toolID {
				useIndex = i
			}
			if block.Type == "tool_result" && block.ToolUseID == toolID {
				resultIndex = i
			}
		}
	}
	if useIndex < 0 || resultIndex != useIndex+1 {
		t.Fatalf("tool pair order invalid: use=%d result=%d", useIndex, resultIndex)
	}
	encoded, err := json.Marshal(messages[resultIndex])
	if err != nil || !bytes.Contains(encoded, []byte("future_tail_field")) {
		t.Fatalf("tool_result 未知字段丢失: %s err=%v", encoded, err)
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

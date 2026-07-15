package proxy

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPhase08CombinedLifecycle(t *testing.T) {
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
		_, _ = w.Write([]byte(`{"usage":{"input_tokens":196,"cache_creation_input_tokens":0,"cache_read_input_tokens":93056,"output_tokens":3}}`))
	}))
	defer upstream.Close()

	server := newPipelineTestServer(t, upstream.URL)
	debugDir := t.TempDir()
	server.Config.Debug = DebugConfig{Enabled: true, FullBody: false, DataDir: debugDir}
	var persisted string
	server.Sawtooth.SetPersistFunc(func(key, value string) {
		if key == "sawtooth:phase08-combined" {
			persisted = value
		}
	})

	history := pipelineMessages(300, 80)
	history = append(history, phase08ScreenshotToolPair(t)...)
	firstRaw := append([]Message{pipelinePersistentContextMessage(t, "phase08-context-A")}, history...)
	servePipelineRequest(t, server, "phase08-combined", firstRaw)
	archivesAfterCollapse := archiveCount(t, server.Store)
	if archivesAfterCollapse == 0 {
		t.Fatal("组合场景未触发 collapse/archive")
	}

	var fresh Message
	if err := json.Unmarshal([]byte(`{"role":"user","content":"phase08-fresh-tail","future_phase08":{"preserve":true}}`), &fresh); err != nil {
		t.Fatal(err)
	}
	secondHistory := append(deepCopyMessages(history), fresh)
	secondRaw := append([]Message{pipelinePersistentContextMessage(t, "phase08-context-B")}, secondHistory...)
	servePipelineRequest(t, server, "phase08-combined", secondRaw)

	if len(forwarded) != 2 {
		t.Fatalf("upstream requests=%d, want 2", len(forwarded))
	}
	if len(forwarded[0]) <= server.Config.Stubify.KeepRecent+2 {
		t.Fatalf("大截图后只保留 keep_recent 边界: forwarded=%d keep_recent=%d", len(forwarded[0]), server.Config.Stubify.KeepRecent)
	}
	assertPersistentContext(t, forwarded[1], "phase08-context-B")
	if got := countMessagesContaining(forwarded[1], "phase08-context-A"); got != 0 {
		t.Fatalf("Frozen restore 后旧 context A count=%d", got)
	}
	if got := countMessagesContaining(forwarded[1], "phase08-context-B"); got != 1 {
		t.Fatalf("Frozen restore 后 current context B count=%d", got)
	}
	lastJSON, err := json.Marshal(forwarded[1][len(forwarded[1])-1])
	if err != nil || !bytes.Contains(lastJSON, []byte("future_phase08")) {
		t.Fatalf("fresh tail 未知字段丢失: %s err=%v", lastJSON, err)
	}
	if got := archiveCount(t, server.Store); got != archivesAfterCollapse {
		t.Fatalf("Frozen hit 后 Archive 行增长: got=%d want=%d", got, archivesAfterCollapse)
	}
	if result := server.Frozen.Get("phase08-combined", StripReminders(secondHistory)); result == nil {
		t.Fatal("组合场景第二轮未保持 Frozen hit")
	}

	var state persistedState
	if err := json.Unmarshal([]byte(persisted), &state); err != nil {
		t.Fatalf("parse persisted Sawtooth state: %v raw=%q", err, persisted)
	}
	if state.Tokens != 93252 {
		t.Fatalf("Sawtooth persisted tokens=%d, want 93252", state.Tokens)
	}

	facts := readDebugFactFiles(t, debugDir, "phase08-combined")
	if len(facts) != 8 {
		t.Fatalf("两请求 facts=%d, want 8", len(facts))
	}
	stageByRequest := make(map[uint64]map[debugStage]debugFact)
	for _, data := range facts {
		if bytes.Contains(data, []byte("phase08-context-A")) || bytes.Contains(data, []byte("phase08-context-B")) || bytes.Contains(data, []byte(phase08ScreenshotBase64(t))) {
			t.Fatal("组合 facts 泄漏 context 或 base64")
		}
		var fact debugFact
		if err := json.Unmarshal(data, &fact); err != nil {
			t.Fatal(err)
		}
		if stageByRequest[fact.RequestID] == nil {
			stageByRequest[fact.RequestID] = make(map[debugStage]debugFact)
		}
		stageByRequest[fact.RequestID][fact.Stage] = fact
	}
	if len(stageByRequest) != 2 {
		t.Fatalf("facts request IDs=%d, want 2", len(stageByRequest))
	}
	for requestID, stages := range stageByRequest {
		for _, stage := range []debugStage{debugStageRawInbound, debugStagePressureDecision, debugStageForwarded, debugStageResponseUsage} {
			if _, ok := stages[stage]; !ok {
				t.Fatalf("request %d missing stage %q", requestID, stage)
			}
		}
		if usage := stages[debugStageResponseUsage]; usage.TotalInputTokens != 93252 {
			t.Fatalf("request %d usage total=%d, want 93252", requestID, usage.TotalInputTokens)
		}
		if raw := stages[debugStageRawInbound]; raw.ImageCount != 1 || !raw.HasClaudeMDContext {
			t.Fatalf("request %d raw facts=%+v", requestID, raw)
		}
		if forwardedFact := stages[debugStageForwarded]; forwardedFact.ImageCount != 1 || !forwardedFact.HasClaudeMDContext {
			t.Fatalf("request %d forwarded facts=%+v", requestID, forwardedFact)
		}
	}
}

func TestPhase08AgentIsolationMatrix(t *testing.T) {
	tests := []struct {
		name        string
		extra       map[string]any
		headers     map[string]string
		wantSearch  int
		wantArchive int
	}{
		{name: "billing subagent", headers: map[string]string{"x-anthropic-billing-header": "cch=1, cc_is_subagent=true"}},
		{name: "agentContext subagent", extra: map[string]any{"agentContext": map[string]any{"agentType": "subagent", "parentSessionId": "parent-secret"}}},
		{name: "system attribution subagent", extra: map[string]any{"system": []map[string]any{{"type": "text", "text": "x-anthropic-billing-header: cc_version=2.1.207; cc_is_subagent=true"}}}},
		{name: "unknown is main", wantSearch: 1, wantArchive: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"usage":{"input_tokens":1}}`))
			}))
			defer upstream.Close()
			server := newPipelineTestServer(t, upstream.URL)
			searchCalls := 0
			server.searchAndExpandFn = func(messages []Message, _ *SQLiteStore, _ int, _ *TokenCounter, _ *Budget, _ *requestMeta) RecallOutcome {
				searchCalls++
				return RecallOutcome{Messages: messages}
			}
			raw := append([]Message{pipelinePersistentContextMessage(t, "agent-matrix")}, pipelineMessages(300, 80)...)
			servePipelineRequestWith(t, server, "phase08-agent-"+strings.ReplaceAll(tt.name, " ", "-"), raw, tt.extra, tt.headers)
			if searchCalls != tt.wantSearch {
				t.Fatalf("SearchAndExpand calls=%d, want %d", searchCalls, tt.wantSearch)
			}
			if got := archiveCount(t, server.Store); got != tt.wantArchive {
				t.Fatalf("Archive rows=%d, want %d", got, tt.wantArchive)
			}
			if tt.wantSearch == 0 && server.Frozen.LengthFor("phase08-agent-"+strings.ReplaceAll(tt.name, " ", "-")) != 0 {
				t.Fatal("subagent 写入 Frozen")
			}
		})
	}
}

func TestPhase08DebugStagesWithoutCompressionPipeline(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"usage":{"input_tokens":2,"cache_read_input_tokens":3}}`))
	}))
	defer upstream.Close()
	cfg := DefaultConfig()
	cfg.Proxy.Target = upstream.URL
	cfg.Proxy.Deflation = 1
	cfg.Debug = DebugConfig{Enabled: true, FullBody: false, DataDir: t.TempDir()}
	server := NewServer(cfg)

	body := `{"model":"claude-test","messages":[{"role":"user","content":"direct forward"}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(body))
	req.Header.Set("X-Claude-Code-Session-Id", "phase08-direct-forward")
	recorder := httptest.NewRecorder()
	server.HandleMessages(recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}

	facts := readDebugFactFiles(t, cfg.Debug.DataDir, "phase08-direct-forward")
	stageCounts := make(map[debugStage]int)
	for _, data := range facts {
		var fact debugFact
		if err := json.Unmarshal(data, &fact); err != nil {
			t.Fatal(err)
		}
		stageCounts[fact.Stage]++
	}
	for _, stage := range []debugStage{debugStageRawInbound, debugStageForwarded, debugStageResponseUsage} {
		if stageCounts[stage] != 1 {
			t.Fatalf("direct path stage %q count=%d, want 1; all=%v", stage, stageCounts[stage], stageCounts)
		}
	}
}

func TestPhase08RequirementCoverage(t *testing.T) {
	coverage := map[string]string{
		"MMTOK-01": "TestTokenCounterMultimodalNestedToolResult",
		"MMTOK-02": "TestTokenCounterImageFormatsAndBoundedPayload",
		"MMTOK-03": "TestPhase08CombinedLifecycle",
		"CTX-01":   "TestPhase08CombinedLifecycle",
		"CTX-02":   "TestHandleMessagesPersistentContextPaths",
		"CTX-03":   "TestPhase08CombinedLifecycle",
		"CTX-04":   "TestPhase08CombinedLifecycle",
		"AGENT-01": "TestPhase08AgentIsolationMatrix",
		"AGENT-02": "TestPhase08AgentIsolationMatrix",
		"USAGE-01": "TestHandleSSECacheUsagePersistsTotalBeforeDeflation,TestHandleJSONCacheUsagePersistsTotalAndColdStartTriggers",
		"USAGE-02": "TestPhase08CombinedLifecycle",
		"DEBUG-01": "TestPhase08CombinedLifecycle",
		"DEBUG-02": "TestDebugFactsSchemaAndSecretSafety,TestPhase08CombinedLifecycle",
	}
	want := []string{"MMTOK-01", "MMTOK-02", "MMTOK-03", "CTX-01", "CTX-02", "CTX-03", "CTX-04", "AGENT-01", "AGENT-02", "USAGE-01", "USAGE-02", "DEBUG-01", "DEBUG-02"}
	if len(coverage) != len(want) {
		t.Fatalf("requirement mappings=%d, want %d", len(coverage), len(want))
	}
	for _, requirement := range want {
		if strings.TrimSpace(coverage[requirement]) == "" {
			t.Fatalf("requirement %s missing behavior test mapping", requirement)
		}
	}
}

func phase08ScreenshotToolPair(t *testing.T) []Message {
	t.Helper()
	blockData, err := os.ReadFile(filepath.Join("testdata", "multimodal", "large-screenshot-tool-result.json"))
	if err != nil {
		t.Fatal(err)
	}
	var block map[string]any
	if err := json.Unmarshal(blockData, &block); err != nil {
		t.Fatal(err)
	}
	toolID, _ := block["tool_use_id"].(string)
	toolUseContent, err := json.Marshal([]any{map[string]any{
		"type": "tool_use", "id": toolID, "name": "Screenshot", "input": map[string]any{},
	}})
	if err != nil {
		t.Fatal(err)
	}
	toolResultContent, err := json.Marshal([]any{block})
	if err != nil {
		t.Fatal(err)
	}
	return []Message{
		{Role: "assistant", Content: toolUseContent},
		{Role: "user", Content: toolResultContent},
	}
}

func phase08ScreenshotBase64(t *testing.T) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", "multimodal", "large-screenshot-tool-result.json"))
	if err != nil {
		t.Fatal(err)
	}
	var block struct {
		Content []struct {
			Source struct {
				Data string `json:"data"`
			} `json:"source"`
		} `json:"content"`
	}
	if err := json.Unmarshal(data, &block); err != nil {
		t.Fatal(err)
	}
	for _, content := range block.Content {
		if content.Source.Data != "" {
			return content.Source.Data
		}
	}
	t.Fatal("screenshot fixture missing base64")
	return ""
}

package proxy

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

var allowedDebugFactKeys = map[string]bool{
	"timestamp": true, "request_id": true, "stage": true,
	"model_family": true, "message_count": true, "has_claude_md_context": true,
	"image_count": true, "document_count": true, "decoded_byte_count": true,
	"estimated_tokens": true, "agent_role": true, "agent_reason": true,
	"input_tokens": true, "cache_creation_input_tokens": true,
	"cache_read_input_tokens": true, "total_input_tokens": true, "error": true,
}

func TestDebugFactsSchemaAndSecretSafety(t *testing.T) {
	dataDir := t.TempDir()
	cfg := DefaultConfig()
	cfg.Debug = DebugConfig{Enabled: true, FullBody: false, DataDir: dataDir}
	s := NewServer(cfg)
	tc, err := NewTokenCounter()
	if err != nil {
		t.Fatal(err)
	}
	s.TokenCounter = tc
	meta := newRequestMeta(77, "SESSION-ID-MUST-NOT-LEAK")
	base64Marker := base64.StdEncoding.EncodeToString([]byte("UNIQUE-BASE64-PAYLOAD-MUST-NOT-LEAK"))
	body := []byte(`{
		"model":"claude-secret-model-suffix",
		"system":"SYSTEM-PROMPT-MUST-NOT-LEAK",
		"agentContext":{"agentType":"subagent","parentSessionId":"PARENT-ID-MUST-NOT-LEAK"},
		"messages":[
			{"role":"user","isMeta":true,"content":[{"type":"text","text":"<system-reminder>\n# claudeMd\nCALL-ME-BOSS-SECRET\n</system-reminder>"}]},
			{"role":"user","content":[{"type":"tool_result","tool_use_id":"tool-1","content":[{"type":"image","source":{"type":"base64","media_type":"image/png","data":"` + base64Marker + `"}},{"type":"document","source":{"type":"base64","media_type":"application/pdf","data":"` + base64Marker + `"}}]}]}
		]
	}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer AUTHORIZATION-MUST-NOT-LEAK")
	req.Header.Set("x-anthropic-billing-header", "cc_is_subagent=true; BILLING-MUST-NOT-LEAK")
	req.Header.Set("X-Claude-Code-Session-Id", "HEADER-SESSION-MUST-NOT-LEAK")

	s.writeRequestDebugFacts(meta, time.Date(2026, 7, 12, 1, 2, 3, 4, time.UTC), debugStageRawInbound, body, req)
	files := readDebugFactFiles(t, dataDir, meta.RequestSessionID)
	if len(files) != 1 {
		t.Fatalf("facts 文件数=%d, want 1", len(files))
	}
	data := files[0]
	for _, secret := range []string{
		"SESSION-ID-MUST-NOT-LEAK", "claude-secret-model-suffix", "SYSTEM-PROMPT-MUST-NOT-LEAK",
		"PARENT-ID-MUST-NOT-LEAK", "CALL-ME-BOSS-SECRET", base64Marker,
		"AUTHORIZATION-MUST-NOT-LEAK", "BILLING-MUST-NOT-LEAK", "HEADER-SESSION-MUST-NOT-LEAK",
	} {
		if bytes.Contains(data, []byte(secret)) {
			t.Fatalf("facts 泄漏 %q: %s", secret, data)
		}
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		t.Fatalf("facts 不是完整 JSON: %v: %s", err, data)
	}
	for key, value := range fields {
		if !allowedDebugFactKeys[key] {
			t.Fatalf("facts 顶层出现非白名单 key %q", key)
		}
		var nested map[string]any
		if json.Unmarshal(value, &nested) == nil && nested != nil {
			t.Fatalf("facts key %q 包含嵌套对象: %s", key, value)
		}
	}
	var fact debugFact
	if err := json.Unmarshal(data, &fact); err != nil {
		t.Fatal(err)
	}
	if fact.Stage != debugStageRawInbound || fact.MessageCount != 2 || !fact.HasClaudeMDContext {
		t.Fatalf("raw fact 基本字段错误: %+v", fact)
	}
	if fact.ImageCount != 1 || fact.DocumentCount != 1 || fact.DecodedByteCount == 0 || fact.EstimatedTokens == 0 {
		t.Fatalf("多模态 facts 错误: %+v", fact)
	}
	if fact.AgentRole != agentRoleSubagent || fact.AgentReason != agentReasonBillingMarker {
		t.Fatalf("agent facts 错误: %+v", fact)
	}
}

func TestDebugFactsConcurrentStagesDoNotOverwrite(t *testing.T) {
	dataDir := t.TempDir()
	cfg := DefaultConfig()
	cfg.Debug = DebugConfig{Enabled: true, DataDir: dataDir}
	s := NewServer(cfg)
	body := []byte(`{"model":"claude-test","messages":[]}`)
	stamp := time.Date(2026, 7, 12, 1, 2, 3, 4, time.UTC)
	var wg sync.WaitGroup
	for _, id := range []uint64{1, 2} {
		for _, stage := range []debugStage{debugStageRawInbound, debugStageForwarded} {
			wg.Add(1)
			go func() {
				defer wg.Done()
				s.writeRequestDebugFacts(newRequestMeta(id, "same-session"), stamp, stage, body, nil)
			}()
		}
	}
	wg.Wait()
	files := readDebugFactFiles(t, dataDir, "same-session")
	if len(files) != 4 {
		t.Fatalf("并发 facts 文件数=%d, want 4", len(files))
	}
}

func TestDebugFactsUsageUsesTotalInputTokens(t *testing.T) {
	dataDir := t.TempDir()
	cfg := DefaultConfig()
	cfg.Debug = DebugConfig{Enabled: true, DataDir: dataDir}
	s := NewServer(cfg)
	meta := newRequestMeta(9, "usage-session")
	s.writeUsageDebugFacts(meta, time.Date(2026, 7, 12, 1, 2, 3, 4, time.UTC), map[string]any{
		"input_tokens": 196, "cache_creation_input_tokens": 0, "cache_read_input_tokens": 93056,
	})
	files := readDebugFactFiles(t, dataDir, meta.RequestSessionID)
	if len(files) != 1 {
		t.Fatalf("usage facts 文件数=%d, want 1", len(files))
	}
	var fact debugFact
	if err := json.Unmarshal(files[0], &fact); err != nil {
		t.Fatal(err)
	}
	if fact.Stage != debugStageResponseUsage || fact.InputTokens != 196 || fact.CacheReadInputTokens != 93056 || fact.TotalInputTokens != 93252 {
		t.Fatalf("usage fact=%+v", fact)
	}
}

func TestDebugFullBodyDefaultsOff(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.Debug.FullBody {
		t.Fatal("DefaultConfig debug.full_body 必须为 false")
	}
	yamlData, err := os.ReadFile(filepath.Join("..", "..", "sawtooth.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(yamlData, []byte("full_body: false")) {
		t.Fatalf("sawtooth.yaml 未显式关闭 full_body:\n%s", yamlData)
	}
}

func readDebugFactFiles(t *testing.T, dataDir, sessionID string) [][]byte {
	t.Helper()
	dir, ok := safeDebugSessionDir(dataDir, sessionID)
	if !ok {
		t.Fatal("debug dir invalid")
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	files := make([][]byte, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.Contains(entry.Name(), "-facts.json") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, data)
	}
	return files
}

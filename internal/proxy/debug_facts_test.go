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

func TestDecodedBase64SizeValidatesInput(t *testing.T) {
	tests := []struct {
		name string
		data string
		want int
	}{
		{name: "valid exact count", data: "QUJDRA==", want: 4},
		{name: "invalid character", data: "QUJD$A==", want: 0},
		{name: "embedded newline", data: "QU\nJDRA==", want: 0},
		{name: "invalid padding", data: "A===", want: 0},
		{name: "oversized", data: strings.Repeat("A", 8*1024*1024+4), want: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := decodedBase64Size(tt.data); got != tt.want {
				t.Fatalf("decodedBase64Size() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestDebugFactsInvalidMediaBase64UsesRestrictedError(t *testing.T) {
	dataDir := t.TempDir()
	cfg := DefaultConfig()
	cfg.Debug = DebugConfig{Enabled: true, DataDir: dataDir}
	s := NewServer(cfg)
	meta := newRequestMeta(10, "invalid-media-session")
	body := []byte(`{"model":"claude-test","messages":[{"role":"user","content":[
		{"type":"image","source":{"type":"base64","media_type":"image/png","data":"QUJDRA=="}},
		{"type":"image","source":{"type":"base64","media_type":"image/png","data":"QUJD$A=="}}
	]}]}`)

	s.writeRequestDebugFacts(meta, time.Date(2026, 7, 12, 1, 2, 3, 4, time.UTC), debugStageRawInbound, body, nil)
	files := readDebugFactFiles(t, dataDir, meta.RequestSessionID)
	if len(files) != 1 {
		t.Fatalf("facts 文件数=%d, want 1", len(files))
	}
	var fact debugFact
	if err := json.Unmarshal(files[0], &fact); err != nil {
		t.Fatal(err)
	}
	if fact.ImageCount != 2 || fact.DecodedByteCount != 0 {
		t.Fatalf("invalid media facts=%+v", fact)
	}
	if fact.Error != debugError("invalid_media_base64") {
		t.Fatalf("invalid media error=%q", fact.Error)
	}
}

func TestDebugFullBodyDefaultsOff(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.Debug.FullBody {
		t.Fatal("DefaultConfig debug.full_body 必须为 false")
	}
	configPath := filepath.Join(t.TempDir(), "sawtooth.yaml")
	yamlData := []byte("debug:\n  enabled: true\n  full_body: false\n  data_dir: ./debug\n")
	if err := os.WriteFile(configPath, yamlData, 0600); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadConfig(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if !loaded.Debug.Enabled || loaded.Debug.FullBody || loaded.Debug.DataDir != "./debug" {
		t.Fatalf("debug 配置解析错误: %+v", loaded.Debug)
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

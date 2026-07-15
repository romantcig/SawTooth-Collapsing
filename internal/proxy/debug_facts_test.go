package proxy

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"log/slog"
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
	"messages_local_tokens": true, "system_local_tokens": true, "tools_local_tokens": true,
	"full_local_tokens": true, "previous_actual_tokens": true, "previous_message_count": true,
	"new_message_delta_tokens": true, "selected_pressure_tokens": true,
	"pressure_threshold_tokens": true, "pressure_source": true, "trigger_reason": true,
	"baseline_reset_reason": true, "compress_decision": true,
	"system_fingerprint_changed": true, "tools_fingerprint_changed": true,
	"baseline_updated": true, "actual_minus_selected_tokens": true,
}

func TestDebugFactsPressureDecisionAndUsageJoin(t *testing.T) {
	dataDir := t.TempDir()
	cfg := DefaultConfig()
	cfg.Debug = DebugConfig{Enabled: true, DataDir: dataDir}
	s := NewServer(cfg)
	s.Sawtooth = NewSawtoothTrigger(time.Minute, 16000, 8000)
	meta := newRequestMeta(101, "pressure-join-session")
	meta.PressureDecision = pressureDecision{
		Available:                true,
		MessagesLocalTokens:      7000,
		SystemLocalTokens:        1200,
		ToolsLocalTokens:         800,
		FullLocalEstimate:        9000,
		PreviousActual:           15000,
		PreviousMessageCount:     12,
		NewMessageDelta:          2500,
		SelectedPressure:         17500,
		Source:                   pressureSourceActualPlusDelta,
		ResetReason:              baselineResetNone,
		TriggerReason:            TriggerTokens,
		Threshold:                16000,
		MessageCount:             14,
		CompressDecision:         true,
		SystemFingerprintChanged: false,
		ToolsFingerprintChanged:  true,
	}
	stamp := time.Date(2026, 7, 15, 1, 2, 3, 4, time.UTC)
	s.writePressureDecisionDebugFacts(meta, stamp)
	s.writePressureDecisionDebugFacts(meta, stamp.Add(time.Nanosecond))
	s.writeUsageDebugFacts(meta, stamp, map[string]any{
		"input_tokens": 19000, "cache_creation_input_tokens": 1000, "cache_read_input_tokens": 500,
	})

	facts := debugFactsByStage(t, dataDir, meta.RequestSessionID)
	if len(facts) != 2 {
		t.Fatalf("facts stage 数=%d, want pressure+usage 共 2: %v", len(facts), facts)
	}
	pressure := facts[debugStagePressureDecision]
	usage := facts[debugStageResponseUsage]
	for key, want := range map[string]any{
		"messages_local_tokens":      7000.0,
		"system_local_tokens":        1200.0,
		"tools_local_tokens":         800.0,
		"full_local_tokens":          9000.0,
		"previous_actual_tokens":     15000.0,
		"previous_message_count":     12.0,
		"new_message_delta_tokens":   2500.0,
		"selected_pressure_tokens":   17500.0,
		"pressure_threshold_tokens":  16000.0,
		"pressure_source":            string(pressureSourceActualPlusDelta),
		"trigger_reason":             string(TriggerTokens),
		"baseline_reset_reason":      string(baselineResetNone),
		"compress_decision":          true,
		"system_fingerprint_changed": false,
		"tools_fingerprint_changed":  true,
	} {
		if got := pressure[key]; got != want {
			t.Fatalf("pressure[%s]=%v (%T), want %v (%T)", key, got, got, want, want)
		}
	}
	if got := usage["total_input_tokens"]; got != 20500.0 {
		t.Fatalf("usage total=%v, want deflation 前 20500", got)
	}
	if got := usage["baseline_updated"]; got != true {
		t.Fatalf("usage baseline_updated=%v, want true", got)
	}
	if got := usage["actual_minus_selected_tokens"]; got != 3000.0 {
		t.Fatalf("usage actual_minus_selected=%v, want 3000", got)
	}
	if pressure["request_id"] != usage["request_id"] || pressure["request_id"] != 101.0 {
		t.Fatalf("request_id join 失败: pressure=%v usage=%v", pressure["request_id"], usage["request_id"])
	}
}

func TestDebugFactsAuxiliaryUsageDoesNotClaimBaseline(t *testing.T) {
	for _, tc := range []struct {
		name string
		meta *requestMeta
	}{
		{name: "session title", meta: &requestMeta{ID: 201, RequestSessionID: "title-usage", RequestKind: requestKindSessionTitle}},
		{name: "subagent", meta: &requestMeta{ID: 202, RequestSessionID: "subagent-usage", AgentRole: agentRoleSubagent}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dataDir := t.TempDir()
			cfg := DefaultConfig()
			cfg.Debug = DebugConfig{Enabled: true, DataDir: dataDir}
			s := NewServer(cfg)
			s.Sawtooth = NewSawtoothTrigger(time.Minute, 16000, 8000)
			tc.meta.PressureDecision = pressureDecision{Available: true, SelectedPressure: 9000}
			stamp := time.Date(2026, 7, 15, 2, 3, 4, 5, time.UTC)
			s.writePressureDecisionDebugFacts(tc.meta, stamp)
			s.writeUsageDebugFacts(tc.meta, stamp, map[string]any{"input_tokens": 10000})

			facts := debugFactsByStage(t, dataDir, tc.meta.RequestSessionID)
			if _, ok := facts[debugStagePressureDecision]; ok {
				t.Fatalf("辅助请求写入 pressure_decision: %v", facts)
			}
			usage, ok := facts[debugStageResponseUsage]
			if !ok || len(facts) != 1 {
				t.Fatalf("辅助请求 facts=%v, want only response_usage", facts)
			}
			if got := usage["baseline_updated"]; got != false {
				t.Fatalf("辅助 usage baseline_updated=%v, want false", got)
			}
			if _, ok := usage["actual_minus_selected_tokens"]; ok {
				t.Fatalf("辅助 usage 不应比较主 pressure: %v", usage)
			}
		})
	}
}

func TestDebugFactsForwardedDoesNotReplacePressure(t *testing.T) {
	dataDir := t.TempDir()
	cfg := DefaultConfig()
	cfg.Debug = DebugConfig{Enabled: true, DataDir: dataDir}
	s := NewServer(cfg)
	tc, err := NewTokenCounter()
	if err != nil {
		t.Fatal(err)
	}
	s.TokenCounter = tc
	meta := newRequestMeta(301, "forwarded-pressure-session")
	meta.PressureDecision = pressureDecision{
		Available: true, MessagesLocalTokens: 12000, FullLocalEstimate: 12000,
		SelectedPressure: 17000, Threshold: 16000, Source: pressureSourceActualPlusDelta,
		TriggerReason: TriggerTokens, CompressDecision: true,
	}
	stamp := time.Date(2026, 7, 15, 3, 4, 5, 6, time.UTC)
	s.writePressureDecisionDebugFacts(meta, stamp)
	forwardedBody := []byte(`{"model":"claude-test","messages":[{"role":"user","content":"tiny forwarded"}]}`)
	s.writeRequestDebugFacts(meta, stamp, debugStageForwarded, forwardedBody, nil)

	facts := debugFactsByStage(t, dataDir, meta.RequestSessionID)
	pressure := facts[debugStagePressureDecision]
	forwarded := facts[debugStageForwarded]
	if pressure["selected_pressure_tokens"] != 17000.0 {
		t.Fatalf("selected pressure=%v, want 17000", pressure["selected_pressure_tokens"])
	}
	if forwarded["estimated_tokens"] == pressure["selected_pressure_tokens"] {
		t.Fatalf("forwarded estimate 冒充 trigger basis: forwarded=%v pressure=%v", forwarded, pressure)
	}
	if _, ok := forwarded["selected_pressure_tokens"]; ok {
		t.Fatalf("forwarded stage 含 pressure 专属字段: %v", forwarded)
	}
}

func TestFormatApproxTokens(t *testing.T) {
	for _, tc := range []struct {
		name   string
		tokens int
		want   string
	}{
		{name: "zero", tokens: 0, want: "0"},
		{name: "below thousand", tokens: 999, want: "999"},
		{name: "exact thousand", tokens: 1000, want: "1k"},
		{name: "round down", tokens: 1049, want: "1k"},
		{name: "one decimal", tokens: 1050, want: "1.1k"},
		{name: "large one decimal", tokens: 17500, want: "17.5k"},
		{name: "round carry", tokens: 19950, want: "20k"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := formatApproxTokens(tc.tokens); got != tc.want {
				t.Fatalf("formatApproxTokens(%d)=%q, want %q", tc.tokens, got, tc.want)
			}
		})
	}
}

func TestPressureSummarySingleMainOnly(t *testing.T) {
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(NewLogHandler(&logs, slog.LevelInfo)))
	t.Cleanup(func() { slog.SetDefault(previous) })

	mainMeta := newRequestMeta(401, "SUMMARY-SESSION-MUST-NOT-LEAK")
	mainMeta.PressureDecision = pressureDecision{
		Available: true, SelectedPressure: 17500, Threshold: 16000,
		Source: pressureSourceActualPlusDelta, TriggerReason: TriggerTokens,
		ResetReason: baselineResetNone, CompressDecision: true,
	}
	titleMeta := newRequestMeta(402, "TITLE-SESSION-MUST-NOT-LEAK")
	titleMeta.RequestKind = requestKindSessionTitle
	titleMeta.PressureDecision = mainMeta.PressureDecision
	subagentMeta := newRequestMeta(403, "SUBAGENT-SESSION-MUST-NOT-LEAK")
	subagentMeta.AgentRole = agentRoleSubagent
	subagentMeta.PressureDecision = mainMeta.PressureDecision

	logPressureSummary(mainMeta)
	logPressureSummary(mainMeta)
	logPressureSummary(titleMeta)
	logPressureSummary(subagentMeta)

	var summaryLines []string
	for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
		if strings.Contains(line, "pressure 摘要") {
			summaryLines = append(summaryLines, line)
		}
	}
	if len(summaryLines) != 1 {
		t.Fatalf("pressure 摘要条数=%d, want 1:\n%s", len(summaryLines), logs.String())
	}
	line := summaryLines[0]
	for _, want := range []string{
		"request_id=401", "pressure=17.5k", "threshold=16k",
		"source=actual_plus_delta", "trigger_reason=tokens",
		"baseline_reset_reason=none", "compress=true",
	} {
		if !strings.Contains(line, want) {
			t.Fatalf("摘要缺少 %q: %s", want, line)
		}
	}
	for _, forbidden := range []string{
		"17500", "16000", "forwarded", "SUMMARY-SESSION-MUST-NOT-LEAK",
		"TITLE-SESSION-MUST-NOT-LEAK", "SUBAGENT-SESSION-MUST-NOT-LEAK",
	} {
		if strings.Contains(line, forbidden) {
			t.Fatalf("摘要泄漏或混淆 %q: %s", forbidden, line)
		}
	}
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
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(NewLogHandler(&logs, slog.LevelInfo)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	meta := newRequestMeta(77, "SESSION-ID-MUST-NOT-LEAK")
	meta.PressureDecision = pressureDecision{
		Available: true, MessagesLocalTokens: 1000, SystemLocalTokens: 200,
		ToolsLocalTokens: 300, FullLocalEstimate: 1500, PreviousActual: 1400,
		PreviousMessageCount: 2, NewMessageDelta: 100, SelectedPressure: 1500,
		Threshold: 16000, Source: pressureSourceActualPlusDelta,
		ResetReason: baselineResetNone, TriggerReason: TriggerNone,
		SystemFingerprint: "FULL-SYSTEM-FINGERPRINT-MUST-NOT-LEAK",
		ToolsFingerprint:  "FULL-TOOLS-FINGERPRINT-MUST-NOT-LEAK",
	}
	base64Marker := base64.StdEncoding.EncodeToString([]byte("UNIQUE-BASE64-PAYLOAD-MUST-NOT-LEAK"))
	body := []byte(`{
		"model":"claude-secret-model-suffix",
		"system":"SYSTEM-PROMPT-MUST-NOT-LEAK",
		"tools":[{"name":"TOOLS-SCHEMA-MUST-NOT-LEAK","description":"TOOLS-DESCRIPTION-MUST-NOT-LEAK","input_schema":{"type":"object","properties":{"secret":{"const":"TOOLS-CONST-MUST-NOT-LEAK"}}}}],
		"metadata":{"title":"SESSION-TITLE-MUST-NOT-LEAK"},
		"agentContext":{"agentType":"subagent","parentSessionId":"PARENT-ID-MUST-NOT-LEAK"},
		"messages":[
			{"role":"user","isMeta":true,"content":[{"type":"text","text":"<system-reminder>\n# claudeMd\nCALL-ME-BOSS-SECRET\n</system-reminder>"}]},
			{"role":"user","content":[{"type":"tool_result","tool_use_id":"tool-1","content":[{"type":"image","source":{"type":"base64","media_type":"image/png","data":"` + base64Marker + `"}},{"type":"document","source":{"type":"base64","media_type":"application/pdf","data":"` + base64Marker + `"}}]}]}
		]
	}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer AUTHORIZATION-MUST-NOT-LEAK")
	req.Header.Set("x-api-key", "API-KEY-MUST-NOT-LEAK")
	req.Header.Set("anthropic-api-key", "ANTHROPIC-KEY-MUST-NOT-LEAK")
	req.Header.Set("x-anthropic-billing-header", "cc_is_subagent=true; BILLING-MUST-NOT-LEAK")
	req.Header.Set("X-Claude-Code-Session-Id", "HEADER-SESSION-MUST-NOT-LEAK")

	stamp := time.Date(2026, 7, 12, 1, 2, 3, 4, time.UTC)
	s.writeRequestDebugFacts(meta, stamp, debugStageRawInbound, body, req)
	s.writePressureDecisionDebugFacts(meta, stamp)
	logPressureSummary(meta)
	files := readDebugFactFiles(t, dataDir, meta.RequestSessionID)
	if len(files) != 2 {
		t.Fatalf("facts 文件数=%d, want raw+pressure 共 2", len(files))
	}
	secrets := []string{
		"SESSION-ID-MUST-NOT-LEAK", "claude-secret-model-suffix", "SYSTEM-PROMPT-MUST-NOT-LEAK",
		"PARENT-ID-MUST-NOT-LEAK", "CALL-ME-BOSS-SECRET", base64Marker,
		"AUTHORIZATION-MUST-NOT-LEAK", "BILLING-MUST-NOT-LEAK", "HEADER-SESSION-MUST-NOT-LEAK",
		"API-KEY-MUST-NOT-LEAK", "ANTHROPIC-KEY-MUST-NOT-LEAK",
		"TOOLS-SCHEMA-MUST-NOT-LEAK", "TOOLS-DESCRIPTION-MUST-NOT-LEAK", "TOOLS-CONST-MUST-NOT-LEAK",
		"SESSION-TITLE-MUST-NOT-LEAK", "FULL-SYSTEM-FINGERPRINT-MUST-NOT-LEAK", "FULL-TOOLS-FINGERPRINT-MUST-NOT-LEAK",
	}
	var rawFact debugFact
	for _, data := range files {
		for _, secret := range secrets {
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
			var nestedSlice []any
			if json.Unmarshal(value, &nestedSlice) == nil && nestedSlice != nil {
				t.Fatalf("facts key %q 包含嵌套数组: %s", key, value)
			}
		}
		var fact debugFact
		if err := json.Unmarshal(data, &fact); err != nil {
			t.Fatal(err)
		}
		if fact.Stage == debugStageRawInbound {
			rawFact = fact
		}
	}
	for _, secret := range secrets {
		if strings.Contains(logs.String(), secret) {
			t.Fatalf("pressure 摘要泄漏 %q: %s", secret, logs.String())
		}
	}
	if rawFact.Stage != debugStageRawInbound || rawFact.MessageCount != 2 || !rawFact.HasClaudeMDContext {
		t.Fatalf("raw fact 基本字段错误: %+v", rawFact)
	}
	if rawFact.ImageCount != 1 || rawFact.DocumentCount != 1 || rawFact.DecodedByteCount == 0 || rawFact.EstimatedTokens == 0 {
		t.Fatalf("多模态 facts 错误: %+v", rawFact)
	}
	if rawFact.AgentRole != agentRoleSubagent || rawFact.AgentReason != agentReasonBillingMarker {
		t.Fatalf("agent facts 错误: %+v", rawFact)
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
		meta := newRequestMeta(id, "same-session")
		meta.PressureDecision = pressureDecision{
			Available: true, SelectedPressure: int(id) * 1000,
			Threshold: 16000, Source: pressureSourceLocalFull,
		}
		for _, stage := range []debugStage{debugStageRawInbound, debugStageForwarded, debugStagePressureDecision} {
			stage := stage
			wg.Add(1)
			go func() {
				defer wg.Done()
				if stage == debugStagePressureDecision {
					s.writePressureDecisionDebugFacts(meta, stamp)
					return
				}
				s.writeRequestDebugFacts(meta, stamp, stage, body, nil)
			}()
		}
	}
	wg.Wait()
	files := readDebugFactFiles(t, dataDir, "same-session")
	if len(files) != 6 {
		t.Fatalf("并发 facts 文件数=%d, want 6", len(files))
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
	maxInt := int(^uint(0) >> 1)
	minInt := -maxInt - 1
	if got := saturatingSubtract(maxInt, -1); got != maxInt {
		t.Fatalf("正向饱和差值=%d, want %d", got, maxInt)
	}
	if got := saturatingSubtract(minInt, 1); got != minInt {
		t.Fatalf("负向饱和差值=%d, want %d", got, minInt)
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

func debugFactsByStage(t *testing.T, dataDir, sessionID string) map[debugStage]map[string]any {
	t.Helper()
	result := make(map[debugStage]map[string]any)
	for _, data := range readDebugFactFiles(t, dataDir, sessionID) {
		var fact map[string]any
		if err := json.Unmarshal(data, &fact); err != nil {
			t.Fatalf("解析 facts: %v: %s", err, data)
		}
		stage, _ := fact["stage"].(string)
		if _, exists := result[debugStage(stage)]; exists {
			t.Fatalf("stage %q 重复: %v", stage, result)
		}
		result[debugStage(stage)] = fact
	}
	return result
}

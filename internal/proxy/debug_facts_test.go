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
	"history_epoch": true, "history_common_prefix": true, "history_transition_reason": true,
	"history_epoch_changed": true, "history_mismatch_first": true, "history_transition_failed": true,
	"messages_local_tokens": true, "system_local_tokens": true, "tools_local_tokens": true,
	"full_local_tokens": true, "previous_actual_tokens": true, "previous_message_count": true,
	"new_message_delta_tokens": true, "selected_pressure_tokens": true,
	"pressure_threshold_tokens": true, "pressure_source": true, "trigger_reason": true,
	"baseline_reset_reason": true, "compress_decision": true,
	"system_fingerprint_changed": true, "tools_fingerprint_changed": true,
	"baseline_updated": true, "baseline_update_kind": true, "actual_minus_selected_tokens": true,
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
	}, true)

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
	if got := usage["baseline_update_kind"]; got != string(pressureBaselineUpdateExact) {
		t.Fatalf("usage baseline_update_kind=%v, want exact", got)
	}
	if got := usage["actual_minus_selected_tokens"]; got != 3000.0 {
		t.Fatalf("usage actual_minus_selected=%v, want 3000", got)
	}
	if pressure["request_id"] != usage["request_id"] || pressure["request_id"] != 101.0 {
		t.Fatalf("request_id join 失败: pressure=%v usage=%v", pressure["request_id"], usage["request_id"])
	}

	failureMeta := newRequestMeta(102, "pressure-failure-session")
	failureMeta.PressureDecision = meta.PressureDecision
	s.writePressureDecisionDebugFacts(failureMeta, stamp)
	failureFacts := debugFactsByStage(t, dataDir, failureMeta.RequestSessionID)
	if len(failureFacts) != 1 || failureFacts[debugStagePressureDecision] == nil {
		t.Fatalf("失败响应必须只保留 decision: %v", failureFacts)
	}
	if _, ok := failureFacts[debugStageResponseUsage]; ok {
		t.Fatalf("失败响应伪造 response_usage: %v", failureFacts)
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
			s.writeUsageDebugFacts(tc.meta, stamp, map[string]any{"input_tokens": 10000}, false)

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
	}, false)
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

func TestDebugFactsUseUnifiedLayoutAndStableStageNames(t *testing.T) {
	dataDir := t.TempDir()
	cfg := DefaultConfig()
	cfg.Debug = DebugConfig{Enabled: true, FullBody: true, DataDir: dataDir}
	s := NewServer(cfg)
	meta := s.nextRequestMeta("unified-layout-session-secret")
	meta.PressureDecision = pressureDecision{
		Available:        true,
		SelectedPressure: 17000,
		Threshold:        16000,
		Source:           pressureSourceLocalFull,
	}
	stamp := time.Date(2026, 7, 24, 12, 34, 56, 789, time.UTC)
	body := []byte(`{"model":"claude-test","messages":[]}`)

	s.writeRequestDebugFacts(meta, stamp, debugStageRawInbound, body, nil)
	s.writeRequestDebugFacts(meta, stamp, debugStageForwarded, body, nil)
	s.writePressureDecisionDebugFacts(meta, stamp)
	s.writeUsageDebugFacts(meta, stamp, map[string]any{"input_tokens": 18000}, false)
	s.writeFullBodyDebug(meta, stamp, debugBodyStageRawInbound, body, nil, "claude-test", 0)
	s.writeFullBodyDebug(meta, stamp, debugBodyStageForwarded, body, nil, "claude-test", 0)
	s.writeFullBodyDebug(meta, stamp, debugBodyStageResponse, []byte(`{"ok":true}`), nil, "claude-test", 0)

	requestDir := filepath.Join(dataDir, "debug", meta.SessionHash, meta.RunID, "1")
	entries, err := os.ReadDir(requestDir)
	if err != nil {
		t.Fatalf("读取统一 Debug 请求目录: %v", err)
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	sort.Strings(names)
	want := []string{"forwarded.json", "forwarded_meta.json", "pressure.json", "raw.json", "raw_meta.json", "response.json", "usage.json"}
	if strings.Join(names, ",") != strings.Join(want, ",") {
		t.Fatalf("Debug stage 文件=%v，want %v", names, want)
	}
	for _, name := range []string{"raw_meta.json", "forwarded_meta.json", "pressure.json", "usage.json"} {
		data, err := os.ReadFile(filepath.Join(requestDir, name))
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(data, []byte(meta.RequestSessionID)) || bytes.Contains(data, []byte(`"session_id"`)) {
			t.Fatalf("安全 facts %s 重复保存 session 身份: %s", name, data)
		}
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

func setServerDebugConfigForTest(t *testing.T, server *Server, cfg DebugConfig) {
	t.Helper()
	layout, err := newDebugLayout(cfg.DataDir, nil)
	if err != nil {
		t.Fatalf("初始化测试 DebugLayout: %v", err)
	}
	server.Config.Debug = cfg
	server.debugLayout = layout
	server.debugRunID = layout.RunID()
}

func readDebugFactFiles(t *testing.T, dataDir, sessionID string) [][]byte {
	t.Helper()
	dir, ok := safeDebugSessionDir(dataDir, sessionID)
	if !ok {
		t.Fatal("debug dir invalid")
	}
	factNames := map[string]bool{
		"raw_meta.json":       true,
		"forwarded_meta.json": true,
		"pressure.json":       true,
		"usage.json":          true,
	}
	var paths []string
	if err := filepath.Walk(dir, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if !info.IsDir() && factNames[info.Name()] {
			paths = append(paths, path)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	sort.Strings(paths)
	files := make([][]byte, 0, len(paths))
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, data)
	}
	return files
}

// ── Plan 08 Task 1：普通日志只保留唯一最终结果 ──

// supersededOrdinarySummaries 列出被 request_outcome 取代的旧普通最终摘要。
// 它们的诊断数值仍进入 collector 与 Debug facts，但不得再作为普通日志重复出现。
var supersededOrdinarySummaries = []string{
	"pressure 摘要",
	"SawtoothTrigger 触发",
	"collapse 完成",
	"stubify+decay 完成",
	"compact 完成",
	"Archive 召回汇总",
}

// captureOrdinaryLogs 把默认 logger 换成只接收 >= Info 的普通日志缓冲。
// 缓冲里出现任何 superseded 摘要，就说明它仍然是普通日志而不是 Debug 事实。
func captureOrdinaryLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	buf := &bytes.Buffer{}
	previous := slog.Default()
	slog.SetDefault(slog.New(NewLogHandler(buf, slog.LevelInfo)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return buf
}

func assertNoSupersededOrdinarySummary(t *testing.T, output string) {
	t.Helper()
	for _, message := range supersededOrdinarySummaries {
		if strings.Contains(output, message) {
			t.Fatalf("旧普通最终摘要 %q 仍在普通日志中:\n%s", message, output)
		}
	}
}

// recallRequestMessages 在正常主请求尾部追加一条明确恢复意图的 user 消息，
// 关键词与 seedRecallArchive 播种的归档同 session 命中。
func recallRequestMessages(count, words int) []Message {
	messages := pipelineMessages(count, words)
	return append(messages, Message{
		Role:    "user",
		Content: mustMarshal("restore archive about flimflam details parser"),
	})
}

func TestRequestOutcomeIsOnlyNormalFinalSummary(t *testing.T) {
	logs := captureOrdinaryLogs(t)
	upstream := jsonOutcomeUpstream(t)
	failingUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"type":"error"}`))
	}))
	t.Cleanup(failingUpstream.Close)

	for _, tc := range []struct {
		name    string
		url     string
		prepare func(t *testing.T, server *Server, sessionID string)
		rounds  [][]Message
	}{
		{
			name:   "no_trigger",
			url:    upstream.URL,
			rounds: [][]Message{pipelineMessages(4, 3)},
		},
		{
			name:   "collapse",
			url:    upstream.URL,
			rounds: [][]Message{pipelineMessages(80, 260)},
		},
		{
			name:    "fallback",
			url:     upstream.URL,
			prepare: func(_ *testing.T, server *Server, _ string) { server.Config.Collapse.Enabled = false },
			rounds:  [][]Message{pipelineMessages(80, 260)},
		},
		{
			name:   "frozen_reuse",
			url:    upstream.URL,
			rounds: [][]Message{pipelineMessages(80, 260), pipelineMessages(80, 260)},
		},
		{
			name: "archive_recall",
			url:  upstream.URL,
			prepare: func(t *testing.T, server *Server, sessionID string) {
				seedRecallArchive(t, server.Store, sessionID)
			},
			rounds: [][]Message{recallRequestMessages(4, 3)},
		},
		{
			name:   "upstream_failure",
			url:    failingUpstream.URL,
			rounds: [][]Message{pipelineMessages(80, 260)},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logs.Reset()
			server, sink := newOutcomePipelineServer(t, tc.url)
			sessionID := "only-summary-" + tc.name
			if tc.prepare != nil {
				tc.prepare(t, server, sessionID)
			}
			for _, messages := range tc.rounds {
				serveOutcomeMessages(t, server, sessionID, messages)
			}

			snapshots := sink.all()
			if len(snapshots) != len(tc.rounds) {
				t.Fatalf("authoritative outcome 数=%d，want %d", len(snapshots), len(tc.rounds))
			}
			seen := make(map[uint64]int, len(snapshots))
			for _, snapshot := range snapshots {
				seen[snapshot.RequestID]++
				closure := formatRequestOutcomeClosure(snapshot)
				if !strings.Contains(closure, "event="+sessionOutcomeEvent) {
					t.Fatalf("最终摘要不是 request_outcome: %q", closure)
				}
			}
			for id, count := range seen {
				if count != 1 {
					t.Fatalf("request %d 的普通最终摘要数=%d，want 1", id, count)
				}
			}
			assertNoSupersededOrdinarySummary(t, logs.String())
		})
	}
}

func TestPressureDecisionFactRemains(t *testing.T) {
	logs := captureOrdinaryLogs(t)
	upstream := jsonOutcomeUpstream(t)
	server, sink := newOutcomePipelineServer(t, upstream.URL)
	dataDir := t.TempDir()
	setServerDebugConfigForTest(t, server, DebugConfig{Enabled: true, DataDir: dataDir})

	const sessionID = "pressure-fact-remains"
	serveOutcomeMessages(t, server, sessionID, pipelineMessages(80, 260))

	facts := debugFactsByStage(t, dataDir, sessionID)
	pressure, ok := facts[debugStagePressureDecision]
	if !ok {
		t.Fatalf("pressure_decision 技术事实被去噪删除: %v", facts)
	}
	snapshot := sink.sole(t)
	if pressure["request_id"] != float64(snapshot.RequestID) {
		t.Fatalf("pressure_decision request_id=%v，want %d", pressure["request_id"], snapshot.RequestID)
	}
	for _, key := range []string{
		"selected_pressure_tokens", "pressure_threshold_tokens",
		"pressure_source", "trigger_reason", "compress_decision",
	} {
		if _, ok := pressure[key]; !ok {
			t.Fatalf("pressure_decision 缺少 Phase 10 字段 %q: %v", key, pressure)
		}
	}
	assertNoSupersededOrdinarySummary(t, logs.String())
}

func TestResponseUsageFactRemains(t *testing.T) {
	logs := captureOrdinaryLogs(t)
	upstream := jsonOutcomeUpstream(t)
	server, sink := newOutcomePipelineServer(t, upstream.URL)
	dataDir := t.TempDir()
	setServerDebugConfigForTest(t, server, DebugConfig{Enabled: true, DataDir: dataDir})

	const sessionID = "usage-fact-remains"
	serveOutcomeMessages(t, server, sessionID, pipelineMessages(80, 260))

	facts := debugFactsByStage(t, dataDir, sessionID)
	usage, ok := facts[debugStageResponseUsage]
	if !ok {
		t.Fatalf("response_usage 技术事实被去噪删除: %v", facts)
	}
	snapshot := sink.sole(t)
	if usage["request_id"] != float64(snapshot.RequestID) {
		t.Fatalf("response_usage request_id=%v，want %d", usage["request_id"], snapshot.RequestID)
	}
	if _, ok := usage["total_input_tokens"]; !ok {
		t.Fatalf("response_usage 缺少 total_input_tokens: %v", usage)
	}
	if facts[debugStagePressureDecision]["request_id"] != usage["request_id"] {
		t.Fatalf("pressure/usage 不再共享同一 request ID: %v", facts)
	}
	assertNoSupersededOrdinarySummary(t, logs.String())
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

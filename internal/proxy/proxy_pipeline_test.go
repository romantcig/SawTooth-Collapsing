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

func TestPressureDecisionLocalFullIncludesTopLevelComponents(t *testing.T) {
	tokenCounter, err := NewTokenCounter()
	if err != nil {
		t.Fatalf("NewTokenCounter: %v", err)
	}
	messages := []Message{
		{Role: "user", Content: mustMarshal("hello pressure")},
		{Role: "assistant", Content: mustMarshal("measured reply")},
	}
	system := json.RawMessage(`{"b":"two","a":"one"}`)
	tools := json.RawMessage(`[{"name":"search","input_schema":{"type":"object"}}]`)

	decision := buildPressureDecision(messages, system, tools, pressureBaseline{}, tokenCounter, 1000)
	wantMessages := tokenCounter.CountMessagesTokens(messages)
	wantSystem := measureTopLevelTokens(system, tokenCounter)
	wantTools := measureTopLevelTokens(tools, tokenCounter)
	wantFull := saturatingAdd(saturatingAdd(wantMessages, wantSystem), wantTools)

	if decision.MessagesLocalTokens != wantMessages || decision.SystemLocalTokens != wantSystem || decision.ToolsLocalTokens != wantTools {
		t.Fatalf("local components=%d/%d/%d, want %d/%d/%d", decision.MessagesLocalTokens, decision.SystemLocalTokens, decision.ToolsLocalTokens, wantMessages, wantSystem, wantTools)
	}
	if decision.FullLocalEstimate != wantFull || decision.SelectedPressure != wantFull {
		t.Fatalf("full/selected=%d/%d, want %d/%d", decision.FullLocalEstimate, decision.SelectedPressure, wantFull, wantFull)
	}
	if decision.Source != pressureSourceLocalFull || decision.ResetReason != baselineResetNoActual {
		t.Fatalf("source/reset=%q/%q, want %q/%q", decision.Source, decision.ResetReason, pressureSourceLocalFull, baselineResetNoActual)
	}
	if !decision.Available || decision.MessageCount != len(messages) || decision.Threshold != 1000 {
		t.Fatalf("request metadata=%+v", decision)
	}

	orderedA := json.RawMessage(`{"a":1,"b":{"x":2,"y":3}}`)
	orderedB := json.RawMessage(`{"b":{"y":3,"x":2},"a":1}`)
	if gotA, gotB := fingerprintTopLevelJSON(orderedA), fingerprintTopLevelJSON(orderedB); gotA != gotB {
		t.Fatalf("同语义不同 key 顺序 fingerprint 不一致: %q != %q", gotA, gotB)
	}
	largeIntegerA := json.RawMessage(`{"tools":[{"input_schema":{"const":9007199254740992}}]}`)
	largeIntegerB := json.RawMessage(`{"tools":[{"input_schema":{"const":9007199254740993}}]}`)
	if gotA, gotB := fingerprintTopLevelJSON(largeIntegerA), fingerprintTopLevelJSON(largeIntegerB); gotA == gotB {
		t.Fatalf("超过 2^53 的不同整数 fingerprint 被折叠: %q", gotA)
	}
	if _, present, _ := canonicalizeTopLevelJSON(json.RawMessage(`{"a":1} {"b":2}`)); present {
		t.Fatal("canonicalization accepted multiple top-level JSON values")
	}
	missingA := fingerprintTopLevelJSON(nil)
	missingB := fingerprintTopLevelJSON(json.RawMessage{})
	nullA := fingerprintTopLevelJSON(json.RawMessage(`null`))
	nullB := fingerprintTopLevelJSON(json.RawMessage(" null "))
	if missingA != missingB || nullA != nullB || missingA == nullA {
		t.Fatalf("missing/null fingerprint 不稳定或未区分: missing=%q/%q null=%q/%q", missingA, missingB, nullA, nullB)
	}
}

func TestPressureDecisionUsesActualPlusDelta(t *testing.T) {
	tokenCounter, err := NewTokenCounter()
	if err != nil {
		t.Fatalf("NewTokenCounter: %v", err)
	}
	messages := pipelineMessages(3, 12)
	system := json.RawMessage(`[{"type":"text","text":"stable system"}]`)
	tools := json.RawMessage(`[{"name":"stable_tool","input_schema":{"type":"object"}}]`)
	baseline := pressureBaseline{
		ActualTokens:              9000,
		MessageCount:              2,
		SystemFingerprint:         fingerprintTopLevelJSON(system),
		ToolsFingerprint:          fingerprintTopLevelJSON(tools),
		MessagesPrefixFingerprint: fingerprintMessagesPrefix(messages, 2),
		Available:                 true,
		ResetReason:               baselineResetNone,
	}

	decision := buildPressureDecision(messages, system, tools, baseline, tokenCounter, 16000)
	wantDelta := tokenCounter.CountMessagesTokens(messages[2:])
	wantSelected := saturatingAdd(baseline.ActualTokens, wantDelta)
	if decision.NewMessageDelta != wantDelta || decision.SelectedPressure != wantSelected {
		t.Fatalf("delta/selected=%d/%d, want %d/%d", decision.NewMessageDelta, decision.SelectedPressure, wantDelta, wantSelected)
	}
	if decision.Source != pressureSourceActualPlusDelta || decision.ResetReason != baselineResetNone {
		t.Fatalf("source/reset=%q/%q", decision.Source, decision.ResetReason)
	}
	if decision.SelectedPressure == saturatingAdd(wantSelected, saturatingAdd(decision.SystemLocalTokens, decision.ToolsLocalTokens)) {
		t.Fatal("actual+delta 路径重复叠加 system/tools overhead")
	}
	if decision.PreviousActual != baseline.ActualTokens || decision.PreviousMessageCount != baseline.MessageCount {
		t.Fatalf("previous baseline facts=%d/%d, want %d/%d", decision.PreviousActual, decision.PreviousMessageCount, baseline.ActualTokens, baseline.MessageCount)
	}
}

func TestPressureDecisionResetsOnMessageShrink(t *testing.T) {
	tokenCounter, err := NewTokenCounter()
	if err != nil {
		t.Fatalf("NewTokenCounter: %v", err)
	}
	messages := pipelineMessages(2, 5)
	system := json.RawMessage(`"stable"`)
	tools := json.RawMessage(`[]`)
	baseline := pressureBaseline{
		ActualTokens:              20000,
		MessageCount:              3,
		SystemFingerprint:         fingerprintTopLevelJSON(system),
		ToolsFingerprint:          fingerprintTopLevelJSON(tools),
		MessagesPrefixFingerprint: strings.Repeat("a", 64),
		Available:                 true,
	}
	decision := buildPressureDecision(messages, system, tools, baseline, tokenCounter, 16000)
	if decision.Source != pressureSourceLocalFull || decision.ResetReason != baselineResetMessageShrink || decision.SelectedPressure != decision.FullLocalEstimate {
		t.Fatalf("message shrink decision=%+v", decision)
	}
}

func TestPressureDecisionResetsOnSystemChange(t *testing.T) {
	tokenCounter, err := NewTokenCounter()
	if err != nil {
		t.Fatalf("NewTokenCounter: %v", err)
	}
	messages := pipelineMessages(2, 5)
	system := json.RawMessage(`"new system"`)
	tools := json.RawMessage(`[]`)
	baseline := pressureBaseline{
		ActualTokens:              20000,
		MessageCount:              2,
		SystemFingerprint:         fingerprintTopLevelJSON(json.RawMessage(`"old system"`)),
		ToolsFingerprint:          fingerprintTopLevelJSON(tools),
		MessagesPrefixFingerprint: fingerprintMessagesPrefix(messages, 2),
		Available:                 true,
	}
	decision := buildPressureDecision(messages, system, tools, baseline, tokenCounter, 16000)
	if decision.Source != pressureSourceLocalFull || decision.ResetReason != baselineResetSystemChanged || decision.SelectedPressure != decision.FullLocalEstimate {
		t.Fatalf("system change decision=%+v", decision)
	}
	if !decision.SystemFingerprintChanged || decision.ToolsFingerprintChanged {
		t.Fatalf("system/tools changed facts=%v/%v, want true/false", decision.SystemFingerprintChanged, decision.ToolsFingerprintChanged)
	}
}

func TestPressureDecisionResetsOnToolsChange(t *testing.T) {
	tokenCounter, err := NewTokenCounter()
	if err != nil {
		t.Fatalf("NewTokenCounter: %v", err)
	}
	messages := pipelineMessages(2, 5)
	system := json.RawMessage(`"stable system"`)
	tools := json.RawMessage(`[{"name":"new_tool"}]`)
	baseline := pressureBaseline{
		ActualTokens:              20000,
		MessageCount:              2,
		SystemFingerprint:         fingerprintTopLevelJSON(system),
		ToolsFingerprint:          fingerprintTopLevelJSON(json.RawMessage(`[{"name":"old_tool"}]`)),
		MessagesPrefixFingerprint: fingerprintMessagesPrefix(messages, 2),
		Available:                 true,
	}
	decision := buildPressureDecision(messages, system, tools, baseline, tokenCounter, 16000)
	if decision.Source != pressureSourceLocalFull || decision.ResetReason != baselineResetToolsChanged || decision.SelectedPressure != decision.FullLocalEstimate {
		t.Fatalf("tools change decision=%+v", decision)
	}
	if decision.SystemFingerprintChanged || !decision.ToolsFingerprintChanged {
		t.Fatalf("system/tools changed facts=%v/%v, want false/true", decision.SystemFingerprintChanged, decision.ToolsFingerprintChanged)
	}
}

func TestPressureDecisionThresholdBehavior(t *testing.T) {
	tokenCounter, err := NewTokenCounter()
	if err != nil {
		t.Fatalf("NewTokenCounter: %v", err)
	}
	messages := pipelineMessages(4, 20)
	decision := buildPressureDecision(messages, nil, nil, pressureBaseline{}, tokenCounter, 0)
	if decision.SelectedPressure < 2 {
		t.Fatalf("fixture pressure 太小: %d", decision.SelectedPressure)
	}

	below := NewSawtoothTrigger(time.Hour, decision.SelectedPressure+1, 1)
	if got := below.ShouldTrigger("below", decision.SelectedPressure); got != TriggerNone {
		t.Fatalf("明显低于配置阈值仍触发: %q", got)
	}
	above := NewSawtoothTrigger(time.Hour, decision.SelectedPressure-1, 1)
	if got := above.ShouldTrigger("above", decision.SelectedPressure); got != TriggerTokens {
		t.Fatalf("超过配置阈值未按精确阈值触发: %q", got)
	}
	emergency := NewSawtoothTrigger(time.Hour, 1000, 100)
	if got := emergency.ShouldTrigger("emergency", 11001); got != TriggerEmergency {
		t.Fatalf("明显超限压力未触发 emergency: %q", got)
	}
	pause := NewSawtoothTrigger(0, 1000, 100)
	fingerprint := fingerprintTopLevelJSON(nil)
	pause.UpdatePressureBaseline("pause", 200, 1, fingerprint, fingerprint, strings.Repeat("a", 64))
	pause.mu.Lock()
	pause.lastRequestTime["pause"] = time.Now().Add(-time.Second)
	pause.mu.Unlock()
	if got := pause.ShouldTrigger("pause", 200); got != TriggerPause {
		t.Fatalf("选定压力超过 minimum 后未保留 pause 语义: %q", got)
	}
	if got := pause.ShouldTrigger("pause", 100); got != TriggerNone {
		t.Fatalf("选定压力未超过 minimum 却触发 pause: %q", got)
	}
}

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
		_, _ = w.Write([]byte(`{"type":"message","usage":{"input_tokens":196,"cache_creation_input_tokens":0,"cache_read_input_tokens":93056,"output_tokens":1}}`))
	}))
	defer upstream.Close()

	server := newPipelineTestServer(t, upstream.URL)
	dataDir := t.TempDir()
	server.Config.Debug = DebugConfig{Enabled: true, FullBody: false, DataDir: dataDir}
	raw := append([]Message{pipelinePersistentContextMessage(t, "DEBUG-STAGE-CLAUDE-MD-SECRET")}, pipelineMessages(4, 5)...)
	servePipelineRequest(t, server, "debug-stage-session-secret", raw)

	files := readDebugFactFiles(t, dataDir, "debug-stage-session-secret")
	if len(files) != 4 {
		t.Fatalf("facts 文件数=%d, want raw+pressure+forwarded+usage 共 4", len(files))
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
	for _, stage := range []debugStage{debugStageRawInbound, debugStagePressureDecision, debugStageForwarded, debugStageResponseUsage} {
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
		if !strings.HasSuffix(entry.Name(), "-facts.json") {
			t.Fatalf("full_body=false 仍写完整 body: %s", entry.Name())
		}
	}
}

func TestHandleMessagesDebugFullBodyOptIn(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"type":"message","usage":{"input_tokens":1,"output_tokens":1}}`))
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
	stages := make(map[debugBodyStage]bool)
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), "-facts.json") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		var bodyEntry debugEntry
		if err := json.Unmarshal(data, &bodyEntry); err != nil {
			t.Fatal(err)
		}
		if bodyEntry.RequestID == 0 || bodyEntry.Stage == "" {
			t.Fatalf("完整 debug 条目缺少 request_id/stage: %+v", bodyEntry)
		}
		stages[bodyEntry.Stage] = true
	}
	for _, stage := range []debugBodyStage{debugBodyStageRawInbound, debugBodyStageForwarded, debugBodyStageResponse} {
		if !stages[stage] {
			t.Fatalf("full_body=true 缺少 %s 正文；已有 stages=%v", stage, stages)
		}
	}
}

func TestConcurrentRequestLogsReconstructable(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"type":"message","usage":{"input_tokens":100,"output_tokens":1}}`))
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

func TestHandleMessagesSessionTitleRequestState(t *testing.T) {
	const sessionID = "SESSION-TITLE-HEADER-SECRET-8F3C1A"
	rawBody, err := os.ReadFile(filepath.Join("testdata", "auxiliary", "session-title.json"))
	if err != nil {
		t.Fatalf("读取 session title fixture 失败: %v", err)
	}

	var forwardedBody []byte
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		forwardedBody, err = io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("读取上游请求失败: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"content":[]}`))
	}))
	defer upstream.Close()

	server := newPipelineTestServer(t, upstream.URL)
	searchCalls := 0
	server.searchAndExpandFn = func(messages []Message, _ *SQLiteStore, _ int, _ *TokenCounter, _ *Budget, _ *requestMeta) RecallOutcome {
		searchCalls++
		return RecallOutcome{Messages: messages}
	}
	frozenMessages := []Message{{Role: "user", Content: mustMarshal("frozen-title-sentinel")}}
	server.Frozen.Store(sessionID, frozenMessages, 1, frozenMessages[0], 10, 20)
	frozenBefore := server.Frozen.LengthFor(sessionID)
	archivesBefore := archiveCount(t, server.Store)
	requestSeqBefore := server.Sawtooth.GetRequestSeq(sessionID)

	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelInfo})))
	t.Cleanup(func() { slog.SetDefault(previous) })

	req := httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(rawBody))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Claude-Code-Session-Id", sessionID)
	recorder := httptest.NewRecorder()
	server.HandleMessages(recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("HandleMessages status=%d body=%s", recorder.Code, recorder.Body.String())
	}

	if !bytes.Equal(forwardedBody, rawBody) {
		t.Fatalf("标题请求未原字节直通\nforwarded: %s\nraw:       %s", forwardedBody, rawBody)
	}
	if searchCalls != 0 {
		t.Fatalf("标题请求调用 Archive 搜索 %d 次", searchCalls)
	}
	if got := server.Sawtooth.GetRequestSeq(sessionID); got != requestSeqBefore {
		t.Fatalf("request sequence=%d, want unchanged %d", got, requestSeqBefore)
	}
	if got := server.Frozen.LengthFor(sessionID); got != frozenBefore {
		t.Fatalf("Frozen length=%d, want unchanged %d", got, frozenBefore)
	}
	if got := archiveCount(t, server.Store); got != archivesBefore {
		t.Fatalf("Archive rows=%d, want unchanged %d", got, archivesBefore)
	}

	gotLogs := logs.String()
	var auxiliaryLog string
	for _, line := range strings.Split(strings.TrimSpace(gotLogs), "\n") {
		if strings.Contains(line, "辅助请求安全直通") {
			if auxiliaryLog != "" {
				t.Fatalf("标题分类 Info 多于一条: %s", gotLogs)
			}
			auxiliaryLog = line
		}
	}
	if auxiliaryLog == "" {
		t.Fatalf("标题分类 Info 数量不正确: %s", gotLogs)
	}
	for _, field := range []string{"request_kind=session_title", "request_reason=title_schema", "message_count=1", "request_id="} {
		if !strings.Contains(auxiliaryLog, field) {
			t.Errorf("分类审计缺少 %q: %s", field, auxiliaryLog)
		}
	}
	for _, secret := range []string{sessionID, "Review the proxy request classifier", titleSystemPrompt, "Harmless fixture variation"} {
		if strings.Contains(auxiliaryLog, secret) {
			t.Fatalf("分类审计泄漏请求敏感值 %q: %s", secret, auxiliaryLog)
		}
	}
}

func TestSessionTitleJSONResponseState(t *testing.T) {
	testSessionTitleResponseState(t, false)
}

func TestSessionTitleSSEResponseState(t *testing.T) {
	testSessionTitleResponseState(t, true)
}

func TestSubagentJSONResponseState(t *testing.T) {
	testSubagentResponseState(t, false)
}

func TestSubagentSSEResponseState(t *testing.T) {
	testSubagentResponseState(t, true)
}

func TestForwardSawtoothStatePolicy(t *testing.T) {
	if !(*requestMeta)(nil).tracksSawtoothState() {
		t.Fatal("nil meta 必须默认跟踪 Sawtooth 状态")
	}
	if !(&requestMeta{}).tracksSawtoothState() {
		t.Fatal("零值 meta 必须默认跟踪 Sawtooth 状态")
	}
	if !(&requestMeta{AgentRole: agentRoleMain}).tracksSawtoothState() {
		t.Fatal("main meta 必须跟踪 Sawtooth 状态")
	}
	if !(&requestMeta{AgentRole: agentRoleUnknown}).tracksSawtoothState() {
		t.Fatal("unknown meta 必须按 main 跟踪 Sawtooth 状态")
	}
	if (&requestMeta{RequestKind: requestKindSessionTitle}).tracksSawtoothState() {
		t.Fatal("session_title meta 不得跟踪 Sawtooth 状态")
	}
	if (&requestMeta{AgentRole: agentRoleSubagent}).tracksSawtoothState() {
		t.Fatal("subagent meta 不得跟踪 Sawtooth 状态")
	}
}

func testSessionTitleResponseState(t *testing.T, sse bool) {
	t.Helper()
	const sessionID = "session-title-response-state"
	rawBody, err := os.ReadFile(filepath.Join("testdata", "auxiliary", "session-title.json"))
	if err != nil {
		t.Fatalf("读取 session title fixture 失败: %v", err)
	}

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if sse {
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, "event: message_start\n"+
				`data: {"type":"message_start","message":{"type":"message","usage":{"input_tokens":196,"cache_creation_input_tokens":0,"cache_read_input_tokens":93056,"output_tokens":20}}}`+"\n\n"+
				"event: message_stop\n"+`data: {"type":"message_stop"}`+"\n\n")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"type":"message","usage":{"input_tokens":196,"cache_creation_input_tokens":0,"cache_read_input_tokens":93056,"output_tokens":20}}`)
	}))
	defer upstream.Close()

	server := newPipelineTestServer(t, upstream.URL)
	server.Config.Proxy.Deflation = 0.5
	server.Config.Debug = DebugConfig{Enabled: true, DataDir: t.TempDir()}
	missingFingerprint := fingerprintTopLevelJSON(nil)
	server.Sawtooth.UpdatePressureBaseline(sessionID, 777, 9, missingFingerprint, missingFingerprint, strings.Repeat("a", 64))
	baselineBefore := server.Sawtooth.PressureBaseline(sessionID)
	requestSeqBefore := server.Sawtooth.GetRequestSeq(sessionID)
	frozenMessages := []Message{{Role: "user", Content: mustMarshal("title-response-frozen")}}
	server.Frozen.Store(sessionID, frozenMessages, 1, frozenMessages[0], 10, 20)
	frozenBefore := server.Frozen.LengthFor(sessionID)
	server.Sawtooth.mu.RLock()
	beforeTime := server.Sawtooth.lastRequestTime[sessionID]
	beforeLoaded := server.Sawtooth.loadedFromDB[sessionID]
	server.Sawtooth.mu.RUnlock()
	persistCalls := 0
	server.Sawtooth.SetPersistFunc(func(_ string, _ string) { persistCalls++ })

	req := httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(rawBody))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Claude-Code-Session-Id", sessionID)
	recorder := httptest.NewRecorder()
	server.HandleMessages(recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("HandleMessages status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	if persistCalls != 0 {
		t.Fatalf("session title persist calls=%d, want 0", persistCalls)
	}
	server.Sawtooth.mu.RLock()
	gotTokens := server.Sawtooth.lastTotalTokens[sessionID]
	gotMessages := server.Sawtooth.lastMessageCount[sessionID]
	gotTime := server.Sawtooth.lastRequestTime[sessionID]
	gotLoaded := server.Sawtooth.loadedFromDB[sessionID]
	server.Sawtooth.mu.RUnlock()
	if gotTokens != 777 || gotMessages != 9 || !gotTime.Equal(beforeTime) || gotLoaded != beforeLoaded {
		t.Fatalf("session title 改写 Sawtooth 状态: tokens=%d messages=%d time_changed=%v loaded=%v", gotTokens, gotMessages, !gotTime.Equal(beforeTime), gotLoaded)
	}
	if baselineAfter := server.Sawtooth.PressureBaseline(sessionID); baselineAfter != baselineBefore {
		t.Fatalf("session title 改写 pressure baseline\nbefore=%+v\nafter=%+v", baselineBefore, baselineAfter)
	}
	if got := server.Sawtooth.GetRequestSeq(sessionID); got != requestSeqBefore {
		t.Fatalf("session title request sequence=%d, want unchanged %d", got, requestSeqBefore)
	}
	if got := server.Frozen.LengthFor(sessionID); got != frozenBefore {
		t.Fatalf("session title Frozen length=%d, want unchanged %d", got, frozenBefore)
	}
	if strings.Contains(recorder.Body.String(), `"input_tokens":196`) || !strings.Contains(recorder.Body.String(), `"input_tokens":98`) {
		t.Fatalf("session title 客户端 deflation 行为变化: %s", recorder.Body.String())
	}

	facts := readDebugFactFiles(t, server.Config.Debug.DataDir, sessionID)
	usageFacts := 0
	for _, data := range facts {
		var fact debugFact
		if json.Unmarshal(data, &fact) == nil && fact.Stage == debugStageResponseUsage {
			usageFacts++
			if fact.TotalInputTokens != 93252 {
				t.Fatalf("usage fact total_input_tokens=%d, want 93252", fact.TotalInputTokens)
			}
		}
	}
	if usageFacts != 1 {
		t.Fatalf("response usage facts=%d, want 1", usageFacts)
	}

	ordinaryID := sessionID + "-ordinary"
	persistCalls = 0
	ordinaryBody := []byte(`{"model":"grok-4.5","messages":[{"role":"user","content":"ordinary request"}]}`)
	ordinaryReq := httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(ordinaryBody))
	ordinaryReq.Header.Set("Content-Type", "application/json")
	ordinaryReq.Header.Set("X-Claude-Code-Session-Id", ordinaryID)
	ordinaryRecorder := httptest.NewRecorder()
	server.HandleMessages(ordinaryRecorder, ordinaryReq)
	if ordinaryRecorder.Code != http.StatusOK {
		t.Fatalf("ordinary status=%d body=%s", ordinaryRecorder.Code, ordinaryRecorder.Body.String())
	}
	if persistCalls != 1 {
		t.Fatalf("ordinary persist calls=%d, want 1", persistCalls)
	}
	server.Sawtooth.mu.RLock()
	ordinaryTokens := server.Sawtooth.lastTotalTokens[ordinaryID]
	ordinaryMessages := server.Sawtooth.lastMessageCount[ordinaryID]
	server.Sawtooth.mu.RUnlock()
	if ordinaryTokens != 93252 || ordinaryMessages != 1 {
		t.Fatalf("ordinary state tokens/messages=%d/%d, want 93252/1", ordinaryTokens, ordinaryMessages)
	}
}

func testSubagentResponseState(t *testing.T, sse bool) {
	t.Helper()
	const sessionID = "subagent-response-state"
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if sse {
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, "event: message_start\n"+
				`data: {"type":"message_start","message":{"type":"message","usage":{"input_tokens":196,"cache_creation_input_tokens":0,"cache_read_input_tokens":93056,"output_tokens":20}}}`+"\n\n"+
				"event: message_stop\n"+`data: {"type":"message_stop"}`+"\n\n")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"type":"message","usage":{"input_tokens":196,"cache_creation_input_tokens":0,"cache_read_input_tokens":93056,"output_tokens":20}}`)
	}))
	defer upstream.Close()

	server := newPipelineTestServer(t, upstream.URL)
	server.Config.Proxy.Deflation = 0.5
	server.Config.Debug = DebugConfig{Enabled: true, DataDir: t.TempDir()}
	missingFingerprint := fingerprintTopLevelJSON(nil)
	server.Sawtooth.UpdatePressureBaseline(sessionID, 888, 11, missingFingerprint, missingFingerprint, strings.Repeat("a", 64))
	baselineBefore := server.Sawtooth.PressureBaseline(sessionID)
	requestSeqBefore := server.Sawtooth.GetRequestSeq(sessionID)
	frozenMessages := []Message{{Role: "user", Content: mustMarshal("subagent-response-frozen")}}
	server.Frozen.Store(sessionID, frozenMessages, 1, frozenMessages[0], 10, 20)
	frozenBefore := server.Frozen.LengthFor(sessionID)
	archivesBefore := archiveCount(t, server.Store)
	persistCalls := 0
	server.Sawtooth.SetPersistFunc(func(_ string, _ string) { persistCalls++ })

	body, err := json.Marshal(map[string]any{
		"model":    "same-model",
		"stream":   sse,
		"messages": pipelineMessages(2, 3),
	})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Claude-Code-Session-Id", sessionID)
	req.Header.Set("x-anthropic-billing-header", "cc_is_subagent=true")
	recorder := httptest.NewRecorder()
	server.HandleMessages(recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("subagent status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	if persistCalls != 0 {
		t.Fatalf("subagent persist calls=%d, want 0", persistCalls)
	}
	if baselineAfter := server.Sawtooth.PressureBaseline(sessionID); baselineAfter != baselineBefore {
		t.Fatalf("subagent 改写 pressure baseline\nbefore=%+v\nafter=%+v", baselineBefore, baselineAfter)
	}
	if got := server.Sawtooth.GetRequestSeq(sessionID); got != requestSeqBefore {
		t.Fatalf("subagent request sequence=%d, want unchanged %d", got, requestSeqBefore)
	}
	if got := server.Frozen.LengthFor(sessionID); got != frozenBefore {
		t.Fatalf("subagent Frozen length=%d, want unchanged %d", got, frozenBefore)
	}
	if got := archiveCount(t, server.Store); got != archivesBefore {
		t.Fatalf("subagent archive rows=%d, want unchanged %d", got, archivesBefore)
	}
	if strings.Contains(recorder.Body.String(), `"input_tokens":196`) || !strings.Contains(recorder.Body.String(), `"input_tokens":98`) {
		t.Fatalf("subagent 客户端 deflation 行为变化: %s", recorder.Body.String())
	}
	facts := readDebugFactFiles(t, server.Config.Debug.DataDir, sessionID)
	usageFacts := 0
	for _, data := range facts {
		var fact debugFact
		if json.Unmarshal(data, &fact) == nil && fact.Stage == debugStageResponseUsage {
			usageFacts++
			if fact.TotalInputTokens != 93252 {
				t.Fatalf("subagent usage total=%d, want 93252", fact.TotalInputTokens)
			}
		}
	}
	if usageFacts != 1 {
		t.Fatalf("subagent response usage facts=%d, want 1", usageFacts)
	}
}

func testHandleMessagesDirectAgentBypass(t *testing.T, _ ...string) {
	t.Helper()
	const sessionID = "thread-agent-subagent"
	var forwardedBodies [][]byte
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read upstream request: %v", err)
		}
		forwardedBodies = append(forwardedBodies, body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"type":"message","usage":{"input_tokens":100,"output_tokens":1}}`))
	}))
	defer upstream.Close()

	server := newPipelineTestServer(t, upstream.URL)
	missingFingerprint := fingerprintTopLevelJSON(nil)
	server.Sawtooth.UpdatePressureBaseline(sessionID, 777, 9, missingFingerprint, missingFingerprint, strings.Repeat("a", 64))
	baselineBefore := server.Sawtooth.PressureBaseline(sessionID)
	requestSeqBefore := server.Sawtooth.GetRequestSeq(sessionID)
	frozenMessages := []Message{{Role: "user", Content: mustMarshal("subagent-frozen-sentinel")}}
	server.Frozen.Store(sessionID, frozenMessages, 1, frozenMessages[0], 10, 20)
	frozenBefore := server.Frozen.LengthFor(sessionID)
	archivesBefore := archiveCount(t, server.Store)
	persistCalls := 0
	server.Sawtooth.SetPersistFunc(func(_ string, _ string) { persistCalls++ })
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
	req.Header.Set("X-Claude-Code-Session-Id", sessionID)
	req.Header.Set("x-anthropic-billing-header", "cch=12345, cc_is_subagent=true")
	recorder := httptest.NewRecorder()
	server.HandleMessages(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("HandleMessages status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	if searchCalls != 0 {
		t.Fatalf("SearchAndExpand calls = %d, want 0", searchCalls)
	}
	if got := archiveCount(t, server.Store); got != archivesBefore {
		t.Fatalf("archive rows = %d, want unchanged %d", got, archivesBefore)
	}
	if got := server.Sawtooth.GetRequestSeq(sessionID); got != requestSeqBefore {
		t.Fatalf("subagent request sequence=%d, want unchanged %d", got, requestSeqBefore)
	}
	if got := server.Frozen.LengthFor(sessionID); got != frozenBefore {
		t.Fatalf("subagent Frozen length=%d, want unchanged %d", got, frozenBefore)
	}
	if persistCalls != 0 {
		t.Fatalf("subagent response persisted pressure baseline %d times", persistCalls)
	}
	if baselineAfter := server.Sawtooth.PressureBaseline(sessionID); baselineAfter != baselineBefore {
		t.Fatalf("subagent response changed pressure baseline\nbefore=%+v\nafter=%+v", baselineBefore, baselineAfter)
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
		_, _ = w.Write([]byte(`{"type":"message","usage":{"input_tokens":100,"output_tokens":1}}`))
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
		_, _ = w.Write([]byte(`{"type":"message","usage":{"input_tokens":100}}`))
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
		_, _ = w.Write([]byte(`{"type":"message","usage":{"input_tokens":100,"output_tokens":1}}`))
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

func TestHandleMessagesCollapsedActualDoesNotCalibrateRawHistory(t *testing.T) {
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
		_, _ = w.Write([]byte(`{"type":"message","usage":{"input_tokens":100,"output_tokens":1}}`))
	}))
	defer upstream.Close()

	server := newPipelineTestServer(t, upstream.URL)
	sessionID := "collapsed-actual-raw-history"
	raw := pipelineMessages(300, 80)
	var decisions []pressureDecision
	server.searchAndExpandFn = func(messages []Message, _ *SQLiteStore, _ int, _ *TokenCounter, _ *Budget, meta *requestMeta) RecallOutcome {
		decisions = append(decisions, meta.PressureDecision)
		return RecallOutcome{Messages: messages}
	}

	servePipelineRequest(t, server, sessionID, raw)
	if baseline := server.Sawtooth.PressureBaseline(sessionID); baseline.Available {
		t.Fatalf("压缩响应的低 actual 建立了 raw baseline: %+v", baseline)
	}

	nextRaw := append(deepCopyMessages(raw), pipelineMessages(1, 20)...)
	servePipelineRequest(t, server, sessionID, nextRaw)

	if len(decisions) != 2 {
		t.Fatalf("pressure decisions=%d, want 2", len(decisions))
	}
	if decisions[1].Source != pressureSourceLocalFull || decisions[1].ResetReason != baselineResetNoActual {
		t.Fatalf("第二轮复用了压缩 actual: %+v", decisions[1])
	}
	if len(forwarded) != 2 || len(forwarded[0]) >= len(raw) || len(forwarded[1]) >= len(nextRaw) {
		t.Fatalf("两轮均应压缩 raw 历史: forwarded=%v raw=%d/%d", []int{len(forwarded[0]), len(forwarded[1])}, len(raw), len(nextRaw))
	}
}

func TestHandleMessagesPreviousUsageAboveThresholdTriggersCollapse(t *testing.T) {
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
		_, _ = w.Write([]byte(`{"type":"message","usage":{"input_tokens":100,"output_tokens":1}}`))
	}))
	defer upstream.Close()

	server := newPipelineTestServer(t, upstream.URL)
	sessionID := "previous-usage-trigger"
	var captured pressureDecision
	server.searchAndExpandFn = func(messages []Message, _ *SQLiteStore, _ int, _ *TokenCounter, _ *Budget, meta *requestMeta) RecallOutcome {
		captured = meta.PressureDecision
		return RecallOutcome{Messages: messages}
	}
	var raw []Message
	for words := 20; words <= 300; words += 10 {
		candidate := pipelineMessages(120, words)
		estimate := server.TokenCounter.CountMessagesTokens(candidate)
		if estimate >= 11000 && estimate < server.Config.Stubify.TokenThreshold {
			raw = candidate
			break
		}
	}
	if len(raw) == 0 {
		t.Fatal("未构造出低于阈值且具有可折叠历史的消息")
	}
	missingFingerprint := fingerprintTopLevelJSON(nil)
	server.Sawtooth.UpdatePressureBaseline(sessionID, server.Config.Stubify.TokenThreshold+1, len(raw), missingFingerprint, missingFingerprint, fingerprintMessagesPrefix(raw, len(raw)))

	servePipelineRequest(t, server, sessionID, raw)

	if captured.Source != pressureSourceActualPlusDelta || captured.SelectedPressure != server.Config.Stubify.TokenThreshold+1 || captured.TriggerReason != TriggerTokens || !captured.CompressDecision {
		t.Fatalf("历史 actual 未进入唯一压缩 decision: %+v", captured)
	}
	if got := archiveCount(t, server.Store); got == 0 {
		t.Fatal("上次真实 usage 已超阈值，但本次未产生 collapse archive")
	}
	if len(forwarded) >= len(raw) {
		t.Fatalf("forwarded message count=%d, want shorter than raw=%d", len(forwarded), len(raw))
	}
}

func TestHandleMessagesPreviousUsageAboveThresholdDoesNotForceCollapse(t *testing.T) {
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
		_, _ = w.Write([]byte(`{"type":"message","usage":{"input_tokens":100,"output_tokens":1}}`))
	}))
	defer upstream.Close()

	server := newPipelineTestServer(t, upstream.URL)
	sessionID := "previous-usage-short-history"
	raw := pipelineMessages(2, 3)
	missingFingerprint := fingerprintTopLevelJSON(nil)
	server.Sawtooth.UpdatePressureBaseline(sessionID, server.Config.Stubify.TokenThreshold+1, len(raw), missingFingerprint, missingFingerprint, fingerprintMessagesPrefix(raw, len(raw)))
	var captured pressureDecision
	server.searchAndExpandFn = func(messages []Message, _ *SQLiteStore, _ int, _ *TokenCounter, _ *Budget, meta *requestMeta) RecallOutcome {
		captured = meta.PressureDecision
		return RecallOutcome{Messages: messages}
	}

	servePipelineRequest(t, server, sessionID, raw)

	if captured.TriggerReason != TriggerTokens || !captured.CompressDecision || captured.Source != pressureSourceActualPlusDelta {
		t.Fatalf("短历史未进入历史 actual decision: %+v", captured)
	}
	if got := archiveCount(t, server.Store); got != 0 {
		t.Fatalf("短历史被历史 actual 无条件 Collapse，archive rows=%d", got)
	}
	if len(forwarded) != len(raw) {
		t.Fatalf("短历史 forwarded=%d, want %d", len(forwarded), len(raw))
	}
}

func TestHandleMessagesLocalFullSystemToolsTrigger(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"type":"message","usage":{"input_tokens":100,"output_tokens":1}}`))
	}))
	defer upstream.Close()

	server := newPipelineTestServer(t, upstream.URL)
	messages := pipelineMessages(2, 3)
	system := strings.Repeat("system pressure ", 6000)
	tools := []map[string]any{{
		"name":        "pressure_tool",
		"description": strings.Repeat("tool pressure ", 6000),
		"input_schema": map[string]any{
			"type": "object",
		},
	}}
	if got := server.TokenCounter.CountMessagesTokens(messages); got >= server.Config.Stubify.TokenThreshold {
		t.Fatalf("messages fixture=%d, want below threshold %d", got, server.Config.Stubify.TokenThreshold)
	}
	var captured pressureDecision
	server.searchAndExpandFn = func(current []Message, _ *SQLiteStore, _ int, _ *TokenCounter, _ *Budget, meta *requestMeta) RecallOutcome {
		captured = meta.PressureDecision
		return RecallOutcome{Messages: current}
	}

	servePipelineRequestWith(t, server, "local-full-system-tools", messages, map[string]any{"system": system, "tools": tools}, nil)

	if captured.Source != pressureSourceLocalFull || captured.ResetReason != baselineResetNoActual || captured.TriggerReason == TriggerNone || !captured.CompressDecision {
		t.Fatalf("system/tools 未驱动 local-full trigger: %+v", captured)
	}
	if captured.MessagesLocalTokens >= server.Config.Stubify.TokenThreshold || captured.SelectedPressure <= server.Config.Stubify.TokenThreshold {
		t.Fatalf("system/tools 分量未改变阈值结果: %+v", captured)
	}
}

func TestHandleMessagesActualPlusDelta(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"type":"message","usage":{"input_tokens":100,"output_tokens":1}}`))
	}))
	defer upstream.Close()

	server := newPipelineTestServer(t, upstream.URL)
	sessionID := "handle-actual-plus-delta"
	base := pipelineMessages(2, 5)
	messages := append(deepCopyMessages(base), pipelineMessages(1, 7)...)
	systemRaw := json.RawMessage(`[{"type":"text","text":"stable system"}]`)
	toolsRaw := json.RawMessage(`[{"name":"stable_tool","input_schema":{"type":"object"}}]`)
	server.Sawtooth.UpdatePressureBaseline(sessionID, 7000, len(base), fingerprintTopLevelJSON(systemRaw), fingerprintTopLevelJSON(toolsRaw), fingerprintMessagesPrefix(base, len(base)))
	var captured pressureDecision
	server.searchAndExpandFn = func(current []Message, _ *SQLiteStore, _ int, _ *TokenCounter, _ *Budget, meta *requestMeta) RecallOutcome {
		captured = meta.PressureDecision
		return RecallOutcome{Messages: current}
	}
	var system any
	var tools any
	if err := json.Unmarshal(systemRaw, &system); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(toolsRaw, &tools); err != nil {
		t.Fatal(err)
	}

	servePipelineRequestWith(t, server, sessionID, messages, map[string]any{"system": system, "tools": tools}, nil)

	wantDelta := server.TokenCounter.CountMessagesTokens(messages[len(base):])
	if captured.Source != pressureSourceActualPlusDelta || captured.NewMessageDelta != wantDelta || captured.SelectedPressure != 7000+wantDelta {
		t.Fatalf("HandleMessages actual+delta=%+v, want delta=%d selected=%d", captured, wantDelta, 7000+wantDelta)
	}
	if captured.TriggerReason != TriggerNone || captured.CompressDecision {
		t.Fatalf("低压 actual+delta 被错误触发: %+v", captured)
	}
}

func TestPressureDecisionRejectsEditedBaselinePrefix(t *testing.T) {
	tokenCounter, err := NewTokenCounter()
	if err != nil {
		t.Fatalf("NewTokenCounter: %v", err)
	}
	base := pipelineMessages(2, 5)
	fingerprint := fingerprintTopLevelJSON(nil)
	baseline := pressureBaseline{
		ActualTokens:              7000,
		MessageCount:              len(base),
		SystemFingerprint:         fingerprint,
		ToolsFingerprint:          fingerprint,
		MessagesPrefixFingerprint: fingerprintMessagesPrefix(base, len(base)),
		Available:                 true,
	}

	tests := []struct {
		name     string
		messages []Message
	}{
		{name: "same length edit", messages: deepCopyMessages(base)},
		{name: "growth with old prefix edit", messages: append(deepCopyMessages(base), pipelineMessages(1, 3)...)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.messages[0].Content = mustMarshal(strings.Repeat("edited historical pressure ", 2000))
			decision := buildPressureDecision(tc.messages, nil, nil, baseline, tokenCounter, 100_000)
			if decision.Source != pressureSourceLocalFull || decision.ResetReason != baselineResetMessagesChanged {
				t.Fatalf("edited prefix decision=%+v", decision)
			}
			if decision.SelectedPressure != decision.FullLocalEstimate || decision.NewMessageDelta != 0 {
				t.Fatalf("edited prefix reused actual: %+v", decision)
			}
		})
	}
}

func TestHandleMessagesEditedPrefixFailureRetryStaysLocalFull(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte(`{"type":"error","error":{"type":"upstream_error"}}`))
	}))
	defer upstream.Close()

	server := newPipelineTestServer(t, upstream.URL)
	sessionID := "edited-prefix-failure-retry"
	base := pipelineMessages(2, 5)
	edited := deepCopyMessages(base)
	edited[0].Content = mustMarshal(strings.Repeat("edited historical pressure ", 2000))
	fingerprint := fingerprintTopLevelJSON(nil)
	server.Sawtooth.UpdatePressureBaseline(sessionID, 7000, len(base), fingerprint, fingerprint, fingerprintMessagesPrefix(base, len(base)))
	var decisions []pressureDecision
	server.searchAndExpandFn = func(current []Message, _ *SQLiteStore, _ int, _ *TokenCounter, _ *Budget, meta *requestMeta) RecallOutcome {
		decisions = append(decisions, meta.PressureDecision)
		return RecallOutcome{Messages: current}
	}

	serveFailure := func() {
		requestBody, err := json.Marshal(map[string]any{
			"model": "deepseek-v4-pro", "thinking": map[string]any{"type": "enabled"}, "messages": edited,
		})
		if err != nil {
			t.Fatal(err)
		}
		req := httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(requestBody))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Claude-Code-Session-Id", sessionID)
		recorder := httptest.NewRecorder()
		server.HandleMessages(recorder, req)
		if recorder.Code != http.StatusBadGateway {
			t.Fatalf("failure status=%d, want %d", recorder.Code, http.StatusBadGateway)
		}
	}
	serveFailure()
	serveFailure()

	if len(decisions) != 2 {
		t.Fatalf("captured decisions=%d, want 2", len(decisions))
	}
	for index, decision := range decisions {
		if decision.Source != pressureSourceLocalFull || decision.ResetReason != baselineResetMessagesChanged {
			t.Fatalf("retry %d reused stale actual: %+v", index+1, decision)
		}
	}
}

func TestHandleMessagesPressureBaselineReset(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"type":"message","usage":{"input_tokens":100,"output_tokens":1}}`))
	}))
	defer upstream.Close()

	tests := []struct {
		name          string
		messages      []Message
		baselineCount int
		baselineSys   json.RawMessage
		baselineTools json.RawMessage
		currentSys    json.RawMessage
		currentTools  json.RawMessage
		wantReason    baselineResetReason
	}{
		{
			name: "message shrink", messages: pipelineMessages(2, 4), baselineCount: 3,
			baselineSys: json.RawMessage(`"stable"`), baselineTools: json.RawMessage(`[]`),
			currentSys: json.RawMessage(`"stable"`), currentTools: json.RawMessage(`[]`),
			wantReason: baselineResetMessageShrink,
		},
		{
			name: "system changed", messages: pipelineMessages(2, 4), baselineCount: 2,
			baselineSys: json.RawMessage(`"old"`), baselineTools: json.RawMessage(`[]`),
			currentSys: json.RawMessage(`"new"`), currentTools: json.RawMessage(`[]`),
			wantReason: baselineResetSystemChanged,
		},
		{
			name: "tools changed", messages: pipelineMessages(2, 4), baselineCount: 2,
			baselineSys: json.RawMessage(`"stable"`), baselineTools: json.RawMessage(`[{"name":"old"}]`),
			currentSys: json.RawMessage(`"stable"`), currentTools: json.RawMessage(`[{"name":"new"}]`),
			wantReason: baselineResetToolsChanged,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server := newPipelineTestServer(t, upstream.URL)
			sessionID := "baseline-reset-" + strings.ReplaceAll(tc.name, " ", "-")
			prefixFingerprint := strings.Repeat("a", 64)
			if tc.baselineCount <= len(tc.messages) {
				prefixFingerprint = fingerprintMessagesPrefix(tc.messages, tc.baselineCount)
			}
			server.Sawtooth.UpdatePressureBaseline(sessionID, 20000, tc.baselineCount, fingerprintTopLevelJSON(tc.baselineSys), fingerprintTopLevelJSON(tc.baselineTools), prefixFingerprint)
			var captured pressureDecision
			server.searchAndExpandFn = func(current []Message, _ *SQLiteStore, _ int, _ *TokenCounter, _ *Budget, meta *requestMeta) RecallOutcome {
				captured = meta.PressureDecision
				return RecallOutcome{Messages: current}
			}
			var system any
			var tools any
			if err := json.Unmarshal(tc.currentSys, &system); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(tc.currentTools, &tools); err != nil {
				t.Fatal(err)
			}

			servePipelineRequestWith(t, server, sessionID, tc.messages, map[string]any{"system": system, "tools": tools}, nil)

			if captured.Source != pressureSourceLocalFull || captured.ResetReason != tc.wantReason || captured.SelectedPressure != captured.FullLocalEstimate {
				t.Fatalf("reset decision=%+v, want reason=%q", captured, tc.wantReason)
			}
			if captured.TriggerReason != TriggerNone || captured.CompressDecision {
				t.Fatalf("失效旧 actual 被重新引入 trigger: %+v", captured)
			}
		})
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
		_, _ = w.Write([]byte(`{"type":"message","usage":{"input_tokens":100,"output_tokens":1}}`))
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
	if got := len(forwarded[1]); got <= len(tail) || got >= len(secondRaw) {
		t.Fatalf("context 坐标变化后的重压缩消息数=%d, want 保留历史且小于原始 %d", got, len(secondRaw))
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
		if got := countMessagesContaining(forwarded[1], fmt.Sprintf("fresh-tail-%d", i)); got != 1 {
			t.Fatalf("fresh tail %d count=%d, want 1", i, got)
		}
	}
	if got := archiveCount(t, server.Store); got != archivesAfterFreeze+1 {
		t.Fatalf("context 坐标变化重压缩后的 archive rows=%d, want %d", got, archivesAfterFreeze+1)
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
		_, _ = w.Write([]byte(`{"type":"message","usage":{"input_tokens":20000,"output_tokens":1}}`))
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
				_, _ = w.Write([]byte(`{"type":"message","usage":{"input_tokens":100}}`))
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
				_, _ = w.Write([]byte(`{"type":"message","usage":{"input_tokens":100,"output_tokens":1}}`))
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

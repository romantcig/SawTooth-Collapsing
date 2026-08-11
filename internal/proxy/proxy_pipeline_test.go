package proxy

import (
	"bytes"
	"encoding/json"
	"errors"
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

func TestFingerprintMessagesPrefixNormalizesEquivalentTextWireForms(t *testing.T) {
	var listForm, stringForm []Message
	if err := json.Unmarshal([]byte(`[{"role":"user","content":[{"type":"text","text":"same text","cache_control":{"type":"ephemeral"}}]}]`), &listForm); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(`[{"role":"user","content":"same text"}]`), &stringForm); err != nil {
		t.Fatal(err)
	}
	if got, want := fingerprintMessagesPrefix(listForm, 1), fingerprintMessagesPrefix(stringForm, 1); got != want {
		t.Fatalf("等价 text wire form 指纹不同: list=%s string=%s", got, want)
	}

	thinkingA := []Message{{Role: "assistant", Content: json.RawMessage(`[{"type":"thinking","thinking":"reasoning","signature":"signed-a"}]`)}}
	thinkingB := []Message{{Role: "assistant", Content: json.RawMessage(`[{"type":"thinking","thinking":"reasoning","signature":"signed-b"}]`)}}
	if fingerprintMessagesPrefix(thinkingA, 1) == fingerprintMessagesPrefix(thinkingB, 1) {
		t.Fatal("thinking signature 变化被错误忽略")
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

func TestPressureDecisionUsesLegacyHighWaterAboveThreshold(t *testing.T) {
	tokenCounter, err := NewTokenCounter()
	if err != nil {
		t.Fatalf("NewTokenCounter: %v", err)
	}
	messages := pipelineMessages(32, 8)
	baseline := pressureBaseline{
		ActualTokens: 194_383,
		MessageCount: 32,
		Available:    false,
		ResetReason:  baselineResetNoActual,
	}
	decision := buildPressureDecision(messages, nil, nil, baseline, tokenCounter, 150_000)
	if decision.FullLocalEstimate >= 150_000 {
		t.Fatalf("fixture local_full=%d，必须低于阈值", decision.FullLocalEstimate)
	}
	if decision.SelectedPressure != 194_383 || decision.Source != pressureSourceLegacyHighWater || decision.ResetReason != baselineResetLegacyUnverified {
		t.Fatalf("legacy high-water decision=%+v", decision)
	}

	shrunk := buildPressureDecision(messages[:31], nil, nil, baseline, tokenCounter, 150_000)
	if shrunk.Source != pressureSourceLocalFull || shrunk.SelectedPressure != shrunk.FullLocalEstimate {
		t.Fatalf("消息缩短后仍使用未验证 legacy high-water: %+v", shrunk)
	}
}

func TestPressureDecisionIgnoresLegacyConservativeBaseline(t *testing.T) {
	tokenCounter, err := NewTokenCounter()
	if err != nil {
		t.Fatalf("NewTokenCounter: %v", err)
	}
	messages := pipelineMessages(4, 200)
	system := json.RawMessage(`"stable system"`)
	tools := json.RawMessage(`[]`)
	baseline := pressureBaseline{
		ActualTokens:              1,
		MessageCount:              2,
		SystemFingerprint:         fingerprintTopLevelJSON(system),
		ToolsFingerprint:          fingerprintTopLevelJSON(tools),
		MessagesPrefixFingerprint: fingerprintMessagesPrefix(messages, 2),
		Conservative:              true,
		Available:                 true,
		ResetReason:               baselineResetNone,
	}
	decision := buildPressureDecision(messages, system, tools, baseline, tokenCounter, 16_000)
	if decision.Source != pressureSourceLocalFull || decision.ResetReason != baselineResetLegacyUnverified || decision.SelectedPressure != decision.FullLocalEstimate {
		t.Fatalf("旧 conservative baseline 未回退到 local_full: %+v", decision)
	}

	baseline.ActualTokens = 190_000
	high := buildPressureDecision(messages, system, tools, baseline, tokenCounter, 150_000)
	if high.Source != pressureSourceLocalFull || high.ResetReason != baselineResetLegacyUnverified || high.SelectedPressure != high.FullLocalEstimate {
		t.Fatalf("高 conservative baseline 仍抬高 pressure: %+v", high)
	}
	baseline.SystemFingerprint = ""
	baseline.ToolsFingerprint = ""
	baseline.MessagesPrefixFingerprint = ""
	invalidCoordinates := buildPressureDecision(messages, system, tools, baseline, tokenCounter, 150_000)
	if invalidCoordinates.Source != pressureSourceLocalFull || invalidCoordinates.ResetReason != baselineResetLegacyUnverified || invalidCoordinates.SelectedPressure != invalidCoordinates.FullLocalEstimate {
		t.Fatalf("缺少坐标的旧 conservative baseline 仍进入 legacy high-water: %+v", invalidCoordinates)
	}
}

func TestPressureDecisionMessageEditKeepsObservedHighWater(t *testing.T) {
	tokenCounter, err := NewTokenCounter()
	if err != nil {
		t.Fatalf("NewTokenCounter: %v", err)
	}
	base := pipelineMessages(3, 8)
	edited := deepCopyMessages(base)
	edited[1].Content = mustMarshal("edited but still the same growing conversation")
	system := json.RawMessage(`"stable system"`)
	tools := json.RawMessage(`[]`)
	baseline := pressureBaseline{
		ActualTokens:              189_730,
		MessageCount:              len(base),
		SystemFingerprint:         fingerprintTopLevelJSON(system),
		ToolsFingerprint:          fingerprintTopLevelJSON(tools),
		MessagesPrefixFingerprint: fingerprintMessagesPrefix(base, len(base)),
		// 这是精确 baseline 的消息编辑场景；旧 conservative baseline
		// 的高水位不再参与 pressure（由上一测试覆盖）。
		Conservative: false,
		Available:    true,
	}

	decision := buildPressureDecision(edited, system, tools, baseline, tokenCounter, 150_000)
	if decision.ResetReason != baselineResetMessagesChanged || decision.Source != pressureSourceConservativeHighWater || decision.SelectedPressure != baseline.ActualTokens {
		t.Fatalf("消息编辑后未保留 observed high-water: %+v", decision)
	}
	if decision.SelectedPressure <= decision.FullLocalEstimate {
		t.Fatalf("high-water 未抬高低估的 local_full: %+v", decision)
	}

	shrunk := buildPressureDecision(edited[:2], system, tools, baseline, tokenCounter, 150_000)
	if shrunk.Source != pressureSourceLocalFull || shrunk.ResetReason != baselineResetMessageShrink || shrunk.SelectedPressure != shrunk.FullLocalEstimate {
		t.Fatalf("消息缩短后仍保留旧 high-water: %+v", shrunk)
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

// TestPressureActualPlusDeltaLifecycle 是 actual_plus_delta 路径的唯一护栏。
//
// 它取代原先的两条测试：一条断言"主动 tool_result 摘要机制改写后的 wire 形状"，
// 该机制已随本次改动移除，其覆盖（压力用最终 wire 形状、低于阈值不压缩、透明
// 转发）由本测试承接；另一条是原两轮 lifecycle 护栏，其全部第二轮断言在此
// 逐条保留。
//
// 生产 trace（FIX_PLAN.md 1.3）里 actual_plus_delta 出现 **0 次**：151 次 pressure
// 决策中 123 次退回 local_full、28 次 conservative_plus_delta。根因是主管线每轮
// 改写已经发送过的历史正文，令 baseline 前缀指纹逐轮失配。改动后主管线不再改写
// 任何已发送区间，纯追加会话的前缀指纹应连续匹配。
//
// 因此轮次从 2 轮扩到 4 轮：第一轮冷启动写下 exact baseline，第 2/3/4 轮必须
// **全部**落在 actual_plus_delta——这是"指纹不再逐轮失配"的自动化证据。
// 四轮都必须 compress=false 且不产生 Archive。
func TestPressureActualPlusDeltaLifecycle(t *testing.T) {
	const sessionID = "pressure-actual-plus-delta-lifecycle"
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
		_, _ = w.Write([]byte(`{"type":"message","usage":{"input_tokens":1200,"output_tokens":1}}`))
	}))
	defer upstream.Close()

	server := newPipelineTestServer(t, upstream.URL)
	toolUse := Message{Role: "assistant", Content: json.RawMessage(mustMarshalJSON(t, []map[string]any{{
		"type": "tool_use", "id": "tool-history-read", "name": "Read",
		"input": map[string]any{"file_path": "history.go"},
	}}))}
	toolResult := Message{Role: "user", Content: json.RawMessage(mustMarshalJSON(t, []map[string]any{{
		"type": "tool_result", "tool_use_id": "tool-history-read",
		"content": strings.Repeat("historical tool output line\n", 20),
	}}))}
	firstRound := []Message{
		{Role: "user", Content: mustMarshal("inspect the history file")},
		toolUse,
		toolResult,
		{Role: "assistant", Content: mustMarshal("I processed the file contents")},
		{Role: "user", Content: mustMarshal("continue with the result")},
	}
	// 前提：raw 历史本身就低于阈值。没有任何环节会替测试把正文压小，fixture
	// 必须自己保证不触发压缩，否则本测试测的是 collapse 而不是精确 delta。
	if raw := server.TokenCounter.CountMessagesTokens(firstRound); raw >= server.Config.Stubify.TokenThreshold {
		t.Fatalf("fixture raw tokens=%d, want well below threshold=%d", raw, server.Config.Stubify.TokenThreshold)
	}

	var decisions []pressureDecision
	server.searchAndExpandFn = func(current []Message, _ *SQLiteStore, _ int, _ *TokenCounter, _ *Budget, meta *requestMeta) RecallOutcome {
		decisions = append(decisions, meta.PressureDecision)
		return RecallOutcome{Messages: current}
	}

	// ── 第一轮：冷启动 local_full，不压缩，成功响应写入 exact baseline ──
	servePipelineRequest(t, server, sessionID, firstRound)
	if len(decisions) != 1 {
		t.Fatalf("第一轮 pressure 决策数=%d, want 1", len(decisions))
	}
	if decisions[0].Source != pressureSourceLocalFull || decisions[0].CompressDecision {
		t.Fatalf("第一轮应为冷启动 local_full 且不压缩: %+v", decisions[0])
	}
	if got := archiveCount(t, server.Store); got != 0 {
		t.Fatalf("第一轮低于阈值却产生 collapse archive: %d", got)
	}
	if len(forwarded) != 1 || len(forwarded[0]) != len(firstRound) {
		t.Fatalf("第一轮未透明转发: rounds=%d count=%d want=%d",
			len(forwarded), len(forwarded[len(forwarded)-1]), len(firstRound))
	}
	baseline := server.Sawtooth.PressureBaseline(sessionID)
	if !baseline.Available || baseline.Conservative || baseline.ActualTokens != 1200 {
		t.Fatalf("第一轮未写入 exact baseline: %+v", baseline)
	}
	if baseline.MessageCount != len(forwarded[0]) {
		t.Fatalf("baseline 未绑定第一轮 forwarded 坐标: baseline=%d forwarded=%d",
			baseline.MessageCount, len(forwarded[0]))
	}

	// ── 第 2/3/4 轮：每轮只追加两条短消息，前缀逐字节不变 ──
	round := deepCopyMessages(firstRound)
	for n := 2; n <= 4; n++ {
		round = append(round,
			Message{Role: "assistant", Content: mustMarshal(fmt.Sprintf("here is the follow-up analysis of round %d", n))},
			Message{Role: "user", Content: mustMarshal(fmt.Sprintf("now summarize what changed in round %d", n))},
		)
		servePipelineRequest(t, server, sessionID, round)
		if len(decisions) != n {
			t.Fatalf("第 %d 轮 pressure 决策数=%d, want %d", n, len(decisions), n)
		}
		current := decisions[n-1]

		// 核心断言：本轮走的是精确 delta，而不是退回 local_full 或高水位。
		if current.Source != pressureSourceActualPlusDelta {
			t.Fatalf("第 %d 轮 pressure_source=%q, want %q（reset_reason=%q）",
				n, current.Source, pressureSourceActualPlusDelta, current.ResetReason)
		}
		if current.ResetReason != baselineResetNone {
			t.Fatalf("第 %d 轮 baseline_reset_reason=%q, want %q", n, current.ResetReason, baselineResetNone)
		}
		if current.PreviousActual != 1200 {
			t.Fatalf("第 %d 轮 previous_actual=%d, want 1200", n, current.PreviousActual)
		}
		if current.NewMessageDelta <= 0 {
			t.Fatalf("第 %d 轮 new_message_delta=%d, want >0（追加的 tail 未计入）", n, current.NewMessageDelta)
		}
		if current.SelectedPressure != current.PreviousActual+current.NewMessageDelta {
			t.Fatalf("第 %d 轮 selected=%d != previous_actual(%d)+delta(%d)",
				n, current.SelectedPressure, current.PreviousActual, current.NewMessageDelta)
		}
		// 旧 baseline 不得把 pressure 拉回重型压缩区间。
		if current.CompressDecision || current.TriggerReason != TriggerNone {
			t.Fatalf("第 %d 轮被旧 baseline 拉回压缩: %+v", n, current)
		}
		if got := archiveCount(t, server.Store); got != 0 {
			t.Fatalf("第 %d 轮产生 collapse archive: %d", n, got)
		}
		if server.Frozen.LengthFor(sessionID) != 0 {
			t.Fatalf("第 %d 轮写入 Frozen prefix: %d", n, server.Frozen.LengthFor(sessionID))
		}
		if got := forwarded[n-1]; len(got) != len(round) {
			t.Fatalf("第 %d 轮未透明转发: forwarded=%d want=%d", n, len(got), len(round))
		}
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
	setServerDebugConfigForTest(t, server, DebugConfig{Enabled: true, FullBody: false, DataDir: dataDir})
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

	requestDir := filepath.Join(dataDir, "debug", stableSessionHash("debug-stage-session-secret"), server.debugRunID, "1")
	for _, name := range []string{"raw.json", "forwarded.json", "response.json"} {
		if _, err := os.Stat(filepath.Join(requestDir, name)); !os.IsNotExist(err) {
			t.Fatalf("full_body=false 仍写完整 body %s: %v", name, err)
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
	setServerDebugConfigForTest(t, server, DebugConfig{Enabled: true, FullBody: true, DataDir: dataDir})
	servePipelineRequest(t, server, "debug-full-body-session", pipelineMessages(2, 2))

	requestDir := filepath.Join(dataDir, "debug", stableSessionHash("debug-full-body-session"), server.debugRunID, "1")
	entries, err := os.ReadDir(requestDir)
	if err != nil {
		t.Fatal(err)
	}
	stages := make(map[debugBodyStage]bool)
	for _, entry := range entries {
		if entry.Name() != "raw.json" && entry.Name() != "forwarded.json" && entry.Name() != "response.json" {
			continue
		}
		data, err := os.ReadFile(filepath.Join(requestDir, entry.Name()))
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
	setServerDebugConfigForTest(t, server, DebugConfig{Enabled: true, DataDir: t.TempDir()})
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
	setServerDebugConfigForTest(t, server, DebugConfig{Enabled: true, DataDir: t.TempDir()})
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
	req.Header.Set("X-Claude-Code-Agent-Id", "ae62648d28a17ee1a")
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
	req.Header.Set("X-Claude-Code-Agent-Id", "ac3d8abb21a98f939")
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
	if baseline := server.Sawtooth.PressureBaseline(sessionID); !baseline.Available || baseline.Conservative || baseline.ActualTokens != 100 || baseline.MessageCount != len(forwarded[0]) || baseline.MessagesPrefixFingerprint != fingerprintMessagesPrefix(forwarded[0], len(forwarded[0])) {
		t.Fatalf("压缩响应未按最终 forwarded 坐标写入 exact baseline: %+v", baseline)
	}

	nextRaw := append(deepCopyMessages(raw), pipelineMessages(1, 20)...)
	servePipelineRequest(t, server, sessionID, nextRaw)

	if len(decisions) != 2 {
		t.Fatalf("pressure decisions=%d, want 2", len(decisions))
	}
	if decisions[1].Source != pressureSourceActualPlusDelta || decisions[1].ResetReason != baselineResetNone || decisions[1].SelectedPressure >= decisions[1].Threshold || decisions[1].CompressDecision {
		t.Fatalf("第二轮未按 exact baseline + tail delta 决策: %+v", decisions[1])
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
	if got := archiveCount(t, server.Store); got != archivesAfterFreeze {
		t.Fatalf("Frozen 命中且有效压力未超阈值时不应重复 archive: rows=%d, want %d", got, archivesAfterFreeze)
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
	store, err := NewSQLiteStore(filepath.Join(tempDirRetryCleanup(t), "pipeline.db"))
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

// ── Plan 06 Task 1：request / upstream / persistence outcome 生命周期 ──

// recordingOutcomeDispatcher 记录每个请求最终 dispatch 的 immutable snapshot。
// 它不做任何投影，只证明「每个 return 都恰好汇合一次真实终态」。
type recordingOutcomeDispatcher struct {
	mu        sync.Mutex
	snapshots []requestOutcomeSnapshot
	admission outcomeDispatchResult
}

func (d *recordingOutcomeDispatcher) TryDispatch(snapshot requestOutcomeSnapshot) outcomeDispatchResult {
	d.mu.Lock()
	d.snapshots = append(d.snapshots, snapshot)
	result := d.admission
	d.mu.Unlock()
	result.Accepted = true
	return result
}

func (d *recordingOutcomeDispatcher) all() []requestOutcomeSnapshot {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]requestOutcomeSnapshot(nil), d.snapshots...)
}

func (d *recordingOutcomeDispatcher) count() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.snapshots)
}

func (d *recordingOutcomeDispatcher) sole(t *testing.T) requestOutcomeSnapshot {
	t.Helper()
	got := d.all()
	if len(got) != 1 {
		t.Fatalf("dispatch 次数=%d, want 1: %+v", len(got), got)
	}
	return got[0]
}

func (d *recordingOutcomeDispatcher) waitFor(t *testing.T, want int) []requestOutcomeSnapshot {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if d.count() >= want {
			return d.all()
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("等待 %d 次 dispatch 超时，实际 %d", want, d.count())
	return nil
}

// blockingStateBackend 是不依赖具体 state key 的可阻塞/可失败 backend。
type blockingStateBackend struct {
	mu      sync.Mutex
	gate    chan struct{}
	started chan struct{}
	once    sync.Once
	calls   int
	failAll bool
}

func newBlockingStateBackend() *blockingStateBackend {
	return &blockingStateBackend{started: make(chan struct{})}
}

func (b *blockingStateBackend) block() {
	b.mu.Lock()
	if b.gate == nil {
		b.gate = make(chan struct{})
	}
	b.mu.Unlock()
}

func (b *blockingStateBackend) release() {
	b.mu.Lock()
	gate := b.gate
	b.gate = nil
	b.mu.Unlock()
	if gate != nil {
		close(gate)
	}
}

func (b *blockingStateBackend) callCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.calls
}

func (b *blockingStateBackend) run() error {
	b.mu.Lock()
	b.calls++
	gate := b.gate
	shouldFail := b.failAll
	b.mu.Unlock()
	if gate != nil {
		b.once.Do(func() { close(b.started) })
		<-gate
	}
	if shouldFail {
		return errors.New("forced state backend failure")
	}
	return nil
}

func (b *blockingStateBackend) PersistState(string, string) error { return b.run() }

func (b *blockingStateBackend) DeleteState(string) error { return b.run() }

func newOutcomePipelineServer(t *testing.T, upstreamURL string) (*Server, *recordingOutcomeDispatcher) {
	t.Helper()
	server := newPipelineTestServer(t, upstreamURL)
	sink := &recordingOutcomeDispatcher{}
	server.outcomeSink = sink
	return server, sink
}

// jsonOutcomeUpstream 返回一个稳定的 2xx JSON 上游，带合法 Anthropic usage。
func jsonOutcomeUpstream(t *testing.T) *httptest.Server {
	t.Helper()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"type":"message","usage":{"input_tokens":1200,"output_tokens":7}}`))
	}))
	t.Cleanup(upstream.Close)
	return upstream
}

func serveOutcomeBody(t *testing.T, server *Server, sessionID string, body []byte, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Claude-Code-Session-Id", sessionID)
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	recorder := httptest.NewRecorder()
	server.HandleMessages(recorder, req)
	return recorder
}

func serveOutcomeMessages(t *testing.T, server *Server, sessionID string, messages []Message) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(map[string]any{"model": "deepseek-v4-pro", "messages": messages})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	return serveOutcomeBody(t, server, sessionID, body, nil)
}

func assertOutcome(t *testing.T, snapshot requestOutcomeSnapshot, eligibility, terminal outcomeEligibility, action outcomeAction, upstream upstreamState) {
	t.Helper()
	if snapshot.Eligibility != eligibility {
		t.Errorf("eligibility=%s, want %s", snapshot.Eligibility, eligibility)
	}
	if snapshot.TerminalEligibility != terminal {
		t.Errorf("terminal_eligibility=%s, want %s", snapshot.TerminalEligibility, terminal)
	}
	if snapshot.Action != action {
		t.Errorf("action=%s, want %s", snapshot.Action, action)
	}
	if snapshot.UpstreamState != upstream {
		t.Errorf("upstream_state=%s, want %s", snapshot.UpstreamState, upstream)
	}
	if snapshot.FinishedAt.IsZero() {
		t.Error("finalized snapshot 缺少 finished_at")
	}
}

func TestRequestOutcomeAllEarlyReturns(t *testing.T) {
	upstream := jsonOutcomeUpstream(t)
	titleBody, err := os.ReadFile(filepath.Join("testdata", "auxiliary", "session-title.json"))
	if err != nil {
		t.Fatalf("读取 session title fixture: %v", err)
	}
	normalBody, err := json.Marshal(map[string]any{"model": "m", "messages": pipelineMessages(4, 3)})
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name        string
		body        []byte
		headers     map[string]string
		disable     bool
		eligibility outcomeEligibility
		terminal    outcomeEligibility
		action      outcomeAction
		upstream    upstreamState
	}{
		{
			name: "stateless", body: normalBody, disable: true,
			eligibility: outcomeEligibilityNotEvaluable, terminal: outcomeEligibilityNotApplicable,
			action: outcomeActionPassthrough, upstream: upstreamStateSuccess,
		},
		{
			name: "invalid_json", body: []byte(`{"model":`),
			eligibility: outcomeEligibilityNotEvaluable, terminal: outcomeEligibilityNotApplicable,
			action: outcomeActionPassthrough, upstream: upstreamStateSuccess,
		},
		{
			name: "missing_messages", body: []byte(`{"model":"m"}`),
			eligibility: outcomeEligibilityNotEvaluable, terminal: outcomeEligibilityNotApplicable,
			action: outcomeActionPassthrough, upstream: upstreamStateSuccess,
		},
		{
			name: "malformed_messages", body: []byte(`{"model":"m","messages":{"role":"user"}}`),
			eligibility: outcomeEligibilityNotEvaluable, terminal: outcomeEligibilityNotApplicable,
			action: outcomeActionPassthrough, upstream: upstreamStateSuccess,
		},
		{
			name: "session_title", body: titleBody,
			eligibility: outcomeEligibilityNotEvaluable, terminal: outcomeEligibilityTerminalIneligible,
			action: outcomeActionPassthrough, upstream: upstreamStateSuccess,
		},
		{
			name: "subagent", body: normalBody, headers: map[string]string{"X-Claude-Code-Agent-Id": "agent-1"},
			eligibility: outcomeEligibilityNotEvaluable, terminal: outcomeEligibilityTerminalIneligible,
			action: outcomeActionPassthrough, upstream: upstreamStateSuccess,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server, sink := newOutcomePipelineServer(t, upstream.URL)
			if tc.disable {
				server.TokenCounter = nil
				server.DecayTracker = nil
			}
			serveOutcomeBody(t, server, "outcome-early-"+tc.name, tc.body, tc.headers)
			assertOutcome(t, sink.sole(t), tc.eligibility, tc.terminal, tc.action, tc.upstream)
		})
	}

	t.Run("oversized_body", func(t *testing.T) {
		server, sink := newOutcomePipelineServer(t, upstream.URL)
		oversized := make([]byte, 10*1024*1024+16)
		for index := range oversized {
			oversized[index] = 'x'
		}
		recorder := serveOutcomeBody(t, server, "outcome-oversized", oversized, nil)
		if recorder.Code != http.StatusRequestEntityTooLarge {
			t.Fatalf("status=%d, want 413", recorder.Code)
		}
		snapshot := sink.sole(t)
		assertOutcome(t, snapshot, outcomeEligibilityNotEvaluable, outcomeEligibilityNotApplicable,
			outcomeActionUnavailable, upstreamStateNotStarted)
		if snapshot.Intervention != interventionRequired {
			t.Fatalf("intervention=%s, want required", snapshot.Intervention)
		}
	})
}

func TestRequestOutcomeActionMatrix(t *testing.T) {
	upstream := jsonOutcomeUpstream(t)

	t.Run("direct", func(t *testing.T) {
		server, sink := newOutcomePipelineServer(t, upstream.URL)
		serveOutcomeMessages(t, server, "outcome-direct", pipelineMessages(4, 3))
		snapshot := sink.sole(t)
		assertOutcome(t, snapshot, outcomeEligibilityEvaluable, outcomeEligibilityTerminalEligible,
			outcomeActionDirect, upstreamStateSuccess)
		if snapshot.TriggerReason != TriggerNone {
			t.Fatalf("trigger=%s, want none", snapshot.TriggerReason)
		}
		if snapshot.BeforeMessages == 0 || snapshot.AfterMessages == 0 {
			t.Fatalf("before/after messages=%d/%d, want 非零", snapshot.BeforeMessages, snapshot.AfterMessages)
		}
	})

	t.Run("collapse", func(t *testing.T) {
		server, sink := newOutcomePipelineServer(t, upstream.URL)
		serveOutcomeMessages(t, server, "outcome-collapse", pipelineMessages(80, 260))
		snapshot := sink.sole(t)
		assertOutcome(t, snapshot, outcomeEligibilityEvaluable, outcomeEligibilityTerminalEligible,
			outcomeActionCollapse, upstreamStateSuccess)
		if snapshot.TriggerReason == TriggerNone || snapshot.TriggerReason == TriggerUnknown {
			t.Fatalf("trigger=%s, want 明确触发原因", snapshot.TriggerReason)
		}
		if snapshot.AfterMessages >= snapshot.BeforeMessages {
			t.Fatalf("collapse 后消息数=%d, want < %d", snapshot.AfterMessages, snapshot.BeforeMessages)
		}
	})

	t.Run("fallback", func(t *testing.T) {
		server, sink := newOutcomePipelineServer(t, upstream.URL)
		// keepRecent 覆盖整个 history 时 CalcCollapseCutoff 无解，主路径必然
		// 退到 stubify+decay fallback。
		server.Config.Stubify.KeepRecent = 200
		serveOutcomeMessages(t, server, "outcome-fallback", pipelineMessages(80, 260))
		snapshot := sink.sole(t)
		if snapshot.Action != outcomeActionFallback && snapshot.Action != outcomeActionCompact {
			t.Fatalf("action=%s, want fallback 或 compact", snapshot.Action)
		}
		if snapshot.Eligibility != outcomeEligibilityEvaluable ||
			snapshot.TerminalEligibility != outcomeEligibilityTerminalEligible {
			t.Fatalf("eligibility=%s/%s", snapshot.Eligibility, snapshot.TerminalEligibility)
		}
	})
}

func TestRequestOutcomeLoadFailureMatrix(t *testing.T) {
	upstream := jsonOutcomeUpstream(t)
	failing := stateLoaderFunc(func(string) StateLoadResult {
		return StateLoadResult{Err: ErrStateLoadClosed, FailureClass: StateLoadFailureSQLiteClosed}
	})

	t.Run("history", func(t *testing.T) {
		server, sink := newOutcomePipelineServer(t, upstream.URL)
		server.HistoryEpoch.SetStateLoader(failing)
		searchCalls := 0
		server.searchAndExpandFn = func(messages []Message, _ *SQLiteStore, _ int, _ *TokenCounter, _ *Budget, _ *requestMeta) RecallOutcome {
			searchCalls++
			return RecallOutcome{Messages: messages}
		}
		serveOutcomeMessages(t, server, "outcome-history-load-fail", pipelineMessages(6, 4))
		snapshot := sink.sole(t)
		assertOutcome(t, snapshot, outcomeEligibilityNotEvaluable, outcomeEligibilityTerminalEligible,
			outcomeActionFailClosed, upstreamStateSuccess)
		if snapshot.FailureClass != persistenceFailureSQLite {
			t.Fatalf("failure_class=%s, want sqlite", snapshot.FailureClass)
		}
		if snapshot.TriggerReason != TriggerUnknown {
			t.Fatalf("trigger=%s, want unknown", snapshot.TriggerReason)
		}
		if searchCalls != 0 {
			t.Fatalf("history fail-closed 仍读取了派生状态: %d 次召回", searchCalls)
		}
	})

	for _, tc := range []struct {
		name      string
		configure func(*Server)
	}{
		{"frozen", func(s *Server) { s.Frozen.SetStateLoader(failing) }},
		{"sawtooth", func(s *Server) { s.Sawtooth.SetStateLoader(failing) }},
		{"decay", func(s *Server) { s.DecayTracker.SetStateLoader(failing) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server, sink := newOutcomePipelineServer(t, upstream.URL)
			tc.configure(server)
			serveOutcomeMessages(t, server, "outcome-load-"+tc.name, pipelineMessages(80, 260))
			snapshot := sink.sole(t)
			if snapshot.FailureClass != persistenceFailureSQLite {
				t.Fatalf("failure_class=%s, want sqlite", snapshot.FailureClass)
			}
			if snapshot.TerminalEligibility != outcomeEligibilityTerminalEligible {
				t.Fatalf("terminal_eligibility=%s, want terminal_eligible", snapshot.TerminalEligibility)
			}
			if snapshot.UpstreamState != upstreamStateSuccess {
				t.Fatalf("upstream=%s, want success", snapshot.UpstreamState)
			}
		})
	}
}

func TestRequestOutcomePersistenceCompletion(t *testing.T) {
	upstream := jsonOutcomeUpstream(t)

	t.Run("fast_receipt", func(t *testing.T) {
		server, sink := newOutcomePipelineServer(t, upstream.URL)
		backend := newBlockingStateBackend()
		writer, _, _ := newPersistenceWriterTest(t, backend)
		defer writer.CloseAndDrain()
		server.Frozen.SetStateSubmitter(writer)
		server.DecayTracker.SetStateSubmitter(writer)

		serveOutcomeMessages(t, server, "outcome-persist-fast", pipelineMessages(80, 260))
		snapshots := sink.waitFor(t, 1)
		if got := snapshots[0].DiskState; got != persistenceStateSaved {
			t.Fatalf("disk_state=%s, want saved", got)
		}
	})

	t.Run("blocked_receipt", func(t *testing.T) {
		server, sink := newOutcomePipelineServer(t, upstream.URL)
		backend := newBlockingStateBackend()
		backend.block()
		writer, _, _ := newPersistenceWriterTest(t, backend)
		defer func() {
			backend.release()
			_ = writer.CloseAndDrain()
		}()
		server.Frozen.SetStateSubmitter(writer)
		server.DecayTracker.SetStateSubmitter(writer)

		done := make(chan struct{})
		go func() {
			defer close(done)
			serveOutcomeMessages(t, server, "outcome-persist-blocked", pipelineMessages(80, 260))
		}()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Fatal("HTTP handler 被 blocked persistence receipt 拖住")
		}
		<-backend.started
		if sink.count() != 0 {
			t.Fatalf("pending receipt 未解析就已 dispatch: %d", sink.count())
		}
		backend.release()
		snapshots := sink.waitFor(t, 1)
		if got := snapshots[0].DiskState; got != persistenceStateSaved {
			t.Fatalf("释放后 disk_state=%s, want saved", got)
		}
	})

	t.Run("queue_full", func(t *testing.T) {
		server, sink := newOutcomePipelineServer(t, upstream.URL)
		backend := newBlockingStateBackend()
		backend.block()
		writer, _, reporter := newPersistenceWriterTest(t, backend)
		for index := 0; index <= PersistenceWriterQueueCapacity; index++ {
			if _, err := writer.TrySubmit(PersistenceOp{
				Target: PersistenceTargetState, Kind: PersistenceOpPut,
				OrderingKey: "frozen:saturate", Key: "frozen:saturate", Value: "v",
			}); err != nil {
				t.Fatalf("预填充 queue: %v", err)
			}
		}
		defer func() {
			backend.release()
			_ = writer.CloseAndDrain()
		}()
		server.Frozen.SetStateSubmitter(writer)
		server.DecayTracker.SetStateSubmitter(writer)

		serveOutcomeMessages(t, server, "outcome-persist-full", pipelineMessages(80, 260))
		snapshots := sink.waitFor(t, 1)
		if got := snapshots[0].DiskState; got != persistenceStateUnavailable {
			t.Fatalf("disk_state=%s, want unavailable", got)
		}
		if got := snapshots[0].FailureClass; got != persistenceFailureQueueFull {
			t.Fatalf("failure_class=%s, want queue_full", got)
		}
		if reporter.count(HealthScopeSQLiteState, HealthTransitionKindEntered) != 0 {
			t.Fatal("queue full 被伪装成 SQLite 故障")
		}
	})
}

func TestPressureAndUsageFactsRemain(t *testing.T) {
	upstream := jsonOutcomeUpstream(t)
	server, sink := newOutcomePipelineServer(t, upstream.URL)
	dataDir := t.TempDir()
	setServerDebugConfigForTest(t, server, DebugConfig{Enabled: true, FullBody: false, DataDir: dataDir})

	const sessionID = "outcome-facts-session"
	serveOutcomeMessages(t, server, sessionID, pipelineMessages(6, 4))

	facts := debugFactsByStage(t, dataDir, sessionID)
	pressure, hasPressure := facts[debugStagePressureDecision]
	usage, hasUsage := facts[debugStageResponseUsage]
	if !hasPressure || !hasUsage {
		t.Fatalf("Phase 10 技术事实被 collector 吞掉: pressure=%v usage=%v", hasPressure, hasUsage)
	}
	snapshot := sink.sole(t)
	if snapshot.RequestID == 0 {
		t.Fatal("outcome 未携带请求 ID")
	}
	for name, fact := range map[string]map[string]any{"pressure": pressure, "usage": usage} {
		id, _ := fact["request_id"].(float64)
		if uint64(id) != snapshot.RequestID {
			t.Fatalf("%s facts request_id=%v, want %d", name, fact["request_id"], snapshot.RequestID)
		}
	}
}

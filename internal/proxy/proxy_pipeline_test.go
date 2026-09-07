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
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// buildPressureDecisionWithEntry 是单函数测试的退化包装：入口 raw 与候选
// 同源（折叠改写只发生在生产管线的跨请求分叉里）。跨请求行为由
// TestPressureBaselineSurvivesCollapseRewrite 覆盖。
func buildPressureDecisionWithEntry(pressureMessages []Message, systemRaw, toolsRaw json.RawMessage, baseline pressureBaseline, tc *TokenCounter, threshold int) pressureDecision {
	entry := pressureEntryCoordinates{
		MessageCount:              len(pressureMessages),
		MessagesPrefixFingerprint: fingerprintMessagesPrefix(pressureMessages, len(pressureMessages)),
	}
	return buildPressureDecision(pressureMessages, systemRaw, toolsRaw, baseline, tc, threshold, entry, pressureMessages)
}

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

	decision := buildPressureDecisionWithEntry(messages, system, tools, pressureBaseline{}, tokenCounter, 1000)
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

	decision := buildPressureDecisionWithEntry(messages, system, tools, baseline, tokenCounter, 16000)
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
	decision := buildPressureDecisionWithEntry(messages, nil, nil, baseline, tokenCounter, 150_000)
	if decision.FullLocalEstimate >= 150_000 {
		t.Fatalf("fixture local_full=%d，必须低于阈值", decision.FullLocalEstimate)
	}
	if decision.SelectedPressure != 194_383 || decision.Source != pressureSourceLegacyHighWater || decision.ResetReason != baselineResetLegacyUnverified {
		t.Fatalf("legacy high-water decision=%+v", decision)
	}

	shrunk := buildPressureDecisionWithEntry(messages[:31], nil, nil, baseline, tokenCounter, 150_000)
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
	decision := buildPressureDecisionWithEntry(messages, system, tools, baseline, tokenCounter, 16_000)
	if decision.Source != pressureSourceLocalFull || decision.ResetReason != baselineResetLegacyUnverified || decision.SelectedPressure != decision.FullLocalEstimate {
		t.Fatalf("旧 conservative baseline 未回退到 local_full: %+v", decision)
	}

	baseline.ActualTokens = 190_000
	high := buildPressureDecisionWithEntry(messages, system, tools, baseline, tokenCounter, 150_000)
	if high.Source != pressureSourceLocalFull || high.ResetReason != baselineResetLegacyUnverified || high.SelectedPressure != high.FullLocalEstimate {
		t.Fatalf("高 conservative baseline 仍抬高 pressure: %+v", high)
	}
	baseline.SystemFingerprint = ""
	baseline.ToolsFingerprint = ""
	baseline.MessagesPrefixFingerprint = ""
	invalidCoordinates := buildPressureDecisionWithEntry(messages, system, tools, baseline, tokenCounter, 150_000)
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

	decision := buildPressureDecisionWithEntry(edited, system, tools, baseline, tokenCounter, 150_000)
	if decision.ResetReason != baselineResetMessagesChanged || decision.Source != pressureSourceConservativeHighWater || decision.SelectedPressure != baseline.ActualTokens {
		t.Fatalf("消息编辑后未保留 observed high-water: %+v", decision)
	}
	if decision.SelectedPressure <= decision.FullLocalEstimate {
		t.Fatalf("high-water 未抬高低估的 local_full: %+v", decision)
	}

	shrunk := buildPressureDecisionWithEntry(edited[:2], system, tools, baseline, tokenCounter, 150_000)
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
	decision := buildPressureDecisionWithEntry(messages, system, tools, baseline, tokenCounter, 16000)
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
	decision := buildPressureDecisionWithEntry(messages, system, tools, baseline, tokenCounter, 16000)
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
	decision := buildPressureDecisionWithEntry(messages, system, tools, baseline, tokenCounter, 16000)
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
	decision := buildPressureDecisionWithEntry(messages, nil, nil, pressureBaseline{}, tokenCounter, 0)
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
	dataDir := tempDirRetryCleanup(t)
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
	dataDir := tempDirRetryCleanup(t)
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
	for _, sessionID := range []string{"concurrent-a", "concurrent-b"} {
		seedRecallArchive(t, server.Store, sessionID)
	}
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
	// LOG-03 之后终端不再渲染 session 身份，因此并发日志的还原键只剩 request_id：
	// 每条并发主请求必须有自己的入口行与完整事件链，且互不串号。
	// 高频行已事件键化（request_in/request_forwarded 模板渲染无 request_id），
	// 入口还原改从同样每请求一条的 agent_features Debug 行提取。
	requestIDs := make(map[string]bool)
	for _, line := range lines {
		if !strings.Contains(line, "agent_features") {
			continue
		}
		for _, field := range strings.Fields(line) {
			if strings.HasPrefix(field, "request_id=") {
				requestIDs[strings.TrimPrefix(field, "request_id=")] = true
			}
		}
	}
	if len(requestIDs) != 2 {
		t.Fatalf("无法从入口日志还原 2 个唯一 request_id: %v\n%s", requestIDs, logs.String())
	}
	for requestID := range requestIDs {
		var chain strings.Builder
		for _, line := range lines {
			if strings.Contains(line, "request_id="+requestID+" ") || strings.HasSuffix(line, "request_id="+requestID) {
				chain.WriteString(line)
				chain.WriteByte('\n')
			}
		}
		got := chain.String()
		for _, event := range []string{"agent_features", "frozen prefix 未命中", "Archive 召回汇总"} {
			if !strings.Contains(got, event) {
				t.Fatalf("request_id=%s 缺少 %s:\n%s", requestID, event, got)
			}
		}
	}
	// 模板化高频行不带 request_id，改按全局出现次数断言每请求各一条。
	if got := strings.Count(logs.String(), "▸ 请求进入"); got != 2 {
		t.Fatalf("模板行 `▸ 请求进入` 出现 %d 次, want 2:\n%s", got, logs.String())
	}
	if got := strings.Count(logs.String(), "→ 上游发送"); got != 2 {
		t.Fatalf("模板行 `→ 上游发送` 出现 %d 次, want 2:\n%s", got, logs.String())
	}
	// 只否定 session 身份属性本身：本用例的 block_id fixture 刻意把 session 名
	// 编进了存档块 ID，那是测试命名而非生产身份泄漏。
	if strings.Contains(logs.String(), "request_session_id") {
		t.Fatalf("终端日志泄漏 session 身份属性:\n%s", logs.String())
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
	// 辅助直通审计已降为 Debug 级事件键（auxiliary_passthrough）。
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
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
		if strings.Contains(line, "auxiliary_passthrough") {
			if auxiliaryLog != "" {
				t.Fatalf("标题分类审计多于一条: %s", gotLogs)
			}
			auxiliaryLog = line
		}
	}
	if auxiliaryLog == "" {
		t.Fatalf("标题分类审计数量不正确: %s", gotLogs)
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
	setServerDebugConfigForTest(t, server, DebugConfig{Enabled: true, DataDir: tempDirRetryCleanup(t)})
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
	setServerDebugConfigForTest(t, server, DebugConfig{Enabled: true, DataDir: tempDirRetryCleanup(t)})
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
	if baseline := server.Sawtooth.PressureBaseline(sessionID); !baseline.Available || baseline.Conservative || baseline.ActualTokens != 100 || baseline.MessageCount != len(raw) || baseline.MessagesPrefixFingerprint != fingerprintMessagesPrefix(raw, len(raw)) {
		t.Fatalf("压缩响应未按入口 raw 坐标写入 exact baseline: %+v", baseline)
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
			decision := buildPressureDecisionWithEntry(tc.messages, nil, nil, baseline, tokenCounter, 100_000)
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

			// local_full 判定在生产路径乘校准系数；本用例判定发生在冷启动
			// （样本窗口为空），故用常数 1.50。断言不能用 CalibrationRatio——
			// 完整响应已写入首个校准样本，窗口中位数在断言时刻已经漂移。
			wantSelected := int(float64(captured.FullLocalEstimate) * defaultCalibrationRatio)
			if captured.Source != pressureSourceLocalFull || captured.ResetReason != tc.wantReason || captured.SelectedPressure != wantSelected {
				t.Fatalf("reset decision=%+v, want reason=%q selected=%d", captured, tc.wantReason, wantSelected)
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

	// threshold 抬到 18000 让两轮落在正确区间：第一轮 raw 校准后越线触发折叠；
	// 第二轮 frozen+tail 候选校准后（≈16.5k）仍低于线，不重压缩、不新增 archive。
	server := newPipelineTestServerWithThreshold(t, upstream.URL, 18000)
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
			seedRecallArchive(t, server.Store, "thread-search-once")
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

func seedRecallArchive(t *testing.T, store *SQLiteStore, sessionID string) {
	t.Helper()
	block := ArchiveBlock{
		ID: "pipeline-recall-" + sessionID, SessionID: sessionID, HistoryEpoch: 1,
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
	return newPipelineTestServerWithThreshold(t, upstreamURL, 16000)
}

// newPipelineTestServerWithThreshold 建立可配置 token 阈值的管线测试环境。
// 折叠落点口径测试需要 threshold ≥60000：旧/新 floor 公式在 16000 环境下
// 都被 10000 下限钉死，行为无区分度。
func newPipelineTestServerWithThreshold(t *testing.T, upstreamURL string, threshold int) *Server {
	t.Helper()
	cfg := DefaultConfig()
	cfg.Proxy.Target = upstreamURL
	cfg.Proxy.Deflation = 1
	cfg.Stubify.TokenThreshold = threshold
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
	server.Sawtooth = NewSawtoothTrigger(time.Minute, threshold, threshold/2)
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

// failAlways 启用既有的 failAll 字段。必须在 b.mu 下写：run() 也在 b.mu 下读它，
// 裸写会与 worker goroutine 形成数据竞争。
func (b *blockingStateBackend) failAlways() {
	b.mu.Lock()
	b.failAll = true
	b.mu.Unlock()
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

// saturatePersistenceQueue 让 writer 队列稳定处于满载：先确保一个 op 已被 worker
// 取走并阻塞（同 ordering key 因此不会再被调度），再把队列精确补到容量。
// 直接连续提交 capacity+1 个 op 是有竞态的——worker 可能在提交途中取走一个，
// 留下一个空位让被测请求的普通 state op 被接受并永久阻塞。
func saturatePersistenceQueue(t *testing.T, writer *PersistenceWriter, backend *blockingStateBackend) {
	t.Helper()
	const key = "frozen:saturate"
	submit := func() (PersistenceReceipt, error) {
		return writer.TrySubmit(PersistenceOp{
			Target: PersistenceTargetState, Kind: PersistenceOpPut,
			OrderingKey: key, Key: key, Value: "v",
		})
	}
	if receipt, err := submit(); err != nil || !receipt.Accepted {
		t.Fatalf("首个饱和 op receipt=%+v err=%v", receipt, err)
	}
	<-backend.started
	for index := 0; index < PersistenceWriterQueueCapacity; index++ {
		receipt, err := submit()
		if err != nil || !receipt.Accepted {
			t.Fatalf("第 %d 个饱和 op 应在容量内被接受: receipt=%+v err=%v", index, receipt, err)
		}
	}
	if got := writer.QueueLength(); got != PersistenceWriterQueueCapacity {
		t.Fatalf("饱和后 queue 深度=%d, want %d", got, PersistenceWriterQueueCapacity)
	}
}

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
		saturatePersistenceQueue(t, writer, backend)
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
	dataDir := tempDirRetryCleanup(t)
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

// ── Plan 06 Task 2：Archive durable gate、消息上限与两个压缩开关 ──

// countingStateLoader 统计四个组件对 SQLite 的读取次数，用于证明「拒绝前零副作用」。
type countingStateLoader struct {
	mu    sync.Mutex
	calls int
	inner StateLoader
}

func (l *countingStateLoader) LoadStateResult(key string) StateLoadResult {
	l.mu.Lock()
	l.calls++
	l.mu.Unlock()
	if l.inner == nil {
		return StateLoadResult{}
	}
	return l.inner.LoadStateResult(key)
}

func (l *countingStateLoader) count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.calls
}

func installCountingLoaders(server *Server) *countingStateLoader {
	loader := &countingStateLoader{inner: server.Store}
	server.HistoryEpoch.SetStateLoader(loader)
	server.Frozen.SetStateLoader(loader)
	server.Sawtooth.SetStateLoader(loader)
	server.DecayTracker.SetStateLoader(loader)
	return loader
}

// failingArchiveCommitter 让同步 Archive 提交稳定失败，用于证明不产生悬空 marker。
type failingArchiveCommitter struct {
	mu    sync.Mutex
	calls int
}

func (c *failingArchiveCommitter) SaveArchiveResult(ArchiveBlock) (ArchiveCommitResult, error) {
	c.mu.Lock()
	c.calls++
	c.mu.Unlock()
	return ArchiveCommitResult{State: persistenceStateFailed, FailureClass: persistenceFailureArchive},
		errors.New("forced archive failure")
}

func (c *failingArchiveCommitter) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

// capturingUpstream 记录最终发往上游的 messages。
func capturingUpstream(t *testing.T, captured *[][]Message) *httptest.Server {
	t.Helper()
	var mu sync.Mutex
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Messages []Message `json:"messages"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err == nil {
			mu.Lock()
			*captured = append(*captured, body.Messages)
			mu.Unlock()
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"type":"message","usage":{"input_tokens":1200,"output_tokens":7}}`))
	}))
	t.Cleanup(upstream.Close)
	return upstream
}

func joinedMessageText(t *testing.T, messages []Message) string {
	t.Helper()
	var builder strings.Builder
	for _, message := range messages {
		builder.WriteString(allText(t, message))
		builder.WriteString("\n")
	}
	return builder.String()
}

// archiveRecoveryIDsIn 从最终 wire 文本中提取全部 canonical 恢复引用。
// 它复用生产 marker 前缀常量，因此 marker 格式变化会立刻反映到断言上。
func archiveRecoveryIDsIn(text string) []string {
	var ids []string
	rest := text
	for {
		index := strings.Index(rest, archiveRecoveryMarkerPrefix)
		if index < 0 {
			return ids
		}
		rest = rest[index+len(archiveRecoveryMarkerPrefix):]
		end := strings.Index(rest, "')")
		if end < 0 {
			return ids
		}
		if id := rest[:end]; id != "" {
			ids = append(ids, id)
		}
		rest = rest[end+2:]
	}
}

func TestTooManyMessagesRejectedBeforeStateSideEffects(t *testing.T) {
	upstream := jsonOutcomeUpstream(t)

	for _, tc := range []struct {
		count      int
		wantStatus int
	}{
		{9999, http.StatusOK},
		{10000, http.StatusOK},
		{10001, http.StatusRequestEntityTooLarge},
		{10002, http.StatusRequestEntityTooLarge},
	} {
		t.Run(strconv.Itoa(tc.count), func(t *testing.T) {
			server, sink := newOutcomePipelineServer(t, upstream.URL)
			// 阈值放到不可能触发的高度，把本用例限定在「消息数上限」这一件事上。
			server.Config.Stubify.TokenThreshold = 50_000_000
			loader := installCountingLoaders(server)
			sessionID := "too-many-" + strconv.Itoa(tc.count)

			recorder := serveOutcomeMessages(t, server, sessionID, pipelineMessages(tc.count, 1))
			if recorder.Code != tc.wantStatus {
				t.Fatalf("status=%d, want %d body=%s", recorder.Code, tc.wantStatus, recorder.Body.String())
			}
			if tc.wantStatus != http.StatusRequestEntityTooLarge {
				if loader.count() == 0 {
					t.Fatal("上限内请求未走正常状态路径")
				}
				return
			}
			if !strings.Contains(recorder.Body.String(), "too_many_messages") {
				t.Fatalf("拒绝响应缺少稳定原因: %s", recorder.Body.String())
			}
			if loader.count() != 0 {
				t.Fatalf("拒绝前读取了 %d 次状态", loader.count())
			}
			if got := server.Sawtooth.GetRequestSeq(sessionID); got != 0 {
				t.Fatalf("拒绝前推进了 request sequence: %d", got)
			}
			if got := archiveCount(t, server.Store); got != 0 {
				t.Fatalf("拒绝前写了 %d 条 Archive", got)
			}
			snapshot := sink.sole(t)
			if snapshot.Action != outcomeActionFailClosed || snapshot.UpstreamState != upstreamStateNotStarted {
				t.Fatalf("outcome=%s/%s, want fail_closed/not_started", snapshot.Action, snapshot.UpstreamState)
			}
			if snapshot.TerminalEligibility != outcomeEligibilityTerminalEligible {
				t.Fatalf("terminal_eligibility=%s, want terminal_eligible", snapshot.TerminalEligibility)
			}
		})
	}
}

func TestArchiveMarkerOnlyAfterDurableCommit(t *testing.T) {
	var captured [][]Message
	upstream := capturingUpstream(t, &captured)
	server, _ := newOutcomePipelineServer(t, upstream.URL)

	const sessionID = "archive-durable-session"
	serveOutcomeMessages(t, server, sessionID, pipelineMessages(80, 260))
	if len(captured) != 1 {
		t.Fatalf("上游收到 %d 个请求, want 1", len(captured))
	}
	text := joinedMessageText(t, captured[0])
	ids := archiveRecoveryIDsIn(text)
	if len(ids) == 0 {
		t.Fatalf("折叠后未产生 canonical 恢复引用: %s", text)
	}
	for _, id := range ids {
		summary, found, err := server.Store.GetVisibleArchiveByID(sessionID, id)
		if err != nil {
			t.Fatalf("按 canonical ID 查询失败: %v", err)
		}
		if !found {
			t.Fatalf("恢复引用 %q 在同 session/当前分支不可见", id)
		}
		if summary.MessagesJSON == "" || summary.SessionID != sessionID {
			t.Fatalf("canonical 引用未指向本 session 原文: %+v", summary)
		}
	}
}

func TestArchiveFailurePreservesOriginalOrFailsClosed(t *testing.T) {
	t.Run("optional_compression_preserves_original", func(t *testing.T) {
		var captured [][]Message
		upstream := capturingUpstream(t, &captured)
		server, _ := newOutcomePipelineServer(t, upstream.URL)
		committer := &failingArchiveCommitter{}
		server.archiveCommitter = committer

		// fixture 压力须落在 (折叠触发线, emergency 拒发线) 区间：local_full 乘
		// 校准后要越 threshold 触发压缩尝试，但不得超过 threshold+10k，否则归档
		// 失败会 fail-closed 503 而非本分支要测的"保留原文"路径。
		// 加权字符口径下 80 条 × 85 词 ≈14560 tok，×冷启动 1.50 ≈21840，
		// 落在 (16000, 26000) 内；旧词表口径的 130 词已超上界。
		messages := pipelineMessages(80, 85)
		recorder := serveOutcomeMessages(t, server, "archive-fail-preserve", messages)
		if recorder.Code != http.StatusOK {
			t.Fatalf("status=%d, want 200 body=%s", recorder.Code, recorder.Body.String())
		}
		if committer.count() == 0 {
			t.Fatal("未尝试同步 Archive 提交")
		}
		if len(captured) != 1 {
			t.Fatalf("上游收到 %d 个请求, want 1", len(captured))
		}
		text := joinedMessageText(t, captured[0])
		if strings.Contains(text, "recover('") {
			t.Fatalf("Archive 失败仍产生了恢复引用: %s", text)
		}
		if len(captured[0]) != len(messages) {
			t.Fatalf("Archive 失败仍执行了破坏性折叠: %d, want %d", len(captured[0]), len(messages))
		}
		if got := archiveCount(t, server.Store); got != 0 {
			t.Fatalf("失败提交仍写入 %d 条 Archive", got)
		}
	})

	t.Run("mandatory_compression_fails_closed", func(t *testing.T) {
		var captured [][]Message
		upstream := capturingUpstream(t, &captured)
		server, _ := newOutcomePipelineServer(t, upstream.URL)
		server.archiveCommitter = &failingArchiveCommitter{}
		// 阈值压到很低，使本轮压力越过 emergency 线：不压缩就一定超限。
		server.Config.Stubify.TokenThreshold = 1000
		server.Sawtooth = NewSawtoothTrigger(time.Minute, 1000, 500)

		recorder := serveOutcomeMessages(t, server, "archive-fail-closed", pipelineMessages(80, 260))
		if recorder.Code != http.StatusServiceUnavailable {
			t.Fatalf("status=%d, want 503 body=%s", recorder.Code, recorder.Body.String())
		}
		if !strings.Contains(recorder.Body.String(), "archive_persistence_unavailable") {
			t.Fatalf("失败响应缺少稳定原因: %s", recorder.Body.String())
		}
		if len(captured) != 0 {
			t.Fatalf("信息已损失仍发出了 %d 个上游请求", len(captured))
		}
	})
}

func TestArchiveStateQueueFullDoesNotAffectCommit(t *testing.T) {
	var captured [][]Message
	upstream := capturingUpstream(t, &captured)
	server, sink := newOutcomePipelineServer(t, upstream.URL)

	backend := newBlockingStateBackend()
	backend.block()
	writer, _, _ := newPersistenceWriterTest(t, backend)
	saturatePersistenceQueue(t, writer, backend)
	defer func() {
		backend.release()
		_ = writer.CloseAndDrain()
	}()
	server.Frozen.SetStateSubmitter(writer)
	server.DecayTracker.SetStateSubmitter(writer)

	const sessionID = "archive-queue-full-session"
	serveOutcomeMessages(t, server, sessionID, pipelineMessages(80, 260))
	if got := archiveCount(t, server.Store); got == 0 {
		t.Fatal("state queue 满时 Archive 被一起丢弃")
	}
	if len(captured) != 1 {
		t.Fatalf("上游收到 %d 个请求, want 1", len(captured))
	}
	if ids := archiveRecoveryIDsIn(joinedMessageText(t, captured[0])); len(ids) == 0 {
		t.Fatal("state queue 满时恢复引用消失")
	}
	// 普通 state 仍是非阻塞 best-effort：本请求的磁盘结果必须如实是 unavailable。
	snapshot := sink.waitFor(t, 1)[0]
	if snapshot.DiskState != persistenceStateUnavailable {
		t.Fatalf("disk_state=%s, want unavailable", snapshot.DiskState)
	}
}

func TestCollapseEnabledOnlyControlsPrimary(t *testing.T) {
	var captured [][]Message
	upstream := capturingUpstream(t, &captured)
	server, sink := newOutcomePipelineServer(t, upstream.URL)
	server.Config.Collapse.Enabled = false

	serveOutcomeMessages(t, server, "collapse-disabled-session", pipelineMessages(80, 260))
	snapshot := sink.sole(t)
	if snapshot.Action == outcomeActionCollapse {
		t.Fatal("collapse.enabled=false 时主 Collapse 仍被执行")
	}
	if snapshot.Action != outcomeActionFallback && snapshot.Action != outcomeActionCompact {
		t.Fatalf("action=%s, want fallback 或 compact", snapshot.Action)
	}
	if len(captured) != 1 {
		t.Fatalf("上游收到 %d 个请求, want 1", len(captured))
	}

	// 开关打开时同一负载走主 Collapse。
	enabledServer, enabledSink := newOutcomePipelineServer(t, upstream.URL)
	serveOutcomeMessages(t, enabledServer, "collapse-enabled-session", pipelineMessages(80, 260))
	if got := enabledSink.sole(t).Action; got != outcomeActionCollapse {
		t.Fatalf("collapse.enabled=true 时 action=%s, want collapse", got)
	}
}

func TestCompactDisabledUsesSafePlan(t *testing.T) {
	var captured [][]Message
	upstream := capturingUpstream(t, &captured)
	server, _ := newOutcomePipelineServer(t, upstream.URL)
	server.Config.Collapse.Enabled = false
	server.Config.Collapse.CompactEnabled = false

	serveOutcomeMessages(t, server, "compact-disabled-session", pipelineMessages(80, 260))
	if len(captured) != 1 {
		t.Fatalf("上游收到 %d 个请求, want 1", len(captured))
	}
	for index, message := range captured[0] {
		if len(bytes.TrimSpace(message.Content)) == 0 {
			t.Fatalf("compact_enabled=false 产生了空正文消息 #%d", index)
		}
		if text := strings.TrimSpace(allText(t, message)); text == "" && !bytes.Contains(message.Content, []byte("tool_")) {
			t.Fatalf("compact_enabled=false 产生了 Stage-3 空正文消息 #%d: %s", index, message.Content)
		}
	}
}

// ── Plan 10 Task 1：闭环的内存维度必须反映真实的内存写入 ──

// TestProductionMemoryDimensionReflectsRealInMemoryWrite 断言的是生产链路，
// 不是 formatter：两个子用例的 snapshot 都由生产 dispatcher 捕获，测试内不出现
// 任何对 requestOutcomeSnapshot 字段的直接赋值。
func TestProductionMemoryDimensionReflectsRealInMemoryWrite(t *testing.T) {
	upstream := jsonOutcomeUpstream(t)

	t.Run("collapse_writes_memory", func(t *testing.T) {
		server, sink := newOutcomePipelineServer(t, upstream.URL)
		backend := newBlockingStateBackend()
		writer, _, _ := newPersistenceWriterTest(t, backend)
		defer writer.CloseAndDrain()
		server.Frozen.SetStateSubmitter(writer)
		server.DecayTracker.SetStateSubmitter(writer)

		serveOutcomeMessages(t, server, "memory-dimension-collapse", pipelineMessages(80, 260))
		snapshot := sink.waitFor(t, 1)[0]
		if got := snapshot.MemoryState; got != persistenceStateSaved {
			t.Fatalf("memory_state=%s, want saved（Frozen 内存写入确已成功）", got)
		}
		if got := snapshot.DiskState; got != persistenceStateSaved {
			t.Fatalf("disk_state=%s, want saved", got)
		}
		closure := formatRequestOutcomeClosure(snapshot)
		for _, want := range []string{"memory=saved", "disk=saved"} {
			if !strings.Contains(closure, want) {
				t.Fatalf("闭环缺少 %q: %s", want, closure)
			}
		}
	})

	// 对照用例：不发生内存写入的直通请求必须保持 not_attempted，且仍恰好
	// dispatch 一次——证明内存维度不适用时既不谎报，也不会让 closure 悬挂。
	t.Run("no_state_write_stays_not_attempted", func(t *testing.T) {
		server, sink := newOutcomePipelineServer(t, upstream.URL)

		serveOutcomeMessages(t, server, "memory-dimension-passthrough", pipelineMessages(4, 3))
		snapshot := sink.sole(t)
		if got := snapshot.MemoryState; got != persistenceStateNotAttempted {
			t.Fatalf("未发生内存写入的请求 memory_state=%s, want not_attempted", got)
		}
		if got := sink.count(); got != 1 {
			t.Fatalf("dispatch 次数=%d, want 1", got)
		}
	})
}

// ── Plan 10 Task 2：内存成功 + 磁盘失败必须能被分别读出（CONTEXT D-11） ──

// TestProductionMemorySavedWithDiskFailedIsReadable 用必失败 backend 做真实故障
// 注入，snapshot 同样由生产 dispatcher 捕获。
func TestProductionMemorySavedWithDiskFailedIsReadable(t *testing.T) {
	upstream := jsonOutcomeUpstream(t)
	server, sink := newOutcomePipelineServer(t, upstream.URL)
	backend := newBlockingStateBackend()
	backend.failAlways()
	writer, _, _ := newPersistenceWriterTest(t, backend)
	defer writer.CloseAndDrain()
	server.Frozen.SetStateSubmitter(writer)
	server.DecayTracker.SetStateSubmitter(writer)

	serveOutcomeMessages(t, server, "memory-saved-disk-failed", pipelineMessages(80, 260))
	snapshot := sink.waitFor(t, 1)[0]
	if got := snapshot.MemoryState; got != persistenceStateSaved {
		t.Fatalf("memory_state=%s, want saved（内存写入先于磁盘且确已成功）", got)
	}
	if got := snapshot.DiskState; got != persistenceStateFailed {
		t.Fatalf("disk_state=%s, want failed", got)
	}
	if got := snapshot.FailureClass; got != persistenceFailureSQLite {
		t.Fatalf("failure_class=%s, want sqlite", got)
	}

	closure := formatRequestOutcomeClosure(snapshot)
	for _, want := range []string{"memory=saved", "disk=failed", "failure=sqlite"} {
		if !strings.Contains(closure, want) {
			t.Fatalf("闭环缺少 %q（两个维度必须可分别读出）: %s", want, closure)
		}
	}
}

// ── Plan 10 Task 3：真实主请求的终端输出不得出现完整 session ID ──

// terminalProbeSessionID 是醒目的探针 session ID：一旦出现在终端输出里就是
// LOG-03 违例，取值不与任何其他 fixture 冲突。
const terminalProbeSessionID = "SECRET-FULL-SESSION-ID-terminal-probe-3c9d"

func TestProductionTerminalNeverPrintsFullSessionID(t *testing.T) {
	logs := captureOrdinaryLogs(t)
	upstream := jsonOutcomeUpstream(t)
	server, sink := newOutcomePipelineServer(t, upstream.URL)

	serveOutcomeMessages(t, server, terminalProbeSessionID, pipelineMessages(80, 260))
	output := logs.String()

	// 正向（范围围栏）先断言：脱敏不得靠降级进度行来换取。请求进入/上游发送
	// 已按双轨规范降为 Debug 事件键（默认 Info 终端不可见），进行中与完成后的
	// 进度反馈由 frozen 存储行与 ctx_tokens 模板行承担。
	for _, want := range []string{"frozen prefix 已存储", "上下文总Tokens=", "[INFO]"} {
		if !strings.Contains(output, want) {
			t.Fatalf("终端丢失请求进行中的进度反馈 %q:\n%s", want, output)
		}
	}
	// 模板占位符必须已被 kv 替换：裸占位符意味着调用点 kv 键与模板发生漂移。
	if strings.Contains(output, "上下文总Tokens={") {
		t.Fatalf("ctx_tokens 模板占位符未被替换（kv 键漂移?）:\n%s", output)
	}

	// 负向：完整 session ID 与属性键都不得进终端。
	if strings.Contains(output, terminalProbeSessionID) {
		t.Fatalf("终端输出泄漏完整 session ID:\n%s", output)
	}
	if strings.Contains(output, "request_session_id") {
		t.Fatalf("终端输出含 request_session_id 属性键:\n%s", output)
	}

	// 关联性：脱敏后仍可用短哈希关联到落盘记录。
	if got, want := sink.sole(t).SessionHash, stableSessionHash(terminalProbeSessionID); got != want {
		t.Fatalf("session_hash=%s, want %s", got, want)
	}
}

// TestProductionLongRequestProgressMapsToSessionLog 把原先需要人工同时观察的
// 两个窗口收敛成一个确定性生产管线合同：上游仍被屏障阻塞时，三条阶段进度
// 已经可见且最终结果尚未出现；释放后，结果行的短 hash 必须对应 session 文件。
// 大请求复用既有 pipelineMessages fixture，不另建 token/消息生成器。
func TestProductionLongRequestProgressMapsToSessionLog(t *testing.T) {
	terminal := &lockedBuffer{}
	dataDir := tempDirRetryCleanup(t)
	terminalHandler := NewLogHandler(terminal, slog.LevelInfo)
	fileHandler := NewSessionLogHandler(dataDir, slog.LevelInfo, nil)
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(NewCombinedLogHandler(terminalHandler, fileHandler)))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })

	upstreamStarted := make(chan struct{})
	releaseUpstream := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseUpstream) }) }
	t.Cleanup(release)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(upstreamStarted)
		<-releaseUpstream
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"type":"message","usage":{"input_tokens":1200,"output_tokens":7}}`))
	}))
	t.Cleanup(upstream.Close)

	server := newPipelineTestServer(t, upstream.URL)
	reporter := NewTerminalHealthReporter(terminalHandler)
	tracker := NewHealthTracker(reporter)
	gaps := NewOutcomeGapAccumulator()
	dispatcher, err := NewOutcomeDispatcherChecked(OutcomeDispatcherOptions{
		TerminalProjector: terminalHandler,
		SessionProjector:  NewSessionOutcomeWriter(fileHandler, gaps, tracker),
		HealthTracker:     tracker,
		Reporter:          reporter,
		GapAccumulator:    gaps,
	})
	if err != nil {
		t.Fatalf("NewOutcomeDispatcherChecked: %v", err)
	}
	var drainOnce sync.Once
	drain := func() { drainOnce.Do(func() { _ = dispatcher.CloseAndDrain() }) }
	t.Cleanup(drain)
	server.SetOutcomeDispatcher(dispatcher)

	const sessionID = "SECRET-FULL-SESSION-ID-long-progress-probe"
	body, err := json.Marshal(map[string]any{
		"model":    "deepseek-v4-pro",
		"messages": pipelineMessages(80, 260),
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Claude-Code-Session-Id", sessionID)
	recorder := httptest.NewRecorder()
	requestDone := make(chan struct{})
	go func() {
		server.HandleMessages(recorder, req)
		close(requestDone)
	}()

	select {
	case <-upstreamStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("长请求未到达受控上游")
	}

	// 请求明确仍在处理中：此时上游没有返回，结果行不可能已经完成。
	// 请求进入/上游发送已降为 Debug 事件键（本终端为默认 Info 级），
	// 进行中可见的进度反馈是折叠路径的 frozen 存储行。
	during := terminal.String()
	for _, want := range []string{"frozen prefix 已存储", "[INFO]"} {
		if !strings.Contains(during, want) {
			t.Fatalf("长请求处理中缺少进度反馈 %q:\n%s", want, during)
		}
	}
	if strings.Contains(during, "ST 已触发") {
		t.Fatalf("上游仍阻塞时提前输出最终结果:\n%s", during)
	}
	if strings.Contains(during, sessionID) || strings.Contains(during, "request_session_id") {
		t.Fatalf("长请求进度泄漏完整 session 身份:\n%s", during)
	}

	release()
	select {
	case <-requestDone:
	case <-time.After(5 * time.Second):
		t.Fatal("释放上游后请求未完成")
	}
	drain()
	if recorder.Code != http.StatusOK {
		t.Fatalf("HandleMessages status=%d body=%s", recorder.Code, recorder.Body.String())
	}

	hash := stableSessionHash(sessionID)
	completed := terminal.String()
	if strings.Count(completed, "ST 已触发") != 1 || !strings.Contains(completed, "会话="+hash) {
		t.Fatalf("最终结果行未唯一输出或短 hash 不匹配 %s:\n%s", hash, completed)
	}
	if strings.Contains(completed, sessionID) || strings.Contains(completed, "request_session_id") {
		t.Fatalf("完整终端输出泄漏 session 身份:\n%s", completed)
	}

	sessionPath := filepath.Join(dataDir, "logs", hash+".log")
	sessionLog := readSessionLogFile(t, sessionPath)
	if strings.Count(sessionLog, "event=request_outcome") != 1 || !strings.Contains(sessionLog, "session="+hash) {
		t.Fatalf("session 文件缺少唯一闭环或短 hash 不匹配 %s: %q", sessionPath, sessionLog)
	}
	if strings.Contains(sessionLog, sessionID) || strings.Contains(sessionLog, "request_session_id") {
		t.Fatalf("session 文件泄漏完整 session 身份: %q", sessionLog)
	}
}

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
		_, _ = w.Write([]byte(`{"type":"message","usage":{"input_tokens":196,"cache_creation_input_tokens":0,"cache_read_input_tokens":93056,"output_tokens":3}}`))
	}))
	defer upstream.Close()

	server := newPipelineTestServer(t, upstream.URL)
	debugDir := tempDirRetryCleanup(t)
	setServerDebugConfigForTest(t, server, DebugConfig{Enabled: true, FullBody: false, DataDir: debugDir})
	var persisted string
	server.Sawtooth.SetPersistFunc(func(key, value string) {
		// Phase 11 起生产状态始终使用显式 epoch-scoped key；测试只观察
		// 当前 epoch，不把旧裸 session key 重新当作当前状态。
		if strings.HasPrefix(key, "sawtooth:phase08-combined:history_epoch:") {
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
	// Archive 行数不能证明"没有重复 collapse"——SaveArchive 是 ON CONFLICT DO NOTHING，
	// 相同 Archive 再折叠一次也不会新增行。这里只保留它能真正证明的那一点：
	// 没有产生一份**不同**的 Archive。是否重复 collapse 由下面的 pressure 状态转换断言。
	if got := archiveCount(t, server.Store); got != archivesAfterCollapse {
		t.Fatalf("第二轮生成了新的 Archive 行: got=%d want=%d", got, archivesAfterCollapse)
	}
	if result := server.Frozen.Get("phase08-combined", StripReminders(secondHistory)); result == nil {
		t.Fatal("组合场景第二轮未保持 Frozen hit")
	}

	var state persistedState
	if err := json.Unmarshal([]byte(persisted), &state); err != nil {
		t.Fatalf("parse persisted Sawtooth state: %v raw=%q", err, persisted)
	}
	// baseline 契约坐标 = 入口 raw 快照（DetachPersistentUserContext +
	// StripReminders 之后、任何改写之前），不随折叠改写为 wire 坐标。
	secondEntry, _ := DetachPersistentUserContext(secondRaw)
	secondEntry = StripReminders(secondEntry)
	if state.Tokens != 93_252 || state.MsgCount != len(secondEntry) || state.Conservative || state.SystemFingerprint == "" || state.ToolsFingerprint == "" || state.MessagesPrefixFingerprint != fingerprintMessagesPrefix(secondEntry, len(secondEntry)) {
		t.Fatalf("baseline 未按入口 raw 坐标写入: %+v", state)
	}
	if baseline := server.Sawtooth.PressureBaseline("phase08-combined"); !baseline.Available || baseline.Conservative || baseline.ActualTokens != 93_252 || baseline.MessageCount != len(secondEntry) || baseline.MessagesPrefixFingerprint != fingerprintMessagesPrefix(secondEntry, len(secondEntry)) {
		t.Fatalf("raw 坐标下的 exact baseline 不可用: %+v", baseline)
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
	firstEntry, _ := DetachPersistentUserContext(firstRaw)
	firstEntry = StripReminders(firstEntry)
	assertPhase08PressureTransition(t, stageByRequest, len(firstEntry))
	for requestID, stages := range stageByRequest {
		for _, stage := range []debugStage{debugStageRawInbound, debugStagePressureDecision, debugStageForwarded, debugStageResponseUsage} {
			if _, ok := stages[stage]; !ok {
				t.Fatalf("request %d missing stage %q", requestID, stage)
			}
		}
		if usage := stages[debugStageResponseUsage]; usage.TotalInputTokens != 93252 {
			t.Fatalf("request %d usage total=%d, want 93252", requestID, usage.TotalInputTokens)
		} else if usage.BaselineUpdated == nil || !*usage.BaselineUpdated {
			t.Fatalf("request %d baseline_updated=%v, want true", requestID, usage.BaselineUpdated)
		} else if usage.BaselineUpdateKind == nil || *usage.BaselineUpdateKind != pressureBaselineUpdateExact {
			t.Fatalf("request %d baseline_update_kind=%v, want exact", requestID, usage.BaselineUpdateKind)
		}
		if raw := stages[debugStageRawInbound]; raw.ImageCount != 1 || !raw.HasClaudeMDContext {
			t.Fatalf("request %d raw facts=%+v", requestID, raw)
		}
		if forwardedFact := stages[debugStageForwarded]; forwardedFact.ImageCount != 1 || !forwardedFact.HasClaudeMDContext {
			t.Fatalf("request %d forwarded facts=%+v", requestID, forwardedFact)
		}
	}
}

// assertPhase08PressureTransition 直接断言两轮之间的 pressure 状态转换，取代
// 「Archive 行数没增加 ⇒ 没有重复 collapse」这个恒真推断（REVIEW_GAPS 4.2）。
//
// 本场景的第二轮**确实**会重新 collapse，这是 fixture 决定的而非缺陷：
// 上游 stub 对一份本地估算约 29k 的 body 固定回报 93252 tokens，而阈值只有 16000。
// 同时 persistent context 从 A 换成 B 使前缀指纹改变，pressure 落到
// conservative_high_water 分支并沿用 93252 高水位 → emergency。
//
// 因此这里断言的是真正有证明力的东西：第一轮冷启动走 local_full，第二轮
// **消费了第一轮写下的 exact baseline**（previous_actual / previous_message_count
// 与第一轮 forwarded 坐标一致）。旧的 Archive 行数断言恰恰漏掉的就是这一段。
// 「不重复 collapse」的正面护栏在 TestPressureActualPlusDeltaLifecycle。
func assertPhase08PressureTransition(t *testing.T, stageByRequest map[uint64]map[debugStage]debugFact, firstRawCount int) {
	t.Helper()
	requestIDs := make([]uint64, 0, len(stageByRequest))
	for requestID := range stageByRequest {
		requestIDs = append(requestIDs, requestID)
	}
	sort.Slice(requestIDs, func(i, j int) bool { return requestIDs[i] < requestIDs[j] })

	first := stageByRequest[requestIDs[0]][debugStagePressureDecision]
	if first.PressureSource == nil || *first.PressureSource != pressureSourceLocalFull {
		t.Fatalf("第一轮 pressure_source=%s, want %q", debugFactValue(first.PressureSource), pressureSourceLocalFull)
	}
	if first.BaselineResetReason == nil || *first.BaselineResetReason != baselineResetNoActual {
		t.Fatalf("第一轮 baseline_reset_reason=%s, want %q", debugFactValue(first.BaselineResetReason), baselineResetNoActual)
	}
	if first.CompressDecision == nil || !*first.CompressDecision {
		t.Fatalf("第一轮 compress_decision=%s, want true", debugFactValue(first.CompressDecision))
	}

	second := stageByRequest[requestIDs[1]][debugStagePressureDecision]
	if second.PreviousActualTokens == nil || *second.PreviousActualTokens != 93_252 {
		t.Fatalf("第二轮 previous_actual_tokens=%s, want 93252（第一轮 exact baseline 未被消费）", debugFactValue(second.PreviousActualTokens))
	}
	if second.PreviousMessageCount == nil || *second.PreviousMessageCount != firstRawCount {
		t.Fatalf("第二轮 previous_message_count=%s, want %d（baseline 绑定入口 raw 坐标）",
			debugFactValue(second.PreviousMessageCount), firstRawCount)
	}
	if second.PressureSource == nil || *second.PressureSource != pressureSourceActualPlusDelta {
		t.Fatalf("第二轮 pressure_source=%s, want %q（raw 坐标下主路径必须生效）", debugFactValue(second.PressureSource), pressureSourceActualPlusDelta)
	}
	if second.SelectedPressureTokens == nil || *second.SelectedPressureTokens <= 93_252 {
		t.Fatalf("第二轮 selected_pressure=%s, want actual+delta（高于高水位）", debugFactValue(second.SelectedPressureTokens))
	}
}

// debugFactValue 渲染 debugFact 里的可空字段，避免失败信息打印成指针地址。
func debugFactValue[T any](p *T) string {
	if p == nil {
		return "nil"
	}
	return fmt.Sprintf("%v", *p)
}

func TestPhase08DebugStagesWithoutCompressionPipeline(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"type":"message","usage":{"input_tokens":2,"cache_read_input_tokens":3}}`))
	}))
	defer upstream.Close()
	cfg := DefaultConfig()
	cfg.Proxy.Target = upstream.URL
	cfg.Proxy.Deflation = 1
	cfg.Debug = DebugConfig{Enabled: true, FullBody: false, DataDir: tempDirRetryCleanup(t)}
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

// mainStateSnapshot 汇总一次请求可能触碰到的全部主线有状态资源。
// 全部字段均为可比较值类型，便于直接用 == 做前后快照比对。
type mainStateSnapshot struct {
	FrozenLen    int
	Archives     int
	RequestSeq   int
	Baseline     pressureBaseline
	DecayEntries int
}

func snapshotMainState(t *testing.T, server *Server, sessionID string) mainStateSnapshot {
	t.Helper()

	server.DecayTracker.mu.RLock()
	decayEntries := len(server.DecayTracker.stubbedAt)
	server.DecayTracker.mu.RUnlock()

	return mainStateSnapshot{
		FrozenLen:    server.Frozen.LengthFor(sessionID),
		Archives:     archiveCount(t, server.Store),
		RequestSeq:   server.Sawtooth.GetRequestSeq(sessionID),
		Baseline:     server.Sawtooth.PressureBaseline(sessionID),
		DecayEntries: decayEntries,
	}
}

// TestHandleMessagesInterleavedAgentStreamsIsolateMainState 复现 CC 2.1.220 的真实并发
// 形态：main 与 5 个 Task subagent 共用同一个 X-Claude-Code-Session-Id，6 条互不相干的
// 历史交错到达同一个状态机。
//
// 生产 trace（FIX_PLAN.md 1.1）中，代理原有的三条 subagent 识别路径全部失效
// （billing header 被关闭、agentContext 从不进 body、无 stream:false 旁路查询），
// 152 个请求全部被判成 main，导致 114/152（75%）请求触发 epoch 变更、common_prefix
// 归零，主线的 baseline / Frozen / Decay 状态每轮被冲刷。
//
// 本测试锁定修复后的不变量：带 X-Claude-Code-Agent-Id 的流量在进入 HistoryEpoch gate
// 之前透明直通，主线状态不被任何 subagent 请求触碰。
func TestHandleMessagesInterleavedAgentStreamsIsolateMainState(t *testing.T) {
	const (
		sessionID           = "SHARED-SESSION-INTERLEAVED-9A7C2F"
		subagentSentinel    = "subagent-isolated-history"
		subagentUsageTokens = 191_000
		mainUsageTokens     = 4_000
	)
	// 取自生产 trace 的真实 agent id（FIX_PLAN.md 1.1 表格）。
	agentIDs := []string{
		"ac3d8abb21a98f939",
		"ae62648d28a17ee1a",
		"a74453d0a5cd80b06",
		"a9c7f0dda3165134a",
		"a1affcdb69ecfa2b5",
	}

	var forwardedAgentIDs []string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("读取上游请求失败: %v", err)
			return
		}
		// 按请求体内容而非 header 区分来源，使 usage 断言不依赖 header 是否透传。
		usage := mainUsageTokens
		if bytes.Contains(body, []byte(subagentSentinel)) {
			usage = subagentUsageTokens
			forwardedAgentIDs = append(forwardedAgentIDs, r.Header.Get("X-Claude-Code-Agent-Id"))
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"type":"message","usage":{"input_tokens":%d,"output_tokens":1}}`, usage)
	}))
	defer upstream.Close()

	server := newPipelineTestServer(t, upstream.URL)

	// subagent 在 HistoryEpoch gate 之前就返回，永远不会走到 Archive 召回。
	// 因此该回调的调用次数本身就是"进入有状态管线的请求数"。
	type epochObservation struct {
		Epoch   uint64
		Changed bool
	}
	var mainObservations []epochObservation
	server.searchAndExpandFn = func(messages []Message, _ *SQLiteStore, _ int, _ *TokenCounter, _ *Budget, meta *requestMeta) RecallOutcome {
		mainObservations = append(mainObservations, epochObservation{
			Epoch:   meta.HistoryEpoch,
			Changed: meta.HistoryEpochChanged,
		})
		return RecallOutcome{Messages: messages}
	}

	// main 流：足够大以触发一次真实 collapse，从而建立 Frozen / Archive / baseline。
	mainHistory := pipelineMessages(300, 80)
	appendMainTurn := func(turn int) {
		mainHistory = append(mainHistory,
			Message{Role: "user", Content: mustMarshal(fmt.Sprintf("main-stream-turn-%d follow-up question", turn))},
			Message{Role: "assistant", Content: mustMarshal(fmt.Sprintf("main-stream-answer-%d", turn))},
		)
	}

	// 每条 subagent 历史都与 main 完全不相干——若它们进入 epoch gate，
	// common prefix 必为 0，必然推进 epoch 并冲刷主线状态。
	subagentHistory := func(agentID string) []Message {
		return []Message{
			{Role: "user", Content: mustMarshal(subagentSentinel + " task brief for " + agentID)},
			{Role: "assistant", Content: mustMarshal(subagentSentinel + " working on " + agentID)},
		}
	}

	serveSubagent := func(agentID string) {
		t.Helper()
		before := snapshotMainState(t, server, sessionID)
		servePipelineRequestWith(t, server, sessionID, subagentHistory(agentID), nil, map[string]string{
			"X-Claude-Code-Agent-Id": agentID,
		})
		after := snapshotMainState(t, server, sessionID)
		if before != after {
			t.Fatalf("subagent %s 触碰了主线状态:\nbefore=%+v\nafter =%+v", agentID, before, after)
		}
	}

	// main#1 —— 建立主线状态。
	servePipelineRequest(t, server, sessionID, mainHistory)
	established := snapshotMainState(t, server, sessionID)
	if established.FrozenLen == 0 || established.Archives == 0 || established.RequestSeq == 0 {
		t.Fatalf("main 首轮未建立可观测状态，后续隔离断言将失去意义: %+v", established)
	}

	// 交错到达：每两次 main 之间至少插入一次 subagent。
	serveSubagent(agentIDs[0])
	serveSubagent(agentIDs[1])

	appendMainTurn(2)
	servePipelineRequest(t, server, sessionID, mainHistory)

	serveSubagent(agentIDs[2])
	serveSubagent(agentIDs[3])

	appendMainTurn(3)
	servePipelineRequest(t, server, sessionID, mainHistory)

	serveSubagent(agentIDs[4])
	serveSubagent(agentIDs[0]) // 同一 subagent 二次到达

	appendMainTurn(4)
	servePipelineRequest(t, server, sessionID, mainHistory)

	// 1. 只有 main 请求进入有状态管线。
	if len(mainObservations) != 4 {
		t.Fatalf("进入有状态管线的请求数=%d, want 4（仅 main）", len(mainObservations))
	}

	// 2. main 的 epoch 只在首次建立时变化一次，此后恒定。
	if !mainObservations[0].Changed {
		t.Fatalf("main 首轮未建立 epoch: %+v", mainObservations[0])
	}
	for i, observation := range mainObservations[1:] {
		if observation.Changed {
			t.Fatalf("main 第 %d 轮 epoch 被重建——subagent 冲刷了主线状态机: %+v", i+2, observation)
		}
		if observation.Epoch != mainObservations[0].Epoch {
			t.Fatalf("main 第 %d 轮 epoch=%d, want 恒定 %d", i+2, observation.Epoch, mainObservations[0].Epoch)
		}
	}

	// 3. subagent 响应的 usage 从未写入主线 pressure baseline。
	if baseline := server.Sawtooth.PressureBaseline(sessionID); baseline.ActualTokens == subagentUsageTokens {
		t.Fatalf("subagent 响应 usage 污染了主线 pressure baseline: %+v", baseline)
	}

	// 4. agent-id header 原样转发上游（FIX_PLAN.md 2026-07-28 决定 1：不剥离）。
	wantAgentIDs := []string{agentIDs[0], agentIDs[1], agentIDs[2], agentIDs[3], agentIDs[4], agentIDs[0]}
	if len(forwardedAgentIDs) != len(wantAgentIDs) {
		t.Fatalf("上游收到的 subagent 请求数=%d, want %d", len(forwardedAgentIDs), len(wantAgentIDs))
	}
	for i, want := range wantAgentIDs {
		if forwardedAgentIDs[i] != want {
			t.Fatalf("第 %d 个 subagent 请求转发的 agent-id=%q, want %q", i+1, forwardedAgentIDs[i], want)
		}
	}
}

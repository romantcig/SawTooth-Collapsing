package proxy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestSawtoothPressureBaselineSnapshot(t *testing.T) {
	trigger := NewSawtoothTrigger(0, 100_000, 10_000)
	const threadID = "pressure-snapshot"
	systemFingerprint := strings.Repeat("a", 64)
	toolsFingerprint := strings.Repeat("b", 64)
	messagesPrefixFingerprint := strings.Repeat("c", 64)

	trigger.mu.Lock()
	trigger.lastTotalTokens[threadID] = 91_234
	trigger.lastMessageCount[threadID] = 27
	trigger.systemFingerprints[threadID] = systemFingerprint
	trigger.toolsFingerprints[threadID] = toolsFingerprint
	trigger.messagesPrefixFingerprints[threadID] = messagesPrefixFingerprint
	trigger.loadedFromDB[threadID] = true
	trigger.mu.Unlock()

	got := trigger.PressureBaseline(threadID)
	if !got.Available || got.ResetReason != baselineResetNone {
		t.Fatalf("baseline availability = %t, reset = %q", got.Available, got.ResetReason)
	}
	if got.ActualTokens != 91_234 || got.MessageCount != 27 {
		t.Fatalf("baseline coordinates = actual %d, messages %d", got.ActualTokens, got.MessageCount)
	}
	if got.SystemFingerprint != systemFingerprint || got.ToolsFingerprint != toolsFingerprint || got.MessagesPrefixFingerprint != messagesPrefixFingerprint {
		t.Fatalf("baseline fingerprints = (%q, %q, %q)", got.SystemFingerprint, got.ToolsFingerprint, got.MessagesPrefixFingerprint)
	}
}

func TestSawtoothPressureBaselineConcurrentAtomicity(t *testing.T) {
	trigger := NewSawtoothTrigger(0, 100_000, 10_000)
	const threadID = "pressure-concurrent"
	type version struct {
		actual int
		count  int
		system string
		tools  string
	}
	versions := []version{
		{actual: 10_001, count: 101, system: strings.Repeat("1", 64), tools: strings.Repeat("2", 64)},
		{actual: 20_002, count: 202, system: strings.Repeat("3", 64), tools: strings.Repeat("4", 64)},
	}
	writeVersion := func(value version) {
		trigger.mu.Lock()
		trigger.lastTotalTokens[threadID] = value.actual
		trigger.lastMessageCount[threadID] = value.count
		trigger.systemFingerprints[threadID] = value.system
		trigger.toolsFingerprints[threadID] = value.tools
		trigger.loadedFromDB[threadID] = true
		trigger.mu.Unlock()
	}
	writeVersion(versions[0])

	const iterations = 20_000
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < iterations; i++ {
			writeVersion(versions[i%len(versions)])
		}
	}()
	close(start)
	for i := 0; i < iterations; i++ {
		got := trigger.PressureBaseline(threadID)
		matches := false
		for _, want := range versions {
			if got.ActualTokens == want.actual && got.MessageCount == want.count &&
				got.SystemFingerprint == want.system && got.ToolsFingerprint == want.tools {
				matches = true
				break
			}
		}
		if !matches {
			t.Fatalf("observed torn baseline: %+v", got)
		}
	}
	wg.Wait()
}

func TestSawtoothPressureBaselineMissingState(t *testing.T) {
	trigger := NewSawtoothTrigger(0, 100_000, 10_000)
	loadCalls := 0
	trigger.SetLoadFunc(func(string) (string, bool) {
		loadCalls++
		return "", false
	})

	for i := 0; i < 2; i++ {
		got := trigger.PressureBaseline("pressure-missing")
		if got.Available || got.ResetReason != baselineResetNoActual {
			t.Fatalf("missing baseline = %+v", got)
		}
		if got.ActualTokens != 0 || got.MessageCount != 0 || got.SystemFingerprint != "" || got.ToolsFingerprint != "" {
			t.Fatalf("missing baseline exposed invented state: %+v", got)
		}
	}
	if loadCalls != 1 {
		t.Fatalf("cold-start load calls = %d, want 1", loadCalls)
	}
}

func TestSawtoothPressureBaselineConcurrentColdStartWaitsForLoad(t *testing.T) {
	const threadID = "pressure-concurrent-cold-start"
	trigger := NewSawtoothTrigger(0, 100_000, 10_000)
	loadStarted := make(chan struct{})
	releaseLoad := make(chan struct{})
	loadCalls := 0
	state := persistedState{
		Tokens: 88_888, MsgCount: 18,
		SystemFingerprint: strings.Repeat("1", 64), ToolsFingerprint: strings.Repeat("2", 64),
		MessagesPrefixFingerprint: strings.Repeat("3", 64),
	}
	raw, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	trigger.SetLoadFunc(func(string) (string, bool) {
		loadCalls++
		close(loadStarted)
		<-releaseLoad
		return string(raw), true
	})

	results := make(chan pressureBaseline, 2)
	go func() { results <- trigger.PressureBaseline(threadID) }()
	<-loadStarted
	go func() { results <- trigger.PressureBaseline(threadID) }()
	select {
	case got := <-results:
		t.Fatalf("concurrent cold-start returned before load completion: %+v", got)
	case <-time.After(50 * time.Millisecond):
	}
	close(releaseLoad)
	for i := 0; i < 2; i++ {
		got := <-results
		if !got.Available || got.ActualTokens != state.Tokens || got.MessageCount != state.MsgCount ||
			got.SystemFingerprint != state.SystemFingerprint || got.ToolsFingerprint != state.ToolsFingerprint ||
			got.MessagesPrefixFingerprint != state.MessagesPrefixFingerprint {
			t.Fatalf("cold-start result=%+v, want %+v", got, state)
		}
	}
	if loadCalls != 1 {
		t.Fatalf("load calls=%d, want 1", loadCalls)
	}
}

func TestSawtoothPressureBaselineSlowLoadDoesNotOverwriteResponse(t *testing.T) {
	const threadID = "pressure-slow-load-response-wins"
	trigger := NewSawtoothTrigger(0, 100_000, 10_000)
	loadStarted := make(chan struct{})
	releaseLoad := make(chan struct{})
	oldState := persistedState{
		Tokens: 11_111, MsgCount: 11,
		SystemFingerprint: strings.Repeat("1", 64), ToolsFingerprint: strings.Repeat("2", 64),
		MessagesPrefixFingerprint: strings.Repeat("3", 64),
	}
	raw, err := json.Marshal(oldState)
	if err != nil {
		t.Fatal(err)
	}
	trigger.SetLoadFunc(func(string) (string, bool) {
		close(loadStarted)
		<-releaseLoad
		return string(raw), true
	})
	loadedResult := make(chan pressureBaseline, 1)
	go func() { loadedResult <- trigger.PressureBaseline(threadID) }()
	<-loadStarted

	trigger.UpdatePressureBaseline(threadID, 22_222, 22, strings.Repeat("4", 64), strings.Repeat("5", 64), strings.Repeat("6", 64))
	close(releaseLoad)
	got := <-loadedResult
	if !got.Available || got.ActualTokens != 22_222 || got.MessageCount != 22 ||
		got.SystemFingerprint != strings.Repeat("4", 64) || got.ToolsFingerprint != strings.Repeat("5", 64) ||
		got.MessagesPrefixFingerprint != strings.Repeat("6", 64) {
		t.Fatalf("slow load overwrote newer response: %+v", got)
	}
}

func TestSawtoothPressureBaselinePersistenceRoundTrip(t *testing.T) {
	const threadID = "pressure-round-trip"
	systemFingerprint := strings.Repeat("a", 64)
	toolsFingerprint := strings.Repeat("b", 64)
	messagesPrefixFingerprint := strings.Repeat("c", 64)
	persisted := make(map[string]string)

	trigger := NewSawtoothTrigger(0, 100_000, 10_000)
	trigger.SetPersistFunc(func(key, value string) { persisted[key] = value })
	trigger.UpdatePressureBaseline(threadID, 93_252, 25, systemFingerprint, toolsFingerprint, messagesPrefixFingerprint)

	raw, ok := persisted["sawtooth:"+threadID]
	if !ok {
		t.Fatal("complete pressure baseline was not persisted")
	}
	var state persistedState
	if err := json.Unmarshal([]byte(raw), &state); err != nil {
		t.Fatalf("unmarshal persisted pressure baseline: %v", err)
	}
	if state.Tokens != 93_252 || state.MsgCount != 25 ||
		state.SystemFingerprint != systemFingerprint || state.ToolsFingerprint != toolsFingerprint || state.MessagesPrefixFingerprint != messagesPrefixFingerprint {
		t.Fatalf("persisted pressure baseline = %+v", state)
	}

	restored := NewSawtoothTrigger(0, 100_000, 10_000)
	restored.SetLoadFunc(func(key string) (string, bool) {
		value, found := persisted[key]
		return value, found
	})
	got := restored.PressureBaseline(threadID)
	if !got.Available || got.ActualTokens != 93_252 || got.MessageCount != 25 ||
		got.SystemFingerprint != systemFingerprint || got.ToolsFingerprint != toolsFingerprint || got.MessagesPrefixFingerprint != messagesPrefixFingerprint {
		t.Fatalf("restored pressure baseline = %+v", got)
	}
}

func TestSawtoothPressureBaselineLoadsLegacyState(t *testing.T) {
	trigger := NewSawtoothTrigger(0, 100_000, 10_000)
	trigger.SetLoadFunc(func(key string) (string, bool) {
		return `{"tokens":81234,"msg_count":19}`, key == "sawtooth:pressure-legacy"
	})

	got := trigger.PressureBaseline("pressure-legacy")
	if got.ActualTokens != 81_234 || got.MessageCount != 19 {
		t.Fatalf("legacy coordinates were not restored: %+v", got)
	}
	if got.Available || got.SystemFingerprint != "" || got.ToolsFingerprint != "" || got.ResetReason != baselineResetNoActual {
		t.Fatalf("legacy state was incorrectly treated as calibrated: %+v", got)
	}
	if reason := trigger.ShouldTrigger("pressure-legacy", 1); reason != TriggerNone {
		t.Fatalf("legacy actual below threshold changed trigger behavior: %q", reason)
	}
}

func TestSawtoothPressureBaselineLegacyUpdateForcesRebaseline(t *testing.T) {
	trigger := NewSawtoothTrigger(0, 100_000, 10_000)
	trigger.UpdatePressureBaseline(
		"pressure-wrapper",
		70_000,
		20,
		strings.Repeat("c", 64),
		strings.Repeat("d", 64),
		strings.Repeat("e", 64),
	)
	trigger.UpdateAfterResponse("pressure-wrapper", 71_000, 22)

	got := trigger.PressureBaseline("pressure-wrapper")
	if got.ActualTokens != 71_000 || got.MessageCount != 22 {
		t.Fatalf("legacy wrapper did not update coordinates: %+v", got)
	}
	if got.Available || got.SystemFingerprint != "" || got.ToolsFingerprint != "" {
		t.Fatalf("legacy wrapper retained reusable fingerprints: %+v", got)
	}
}

func TestSawtoothPressureBaselineRejectsInvalidFingerprint(t *testing.T) {
	valid := strings.Repeat("e", 64)
	invalidFingerprints := []string{
		strings.Repeat("A", 64),
		strings.Repeat("f", 63),
		strings.Repeat("g", 64),
	}
	for index, invalid := range invalidFingerprints {
		threadID := fmt.Sprintf("pressure-invalid-%d", index)
		trigger := NewSawtoothTrigger(0, 100_000, 10_000)
		trigger.UpdatePressureBaseline(threadID, 50_000, 12, invalid, valid, valid)
		got := trigger.PressureBaseline(threadID)
		if got.Available || got.SystemFingerprint != "" || got.ToolsFingerprint != valid {
			t.Fatalf("invalid fingerprint entered calibrated baseline: %+v", got)
		}
	}

	trigger := NewSawtoothTrigger(0, 100_000, 10_000)
	trigger.UpdatePressureBaseline("pressure-no-actual", 0, 12, valid, valid, valid)
	got := trigger.PressureBaseline("pressure-no-actual")
	if got.Available || got.ActualTokens != 0 || got.SystemFingerprint != "" || got.ToolsFingerprint != "" {
		t.Fatalf("non-positive actual established baseline: %+v", got)
	}
}

func TestSawtoothPressureBaselineUpdateIsAtomic(t *testing.T) {
	trigger := NewSawtoothTrigger(0, 100_000, 10_000)
	const threadID = "pressure-update-atomic"
	type version struct {
		actual int
		count  int
		system string
		tools  string
		prefix string
	}
	versions := []version{
		{actual: 30_003, count: 303, system: strings.Repeat("5", 64), tools: strings.Repeat("6", 64), prefix: strings.Repeat("9", 64)},
		{actual: 40_004, count: 404, system: strings.Repeat("7", 64), tools: strings.Repeat("8", 64), prefix: strings.Repeat("a", 64)},
	}
	trigger.UpdatePressureBaseline(threadID, versions[0].actual, versions[0].count, versions[0].system, versions[0].tools, versions[0].prefix)

	const iterations = 20_000
	start := make(chan struct{})
	var wg sync.WaitGroup
	for writer := 0; writer < 2; writer++ {
		writer := writer
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for i := 0; i < iterations; i++ {
				value := versions[(i+writer)%len(versions)]
				trigger.UpdatePressureBaseline(threadID, value.actual, value.count, value.system, value.tools, value.prefix)
			}
		}()
	}
	close(start)
	for i := 0; i < iterations; i++ {
		got := trigger.PressureBaseline(threadID)
		matches := false
		for _, want := range versions {
			if got.Available && got.ActualTokens == want.actual && got.MessageCount == want.count &&
				got.SystemFingerprint == want.system && got.ToolsFingerprint == want.tools && got.MessagesPrefixFingerprint == want.prefix {
				matches = true
				break
			}
		}
		if !matches {
			t.Fatalf("observed torn public baseline update: %+v", got)
		}
	}
	wg.Wait()
}

func TestSawtoothPressureBaselinePersistenceKeepsStateOrder(t *testing.T) {
	trigger := NewSawtoothTrigger(0, 100_000, 10_000)
	const threadID = "pressure-persistence-order"
	firstEntered := make(chan struct{})
	releaseFirst := make(chan struct{})
	var persistedMu sync.Mutex
	var persisted string
	trigger.SetPersistFunc(func(_ string, value string) {
		var state persistedState
		if err := json.Unmarshal([]byte(value), &state); err != nil {
			t.Errorf("unmarshal persisted state: %v", err)
			return
		}
		if state.Tokens == 11_111 {
			close(firstEntered)
			<-releaseFirst
		}
		persistedMu.Lock()
		persisted = value
		persistedMu.Unlock()
	})

	firstDone := make(chan struct{})
	go func() {
		trigger.UpdatePressureBaseline(threadID, 11_111, 11, strings.Repeat("1", 64), strings.Repeat("2", 64), strings.Repeat("3", 64))
		close(firstDone)
	}()
	<-firstEntered

	secondDone := make(chan struct{})
	go func() {
		trigger.UpdatePressureBaseline(threadID, 22_222, 22, strings.Repeat("4", 64), strings.Repeat("5", 64), strings.Repeat("6", 64))
		close(secondDone)
	}()
	select {
	case <-secondDone:
		t.Fatal("second update completed before the first persistence was released")
	case <-time.After(50 * time.Millisecond):
	}
	close(releaseFirst)
	<-firstDone
	<-secondDone

	baseline := trigger.PressureBaseline(threadID)
	persistedMu.Lock()
	raw := persisted
	persistedMu.Unlock()
	var state persistedState
	if err := json.Unmarshal([]byte(raw), &state); err != nil {
		t.Fatalf("unmarshal final persisted state: %v", err)
	}
	if state.Tokens != baseline.ActualTokens || state.MsgCount != baseline.MessageCount ||
		state.SystemFingerprint != baseline.SystemFingerprint || state.ToolsFingerprint != baseline.ToolsFingerprint ||
		state.MessagesPrefixFingerprint != baseline.MessagesPrefixFingerprint {
		t.Fatalf("persistent baseline diverged\nstate=%+v\nbaseline=%+v", state, baseline)
	}
}

func TestFrozenStoreKeepsRawCutoffSeparateFromFrozenPrefixLength(t *testing.T) {
	frozen := NewFrozenStubs()
	raw := frozenTestMessages(302)
	prefix := deepCopyMessages(raw[:5])

	frozen.Store("thread", prefix, 300, raw[299], 120, 6000)

	if got := frozen.LengthFor("thread"); got != len(prefix) {
		t.Fatalf("frozen prefix length = %d, want %d", got, len(prefix))
	}
	result := frozen.Get("thread", raw)
	if result == nil {
		t.Fatal("expected frozen result for matching raw boundary message")
	}
	if result.Cutoff != 300 {
		t.Fatalf("raw cutoff = %d, want 300", result.Cutoff)
	}
	if len(result.Messages) != len(prefix) {
		t.Fatalf("stored frozen prefix length = %d, want %d", len(result.Messages), len(prefix))
	}
	if result.Tokens != 120 || result.RawTokens != 6000 {
		t.Fatalf("token metadata = (%d, %d), want (120, 6000)", result.Tokens, result.RawTokens)
	}
}

func TestFrozenPersistentContextSnapshotsRejected(t *testing.T) {
	contextMessage := persistentContextMessage("claudeMd", "FROZEN-CONTEXT-MUST-NOT-PERSIST")
	history := frozenTestMessages(3)

	t.Run("new store", func(t *testing.T) {
		frozen := NewFrozenStubs()
		frozen.Store("thread", []Message{contextMessage, history[0]}, 2, history[1], 20, 40)
		if got := frozen.LengthFor("thread"); got != 0 {
			t.Fatalf("包含 persistent context 的 snapshot 被存储，length=%d", got)
		}
	})

	t.Run("legacy persisted snapshot", func(t *testing.T) {
		messages := []Message{contextMessage, history[0]}
		messagesJSON, err := json.Marshal(messages)
		if err != nil {
			t.Fatal(err)
		}
		persisted, err := json.Marshal(frozenPersisted{
			Messages: messages, Cutoff: 2, BoundaryHash: stableBoundaryHash(history[1]),
			PrefixHash: sha256hex(messagesJSON), Tokens: 20, RawTokens: 40,
		})
		if err != nil {
			t.Fatal(err)
		}
		frozen := NewFrozenStubs()
		frozen.SetLoadFunc(func(string) (string, bool) { return string(persisted), true })
		if got := frozen.Get("thread", history); got != nil {
			t.Fatal("包含旧 persistent context 的持久化 snapshot 不得恢复")
		}
	})
}

func TestFrozenPersistentContextChangesDoNotInvalidateDetachedHistory(t *testing.T) {
	history := frozenTestMessages(4)
	firstRaw := append([]Message{persistentContextMessage("claudeMd", "context-A")}, history...)
	firstHistory, _ := DetachPersistentUserContext(firstRaw)
	prefix := deepCopyMessages(firstHistory[:2])
	frozen := NewFrozenStubs()
	frozen.Store("thread", prefix, 3, firstHistory[2], 20, 40)

	secondRaw := append([]Message{persistentContextMessage("claudeMd", "context-B")}, history...)
	secondHistory, context := DetachPersistentUserContext(secondRaw)
	result := frozen.Get("thread", secondHistory)
	if result == nil {
		t.Fatal("只修改 current context 不应使 detached historical Frozen 失效")
	}
	forwarded := PrependPersistentUserContext(append(result.Messages, secondHistory[result.Cutoff:]...), context)
	if got := countMessagesContaining(forwarded, "context-B"); got != 1 {
		t.Fatalf("forwarded context B count=%d, want 1", got)
	}
	if got := countMessagesContaining(forwarded, "context-A"); got != 0 {
		t.Fatalf("forwarded context A count=%d, want 0", got)
	}
}

func TestFrozenUpdateMessagesPersistsBytesAndPreservesRawMetadata(t *testing.T) {
	persisted := make(map[string]string)
	frozen := NewFrozenStubs()
	frozen.SetPersistFunc(func(key, value string) {
		persisted[key] = value
	})

	raw := frozenTestMessages(302)
	prefix := deepCopyMessages(raw[:4])
	frozen.Store("thread", prefix, 300, raw[299], 120, 6000)

	updated := deepCopyMessages(prefix)
	updated[1].Content = mustMarshal("updated frozen bytes")
	if !frozen.UpdateMessages("thread", updated) {
		t.Fatal("UpdateMessages should accept an equal-length frozen prefix")
	}
	if frozen.UpdateMessages("thread", updated[:len(updated)-1]) {
		t.Fatal("UpdateMessages should reject a different frozen prefix length")
	}
	if frozen.UpdateMessages("missing", updated) {
		t.Fatal("UpdateMessages should reject a missing frozen entry")
	}

	restored := NewFrozenStubs()
	restored.SetLoadFunc(func(key string) (string, bool) {
		value, ok := persisted[key]
		return value, ok
	})
	result := restored.Get("thread", raw)
	if result == nil {
		t.Fatal("expected cold-start load of updated frozen prefix")
	}
	if result.Cutoff != 300 || result.Tokens != 120 || result.RawTokens != 6000 {
		t.Fatalf("cold-start metadata = cutoff %d, tokens %d, raw tokens %d", result.Cutoff, result.Tokens, result.RawTokens)
	}
	want, err := json.Marshal(updated)
	if err != nil {
		t.Fatalf("marshal updated prefix: %v", err)
	}
	got, err := json.Marshal(result.Messages)
	if err != nil {
		t.Fatalf("marshal restored prefix: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("cold-start bytes differ\ngot:  %s\nwant: %s", got, want)
	}
}

func TestFrozenBoundaryHashCoversCompleteMessage(t *testing.T) {
	tests := []struct {
		name   string
		before json.RawMessage
		after  json.RawMessage
	}{
		{
			name:   "第 201 字节后文本变化",
			before: mustMarshal(strings.Repeat("a", 220) + "before"),
			after:  mustMarshal(strings.Repeat("a", 220) + "after"),
		},
		{
			name:   "第二个文本块变化",
			before: json.RawMessage(`[{"type":"text","text":"same"},{"type":"text","text":"before"}]`),
			after:  json.RawMessage(`[{"type":"text","text":"same"},{"type":"text","text":"after"}]`),
		},
		{
			name:   "tool_result 内容变化",
			before: json.RawMessage(`[{"type":"tool_result","tool_use_id":"tool-1","content":"before"}]`),
			after:  json.RawMessage(`[{"type":"tool_result","tool_use_id":"tool-1","content":"after"}]`),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			frozen := NewFrozenStubs()
			current := frozenTestMessages(3)
			current[1].Content = tt.before
			frozen.Store("thread", current[:1], 2, current[1], 10, 20)
			current[1].Content = tt.after
			if got := frozen.Get("thread", current); got != nil {
				t.Fatal("完整 boundary 内容变化后仍命中 frozen")
			}
		})
	}
}

func TestFrozenBoundaryIgnoresNonSemanticContentBlockShapeChanges(t *testing.T) {
	t.Run("省略字段与显式 null 等价", func(t *testing.T) {
		frozen := NewFrozenStubs()
		current := frozenTestMessages(3)
		current[1] = Message{Role: "user", Content: json.RawMessage(`[{"type":"tool_result","tool_use_id":"tool-1","content":"same"}]`)}
		frozen.Store("thread", current[:1], 2, current[1], 10, 20)

		current[1].Content = json.RawMessage(`[{"type":"tool_result","tool_use_id":"tool-1","input":null,"content":"same"}]`)
		if got := frozen.Get("thread", current); got == nil {
			t.Fatal("仅出现无语义的 input:null 后 frozen boundary 不应失效")
		}
	})

	t.Run("真实 tool_result 内容编辑仍失效", func(t *testing.T) {
		frozen := NewFrozenStubs()
		current := frozenTestMessages(3)
		current[1] = Message{Role: "user", Content: json.RawMessage(`[{"type":"tool_result","tool_use_id":"tool-1","content":"before"}]`)}
		frozen.Store("thread", current[:1], 2, current[1], 10, 20)

		current[1].Content = json.RawMessage(`[{"type":"tool_result","tool_use_id":"tool-1","input":null,"content":"after"}]`)
		if got := frozen.Get("thread", current); got != nil {
			t.Fatal("真实 tool_result 内容编辑后 frozen boundary 仍命中")
		}
	})

	t.Run("未知扩展字段显式 null 保持语义敏感", func(t *testing.T) {
		frozen := NewFrozenStubs()
		current := frozenTestMessages(3)
		current[1] = Message{Role: "user", Content: json.RawMessage(`[{"type":"tool_result","tool_use_id":"tool-1","content":"same"}]`)}
		frozen.Store("thread", current[:1], 2, current[1], 10, 20)

		current[1].Content = json.RawMessage(`[{"type":"tool_result","tool_use_id":"tool-1","content":"same","future_semantic":null}]`)
		if got := frozen.Get("thread", current); got != nil {
			t.Fatal("未知扩展字段从 absent 变为显式 null 后 frozen boundary 仍命中")
		}
	})
}

func TestStableBoundaryHashPreservesUnprovenNullAndUnknownFields(t *testing.T) {
	base := Message{Role: "user", Content: json.RawMessage(`[{"type":"tool_result","content":["same",null]}]`)}
	withArrayValue := Message{Role: "user", Content: json.RawMessage(`[{"type":"tool_result","content":["same","changed"]}]`)}
	if stableBoundaryHash(base) == stableBoundaryHash(withArrayValue) {
		t.Fatal("数组中的 null 被错误规范化或忽略")
	}

	withUnknownNonNull := Message{Role: "user", Content: json.RawMessage(`[{"type":"tool_result","content":["same",null],"future_semantic":"value"}]`)}
	if stableBoundaryHash(base) == stableBoundaryHash(withUnknownNonNull) {
		t.Fatal("未知非 null 扩展字段被错误规范化或忽略")
	}

	toolUseAbsentInput := Message{Role: "assistant", Content: json.RawMessage(`[{"type":"tool_use","id":"tool-1","name":"Read"}]`)}
	toolUseNullInput := Message{Role: "assistant", Content: json.RawMessage(`[{"type":"tool_use","id":"tool-1","name":"Read","input":null}]`)}
	if stableBoundaryHash(toolUseAbsentInput) == stableBoundaryHash(toolUseNullInput) {
		t.Fatal("tool_use 的显式 input:null 被错误视为字段省略")
	}
}

func TestFrozenBoundaryHashIncludesMessageUnknownFieldStates(t *testing.T) {
	decode := func(raw string) Message {
		t.Helper()
		var message Message
		if err := json.Unmarshal([]byte(raw), &message); err != nil {
			t.Fatalf("unmarshal boundary message: %v", err)
		}
		return message
	}
	baseBoundary := decode(`{"role":"assistant","content":[{"type":"text","text":"same"}]}`)
	tests := []struct {
		name     string
		boundary Message
	}{
		{name: "explicit null", boundary: decode(`{"role":"assistant","content":[{"type":"text","text":"same"}],"future":null}`)},
		{name: "non-null value", boundary: decode(`{"role":"assistant","content":[{"type":"text","text":"same"}],"future":{"mode":"strict"}}`)},
		{name: "known metadata", boundary: decode(`{"role":"assistant","content":[{"type":"text","text":"same"}],"isMeta":true}`)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if stableBoundaryHash(baseBoundary) == stableBoundaryHash(tt.boundary) {
				t.Fatal("message-level boundary field state did not affect hash")
			}

			current := []Message{
				{Role: "user", Content: json.RawMessage(`"prefix"`)},
				baseBoundary,
				{Role: "user", Content: json.RawMessage(`"tail"`)},
			}
			frozen := NewFrozenStubs()
			frozen.Store("thread", current[:1], 2, current[1], 10, 20)
			current[1] = tt.boundary
			if got := frozen.Get("thread", current); got != nil {
				t.Fatal("Frozen.Get accepted a boundary with different message-level fields")
			}
		})
	}
}

func TestFrozenColdStartRejectsInvalidCutoff(t *testing.T) {
	current := frozenTestMessages(3)
	prefix := deepCopyMessages(current[:1])
	prefixJSON, err := json.Marshal(prefix)
	if err != nil {
		t.Fatal(err)
	}
	for _, cutoff := range []int{-1, 0, 4} {
		t.Run(fmt.Sprintf("cutoff=%d", cutoff), func(t *testing.T) {
			persisted, err := json.Marshal(frozenPersisted{
				Messages: prefix, Cutoff: cutoff,
				BoundaryHash: stableBoundaryHash(current[1]),
				PrefixHash:   sha256hex(prefixJSON), Tokens: 10, RawTokens: 20,
			})
			if err != nil {
				t.Fatal(err)
			}
			frozen := NewFrozenStubs()
			frozen.SetLoadFunc(func(string) (string, bool) { return string(persisted), true })
			if got := frozen.Get("thread", current); got != nil {
				t.Fatalf("非法 cutoff=%d 不应恢复 frozen", cutoff)
			}
			if got := frozen.LengthFor("thread"); got != 0 {
				t.Fatalf("非法状态进入内存，长度=%d", got)
			}
		})
	}
}

func TestFrozenConcurrentPersistenceKeepsStateOrder(t *testing.T) {
	frozen := NewFrozenStubs()
	raw := frozenTestMessages(4)
	firstPersistStarted := make(chan struct{})
	releaseFirstPersist := make(chan struct{})
	var once sync.Once
	var persisted string
	frozen.SetPersistFunc(func(_ string, value string) {
		once.Do(func() {
			close(firstPersistStarted)
			<-releaseFirstPersist
		})
		persisted = value
	})

	firstDone := make(chan struct{})
	go func() {
		frozen.Store("thread", raw[:1], 2, raw[1], 10, 20)
		close(firstDone)
	}()
	<-firstPersistStarted
	secondDone := make(chan struct{})
	go func() {
		frozen.Store("thread", raw[:2], 3, raw[2], 20, 30)
		close(secondDone)
	}()
	close(releaseFirstPersist)
	<-firstDone
	<-secondDone

	var got frozenPersisted
	if err := json.Unmarshal([]byte(persisted), &got); err != nil {
		t.Fatalf("解析最终持久化状态: %v", err)
	}
	if got.Cutoff != 3 || len(got.Messages) != 2 {
		t.Fatalf("最终持久化状态过期: cutoff=%d messages=%d", got.Cutoff, len(got.Messages))
	}
	result := frozen.Get("thread", raw)
	if result == nil || result.Cutoff != got.Cutoff || len(result.Messages) != len(got.Messages) {
		t.Fatalf("内存与持久化状态不一致: result=%+v persisted=%+v", result, got)
	}
}

func TestFrozenBoundaryInvalidationDeletesPersistedStateOnce(t *testing.T) {
	persisted := make(map[string]string)
	source := NewFrozenStubs()
	source.SetPersistFunc(func(key, value string) { persisted[key] = value })
	current := frozenTestMessages(3)
	source.Store("thread", current[:1], 2, current[1], 10, 20)

	loadCalls := 0
	deleteCalls := 0
	restored := NewFrozenStubs()
	restored.SetLoadFunc(func(key string) (string, bool) {
		loadCalls++
		value, ok := persisted[key]
		return value, ok
	})
	restored.SetDeleteFunc(func(key string) {
		deleteCalls++
		delete(persisted, key)
	})
	current[1].Content = mustMarshal("edited boundary")
	if got := restored.Get("thread", current); got != nil {
		t.Fatal("boundary 不匹配时不应返回 frozen")
	}
	if _, ok := persisted["frozen:thread"]; ok {
		t.Fatal("boundary 不匹配后持久化 frozen 状态未删除")
	}
	if got := restored.Get("thread", current); got != nil {
		t.Fatal("重复 Get 不应恢复已失效 frozen")
	}
	if loadCalls != 1 || deleteCalls != 1 {
		t.Fatalf("失效状态重复加载或删除: load=%d delete=%d", loadCalls, deleteCalls)
	}
}

func frozenTestMessages(count int) []Message {
	messages := make([]Message, count)
	for i := range messages {
		role := "user"
		if i%2 == 1 {
			role = "assistant"
		}
		messages[i] = Message{Role: role, Content: mustMarshal(fmt.Sprintf("message-%03d", i))}
	}
	return messages
}

package proxy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
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

func TestSawtoothConservativePressureFloorRoundTrip(t *testing.T) {
	const threadID = "pressure-floor-round-trip"
	fingerprint := strings.Repeat("a", 64)
	persisted := make(map[string]string)
	trigger := NewSawtoothTrigger(0, 150_000, 75_000)
	trigger.SetPersistFunc(func(key, value string) { persisted[key] = value })
	if !trigger.UpdatePressureFloorForRequest(threadID, 0, 194_383, 32, fingerprint, fingerprint, fingerprint) {
		t.Fatal("conservative pressure floor 未写入")
	}

	restored := NewSawtoothTrigger(0, 150_000, 75_000)
	restored.SetLoadFunc(func(key string) (string, bool) {
		value, ok := persisted[key]
		return value, ok
	})
	got := restored.PressureBaseline(threadID)
	if !got.Available || !got.Conservative || got.ActualTokens != 194_383 || got.MessageCount != 32 {
		t.Fatalf("恢复 conservative pressure floor=%+v", got)
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

func TestSawtoothPressureBaselinePersistCallbackCanReadBaseline(t *testing.T) {
	trigger := NewSawtoothTrigger(0, 100_000, 10_000)
	const threadID = "pressure-persist-reentrant-read"
	callbackRead := make(chan pressureBaseline, 1)
	trigger.SetPersistFunc(func(_ string, _ string) {
		callbackRead <- trigger.PressureBaseline(threadID)
	})
	done := make(chan struct{})
	go func() {
		trigger.UpdatePressureBaseline(threadID, 33_333, 33, strings.Repeat("1", 64), strings.Repeat("2", 64), strings.Repeat("3", 64))
		close(done)
	}()

	select {
	case got := <-callbackRead:
		if !got.Available || got.ActualTokens != 33_333 || got.MessageCount != 33 {
			t.Fatalf("persist callback read stale baseline: %+v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("persist callback reading PressureBaseline deadlocked")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("baseline update did not return after reentrant read")
	}
}

func TestSawtoothPressureBaselineSlowPersistenceDoesNotBlockOtherThread(t *testing.T) {
	trigger := NewSawtoothTrigger(0, 100_000, 10_000)
	firstEntered := make(chan struct{})
	releaseFirst := make(chan struct{})
	trigger.SetPersistFunc(func(key, _ string) {
		if key == "sawtooth:slow-thread" {
			close(firstEntered)
			<-releaseFirst
		}
	})

	slowDone := make(chan struct{})
	go func() {
		trigger.UpdatePressureBaseline("slow-thread", 11_111, 11, strings.Repeat("1", 64), strings.Repeat("2", 64), strings.Repeat("3", 64))
		close(slowDone)
	}()
	<-firstEntered

	fastDone := make(chan struct{})
	go func() {
		trigger.UpdatePressureBaseline("fast-thread", 22_222, 22, strings.Repeat("4", 64), strings.Repeat("5", 64), strings.Repeat("6", 64))
		close(fastDone)
	}()
	select {
	case <-fastDone:
	case <-time.After(time.Second):
		t.Fatal("另一个 session 被慢持久化阻塞")
	}
	close(releaseFirst)
	<-slowDone
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

// 已知 content block 的直接 cache_control 是缓存元数据，与 reuseSafetyPrefixHash
// 的规范化规则同源：只有已知 block 类型忽略，未知类型 fail-closed。
// 回归自 .planning/debug/context-threshold-cache-hit.md req3 假失效。
func TestFrozenBoundaryIgnoresKnownBlockCacheControlMetadata(t *testing.T) {
	boundaryWithoutCacheControl := json.RawMessage(`[{"type":"tool_result","tool_use_id":"tool-1","content":"same"}]`)
	boundaryWithCacheControl := json.RawMessage(`[{"type":"tool_result","tool_use_id":"tool-1","content":"same","cache_control":{"type":"ephemeral"}}]`)
	boundaryUnknownBlockCacheControl := json.RawMessage(`[{"type":"future_block","content":"same","cache_control":{"type":"ephemeral"}}]`)
	boundaryNestedCacheControl := json.RawMessage(`[{"type":"tool_result","tool_use_id":"tool-1","content":[{"type":"text","text":"same","cache_control":{"type":"ephemeral"}}]}]`)

	store := func(boundary json.RawMessage) (*FrozenStubs, []Message) {
		frozen := NewFrozenStubs()
		current := frozenTestMessages(3)
		current[1].Content = boundary
		frozen.Store("thread", current[:1], 2, current[1], 10, 20)
		return frozen, current
	}

	t.Run("已知 block 出现或消失 cache_control 不失效", func(t *testing.T) {
		frozen, current := store(boundaryWithoutCacheControl)
		current[1].Content = boundaryWithCacheControl
		if got := frozen.Get("thread", current); got == nil {
			t.Fatal("已知 block 仅 cache_control 元数据变化不应使 frozen boundary 失效")
		}

		frozen, current = store(boundaryWithCacheControl)
		current[1].Content = boundaryWithoutCacheControl
		if got := frozen.Get("thread", current); got == nil {
			t.Fatal("已知 block 仅 cache_control 元数据消失不应使 frozen boundary 失效")
		}
	})

	t.Run("未知 block 类型的 cache_control 保持语义敏感", func(t *testing.T) {
		frozen, current := store(boundaryWithoutCacheControl)
		current[1].Content = boundaryUnknownBlockCacheControl
		if got := frozen.Get("thread", current); got != nil {
			t.Fatal("未知 block 类型的 cache_control 必须保持 fail-closed")
		}
	})

	t.Run("嵌套业务内容里的 cache_control 保持语义敏感", func(t *testing.T) {
		frozen, current := store(boundaryWithoutCacheControl)
		current[1].Content = boundaryNestedCacheControl
		if got := frozen.Get("thread", current); got != nil {
			t.Fatal("tool_result.content 嵌套 block 的 cache_control 不在已知规范化范围内，必须失效")
		}
	})

	t.Run("真实业务内容变化仍失效", func(t *testing.T) {
		frozen, current := store(boundaryWithCacheControl)
		current[1].Content = json.RawMessage(`[{"type":"tool_result","tool_use_id":"tool-1","content":"changed","cache_control":{"type":"ephemeral"}}]`)
		if got := frozen.Get("thread", current); got != nil {
			t.Fatal("真实 tool_result 内容变化后 frozen boundary 仍命中")
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

func TestStableBoundaryHashSurvivesCCShellRewrite(t *testing.T) {
	// CC 当轮给最后一条 user 消息包壳挂 cache_control，退位后按内部原样
	// 直出纯文本（CC 2.1.258 T5o）。内容一致时两种 wire 形态必须同哈希，
	// 否则 frozen boundary 每轮失配一次、反复重折叠。
	shelled := Message{Role: "user", Content: json.RawMessage(`[{"type":"text","text":"同一份正文","cache_control":{"type":"ephemeral"}}]`)}
	bare := Message{Role: "user", Content: json.RawMessage(`"同一份正文"`)}
	if stableBoundaryHash(shelled) != stableBoundaryHash(bare) {
		t.Fatal("CC 退位消息的壳差异导致 boundary 哈希失配")
	}

	// 折叠只对恰好 type+text 两键的单 text 块生效，未知字段有语义不得吞掉
	withUnknownField := Message{Role: "user", Content: json.RawMessage(`[{"type":"text","text":"x","future_semantic":"v"}]`)}
	folded := Message{Role: "user", Content: json.RawMessage(`"x"`)}
	if stableBoundaryHash(withUnknownField) == stableBoundaryHash(folded) {
		t.Fatal("带未知字段的单 text 块被错误折叠")
	}

	multiBlock := Message{Role: "user", Content: json.RawMessage(`[{"type":"text","text":"a"},{"type":"text","text":"b"}]`)}
	concatenated := Message{Role: "user", Content: json.RawMessage(`"ab"`)}
	if stableBoundaryHash(multiBlock) == stableBoundaryHash(concatenated) {
		t.Fatal("多 text 块消息被错误折叠为拼接文本")
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

// W4-1：同一 stateKey 的并发冷启动只触发一次 DB 查询，且等待方在加载完成后
// 仍能拿到恢复出的 frozen prefix。
func TestFrozenLoadFromDBConcurrentColdStartLoadsOnce(t *testing.T) {
	raw := frozenTestMessages(4)
	var persisted string
	producer := NewFrozenStubs()
	producer.SetPersistFunc(func(_ string, value string) { persisted = value })
	producer.Store("thread", deepCopyMessages(raw[:2]), 3, raw[2], 20, 30)
	if persisted == "" {
		t.Fatal("producer 未产出持久化状态")
	}

	// 先单线程确认这套构造真的能恢复，否则并发断言会退化成两个 nil 的平凡通过。
	warm := NewFrozenStubs()
	warm.SetLoadFunc(func(string) (string, bool) { return persisted, true })
	if got := warm.Get("thread", raw); got == nil {
		t.Fatal("单线程冷启动未能恢复 frozen prefix，测试构造无效")
	}

	var loadCalls int64
	loadStarted := make(chan struct{})
	releaseLoad := make(chan struct{})
	restored := NewFrozenStubs()
	restored.SetLoadFunc(func(string) (string, bool) {
		if atomic.AddInt64(&loadCalls, 1) == 1 {
			close(loadStarted)
			<-releaseLoad
		}
		return persisted, true
	})

	results := make(chan *FrozenResult, 2)
	go func() { results <- restored.Get("thread", raw) }()
	<-loadStarted
	go func() { results <- restored.Get("thread", raw) }()
	select {
	case got := <-results:
		t.Fatalf("并发冷启动在加载完成前返回: %+v", got)
	case <-time.After(50 * time.Millisecond):
	}
	close(releaseLoad)

	for i := 0; i < 2; i++ {
		got := <-results
		if got == nil {
			t.Fatal("并发冷启动未能拿到恢复出的 frozen prefix")
		}
		if got.Cutoff != 3 || len(got.Messages) != 2 || got.Tokens != 20 || got.RawTokens != 30 {
			t.Fatalf("恢复出的 frozen prefix 不符: %+v", got)
		}
	}
	if n := atomic.LoadInt64(&loadCalls); n != 1 {
		t.Fatalf("loadFn 调用次数 = %d, want 1", n)
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

// ── Plan 12.1-05：error-aware StateLoader 与 result-bearing persistence ──

// stateLoaderFunc 让测试用闭包实现 Plan 04 的 StateLoader 合同。
type stateLoaderFunc func(key string) StateLoadResult

func (f stateLoaderFunc) LoadStateResult(key string) StateLoadResult { return f(key) }

// stateLoadErrorCases 是三类必须与 missing 严格区分的真实读取失败。
func stateLoadErrorCases() map[string]StateLoadResult {
	return map[string]StateLoadResult{
		"closed": {Err: ErrStateLoadClosed, FailureClass: StateLoadFailureSQLiteClosed},
		"busy":   {Err: ErrStateLoadBusy, FailureClass: StateLoadFailureSQLiteBusy},
		"query":  {Err: ErrStateLoadQueryFailed, FailureClass: StateLoadFailureQueryFailed},
	}
}

// reentrantStateSubmitter 在提交路径上回读组件状态。若组件在自己的 mutex 内
// 调用 submit，这次回读会自锁死，因此它是"锁外提交"的可执行证据。
type reentrantStateSubmitter struct {
	inner StateSubmitter
	probe func()
}

func (s reentrantStateSubmitter) TrySubmit(op PersistenceOp) (PersistenceReceipt, error) {
	s.probe()
	return s.inner.TrySubmit(op)
}

// runWithinTimeout 把"可能自锁死"的调用变成可失败断言而不是挂起。
func runWithinTimeout(t *testing.T, name string, fn func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		fn()
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("%s 超时：提交很可能发生在组件 mutex 内", name)
	}
}

func TestFrozenStateLoaderMissingVsError(t *testing.T) {
	const threadID = "frozen-loader-truth"
	current := frozenTestMessages(3)

	t.Run("missing", func(t *testing.T) {
		frozen := NewFrozenStubs()
		calls := 0
		frozen.SetStateLoader(stateLoaderFunc(func(string) StateLoadResult {
			calls++
			return StateLoadResult{}
		}))
		result, failure := frozen.GetWithLoadResult(nil, threadID, current)
		if result != nil || failure.Failed {
			t.Fatalf("missing 应是正常冷启动: result=%+v failure=%+v", result, failure)
		}
		frozen.mu.RLock()
		loaded, found := frozen.loadedFromDB[threadID], frozen.stateFoundDB[threadID]
		frozen.mu.RUnlock()
		if !loaded || found {
			t.Fatalf("missing 后 loaded=%v found=%v, want true/false", loaded, found)
		}
		frozen.GetWithLoadResult(nil, threadID, current)
		if calls != 1 {
			t.Fatalf("missing 是终态，却查询了 %d 次", calls)
		}
	})

	for name, failureResult := range stateLoadErrorCases() {
		t.Run(name, func(t *testing.T) {
			frozen := NewFrozenStubs()
			calls := 0
			frozen.SetStateLoader(stateLoaderFunc(func(string) StateLoadResult {
				calls++
				return failureResult
			}))
			result, failure := frozen.GetWithLoadResult(nil, threadID, current)
			if result != nil {
				t.Fatalf("读取失败不得返回 frozen prefix: %+v", result)
			}
			if !failure.Failed || failure.Class != failureResult.FailureClass {
				t.Fatalf("failure=%+v, want class %q", failure, failureResult.FailureClass)
			}
			frozen.mu.RLock()
			loaded, found := frozen.loadedFromDB[threadID], frozen.stateFoundDB[threadID]
			frozen.mu.RUnlock()
			if loaded || found {
				t.Fatalf("读取失败被当成冷启动 missing: loaded=%v found=%v", loaded, found)
			}
			frozen.GetWithLoadResult(nil, threadID, current)
			if calls != 2 {
				t.Fatalf("读取失败后不可重试，查询次数=%d", calls)
			}
		})
	}
}

func TestFrozenStateLoaderFailClosedAndRetry(t *testing.T) {
	const threadID = "frozen-loader-retry"
	raw := frozenTestMessages(4)
	var persisted string
	producer := NewFrozenStubs()
	producer.SetPersistFunc(func(_ string, value string) { persisted = value })
	producer.Store(threadID, deepCopyMessages(raw[:2]), 3, raw[2], 20, 30)
	if persisted == "" {
		t.Fatal("producer 未产出持久化状态")
	}

	var calls int64
	restored := NewFrozenStubs()
	restored.SetStateLoader(stateLoaderFunc(func(string) StateLoadResult {
		if atomic.AddInt64(&calls, 1) == 1 {
			return StateLoadResult{Err: ErrStateLoadBusy, FailureClass: StateLoadFailureSQLiteBusy}
		}
		return StateLoadResult{Value: persisted, Found: true}
	}))
	if result, failure := restored.GetWithLoadResult(nil, threadID, raw); result != nil || !failure.Failed {
		t.Fatalf("busy 时应 fail closed: result=%+v failure=%+v", result, failure)
	}
	result, failure := restored.GetWithLoadResult(nil, threadID, raw)
	if failure.Failed || result == nil || result.Cutoff != 3 || len(result.Messages) != 2 {
		t.Fatalf("后续成功读取未恢复状态: result=%+v failure=%+v", result, failure)
	}
	if got := atomic.LoadInt64(&calls); got != 2 {
		t.Fatalf("loader 调用次数=%d, want 2", got)
	}

	// 并发等待方共享同一次失败结果，而不是把 error 悄悄折叠成 missing。
	blocked := NewFrozenStubs()
	loadStarted := make(chan struct{})
	releaseLoad := make(chan struct{})
	var blockedCalls int64
	blocked.SetStateLoader(stateLoaderFunc(func(string) StateLoadResult {
		if atomic.AddInt64(&blockedCalls, 1) == 1 {
			close(loadStarted)
			<-releaseLoad
		}
		return StateLoadResult{Err: ErrStateLoadClosed, FailureClass: StateLoadFailureSQLiteClosed}
	}))
	failures := make(chan StateLoadFailure, 2)
	go func() {
		_, got := blocked.GetWithLoadResult(nil, threadID, raw)
		failures <- got
	}()
	<-loadStarted
	go func() {
		_, got := blocked.GetWithLoadResult(nil, threadID, raw)
		failures <- got
	}()
	select {
	case got := <-failures:
		t.Fatalf("并发等待方在加载完成前返回: %+v", got)
	case <-time.After(50 * time.Millisecond):
	}
	close(releaseLoad)
	for i := 0; i < 2; i++ {
		got := <-failures
		if !got.Failed || got.Class != StateLoadFailureSQLiteClosed {
			t.Fatalf("并发等待方 failure=%+v, want closed", got)
		}
	}
	if got := atomic.LoadInt64(&blockedCalls); got != 1 {
		t.Fatalf("并发冷启动查询 %d 次, want 1", got)
	}
}

func TestFrozenMigrationTargetErrorStopsLegacy(t *testing.T) {
	const sessionID = "frozen-migrate"
	stateKey := historyEpochStateKey(sessionID, 1)
	raw := frozenTestMessages(4)
	var legacyValue string
	producer := NewFrozenStubs()
	producer.SetPersistFunc(func(_ string, value string) { legacyValue = value })
	producer.Store(sessionID, deepCopyMessages(raw[:2]), 3, raw[2], 20, 30)
	if legacyValue == "" {
		t.Fatal("producer 未产出 legacy 状态")
	}

	for name, failureResult := range stateLoadErrorCases() {
		t.Run(name, func(t *testing.T) {
			var queried []string
			frozen := NewFrozenStubs()
			frozen.SetStateLoader(stateLoaderFunc(func(key string) StateLoadResult {
				queried = append(queried, key)
				if key == "frozen:"+stateKey {
					return failureResult
				}
				return StateLoadResult{Value: legacyValue, Found: true}
			}))
			failure := frozen.MigrateLegacyState(nil, sessionID, stateKey)
			if !failure.Failed || failure.Class != failureResult.FailureClass {
				t.Fatalf("target 读取失败未上报: %+v", failure)
			}
			if len(queried) != 1 || queried[0] != "frozen:"+stateKey {
				t.Fatalf("target 失败后仍读取 legacy: %v", queried)
			}
			if frozen.LengthFor(stateKey) != 0 {
				t.Fatal("target 读取失败后仍复用旧状态")
			}
		})
	}

	// ErrNoRows 是 missing，既有 epoch 1 legacy migration 必须保持可用。
	var queried []string
	frozen := NewFrozenStubs()
	frozen.SetStateLoader(stateLoaderFunc(func(key string) StateLoadResult {
		queried = append(queried, key)
		if key == "frozen:"+stateKey {
			return StateLoadResult{}
		}
		return StateLoadResult{Value: legacyValue, Found: true}
	}))
	if failure := frozen.MigrateLegacyState(nil, sessionID, stateKey); failure.Failed {
		t.Fatalf("missing 不应被当作失败: %+v", failure)
	}
	if got := frozen.LengthFor(stateKey); got != 2 {
		t.Fatalf("epoch 1 legacy migration 未执行: len=%d", got)
	}
	if len(queried) != 2 {
		t.Fatalf("migration 查询序列=%v, want target 后再读 legacy", queried)
	}
}

func TestSawtoothStateLoaderMissingVsError(t *testing.T) {
	const threadID = "sawtooth-loader-truth"

	t.Run("missing", func(t *testing.T) {
		trigger := NewSawtoothTrigger(0, 100_000, 10_000)
		calls := 0
		trigger.SetStateLoader(stateLoaderFunc(func(string) StateLoadResult {
			calls++
			return StateLoadResult{}
		}))
		baseline, failure := trigger.PressureBaselineWithLoadResult(threadID)
		if failure.Failed || baseline.Available || baseline.ResetReason != baselineResetNoActual {
			t.Fatalf("missing baseline=%+v failure=%+v", baseline, failure)
		}
		trigger.PressureBaselineWithLoadResult(threadID)
		if calls != 1 {
			t.Fatalf("missing 是终态，却查询了 %d 次", calls)
		}
	})

	for name, failureResult := range stateLoadErrorCases() {
		t.Run(name, func(t *testing.T) {
			trigger := NewSawtoothTrigger(0, 100_000, 10_000)
			calls := 0
			trigger.SetStateLoader(stateLoaderFunc(func(string) StateLoadResult {
				calls++
				return failureResult
			}))
			baseline, failure := trigger.PressureBaselineWithLoadResult(threadID)
			if !failure.Failed || failure.Class != failureResult.FailureClass {
				t.Fatalf("failure=%+v, want class %q", failure, failureResult.FailureClass)
			}
			if baseline.Available || baseline.ResetReason != baselineResetStateLoadFailed {
				t.Fatalf("读取失败伪装成 %q: %+v", baseline.ResetReason, baseline)
			}
			if baseline.ActualTokens != 0 || baseline.MessageCount != 0 {
				t.Fatalf("读取失败后暴露了臆造 baseline: %+v", baseline)
			}
			trigger.mu.RLock()
			loaded, found := trigger.loadedFromDB[threadID], trigger.stateFoundDB[threadID]
			trigger.mu.RUnlock()
			if loaded || found {
				t.Fatalf("读取失败被当成冷启动 missing: loaded=%v found=%v", loaded, found)
			}
			trigger.PressureBaselineWithLoadResult(threadID)
			if calls != 2 {
				t.Fatalf("读取失败后不可重试，查询次数=%d", calls)
			}
		})
	}
}

func TestSawtoothLoadFailureUsesLocalFull(t *testing.T) {
	const threadID = "sawtooth-local-full"
	state, err := json.Marshal(persistedState{
		Tokens: 120_000, MsgCount: 2,
		SystemFingerprint:         strings.Repeat("a", 64),
		ToolsFingerprint:          strings.Repeat("b", 64),
		MessagesPrefixFingerprint: strings.Repeat("c", 64),
	})
	if err != nil {
		t.Fatal(err)
	}
	var calls int64
	trigger := NewSawtoothTrigger(0, 100_000, 10_000)
	trigger.SetStateLoader(stateLoaderFunc(func(string) StateLoadResult {
		if atomic.AddInt64(&calls, 1) == 1 {
			return StateLoadResult{Err: ErrStateLoadClosed, FailureClass: StateLoadFailureSQLiteClosed}
		}
		return StateLoadResult{Value: string(state), Found: true}
	}))

	baseline, failure := trigger.PressureBaselineWithLoadResult(threadID)
	if !failure.Failed {
		t.Fatalf("closed 未上报为读取失败: %+v", failure)
	}
	tokenCounter, err := NewTokenCounter()
	if err != nil {
		t.Fatal(err)
	}
	decision := buildPressureDecisionWithEntry(frozenTestMessages(2), nil, nil, baseline, tokenCounter, 100_000)
	if decision.Source != pressureSourceLocalFull {
		t.Fatalf("读取失败时 pressure source=%q, want local_full", decision.Source)
	}
	if decision.ResetReason != baselineResetStateLoadFailed {
		t.Fatalf("读取失败被记成 %q, 掩盖了真实原因", decision.ResetReason)
	}

	recovered, recoveredFailure := trigger.PressureBaselineWithLoadResult(threadID)
	if recoveredFailure.Failed || !recovered.Available || recovered.ActualTokens != 120_000 {
		t.Fatalf("后续成功读取未恢复 baseline: %+v failure=%+v", recovered, recoveredFailure)
	}
}

func TestSawtoothMigrationTargetErrorStopsLegacy(t *testing.T) {
	const sessionID = "sawtooth-migrate"
	stateKey := historyEpochStateKey(sessionID, 1)
	legacyValue, err := json.Marshal(persistedState{
		Tokens: 55_000, MsgCount: 3,
		SystemFingerprint:         strings.Repeat("d", 64),
		ToolsFingerprint:          strings.Repeat("e", 64),
		MessagesPrefixFingerprint: strings.Repeat("f", 64),
	})
	if err != nil {
		t.Fatal(err)
	}

	for name, failureResult := range stateLoadErrorCases() {
		t.Run(name, func(t *testing.T) {
			var queried []string
			trigger := NewSawtoothTrigger(0, 100_000, 10_000)
			trigger.SetStateLoader(stateLoaderFunc(func(key string) StateLoadResult {
				queried = append(queried, key)
				if key == "sawtooth:"+stateKey {
					return failureResult
				}
				return StateLoadResult{Value: string(legacyValue), Found: true}
			}))
			failure := trigger.MigrateLegacyState(sessionID, stateKey)
			if !failure.Failed || failure.Class != failureResult.FailureClass {
				t.Fatalf("target 读取失败未上报: %+v", failure)
			}
			if len(queried) != 1 || queried[0] != "sawtooth:"+stateKey {
				t.Fatalf("target 失败后仍读取 legacy: %v", queried)
			}
			trigger.mu.RLock()
			migrated := trigger.hasStateLocked(stateKey)
			trigger.mu.RUnlock()
			if migrated {
				t.Fatal("target 读取失败后仍复用 legacy 状态")
			}
		})
	}

	var queried []string
	trigger := NewSawtoothTrigger(0, 100_000, 10_000)
	trigger.SetStateLoader(stateLoaderFunc(func(key string) StateLoadResult {
		queried = append(queried, key)
		if key == "sawtooth:"+stateKey {
			return StateLoadResult{}
		}
		return StateLoadResult{Value: string(legacyValue), Found: true}
	}))
	if failure := trigger.MigrateLegacyState(sessionID, stateKey); failure.Failed {
		t.Fatalf("missing 不应被当作失败: %+v", failure)
	}
	baseline, failure := trigger.PressureBaselineWithLoadResult(stateKey)
	if failure.Failed || !baseline.Available || baseline.ActualTokens != 55_000 {
		t.Fatalf("epoch 1 legacy migration 未执行: %+v failure=%+v", baseline, failure)
	}
	if len(queried) != 2 {
		t.Fatalf("migration 查询序列=%v, want target 后再读 legacy", queried)
	}
}

func TestFrozenAndSawtoothResultBearingPersistence(t *testing.T) {
	const threadID = "result-bearing"
	raw := frozenTestMessages(4)
	backend := newPersistenceFakeBackend()
	writer, _, _ := newPersistenceWriterTest(t, backend)
	defer func() { _ = writer.CloseAndDrain() }()

	frozen := NewFrozenStubs()
	trigger := NewSawtoothTrigger(0, 100_000, 10_000)

	// 提交必须发生在组件 mutex 之外：submitter 回读组件状态，锁内提交会自锁死。
	frozen.SetStateSubmitter(reentrantStateSubmitter{inner: writer, probe: func() { frozen.LengthFor(threadID) }})
	trigger.SetStateSubmitter(reentrantStateSubmitter{inner: writer, probe: func() { trigger.GetRequestSeq(threadID) }})

	// 1. 磁盘阻塞时内存先行，当前请求不被 SQLite 拖住。
	started := backend.blockKey("frozen:" + threadID)
	blockedProbe := newPersistenceProbe(t, 1)
	var blockedReceipt PersistenceReceipt
	runWithinTimeout(t, "Frozen 阻塞写入", func() {
		blockedReceipt = frozen.StoreWithResult(nil, threadID, deepCopyMessages(raw[:1]), 2, raw[1], 10, 20, blockedProbe.completion)
	})
	if !blockedReceipt.Accepted {
		t.Fatalf("阻塞写入未被接受: %+v", blockedReceipt)
	}
	if frozen.LengthFor(threadID) != 1 {
		t.Fatal("内存状态未先于磁盘更新")
	}
	<-started
	if got := blockedProbe.diskState(); got == persistenceStateSaved {
		t.Fatalf("SQLite 尚未返回就记为 saved: %q", got)
	}
	backend.releaseAll()
	waitForPersistence(t, func() bool { return blockedProbe.diskState() == persistenceStateSaved })

	// 2. SQLite 失败必须如实进入 request closure，且不阻塞内存更新。
	backend.failKey("frozen:"+threadID, true)
	failedProbe := newPersistenceProbe(t, 2)
	runWithinTimeout(t, "Frozen 失败写入", func() {
		frozen.StoreWithResult(nil, threadID, deepCopyMessages(raw[:2]), 3, raw[2], 20, 30, failedProbe.completion)
	})
	waitForPersistence(t, func() bool { return failedProbe.diskState() == persistenceStateFailed })
	if got := failedProbe.failureClass(); got != persistenceFailureSQLite {
		t.Fatalf("失败类别=%q, want sqlite", got)
	}
	if frozen.LengthFor(threadID) != 2 {
		t.Fatal("磁盘失败不应回滚内存状态")
	}

	// 3. Sawtooth baseline 同样 result-bearing。
	baselineProbe := newPersistenceProbe(t, 3)
	var accepted bool
	var baselineReceipt PersistenceReceipt
	runWithinTimeout(t, "Sawtooth baseline 写入", func() {
		accepted, baselineReceipt = trigger.UpdatePressureBaselineWithResult(threadID, 0, 77_000, 4,
			strings.Repeat("a", 64), strings.Repeat("b", 64), strings.Repeat("c", 64), baselineProbe.completion)
	})
	if !accepted || !baselineReceipt.Accepted {
		t.Fatalf("baseline 写入未被接受: accepted=%v receipt=%+v", accepted, baselineReceipt)
	}
	waitForPersistence(t, func() bool { return baselineProbe.diskState() == persistenceStateSaved })
	if got := trigger.PressureBaseline(threadID); !got.Available || got.ActualTokens != 77_000 {
		t.Fatalf("baseline 内存状态=%+v", got)
	}

	// 4. 兼容 void callback 不得产生 saved 事实。
	legacy := NewFrozenStubs()
	legacy.SetPersistFunc(func(string, string) {})
	legacyProbe := newPersistenceProbe(t, 4)
	receipt := legacy.StoreWithResult(nil, threadID, deepCopyMessages(raw[:1]), 2, raw[1], 10, 20, legacyProbe.completion)
	if receipt.Result.State == persistenceStateSaved || legacyProbe.diskState() == persistenceStateSaved {
		t.Fatalf("兼容 void callback 伪造了 saved: receipt=%+v disk=%q", receipt, legacyProbe.diskState())
	}
}

func TestFrozenFullRawPrefixRejectsEarlyEditWithStableBoundary(t *testing.T) {
	raw := []Message{
		{Role: "user", Content: mustMarshal("early-original")},
		{Role: "assistant", Content: mustMarshal("middle-original")},
		{Role: "user", Content: mustMarshal("boundary")},
		{Role: "assistant", Content: mustMarshal("tail")},
	}
	frozen := NewFrozenStubs()
	frozen.StoreWithLogger(nil, "full-prefix", []Message{{Role: "assistant", Content: mustMarshal("stubbed")}}, 3, raw[2], 10, 100, raw)

	changed := deepCopyMessages(raw)
	changed[0].Content = mustMarshal("early-edited")
	if got := frozen.Get("full-prefix", changed); got != nil {
		t.Fatal("cutoff 前早期消息变化且 boundary 不变时不应命中 Frozen")
	}
}

func TestFrozenFullRawPrefixAcceptsKnownWireEquivalent(t *testing.T) {
	raw := []Message{
		{Role: "user", Content: mustMarshal("hello")},
		{Role: "assistant", Content: json.RawMessage(`[{"type":"text","text":"answer","cache_control":{"type":"ephemeral"}}]`)},
		{Role: "user", Content: mustMarshal("boundary")},
	}
	frozen := NewFrozenStubs()
	frozen.Store("wire-equivalent", []Message{{Role: "assistant", Content: mustMarshal("stubbed")}}, 3, raw[2], 10, 100, raw)

	equivalent := deepCopyMessages(raw)
	equivalent[1].Content = json.RawMessage(`"answer"`)
	if got := frozen.Get("wire-equivalent", equivalent); got == nil {
		t.Fatal("纯字符串与单 text block 等价形态不应使 Frozen 失效")
	}
}

func TestFrozenFullRawPrefixRejectsBusinessPayloadChanges(t *testing.T) {
	raw := []Message{
		{Role: "user", Content: json.RawMessage(`[{"type":"tool_use","id":"tool-1","name":"read","input":{"path":"a"}}]`)},
		{Role: "user", Content: json.RawMessage(`[{"type":"tool_result","tool_use_id":"tool-1","content":"same"}]`)},
		{Role: "assistant", Content: mustMarshal("boundary")},
	}
	frozen := NewFrozenStubs()
	frozen.Store("business-payload", []Message{{Role: "assistant", Content: mustMarshal("stubbed")}}, 3, raw[2], 10, 100, raw)

	cases := []struct {
		name   string
		mutate func([]Message)
	}{
		{name: "tool input", mutate: func(messages []Message) {
			messages[0].Content = json.RawMessage(`[{"type":"tool_use","id":"tool-1","name":"read","input":{"path":"b"}}]`)
		}},
		{name: "tool result", mutate: func(messages []Message) {
			messages[1].Content = json.RawMessage(`[{"type":"tool_result","tool_use_id":"tool-1","content":"changed"}]`)
		}},
		{name: "unknown field", mutate: func(messages []Message) {
			messages[0].Content = json.RawMessage(`[{"type":"tool_use","id":"tool-1","name":"read","input":{"path":"a"},"future_semantic":true}]`)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			current := deepCopyMessages(raw)
			tc.mutate(current)
			if got := frozen.Get("business-payload", current); got != nil {
				t.Fatal("业务 payload 变化后 Frozen 仍命中")
			}
		})
	}
}

func TestFrozenLegacyPersistedStateWithoutRawPrefixHashFailsClosed(t *testing.T) {
	raw := frozenTestMessages(3)
	prefix := deepCopyMessages(raw[:1])
	prefixJSON, err := json.Marshal(prefix)
	if err != nil {
		t.Fatal(err)
	}
	persisted, err := json.Marshal(frozenPersisted{
		Messages: prefix, Cutoff: 2,
		BoundaryHash: stableBoundaryHash(raw[1]),
		PrefixHash:   sha256hex(prefixJSON), Tokens: 10, RawTokens: 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	deleted := 0
	frozen := NewFrozenStubs()
	frozen.SetLoadFunc(func(string) (string, bool) { return string(persisted), true })
	frozen.SetDeleteFunc(func(string) { deleted++ })
	if got := frozen.Get("legacy-prefix", raw); got != nil {
		t.Fatal("缺少完整 raw prefix hash 的旧状态不得命中")
	}
	if deleted != 1 {
		t.Fatalf("旧 schema 失效后删除次数=%d，want 1", deleted)
	}
}

func TestFrozenThinkingSignatureChangeMissesButDoesNotNeedBoundaryGuess(t *testing.T) {
	raw := []Message{
		{Role: "assistant", Content: json.RawMessage(`[{"type":"thinking","thinking":"one","signature":"sig-1"}]`)},
		{Role: "user", Content: mustMarshal("boundary")},
	}
	frozen := NewFrozenStubs()
	frozen.Store("thinking-signature", []Message{{Role: "assistant", Content: mustMarshal("stubbed")}}, 2, raw[1], 10, 100, raw)
	changed := deepCopyMessages(raw)
	changed[0].Content = json.RawMessage(`[{"type":"thinking","thinking":"two","signature":"sig-2"}]`)
	if got := frozen.Get("thinking-signature", changed); got != nil {
		t.Fatal("thinking/signature 变化后 Frozen 不应仅凭 boundary 命中")
	}
}

func TestFrozenFullRawPrefixPersistsAndRestores(t *testing.T) {
	raw := frozenTestMessages(4)
	persisted := make(map[string]string)
	source := NewFrozenStubs()
	source.SetPersistFunc(func(key, value string) { persisted[key] = value })
	source.Store("persisted-full", raw[:1], 3, raw[2], 10, 100, raw)

	var state frozenPersisted
	if err := json.Unmarshal([]byte(persisted["frozen:persisted-full"]), &state); err != nil {
		t.Fatalf("解析 Frozen 持久状态: %v", err)
	}
	if state.RawPrefixMode != frozenRawPrefixModeFull || !validPressureFingerprint(state.RawPrefixHash) {
		t.Fatalf("持久化 raw prefix proof 不完整: mode=%q hash=%q", state.RawPrefixMode, state.RawPrefixHash)
	}

	restored := NewFrozenStubs()
	restored.SetLoadFunc(func(key string) (string, bool) {
		value, ok := persisted[key]
		return value, ok
	})
	if got := restored.Get("persisted-full", raw); got == nil {
		t.Fatal("完整 raw prefix proof 冷启动后未命中")
	}
}

func TestTriggerEvaluation(t *testing.T) {
	now := time.Now()
	tests := []struct {
		name             string
		selectedPressure int
		lastRequestAgo   time.Duration
		wantReason       TriggerReason
		wantActualKnown  bool
	}{
		{
			name:             "emergency 优先于 token 与 pause",
			selectedPressure: 11_001,
			lastRequestAgo:   5 * time.Minute,
			wantReason:       TriggerEmergency,
			wantActualKnown:  true,
		},
		{
			name:             "tokens 优先于 pause",
			selectedPressure: 1_001,
			lastRequestAgo:   5 * time.Minute,
			wantReason:       TriggerTokens,
			wantActualKnown:  true,
		},
		{
			name:             "pause 使用本次等待快照",
			selectedPressure: 501,
			lastRequestAgo:   5 * time.Minute,
			wantReason:       TriggerPause,
			wantActualKnown:  true,
		},
		{
			name:             "minimum 相等时不触发 pause",
			selectedPressure: 500,
			lastRequestAgo:   5 * time.Minute,
			wantReason:       TriggerNone,
			wantActualKnown:  true,
		},
		{
			name:             "没有历史时间时 actual wait unavailable",
			selectedPressure: 501,
			wantReason:       TriggerNone,
			wantActualKnown:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			trigger := NewSawtoothTrigger(CacheGapForTTL("ephemeral"), 1_000, 500)
			if tt.lastRequestAgo > 0 {
				trigger.mu.Lock()
				trigger.lastRequestTime["thread"] = now.Add(-tt.lastRequestAgo)
				trigger.mu.Unlock()
			}

			evaluation := trigger.Evaluate("thread", tt.selectedPressure, now)
			if evaluation.Reason != tt.wantReason {
				t.Fatalf("Evaluate reason=%q, want %q", evaluation.Reason, tt.wantReason)
			}
			if evaluation.RequiredWait != 4*time.Minute {
				t.Fatalf("RequiredWait=%s, want 4m", evaluation.RequiredWait)
			}
			if evaluation.ActualWaitKnown != tt.wantActualKnown {
				t.Fatalf("ActualWaitKnown=%v, want %v", evaluation.ActualWaitKnown, tt.wantActualKnown)
			}
			if tt.wantActualKnown {
				if evaluation.ActualWait != tt.lastRequestAgo {
					t.Fatalf("ActualWait=%s, want %s", evaluation.ActualWait, tt.lastRequestAgo)
				}
			} else if evaluation.ActualWait != 0 {
				t.Fatalf("unknown ActualWait=%s, want zero", evaluation.ActualWait)
			}
			if evaluation.SelectedPressure != tt.selectedPressure {
				t.Fatalf("SelectedPressure=%d, want %d", evaluation.SelectedPressure, tt.selectedPressure)
			}
			if evaluation.EmergencyThreshold != 11_000 || evaluation.TokenThreshold != 1_000 || evaluation.TokenMinimum != 500 {
				t.Fatalf("threshold snapshot=%+v", evaluation)
			}
			if got := trigger.ShouldTrigger("thread", tt.selectedPressure); got != tt.wantReason {
				t.Fatalf("ShouldTrigger=%q, want %q", got, tt.wantReason)
			}
		})
	}
}

func TestTriggerEvaluationStrictPriority(t *testing.T) {
	now := time.Now()
	tests := []struct {
		name             string
		selectedPressure int
		lastRequestAgo   time.Duration
		wantReason       TriggerReason
	}{
		{name: "超过 emergency", selectedPressure: 11_001, lastRequestAgo: 5 * time.Minute, wantReason: TriggerEmergency},
		{name: "等于 emergency 后落到 tokens", selectedPressure: 11_000, lastRequestAgo: 5 * time.Minute, wantReason: TriggerTokens},
		{name: "超过 tokens", selectedPressure: 1_001, lastRequestAgo: 5 * time.Minute, wantReason: TriggerTokens},
		{name: "等于 tokens 后落到 pause", selectedPressure: 1_000, lastRequestAgo: 5 * time.Minute, wantReason: TriggerPause},
		{name: "等于 minimum", selectedPressure: 500, lastRequestAgo: 5 * time.Minute, wantReason: TriggerNone},
		{name: "等于 required wait", selectedPressure: 501, lastRequestAgo: 4 * time.Minute, wantReason: TriggerNone},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			trigger := NewSawtoothTrigger(4*time.Minute, 1_000, 500)
			trigger.mu.Lock()
			trigger.lastRequestTime["thread"] = now.Add(-tt.lastRequestAgo)
			trigger.mu.Unlock()

			if got := trigger.Evaluate("thread", tt.selectedPressure, now).Reason; got != tt.wantReason {
				t.Fatalf("reason=%q, want %q", got, tt.wantReason)
			}
		})
	}
}

func TestTriggerEvaluationCurrentAndNextTTL(t *testing.T) {
	now := time.Now()
	trigger := NewSawtoothTrigger(CacheGapForTTL("ephemeral"), 1_000, 500)
	trigger.mu.Lock()
	trigger.lastRequestTime["thread"] = now.Add(-5 * time.Minute)
	trigger.mu.Unlock()

	current := trigger.Evaluate("thread", 501, now)
	trigger.SetPauseThreshold(CacheGapForTTL("1h"))
	next := trigger.Evaluate("thread", 501, now)

	if current.Reason != TriggerPause || current.RequiredWait != 4*time.Minute || current.ActualWait != 5*time.Minute || !current.ActualWaitKnown {
		t.Fatalf("current evaluation=%+v", current)
	}
	if next.Reason != TriggerNone || next.RequiredWait != 61*time.Minute || next.ActualWait != 5*time.Minute || !next.ActualWaitKnown {
		t.Fatalf("next evaluation=%+v", next)
	}
	if current.RequiredWait != 4*time.Minute {
		t.Fatalf("TTL 更新污染已完成 evaluation: %+v", current)
	}

	trigger.mu.Lock()
	trigger.lastRequestTime["thread"] = now.Add(-62 * time.Minute)
	trigger.mu.Unlock()
	oneHour := trigger.Evaluate("thread", 501, now)
	if oneHour.Reason != TriggerPause || oneHour.RequiredWait != 61*time.Minute || oneHour.ActualWait != 62*time.Minute {
		t.Fatalf("1h evaluation=%+v", oneHour)
	}
}

func TestCacheGapForTTL(t *testing.T) {
	tests := []struct {
		cacheTTL string
		want     time.Duration
	}{
		{cacheTTL: "ephemeral", want: 4 * time.Minute},
		{cacheTTL: "1h", want: 61 * time.Minute},
		{cacheTTL: "", want: 4 * time.Minute},
	}

	for _, tt := range tests {
		t.Run(tt.cacheTTL, func(t *testing.T) {
			if got := CacheGapForTTL(tt.cacheTTL); got != tt.want {
				t.Fatalf("CacheGapForTTL(%q)=%s, want %s", tt.cacheTTL, got, tt.want)
			}
		})
	}
}

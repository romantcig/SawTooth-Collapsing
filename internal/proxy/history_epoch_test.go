package proxy

import (
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestHistoryCanonicalReuseSafetySchemaBoundaries(t *testing.T) {
	tests := []struct {
		name       string
		before     string
		after      string
		reuseEqual bool
		inputEqual bool
	}{
		{
			name:       "content block cache_control",
			before:     `[{"role":"user","content":[{"type":"text","text":"same","cache_control":{"type":"ephemeral"}}]}]`,
			after:      `[{"role":"user","content":[{"cache_control":{"ttl":"1h","type":"ephemeral"},"text":"same","type":"text"}]}]`,
			reuseEqual: true,
			inputEqual: true,
		},
		{
			name:       "string and single text block",
			before:     `[{"role":"user","content":"same"}]`,
			after:      `[{"role":"user","content":[{"type":"text","text":"same","cache_control":{"type":"ephemeral"}}]}]`,
			reuseEqual: true,
			inputEqual: true,
		},
		{
			name:       "tool_use input cache_control is business data",
			before:     `[{"role":"assistant","content":[{"type":"tool_use","id":"t1","name":"configure","input":{"cache_control":"enabled"}}]}]`,
			after:      `[{"role":"assistant","content":[{"type":"tool_use","id":"t1","name":"configure","input":{"cache_control":"disabled"}}]}]`,
			reuseEqual: false,
			inputEqual: false,
		},
		{
			name:       "tool_result content cache_control is business data",
			before:     `[{"role":"user","content":[{"type":"tool_result","tool_use_id":"t1","content":{"cache_control":"enabled"}}]}]`,
			after:      `[{"role":"user","content":[{"type":"tool_result","tool_use_id":"t1","content":{"cache_control":"disabled"}}]}]`,
			reuseEqual: false,
			inputEqual: false,
		},
		{
			name:       "source document remains semantic",
			before:     `[{"role":"user","content":[{"type":"document","source":{"type":"text","data":"before","cache_control":"enabled"}}]}]`,
			after:      `[{"role":"user","content":[{"type":"document","source":{"type":"text","data":"after","cache_control":"enabled"}}]}]`,
			reuseEqual: false,
			inputEqual: false,
		},
		{
			name:       "message unknown field cache_control is business data",
			before:     `[{"role":"user","content":"same","future":{"cache_control":"enabled"}}]`,
			after:      `[{"role":"user","content":"same","future":{"cache_control":"disabled"}}]`,
			reuseEqual: false,
			inputEqual: false,
		},
		{
			name:       "unknown content block cache_control is business data",
			before:     `[{"role":"user","content":[{"type":"future_block","cache_control":"enabled","value":"same"}]}]`,
			after:      `[{"role":"user","content":[{"type":"future_block","cache_control":"disabled","value":"same"}]}]`,
			reuseEqual: false,
			inputEqual: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			before := historyMessagesFromJSON(t, tt.before)
			after := historyMessagesFromJSON(t, tt.after)
			beforePrints, err := canonicalizeHistory(before)
			if err != nil {
				t.Fatalf("canonicalize before: %v", err)
			}
			afterPrints, err := canonicalizeHistory(after)
			if err != nil {
				t.Fatalf("canonicalize after: %v", err)
			}
			if got := beforePrints.ReuseHash == afterPrints.ReuseHash; got != tt.reuseEqual {
				t.Fatalf("reuse equality=%t, want %t", got, tt.reuseEqual)
			}
			if got := beforePrints.InputHash == afterPrints.InputHash; got != tt.inputEqual {
				t.Fatalf("input equality=%t, want %t", got, tt.inputEqual)
			}
		})
	}
}

func TestHistoryReuseTransportEquivalenceDoesNotAdvanceEpoch(t *testing.T) {
	manager := NewHistoryEpochManager()
	const sessionID = "history-transport"
	before := historyMessagesFromJSON(t, `[
		{"role":"user","content":"prompt"},
		{"role":"assistant","content":[{"type":"thinking","thinking":"before","signature":"sig-before"},{"type":"text","text":"answer"}]},
		{"role":"user","content":[{"type":"tool_result","tool_use_id":"t1","content":"same"}]}
	]`)
	after := historyMessagesFromJSON(t, `[
		{"role":"user","content":[{"type":"text","text":"prompt","cache_control":{"type":"ephemeral"}}]},
		{"role":"assistant","content":[{"type":"thinking","thinking":"after","signature":"sig-after","input":null},{"type":"text","text":"answer","input":null}]},
		{"role":"user","content":[{"type":"tool_result","tool_use_id":"t1","input":null,"content":"same"}]}
	]`)

	first := manager.Begin(sessionID, before)
	second := manager.Begin(sessionID, after)
	if first.Epoch != 1 || second.Epoch != first.Epoch {
		t.Fatalf("transport-only change advanced epoch: first=%+v second=%+v", first, second)
	}
	if second.ReuseSafe {
		t.Fatal("thinking/signature/input:null change must invalidate exact reuse")
	}
	if second.CommonPrefix != len(before) || second.Reason != HistoryEpochReasonReuseChanged {
		t.Fatalf("transport decision=%+v", second)
	}
	if !manager.IsCurrent(sessionID, second.Epoch) {
		t.Fatal("unchanged epoch is not current")
	}
}

func TestHistoryBoundaryEpochTransitionsAndCommonPrefix(t *testing.T) {
	manager := NewHistoryEpochManager()
	const sessionID = "history-boundary"
	base := historyTextMessages("zero", "one", "two")

	initial := manager.Begin(sessionID, base)
	if initial.Epoch != 1 || initial.CommonPrefix != 0 || initial.StateKey == "" || !initial.ReuseSafe {
		t.Fatalf("initial decision=%+v", initial)
	}
	appended := manager.Begin(sessionID, append(deepCopyMessages(base), Message{Role: "assistant", Content: mustMarshal("three")}))
	if appended.Epoch != 1 || appended.CommonPrefix != len(base) || appended.Reason != HistoryEpochReasonAppended || !appended.ReuseSafe {
		t.Fatalf("append decision=%+v", appended)
	}

	shrunk := manager.Begin(sessionID, base[:2])
	if shrunk.Epoch != 2 || shrunk.CommonPrefix != 2 || shrunk.Reason != HistoryEpochReasonShrunk || shrunk.StateKey == appended.StateKey {
		t.Fatalf("shrink decision=%+v", shrunk)
	}

	middleEdited := deepCopyMessages(base[:2])
	middleEdited[1].Content = mustMarshal("edited-one")
	edited := manager.Begin(sessionID, middleEdited)
	if edited.Epoch != 3 || edited.CommonPrefix != 1 || edited.Reason != HistoryEpochReasonChanged {
		t.Fatalf("middle edit decision=%+v", edited)
	}

	firstEdited := deepCopyMessages(middleEdited)
	firstEdited[0].Content = mustMarshal("edited-zero")
	first := manager.Begin(sessionID, firstEdited)
	if first.Epoch != 4 || first.CommonPrefix != 0 || first.Reason != HistoryEpochReasonChanged {
		t.Fatalf("first edit decision=%+v", first)
	}
}

func TestHistoryEmptyTransitionsAreExplicit(t *testing.T) {
	manager := NewHistoryEpochManager()
	const sessionID = "history-empty"

	empty := manager.Begin(sessionID, nil)
	if empty.Epoch != 1 || empty.Reason != HistoryEpochReasonInitial || !empty.ReuseSafe {
		t.Fatalf("initial empty=%+v", empty)
	}
	unchanged := manager.Begin(sessionID, []Message{})
	if unchanged.Epoch != 1 || unchanged.Reason != HistoryEpochReasonUnchanged {
		t.Fatalf("unchanged empty=%+v", unchanged)
	}
	nonEmpty := manager.Begin(sessionID, historyTextMessages("first"))
	if nonEmpty.Epoch != 1 || nonEmpty.Reason != HistoryEpochReasonAppended || nonEmpty.CommonPrefix != 0 {
		t.Fatalf("empty append=%+v", nonEmpty)
	}
	backToEmpty := manager.Begin(sessionID, nil)
	if backToEmpty.Epoch != 2 || backToEmpty.Reason != HistoryEpochReasonShrunk || backToEmpty.CommonPrefix != 0 {
		t.Fatalf("non-empty to empty=%+v", backToEmpty)
	}
}

func TestHistoryEncodingAndUnknownStatesRemainSemantic(t *testing.T) {
	orderedA := historyMessagesFromJSON(t, `[{"role":"user","content":[{"type":"tool_result","tool_use_id":"t1","content":{"a":1,"b":{"x":2,"y":3}}}]}]`)
	orderedB := historyMessagesFromJSON(t, `[{"content":[{"content":{"b":{"y":3,"x":2},"a":1},"tool_use_id":"t1","type":"tool_result"}],"role":"user"}]`)
	a, err := canonicalizeHistory(orderedA)
	if err != nil {
		t.Fatal(err)
	}
	b, err := canonicalizeHistory(orderedB)
	if err != nil {
		t.Fatal(err)
	}
	if a.ReuseHash != b.ReuseHash || a.InputHash != b.InputHash {
		t.Fatal("JSON object key order changed canonical fingerprints")
	}

	utf8A := historyTextMessages("你好，世界🌲")
	utf8B := historyTextMessages("你好，世界🌳")
	printA, _ := canonicalizeHistory(utf8A)
	printB, _ := canonicalizeHistory(utf8B)
	if printA.InputHash == printB.InputHash {
		t.Fatal("different UTF-8 text collapsed to the same input fingerprint")
	}

	absent := historyMessagesFromJSON(t, `[{"role":"assistant","content":[{"type":"text","text":"same"}]}]`)
	nullState := historyMessagesFromJSON(t, `[{"role":"assistant","content":[{"type":"text","text":"same","future":null}]}]`)
	absentPrint, _ := canonicalizeHistory(absent)
	nullPrint, _ := canonicalizeHistory(nullState)
	if absentPrint.InputHash == nullPrint.InputHash {
		t.Fatal("unknown absent/null states were treated as equivalent")
	}

	invalid := []Message{{Role: "user", Content: json.RawMessage(`{"broken":`)}}
	if _, err := canonicalizeHistory(invalid); err == nil {
		t.Fatal("invalid content unexpectedly canonicalized")
	}
	manager := NewHistoryEpochManager()
	valid := manager.Begin("invalid", absent)
	failed := manager.Begin("invalid", invalid)
	if failed.Epoch <= valid.Epoch || failed.Reason != HistoryEpochReasonInvalid || failed.ReuseSafe {
		t.Fatalf("invalid history did not fail closed: valid=%+v failed=%+v", valid, failed)
	}
}

func TestHistoryPrecisionKeepsLargeIntegersDistinct(t *testing.T) {
	before := historyMessagesFromJSON(t, `[{"role":"assistant","content":[{"type":"tool_use","id":"t1","name":"number","input":{"value":9007199254740992}}]}]`)
	after := historyMessagesFromJSON(t, `[{"role":"assistant","content":[{"type":"tool_use","id":"t1","name":"number","input":{"value":9007199254740993}}]}]`)
	beforePrints, err := canonicalizeHistory(before)
	if err != nil {
		t.Fatal(err)
	}
	afterPrints, err := canonicalizeHistory(after)
	if err != nil {
		t.Fatal(err)
	}
	if beforePrints.ReuseHash == afterPrints.ReuseHash || beforePrints.InputHash == afterPrints.InputHash {
		t.Fatal("integers above 2^53 lost precision")
	}
}

func TestHistoryEpochPersistenceRoundTrip(t *testing.T) {
	persisted := make(map[string]string)
	manager := NewHistoryEpochManager()
	manager.SetPersistFunc(func(key, value string) { persisted[key] = value })
	base := historyTextMessages("zero", "one", "two")
	first := manager.Begin("persisted", base)
	if first.Epoch != 1 {
		t.Fatalf("first=%+v", first)
	}
	branched := deepCopyMessages(base)
	branched[1].Content = mustMarshal("changed")
	second := manager.Begin("persisted", branched)
	if second.Epoch != 2 {
		t.Fatalf("second=%+v", second)
	}

	restored := NewHistoryEpochManager()
	restored.SetLoadFunc(func(key string) (string, bool) {
		value, ok := persisted[key]
		return value, ok
	})
	decision := restored.Begin("persisted", append(branched, Message{Role: "user", Content: mustMarshal("tail")}))
	if decision.Epoch != second.Epoch || decision.CommonPrefix != len(branched) || decision.StateKey != second.StateKey {
		t.Fatalf("restored decision=%+v, previous=%+v", decision, second)
	}
	for key, value := range persisted {
		if strings.Contains(key, "persisted") {
			t.Fatalf("history epoch persistence key leaked session plaintext: %q", key)
		}
		if strings.Contains(value, "zero") || strings.Contains(value, "changed") || strings.Contains(value, "persisted") {
			t.Fatalf("persisted history state leaked plaintext at %q: %s", key, value)
		}
	}
}

func TestHistoryEpochConcurrentBeginsStayMonotonic(t *testing.T) {
	manager := NewHistoryEpochManager()
	const sessionID = "history-concurrent"
	base := historyTextMessages("zero", "one")

	const workers = 32
	results := make(chan HistoryEpochDecision, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- manager.Begin(sessionID, deepCopyMessages(base))
		}()
	}
	wg.Wait()
	close(results)
	for result := range results {
		if result.Epoch != 1 || result.StateKey == "" {
			t.Fatalf("identical concurrent begin=%+v", result)
		}
	}

	left := deepCopyMessages(base)
	left[1].Content = mustMarshal("left")
	right := deepCopyMessages(base)
	right[1].Content = mustMarshal("right")
	branchResults := make(chan HistoryEpochDecision, 2)
	go func() { branchResults <- manager.Begin(sessionID, left) }()
	go func() { branchResults <- manager.Begin(sessionID, right) }()
	one := <-branchResults
	two := <-branchResults
	if one.Epoch == two.Epoch || one.Epoch < 2 || two.Epoch < 2 {
		t.Fatalf("concurrent branch epochs are not monotonic: one=%+v two=%+v", one, two)
	}
	current := one
	stale := two
	if two.Epoch > one.Epoch {
		current, stale = two, one
	}
	if !manager.IsCurrent(sessionID, current.Epoch) || manager.IsCurrent(sessionID, stale.Epoch) {
		t.Fatalf("current/stale epoch check failed: current=%+v stale=%+v", current, stale)
	}
}

func TestForwardedCoordinatesUsePositionSensitiveHistoryFingerprint(t *testing.T) {
	base := historyMessagesFromJSON(t, `[{"role":"assistant","content":[{"type":"tool_use","id":"t1","name":"configure","input":{"cache_control":"enabled"}}]}]`)
	meta := newRequestMeta(901, "forwarded-fingerprint")
	meta.PressureDecision = pressureDecision{
		Available:                 true,
		MessageCount:              len(base),
		MessagesPrefixFingerprint: fingerprintMessagesPrefix(base, len(base)),
	}
	changed := historyMessagesFromJSON(t, `[{"role":"assistant","content":[{"type":"tool_use","id":"t1","name":"configure","input":{"cache_control":"disabled"}}]}]`)
	body, err := json.Marshal(map[string]any{"messages": changed})
	if err != nil {
		t.Fatal(err)
	}
	markForwardedPressureCoordinates(meta, body)
	if !meta.PressureDecision.ForwardedCoordinatesChanged {
		t.Fatal("tool_use.input.cache_control 业务变化未使 forwarded 坐标失效")
	}
}

func TestHistoryEpochStateIsolationAndLateResponse(t *testing.T) {
	manager := NewHistoryEpochManager()
	trigger := NewSawtoothTrigger(time.Minute, 100_000, 50_000)
	server := NewServer(Config{})
	server.HistoryEpoch = manager
	server.Sawtooth = trigger

	base := historyTextMessages("zero", "one")
	old := manager.Begin("late-response", base)
	oldGeneration := trigger.BeginPressureRequest(old.StateKey)
	oldFingerprint := reuseSafetyPrefixHash(base, len(base))
	if !trigger.UpdatePressureBaselineForRequest(old.StateKey, oldGeneration, 88_000, len(base), oldFingerprint, oldFingerprint, oldFingerprint) {
		t.Fatal("old epoch baseline 未写入")
	}

	branched := deepCopyMessages(base)
	branched[1].Content = mustMarshal("new branch")
	current := manager.Begin("late-response", branched)
	if current.Epoch <= old.Epoch || current.StateKey == old.StateKey {
		t.Fatalf("branch did not allocate a new state key: old=%+v current=%+v", old, current)
	}
	currentGeneration := trigger.BeginPressureRequest(current.StateKey)
	if !trigger.UpdatePressureBaselineForRequest(current.StateKey, currentGeneration, 12_000, len(branched), currentHistoryFingerprint(branched), currentHistoryFingerprint(branched), currentHistoryFingerprint(branched)) {
		t.Fatal("current epoch baseline 未写入")
	}

	meta := newRequestMeta(902, "late-response")
	meta.HistoryEpoch = old.Epoch
	meta.HistoryStateKey = old.StateKey
	meta.BaselineGeneration = oldGeneration
	meta.PressureDecision = pressureDecision{
		Available:                 true,
		MessageCount:              len(base),
		SelectedPressure:          88_000,
		SystemFingerprint:         currentHistoryFingerprint(base),
		ToolsFingerprint:          currentHistoryFingerprint(base),
		MessagesPrefixFingerprint: oldFingerprint,
	}
	if updated := server.applyPressureBaselineUsage(meta, 99_000); updated {
		t.Fatal("旧 epoch 的迟到响应被接受为 baseline")
	}
	if meta.BaselineUpdateKind != pressureBaselineUpdateStale {
		t.Fatalf("stale baseline kind=%q, want %q", meta.BaselineUpdateKind, pressureBaselineUpdateStale)
	}
	got := trigger.PressureBaseline(current.StateKey)
	if !got.Available || got.ActualTokens != 12_000 {
		t.Fatalf("旧响应污染 current epoch baseline: %+v", got)
	}
	if trigger.PressureBaseline(old.StateKey).ActualTokens != 88_000 {
		t.Fatal("旧 epoch baseline unexpectedly mutated")
	}
}

func currentHistoryFingerprint(messages []Message) string {
	return reuseSafetyPrefixHash(messages, len(messages))
}

func historyMessagesFromJSON(t *testing.T, raw string) []Message {
	t.Helper()
	var messages []Message
	if err := json.Unmarshal([]byte(raw), &messages); err != nil {
		t.Fatalf("unmarshal history fixture: %v", err)
	}
	return messages
}

func historyTextMessages(texts ...string) []Message {
	messages := make([]Message, len(texts))
	for index, text := range texts {
		role := "user"
		if index%2 == 1 {
			role = "assistant"
		}
		messages[index] = Message{Role: role, Content: mustMarshal(text)}
	}
	return messages
}

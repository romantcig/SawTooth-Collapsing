package proxy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
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
	if initial.Epoch != 1 || initial.CommonPrefix != 0 || initial.StateKey == "" || initial.StateKey == sessionID || !strings.HasSuffix(initial.StateKey, ":history_epoch:1") || !initial.ReuseSafe {
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
	// 入口 raw = base；wire 的 tool_use.input.cache_control 被改写。Changed
	// 对比 entry 与 wire，只有指纹算法对该业务字段敏感时才会置位——恒真化
	// 等于放弃这条判别力。
	meta.PressureEntryCoordinates = pressureEntryCoordinates{
		MessageCount:              len(base),
		MessagesPrefixFingerprint: fingerprintMessagesPrefix(base, len(base)),
	}
	changed := historyMessagesFromJSON(t, `[{"role":"assistant","content":[{"type":"tool_use","id":"t1","name":"configure","input":{"cache_control":"disabled"}}]}]`)
	body, err := json.Marshal(map[string]any{"messages": changed})
	if err != nil {
		t.Fatal(err)
	}
	markForwardedPressureCoordinates(meta, body, nil)
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
	// 先完成真实坐标绑定：stale epoch 门禁必须独立于 binding 生效，
	// 否则这条测试只能证明"未绑定被拒"，证不出"迟到响应被拒"。
	lateBody, err := json.Marshal(map[string]any{"messages": base})
	if err != nil {
		t.Fatal(err)
	}
	markForwardedPressureCoordinates(meta, lateBody, nil)
	if !meta.PressureDecision.ForwardedCoordinatesBound {
		t.Fatalf("迟到响应的 forwarded 坐标未绑定: %+v", meta.PressureDecision)
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

func TestHistoryEpochArchiveTransitionPublishesAfterSQLiteCommit(t *testing.T) {
	store, err := NewSQLiteStore(filepath.Join(tempDirRetryCleanup(t), "epoch-archive.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	manager := NewHistoryEpochManager()
	manager.SetPersistFunc(func(key, value string) { _ = store.PersistState(key, value) })
	manager.SetLoadFunc(store.LoadState)
	manager.SetTransitionFunc(store.CommitHistoryTransition)
	base := historyTextMessages("zero", "one", "two", "three")
	initial := manager.Begin("epoch-archive-session", base)
	if initial.Epoch != 1 || initial.TransitionFailed {
		t.Fatalf("initial decision=%+v", initial)
	}
	for _, block := range []ArchiveBlock{
		{ID: "common-prefix", SessionID: "epoch-archive-session", HistoryEpoch: initial.Epoch, BlockRangeStart: 0, BlockRangeEnd: 1, MessageCount: 2, SummaryText: "common", Keywords: []KeywordEntry{{Word: "common", Source: "user_message"}}},
		{ID: "mixed-branch", SessionID: "epoch-archive-session", HistoryEpoch: initial.Epoch, BlockRangeStart: 1, BlockRangeEnd: 2, MessageCount: 2, SummaryText: "mixed", Keywords: []KeywordEntry{{Word: "mixed", Source: "user_message"}}},
	} {
		if err := store.SaveArchive(block); err != nil {
			t.Fatalf("SaveArchive(%s): %v", block.ID, err)
		}
	}
	branched := deepCopyMessages(base)
	branched[2].Content = mustMarshal("edited-two")
	current := manager.Begin("epoch-archive-session", branched)
	if current.Epoch != 2 || !current.EpochChanged || current.TransitionFailed || current.CommonPrefix != 2 {
		t.Fatalf("branch decision=%+v", current)
	}
	if !manager.IsCurrent("epoch-archive-session", current.Epoch) || manager.IsCurrent("epoch-archive-session", initial.Epoch) {
		t.Fatalf("manager current epoch gate failed: initial=%+v current=%+v", initial, current)
	}

	var commonIsolated, mixedIsolated int
	if err := store.db.QueryRow(`SELECT isolated FROM archive_blocks WHERE id='common-prefix'`).Scan(&commonIsolated); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRow(`SELECT isolated FROM archive_blocks WHERE id='mixed-branch'`).Scan(&mixedIsolated); err != nil {
		t.Fatal(err)
	}
	if commonIsolated != 0 || mixedIsolated != 1 {
		t.Fatalf("transition visibility=%d/%d, want common=0 mixed=1", commonIsolated, mixedIsolated)
	}
	state, ok := store.LoadState(historyEpochPersistenceKey("epoch-archive-session"))
	if !ok || !strings.Contains(state, `"epoch":2`) {
		t.Fatalf("SQLite 未提交 current epoch state: %q ok=%v", state, ok)
	}
}

func TestHistoryEpochTransitionFailureDoesNotPublishOrReadOldArchive(t *testing.T) {
	store, err := NewSQLiteStore(filepath.Join(tempDirRetryCleanup(t), "epoch-archive-failure.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	manager := NewHistoryEpochManager()
	manager.SetPersistFunc(func(key, value string) { _ = store.PersistState(key, value) })
	manager.SetLoadFunc(store.LoadState)
	manager.SetTransitionFunc(func(HistoryTransition) error {
		return fmt.Errorf("forced transition failure")
	})
	base := historyTextMessages("zero", "one", "two")
	initial := manager.Begin("failed-transition-session", base)
	if err := store.SaveArchive(ArchiveBlock{
		ID: "old-branch", SessionID: "failed-transition-session", HistoryEpoch: initial.Epoch,
		BlockRangeStart: 0, BlockRangeEnd: 2, MessageCount: 3, SummaryText: "old branch",
		Keywords: []KeywordEntry{{Word: "old", Source: "user_message"}},
	}); err != nil {
		t.Fatal(err)
	}
	branched := deepCopyMessages(base)
	branched[1].Content = mustMarshal("new branch")
	failed := manager.Begin("failed-transition-session", branched)
	if !failed.TransitionFailed || failed.Reason != HistoryEpochReasonTransitionFailed || !failed.EpochChanged || failed.Epoch != initial.Epoch+1 {
		t.Fatalf("failed transition decision=%+v", failed)
	}
	if !manager.IsCurrent("failed-transition-session", initial.Epoch) || manager.IsCurrent("failed-transition-session", failed.Epoch) {
		t.Fatalf("failed transition published epoch: initial=%+v failed=%+v", initial, failed)
	}
	if _, ok := store.LoadState(historyEpochStateKey("failed-transition-session", failed.Epoch)); ok {
		t.Fatal("failed transition wrote target epoch state")
	}
	var isolated int
	if err := store.db.QueryRow(`SELECT isolated FROM archive_blocks WHERE id='old-branch'`).Scan(&isolated); err != nil {
		t.Fatal(err)
	}
	if isolated != 0 {
		t.Fatalf("failed transition changed old Archive visibility: %d", isolated)
	}

	// The manager keeps the old state, so the same branch must retry rather than
	// silently accepting a stale target or reading a partially published epoch.
	retry := manager.Begin("failed-transition-session", branched)
	if !retry.TransitionFailed || retry.Epoch != failed.Epoch || retry.StateKey != failed.StateKey {
		t.Fatalf("failed transition retry=%+v, first=%+v", retry, failed)
	}
}

func TestHistoryEpochTransitionCarriesOnlyStableState(t *testing.T) {
	manager := NewHistoryEpochManager()
	var transition HistoryTransition
	manager.SetTransitionFunc(func(got HistoryTransition) error {
		transition = got
		return nil
	})
	manager.SetPersistFunc(func(key, value string) {
		if strings.Contains(key, "session-with-sensitive") || strings.Contains(value, "zero") {
			t.Fatalf("transition persistence leaked sensitive history: key=%q value=%q", key, value)
		}
	})
	base := historyTextMessages("zero", "one")
	manager.Begin("session-with-sensitive", base)
	changed := deepCopyMessages(base)
	changed[0].Content = mustMarshal("secret body")
	decision := manager.Begin("session-with-sensitive", changed)
	if decision.TransitionFailed || transition.SessionID != "session-with-sensitive" || transition.CommonPrefix != 0 {
		t.Fatalf("transition=%+v decision=%+v", transition, decision)
	}
	if strings.Contains(transition.StateKey, "session-with-sensitive") || strings.Contains(transition.StateValue, "secret body") {
		t.Fatalf("transition carried identity/body: %+v", transition)
	}
	var persisted historyEpochPersisted
	if err := json.Unmarshal([]byte(transition.StateValue), &persisted); err != nil {
		t.Fatal(err)
	}
	if persisted.Epoch != decision.Epoch || !persisted.Valid || len(persisted.InputMessageHashes) != len(changed) {
		t.Fatalf("transition state=%+v decision=%+v", persisted, decision)
	}
	if len(persisted.RecentMismatchDigests) != 1 {
		t.Fatalf("unexpected mismatch state=%+v", persisted)
	}
}

func TestHandleMessagesTransitionFailureForwardsRawWithoutDerivedReads(t *testing.T) {
	var forwarded []Message
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Messages []Message `json:"messages"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		forwarded = body.Messages
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"type":"message","usage":{"input_tokens":1}}`))
	}))
	defer upstream.Close()

	server := newPipelineTestServer(t, upstream.URL)
	manager := NewHistoryEpochManager()
	manager.SetPersistFunc(func(string, string) {})
	manager.SetTransitionFunc(func(HistoryTransition) error { return fmt.Errorf("forced transition failure") })
	server.HistoryEpoch = manager

	var searchCalls, derivedLoadCalls int
	server.searchAndExpandFn = func(messages []Message, _ *SQLiteStore, _ int, _ *TokenCounter, _ *Budget, _ *requestMeta) RecallOutcome {
		searchCalls++
		return RecallOutcome{Messages: messages}
	}
	server.Frozen.SetLoadFunc(func(string) (string, bool) { derivedLoadCalls++; return "", false })
	server.Sawtooth.SetLoadFunc(func(string) (string, bool) { derivedLoadCalls++; return "", false })
	server.DecayTracker.SetLoadFunc(func(string) (string, bool) { derivedLoadCalls++; return "", false })

	const sessionID = "transition-failure-pipeline"
	base := historyTextMessages("zero", "one", "two")
	initial := manager.Begin(sessionID, base)
	if initial.Epoch != 1 {
		t.Fatalf("initial=%+v", initial)
	}
	branched := deepCopyMessages(base)
	branched[1].Content = mustMarshal("new branch")
	servePipelineRequest(t, server, sessionID, branched)

	if searchCalls != 0 || derivedLoadCalls != 0 {
		t.Fatalf("transition failure touched derived state: search=%d loads=%d", searchCalls, derivedLoadCalls)
	}
	if !reflect.DeepEqual(forwarded, branched) {
		t.Fatalf("forwarded messages did not remain raw history:\ngot=%+v\nwant=%+v", forwarded, branched)
	}
}

// ── Plan 12.1-05 Task 2：HistoryEpochManager 的 error-aware 加载与 raw fail-closed ──

func TestHistoryStateLoaderMissingVsError(t *testing.T) {
	const sessionID = "history-loader-truth"
	base := historyTextMessages("zero", "one")

	t.Run("missing", func(t *testing.T) {
		manager := NewHistoryEpochManager()
		calls := 0
		manager.SetStateLoader(stateLoaderFunc(func(string) StateLoadResult {
			calls++
			return StateLoadResult{}
		}))
		manager.SetPersistFunc(func(string, string) {})
		decision := manager.Begin(sessionID, base)
		if decision.LoadFailed || decision.Epoch != 1 || decision.Reason != HistoryEpochReasonInitial {
			t.Fatalf("missing 应进入 initial epoch: %+v", decision)
		}
		if calls != 1 {
			t.Fatalf("missing 是终态，却查询了 %d 次", calls)
		}
	})

	for name, failureResult := range stateLoadErrorCases() {
		t.Run(name, func(t *testing.T) {
			manager := NewHistoryEpochManager()
			var persistCalls, transitionCalls int
			manager.SetStateLoader(stateLoaderFunc(func(string) StateLoadResult { return failureResult }))
			manager.SetPersistFunc(func(string, string) { persistCalls++ })
			manager.SetTransitionFunc(func(HistoryTransition) error { transitionCalls++; return nil })

			decision := manager.Begin(sessionID, base)
			if !decision.LoadFailed || decision.LoadFailure.Class != failureResult.FailureClass {
				t.Fatalf("读取失败未上报: %+v", decision)
			}
			if decision.Reason != HistoryEpochReasonLoadFailed {
				t.Fatalf("读取失败 reason=%q, want state_load_failed", decision.Reason)
			}
			if decision.ReuseSafe || decision.TransitionFailed || decision.EpochChanged {
				t.Fatalf("读取失败必须 fail closed 且与 transition 失败区分: %+v", decision)
			}
			if decision.Epoch != 0 || decision.StateKey != "" {
				t.Fatalf("读取失败仍发布了 epoch: %+v", decision)
			}
			if persistCalls != 0 || transitionCalls != 0 {
				t.Fatalf("读取失败仍写入状态: persist=%d transition=%d", persistCalls, transitionCalls)
			}
			if manager.IsCurrent(sessionID, 1) {
				t.Fatal("读取失败后仍声称某个 epoch 是当前 epoch")
			}
		})
	}
}

func TestHistoryLoadFailureRetries(t *testing.T) {
	const sessionID = "history-load-retry"
	state, err := json.Marshal(historyEpochPersisted{
		Version: historyEpochStateVersion,
		Epoch:   5,
		Valid:   false,
	})
	if err != nil {
		t.Fatal(err)
	}

	calls := 0
	manager := NewHistoryEpochManager()
	manager.SetStateLoader(stateLoaderFunc(func(string) StateLoadResult {
		calls++
		if calls == 1 {
			return StateLoadResult{Err: ErrStateLoadQueryFailed, FailureClass: StateLoadFailureQueryFailed}
		}
		return StateLoadResult{Value: string(state), Found: true}
	}))
	manager.SetPersistFunc(func(string, string) {})

	base := historyTextMessages("zero", "one")
	first := manager.Begin(sessionID, base)
	if !first.LoadFailed {
		t.Fatalf("首次读取失败未上报: %+v", first)
	}

	second := manager.Begin(sessionID, base)
	if second.LoadFailed {
		t.Fatalf("读取失败后不可重试: %+v", second)
	}
	// 已持久化的 epoch 5 必须被承认；退回 initial epoch 1 会让旧派生状态复活。
	if second.Epoch != 6 || !second.EpochChanged || second.Reason != HistoryEpochReasonInvalid {
		t.Fatalf("重试未沿用已持久化 epoch: %+v", second)
	}
}

func TestHistoryTransitionStillBypassesWriter(t *testing.T) {
	const sessionID = "history-transition-bypass"
	manager := NewHistoryEpochManager()
	var persistCalls, transitionCalls int
	manager.SetStateLoader(stateLoaderFunc(func(string) StateLoadResult { return StateLoadResult{} }))
	manager.SetPersistFunc(func(string, string) { persistCalls++ })
	manager.SetTransitionFunc(func(HistoryTransition) error {
		transitionCalls++
		return fmt.Errorf("forced transition failure")
	})

	base := historyTextMessages("zero", "one")
	if initial := manager.Begin(sessionID, base); initial.Epoch != 1 {
		t.Fatalf("initial=%+v", initial)
	}
	branched := deepCopyMessages(base)
	branched[0].Content = mustMarshal("changed")
	failed := manager.Begin(sessionID, branched)

	if !failed.TransitionFailed || failed.LoadFailed {
		t.Fatalf("transition 失败与 load 失败被混为一谈: %+v", failed)
	}
	if failed.Reason != HistoryEpochReasonTransitionFailed {
		t.Fatalf("transition 失败 reason=%q", failed.Reason)
	}
	if transitionCalls != 1 {
		t.Fatalf("transition 调用数=%d, want 1（同步事务）", transitionCalls)
	}
	if persistCalls != 1 {
		t.Fatalf("persist 调用数=%d, want 1（仅 initial），transition 不得走普通 writer", persistCalls)
	}
	// transition 必须保持同步：manager 上不得出现异步 state submitter 接口。
	if _, ok := interface{}(manager).(interface{ SetStateSubmitter(StateSubmitter) }); ok {
		t.Fatal("HistoryEpochManager 暴露了 StateSubmitter，transition 可能被放入普通 writer")
	}
}

func TestHistoryLoadFailureRawFailClosed(t *testing.T) {
	var forwarded []Message
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Messages []Message `json:"messages"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		forwarded = body.Messages
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"type":"message","usage":{"input_tokens":1}}`))
	}))
	defer upstream.Close()

	server := newPipelineTestServer(t, upstream.URL)
	manager := NewHistoryEpochManager()
	var persistCalls, transitionCalls int
	manager.SetStateLoader(stateLoaderFunc(func(string) StateLoadResult {
		return StateLoadResult{Err: ErrStateLoadClosed, FailureClass: StateLoadFailureSQLiteClosed}
	}))
	manager.SetPersistFunc(func(string, string) { persistCalls++ })
	manager.SetTransitionFunc(func(HistoryTransition) error { transitionCalls++; return nil })
	server.HistoryEpoch = manager

	var searchCalls, derivedLoadCalls int
	server.searchAndExpandFn = func(messages []Message, _ *SQLiteStore, _ int, _ *TokenCounter, _ *Budget, _ *requestMeta) RecallOutcome {
		searchCalls++
		return RecallOutcome{Messages: messages}
	}
	server.Frozen.SetLoadFunc(func(string) (string, bool) { derivedLoadCalls++; return "", false })
	server.Sawtooth.SetLoadFunc(func(string) (string, bool) { derivedLoadCalls++; return "", false })
	server.DecayTracker.SetLoadFunc(func(string) (string, bool) { derivedLoadCalls++; return "", false })

	messages := historyTextMessages("zero", "one", "two")
	servePipelineRequest(t, server, "history-load-failure-pipeline", messages)

	if persistCalls != 0 || transitionCalls != 0 {
		t.Fatalf("读取失败仍发布 epoch: persist=%d transition=%d", persistCalls, transitionCalls)
	}
	if searchCalls != 0 || derivedLoadCalls != 0 {
		t.Fatalf("读取失败仍读取派生状态: search=%d loads=%d", searchCalls, derivedLoadCalls)
	}
	if !reflect.DeepEqual(forwarded, messages) {
		t.Fatalf("读取失败未按 raw history 直通:\ngot=%+v\nwant=%+v", forwarded, messages)
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

func TestHistoryMismatchExactTuple(t *testing.T) {
	previous := historyEpochPersisted{
		InputHash: strings.Repeat("a", 64),
		ReuseHash: strings.Repeat("b", 64),
	}
	current := historyEpochPersisted{
		InputHash: strings.Repeat("c", 64),
		ReuseHash: strings.Repeat("d", 64),
	}
	const sessionID = "mismatch-tuple-session"

	base := historyMismatchDigestForSession(sessionID, previous, current, 2, HistoryEpochReasonChanged)
	reasonOnly := historyMismatchDigestForSession(sessionID, previous, current, 2, HistoryEpochReasonShrunk)
	if base != reasonOnly {
		t.Fatal("input-history mismatch key must not include the descriptive reason")
	}

	reuseChanged := previous
	reuseChanged.ReuseHash = strings.Repeat("e", 64)
	if got := historyMismatchDigestForSession(sessionID, reuseChanged, current, 2, HistoryEpochReasonChanged); got != base {
		t.Fatal("input-history mismatch key unexpectedly included reuse-only hashes")
	}

	cases := []struct {
		name      string
		sessionID string
		previous  historyEpochPersisted
		current   historyEpochPersisted
		cutoff    int
	}{
		{name: "session", sessionID: "other-session", previous: previous, current: current, cutoff: 2},
		{name: "cutoff", sessionID: sessionID, previous: previous, current: current, cutoff: 1},
		{name: "old canonical", sessionID: sessionID, previous: historyEpochPersisted{InputHash: strings.Repeat("f", 64)}, current: current, cutoff: 2},
		{name: "new canonical", sessionID: sessionID, previous: previous, current: historyEpochPersisted{InputHash: strings.Repeat("0", 64)}, cutoff: 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := historyMismatchDigestForSession(tc.sessionID, tc.previous, tc.current, tc.cutoff, HistoryEpochReasonChanged)
			if got == base {
				t.Fatalf("different %s was swallowed by the mismatch key", tc.name)
			}
		})
	}
}
func TestHistoryMismatchPersistsAcrossRestart(t *testing.T) {
	var persistedMu sync.Mutex
	persisted := make(map[string]string)
	newManager := func() *HistoryEpochManager {
		manager := NewHistoryEpochManager()
		manager.SetPersistFunc(func(key, value string) {
			persistedMu.Lock()
			persisted[key] = value
			persistedMu.Unlock()
		})
		manager.SetLoadFunc(func(key string) (string, bool) {
			persistedMu.Lock()
			defer persistedMu.Unlock()
			value, ok := persisted[key]
			return value, ok
		})
		return manager
	}

	const sessionID = "mismatch-restart-session"
	base := historyTextMessages("common", "old", "tail")
	branch := deepCopyMessages(base)
	branch[1].Content = mustMarshal("new")

	firstManager := newManager()
	firstManager.Begin(sessionID, base)
	firstBranch := firstManager.Begin(sessionID, branch)
	if !firstBranch.EpochChanged || !firstBranch.FirstMismatch {
		t.Fatalf("first branch=%+v, want first mismatch", firstBranch)
	}

	restarted := newManager()
	if same := restarted.Begin(sessionID, branch); same.EpochChanged || same.FirstMismatch {
		t.Fatalf("restored current branch=%+v, want unchanged", same)
	}
	back := restarted.Begin(sessionID, base)
	if !back.EpochChanged || !back.FirstMismatch {
		t.Fatalf("reverse branch=%+v, want its own first mismatch", back)
	}
	repeated := restarted.Begin(sessionID, branch)
	if !repeated.EpochChanged || repeated.FirstMismatch {
		t.Fatalf("persisted A->B mismatch was not deduplicated: %+v", repeated)
	}

	persistedMu.Lock()
	raw := persisted[historyEpochPersistenceKey(sessionID)]
	persistedMu.Unlock()
	var state historyEpochPersisted
	if err := json.Unmarshal([]byte(raw), &state); err != nil {
		t.Fatal(err)
	}
	if len(state.RecentMismatchDigests) != 2 {
		t.Fatalf("persisted mismatch keys=%d, want two directional tuples: %+v", len(state.RecentMismatchDigests), state)
	}
}

func TestHistoryMismatchConcurrent(t *testing.T) {
	manager := NewHistoryEpochManager()
	const sessionID = "mismatch-concurrent-session"
	base := historyTextMessages("common", "old", "tail")
	branch := deepCopyMessages(base)
	branch[1].Content = mustMarshal("new")
	manager.Begin(sessionID, base)

	const workers = 100
	start := make(chan struct{})
	results := make(chan HistoryEpochDecision, workers)
	var wg sync.WaitGroup
	for index := 0; index < workers; index++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			results <- manager.Begin(sessionID, branch)
		}()
	}
	close(start)
	wg.Wait()
	close(results)

	firstCount := 0
	transitionCount := 0
	for decision := range results {
		if decision.FirstMismatch {
			firstCount++
		}
		if decision.EpochChanged {
			transitionCount++
		}
	}
	if firstCount != 1 || transitionCount != 1 {
		t.Fatalf("concurrent mismatch first/transition=%d/%d, want 1/1", firstCount, transitionCount)
	}
}

func TestHistoryMismatchBoundedDeterministicEviction(t *testing.T) {
	const sessionID = "mismatch-eviction-session"
	var persisted string
	manager := NewHistoryEpochManager()
	manager.SetPersistFunc(func(_ string, value string) { persisted = value })

	currentMessages := historyTextMessages("seed")
	currentPrints, err := canonicalizeHistory(currentMessages)
	if err != nil {
		t.Fatal(err)
	}
	manager.Begin(sessionID, currentMessages)
	expected := make([]string, 0, historyMismatchDigestLimit+5)
	previousState := historyEpochPersisted{InputHash: currentPrints.InputHash, ReuseHash: currentPrints.ReuseHash}
	for index := 0; index < historyMismatchDigestLimit+5; index++ {
		nextMessages := historyTextMessages(fmt.Sprintf("branch-%d", index))
		nextPrints, err := canonicalizeHistory(nextMessages)
		if err != nil {
			t.Fatal(err)
		}
		nextState := historyEpochPersisted{InputHash: nextPrints.InputHash, ReuseHash: nextPrints.ReuseHash}
		expected = append(expected, historyMismatchDigestForSession(sessionID, previousState, nextState, 0, HistoryEpochReasonChanged))
		decision := manager.Begin(sessionID, nextMessages)
		if !decision.FirstMismatch || !decision.EpochChanged {
			t.Fatalf("unique transition %d was not first: %+v", index, decision)
		}
		previousState = nextState
	}

	var state historyEpochPersisted
	if err := json.Unmarshal([]byte(persisted), &state); err != nil {
		t.Fatal(err)
	}
	want := expected[len(expected)-historyMismatchDigestLimit:]
	if !reflect.DeepEqual(state.RecentMismatchDigests, want) {
		t.Fatalf("bounded mismatch eviction is not deterministic:\ngot=%v\nwant=%v", state.RecentMismatchDigests, want)
	}
}

func TestHandleMessagesBranchWarnsOnceWithoutSensitiveValues(t *testing.T) {
	var logs bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(NewLogHandler(&logs, slog.LevelDebug)))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"type":"message","usage":{"input_tokens":1}}`))
	}))
	defer upstream.Close()

	server := newPipelineTestServer(t, upstream.URL)
	const sessionID = "SESSION-ID-MUST-NOT-LEAK-HISTORY-WARN"
	base := historyMessagesFromJSON(t, `[
		{"role":"user","content":"common prompt"},
		{"role":"assistant","content":[{"type":"thinking","thinking":"old private reasoning","signature":"old-signature"},{"type":"text","text":"stable answer"}]},
		{"role":"user","content":[{"type":"tool_result","tool_use_id":"tool-1","content":{"status":"OLD-TOOL-PAYLOAD-MUST-NOT-LEAK"}}]}
	]`)
	branch := historyMessagesFromJSON(t, `[
		{"role":"user","content":[{"type":"text","text":"common prompt","cache_control":{"type":"ephemeral"}}]},
		{"role":"assistant","content":[{"type":"thinking","thinking":"new private reasoning","signature":"new-signature"},{"type":"text","text":"stable answer"}]},
		{"role":"user","content":[{"type":"tool_result","tool_use_id":"tool-1","content":{"status":"NEW-TOOL-PAYLOAD-MUST-NOT-LEAK"}}]}
	]`)
	equivalentBranch := historyMessagesFromJSON(t, `[
		{"role":"user","content":"common prompt"},
		{"role":"assistant","content":[{"type":"thinking","thinking":"transport-only reasoning","signature":"transport-signature"},{"type":"text","text":"stable answer","cache_control":{"type":"ephemeral"}}]},
		{"role":"user","content":[{"type":"tool_result","tool_use_id":"tool-1","content":{"status":"NEW-TOOL-PAYLOAD-MUST-NOT-LEAK"}}]}
	]`)

	servePipelineRequest(t, server, sessionID, base)
	servePipelineRequest(t, server, sessionID, branch)
	servePipelineRequest(t, server, sessionID, equivalentBranch)
	servePipelineRequest(t, server, sessionID, equivalentBranch)

	var warningLines []string
	for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
		if strings.Contains(line, "历史不再连续") {
			warningLines = append(warningLines, line)
		}
	}
	if len(warningLines) != 1 {
		t.Fatalf("history mismatch WARN count=%d, want 1:\n%s", len(warningLines), logs.String())
	}
	line := warningLines[0]
	for _, want := range []string{"epoch=2", "cutoff=2", "common_prefix=2", "reason=history_changed", "first=true"} {
		if !strings.Contains(line, want) {
			t.Fatalf("history mismatch WARN missing %q: %s", want, line)
		}
	}
	basePrints, _ := canonicalizeHistory(base)
	branchPrints, _ := canonicalizeHistory(branch)
	for _, forbidden := range []string{
		sessionID,
		stableSessionHash(sessionID),
		basePrints.InputHash,
		branchPrints.InputHash,
		"OLD-TOOL-PAYLOAD-MUST-NOT-LEAK",
		"NEW-TOOL-PAYLOAD-MUST-NOT-LEAK",
		"old private reasoning",
		"new private reasoning",
		"Authorization",
		"API-KEY-MUST-NOT-LEAK",
	} {
		if strings.Contains(line, forbidden) {
			t.Fatalf("history mismatch WARN leaked %q: %s", forbidden, line)
		}
	}
}

func TestDebugFactsHistoryScalarsAreSafe(t *testing.T) {
	dataDir := tempDirRetryCleanup(t)
	cfg := DefaultConfig()
	cfg.Debug = DebugConfig{Enabled: true, DataDir: dataDir}
	server := NewServer(cfg)
	meta := server.nextRequestMeta("HISTORY-FACT-SESSION-MUST-NOT-LEAK")
	meta.HistoryEpoch = 4
	meta.HistoryCommonPrefix = 2
	meta.HistoryEpochReason = HistoryEpochReasonChanged
	meta.HistoryEpochChanged = true
	meta.HistoryMismatch = true
	meta.HistoryMismatchFirst = true
	meta.HistoryStateKey = "HISTORY-FACT-SESSION-MUST-NOT-LEAK:" + strings.Repeat("a", 64) + ":MESSAGE-SENTINEL-MUST-NOT-LEAK"
	meta.PressureDecision = pressureDecision{
		Available: true,
		Source:    pressureSourceLocalFull,
	}

	server.writePressureDecisionDebugFacts(meta, time.Date(2026, 7, 26, 1, 2, 3, 4, time.UTC))
	facts := debugFactsByStage(t, dataDir, meta.RequestSessionID)
	pressure := facts[debugStagePressureDecision]
	if pressure == nil {
		t.Fatalf("history pressure fact missing: %v", facts)
	}
	for key, want := range map[string]any{
		"history_epoch":             float64(4),
		"history_common_prefix":     float64(2),
		"history_transition_reason": string(HistoryEpochReasonChanged),
		"history_epoch_changed":     true,
		"history_mismatch_first":    true,
	} {
		if got := pressure[key]; !reflect.DeepEqual(got, want) {
			t.Fatalf("pressure[%s]=%v (%T), want %v (%T)", key, got, got, want, want)
		}
	}

	for _, data := range readDebugFactFiles(t, dataDir, meta.RequestSessionID) {
		for _, forbidden := range []string{
			meta.RequestSessionID,
			meta.SessionHash,
			strings.Repeat("a", 64),
			"MESSAGE-SENTINEL-MUST-NOT-LEAK",
			"Authorization",
			"API-KEY-MUST-NOT-LEAK",
		} {
			if bytes.Contains(data, []byte(forbidden)) {
				t.Fatalf("history fact leaked %q: %s", forbidden, data)
			}
		}
	}
}

func TestHistoryEpochOneMigratesLegacyDerivedStateAndBranchesCleanly(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"type":"message","usage":{"input_tokens":321,"output_tokens":1}}`))
	}))
	defer upstream.Close()

	server := newPipelineTestServer(t, upstream.URL)
	server.HistoryEpoch = NewHistoryEpochManager()
	const sessionID = "legacy-epoch-migration"
	stateKey1 := historyEpochStateKey(sessionID, 1)
	stateKey2 := historyEpochStateKey(sessionID, 2)
	base := historyTextMessages("legacy user", "legacy assistant")

	var persistedKeys []string
	persist := func(key, _ string) {
		persistedKeys = append(persistedKeys, key)
	}
	server.Sawtooth.SetPersistFunc(persist)
	server.Frozen.SetPersistFunc(persist)
	server.DecayTracker.SetPersistFunc(persist)

	contextFingerprint := fingerprintTopLevelJSON(nil)
	server.Sawtooth.UpdatePressureBaseline(
		sessionID,
		2_000,
		len(base),
		contextFingerprint,
		contextFingerprint,
		fingerprintMessagesPrefix(base, len(base)),
	)
	server.Sawtooth.IncrementRequestSeq(sessionID)
	server.Sawtooth.IncrementRequestSeq(sessionID)
	server.Frozen.Store(
		sessionID,
		deepCopyMessages(base[:1]),
		1,
		base[0],
		100,
		200,
		base,
	)
	server.DecayTracker.MarkStubbed(sessionID, 0, 1, 0.25)
	server.DecayTracker.SetFilePath(sessionID, 0, "legacy.go")
	server.DecayTracker.Persist(sessionID)
	persistedKeys = nil

	var observedStateKeys []string
	server.searchAndExpandFn = func(messages []Message, _ *SQLiteStore, _ int, _ *TokenCounter, _ *Budget, meta *requestMeta) RecallOutcome {
		observedStateKeys = append(observedStateKeys, meta.HistoryStateKey)
		return RecallOutcome{Messages: messages}
	}

	servePipelineRequest(t, server, sessionID, base)

	if len(observedStateKeys) != 1 || observedStateKeys[0] != stateKey1 || observedStateKeys[0] == sessionID {
		t.Fatalf("epoch 1 state key=%v, want %q", observedStateKeys, stateKey1)
	}
	if got := server.Sawtooth.GetRequestSeq(stateKey1); got != 3 {
		t.Fatalf("epoch 1 request sequence=%d, want migrated 2 + current 1", got)
	}
	if got := server.Sawtooth.GetRequestSeq(sessionID); got != 3 {
		t.Fatalf("legacy Sawtooth observer did not resolve current epoch: %d", got)
	}
	if baseline := server.Sawtooth.PressureBaseline(stateKey1); !baseline.Available || baseline.ActualTokens != 321 {
		t.Fatalf("epoch 1 pressure baseline=%+v", baseline)
	}

	server.Frozen.mu.RLock()
	_, frozenMigrated := server.Frozen.messages[stateKey1]
	frozenMode := server.Frozen.rawPrefixMode[stateKey1]
	server.Frozen.mu.RUnlock()
	if !frozenMigrated || frozenMode != frozenRawPrefixModeFull {
		t.Fatalf("Frozen 未迁移完整前缀证明: exists=%v mode=%q", frozenMigrated, frozenMode)
	}
	server.DecayTracker.mu.RLock()
	decayRequest, decayMigrated := server.DecayTracker.stubbedAt[stateKey1+":msg_0"]
	decayPath := server.DecayTracker.filePaths[stateKey1+":msg_0"]
	server.DecayTracker.mu.RUnlock()
	if !decayMigrated || decayRequest != 1 || decayPath != "legacy.go" {
		t.Fatalf("Decay 未迁移: exists=%v request=%d path=%q", decayMigrated, decayRequest, decayPath)
	}

	wantPersisted := map[string]bool{
		"sawtooth:" + stateKey1: false,
		"frozen:" + stateKey1:   false,
		"decay:" + stateKey1:    false,
	}
	for _, key := range persistedKeys {
		if _, tracked := wantPersisted[key]; tracked {
			wantPersisted[key] = true
		}
		if key == "sawtooth:"+sessionID || key == "frozen:"+sessionID || key == "decay:"+sessionID {
			t.Fatalf("主管线继续持久化裸 session key: %q", key)
		}
	}
	for key, found := range wantPersisted {
		if !found {
			t.Fatalf("未持久化 epoch 1 迁移目标 %q；keys=%v", key, persistedKeys)
		}
	}

	// 模拟进程内仍残留的裸 key。下一次真实分支必须直接进入 epoch 2，
	// 不能再次把这些值迁移为当前状态。
	server.Sawtooth.mu.Lock()
	server.Sawtooth.requestSeq[sessionID] = 900
	server.Sawtooth.mu.Unlock()
	server.Frozen.mu.Lock()
	server.Frozen.messages[sessionID] = historyTextMessages("stale frozen")
	server.Frozen.mu.Unlock()
	server.DecayTracker.mu.Lock()
	server.DecayTracker.stubbedAt[sessionID+":msg_99"] = 900
	server.DecayTracker.mu.Unlock()

	branched := deepCopyMessages(base)
	branched[1].Content = mustMarshal("new branch")
	persistedKeys = nil
	servePipelineRequest(t, server, sessionID, branched)

	if len(observedStateKeys) != 2 || observedStateKeys[1] != stateKey2 {
		t.Fatalf("branch state keys=%v, want second %q", observedStateKeys, stateKey2)
	}
	if got := server.Sawtooth.GetRequestSeq(stateKey2); got != 1 {
		t.Fatalf("epoch 2 request sequence=%d, stale bare key leaked", got)
	}
	if got := server.Sawtooth.GetRequestSeq(sessionID); got != 1 {
		t.Fatalf("legacy Sawtooth observer did not advance to epoch 2: %d", got)
	}
	server.Frozen.mu.RLock()
	_, frozenLeaked := server.Frozen.messages[stateKey2]
	server.Frozen.mu.RUnlock()
	if frozenLeaked || server.Frozen.LengthFor(sessionID) != 0 {
		t.Fatal("裸 Frozen 状态迁入了 epoch 2")
	}
	server.DecayTracker.mu.RLock()
	_, decayLeaked := server.DecayTracker.stubbedAt[stateKey2+":msg_99"]
	server.DecayTracker.mu.RUnlock()
	if decayLeaked || server.DecayTracker.GetStage(sessionID, 0, 100, 100, 1) != DecayFresh {
		t.Fatal("裸 Decay 状态迁入了 epoch 2")
	}
	for _, key := range persistedKeys {
		if key == "frozen:"+stateKey2 || key == "decay:"+stateKey2 {
			t.Fatalf("epoch 2 意外执行 legacy migration: %q", key)
		}
		if strings.HasSuffix(key, ":"+sessionID) {
			t.Fatalf("branch 后持久化了裸 session key: %q", key)
		}
	}
}

func TestLegacyDerivedStateMigrationPrefersExistingEpochKey(t *testing.T) {
	const sessionID = "legacy-target-priority"
	stateKey := historyEpochStateKey(sessionID, 1)
	fingerprint := strings.Repeat("a", 64)

	trigger := NewSawtoothTrigger(time.Minute, 100_000, 50_000)
	trigger.UpdatePressureBaseline(sessionID, 111, 1, fingerprint, fingerprint, fingerprint)
	trigger.UpdatePressureBaseline(stateKey, 222, 2, fingerprint, fingerprint, fingerprint)
	trigger.MigrateLegacyState(sessionID, stateKey)
	if got := trigger.PressureBaseline(stateKey); got.ActualTokens != 222 || got.MessageCount != 2 {
		t.Fatalf("Sawtooth 显式目标被裸状态覆盖: %+v", got)
	}

	frozen := NewFrozenStubs()
	legacyRaw := historyTextMessages("legacy")
	targetRaw := historyTextMessages("target")
	frozen.Store(sessionID, legacyRaw, 1, legacyRaw[0], 10, 20, legacyRaw)
	frozen.Store(stateKey, targetRaw, 1, targetRaw[0], 30, 40, targetRaw)
	frozen.MigrateLegacyState(nil, sessionID, stateKey)
	if got := frozen.Get(stateKey, targetRaw); got == nil || got.Tokens != 30 || got.RawTokens != 40 {
		t.Fatalf("Frozen 显式目标被裸状态覆盖: %+v", got)
	}

	decay := NewDecayTracker()
	decay.MarkStubbed(sessionID, 0, 1, 0.1)
	decay.MarkStubbed(stateKey, 0, 9, 0.9)
	decay.MigrateLegacyState(sessionID, stateKey)
	decay.mu.RLock()
	decayRequest := decay.stubbedAt[stateKey+":msg_0"]
	decay.mu.RUnlock()
	if decayRequest != 9 {
		t.Fatalf("Decay 显式目标被裸状态覆盖: request=%d", decayRequest)
	}
}

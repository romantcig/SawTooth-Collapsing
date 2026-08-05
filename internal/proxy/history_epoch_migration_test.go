package proxy

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

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

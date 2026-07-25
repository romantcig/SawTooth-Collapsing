package proxy

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

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
	dataDir := t.TempDir()
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

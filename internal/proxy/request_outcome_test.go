package proxy

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"
)

type outcomeDispatchFake struct {
	called int
	last   requestOutcomeSnapshot
}

func (f *outcomeDispatchFake) TryDispatch(snapshot requestOutcomeSnapshot) outcomeDispatchResult {
	f.called++
	f.last = snapshot
	return outcomeDispatchResult{Accepted: true, TerminalAccepted: true, SessionAccepted: true}
}

func attrsJSON(t *testing.T, attrs []slog.Attr) []byte {
	t.Helper()
	var buf bytes.Buffer
	handler := slog.NewJSONHandler(&buf, &slog.HandlerOptions{ReplaceAttr: func(_ []string, attr slog.Attr) slog.Attr {
		return attr
	}})
	record := slog.NewRecord(time.Unix(0, 0).UTC(), slog.LevelInfo, "outcome", 0)
	record.AddAttrs(attrs...)
	if err := handler.Handle(nil, record); err != nil {
		t.Fatalf("序列化 outcome attrs 失败: %v", err)
	}
	return buf.Bytes()
}

func TestRequestOutcomeSnapshotDefaults(t *testing.T) {
	started := time.Date(2026, 8, 9, 2, 0, 0, 123, time.UTC)
	fullSessionID := "PARENT-SESSION-AND-CREDENTIAL-MUST-NOT-LEAK"
	collector := newRequestOutcomeCollector(42, fullSessionID, started, nil)

	snapshot := collector.Snapshot()
	if snapshot.RequestID != 42 {
		t.Fatalf("request id=%d, want 42", snapshot.RequestID)
	}
	if snapshot.SessionHash != stableSessionHash(fullSessionID) {
		t.Fatalf("session hash=%q, want %q", snapshot.SessionHash, stableSessionHash(fullSessionID))
	}
	if !snapshot.StartedAt.Equal(started) {
		t.Fatalf("started_at=%s, want %s", snapshot.StartedAt, started)
	}
	if !snapshot.FinishedAt.IsZero() {
		t.Fatalf("未 seal 的 finished_at=%s，不应有终态", snapshot.FinishedAt)
	}
	if snapshot.Eligibility != outcomeEligibilityNotEvaluable {
		t.Fatalf("默认 AI eligibility=%q", snapshot.Eligibility)
	}
	if snapshot.TerminalEligibility != outcomeEligibilityNotApplicable {
		t.Fatalf("默认 terminal eligibility=%q", snapshot.TerminalEligibility)
	}
	if snapshot.Action != outcomeActionPassthrough {
		t.Fatalf("默认 action=%q", snapshot.Action)
	}
	if snapshot.UpstreamState != upstreamStateNotStarted {
		t.Fatalf("默认 upstream=%q", snapshot.UpstreamState)
	}
	if snapshot.MemoryState != persistenceStateNotAttempted || snapshot.DiskState != persistenceStateNotAttempted {
		t.Fatalf("默认 persistence memory=%q disk=%q", snapshot.MemoryState, snapshot.DiskState)
	}
	if snapshot.FailureClass != persistenceFailureUnknown {
		t.Fatalf("默认 failure=%q", snapshot.FailureClass)
	}
	if snapshot.Intervention != interventionNone {
		t.Fatalf("默认 intervention=%q", snapshot.Intervention)
	}

	collector.SetEligibility(outcomeEligibility("untrusted-value"))
	collector.SetTerminalEligibility(outcomeEligibility("untrusted-value"))
	collector.SetTriggerReason(TriggerReason("untrusted-value"))
	collector.SetAction(outcomeAction("untrusted-value"))
	collector.SetUpstreamState(upstreamState("untrusted-value"))
	collector.SetMemoryState(persistenceState("untrusted-value"))
	collector.SetDiskState(persistenceState("untrusted-value"))
	collector.SetFailureClass(persistenceFailureClass("untrusted-value"))
	collector.SetIntervention(interventionState("untrusted-value"))

	snapshot = collector.Snapshot()
	if snapshot.Eligibility != outcomeEligibilityUnknown || snapshot.TerminalEligibility != outcomeEligibilityUnknown {
		t.Fatalf("未知 eligibility 未归一: ai=%q terminal=%q", snapshot.Eligibility, snapshot.TerminalEligibility)
	}
	if snapshot.TriggerReason != TriggerUnknown || snapshot.Action != outcomeActionUnknown {
		t.Fatalf("未知 trigger/action 未归一: trigger=%q action=%q", snapshot.TriggerReason, snapshot.Action)
	}
	if snapshot.UpstreamState != upstreamStateUnknown || snapshot.MemoryState != persistenceStateUnknown || snapshot.DiskState != persistenceStateUnknown {
		t.Fatalf("未知 state 未归一: upstream=%q memory=%q disk=%q", snapshot.UpstreamState, snapshot.MemoryState, snapshot.DiskState)
	}
	if snapshot.FailureClass != persistenceFailureUnknown || snapshot.Intervention != interventionUnknown {
		t.Fatalf("未知 failure/intervention 未归一: failure=%q intervention=%q", snapshot.FailureClass, snapshot.Intervention)
	}
}

func TestRequestOutcomeSafeAttrs(t *testing.T) {
	fullSessionID := "SESSION-ID-MUST-NOT-LEAK"
	collector := newRequestOutcomeCollector(77, fullSessionID, time.Unix(123, 456).UTC(), nil)
	collector.SetTerminalEligibility(outcomeEligibilityTerminalEligible)
	collector.SetEligibility(outcomeEligibilityEvaluable)
	collector.SetTriggerReason(TriggerTokens)
	collector.SetPressureSource(pressureSourceActualPlusDelta)
	collector.SetWait(5*time.Minute, 2*time.Minute, true)
	collector.SetAction(outcomeActionCollapse)
	collector.SetSizes(100, 20, 24000, 6000)
	collector.SetRecallCounts(1, 4, 2, 2, 2)
	collector.SetUpstreamState(upstreamStateSuccess)
	collector.SetUpstreamStatus(200)
	collector.SetMemoryState(persistenceStateSaved)
	collector.SetDiskState(persistenceStateFailed)
	collector.SetFailureClass(persistenceFailureSQLite)
	collector.SetIntervention(interventionRequired)

	data := attrsJSON(t, collector.Snapshot().SafeLogAttrs())
	if bytes.Contains(data, []byte(fullSessionID)) {
		t.Fatalf("safe attrs 泄漏完整 session: %s", data)
	}
	if !json.Valid(data) {
		t.Fatalf("safe attrs 不是 JSON: %s", data)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		t.Fatal(err)
	}
	for key := range fields {
		lower := strings.ToLower(key)
		for _, denied := range []string{"session_id", "parent", "authorization", "credential", "body", "base64", "fingerprint", "path", "key", "raw", "error_text"} {
			if strings.Contains(lower, denied) {
				t.Fatalf("safe attrs 出现敏感 key %q: %s", key, data)
			}
		}
	}
	if !bytes.Contains(data, []byte(`"session_hash"`)) {
		t.Fatalf("safe attrs 缺少短 session hash: %s", data)
	}
	if bytes.Contains(data, []byte("AUTHORIZATION-MUST-NOT-LEAK")) ||
		bytes.Contains(data, []byte("BODY-MUST-NOT-LEAK")) ||
		bytes.Contains(data, []byte("FULL-FINGERPRINT-MUST-NOT-LEAK")) {
		t.Fatalf("safe attrs 包含敏感 sentinel: %s", data)
	}
}

func TestRequestMetaInitializesOutcome(t *testing.T) {
	fullSessionID := "request-meta-session"
	meta := newRequestMeta(101, fullSessionID)
	if meta == nil || meta.Outcome == nil {
		t.Fatal("newRequestMeta 未初始化 Outcome")
	}
	if got := meta.Outcome.Snapshot(); got.RequestID != meta.ID || got.SessionHash != meta.SessionHash {
		t.Fatalf("Outcome identity 未固定: %+v meta=%+v", got, meta)
	}
	metaWithRun := newRequestMetaWithRun(102, fullSessionID, "0123456789abcdef")
	if metaWithRun == nil || metaWithRun.Outcome == nil {
		t.Fatal("newRequestMetaWithRun 未初始化 Outcome")
	}
	if metaWithRun.Logger == nil || metaWithRun.auxiliaryLogger() == nil {
		t.Fatal("request logger 路由属性被破坏")
	}
	if metaWithRun.Outcome.Snapshot().SessionHash != stableSessionHash(fullSessionID) {
		t.Fatal("Outcome 未使用 stableSessionHash")
	}
}

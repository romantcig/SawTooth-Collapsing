package proxy

import (
	"math"
	"sync"
	"testing"
	"time"
)

func gapTestEvent(source OutcomeGapSource, requestID uint64, at time.Time, reason string) OutcomeGapEvent {
	return OutcomeGapEvent{
		Source:      source,
		SessionHash: "0123456789abcdef",
		RequestID:   requestID,
		From:        at,
		To:          at,
		Reason:      reason,
	}
}

func hasRecoverableScope(result OutcomeGapCommitResult, want HealthScope) (uint64, bool) {
	for _, scope := range result.RecoverableScopes {
		if scope == want {
			generation, ok := result.CommittedGeneration(want)
			return generation, ok
		}
	}
	return 0, false
}

func TestOutcomeGapAccumulate(t *testing.T) {
	acc := NewOutcomeGapAccumulator()
	base := time.Unix(100, 0).UTC()
	if got := acc.Accumulate(gapTestEvent(OutcomeGapSourceSessionQueueFull, 7, base, "queue_full")); got != 1 {
		t.Fatalf("first generation=%d, want 1", got)
	}
	if got := acc.Accumulate(gapTestEvent(OutcomeGapSourceSessionQueueFull, 8, base.Add(time.Second), "queue_full")); got != 2 {
		t.Fatalf("second generation=%d, want 2", got)
	}
	if got := acc.Accumulate(gapTestEvent(OutcomeGapSourceSessionLogSink, 9, base.Add(2*time.Second), "sink_write")); got != 1 {
		t.Fatalf("sink generation=%d, want 1", got)
	}

	queue := acc.Snapshot(OutcomeGapSourceSessionQueueFull)
	if queue.Generation != 2 || queue.Count != 2 {
		t.Fatalf("queue aggregate=%+v, want generation=2 count=2", queue)
	}
	if queue.First.RequestID != 7 || queue.Last.RequestID != 8 {
		t.Fatalf("queue correlation=%+v, want first=7 last=8", queue)
	}
	if !queue.From.Equal(base) || !queue.To.Equal(base.Add(time.Second)) {
		t.Fatalf("queue range=%v..%v", queue.From, queue.To)
	}
	if queue.Reason != "queue_full" || queue.SourceMask == 0 {
		t.Fatalf("queue reason/mask=%q/%d", queue.Reason, queue.SourceMask)
	}
	if sink := acc.Snapshot(OutcomeGapSourceSessionLogSink); sink.Generation != 1 || sink.Count != 1 {
		t.Fatalf("sink aggregate=%+v", sink)
	}
}

func TestOutcomeGapGenerationAwareCommit(t *testing.T) {
	acc := NewOutcomeGapAccumulator()
	acc.Accumulate(gapTestEvent(OutcomeGapSourceSessionQueueFull, 1, time.Unix(1, 0), "queue_full"))
	claim := acc.Claim()
	result := acc.Commit(claim)
	if generation, ok := hasRecoverableScope(result, HealthScopeSessionQueueFull); !ok || generation != 1 {
		t.Fatalf("commit=%+v, want queue@1", result)
	}
	if replay := acc.Commit(claim); len(replay.RecoverableScopes) != 0 {
		t.Fatalf("replayed commit recovered=%+v", replay)
	}
}

func TestOutcomeGapSelectiveScopeRecovery(t *testing.T) {
	acc := NewOutcomeGapAccumulator()
	acc.Accumulate(gapTestEvent(OutcomeGapSourceSessionQueueFull, 1, time.Unix(1, 0), "queue_full"))
	acc.Accumulate(gapTestEvent(OutcomeGapSourceSessionLogSink, 2, time.Unix(2, 0), "sink_write"))
	claim := acc.Claim()
	acc.Accumulate(gapTestEvent(OutcomeGapSourceSessionQueueFull, 3, time.Unix(3, 0), "queue_full"))
	result := acc.Commit(claim)
	if _, ok := hasRecoverableScope(result, HealthScopeSessionQueueFull); ok {
		t.Fatalf("stale queue unexpectedly recovered: %+v", result)
	}
	if generation, ok := hasRecoverableScope(result, HealthScopeSessionLogSink); !ok || generation != 1 {
		t.Fatalf("selective result=%+v, want sink@1", result)
	}

	second := acc.Claim()
	result = acc.Commit(second)
	if generation, ok := hasRecoverableScope(result, HealthScopeSessionQueueFull); !ok || generation != 2 {
		t.Fatalf("second result=%+v, want queue@2", result)
	}
}

func TestOutcomeGapMergePreservesMaxGeneration(t *testing.T) {
	acc := NewOutcomeGapAccumulator()
	base := time.Unix(10, 0).UTC()
	acc.Accumulate(gapTestEvent(OutcomeGapSourceSessionQueueFull, 1, base, "queue_full"))
	acc.Accumulate(gapTestEvent(OutcomeGapSourceSessionLogSink, 2, base, "sink_write"))
	claim := acc.Claim()
	acc.Accumulate(gapTestEvent(OutcomeGapSourceSessionQueueFull, 3, base.Add(time.Second), "queue_full"))
	acc.Accumulate(gapTestEvent(OutcomeGapSourceSessionLogSink, 4, base.Add(2*time.Second), "sink_write_2"))
	acc.Merge(claim)
	acc.Merge(claim)

	queue := acc.Snapshot(OutcomeGapSourceSessionQueueFull)
	if queue.Generation != 2 || queue.Count != 2 || queue.First.RequestID != 1 || queue.Last.RequestID != 3 {
		t.Fatalf("merged queue=%+v", queue)
	}
	sink := acc.Snapshot(OutcomeGapSourceSessionLogSink)
	if sink.Generation != 2 || sink.Count != 2 || sink.First.RequestID != 2 || sink.Last.RequestID != 4 {
		t.Fatalf("merged sink=%+v", sink)
	}
	if sink.Reason != "mixed" {
		t.Fatalf("merged sink reason=%q, want mixed", sink.Reason)
	}
	// Merge 本身没有 recovery 结果，也不会清空已合并的 current；恢复只能
	// 由后续成功写入并显式 Commit 新 claim 证明。
	if !acc.HasCurrent(OutcomeGapSourceSessionQueueFull) || !acc.HasCurrent(OutcomeGapSourceSessionLogSink) {
		t.Fatal("merge unexpectedly cleared current aggregates")
	}
}

func TestOutcomeGapConcurrentBounded(t *testing.T) {
	acc := NewOutcomeGapAccumulator()
	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			acc.Accumulate(gapTestEvent(OutcomeGapSourceSessionQueueFull, uint64(id+1), time.Unix(int64(id), 0), "queue_full"))
		}(i)
	}
	wg.Wait()
	snapshot := acc.Snapshot(OutcomeGapSourceSessionQueueFull)
	if snapshot.Count != 1000 || snapshot.Generation != 1000 {
		t.Fatalf("concurrent aggregate=%+v, want count/generation=1000", snapshot)
	}
	if snapshot.Count == math.MaxUint64 {
		t.Fatal("unexpected saturated count")
	}
}

func TestOutcomeGapCompatibilityAliasesAndImplicitClaim(t *testing.T) {
	acc := NewOutcomeGapAccumulator()
	when := time.Unix(42, 123).UTC()
	acc.Accumulate(OutcomeGapEvent{
		Source:      OutcomeGapSourceSessionQueueFull,
		SessionHash: "raw-session-secret",
		RequestID:   17,
		From:        when,
		To:          when.Add(time.Second),
		Reason:      "queue_full",
	})
	current := acc.Current(HealthScopeSessionQueueFull)
	if current.FirstShortHash != current.First.SessionHash || current.LastShortHash != current.Last.SessionHash {
		t.Fatalf("short hash aliases diverged: %+v", current)
	}
	if !current.FromTime.Equal(current.From) || !current.ToTime.Equal(current.To) {
		t.Fatalf("time aliases diverged: %+v", current)
	}
	if got := acc.Generation(OutcomeGapSourceSessionQueueFull); got != current.Generation {
		t.Fatalf("generation accessor=%d, snapshot=%d", got, current.Generation)
	}
	claim := acc.Claim()
	if !acc.ClaimInFlight() {
		t.Fatal("claim should be in flight")
	}
	result := acc.Commit()
	if result.ClaimID != claim.ID || result.RecoverableCount != 1 || !result.IsRecoverable(HealthScopeSessionQueueFull) {
		t.Fatalf("implicit commit=%+v claim=%+v", result, claim)
	}
	if acc.ClaimInFlight() {
		t.Fatal("commit should consume in-flight claim")
	}
}

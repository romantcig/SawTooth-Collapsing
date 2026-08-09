package proxy

import (
	"sync"
	"testing"
)

type healthTestReporter struct {
	mu          sync.Mutex
	transitions []HealthTransition
}

func (r *healthTestReporter) ReportHealthTransition(transition HealthTransition) {
	r.mu.Lock()
	r.transitions = append(r.transitions, transition)
	r.mu.Unlock()
}

func (r *healthTestReporter) snapshot() []HealthTransition {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]HealthTransition(nil), r.transitions...)
}

func TestHealthTrackerRejectsStaleGapCommit(t *testing.T) {
	reporter := &healthTestReporter{}
	tracker := NewHealthTracker(reporter)
	if got := tracker.ObserveFailure(HealthScopeSessionQueueFull, HealthFailureClassSessionQueueFull, 1); got.Kind != HealthTransitionEntered {
		t.Fatalf("first failure=%+v, want entered", got)
	}
	if got := tracker.ObserveFailure(HealthScopeSessionQueueFull, HealthFailureClassSessionQueueFull, 2); got.Kind != HealthTransitionOngoing {
		t.Fatalf("second failure=%+v, want ongoing", got)
	}
	if got := tracker.ObserveGapCommit(HealthScopeSessionQueueFull, 1); got.Kind == HealthTransitionRecovered {
		t.Fatalf("stale commit recovered: %+v", got)
	}
	if tracker.LatestFailureGeneration(HealthScopeSessionQueueFull) != 2 || !tracker.IsDegraded(HealthScopeSessionQueueFull) {
		t.Fatalf("stale commit changed state: degraded=%v generation=%d", tracker.IsDegraded(HealthScopeSessionQueueFull), tracker.LatestFailureGeneration(HealthScopeSessionQueueFull))
	}
	if got := tracker.ObserveGapCommit(HealthScopeSessionQueueFull, 2); got.Kind != HealthTransitionRecovered {
		t.Fatalf("matching commit=%+v, want recovered", got)
	}
}

func TestHealthTrackerTransitionsAllScopes(t *testing.T) {
	reporter := &healthTestReporter{}
	tracker := NewHealthTracker(reporter)
	cases := []struct {
		scope HealthScope
		class HealthFailureClass
		gap   bool
	}{
		{HealthScopeSQLiteState, HealthFailureClassSQLiteState, false},
		{HealthScopeSQLiteArchive, HealthFailureClassSQLiteArchive, false},
		{HealthScopeSessionQueueFull, HealthFailureClassSessionQueueFull, true},
		{HealthScopeSessionLogSink, HealthFailureClassSessionLogSink, true},
	}
	for _, tc := range cases {
		tracker.ObserveFailure(tc.scope, tc.class, 1)
		tracker.ObserveFailure(tc.scope, tc.class, 1)
		if tc.gap {
			tracker.ObserveGapCommit(tc.scope, 1)
		} else {
			tracker.ObserveSuccess(tc.scope, tc.class)
		}
	}
	transitions := reporter.snapshot()
	if len(transitions) != 8 {
		t.Fatalf("reported transitions=%d, want 8: %+v", len(transitions), transitions)
	}
	seen := make(map[HealthScope]int)
	for _, transition := range transitions {
		if transition.Kind != HealthTransitionEntered && transition.Kind != HealthTransitionRecovered {
			t.Fatalf("unexpected reported transition=%+v", transition)
		}
		seen[transition.Scope]++
	}
	for _, tc := range cases {
		if seen[tc.scope] != 2 {
			t.Fatalf("scope %s reports=%d, want entered+recovered", tc.scope, seen[tc.scope])
		}
	}
}

func TestHealthTrackerScopesIndependent(t *testing.T) {
	tracker := NewHealthTracker(&healthTestReporter{})
	tracker.ObserveFailure(HealthScopeSessionQueueFull, HealthFailureClassSessionQueueFull, 4)
	tracker.ObserveFailure(HealthScopeSQLiteState, HealthFailureClassSQLiteState, 1)
	tracker.ObserveSuccess(HealthScopeSQLiteState, HealthFailureClassSQLiteState)
	if !tracker.IsDegraded(HealthScopeSessionQueueFull) {
		t.Fatal("sqlite success cleared queue scope")
	}
	if tracker.IsDegraded(HealthScopeSQLiteState) {
		t.Fatal("sqlite state remained degraded after matching success")
	}
	tracker.ObserveGapCommit(HealthScopeSessionQueueFull, 4)
	if tracker.IsDegraded(HealthScopeSessionQueueFull) {
		t.Fatal("queue scope did not recover")
	}
}

func TestHealthTransitionReporterFiltersOngoing(t *testing.T) {
	reporter := &healthTestReporter{}
	tracker := NewHealthTracker(reporter)
	tracker.ObserveFailure(HealthScopeSessionLogSink, HealthFailureClassSessionLogSink, 1)
	tracker.ObserveFailure(HealthScopeSessionLogSink, HealthFailureClassSessionLogSink, 2)
	tracker.ObserveFailure(HealthScopeSessionLogSink, HealthFailureClassSessionLogSink, 3)
	tracker.ObserveGapCommit(HealthScopeSessionLogSink, 1)
	tracker.ObserveGapCommit(HealthScopeSessionLogSink, 2)
	tracker.ObserveGapCommit(HealthScopeSessionLogSink, 3)
	transitions := reporter.snapshot()
	if len(transitions) != 2 {
		t.Fatalf("ongoing/stale transitions reported=%+v", transitions)
	}
	if transitions[0].Kind != HealthTransitionEntered || transitions[1].Kind != HealthTransitionRecovered {
		t.Fatalf("reported=%+v, want entered/recovered", transitions)
	}
}


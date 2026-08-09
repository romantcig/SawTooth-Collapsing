package proxy

import (
	"errors"
	"sync"
	"testing"
	"time"
)

type dispatcherTestReporter struct {
	mu          sync.Mutex
	transitions []HealthTransition
}

func (r *dispatcherTestReporter) ReportHealthTransition(transition HealthTransition) {
	r.mu.Lock()
	r.transitions = append(r.transitions, transition)
	r.mu.Unlock()
}

func (r *dispatcherTestReporter) snapshot() []HealthTransition {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]HealthTransition(nil), r.transitions...)
}

type dispatcherTerminalFake struct {
	mu      sync.Mutex
	entered chan struct{}
	release chan struct{}
	block   bool
	jobs    []terminalAdmissionJob
	errors  int
}

func (f *dispatcherTerminalFake) ProjectTerminal(snapshot requestOutcomeSnapshot, admission outcomeDispatchResult) error {
	f.mu.Lock()
	if f.entered != nil {
		select {
		case <-f.entered:
		default:
			close(f.entered)
		}
	}
	f.jobs = append(f.jobs, terminalAdmissionJob{snapshot: snapshot, admission: admission})
	block := f.block
	release := f.release
	f.mu.Unlock()
	if block && release != nil {
		<-release
	}
	return nil
}

func (f *dispatcherTerminalFake) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.jobs)
}

func (f *dispatcherTerminalFake) last() terminalAdmissionJob {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.jobs) == 0 {
		return terminalAdmissionJob{}
	}
	return f.jobs[len(f.jobs)-1]
}

type dispatcherSessionFake struct {
	mu      sync.Mutex
	entered chan struct{}
	release chan struct{}
	block   bool
	jobs    []requestOutcomeSnapshot
	err     error
}

func (f *dispatcherSessionFake) ProjectSession(snapshot requestOutcomeSnapshot) error {
	f.mu.Lock()
	if f.entered != nil {
		select {
		case <-f.entered:
		default:
			close(f.entered)
		}
	}
	f.jobs = append(f.jobs, snapshot)
	block := f.block
	release := f.release
	err := f.err
	f.mu.Unlock()
	if block && release != nil {
		<-release
	}
	return err
}

func (f *dispatcherSessionFake) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.jobs)
}

func dispatcherTestSnapshot(id uint64) requestOutcomeSnapshot {
	started := time.Unix(int64(id), 0).UTC()
	return requestOutcomeSnapshot{
		RequestID:           id,
		SessionHash:         "0123456789abcdef",
		StartedAt:           started,
		FinishedAt:          started.Add(time.Millisecond),
		Eligibility:         outcomeEligibilityEvaluable,
		TerminalEligibility: outcomeEligibilityTerminalEligible,
		TriggerReason:       TriggerTokens,
		Action:              outcomeActionDirect,
		UpstreamState:       upstreamStateSuccess,
		MemoryState:         persistenceStateNotAttempted,
		DiskState:           persistenceStateNotAttempted,
		FailureClass:        persistenceFailureUnknown,
		Intervention:        interventionNone,
	}
}

func newDispatcherTest(t *testing.T, terminal *dispatcherTerminalFake, session *dispatcherSessionFake, reporter *dispatcherTestReporter) *OutcomeDispatcher {
	t.Helper()
	tracker := NewHealthTracker(reporter)
	dispatcher := NewOutcomeDispatcher(terminal, session, tracker, reporter)
	if dispatcher == nil || dispatcher.Configured() == false {
		t.Fatal("dispatcher did not retain explicit tracker/reporter configuration")
	}
	t.Cleanup(func() { _ = dispatcher.CloseAndDrain() })
	return dispatcher
}

func waitForDispatcher(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("dispatcher condition did not become true")
}

func channelSignaled(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func TestOutcomeDispatcherIndependentLanes(t *testing.T) {
	terminal := &dispatcherTerminalFake{entered: make(chan struct{}), release: make(chan struct{}), block: true}
	session := &dispatcherSessionFake{}
	reporter := &dispatcherTestReporter{}
	dispatcher := newDispatcherTest(t, terminal, session, reporter)
	dispatcher.TryDispatch(dispatcherTestSnapshot(1))
	waitForDispatcher(t, func() bool { return session.count() == 1 })
	close(terminal.release)
	waitForDispatcher(t, func() bool { return terminal.count() == 1 })

	terminal2 := &dispatcherTerminalFake{}
	session2 := &dispatcherSessionFake{entered: make(chan struct{}), release: make(chan struct{}), block: true}
	dispatcher2 := newDispatcherTest(t, terminal2, session2, &dispatcherTestReporter{})
	dispatcher2.TryDispatch(dispatcherTestSnapshot(2))
	waitForDispatcher(t, func() bool { return terminal2.count() == 1 })
	close(session2.release)
}

func TestOutcomeDispatcherSessionFullStillQueuesTerminal(t *testing.T) {
	terminal := &dispatcherTerminalFake{}
	session := &dispatcherSessionFake{entered: make(chan struct{}), release: make(chan struct{}), block: true}
	reporter := &dispatcherTestReporter{}
	dispatcher := newDispatcherTest(t, terminal, session, reporter)
	first := dispatcher.TryDispatch(dispatcherTestSnapshot(1))
	if !first.SessionAccepted {
		t.Fatalf("first admission=%+v", first)
	}
	waitForDispatcher(t, func() bool { return channelSignaled(session.entered) })
	for i := 0; i < OutcomeDispatcherQueueCapacity; i++ {
		dispatcher.TryDispatch(dispatcherTestSnapshot(uint64(i + 2)))
	}
	full := dispatcher.TryDispatch(dispatcherTestSnapshot(9999))
	if !full.SessionRejected || full.SessionAccepted {
		t.Fatalf("full admission=%+v, want rejected session", full)
	}
	waitForDispatcher(t, func() bool { return terminal.last().admission.SessionRejected })
	job := terminal.last()
	if job.snapshot.RequestID != 9999 || !job.admission.SessionRejected {
		t.Fatalf("terminal job=%+v, want rejected request 9999", job)
	}
	close(session.release)
}

func TestOutcomeDispatcherSessionFullHealthEnteredOnce(t *testing.T) {
	terminal := &dispatcherTerminalFake{}
	session := &dispatcherSessionFake{entered: make(chan struct{}), release: make(chan struct{}), block: true}
	reporter := &dispatcherTestReporter{}
	dispatcher := newDispatcherTest(t, terminal, session, reporter)
	dispatcher.TryDispatch(dispatcherTestSnapshot(1))
	waitForDispatcher(t, func() bool { return channelSignaled(session.entered) })
	for i := 0; i < OutcomeDispatcherQueueCapacity+3; i++ {
		dispatcher.TryDispatch(dispatcherTestSnapshot(uint64(i + 2)))
	}
	transitions := reporter.snapshot()
	entered := 0
	for _, transition := range transitions {
		if transition.Scope == HealthScopeSessionQueueFull && transition.Kind == HealthTransitionEntered {
			entered++
		}
		if transition.Kind == HealthTransitionOngoing {
			t.Fatalf("ongoing transition reached reporter: %+v", transition)
		}
	}
	if entered != 1 {
		t.Fatalf("queue entered reports=%d, transitions=%+v", entered, transitions)
	}
	close(session.release)
}

func TestOutcomeDispatcherAcceptedDoesNotRecoverQueueHealth(t *testing.T) {
	terminal := &dispatcherTerminalFake{}
	session := &dispatcherSessionFake{entered: make(chan struct{}), release: make(chan struct{}), block: true}
	reporter := &dispatcherTestReporter{}
	dispatcher := newDispatcherTest(t, terminal, session, reporter)
	dispatcher.TryDispatch(dispatcherTestSnapshot(1))
	waitForDispatcher(t, func() bool { return channelSignaled(session.entered) })
	for i := 0; i < OutcomeDispatcherQueueCapacity+1; i++ {
		dispatcher.TryDispatch(dispatcherTestSnapshot(uint64(i + 2)))
	}
	close(session.release)
	waitForDispatcher(t, func() bool { return session.count() > 1 })
	if !dispatcher.HealthTracker().IsDegraded(HealthScopeSessionQueueFull) {
		t.Fatal("accepted session enqueue falsely recovered queue scope")
	}
}

func TestOutcomeDispatcherTerminalFullIsBestEffortOnly(t *testing.T) {
	terminal := &dispatcherTerminalFake{entered: make(chan struct{}), release: make(chan struct{}), block: true}
	session := &dispatcherSessionFake{}
	reporter := &dispatcherTestReporter{}
	dispatcher := newDispatcherTest(t, terminal, session, reporter)
	dispatcher.TryDispatch(dispatcherTestSnapshot(1))
	waitForDispatcher(t, func() bool { return channelSignaled(terminal.entered) })
	for i := 0; i < OutcomeDispatcherQueueCapacity; i++ {
		dispatcher.TryDispatch(dispatcherTestSnapshot(uint64(i + 2)))
	}
	before := len(reporter.snapshot())
	full := dispatcher.TryDispatch(dispatcherTestSnapshot(9999))
	if !full.TerminalEmissionUnavailable || !full.SessionAccepted {
		t.Fatalf("terminal full result=%+v", full)
	}
	waitForDispatcher(t, func() bool { return session.count() >= 257 })
	if after := len(reporter.snapshot()); after != before {
		t.Fatalf("terminal overflow changed health reports %d -> %d", before, after)
	}
	if got := dispatcher.GapAccumulator().Snapshot(OutcomeGapSourceSessionQueueFull).Count; got != 0 {
		t.Fatalf("terminal overflow added queue gap count=%d", got)
	}
	close(terminal.release)
}

func TestOutcomeDispatcherCloseAndDrain(t *testing.T) {
	terminal := &dispatcherTerminalFake{}
	session := &dispatcherSessionFake{}
	dispatcher := newDispatcherTest(t, terminal, session, &dispatcherTestReporter{})
	for i := 0; i < 20; i++ {
		result := dispatcher.TryDispatch(dispatcherTestSnapshot(uint64(i + 1)))
		if !result.SessionAccepted || !result.TerminalAccepted {
			t.Fatalf("submission %d result=%+v", i, result)
		}
	}
	if err := dispatcher.CloseAndDrain(); err != nil {
		t.Fatalf("CloseAndDrain: %v", err)
	}
	if session.count() != 20 || terminal.count() != 20 {
		t.Fatalf("drain counts session=%d terminal=%d", session.count(), terminal.count())
	}
	if err := dispatcher.CloseAndDrain(); err != nil {
		t.Fatalf("second CloseAndDrain: %v", err)
	}
}

func TestOutcomeDispatcherClosingIsLifecycleError(t *testing.T) {
	terminal := &dispatcherTerminalFake{}
	session := &dispatcherSessionFake{}
	dispatcher := newDispatcherTest(t, terminal, session, &dispatcherTestReporter{})
	if err := dispatcher.CloseAndDrain(); err != nil {
		t.Fatalf("CloseAndDrain: %v", err)
	}
	before := dispatcher.GapAccumulator().Snapshot(OutcomeGapSourceSessionQueueFull)
	result := dispatcher.TryDispatch(dispatcherTestSnapshot(77))
	if !result.LifecycleError || result.Accepted {
		t.Fatalf("closing result=%+v", result)
	}
	after := dispatcher.GapAccumulator().Snapshot(OutcomeGapSourceSessionQueueFull)
	if after.Count != before.Count {
		t.Fatalf("closing added normal gap: before=%+v after=%+v", before, after)
	}
	if !errors.Is(dispatcher.LastLifecycleError(), ErrOutcomeDispatcherClosing) {
		t.Fatalf("last lifecycle error=%v", dispatcher.LastLifecycleError())
	}
}

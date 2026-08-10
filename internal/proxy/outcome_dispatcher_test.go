package proxy

import (
	"bytes"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
)

// lockedBuffer 让终端 writer 在 race 检测下可被测试安全读取；
// LogHandler 自身已保证整行写入，这里只额外保护读侧。
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func (b *lockedBuffer) lines() []string {
	text := strings.TrimSuffix(b.String(), "\n")
	if text == "" {
		return nil
	}
	return strings.Split(text, "\n")
}

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

// terminalLaneFixture 组装真实 terminal renderer + terminal-only health reporter，
// 用于验证两条 lane 的实际投影而不是 fake seam。
type terminalLaneFixture struct {
	out        *lockedBuffer
	handler    *LogHandler
	reporter   *TerminalHealthReporter
	tracker    *HealthTracker
	dispatcher *OutcomeDispatcher
}

func newTerminalLaneFixture(t *testing.T, session any) *terminalLaneFixture {
	t.Helper()
	out := &lockedBuffer{}
	handler := NewLogHandler(out, slog.LevelInfo)
	reporter := NewTerminalHealthReporter(handler)
	tracker := NewHealthTracker(reporter)
	dispatcher := NewOutcomeDispatcher(OutcomeDispatcherOptions{
		TerminalProjector: handler,
		SessionProjector:  session,
		HealthTracker:     tracker,
		Reporter:          reporter,
	})
	if !dispatcher.Configured() {
		t.Fatal("dispatcher 未接受 terminal renderer/health reporter 配置")
	}
	t.Cleanup(func() { _ = dispatcher.CloseAndDrain() })
	return &terminalLaneFixture{out: out, handler: handler, reporter: reporter, tracker: tracker, dispatcher: dispatcher}
}

func countOutcomeResultLines(lines []string) int {
	count := 0
	for _, line := range lines {
		if strings.Contains(line, "ST ") {
			count++
		}
	}
	return count
}

func TestTerminalOutcomeLateSessionFailureIsNotRetroactive(t *testing.T) {
	release := make(chan struct{})
	session := sessionProjectorFunc(func(requestOutcomeSnapshot) error {
		<-release
		return errors.New("session sink failed")
	})
	fixture := newTerminalLaneFixture(t, session)

	result := fixture.dispatcher.TryDispatch(dispatcherTestSnapshot(41))
	if !result.SessionAccepted || !result.TerminalAccepted {
		t.Fatalf("admission=%+v，want session/terminal 均接受", result)
	}
	waitForDispatcher(t, func() bool { return len(fixture.out.lines()) == 1 })
	before := fixture.out.String()
	if strings.Contains(before, "详细记录未保存") {
		t.Fatalf("accepted 请求错误追加缺失后缀: %q", before)
	}

	close(release)
	waitForDispatcher(t, func() bool { return len(fixture.out.lines()) == 2 })
	after := fixture.out.String()
	if !strings.HasPrefix(after, before) {
		t.Fatalf("已输出的终端结果被回改: before=%q after=%q", before, after)
	}
	lines := fixture.out.lines()
	if got := countOutcomeResultLines(lines); got != 1 {
		t.Fatalf("终端结果行数=%d，want 1: %q", got, lines)
	}
	if !strings.Contains(lines[1], "详细记录暂时写不进文件") {
		t.Fatalf("accepted 后的写入失败未由独立健康提示报告: %q", lines[1])
	}
	if !fixture.tracker.IsDegraded(HealthScopeSessionLogSink) {
		t.Fatal("session_log_sink 未进入退化")
	}
	if fixture.tracker.IsDegraded(HealthScopeSessionQueueFull) {
		t.Fatal("写入失败错误污染 session_queue_full scope")
	}
}

func TestTerminalOutcomeOverflowIsBestEffortOnly(t *testing.T) {
	sessionDone := make(chan struct{}, OutcomeDispatcherQueueCapacity+8)
	session := sessionProjectorFunc(func(requestOutcomeSnapshot) error {
		sessionDone <- struct{}{}
		return nil
	})
	blockTerminal := make(chan struct{})
	terminalEntered := make(chan struct{})
	var enteredOnce sync.Once

	out := &lockedBuffer{}
	handler := NewLogHandler(out, slog.LevelInfo)
	reporter := NewTerminalHealthReporter(handler)
	tracker := NewHealthTracker(reporter)
	terminal := terminalProjectorFunc(func(snapshot requestOutcomeSnapshot, admission outcomeDispatchResult) error {
		enteredOnce.Do(func() { close(terminalEntered) })
		<-blockTerminal
		return handler.ProjectTerminal(snapshot, admission)
	})
	dispatcher := NewOutcomeDispatcher(OutcomeDispatcherOptions{
		TerminalProjector: terminal,
		SessionProjector:  session,
		HealthTracker:     tracker,
		Reporter:          reporter,
	})
	t.Cleanup(func() { close(blockTerminal); _ = dispatcher.CloseAndDrain() })

	dispatcher.TryDispatch(dispatcherTestSnapshot(1))
	<-terminalEntered
	for i := 0; i < OutcomeDispatcherQueueCapacity; i++ {
		dispatcher.TryDispatch(dispatcherTestSnapshot(uint64(i + 2)))
	}
	beforeLines := len(out.lines())
	overflow := dispatcher.TryDispatch(dispatcherTestSnapshot(9999))
	if !overflow.TerminalEmissionUnavailable || !overflow.SessionAccepted {
		t.Fatalf("terminal 饱和结果=%+v", overflow)
	}
	if dispatcher.TerminalOverflowCount() == 0 {
		t.Fatal("terminal 饱和未计入有界计数")
	}
	waitForDispatcher(t, func() bool { return len(sessionDone) >= OutcomeDispatcherQueueCapacity+2 })

	if after := len(out.lines()); after != beforeLines {
		t.Fatalf("terminal 饱和产生了补偿输出 %d -> %d", beforeLines, after)
	}
	for _, scope := range HealthScopes() {
		if tracker.IsDegraded(scope) {
			t.Fatalf("terminal 饱和污染 health scope=%s", scope)
		}
	}
	for _, source := range []OutcomeGapSource{OutcomeGapSourceSessionQueueFull, OutcomeGapSourceSessionLogSink} {
		if got := dispatcher.GapAccumulator().Snapshot(source).Count; got != 0 {
			t.Fatalf("terminal 饱和写入 gap source=%s count=%d", source, got)
		}
	}
}

func TestSessionQueueFullEnteredOnceAndAcceptedDoesNotRecover(t *testing.T) {
	terminal := &lockedBuffer{}
	handler := NewLogHandler(terminal, slog.LevelInfo)
	reporter := NewTerminalHealthReporter(handler)
	tracker := NewHealthTracker(reporter)
	gaps := NewOutcomeGapAccumulator()
	sink := &fakeOutcomeSink{}

	sessionEntered := make(chan struct{})
	releaseSession := make(chan struct{})
	var sessionOnce sync.Once
	sink.setSessionHook(func() {
		sessionOnce.Do(func() {
			close(sessionEntered)
			<-releaseSession
		})
	})
	processEntered := make(chan struct{})
	releaseProcess := make(chan struct{})
	var processOnce sync.Once
	sink.setProcessHook(func() {
		processOnce.Do(func() {
			close(processEntered)
			<-releaseProcess
		})
	})

	dispatcher := NewOutcomeDispatcher(OutcomeDispatcherOptions{
		TerminalProjector: handler,
		SessionProjector:  NewSessionOutcomeWriter(sink, gaps, tracker),
		HealthTracker:     tracker,
		Reporter:          reporter,
		GapAccumulator:    gaps,
	})
	t.Cleanup(func() { _ = dispatcher.CloseAndDrain() })

	dispatcher.TryDispatch(dispatcherTestSnapshot(1))
	<-sessionEntered
	rejected := 0
	for i := 0; i < OutcomeDispatcherQueueCapacity+4; i++ {
		if dispatcher.TryDispatch(dispatcherTestSnapshot(uint64(i + 2))).SessionRejected {
			rejected++
		}
	}
	if rejected < 3 {
		t.Fatalf("队列饱和拒绝次数=%d，want >=3", rejected)
	}
	if got := countTerminalText(terminal.lines(), "详细记录暂时来不及保存"); got != 1 {
		t.Fatalf("queue entered 提示=%d，want 1: %v", got, terminal.lines())
	}

	close(releaseSession)
	<-processEntered
	// 此刻：多次 accepted enqueue 已发生，一条普通 closure 也已成功写入，
	// 但缺口摘要尚未落盘，queue scope 必须仍然退化。
	if !tracker.IsDegraded(HealthScopeSessionQueueFull) {
		t.Fatal("accepted enqueue 或普通 closure 写入伪造了 queue 恢复")
	}
	if got := countTerminalText(terminal.lines(), "详细记录已恢复保存"); got != 0 {
		t.Fatalf("摘要落盘前出现恢复提示 %d 次", got)
	}

	close(releaseProcess)
	waitForDispatcher(t, func() bool { return !tracker.IsDegraded(HealthScopeSessionQueueFull) })
	if got := countTerminalText(terminal.lines(), "详细记录已恢复保存"); got != 1 {
		t.Fatalf("queue recovered 提示=%d，want 1: %v", got, terminal.lines())
	}
	records := sink.process()
	if len(records) != 1 {
		t.Fatalf("process-level 摘要记录数=%d，want 1: %v", len(records), records)
	}
	if !strings.Contains(records[0], "event=outcome_gap") || !strings.Contains(records[0], "session_queue_full.count=") {
		t.Fatalf("摘要缺少 queue 缺口证据: %q", records[0])
	}
}

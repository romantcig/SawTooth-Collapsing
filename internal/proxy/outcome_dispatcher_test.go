package proxy

import (
	"errors"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

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

func newDispatcherTest(t *testing.T, terminal *dispatcherTerminalFake, session *dispatcherSessionFake, reporter *healthTestReporter) *OutcomeDispatcher {
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
	reporter := &healthTestReporter{}
	dispatcher := newDispatcherTest(t, terminal, session, reporter)
	dispatcher.TryDispatch(dispatcherTestSnapshot(1))
	waitForDispatcher(t, func() bool { return session.count() == 1 })
	close(terminal.release)
	waitForDispatcher(t, func() bool { return terminal.count() == 1 })

	terminal2 := &dispatcherTerminalFake{}
	session2 := &dispatcherSessionFake{entered: make(chan struct{}), release: make(chan struct{}), block: true}
	dispatcher2 := newDispatcherTest(t, terminal2, session2, &healthTestReporter{})
	dispatcher2.TryDispatch(dispatcherTestSnapshot(2))
	waitForDispatcher(t, func() bool { return terminal2.count() == 1 })
	close(session2.release)
}

func TestOutcomeDispatcherSessionFullStillQueuesTerminal(t *testing.T) {
	terminal := &dispatcherTerminalFake{}
	session := &dispatcherSessionFake{entered: make(chan struct{}), release: make(chan struct{}), block: true}
	reporter := &healthTestReporter{}
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
	reporter := &healthTestReporter{}
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
	reporter := &healthTestReporter{}
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
	reporter := &healthTestReporter{}
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
	dispatcher := newDispatcherTest(t, terminal, session, &healthTestReporter{})
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
	dispatcher := newDispatcherTest(t, terminal, session, &healthTestReporter{})
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
	// 状态翻转在 tracker 锁内完成，恢复日志行在解锁后才写终端；等待必须锚在
	// 日志行上（日志出现蕴含状态已翻转），只等状态会在两个同步点之间放行。
	waitForDispatcher(t, func() bool {
		return countTerminalText(terminal.lines(), "详细记录已恢复保存") >= 1
	})
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

// sessionOutcomeFixture 组装真实 writer + gap accumulator + 四 scope tracker +
// terminal-only reporter，用来验证缺口证据与健康恢复的真实交互。
type sessionOutcomeFixture struct {
	sink     *fakeOutcomeSink
	gaps     *OutcomeGapAccumulator
	tracker  *HealthTracker
	reporter *TerminalHealthReporter
	terminal *lockedBuffer
	writer   *SessionOutcomeWriter
}

func newSessionOutcomeFixture(t *testing.T) *sessionOutcomeFixture {
	t.Helper()
	terminal := &lockedBuffer{}
	reporter := NewTerminalHealthReporter(NewLogHandler(terminal, slog.LevelInfo))
	tracker := NewHealthTracker(reporter)
	gaps := NewOutcomeGapAccumulator()
	sink := &fakeOutcomeSink{}
	return &sessionOutcomeFixture{
		sink:     sink,
		gaps:     gaps,
		tracker:  tracker,
		reporter: reporter,
		terminal: terminal,
		writer:   NewSessionOutcomeWriter(sink, gaps, tracker),
	}
}

// degrade 模拟一次真实受控缺口：先原子累计，再用返回的 generation 报告失败。
func (f *sessionOutcomeFixture) degrade(source OutcomeGapSource, scope HealthScope, requestID uint64, reason string) uint64 {
	generation := f.gaps.Accumulate(OutcomeGapEvent{
		Source:      source,
		SessionHash: "0123456789abcdef",
		RequestID:   requestID,
		From:        logTestTime,
		To:          logTestTime,
		Reason:      reason,
	})
	f.tracker.ObserveFailure(scope, HealthFailureClass(scope), generation)
	return generation
}

func (f *sessionOutcomeFixture) terminalLines() []string {
	return f.terminal.lines()
}

func countTerminalText(lines []string, text string) int {
	count := 0
	for _, line := range lines {
		if strings.Contains(line, text) {
			count++
		}
	}
	return count
}

func TestSessionOutcomeSingleClosure(t *testing.T) {
	dataDir := tempDirRetryCleanup(t)
	handler := NewSessionLogHandler(dataDir, slog.LevelDebug, nil)
	writer := NewSessionOutcomeWriter(handler, NewOutcomeGapAccumulator(), NewHealthTracker())
	snapshot := terminalOutcomeSnapshot(func(mutable *requestOutcomeSnapshot) {
		mutable.DiskState = persistenceStateFailed
		mutable.FailureClass = persistenceFailureSQLite
		mutable.Intervention = interventionRequired
	})
	if err := writer.ProjectSession(snapshot); err != nil {
		t.Fatalf("ProjectSession: %v", err)
	}

	sessionPath := filepath.Join(dataDir, "logs", snapshot.SessionHash+".log")
	content := readSessionLogFile(t, sessionPath)
	lines := strings.Split(strings.TrimSuffix(content, "\n"), "\n")
	if len(lines) != 1 {
		t.Fatalf("accepted 请求的 closure 行数=%d，want 1: %q", len(lines), content)
	}
	line := lines[0]
	for _, want := range []string{
		"event=request_outcome", "trigger=tokens", "action=collapse",
		"required_wait=4m0s", "actual_wait=5m0s", "actual_wait_known=true",
		"pressure_source=actual_plus_delta",
		"before_messages=120", "after_messages=40", "before_tokens=90000", "after_tokens=30000",
		"upstream=success", "upstream_status=200", "memory=saved", "disk=failed",
		"failure=sqlite", "intervention=required", "session=0123456789abcdef", "#75",
	} {
		if !strings.Contains(line, want) {
			t.Fatalf("closure 缺少字段 %q: %q", want, line)
		}
	}
	if !strings.HasPrefix(line, logTestTime.Format("15:04:05")+" ") {
		t.Fatalf("closure 不是 HH:MM:SS 前缀: %q", line)
	}
	if strings.Contains(line, "2026/") {
		t.Fatalf("closure 重复年月日: %q", line)
	}
	if _, err := os.Stat(filepath.Join(dataDir, "logs", "process.log")); err == nil {
		t.Fatal("健康路径不应把另一半因果链写进 process.log")
	}

	zeroed := terminalOutcomeSnapshot(func(mutable *requestOutcomeSnapshot) {
		mutable.RequestID = 76
		mutable.RequiredWait, mutable.ActualWait, mutable.ActualWaitKnown = 0, 0, false
		mutable.BeforeMessages, mutable.AfterMessages = 0, 0
		mutable.BeforeTokens, mutable.AfterTokens = 0, 0
		mutable.UpstreamStatus = 0
		mutable.Intervention = interventionNone
	})
	if err := writer.ProjectSession(zeroed); err != nil {
		t.Fatalf("ProjectSession(zeroed): %v", err)
	}
	sparse := strings.Split(strings.TrimSuffix(readSessionLogFile(t, sessionPath), "\n"), "\n")[1]
	for _, omitted := range []string{"required_wait", "actual_wait", "before_messages", "before_tokens", "upstream_status", "intervention", "failure"} {
		if strings.Contains(sparse, omitted) {
			t.Fatalf("零值字段 %q 未省略: %q", omitted, sparse)
		}
	}
}

func TestSessionOutcomeWholeRecordFailure(t *testing.T) {
	fixture := newSessionOutcomeFixture(t)
	fixture.sink.setSessionErr(errors.New("session sink write failed"))
	snapshot := terminalOutcomeSnapshot(nil)
	if err := fixture.writer.ProjectSession(snapshot); err == nil {
		t.Fatal("closure 写失败未返回错误")
	}
	if got := fixture.sink.session(snapshot.SessionHash); len(got) != 0 {
		t.Fatalf("closure 失败仍留下半条记录: %v", got)
	}
	if got := fixture.sink.process(); len(got) != 0 {
		t.Fatalf("closure 失败仍写 process 记录: %v", got)
	}
	if count := fixture.gaps.Snapshot(OutcomeGapSourceSessionLogSink).Count; count != 0 {
		t.Fatalf("writer 自行重复累计 sink gap count=%d，应由 dispatcher 统一累计", count)
	}

	terminal := &lockedBuffer{}
	reporter := NewTerminalHealthReporter(NewLogHandler(terminal, slog.LevelInfo))
	tracker := NewHealthTracker(reporter)
	gaps := NewOutcomeGapAccumulator()
	failingSink := &fakeOutcomeSink{}
	failingSink.setSessionErr(errors.New("session sink write failed"))
	dispatcher := NewOutcomeDispatcher(OutcomeDispatcherOptions{
		TerminalProjector: NewLogHandler(terminal, slog.LevelInfo),
		SessionProjector:  NewSessionOutcomeWriter(failingSink, gaps, tracker),
		HealthTracker:     tracker,
		Reporter:          reporter,
		GapAccumulator:    gaps,
	})
	t.Cleanup(func() { _ = dispatcher.CloseAndDrain() })
	dispatcher.TryDispatch(dispatcherTestSnapshot(11))
	waitForDispatcher(t, func() bool { return tracker.IsDegraded(HealthScopeSessionLogSink) })
	if got := gaps.Snapshot(OutcomeGapSourceSessionLogSink).Count; got != 1 {
		t.Fatalf("closure 写失败的整条缺口 count=%d，want 1", got)
	}
	if tracker.IsDegraded(HealthScopeSessionQueueFull) {
		t.Fatal("sink 写失败污染 queue scope")
	}
}

func TestOutcomeGapSameScopeStaleClaimBarrier(t *testing.T) {
	fixture := newSessionOutcomeFixture(t)
	fixture.degrade(OutcomeGapSourceSessionQueueFull, HealthScopeSessionQueueFull, 10, string(OutcomeGapReasonSessionQueueFull))

	entered := make(chan struct{})
	release := make(chan struct{})
	fixture.sink.setProcessHook(func() {
		close(entered)
		<-release
	})
	done := make(chan error, 1)
	go func() { done <- fixture.writer.ProjectSession(terminalOutcomeSnapshot(nil)) }()
	<-entered
	// 旧 claim 正在写入期间，同 scope 再次退化。
	fixture.degrade(OutcomeGapSourceSessionQueueFull, HealthScopeSessionQueueFull, 11, string(OutcomeGapReasonSessionQueueFull))
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("closure 写入失败: %v", err)
	}
	if !fixture.tracker.IsDegraded(HealthScopeSessionQueueFull) {
		t.Fatal("旧 claim 错误恢复了 claim 期间新增的同 scope 缺口")
	}
	if got := countTerminalText(fixture.terminalLines(), "详细记录已恢复保存"); got != 0 {
		t.Fatalf("stale claim 产生 recovered 提示 %d 次", got)
	}

	fixture.sink.setProcessHook(nil)
	second := terminalOutcomeSnapshot(func(mutable *requestOutcomeSnapshot) { mutable.RequestID = 76 })
	if err := fixture.writer.ProjectSession(second); err != nil {
		t.Fatalf("第二次 closure: %v", err)
	}
	if fixture.tracker.IsDegraded(HealthScopeSessionQueueFull) {
		t.Fatal("第二份摘要提交后仍未恢复 queue scope")
	}
	if got := countTerminalText(fixture.terminalLines(), "详细记录已恢复保存"); got != 1 {
		t.Fatalf("queue recovered 提示=%d，want 1: %v", got, fixture.terminalLines())
	}
	records := fixture.sink.process()
	if len(records) != 2 {
		t.Fatalf("process-level outcome_gap 记录数=%d，want 2: %v", len(records), records)
	}
	if !strings.Contains(records[1], "session_queue_full.generation=2") || !strings.Contains(records[1], "session_queue_full.count=1") {
		t.Fatalf("第二份摘要未记录新 generation/count: %q", records[1])
	}
}

func TestOutcomeGapMultiScopeSelectiveRecoveryBarrier(t *testing.T) {
	fixture := newSessionOutcomeFixture(t)
	fixture.degrade(OutcomeGapSourceSessionQueueFull, HealthScopeSessionQueueFull, 20, string(OutcomeGapReasonSessionQueueFull))
	fixture.degrade(OutcomeGapSourceSessionLogSink, HealthScopeSessionLogSink, 21, string(OutcomeGapReasonSessionLogSink))

	entered := make(chan struct{})
	release := make(chan struct{})
	fixture.sink.setProcessHook(func() {
		close(entered)
		<-release
	})
	done := make(chan error, 1)
	go func() { done <- fixture.writer.ProjectSession(terminalOutcomeSnapshot(nil)) }()
	<-entered
	fixture.degrade(OutcomeGapSourceSessionQueueFull, HealthScopeSessionQueueFull, 22, string(OutcomeGapReasonSessionQueueFull))
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("closure 写入失败: %v", err)
	}
	if !fixture.tracker.IsDegraded(HealthScopeSessionQueueFull) {
		t.Fatal("claim 期间更新的 queue scope 被错误恢复")
	}
	if fixture.tracker.IsDegraded(HealthScopeSessionLogSink) {
		t.Fatal("未更新的 sink scope 未被选择性恢复")
	}

	fixture.sink.setProcessHook(nil)
	if err := fixture.writer.ProjectSession(terminalOutcomeSnapshot(func(mutable *requestOutcomeSnapshot) { mutable.RequestID = 77 })); err != nil {
		t.Fatalf("第二次 closure: %v", err)
	}
	if fixture.tracker.IsDegraded(HealthScopeSessionQueueFull) {
		t.Fatal("下一份摘要提交后仍未恢复 queue scope")
	}
	lines := fixture.terminalLines()
	if got := countTerminalText(lines, "详细记录文件写入已恢复"); got != 1 {
		t.Fatalf("sink recovered=%d，want 1: %v", got, lines)
	}
	if got := countTerminalText(lines, "详细记录已恢复保存"); got != 1 {
		t.Fatalf("queue recovered=%d，want 1: %v", got, lines)
	}
}

func TestOutcomeGapReplayMergePreservesGenerations(t *testing.T) {
	fixture := newSessionOutcomeFixture(t)
	fixture.degrade(OutcomeGapSourceSessionQueueFull, HealthScopeSessionQueueFull, 30, string(OutcomeGapReasonSessionQueueFull))
	fixture.degrade(OutcomeGapSourceSessionQueueFull, HealthScopeSessionQueueFull, 31, string(OutcomeGapReasonSessionQueueFull))
	fixture.degrade(OutcomeGapSourceSessionLogSink, HealthScopeSessionLogSink, 32, string(OutcomeGapReasonSessionLogSink))
	fixture.sink.setProcessErr(errors.New("process sink write failed"))

	snapshot := terminalOutcomeSnapshot(nil)
	if err := fixture.writer.ProjectSession(snapshot); err != nil {
		t.Fatalf("closure 成功但 gap 写失败不应让整条 closure 失败: %v", err)
	}
	if !fixture.tracker.IsDegraded(HealthScopeSessionQueueFull) || !fixture.tracker.IsDegraded(HealthScopeSessionLogSink) {
		t.Fatal("gap 写失败后 scope 被错误恢复")
	}
	if got := countTerminalText(fixture.terminalLines(), "已恢复"); got != 0 {
		t.Fatalf("gap 写失败仍产生 recovered 提示 %d 次", got)
	}
	queue := fixture.gaps.Snapshot(OutcomeGapSourceSessionQueueFull)
	sink := fixture.gaps.Snapshot(OutcomeGapSourceSessionLogSink)
	if queue.Count != 2 || queue.Generation != 2 {
		t.Fatalf("Merge 后 queue 摘要=%+v，want count=2 generation=2", queue)
	}
	if sink.Count != 2 || sink.Generation < 2 {
		t.Fatalf("Merge 后 sink 摘要=%+v，want count=2 且 generation 不回退", sink)
	}
	if queue.FirstRequestID != 30 || queue.LastRequestID != 31 {
		t.Fatalf("Merge 丢失 queue 首尾关联: %+v", queue)
	}

	fixture.sink.setProcessErr(nil)
	if err := fixture.writer.ProjectSession(terminalOutcomeSnapshot(func(mutable *requestOutcomeSnapshot) { mutable.RequestID = 78 })); err != nil {
		t.Fatalf("恢复后 closure: %v", err)
	}
	records := fixture.sink.process()
	if len(records) != 1 {
		t.Fatalf("重放摘要记录数=%d，want 1: %v", len(records), records)
	}
	for _, want := range []string{
		"event=outcome_gap",
		"session_queue_full.count=2", "session_queue_full.generation=2",
		"session_queue_full.first_request=30", "session_queue_full.last_request=31",
		"session_log_sink.count=2",
	} {
		if !strings.Contains(records[0], want) {
			t.Fatalf("重放摘要缺少 %q: %q", want, records[0])
		}
	}
	for _, forbidden := range []string{"process sink write failed", ".log", "session-secret"} {
		if strings.Contains(records[0], forbidden) {
			t.Fatalf("重放摘要泄漏 %q: %q", forbidden, records[0])
		}
	}
	if fixture.tracker.IsDegraded(HealthScopeSessionQueueFull) || fixture.tracker.IsDegraded(HealthScopeSessionLogSink) {
		t.Fatal("重放成功后 scope 未恢复")
	}
}

func TestOutcomeGapMixedReason(t *testing.T) {
	fixture := newSessionOutcomeFixture(t)
	fixture.degrade(OutcomeGapSourceSessionQueueFull, HealthScopeSessionQueueFull, 40, string(OutcomeGapReasonSessionQueueFull))
	fixture.degrade(OutcomeGapSourceSessionQueueFull, HealthScopeSessionQueueFull, 41, "closing")
	if err := fixture.writer.ProjectSession(terminalOutcomeSnapshot(nil)); err != nil {
		t.Fatalf("closure: %v", err)
	}
	records := fixture.sink.process()
	if len(records) != 1 {
		t.Fatalf("摘要记录数=%d，want 1", len(records))
	}
	if !strings.Contains(records[0], "session_queue_full.reason=mixed") {
		t.Fatalf("混合原因未收敛为 mixed: %q", records[0])
	}
	if !strings.Contains(records[0], "session_queue_full.count=2") {
		t.Fatalf("混合原因丢失累计数量: %q", records[0])
	}
}

func TestSessionHealthRejectsStaleCommittedGeneration(t *testing.T) {
	fixture := newSessionOutcomeFixture(t)
	stale := fixture.degrade(OutcomeGapSourceSessionQueueFull, HealthScopeSessionQueueFull, 50, string(OutcomeGapReasonSessionQueueFull))
	fixture.degrade(OutcomeGapSourceSessionQueueFull, HealthScopeSessionQueueFull, 51, string(OutcomeGapReasonSessionQueueFull))
	before := len(fixture.terminalLines())

	transition := fixture.tracker.ObserveGapCommit(HealthScopeSessionQueueFull, stale)
	if transition.Kind == HealthTransitionRecovered {
		t.Fatalf("stale committed generation 产生了恢复: %+v", transition)
	}
	if !fixture.tracker.IsDegraded(HealthScopeSessionQueueFull) {
		t.Fatal("stale proof 清除了退化状态")
	}
	if after := len(fixture.terminalLines()); after != before {
		t.Fatalf("stale proof 写了终端提示 %d -> %d", before, after)
	}
	if zero := fixture.tracker.ObserveGapCommit(HealthScopeSessionQueueFull, 0); zero.Kind == HealthTransitionRecovered {
		t.Fatalf("零 generation 产生了恢复: %+v", zero)
	}
}

func TestSessionQueueAndSinkScopesInterleave(t *testing.T) {
	fixture := newSessionOutcomeFixture(t)
	fixture.degrade(OutcomeGapSourceSessionLogSink, HealthScopeSessionLogSink, 60, string(OutcomeGapReasonSessionLogSink))
	if err := fixture.writer.ProjectSession(terminalOutcomeSnapshot(nil)); err != nil {
		t.Fatalf("closure: %v", err)
	}
	if fixture.tracker.IsDegraded(HealthScopeSessionLogSink) {
		t.Fatal("sink scope 未恢复")
	}

	fixture.degrade(OutcomeGapSourceSessionQueueFull, HealthScopeSessionQueueFull, 61, string(OutcomeGapReasonSessionQueueFull))
	fixture.degrade(OutcomeGapSourceSessionLogSink, HealthScopeSessionLogSink, 62, string(OutcomeGapReasonSessionLogSink))
	if err := fixture.writer.ProjectSession(terminalOutcomeSnapshot(func(mutable *requestOutcomeSnapshot) { mutable.RequestID = 79 })); err != nil {
		t.Fatalf("第二次 closure: %v", err)
	}
	for _, scope := range []HealthScope{HealthScopeSessionQueueFull, HealthScopeSessionLogSink} {
		if fixture.tracker.IsDegraded(scope) {
			t.Fatalf("scope=%s 未恢复", scope)
		}
	}
	records := fixture.sink.process()
	if len(records) != 2 {
		t.Fatalf("摘要记录数=%d，want 2: %v", len(records), records)
	}
	if strings.Contains(records[0], "session_queue_full.") {
		t.Fatalf("第一份摘要错误包含未发生的 queue 缺口: %q", records[0])
	}
	if !strings.Contains(records[1], "session_queue_full.") || !strings.Contains(records[1], "session_log_sink.") {
		t.Fatalf("第二份摘要缺少逐 scope 证据: %q", records[1])
	}
}

func TestSessionLogSinkEnteredRecoveredTerminalOnly(t *testing.T) {
	fixture := newSessionOutcomeFixture(t)
	combinedProbe := &countingHandler{}
	fileProbe := &countingHandler{}
	defaultProbe := &countingHandler{}
	previous := slog.Default()
	slog.SetDefault(slog.New(defaultProbe))
	t.Cleanup(func() { slog.SetDefault(previous) })
	_ = NewCombinedLogHandler(combinedProbe, fileProbe)

	fixture.sink.setProcessErr(errors.New("process sink write failed"))
	fixture.degrade(OutcomeGapSourceSessionQueueFull, HealthScopeSessionQueueFull, 70, string(OutcomeGapReasonSessionQueueFull))
	for index := 0; index < 3; index++ {
		if err := fixture.writer.ProjectSession(terminalOutcomeSnapshot(func(mutable *requestOutcomeSnapshot) {
			mutable.RequestID = uint64(80 + index)
		})); err != nil {
			t.Fatalf("closure %d: %v", index, err)
		}
	}
	lines := fixture.terminalLines()
	if got := countTerminalText(lines, "详细记录暂时写不进文件"); got != 1 {
		t.Fatalf("sink entered 提示=%d，want 1: %v", got, lines)
	}
	if got := countTerminalText(lines, "已恢复"); got != 0 {
		t.Fatalf("连续失败期间出现恢复提示 %d 次: %v", got, lines)
	}

	fixture.sink.setProcessErr(nil)
	if err := fixture.writer.ProjectSession(terminalOutcomeSnapshot(func(mutable *requestOutcomeSnapshot) { mutable.RequestID = 90 })); err != nil {
		t.Fatalf("恢复后 closure: %v", err)
	}
	lines = fixture.terminalLines()
	if got := countTerminalText(lines, "详细记录文件写入已恢复"); got != 1 {
		t.Fatalf("sink recovered 提示=%d，want 1: %v", got, lines)
	}
	if combinedProbe.calls != 0 || fileProbe.calls != 0 || defaultProbe.calls != 0 {
		t.Fatalf("健康提示流经 file-capable logger: combined=%d file=%d default=%d",
			combinedProbe.calls, fileProbe.calls, defaultProbe.calls)
	}
}

func TestSessionLogSinkHealthIsIndependentFromSQLite(t *testing.T) {
	fixture := newSessionOutcomeFixture(t)
	fixture.tracker.ObserveFailure(HealthScopeSQLiteState, HealthFailureClassSQLiteState)
	fixture.tracker.ObserveFailure(HealthScopeSQLiteArchive, HealthFailureClassSQLiteArchive)
	fixture.degrade(OutcomeGapSourceSessionLogSink, HealthScopeSessionLogSink, 100, string(OutcomeGapReasonSessionLogSink))
	if err := fixture.writer.ProjectSession(terminalOutcomeSnapshot(nil)); err != nil {
		t.Fatalf("closure: %v", err)
	}
	if fixture.tracker.IsDegraded(HealthScopeSessionLogSink) {
		t.Fatal("sink scope 未恢复")
	}
	for _, scope := range []HealthScope{HealthScopeSQLiteState, HealthScopeSQLiteArchive} {
		if !fixture.tracker.IsDegraded(scope) {
			t.Fatalf("session 恢复清除了 SQLite scope=%s", scope)
		}
	}
	if got := countTerminalText(fixture.terminalLines(), "本地状态已恢复正常保存"); got != 0 {
		t.Fatalf("session 恢复伪造了 SQLite 恢复提示 %d 次", got)
	}
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

func TestHealthTrackerFlexibleObservationArguments(t *testing.T) {
	reporter := &healthTestReporter{}
	tracker := NewHealthTracker(reporter)
	if got := tracker.ObserveFailure(HealthScopeSessionQueueFull, uint64(7), "queue_full"); got.Kind != HealthTransitionEntered || got.Generation != 7 {
		t.Fatalf("reordered failure args=%+v", got)
	}
	if got := tracker.ObserveGapCommit(HealthScopeSessionQueueFull, 7); got.Kind != HealthTransitionRecovered {
		t.Fatalf("matching proof=%+v", got)
	}
	if got := tracker.ObserveFailure(HealthScopeSQLiteState, HealthFailureClassUnknown); got.Kind != HealthTransitionUnchanged {
		t.Fatalf("explicit unknown class accepted: %+v", got)
	}
}

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

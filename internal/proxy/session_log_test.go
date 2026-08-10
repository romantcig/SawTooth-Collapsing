package proxy

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
)

func TestSessionLogRoutesProcessAndSession(t *testing.T) {
	dataDir := t.TempDir()
	logger := slog.New(NewSessionLogHandler(dataDir, slog.LevelDebug, nil))
	secret := "session-secret-must-not-be-rendered"

	logger.Info("启动代理", "component", "proxy")
	logger.With("request_session_id", secret, "request_id", uint64(27)).Info("请求进入", "status", 200)
	logger.With("request_session_id", secret, "request_id", uint64(27)).Warn("上游返回非 2xx", "status", 502)

	process := readSessionLogFile(t, filepath.Join(dataDir, "logs", "process.log"))
	sessionPath := filepath.Join(dataDir, "logs", stableSessionHash(secret)+".log")
	session := readSessionLogFile(t, sessionPath)

	if !strings.Contains(process, "启动代理 component=proxy") {
		t.Fatalf("process.log 缺少无 session 事件: %q", process)
	}
	if strings.Contains(process, "请求进入") || strings.Contains(process, secret) {
		t.Fatalf("process.log 错误接收 session 事件或身份: %q", process)
	}
	if strings.Contains(session, secret) || strings.Contains(session, "request_session_id") {
		t.Fatalf("session.log 泄漏完整 session 身份: %q", session)
	}
	if !strings.Contains(session, "#27 请求进入 status=200") {
		t.Fatalf("session.log 缺少紧凑 request 关联: %q", session)
	}
	if !strings.Contains(session, "[WARN] #27 上游返回非 2xx status=502") {
		t.Fatalf("session.log Warn 标签格式错误: %q", session)
	}
	if strings.Contains(session, "[INFO]") || strings.Contains(session, "\033[") {
		t.Fatalf("session.log 含多余 Info 标签或 ANSI: %q", session)
	}
	linePattern := regexp.MustCompile(`(?m)^\d{2}:\d{2}:\d{2} (?:\[WARN\] )?.+$`)
	if !linePattern.MatchString(session) {
		t.Fatalf("session.log 时间/级别格式不是 HH:MM:SS: %q", session)
	}
}

func TestSessionLogHashIsStableShortLowerHex(t *testing.T) {
	first := stableSessionHash("same-session")
	if first != stableSessionHash("same-session") {
		t.Fatal("相同 session hash 不稳定")
	}
	if first == stableSessionHash("other-session") {
		t.Fatal("不同 session hash 未区分")
	}
	if len(first) != 16 {
		t.Fatalf("session hash 长度=%d，want 16: %q", len(first), first)
	}
	if strings.Trim(first, "0123456789abcdef") != "" {
		t.Fatalf("session hash 含非小写 hex: %q", first)
	}
}

func TestSessionLogAppendAndConcurrentLines(t *testing.T) {
	dataDir := tempDirRetryCleanup(t)
	secret := "concurrent-session"
	first := slog.New(NewSessionLogHandler(dataDir, slog.LevelInfo, nil))
	second := slog.New(NewSessionLogHandler(dataDir, slog.LevelInfo, nil))
	first.Info("重启前")
	second.Info("重启后")

	const workers = 8
	const perWorker = 20
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		worker := worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			logger := slog.New(NewSessionLogHandler(dataDir, slog.LevelInfo, nil)).With(
				"request_session_id", secret,
				"request_id", uint64(worker+1),
			)
			for index := 0; index < perWorker; index++ {
				logger.Info(fmt.Sprintf("并发-%d-%d", worker, index))
			}
		}()
	}
	wg.Wait()

	process := readSessionLogFile(t, filepath.Join(dataDir, "logs", "process.log"))
	if !strings.Contains(process, "重启前") || !strings.Contains(process, "重启后") {
		t.Fatalf("追加写入被截断: %q", process)
	}
	session := readSessionLogFile(t, filepath.Join(dataDir, "logs", stableSessionHash(secret)+".log"))
	lines := strings.Split(strings.TrimSpace(session), "\n")
	if len(lines) != workers*perWorker {
		t.Fatalf("并发 session 日志行数=%d，want %d: %q", len(lines), workers*perWorker, session)
	}
	linePattern := regexp.MustCompile(`^\d{2}:\d{2}:\d{2} #\d+ 并发-\d+-\d+$`)
	for _, line := range lines {
		if !linePattern.MatchString(line) {
			t.Fatalf("发现交错或半行: %q", line)
		}
	}
}

func TestCombinedLogHandlerPreservesTerminalContract(t *testing.T) {
	var terminal strings.Builder
	terminalHandler := NewLogHandler(&terminal, slog.LevelInfo)
	dataDir := t.TempDir()
	fileHandler := NewSessionLogHandler(dataDir, slog.LevelInfo, nil)
	logger := slog.New(NewCombinedLogHandler(terminalHandler, fileHandler))
	logger.InfoContext(context.Background(), "组合事件")

	if !strings.Contains(terminal.String(), "[INFO]  组合事件") {
		t.Fatalf("组合 handler 破坏终端格式: %q", terminal.String())
	}
	filePath := filepath.Join(dataDir, "logs", "process.log")
	file := readSessionLogFile(t, filePath)
	if strings.Contains(file, "[INFO]") || strings.Contains(file, "\033[") {
		t.Fatalf("文件 handler 不应复制终端级别/ANSI: %q", file)
	}
	if !strings.Contains(file, "组合事件") {
		t.Fatalf("组合 handler 未写文件日志: %q", file)
	}
}

func readSessionLogFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("读取日志 %s: %v", path, err)
	}
	return string(data)
}

// fakeOutcomeSink 用可注入的整行 seam 替代真实文件，避免依赖平台权限行为。
type fakeOutcomeSink struct {
	mu           sync.Mutex
	sessionLines map[string][]string
	processLines []string
	sessionErr   error
	processErr   error
	processHook  func()
	sessionHook  func()
}

func (s *fakeOutcomeSink) WriteSessionLine(shortHash, line string) error {
	s.mu.Lock()
	hook := s.processHook
	if shortHash != "" {
		hook = s.sessionHook
	}
	sessionErr := s.sessionErr
	processErr := s.processErr
	s.mu.Unlock()
	if hook != nil {
		hook()
	}
	if shortHash == "" {
		if processErr != nil {
			return processErr
		}
		s.mu.Lock()
		s.processLines = append(s.processLines, line)
		s.mu.Unlock()
		return nil
	}
	if sessionErr != nil {
		return sessionErr
	}
	s.mu.Lock()
	if s.sessionLines == nil {
		s.sessionLines = map[string][]string{}
	}
	s.sessionLines[shortHash] = append(s.sessionLines[shortHash], line)
	s.mu.Unlock()
	return nil
}

func (s *fakeOutcomeSink) process() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.processLines...)
}

func (s *fakeOutcomeSink) session(shortHash string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.sessionLines[shortHash]...)
}

func (s *fakeOutcomeSink) setProcessErr(err error) {
	s.mu.Lock()
	s.processErr = err
	s.mu.Unlock()
}

func (s *fakeOutcomeSink) setSessionErr(err error) {
	s.mu.Lock()
	s.sessionErr = err
	s.mu.Unlock()
}

func (s *fakeOutcomeSink) setProcessHook(hook func()) {
	s.mu.Lock()
	s.processHook = hook
	s.mu.Unlock()
}

func (s *fakeOutcomeSink) setSessionHook(hook func()) {
	s.mu.Lock()
	s.sessionHook = hook
	s.mu.Unlock()
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
	dataDir := t.TempDir()
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

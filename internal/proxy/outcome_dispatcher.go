package proxy

import (
	"errors"
	"sync"
)

const (
	// 两条 lane 的容量是固定合同；不得在请求路径按负载扩容。
	OutcomeDispatcherQueueCapacity         = 256
	OutcomeDispatcherTerminalQueueCapacity = OutcomeDispatcherQueueCapacity
	OutcomeDispatcherSessionQueueCapacity  = OutcomeDispatcherQueueCapacity
	OutcomeDispatcherTerminalWorkers       = 1
	OutcomeDispatcherSessionWorkers        = 1
	OutcomeDispatcherWorkerCount           = 2
)

// ErrOutcomeDispatcherClosing 是严格 shutdown 生命周期错误，不属于普通 gap。
var ErrOutcomeDispatcherClosing = errors.New("outcome dispatcher closing")

var ErrOutcomeDispatcherNotConfigured = errors.New("outcome dispatcher dependencies not configured")

// OutcomeDispatchResult 是 request_outcome.go 中接口返回类型的导出别名。
type OutcomeDispatchResult = outcomeDispatchResult

// TerminalOutcomeProjector 与 SessionOutcomeProjector 是可替换的测试/生产
// 投影 seam；dispatcher 不依赖具体 log/slog handler。
type TerminalOutcomeProjector interface {
	ProjectTerminal(requestOutcomeSnapshot, outcomeDispatchResult) error
}

type SessionOutcomeProjector interface {
	ProjectSession(requestOutcomeSnapshot) error
}

type OutcomeTerminalProjector = TerminalOutcomeProjector
type OutcomeSessionProjector = SessionOutcomeProjector

type terminalProjectorFunc func(requestOutcomeSnapshot, outcomeDispatchResult) error

func (f terminalProjectorFunc) ProjectTerminal(snapshot requestOutcomeSnapshot, admission outcomeDispatchResult) error {
	return f(snapshot, admission)
}

type sessionProjectorFunc func(requestOutcomeSnapshot) error

func (f sessionProjectorFunc) ProjectSession(snapshot requestOutcomeSnapshot) error {
	return f(snapshot)
}

// OutcomeDispatcherOptions 允许生产 wiring 使用单个显式 options 值；默认容量
// 字段故意不开放，避免破坏 256 bounded 合同。GapAccumulator 可注入，使
// dispatcher 与 session outcome writer 共享同一份缺口证据。
type OutcomeDispatcherOptions struct {
	TerminalProjector any
	SessionProjector  any
	HealthTracker     *HealthTracker
	Reporter          any
	GapAccumulator    *OutcomeGapAccumulator
}

type terminalAdmissionJob struct {
	snapshot  requestOutcomeSnapshot
	admission outcomeDispatchResult
}

type sessionOutcomeJob struct {
	snapshot requestOutcomeSnapshot
}

type outcomeReporterAdapter struct {
	fn func(HealthTransition)
}

func (a outcomeReporterAdapter) ReportHealthTransition(transition HealthTransition) {
	if a.fn != nil {
		a.fn(transition)
	}
}

// OutcomeDispatcher 拥有两条独立 channel 与两个长期 worker。TryDispatch
// 只在受保护的 bounded channel 上做 nonblocking send，不启动每请求 goroutine。
type OutcomeDispatcher struct {
	mu sync.Mutex

	terminalQueue chan terminalAdmissionJob
	sessionQueue  chan sessionOutcomeJob
	terminalEmit  func(requestOutcomeSnapshot, outcomeDispatchResult) error
	sessionEmit   func(requestOutcomeSnapshot) error

	tracker  *HealthTracker
	reporter HealthTransitionReporter
	gaps     *OutcomeGapAccumulator

	workers    sync.WaitGroup
	closing    bool
	configured bool

	terminalOverflow       uint64
	sessionQueueFull       uint64
	sessionProjectorError  uint64
	terminalProjectorError uint64
	lifecycleRejections    uint64
	lastLifecycleError     error
}

func adaptHealthReporter(value any) HealthTransitionReporter {
	if value == nil {
		return nil
	}
	switch typed := value.(type) {
	case HealthTransitionReporter:
		return typed
	case interface{ Report(HealthTransition) }:
		return outcomeReporterAdapter{fn: typed.Report}
	case interface{ ReportTransition(HealthTransition) }:
		return outcomeReporterAdapter{fn: typed.ReportTransition}
	case func(HealthTransition):
		return outcomeReporterAdapter{fn: typed}
	default:
		return nil
	}
}

func adaptTerminalProjector(value any) func(requestOutcomeSnapshot, outcomeDispatchResult) error {
	if value == nil {
		return nil
	}
	switch typed := value.(type) {
	case TerminalOutcomeProjector:
		return typed.ProjectTerminal
	case interface {
		Project(requestOutcomeSnapshot, outcomeDispatchResult) error
	}:
		return typed.Project
	case interface {
		Handle(requestOutcomeSnapshot, outcomeDispatchResult) error
	}:
		return typed.Handle
	case interface {
		Emit(requestOutcomeSnapshot, outcomeDispatchResult) error
	}:
		return typed.Emit
	case func(requestOutcomeSnapshot, outcomeDispatchResult) error:
		return typed
	case func(requestOutcomeSnapshot, bool) error:
		return func(snapshot requestOutcomeSnapshot, admission outcomeDispatchResult) error {
			return typed(snapshot, admission.SessionRejected)
		}
	case func(requestOutcomeSnapshot, outcomeDispatchResult):
		return func(snapshot requestOutcomeSnapshot, admission outcomeDispatchResult) error {
			typed(snapshot, admission)
			return nil
		}
	case func(requestOutcomeSnapshot, bool):
		return func(snapshot requestOutcomeSnapshot, admission outcomeDispatchResult) error {
			typed(snapshot, admission.SessionRejected)
			return nil
		}
	default:
		return nil
	}
}

func adaptSessionProjector(value any) func(requestOutcomeSnapshot) error {
	if value == nil {
		return nil
	}
	switch typed := value.(type) {
	case SessionOutcomeProjector:
		return typed.ProjectSession
	case interface{ ProjectSession(requestOutcomeSnapshot) }:
		return func(snapshot requestOutcomeSnapshot) error {
			typed.ProjectSession(snapshot)
			return nil
		}
	case interface {
		Project(requestOutcomeSnapshot) error
	}:
		return typed.Project
	case interface {
		Handle(requestOutcomeSnapshot) error
	}:
		return typed.Handle
	case interface {
		Emit(requestOutcomeSnapshot) error
	}:
		return typed.Emit
	case func(requestOutcomeSnapshot) error:
		return typed
	case func(requestOutcomeSnapshot):
		return func(snapshot requestOutcomeSnapshot) error {
			typed(snapshot)
			return nil
		}
	default:
		return nil
	}
}

func parseOutcomeDispatcherOptions(args ...any) (terminal, session any, tracker *HealthTracker, reporter HealthTransitionReporter, gaps *OutcomeGapAccumulator) {
	for _, arg := range args {
		switch typed := arg.(type) {
		case OutcomeDispatcherOptions:
			if terminal == nil {
				terminal = typed.TerminalProjector
			}
			if session == nil {
				session = typed.SessionProjector
			}
			if tracker == nil {
				tracker = typed.HealthTracker
			}
			if reporter == nil {
				reporter = adaptHealthReporter(typed.Reporter)
			}
			if gaps == nil {
				gaps = typed.GapAccumulator
			}
		case *HealthTracker:
			if tracker == nil {
				tracker = typed
			}
		case *OutcomeGapAccumulator:
			if gaps == nil {
				gaps = typed
			}
		case HealthTransitionReporter:
			if reporter == nil {
				reporter = typed
			}
		case func(HealthTransition):
			if reporter == nil {
				reporter = adaptHealthReporter(typed)
			}
		default:
			if terminal == nil {
				if candidate := adaptTerminalProjector(arg); candidate != nil {
					terminal = arg
					continue
				}
			}
			if session == nil {
				if candidate := adaptSessionProjector(arg); candidate != nil {
					session = arg
				}
			}
		}
	}
	return terminal, session, tracker, reporter, gaps
}

// NewOutcomeDispatcher 需要调用方显式传入 HealthTracker 与 reporter。变参
// 形式兼容不同阶段的 projector seam，但缺少任一依赖时不会启动可用 worker。
func NewOutcomeDispatcher(args ...any) *OutcomeDispatcher {
	terminal, session, tracker, reporter, gaps := parseOutcomeDispatcherOptions(args...)
	if gaps == nil {
		gaps = NewOutcomeGapAccumulator()
	}
	dispatcher := &OutcomeDispatcher{
		terminalQueue: make(chan terminalAdmissionJob, OutcomeDispatcherTerminalQueueCapacity),
		sessionQueue:  make(chan sessionOutcomeJob, OutcomeDispatcherSessionQueueCapacity),
		terminalEmit:  adaptTerminalProjector(terminal),
		sessionEmit:   adaptSessionProjector(session),
		tracker:       tracker,
		reporter:      reporter,
		gaps:          gaps,
	}
	if tracker == nil || reporter == nil {
		return dispatcher
	}
	// 确保 dispatcher 与外部传入的 reporter 共享同一 tracker 状态。
	tracker.SetReporter(reporter)
	dispatcher.configured = true
	dispatcher.workers.Add(2)
	go dispatcher.runTerminal()
	go dispatcher.runSession()
	return dispatcher
}

func NewOutcomeDispatcherWithProjectors(terminal, session any, tracker *HealthTracker, reporter HealthTransitionReporter) *OutcomeDispatcher {
	return NewOutcomeDispatcher(terminal, session, tracker, reporter)
}

func NewOutcomeDispatcherChecked(args ...any) (*OutcomeDispatcher, error) {
	dispatcher := NewOutcomeDispatcher(args...)
	if dispatcher == nil || !dispatcher.Configured() {
		return dispatcher, ErrOutcomeDispatcherNotConfigured
	}
	return dispatcher, nil
}

func (d *OutcomeDispatcher) Configured() bool {
	return d != nil && d.configured
}

func (d *OutcomeDispatcher) HealthTracker() *HealthTracker {
	if d == nil {
		return nil
	}
	return d.tracker
}

func (d *OutcomeDispatcher) Reporter() HealthTransitionReporter {
	if d == nil {
		return nil
	}
	return d.reporter
}

func (d *OutcomeDispatcher) GapAccumulator() *OutcomeGapAccumulator {
	if d == nil {
		return nil
	}
	return d.gaps
}

func (d *OutcomeDispatcher) TryDispatch(snapshot requestOutcomeSnapshot) outcomeDispatchResult {
	if d == nil {
		return outcomeDispatchResult{LifecycleError: true}
	}
	snapshot = snapshot.normalized()
	result := outcomeDispatchResult{}
	d.mu.Lock()
	if !d.configured || d.closing {
		d.lifecycleRejections++
		d.lastLifecycleError = ErrOutcomeDispatcherClosing
		d.mu.Unlock()
		return outcomeDispatchResult{LifecycleError: true}
	}
	select {
	case d.sessionQueue <- sessionOutcomeJob{snapshot: snapshot}:
		result.SessionAccepted = true
	default:
		result.SessionRejected = true
		d.sessionQueueFull++
	}
	if snapshot.TerminalEligibility == outcomeEligibilityTerminalEligible {
		admission := result
		select {
		case d.terminalQueue <- terminalAdmissionJob{snapshot: snapshot, admission: admission}:
			result.TerminalAccepted = true
		default:
			result.TerminalEmissionUnavailable = true
			d.terminalOverflow++
		}
	}
	result.Accepted = result.SessionAccepted || result.TerminalAccepted
	d.mu.Unlock()

	// Admission 已先固定，再记录 queue gap；terminal job 永远携带同一 rejected
	// 结果，不会因为 session full 被取消。
	if result.SessionRejected && d.tracker != nil {
		generation := d.gaps.Accumulate(OutcomeGapEvent{
			Source:      OutcomeGapSourceSessionQueueFull,
			SessionHash: snapshot.SessionHash,
			RequestID:   snapshot.RequestID,
			From:        snapshot.StartedAt,
			To:          snapshot.FinishedAt,
			Reason:      string(OutcomeGapReasonSessionQueueFull),
		})
		d.tracker.ObserveFailure(HealthScopeSessionQueueFull, HealthFailureClassSessionQueueFull, generation)
	}
	return result
}

// Dispatch 是需要 error 语义的适配器；Plan 01 使用的 TryDispatch 仍保持
// nonblocking result-only 合同。
func (d *OutcomeDispatcher) Dispatch(snapshot requestOutcomeSnapshot) (outcomeDispatchResult, error) {
	result := d.TryDispatch(snapshot)
	if result.LifecycleError {
		return result, ErrOutcomeDispatcherClosing
	}
	return result, nil
}

func (d *OutcomeDispatcher) runTerminal() {
	defer d.workers.Done()
	for job := range d.terminalQueue {
		if d.terminalEmit == nil {
			continue
		}
		if err := callTerminalProjector(d.terminalEmit, job.snapshot, job.admission); err != nil {
			d.mu.Lock()
			d.terminalProjectorError++
			d.mu.Unlock()
		}
	}
}

func (d *OutcomeDispatcher) runSession() {
	defer d.workers.Done()
	for job := range d.sessionQueue {
		if d.sessionEmit == nil {
			continue
		}
		if err := callSessionProjector(d.sessionEmit, job.snapshot); err != nil {
			d.mu.Lock()
			d.sessionProjectorError++
			d.mu.Unlock()
			generation := d.gaps.Accumulate(OutcomeGapEvent{
				Source:      OutcomeGapSourceSessionLogSink,
				SessionHash: job.snapshot.SessionHash,
				RequestID:   job.snapshot.RequestID,
				From:        job.snapshot.StartedAt,
				To:          job.snapshot.FinishedAt,
				Reason:      string(OutcomeGapReasonSessionLogSink),
			})
			if d.tracker != nil {
				d.tracker.ObserveFailure(HealthScopeSessionLogSink, HealthFailureClassSessionLogSink, generation)
			}
		}
	}
}

func callTerminalProjector(projector func(requestOutcomeSnapshot, outcomeDispatchResult) error, snapshot requestOutcomeSnapshot, admission outcomeDispatchResult) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = errors.New("terminal projector failed")
		}
	}()
	return projector(snapshot, admission)
}

func callSessionProjector(projector func(requestOutcomeSnapshot) error, snapshot requestOutcomeSnapshot) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = errors.New("session projector failed")
		}
	}()
	return projector(snapshot)
}

// CloseAndDrain 停止接收后关闭两条 channel，worker 会先处理所有已接受 job。
func (d *OutcomeDispatcher) CloseAndDrain() error {
	if d == nil {
		return nil
	}
	d.mu.Lock()
	if !d.configured {
		d.closing = true
		d.mu.Unlock()
		return nil
	}
	if !d.closing {
		d.closing = true
		close(d.sessionQueue)
		close(d.terminalQueue)
	}
	d.mu.Unlock()
	d.workers.Wait()
	return nil
}

func (d *OutcomeDispatcher) TerminalOverflowCount() uint64 {
	if d == nil {
		return 0
	}
	d.mu.Lock()
	value := d.terminalOverflow
	d.mu.Unlock()
	return value
}

func (d *OutcomeDispatcher) SessionQueueFullCount() uint64 {
	if d == nil {
		return 0
	}
	d.mu.Lock()
	value := d.sessionQueueFull
	d.mu.Unlock()
	return value
}

func (d *OutcomeDispatcher) LifecycleRejectionCount() uint64 {
	if d == nil {
		return 0
	}
	d.mu.Lock()
	value := d.lifecycleRejections
	d.mu.Unlock()
	return value
}

func (d *OutcomeDispatcher) SessionProjectorErrorCount() uint64 {
	if d == nil {
		return 0
	}
	d.mu.Lock()
	value := d.sessionProjectorError
	d.mu.Unlock()
	return value
}

func (d *OutcomeDispatcher) TerminalProjectorErrorCount() uint64 {
	if d == nil {
		return 0
	}
	d.mu.Lock()
	value := d.terminalProjectorError
	d.mu.Unlock()
	return value
}

func (d *OutcomeDispatcher) QueueLengths() (terminal, session int) {
	if d == nil {
		return 0, 0
	}
	d.mu.Lock()
	terminal, session = len(d.terminalQueue), len(d.sessionQueue)
	d.mu.Unlock()
	return terminal, session
}

func (d *OutcomeDispatcher) LastLifecycleError() error {
	if d == nil {
		return ErrOutcomeDispatcherClosing
	}
	d.mu.Lock()
	err := d.lastLifecycleError
	d.mu.Unlock()
	return err
}

func (d *OutcomeDispatcher) WorkerCount() int {
	if d == nil || !d.configured {
		return 0
	}
	return OutcomeDispatcherWorkerCount
}

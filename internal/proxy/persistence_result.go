package proxy

import (
	"errors"
	"sync"
)

// 固定资源边界：writer 不按负载扩容 worker、队列或 goroutine。
const (
	PersistenceWriterWorkers       = 4
	PersistenceWriterQueueCapacity = 256
)

// PersistenceTarget 是 writer 接受的持久化目标。只有普通 state 属于 best-effort
// 队列；correctness-critical 的 Archive 与 history transition 必须同步提交，
// 因此刻意不在这个枚举里。
type PersistenceTarget string

const (
	PersistenceTargetState       PersistenceTarget = "state"
	PersistenceTargetUnsupported PersistenceTarget = "unsupported"
)

// PersistenceOpKind 是 state 的两种写操作。
type PersistenceOpKind string

const (
	PersistenceOpPut    PersistenceOpKind = "put"
	PersistenceOpDelete PersistenceOpKind = "delete"
)

// PersistenceBackend 是 worker 唯一允许调用的 SQLite 入口。接口里没有
// SaveArchive 或 transaction 方法，writer 因此在类型层面就无法写它们。
type PersistenceBackend interface {
	PersistState(key, value string) error
	DeleteState(key string) error
}

// PersistenceOp 是一次 typed 提交。OrderingKey 决定串行化边界，为空时回退到 Key。
type PersistenceOp struct {
	Target      PersistenceTarget
	Kind        PersistenceOpKind
	OrderingKey string
	Key         string
	Value       string
	Completion  *outcomeCompletion
}

// PersistenceResult 只保存受限事实：target、终态与稳定 failure class。
// 它不保存 state key、数据库路径或 driver error 原文。
type PersistenceResult struct {
	Target       PersistenceTarget
	State        persistenceState
	FailureClass persistenceFailureClass
}

// PersistenceReceipt 是 TrySubmit 的同步回执。Accepted=false 时 Result 已是
// 终态，对应 completion 也已在返回前完成。
type PersistenceReceipt struct {
	Accepted bool
	Result   PersistenceResult
}

// ArchiveCommitResult 是同步 Archive commit 的 typed 结果。它不经过
// PersistenceWriter 队列，只与普通 state receipt 共用 result→health 映射。
type ArchiveCommitResult struct {
	ID           string
	State        persistenceState
	FailureClass persistenceFailureClass
}

var (
	// ErrPersistenceWriterNotConfigured 表示缺少显式 backend/tracker/reporter。
	ErrPersistenceWriterNotConfigured = errors.New("persistence writer dependencies not configured")
	// ErrPersistenceWriterClosing 是 shutdown 生命周期错误，不是磁盘失败。
	ErrPersistenceWriterClosing = errors.New("persistence writer closing")
	// ErrPersistenceWriterUnsupportedTarget 拒绝 Archive 等非普通 state 目标。
	ErrPersistenceWriterUnsupportedTarget = errors.New("persistence writer only accepts plain state operations")
)

// StateLoadFailureClass 是 loader 对外暴露的稳定失败类别 allowlist。
// 它只描述失败种类，绝不包含 state key、数据库路径或 driver 原文。
type StateLoadFailureClass string

const (
	StateLoadFailureNone         StateLoadFailureClass = "none"
	StateLoadFailureSQLiteClosed StateLoadFailureClass = "sqlite_closed"
	StateLoadFailureSQLiteBusy   StateLoadFailureClass = "sqlite_busy"
	StateLoadFailureQueryFailed  StateLoadFailureClass = "query_failed"
)

// StateLoader 是本阶段唯一的 state 读取合同。bool-only 的旧 LoadState 只保留
// 给尚未迁移的调用点，不属于这个接口。
type StateLoader interface {
	LoadStateResult(key string) StateLoadResult
}

// StateLoadResult 精确表达三种互斥事实：命中、确认不存在、读取失败。
// Found=false 且 Err=nil 只能来自 sql.ErrNoRows；任何 closed/busy/query error
// 都必须保留非 nil Err 与稳定 FailureClass。
//
// FailureClass 是对外的安全字段；Err 保留 %w 链只为让调用方用 errors.Is/As
// 分类，构造时已确保消息里没有 key 与数据库路径。
type StateLoadResult struct {
	Value        string
	Found        bool
	Err          error
	FailureClass StateLoadFailureClass
}

// 三个 sentinel 让调用方无需匹配 driver 文案即可分类失败。
var (
	ErrStateLoadClosed      = errors.New("sqlite state loader: database closed")
	ErrStateLoadBusy        = errors.New("sqlite state loader: database busy")
	ErrStateLoadQueryFailed = errors.New("sqlite state loader: query failed")
)

// StateLoadFailure 是状态组件向调用方与 request outcome 暴露的稳定读取失败事实。
// Failed=false 表示本次读取给出了终态（命中或确认不存在）；Failed=true 时
// 组件既没有应用旧状态，也没有把失败标记成冷启动 missing。
type StateLoadFailure struct {
	Failed bool
	Class  StateLoadFailureClass
}

// stateLoadFailureFrom 只在 Err 非 nil 时构造失败事实。
func stateLoadFailureFrom(result StateLoadResult) StateLoadFailure {
	if result.Err == nil {
		return StateLoadFailure{}
	}
	class := result.FailureClass
	if class == "" || class == StateLoadFailureNone {
		class = StateLoadFailureQueryFailed
	}
	return StateLoadFailure{Failed: true, Class: class}
}

// StateSubmitter 是状态组件在自身 mutex 之外提交普通 state 写入的最小合同。
// *PersistenceWriter 满足它；组件因此永远不直接调用 SQLite，也不会为提交
// 启动 goroutine。
type StateSubmitter interface {
	TrySubmit(op PersistenceOp) (PersistenceReceipt, error)
}

// legacyStateLoaderAdapter 把 bool-only LoadFunc 包成 StateLoader，只供尚未
// 迁移的测试使用。它永远不产生 Err：旧回调无法区分 missing 与查询失败，
// 因此只能表达 missing，生产 wiring 必须注入真正的 StateLoader。
type legacyStateLoaderAdapter struct{ fn LoadFunc }

func (a legacyStateLoaderAdapter) LoadStateResult(key string) StateLoadResult {
	value, ok := a.fn(key)
	return StateLoadResult{Value: value, Found: ok}
}

func stateLoaderFromLoadFunc(fn LoadFunc) StateLoader {
	if fn == nil {
		return nil
	}
	return legacyStateLoaderAdapter{fn: fn}
}

// submitStateOp 是四个状态组件共用的锁外提交路径。
// 只有真正的 StateSubmitter 才可能产生 saved：legacy void callback 无法证明
// SQLite 写入成功，最多得到 unavailable。
func submitStateOp(submitter StateSubmitter, legacy func(), op PersistenceOp) PersistenceReceipt {
	op.Target = PersistenceTargetState
	if submitter != nil {
		receipt, _ := submitter.TrySubmit(op)
		return receipt
	}
	if legacy != nil {
		legacy()
		return completeStateOp(op.Completion, persistenceStateUnavailable, persistenceFailureLifecycle)
	}
	return completeStateOp(op.Completion, persistenceStateNotAttempted, persistenceFailureNone)
}

func completeStateOp(completion *outcomeCompletion, state persistenceState, class persistenceFailureClass) PersistenceReceipt {
	result := PersistenceResult{Target: PersistenceTargetState, State: state, FailureClass: class}
	completePersistenceOutcome(completion, result)
	return PersistenceReceipt{Result: result}
}

type persistenceJob struct {
	kind        PersistenceOpKind
	orderingKey string
	key         string
	value       string
	completion  *outcomeCompletion
}

// PersistenceWriter 用固定 worker 池 + 全局有界 pending 队列 + active-key
// 调度器实现同 key FIFO；它不给 key 或 op 分配 goroutine，也不做 retry/spool。
type PersistenceWriter struct {
	mu   sync.Mutex
	cond *sync.Cond

	backend  PersistenceBackend
	tracker  *HealthTracker
	reporter HealthTransitionReporter

	pending []persistenceJob
	active  map[string]bool

	configured bool
	closing    bool

	maxQueueLength      int
	queueFull           uint64
	lifecycleRejections uint64

	workers sync.WaitGroup
}

// NewPersistenceWriter 要求调用方显式传入 backend 与 Plan 02 的
// HealthTracker/HealthTransitionReporter。缺少任一依赖时返回不可用 writer：
// 它不启动 worker，也不会用内部 no-op 冒充生产可用。
func NewPersistenceWriter(backend PersistenceBackend, tracker *HealthTracker, reporter HealthTransitionReporter) *PersistenceWriter {
	writer := &PersistenceWriter{
		backend:  backend,
		tracker:  tracker,
		reporter: reporter,
		active:   make(map[string]bool),
	}
	writer.cond = sync.NewCond(&writer.mu)
	if backend == nil || tracker == nil || reporter == nil {
		return writer
	}
	// 与 OutcomeDispatcher 一致：writer 复用调用方的 tracker 状态，不新建第二套。
	tracker.SetReporter(reporter)
	writer.configured = true
	writer.workers.Add(PersistenceWriterWorkers)
	for index := 0; index < PersistenceWriterWorkers; index++ {
		go writer.run()
	}
	return writer
}

// NewPersistenceWriterChecked 让生产 wiring 能在启动期直接拿到依赖缺失错误。
func NewPersistenceWriterChecked(backend PersistenceBackend, tracker *HealthTracker, reporter HealthTransitionReporter) (*PersistenceWriter, error) {
	writer := NewPersistenceWriter(backend, tracker, reporter)
	if !writer.Configured() {
		return writer, ErrPersistenceWriterNotConfigured
	}
	return writer, nil
}

func (w *PersistenceWriter) Configured() bool {
	return w != nil && w.configured
}

func (w *PersistenceWriter) HealthTracker() *HealthTracker {
	if w == nil {
		return nil
	}
	return w.tracker
}

func (w *PersistenceWriter) Reporter() HealthTransitionReporter {
	if w == nil {
		return nil
	}
	return w.reporter
}

func (w *PersistenceWriter) WorkerCount() int {
	if !w.Configured() {
		return 0
	}
	return PersistenceWriterWorkers
}

// TrySubmit 永不阻塞模型请求：队列满时立即完成 unavailable receipt，
// closing 时返回 lifecycle 错误，两者都不改变 SQLite health。
func (w *PersistenceWriter) TrySubmit(op PersistenceOp) (PersistenceReceipt, error) {
	if w == nil {
		return PersistenceReceipt{Result: PersistenceResult{
			Target: PersistenceTargetUnsupported, State: persistenceStateUnavailable,
			FailureClass: persistenceFailureLifecycle,
		}}, ErrPersistenceWriterNotConfigured
	}
	if op.Target != PersistenceTargetState || (op.Kind != PersistenceOpPut && op.Kind != PersistenceOpDelete) {
		// 非法 target/op 是调用方错误：既不入队，也不冒充 queue full。
		return w.completeImmediately(PersistenceTargetUnsupported, op.Completion,
			persistenceStateUnavailable, persistenceFailureInvalidInput), ErrPersistenceWriterUnsupportedTarget
	}
	orderingKey := op.OrderingKey
	if orderingKey == "" {
		orderingKey = op.Key
	}

	w.mu.Lock()
	switch {
	case !w.configured:
		w.lifecycleRejections++
		w.mu.Unlock()
		return w.completeImmediately(PersistenceTargetState, op.Completion,
			persistenceStateUnavailable, persistenceFailureLifecycle), ErrPersistenceWriterNotConfigured
	case w.closing:
		w.lifecycleRejections++
		w.mu.Unlock()
		return w.completeImmediately(PersistenceTargetState, op.Completion,
			persistenceStateUnavailable, persistenceFailureLifecycle), ErrPersistenceWriterClosing
	case len(w.pending) >= PersistenceWriterQueueCapacity:
		w.queueFull++
		w.mu.Unlock()
		return w.completeImmediately(PersistenceTargetState, op.Completion,
			persistenceStateUnavailable, persistenceFailureQueueFull), nil
	}
	w.pending = append(w.pending, persistenceJob{
		kind:        op.Kind,
		orderingKey: orderingKey,
		key:         op.Key,
		value:       op.Value,
		completion:  op.Completion,
	})
	if len(w.pending) > w.maxQueueLength {
		w.maxQueueLength = len(w.pending)
	}
	w.cond.Signal()
	w.mu.Unlock()
	return PersistenceReceipt{Accepted: true}, nil
}

func (w *PersistenceWriter) completeImmediately(target PersistenceTarget, completion *outcomeCompletion, state persistenceState, class persistenceFailureClass) PersistenceReceipt {
	result := PersistenceResult{Target: target, State: state, FailureClass: class}
	completePersistenceOutcome(completion, result)
	return PersistenceReceipt{Result: result}
}

// completePersistenceOutcome 把受限结果交给 Plan 01 的 one-shot completion。
// 重复完成是幂等成功，writer 不会覆盖调用方已经写下的首个结果。
func completePersistenceOutcome(completion *outcomeCompletion, result PersistenceResult) {
	if completion == nil {
		return
	}
	_ = completion.CompleteResult(outcomeCompletionResult{
		State:        result.State,
		FailureClass: result.FailureClass,
		DiskState:    result.State,
	})
}

func (w *PersistenceWriter) run() {
	defer w.workers.Done()
	w.mu.Lock()
	defer w.mu.Unlock()
	for {
		index, ok := w.nextRunnableLocked()
		if !ok {
			if w.closing && len(w.pending) == 0 {
				return
			}
			// 仍有 pending 但其 key 正在执行：等待活跃 op 结束后再调度。
			w.cond.Wait()
			continue
		}
		job := w.pending[index]
		w.pending = append(w.pending[:index], w.pending[index+1:]...)
		w.active[job.orderingKey] = true
		backend := w.backend
		w.mu.Unlock()

		result := executePersistenceJob(backend, job)
		// 先固定逐请求 receipt，再传播健康状态：health 去重不得改变 closure。
		completePersistenceOutcome(job.completion, result)
		w.ObserveStateResult(result)

		w.mu.Lock()
		delete(w.active, job.orderingKey)
		w.cond.Broadcast()
	}
}

// nextRunnableLocked 返回第一个 ordering key 空闲的 pending op。
// 同 key 的相对顺序始终保持 accepted 顺序，因此严格 FIFO；
// 被占用的 key 只是被跳过，不阻塞其余 key 使用剩余 worker。
func (w *PersistenceWriter) nextRunnableLocked() (int, bool) {
	for index, job := range w.pending {
		if !w.active[job.orderingKey] {
			return index, true
		}
	}
	return 0, false
}

func executePersistenceJob(backend PersistenceBackend, job persistenceJob) PersistenceResult {
	result := PersistenceResult{Target: PersistenceTargetState}
	var err error
	if job.kind == PersistenceOpDelete {
		err = backend.DeleteState(job.key)
	} else {
		err = backend.PersistState(job.key, job.value)
	}
	if err != nil {
		result.State = persistenceStateFailed
		result.FailureClass = persistenceFailureSQLite
		return result
	}
	// SQLite 返回 nil error 是普通 state saved 的唯一证据。
	result.State = persistenceStateSaved
	result.FailureClass = persistenceFailureNone
	return result
}

// ObserveStateResult 与 ObserveArchiveCommit 共用同一 result→health 映射：
// 只有真实 backend 的 saved/failed 会进入 tracker，
// queue full/closing/invalid input 一律不触碰任何 SQLite scope。
func (w *PersistenceWriter) ObserveStateResult(result PersistenceResult) HealthTransition {
	return w.observeSQLiteResult(HealthScopeSQLiteState, HealthFailureClassSQLiteState, result.State)
}

func (w *PersistenceWriter) ObserveArchiveCommit(result ArchiveCommitResult) HealthTransition {
	return w.observeSQLiteResult(HealthScopeSQLiteArchive, HealthFailureClassSQLiteArchive, result.State)
}

func (w *PersistenceWriter) observeSQLiteResult(scope HealthScope, class HealthFailureClass, state persistenceState) HealthTransition {
	if w == nil || w.tracker == nil {
		return makeHealthTransition(scope, class, HealthTransitionKindUnchanged, 0, 0)
	}
	switch state {
	case persistenceStateFailed:
		return w.tracker.ObserveFailure(scope, class)
	case persistenceStateSaved:
		return w.tracker.ObserveSuccess(scope, class)
	default:
		return makeHealthTransition(scope, class, HealthTransitionKindUnchanged, 0, 0)
	}
}

// CloseAndDrain 先停止接收，再让 worker 执行完所有已接受 op。
// 生产 shutdown 必须在 Store.Close 之前调用它。
func (w *PersistenceWriter) CloseAndDrain() error {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	if !w.closing {
		w.closing = true
		w.cond.Broadcast()
	}
	w.mu.Unlock()
	w.workers.Wait()
	return nil
}

func (w *PersistenceWriter) QueueLength() int {
	if w == nil {
		return 0
	}
	w.mu.Lock()
	length := len(w.pending)
	w.mu.Unlock()
	return length
}

func (w *PersistenceWriter) MaxQueueLength() int {
	if w == nil {
		return 0
	}
	w.mu.Lock()
	length := w.maxQueueLength
	w.mu.Unlock()
	return length
}

func (w *PersistenceWriter) QueueFullCount() uint64 {
	if w == nil {
		return 0
	}
	w.mu.Lock()
	value := w.queueFull
	w.mu.Unlock()
	return value
}

func (w *PersistenceWriter) LifecycleRejectionCount() uint64 {
	if w == nil {
		return 0
	}
	w.mu.Lock()
	value := w.lifecycleRejections
	w.mu.Unlock()
	return value
}

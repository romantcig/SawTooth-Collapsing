package proxy

import (
	"errors"
	"log/slog"
	"sync"
	"time"
)

// TriggerUnknown 是 outcome 层对未知触发原因的显式归一值。
// TriggerNone 仍表示已评估且明确未触发，不能与未知混用。
const TriggerUnknown TriggerReason = "unknown"

// pressureSourceUnknown 只在 outcome 层表示无法取得压力来源；它不改变既有
// pressureDecision 的零值语义。
const pressureSourceUnknown pressureSource = "unknown"

// outcomeEligibility 描述 request 是否完成了对应层次的评估。
// AI closure 和 terminal projection 使用同一受限类型，但由两个字段分别表示。
type outcomeEligibility string

const (
	outcomeEligibilityUnknown            outcomeEligibility = "unknown"
	outcomeEligibilityNotEvaluable       outcomeEligibility = "not_evaluable"
	outcomeEligibilityNotApplicable      outcomeEligibility = "not_applicable"
	outcomeEligibilityEvaluable          outcomeEligibility = "evaluable"
	outcomeEligibilityTerminalEligible   outcomeEligibility = "terminal_eligible"
	outcomeEligibilityTerminalIneligible outcomeEligibility = "terminal_ineligible"
)

// outcomeAction 是实际处理路径，而不是由 renderer 根据消息反推的文本。
type outcomeAction string

const (
	outcomeActionUnknown     outcomeAction = "unknown"
	outcomeActionPassthrough outcomeAction = "passthrough"
	outcomeActionDirect      outcomeAction = "direct"
	outcomeActionLight       outcomeAction = "light"
	outcomeActionCollapse    outcomeAction = "collapse"
	outcomeActionFallback    outcomeAction = "fallback"
	outcomeActionCompact     outcomeAction = "compact"
	outcomeActionFailClosed  outcomeAction = "fail_closed"
	outcomeActionUnavailable outcomeAction = "unavailable"
)

// upstreamState 是受限的上游生命周期结果。
type upstreamState string

const (
	upstreamStateUnknown          upstreamState = "unknown"
	upstreamStateNotStarted       upstreamState = "not_started"
	upstreamStateSuccess          upstreamState = "success"
	upstreamStateNon2xx           upstreamState = "non_2xx"
	upstreamStateTransportFailure upstreamState = "transport_failure"
	upstreamStateHeaderTimeout    upstreamState = "header_timeout"
	upstreamStateHardTimeout      upstreamState = "hard_timeout"
	upstreamStateIdleTimeout      upstreamState = "idle_timeout"
	upstreamStateBodyReadFailure  upstreamState = "body_read_failure"
	upstreamStateCommittedFailure upstreamState = "committed_failure"
	upstreamStateUnavailable      upstreamState = "unavailable"
)

// persistenceState 明确区分内存与磁盘的保存结果。
type persistenceState string

const (
	persistenceStateUnknown      persistenceState = "unknown"
	persistenceStateNotAttempted persistenceState = "not_attempted"
	persistenceStateSaved        persistenceState = "saved"
	persistenceStateFailed       persistenceState = "failed"
	persistenceStateUnavailable  persistenceState = "unavailable"
)

// persistenceFailureClass 只保存稳定类别，不保存 error.Error() 的原文。
type persistenceFailureClass string

const (
	persistenceFailureUnknown      persistenceFailureClass = "unknown"
	persistenceFailureNone         persistenceFailureClass = "none"
	persistenceFailureSQLite       persistenceFailureClass = "sqlite"
	persistenceFailureArchive      persistenceFailureClass = "sqlite_archive"
	persistenceFailureSessionSink  persistenceFailureClass = "session_sink"
	persistenceFailureQueueFull    persistenceFailureClass = "queue_full"
	persistenceFailureLifecycle    persistenceFailureClass = "lifecycle"
	persistenceFailureInvalidInput persistenceFailureClass = "invalid_input"
	persistenceFailureUpstream     persistenceFailureClass = "upstream"
)

// interventionState 表示是否需要人工介入。
type interventionState string

const (
	interventionUnknown     interventionState = "unknown"
	interventionNone        interventionState = "none"
	interventionRequired    interventionState = "required"
	interventionNotRequired interventionState = "not_required"
)

// outcomeCompletionKind 标记异步结果将影响哪个受限字段。
// 它不保存 session、正文、路径或 error。
type outcomeCompletionKind string

const (
	outcomeCompletionKindUnknown outcomeCompletionKind = "unknown"
	outcomeCompletionKindMemory  outcomeCompletionKind = "memory"
	outcomeCompletionKindDisk    outcomeCompletionKind = "disk"
	outcomeCompletionKindSQLite  outcomeCompletionKind = "sqlite"
)

// outcomeCompletionResult 是 completion 的最小、可脱敏结果。
type outcomeCompletionResult struct {
	State        persistenceState
	FailureClass persistenceFailureClass
	MemoryState  persistenceState
	DiskState    persistenceState
}

// requestOutcomeSnapshot 是 authoritative request outcome 的 immutable value。
// 所有字段均为数字、时间或受限 enum；不携带完整 session、正文、credential、
// 路径/key、完整 fingerprint 或 raw error。
type requestOutcomeSnapshot struct {
	RequestID           uint64                  `json:"request_id"`
	SessionHash         string                  `json:"session_hash"`
	StartedAt           time.Time               `json:"started_at"`
	FinishedAt          time.Time               `json:"finished_at"`
	Eligibility         outcomeEligibility      `json:"eligibility"`
	TerminalEligibility outcomeEligibility      `json:"terminal_eligibility"`
	TriggerReason       TriggerReason           `json:"trigger_reason"`
	PressureSource      pressureSource          `json:"pressure_source"`
	RequiredWait        time.Duration           `json:"required_wait,omitempty"`
	ActualWait          time.Duration           `json:"actual_wait,omitempty"`
	ActualWaitKnown     bool                    `json:"actual_wait_known"`
	Action              outcomeAction           `json:"action"`
	BeforeMessages      int                     `json:"before_messages,omitempty"`
	AfterMessages       int                     `json:"after_messages,omitempty"`
	BeforeTokens        int                     `json:"before_tokens,omitempty"`
	AfterTokens         int                     `json:"after_tokens,omitempty"`
	RecallAttempted     int                     `json:"recall_attempted,omitempty"`
	RecallCandidates    int                     `json:"recall_candidates,omitempty"`
	RecallSelected      int                     `json:"recall_selected,omitempty"`
	RecallInjected      int                     `json:"recall_injected,omitempty"`
	RecallDiscarded     int                     `json:"recall_discarded,omitempty"`
	UpstreamState       upstreamState           `json:"upstream_state"`
	UpstreamStatus      int                     `json:"upstream_status,omitempty"`
	MemoryState         persistenceState        `json:"memory_state"`
	DiskState           persistenceState        `json:"disk_state"`
	FailureClass        persistenceFailureClass `json:"failure_class"`
	Intervention        interventionState       `json:"intervention"`
}

// outcomeDispatcher 是后续 terminal/session projection 的唯一注入边界。
// TryDispatch 必须由实现保证非阻塞；collector 不等待其队列或 sink。
type outcomeDispatcher interface {
	TryDispatch(requestOutcomeSnapshot) outcomeDispatchResult
}

// outcomeDispatchResult 只描述 admission/projection 的受限结果，不携带 raw error。
type outcomeDispatchResult struct {
	Accepted                    bool
	TerminalAccepted            bool
	SessionAccepted             bool
	SessionRejected             bool
	TerminalEmissionUnavailable bool
	LifecycleError              bool
}

// ErrOutcomeProducersSealed 表示 collector 已封口，不能再登记 producer。
var ErrOutcomeProducersSealed = errors.New("request outcome producers sealed")

// 兼容同包调用点与测试中更短的 sentinel 名称。
var errOutcomeProducersSealed = ErrOutcomeProducersSealed

// ErrOutcomeCompletionAlreadyFinalized 仅用于 nil/生命周期防御；重复 Complete 是幂等成功。
var ErrOutcomeCompletionAlreadyFinalized = errors.New("request outcome already finalized")

type requestOutcomeCollector struct {
	mu         sync.Mutex
	finishOnce sync.Once
	snapshot   requestOutcomeSnapshot
	dispatcher outcomeDispatcher
	pending    int
	sealed     bool
	finalized  bool
}

// newRequestOutcomeCollector 创建 request-scoped collector。
// args 允许可选 time.Time 与 outcomeDispatcher，保持构造入口简单且便于后续注入。
func newRequestOutcomeCollector(requestID uint64, sessionIDOrHash string, args ...any) *requestOutcomeCollector {
	started := time.Now().UTC()
	var dispatcher outcomeDispatcher
	for _, arg := range args {
		switch value := arg.(type) {
		case time.Time:
			started = value.UTC()
		case outcomeDispatcher:
			dispatcher = value
		}
	}

	return &requestOutcomeCollector{
		snapshot: requestOutcomeSnapshot{
			RequestID:           requestID,
			SessionHash:         normalizeOutcomeSessionHash(sessionIDOrHash),
			StartedAt:           started,
			Eligibility:         outcomeEligibilityNotEvaluable,
			TerminalEligibility: outcomeEligibilityNotApplicable,
			TriggerReason:       TriggerNone,
			PressureSource:      pressureSourceUnknown,
			Action:              outcomeActionPassthrough,
			UpstreamState:       upstreamStateNotStarted,
			MemoryState:         persistenceStateNotAttempted,
			DiskState:           persistenceStateNotAttempted,
			FailureClass:        persistenceFailureUnknown,
			Intervention:        interventionNone,
		},
		dispatcher: dispatcher,
	}
}

func normalizeOutcomeSessionHash(value string) string {
	if isShortLowerHex(value) {
		return value
	}
	return stableSessionHash(value)
}

func isShortLowerHex(value string) bool {
	if len(value) != 16 {
		return false
	}
	for _, char := range value {
		if (char < '0' || char > '9') && (char < 'a' || char > 'f') {
			return false
		}
	}
	return true
}

// SetDispatcher 注入后续计划提供的 nonblocking dispatcher。
func (o *requestOutcomeCollector) SetDispatcher(dispatcher outcomeDispatcher) {
	if o == nil {
		return
	}
	o.mu.Lock()
	if !o.finalized && !o.sealed {
		o.dispatcher = dispatcher
	}
	o.mu.Unlock()
}

func (o *requestOutcomeCollector) setIfMutable(fn func(*requestOutcomeSnapshot)) {
	if o == nil || fn == nil {
		return
	}
	o.mu.Lock()
	if !o.finalized && !o.sealed {
		fn(&o.snapshot)
	}
	o.mu.Unlock()
}

func (o *requestOutcomeCollector) SetEligibility(value outcomeEligibility) {
	o.setIfMutable(func(snapshot *requestOutcomeSnapshot) { snapshot.Eligibility = normalizeOutcomeEligibility(value) })
}

func (o *requestOutcomeCollector) SetTerminalEligibility(value outcomeEligibility) {
	o.setIfMutable(func(snapshot *requestOutcomeSnapshot) {
		snapshot.TerminalEligibility = normalizeOutcomeEligibility(value)
	})
}

// SetTerminalEligible 只允许调用方明确声明 terminal projection 是否适用。
func (o *requestOutcomeCollector) SetTerminalEligible(eligible bool) {
	if eligible {
		o.SetTerminalEligibility(outcomeEligibilityTerminalEligible)
		return
	}
	o.SetTerminalEligibility(outcomeEligibilityTerminalIneligible)
}

func (o *requestOutcomeCollector) SetTriggerReason(value TriggerReason) {
	o.setIfMutable(func(snapshot *requestOutcomeSnapshot) { snapshot.TriggerReason = normalizeTriggerReason(value) })
}

func (o *requestOutcomeCollector) SetTrigger(value TriggerReason) {
	o.SetTriggerReason(value)
}

func (o *requestOutcomeCollector) SetPressureSource(value pressureSource) {
	o.setIfMutable(func(snapshot *requestOutcomeSnapshot) { snapshot.PressureSource = normalizePressureSource(value) })
}

func (o *requestOutcomeCollector) SetAction(value outcomeAction) {
	o.setIfMutable(func(snapshot *requestOutcomeSnapshot) { snapshot.Action = normalizeOutcomeAction(value) })
}

func (o *requestOutcomeCollector) SetUpstreamState(value upstreamState) {
	o.setIfMutable(func(snapshot *requestOutcomeSnapshot) { snapshot.UpstreamState = normalizeUpstreamState(value) })
}

func (o *requestOutcomeCollector) SetUpstreamStatus(status int) {
	o.setIfMutable(func(snapshot *requestOutcomeSnapshot) {
		if status < 0 {
			status = 0
		}
		snapshot.UpstreamStatus = status
	})
}

func (o *requestOutcomeCollector) SetMemoryState(value persistenceState) {
	o.setIfMutable(func(snapshot *requestOutcomeSnapshot) { snapshot.MemoryState = normalizePersistenceState(value) })
}

func (o *requestOutcomeCollector) SetDiskState(value persistenceState) {
	o.setIfMutable(func(snapshot *requestOutcomeSnapshot) { snapshot.DiskState = normalizePersistenceState(value) })
}

func (o *requestOutcomeCollector) SetFailureClass(value persistenceFailureClass) {
	o.setIfMutable(func(snapshot *requestOutcomeSnapshot) { snapshot.FailureClass = normalizeFailureClass(value) })
}

func (o *requestOutcomeCollector) SetIntervention(value interventionState) {
	o.setIfMutable(func(snapshot *requestOutcomeSnapshot) { snapshot.Intervention = normalizeIntervention(value) })
}

func (o *requestOutcomeCollector) SetWait(required, actual time.Duration, actualKnown bool) {
	o.setIfMutable(func(snapshot *requestOutcomeSnapshot) {
		if required < 0 {
			required = 0
		}
		if actual < 0 {
			actual = 0
		}
		snapshot.RequiredWait = required
		snapshot.ActualWait = actual
		snapshot.ActualWaitKnown = actualKnown
	})
}

func (o *requestOutcomeCollector) SetSizes(beforeMessages, afterMessages, beforeTokens, afterTokens int) {
	o.setIfMutable(func(snapshot *requestOutcomeSnapshot) {
		snapshot.BeforeMessages = nonNegativeOutcomeInt(beforeMessages)
		snapshot.AfterMessages = nonNegativeOutcomeInt(afterMessages)
		snapshot.BeforeTokens = nonNegativeOutcomeInt(beforeTokens)
		snapshot.AfterTokens = nonNegativeOutcomeInt(afterTokens)
	})
}

func (o *requestOutcomeCollector) SetRecallCounts(attempted, candidates, selected, injected, discarded int) {
	o.setIfMutable(func(snapshot *requestOutcomeSnapshot) {
		snapshot.RecallAttempted = nonNegativeOutcomeInt(attempted)
		snapshot.RecallCandidates = nonNegativeOutcomeInt(candidates)
		snapshot.RecallSelected = nonNegativeOutcomeInt(selected)
		snapshot.RecallInjected = nonNegativeOutcomeInt(injected)
		snapshot.RecallDiscarded = nonNegativeOutcomeInt(discarded)
	})
}

func (o *requestOutcomeCollector) Snapshot() requestOutcomeSnapshot {
	if o == nil {
		return requestOutcomeSnapshot{
			Eligibility:         outcomeEligibilityUnknown,
			TerminalEligibility: outcomeEligibilityUnknown,
			TriggerReason:       TriggerUnknown,
			PressureSource:      pressureSourceUnknown,
			Action:              outcomeActionUnknown,
			UpstreamState:       upstreamStateUnknown,
			MemoryState:         persistenceStateUnknown,
			DiskState:           persistenceStateUnknown,
			FailureClass:        persistenceFailureUnknown,
			Intervention:        interventionUnknown,
		}
	}
	o.mu.Lock()
	snapshot := o.snapshot
	o.mu.Unlock()
	return snapshot.normalized()
}

func (snapshot requestOutcomeSnapshot) normalized() requestOutcomeSnapshot {
	snapshot.Eligibility = normalizeOutcomeEligibility(snapshot.Eligibility)
	snapshot.TerminalEligibility = normalizeOutcomeEligibility(snapshot.TerminalEligibility)
	snapshot.TriggerReason = normalizeTriggerReason(snapshot.TriggerReason)
	snapshot.PressureSource = normalizePressureSource(snapshot.PressureSource)
	snapshot.Action = normalizeOutcomeAction(snapshot.Action)
	snapshot.UpstreamState = normalizeUpstreamState(snapshot.UpstreamState)
	snapshot.MemoryState = normalizePersistenceState(snapshot.MemoryState)
	snapshot.DiskState = normalizePersistenceState(snapshot.DiskState)
	snapshot.FailureClass = normalizeFailureClass(snapshot.FailureClass)
	snapshot.Intervention = normalizeIntervention(snapshot.Intervention)
	if snapshot.SessionHash != "" && !isShortLowerHex(snapshot.SessionHash) {
		snapshot.SessionHash = stableSessionHash(snapshot.SessionHash)
	}
	if snapshot.RequiredWait < 0 {
		snapshot.RequiredWait = 0
	}
	if snapshot.ActualWait < 0 {
		snapshot.ActualWait = 0
	}
	if snapshot.UpstreamStatus < 0 {
		snapshot.UpstreamStatus = 0
	}
	snapshot.BeforeMessages = nonNegativeOutcomeInt(snapshot.BeforeMessages)
	snapshot.AfterMessages = nonNegativeOutcomeInt(snapshot.AfterMessages)
	snapshot.BeforeTokens = nonNegativeOutcomeInt(snapshot.BeforeTokens)
	snapshot.AfterTokens = nonNegativeOutcomeInt(snapshot.AfterTokens)
	snapshot.RecallAttempted = nonNegativeOutcomeInt(snapshot.RecallAttempted)
	snapshot.RecallCandidates = nonNegativeOutcomeInt(snapshot.RecallCandidates)
	snapshot.RecallSelected = nonNegativeOutcomeInt(snapshot.RecallSelected)
	snapshot.RecallInjected = nonNegativeOutcomeInt(snapshot.RecallInjected)
	snapshot.RecallDiscarded = nonNegativeOutcomeInt(snapshot.RecallDiscarded)
	return snapshot
}

// SafeLogAttrs 返回 typed allowlist；调用者可将其投影到任意 sink，但 collector 不持有 sink。
func (snapshot requestOutcomeSnapshot) SafeLogAttrs() []slog.Attr {
	snapshot = snapshot.normalized()
	attrs := []slog.Attr{
		slog.String("event", "request_outcome"),
		slog.Uint64("request_id", snapshot.RequestID),
	}
	if snapshot.SessionHash != "" {
		attrs = append(attrs, slog.String("session_hash", snapshot.SessionHash))
	}
	attrs = append(attrs,
		slog.String("eligibility", string(snapshot.Eligibility)),
		slog.String("terminal_eligibility", string(snapshot.TerminalEligibility)),
		slog.String("trigger_reason", string(snapshot.TriggerReason)),
		slog.String("pressure_source", string(snapshot.PressureSource)),
		slog.String("action", string(snapshot.Action)),
		slog.String("upstream_state", string(snapshot.UpstreamState)),
		slog.String("memory_state", string(snapshot.MemoryState)),
		slog.String("disk_state", string(snapshot.DiskState)),
		slog.String("failure_class", string(snapshot.FailureClass)),
		slog.String("intervention", string(snapshot.Intervention)),
	)
	if !snapshot.StartedAt.IsZero() {
		attrs = append(attrs, slog.Time("started_at", snapshot.StartedAt))
	}
	if !snapshot.FinishedAt.IsZero() {
		attrs = append(attrs, slog.Time("finished_at", snapshot.FinishedAt))
	}
	if snapshot.RequiredWait > 0 || snapshot.ActualWaitKnown {
		attrs = append(attrs,
			slog.Duration("required_wait", snapshot.RequiredWait),
			slog.Duration("actual_wait", snapshot.ActualWait),
			slog.Bool("actual_wait_known", snapshot.ActualWaitKnown),
		)
	}
	if snapshot.BeforeMessages > 0 || snapshot.AfterMessages > 0 {
		attrs = append(attrs,
			slog.Int("before_messages", snapshot.BeforeMessages),
			slog.Int("after_messages", snapshot.AfterMessages),
		)
	}
	if snapshot.BeforeTokens > 0 || snapshot.AfterTokens > 0 {
		attrs = append(attrs,
			slog.Int("before_tokens", snapshot.BeforeTokens),
			slog.Int("after_tokens", snapshot.AfterTokens),
		)
	}
	if snapshot.RecallAttempted > 0 || snapshot.RecallCandidates > 0 || snapshot.RecallSelected > 0 || snapshot.RecallInjected > 0 || snapshot.RecallDiscarded > 0 {
		attrs = append(attrs,
			slog.Int("recall_attempted", snapshot.RecallAttempted),
			slog.Int("recall_candidates", snapshot.RecallCandidates),
			slog.Int("recall_selected", snapshot.RecallSelected),
			slog.Int("recall_injected", snapshot.RecallInjected),
			slog.Int("recall_discarded", snapshot.RecallDiscarded),
		)
	}
	if snapshot.UpstreamStatus > 0 {
		attrs = append(attrs, slog.Int("upstream_status", snapshot.UpstreamStatus))
	}
	return attrs
}

// safeLogAttrs 是同包后续 projection 使用的兼容小写入口。
func (snapshot requestOutcomeSnapshot) safeLogAttrs() []slog.Attr {
	return snapshot.SafeLogAttrs()
}

func (o *requestOutcomeCollector) BeginAsyncResult(kind ...any) (*outcomeCompletion, error) {
	if o == nil {
		return nil, ErrOutcomeProducersSealed
	}
	completionKind := outcomeCompletionKindUnknown
	for _, arg := range kind {
		switch value := arg.(type) {
		case outcomeCompletionKind:
			completionKind = normalizeCompletionKind(value)
		case string:
			completionKind = normalizeCompletionKind(outcomeCompletionKind(value))
		}
	}
	o.mu.Lock()
	if o.sealed || o.finalized {
		o.mu.Unlock()
		return nil, ErrOutcomeProducersSealed
	}
	o.pending++
	o.mu.Unlock()
	return &outcomeCompletion{collector: o, kind: completionKind}, nil
}

// SealProducers 标记同步 producer 已结束；若没有 pending receipt，则立即 finalize。
func (o *requestOutcomeCollector) SealProducers() error {
	if o == nil {
		return ErrOutcomeProducersSealed
	}
	o.mu.Lock()
	if !o.sealed {
		o.sealed = true
	}
	o.mu.Unlock()
	o.tryFinalize()
	return nil
}

func (o *requestOutcomeCollector) complete(kind outcomeCompletionKind, result outcomeCompletionResult) error {
	if o == nil {
		return ErrOutcomeCompletionAlreadyFinalized
	}
	o.mu.Lock()
	if o.finalized {
		o.mu.Unlock()
		return ErrOutcomeCompletionAlreadyFinalized
	}
	if o.pending > 0 {
		o.pending--
	}
	applyOutcomeCompletionLocked(&o.snapshot, kind, result)
	o.mu.Unlock()
	o.tryFinalize()
	return nil
}

func (o *requestOutcomeCollector) tryFinalize() {
	if o == nil {
		return
	}
	o.mu.Lock()
	if !o.sealed || o.pending != 0 || o.finalized {
		o.mu.Unlock()
		return
	}
	o.finalized = true
	o.snapshot.FinishedAt = time.Now().UTC()
	snapshot := o.snapshot.normalized()
	dispatcher := o.dispatcher
	o.mu.Unlock()

	o.finishOnce.Do(func() {
		if dispatcher != nil {
			// Dispatcher 合同明确要求非阻塞；此处不创建额外并发执行、队列或超时器。
			_ = dispatcher.TryDispatch(snapshot)
		}
	})
}

type outcomeCompletion struct {
	collector *requestOutcomeCollector
	kind      outcomeCompletionKind
	once      sync.Once
}

// Complete 只接受受限结果；额外参数采用 any 是为了兼容后续 persistence receipt
// 的 typed result，而不会把参数保存在 completion 中。
func (c *outcomeCompletion) Complete(values ...any) error {
	if c == nil || c.collector == nil {
		return ErrOutcomeCompletionAlreadyFinalized
	}
	var firstErr error
	c.once.Do(func() {
		result := parseOutcomeCompletionResult(values...)
		firstErr = c.collector.complete(c.kind, result)
	})
	return firstErr
}

// CompleteResult 是不依赖 any 的 typed convenience API，供 persistence writer 使用。
func (c *outcomeCompletion) CompleteResult(result outcomeCompletionResult) error {
	return c.Complete(result)
}

func parseOutcomeCompletionResult(values ...any) outcomeCompletionResult {
	result := outcomeCompletionResult{State: persistenceStateUnknown, FailureClass: persistenceFailureUnknown}
	for _, value := range values {
		switch typed := value.(type) {
		case outcomeCompletionResult:
			result = typed
		case persistenceState:
			result.State = normalizePersistenceState(typed)
		case string:
			result.State = normalizePersistenceState(persistenceState(typed))
		case persistenceFailureClass:
			result.FailureClass = normalizeFailureClass(typed)
		case error:
			if typed != nil {
				result.State = persistenceStateFailed
				result.FailureClass = persistenceFailureUnknown
			}
		case bool:
			if typed {
				result.State = persistenceStateSaved
			} else {
				result.State = persistenceStateFailed
			}
		}
	}
	result.State = normalizePersistenceState(result.State)
	result.FailureClass = normalizeFailureClass(result.FailureClass)
	result.MemoryState = normalizePersistenceState(result.MemoryState)
	result.DiskState = normalizePersistenceState(result.DiskState)
	return result
}

func applyOutcomeCompletionLocked(snapshot *requestOutcomeSnapshot, kind outcomeCompletionKind, result outcomeCompletionResult) {
	if snapshot == nil {
		return
	}
	result.State = normalizePersistenceState(result.State)
	result.FailureClass = normalizeFailureClass(result.FailureClass)
	result.MemoryState = normalizePersistenceState(result.MemoryState)
	result.DiskState = normalizePersistenceState(result.DiskState)
	if result.FailureClass != persistenceFailureUnknown && result.FailureClass != persistenceFailureNone {
		snapshot.FailureClass = result.FailureClass
	}
	switch normalizeCompletionKind(kind) {
	case outcomeCompletionKindMemory:
		if result.MemoryState != persistenceStateUnknown {
			snapshot.MemoryState = result.MemoryState
		} else if result.State != persistenceStateUnknown {
			snapshot.MemoryState = result.State
		}
	case outcomeCompletionKindDisk, outcomeCompletionKindSQLite:
		if result.DiskState != persistenceStateUnknown {
			snapshot.DiskState = result.DiskState
		} else if result.State != persistenceStateUnknown {
			snapshot.DiskState = result.State
		}
	default:
		if result.MemoryState != persistenceStateUnknown {
			snapshot.MemoryState = result.MemoryState
		}
		if result.DiskState != persistenceStateUnknown {
			snapshot.DiskState = result.DiskState
		}
	}
}

func normalizeOutcomeEligibility(value outcomeEligibility) outcomeEligibility {
	switch value {
	case outcomeEligibilityUnknown, outcomeEligibilityNotEvaluable, outcomeEligibilityNotApplicable,
		outcomeEligibilityEvaluable, outcomeEligibilityTerminalEligible, outcomeEligibilityTerminalIneligible:
		return value
	default:
		return outcomeEligibilityUnknown
	}
}

func normalizeTriggerReason(value TriggerReason) TriggerReason {
	switch value {
	case TriggerNone, TriggerTokens, TriggerPause, TriggerEmergency, TriggerUnknown:
		return value
	default:
		return TriggerUnknown
	}
}

func normalizePressureSource(value pressureSource) pressureSource {
	switch value {
	case pressureSourceLocalFull, pressureSourceActualPlusDelta, pressureSourceConservativePlusDelta,
		pressureSourceConservativeHighWater, pressureSourceLegacyHighWater, pressureSourceUnknown:
		return value
	default:
		return pressureSourceUnknown
	}
}

func normalizeOutcomeAction(value outcomeAction) outcomeAction {
	switch value {
	case outcomeActionUnknown, outcomeActionPassthrough, outcomeActionDirect, outcomeActionLight,
		outcomeActionCollapse, outcomeActionFallback, outcomeActionCompact, outcomeActionFailClosed,
		outcomeActionUnavailable:
		return value
	default:
		return outcomeActionUnknown
	}
}

func normalizeUpstreamState(value upstreamState) upstreamState {
	switch value {
	case upstreamStateUnknown, upstreamStateNotStarted, upstreamStateSuccess, upstreamStateNon2xx,
		upstreamStateTransportFailure, upstreamStateHeaderTimeout, upstreamStateHardTimeout,
		upstreamStateIdleTimeout, upstreamStateBodyReadFailure, upstreamStateCommittedFailure,
		upstreamStateUnavailable:
		return value
	default:
		return upstreamStateUnknown
	}
}

func normalizePersistenceState(value persistenceState) persistenceState {
	switch value {
	case persistenceStateUnknown, persistenceStateNotAttempted, persistenceStateSaved,
		persistenceStateFailed, persistenceStateUnavailable:
		return value
	default:
		return persistenceStateUnknown
	}
}

func normalizeFailureClass(value persistenceFailureClass) persistenceFailureClass {
	switch value {
	case persistenceFailureUnknown, persistenceFailureNone, persistenceFailureSQLite,
		persistenceFailureArchive, persistenceFailureSessionSink, persistenceFailureQueueFull,
		persistenceFailureLifecycle, persistenceFailureInvalidInput, persistenceFailureUpstream:
		return value
	default:
		return persistenceFailureUnknown
	}
}

func normalizeIntervention(value interventionState) interventionState {
	switch value {
	case interventionUnknown, interventionNone, interventionRequired, interventionNotRequired:
		return value
	default:
		return interventionUnknown
	}
}

func normalizeCompletionKind(value outcomeCompletionKind) outcomeCompletionKind {
	switch value {
	case outcomeCompletionKindMemory, outcomeCompletionKindDisk, outcomeCompletionKindSQLite:
		return value
	default:
		return outcomeCompletionKindUnknown
	}
}

func nonNegativeOutcomeInt(value int) int {
	if value < 0 {
		return 0
	}
	return value
}

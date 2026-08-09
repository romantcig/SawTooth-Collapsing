package proxy

import "sync"

// HealthScope 是可向终端暴露的健康边界。范围刻意固定为四项；新增范围必须
// 经过新的阶段合同，不能由任意字符串在运行时扩展。
type HealthScope string

const (
	HealthScopeSQLiteState      HealthScope = "sqlite_state"
	HealthScopeSQLiteArchive    HealthScope = "sqlite_archive"
	HealthScopeSessionQueueFull HealthScope = "session_queue_full"
	HealthScopeSessionLogSink   HealthScope = "session_log_sink"
)

// 兼容后续计划/同包测试使用的简短别名。
const (
	ScopeSQLiteState      = HealthScopeSQLiteState
	ScopeSQLiteArchive    = HealthScopeSQLiteArchive
	ScopeSessionQueueFull = HealthScopeSessionQueueFull
	ScopeSessionLogSink   = HealthScopeSessionLogSink
)

// HealthFailureClass 是与 scope 配对的稳定失败类别。canonical 值与 scope
// 相同，保留几个旧语义别名只用于输入归一，不会产生第五个状态。
type HealthFailureClass string

const (
	HealthFailureClassUnknown          HealthFailureClass = "unknown"
	HealthFailureClassSQLiteState      HealthFailureClass = "sqlite_state"
	HealthFailureClassSQLiteArchive    HealthFailureClass = "sqlite_archive"
	HealthFailureClassSessionQueueFull HealthFailureClass = "session_queue_full"
	HealthFailureClassSessionLogSink   HealthFailureClass = "session_log_sink"

	HealthFailureClassSQLite      HealthFailureClass = "sqlite"
	HealthFailureClassArchive     HealthFailureClass = "archive"
	HealthFailureClassQueueFull   HealthFailureClass = "queue_full"
	HealthFailureClassSessionSink HealthFailureClass = "session_sink"
)

// HealthTransitionKind 是状态变化的受限枚举。
type HealthTransitionKind string

const (
	HealthTransitionUnchanged HealthTransitionKind = "unchanged"
	HealthTransitionEntered   HealthTransitionKind = "entered"
	HealthTransitionOngoing   HealthTransitionKind = "ongoing"
	HealthTransitionRecovered HealthTransitionKind = "recovered"

	// 带 Kind 后缀的别名用于避免与旧调用方的语义枚举混淆。
	HealthTransitionKindUnchanged = HealthTransitionUnchanged
	HealthTransitionKindEntered   = HealthTransitionEntered
	HealthTransitionKindOngoing   = HealthTransitionOngoing
	HealthTransitionKindRecovered = HealthTransitionRecovered
)

// HealthTransition 是 tracker 输出的一次 typed 状态观察。除 Kind 外的别名
// 字段是只读兼容信息，方便不同阶段的调用方不用重新解析字符串。
type HealthTransition struct {
	Scope               HealthScope
	FailureClass        HealthFailureClass
	Kind                HealthTransitionKind
	Transition          HealthTransitionKind
	Type                HealthTransitionKind
	Event               HealthTransitionKind
	Generation          uint64
	CommittedGeneration uint64
}

// HealthTransitionReporter 是唯一的 typed 健康事件出口。实现不得通过
// CombinedLogHandler/SessionLogHandler 回写失败的 sink。
type HealthTransitionReporter interface {
	ReportHealthTransition(HealthTransition)
}

type healthState struct {
	degraded         bool
	class            HealthFailureClass
	latestGeneration uint64
}

// HealthStatus 是不触发 reporter 的只读状态快照。
type HealthStatus struct {
	Scope            HealthScope
	FailureClass     HealthFailureClass
	Degraded         bool
	LatestGeneration uint64
}

// HealthTracker 仅使用固定四项状态槽；它不维护按请求或按缺口的容器。
type HealthTracker struct {
	mu       sync.Mutex
	reporter HealthTransitionReporter
	states   [4]healthState
}

// NewHealthTracker 创建 tracker。reporter 可省略，便于底层单元测试；生产
// OutcomeDispatcher/PersistenceWriter 会显式校验并注入自己的 reporter。
func NewHealthTracker(reporters ...HealthTransitionReporter) *HealthTracker {
	tracker := &HealthTracker{}
	if len(reporters) > 0 {
		tracker.reporter = reporters[0]
	}
	return tracker
}

func (t *HealthTracker) SetReporter(reporter HealthTransitionReporter) {
	if t == nil {
		return
	}
	t.mu.Lock()
	t.reporter = reporter
	t.mu.Unlock()
}

// HealthScopes 返回固定 allowlist 的副本，调用方不能借此扩展 tracker。
func HealthScopes() [4]HealthScope {
	return [4]HealthScope{
		HealthScopeSQLiteState,
		HealthScopeSQLiteArchive,
		HealthScopeSessionQueueFull,
		HealthScopeSessionLogSink,
	}
}

func healthScopeIndex(scope HealthScope) (int, bool) {
	switch scope {
	case HealthScopeSQLiteState:
		return 0, true
	case HealthScopeSQLiteArchive:
		return 1, true
	case HealthScopeSessionQueueFull:
		return 2, true
	case HealthScopeSessionLogSink:
		return 3, true
	default:
		return 0, false
	}
}

func normalizeHealthScope(scope HealthScope) (HealthScope, bool) {
	if _, ok := healthScopeIndex(scope); !ok {
		return "", false
	}
	return scope, true
}

func normalizeHealthFailureClass(scope HealthScope, class HealthFailureClass) (HealthFailureClass, bool) {
	if _, ok := healthScopeIndex(scope); !ok {
		return "", false
	}
	if class == "" {
		return HealthFailureClass(scope), true
	}
	if class == HealthFailureClassUnknown {
		return "", false
	}
	canonical := HealthFailureClass(scope)
	if class == canonical {
		return canonical, true
	}
	switch scope {
	case HealthScopeSQLiteState:
		if class == HealthFailureClassSQLite {
			return canonical, true
		}
	case HealthScopeSQLiteArchive:
		if class == HealthFailureClassArchive || class == HealthFailureClassSQLite {
			return canonical, true
		}
	case HealthScopeSessionQueueFull:
		if class == HealthFailureClassQueueFull {
			return canonical, true
		}
	case HealthScopeSessionLogSink:
		if class == HealthFailureClassSessionSink {
			return canonical, true
		}
	}
	return "", false
}

func makeHealthTransition(scope HealthScope, class HealthFailureClass, kind HealthTransitionKind, generation, committed uint64) HealthTransition {
	return HealthTransition{
		Scope:               scope,
		FailureClass:        class,
		Kind:                kind,
		Transition:          kind,
		Type:                kind,
		Event:               kind,
		Generation:          generation,
		CommittedGeneration: committed,
	}
}

func shouldReportHealth(kind HealthTransitionKind) bool {
	return kind == HealthTransitionKindEntered || kind == HealthTransitionKindRecovered
}

func (t *HealthTracker) emit(transition HealthTransition, reporter HealthTransitionReporter) {
	if reporter != nil && shouldReportHealth(transition.Kind) {
		reporter.ReportHealthTransition(transition)
	}
}

// ObserveFailure 记录一次真实失败。session scope 的 generation 必须来自
// OutcomeGapAccumulator；SQLite scope 没有 generation 时由 tracker 单调分配。
func parseHealthObservationArgs(scope HealthScope, args ...any) (HealthFailureClass, uint64) {
	class := HealthFailureClass(scope)
	var generation uint64
	for _, arg := range args {
		switch typed := arg.(type) {
		case HealthFailureClass:
			class = typed
		case HealthScope:
			class = HealthFailureClass(typed)
		case OutcomeGapSource:
			class = HealthFailureClass(typed)
		case string:
			class = HealthFailureClass(typed)
		case uint64:
			generation = typed
		case uint:
			generation = uint64(typed)
		case uint32:
			generation = uint64(typed)
		case uint16:
			generation = uint64(typed)
		case uint8:
			generation = uint64(typed)
		case int64:
			if typed >= 0 {
				generation = uint64(typed)
			}
		case int32:
			if typed >= 0 {
				generation = uint64(typed)
			}
		case int16:
			if typed >= 0 {
				generation = uint64(typed)
			}
		case int8:
			if typed >= 0 {
				generation = uint64(typed)
			}
		case int:
			if typed >= 0 {
				generation = uint64(typed)
			}
		}
	}
	return class, generation
}

func (t *HealthTracker) ObserveFailure(scope HealthScope, args ...any) HealthTransition {
	class, generation := parseHealthObservationArgs(scope, args...)
	if t == nil {
		return makeHealthTransition(scope, class, HealthTransitionKindUnchanged, 0, 0)
	}
	canonicalScope, ok := normalizeHealthScope(scope)
	if !ok {
		return makeHealthTransition(scope, class, HealthTransitionKindUnchanged, 0, 0)
	}
	class, generation = parseHealthObservationArgs(canonicalScope, args...)
	canonicalClass, ok := normalizeHealthFailureClass(canonicalScope, class)
	if !ok {
		return makeHealthTransition(canonicalScope, class, HealthTransitionKindUnchanged, 0, 0)
	}
	t.mu.Lock()
	index, _ := healthScopeIndex(canonicalScope)
	state := &t.states[index]
	if generation == 0 {
		generation = state.latestGeneration + 1
	}
	if generation < state.latestGeneration {
		generation = state.latestGeneration
	}
	state.latestGeneration = generation
	state.class = canonicalClass
	kind := HealthTransitionKindOngoing
	if !state.degraded {
		state.degraded = true
		kind = HealthTransitionKindEntered
	}
	reporter := t.reporter
	transition := makeHealthTransition(canonicalScope, canonicalClass, kind, generation, 0)
	t.mu.Unlock()
	t.emit(transition, reporter)
	return transition
}

// ObserveSuccess 只对 SQLite scope 产生恢复；session scope 必须使用
// ObserveGapCommit 的 generation proof，普通 enqueue/write 成功不能清除退化。
func (t *HealthTracker) ObserveSuccess(scope HealthScope, args ...any) HealthTransition {
	if t == nil {
		return makeHealthTransition(scope, "", HealthTransitionKindUnchanged, 0, 0)
	}
	canonicalScope, ok := normalizeHealthScope(scope)
	if !ok {
		return makeHealthTransition(scope, "", HealthTransitionKindUnchanged, 0, 0)
	}
	class, _ := parseHealthObservationArgs(canonicalScope, args...)
	canonicalClass, ok := normalizeHealthFailureClass(canonicalScope, class)
	if !ok {
		return makeHealthTransition(canonicalScope, class, HealthTransitionKindUnchanged, 0, 0)
	}
	if canonicalScope == HealthScopeSessionQueueFull || canonicalScope == HealthScopeSessionLogSink {
		return t.observeSessionSuccessRejected(canonicalScope, canonicalClass)
	}
	t.mu.Lock()
	index, _ := healthScopeIndex(canonicalScope)
	state := &t.states[index]
	kind := HealthTransitionKindUnchanged
	if state.degraded {
		state.degraded = false
		kind = HealthTransitionKindRecovered
	}
	transition := makeHealthTransition(canonicalScope, canonicalClass, kind, state.latestGeneration, state.latestGeneration)
	reporter := t.reporter
	t.mu.Unlock()
	t.emit(transition, reporter)
	return transition
}

func (t *HealthTracker) observeSessionSuccessRejected(scope HealthScope, class HealthFailureClass) HealthTransition {
	t.mu.Lock()
	index, _ := healthScopeIndex(scope)
	state := t.states[index]
	kind := HealthTransitionKindUnchanged
	if state.degraded {
		kind = HealthTransitionKindOngoing
	}
	transition := makeHealthTransition(scope, class, kind, state.latestGeneration, 0)
	t.mu.Unlock()
	return transition
}

// ObserveGapCommit 只接受 session gap scope 且 generation 精确匹配最新失败。
func (t *HealthTracker) ObserveGapCommit(scope HealthScope, committedGeneration uint64) HealthTransition {
	if t == nil {
		return makeHealthTransition(scope, "", HealthTransitionKindUnchanged, 0, committedGeneration)
	}
	canonicalScope, ok := normalizeHealthScope(scope)
	if !ok || (canonicalScope != HealthScopeSessionQueueFull && canonicalScope != HealthScopeSessionLogSink) {
		return makeHealthTransition(scope, HealthFailureClass(scope), HealthTransitionKindUnchanged, 0, committedGeneration)
	}
	t.mu.Lock()
	index, _ := healthScopeIndex(canonicalScope)
	state := &t.states[index]
	class := state.class
	kind := HealthTransitionKindUnchanged
	if state.degraded {
		if committedGeneration == state.latestGeneration && committedGeneration != 0 {
			state.degraded = false
			kind = HealthTransitionKindRecovered
		} else {
			kind = HealthTransitionKindOngoing
		}
	}
	transition := makeHealthTransition(canonicalScope, class, kind, state.latestGeneration, committedGeneration)
	reporter := t.reporter
	t.mu.Unlock()
	t.emit(transition, reporter)
	return transition
}

func (t *HealthTracker) IsDegraded(scope HealthScope) bool {
	if t == nil {
		return false
	}
	index, ok := healthScopeIndex(scope)
	if !ok {
		return false
	}
	t.mu.Lock()
	degraded := t.states[index].degraded
	t.mu.Unlock()
	return degraded
}

func (t *HealthTracker) LatestFailureGeneration(scope HealthScope) uint64 {
	if t == nil {
		return 0
	}
	index, ok := healthScopeIndex(scope)
	if !ok {
		return 0
	}
	t.mu.Lock()
	generation := t.states[index].latestGeneration
	t.mu.Unlock()
	return generation
}

func (t *HealthTracker) Status(scope HealthScope) HealthStatus {
	status := HealthStatus{Scope: scope, FailureClass: HealthFailureClass(scope)}
	if t == nil {
		return status
	}
	index, ok := healthScopeIndex(scope)
	if !ok {
		status.FailureClass = HealthFailureClassUnknown
		return status
	}
	t.mu.Lock()
	state := t.states[index]
	t.mu.Unlock()
	status.FailureClass = state.class
	status.Degraded = state.degraded
	status.LatestGeneration = state.latestGeneration
	return status
}

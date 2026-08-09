package proxy

import (
	"math"
	"sync"
	"time"
)

// OutcomeGapSource 是两个允许累计受控缺口的固定来源。
type OutcomeGapSource string

const (
	OutcomeGapSourceSessionQueueFull OutcomeGapSource = "session_queue_full"
	OutcomeGapSourceSessionLogSink   OutcomeGapSource = "session_log_sink"
)

const (
	GapSourceSessionQueueFull = OutcomeGapSourceSessionQueueFull
	GapSourceSessionLogSink   = OutcomeGapSourceSessionLogSink
)

// OutcomeGapReason 是脱敏、稳定的缺口原因；不同原因在同一 slot 混合时只保留 mixed。
type OutcomeGapReason string

const (
	OutcomeGapReasonMixed            OutcomeGapReason = "mixed"
	OutcomeGapReasonSessionQueueFull OutcomeGapReason = "session_queue_full"
	OutcomeGapReasonSessionLogSink   OutcomeGapReason = "session_log_sink"
)

// OutcomeGapCorrelation 是首尾关联所需的最小身份。
type OutcomeGapCorrelation struct {
	SessionHash string
	RequestID   uint64
}

// OutcomeGapEvent 是 Accumulate 的推荐输入。Accumulate 仍接受变参，以便
// 后续 writer/dispatcher 可以传入 immutable snapshot 或 scope/reason 组合。
type OutcomeGapEvent struct {
	Source      OutcomeGapSource
	Scope       HealthScope
	SessionHash string
	RequestID   uint64
	From        time.Time
	To          time.Time
	At          time.Time
	Reason      string
	Count       uint64
	Snapshot    requestOutcomeSnapshot
}

// OutcomeGapSnapshot 是一个 source slot 的固定大小摘要。
type OutcomeGapSnapshot struct {
	Source           OutcomeGapSource
	Scope            HealthScope
	Generation       uint64
	Count            uint64
	First            OutcomeGapCorrelation
	Last             OutcomeGapCorrelation
	FirstSessionHash string
	FirstRequestID   uint64
	LastSessionHash  string
	LastRequestID    uint64
	From             time.Time
	To               time.Time
	Reason           string
	StableReason     string
	SourceMask       uint8
}

// OutcomeGapCommittedScope 将 committed generation 与 scope 绑定。
type OutcomeGapCommittedScope struct {
	Scope      HealthScope
	Generation uint64
}

// OutcomeGapClaim 固化一次 in-flight claim。Slots/Queue/Sink 均为固定槽，
// 不保存每条丢失记录。
type OutcomeGapClaim struct {
	ID       uint64
	Queue    OutcomeGapSnapshot
	Sink     OutcomeGapSnapshot
	HasQueue bool
	HasSink  bool
	Slots    [2]OutcomeGapSnapshot
}

// OutcomeGapCommitResult 只返回可恢复 scope；其他 scope 的 current 保持原样。
type OutcomeGapCommitResult struct {
	ClaimID              uint64
	RecoverableScopes    []HealthScope
	Committed            [2]OutcomeGapCommittedScope
	CommittedGenerations [2]uint64
}

func (r OutcomeGapCommitResult) CommittedGeneration(scope HealthScope) (uint64, bool) {
	for i := range r.Committed {
		if r.Committed[i].Scope == scope {
			return r.Committed[i].Generation, true
		}
	}
	return 0, false
}

func (r OutcomeGapCommitResult) IsRecoverable(scope HealthScope) bool {
	for _, candidate := range r.RecoverableScopes {
		if candidate == scope {
			return true
		}
	}
	return false
}

type outcomeGapSlot struct {
	generation uint64
	current    OutcomeGapSnapshot
	hasCurrent bool
}

// OutcomeGapAccumulator 只有两个 current slot、两个 monotonic counter 和一个
// in-flight claim。mutex 保护 claim/accumulate/merge 的原子边界。
type OutcomeGapAccumulator struct {
	mu       sync.Mutex
	slots    [2]outcomeGapSlot
	claimed  OutcomeGapClaim
	inFlight bool
	nextID   uint64
}

func NewOutcomeGapAccumulator() *OutcomeGapAccumulator {
	return &OutcomeGapAccumulator{}
}

func outcomeGapSourceIndex(source OutcomeGapSource) (int, HealthScope, bool) {
	switch source {
	case OutcomeGapSourceSessionQueueFull:
		return 0, HealthScopeSessionQueueFull, true
	case OutcomeGapSourceSessionLogSink:
		return 1, HealthScopeSessionLogSink, true
	default:
		return 0, "", false
	}
}

func normalizeOutcomeGapSource(source OutcomeGapSource) (OutcomeGapSource, bool) {
	if _, _, ok := outcomeGapSourceIndex(source); ok {
		return source, true
	}
	return "", false
}

func gapSourceFromAny(value any) (OutcomeGapSource, bool) {
	switch typed := value.(type) {
	case OutcomeGapSource:
		return normalizeOutcomeGapSource(typed)
	case HealthScope:
		return normalizeOutcomeGapSource(OutcomeGapSource(typed))
	case string:
		return normalizeOutcomeGapSource(OutcomeGapSource(typed))
	default:
		return "", false
	}
}

func normalizeGapReason(reason string, source OutcomeGapSource) string {
	if reason == "" {
		return string(source)
	}
	// 不接受任意高基数字符串进入摘要；稳定原因只保留短 token。
	for _, char := range reason {
		if (char >= 'a' && char <= 'z') || (char >= '0' && char <= '9') || char == '_' || char == '-' {
			continue
		}
		return string(OutcomeGapReasonMixed)
	}
	return reason
}

func parseOutcomeGapEvent(args ...any) (OutcomeGapEvent, bool) {
	event := OutcomeGapEvent{}
	var times [2]time.Time
	timeCount := 0
	for _, arg := range args {
		switch typed := arg.(type) {
		case OutcomeGapEvent:
			event = typed
		case *OutcomeGapEvent:
			if typed != nil {
				event = *typed
			}
		case requestOutcomeSnapshot:
			event.Snapshot = typed
			if event.SessionHash == "" {
				event.SessionHash = typed.SessionHash
			}
			if event.RequestID == 0 {
				event.RequestID = typed.RequestID
			}
			if event.From.IsZero() {
				event.From = typed.StartedAt
			}
			if event.To.IsZero() {
				event.To = typed.FinishedAt
			}
		case *requestOutcomeSnapshot:
			if typed != nil {
				event.Snapshot = *typed
			}
		case OutcomeGapSource:
			event.Source = typed
		case HealthScope:
			event.Scope = typed
		case OutcomeGapReason:
			event.Reason = string(typed)
		case OutcomeGapCorrelation:
			if event.SessionHash == "" {
				event.SessionHash = typed.SessionHash
			}
			if event.RequestID == 0 {
				event.RequestID = typed.RequestID
			}
		case time.Time:
			if timeCount < len(times) {
				times[timeCount] = typed
				timeCount++
			}
		case uint64:
			if event.RequestID == 0 {
				event.RequestID = typed
			} else if event.Count == 0 {
				event.Count = typed
			}
		case uint:
			if event.RequestID == 0 {
				event.RequestID = uint64(typed)
			}
		case int:
			if event.RequestID == 0 && typed >= 0 {
				event.RequestID = uint64(typed)
			}
		case string:
			if source, ok := gapSourceFromAny(typed); ok {
				event.Source = source
			} else if event.Reason == "" {
				event.Reason = typed
			}
		}
	}
	if event.Source == "" && event.Scope != "" {
		event.Source = OutcomeGapSource(event.Scope)
	}
	if _, _, ok := outcomeGapSourceIndex(event.Source); !ok {
		return OutcomeGapEvent{}, false
	}
	if event.SessionHash == "" {
		event.SessionHash = event.Snapshot.SessionHash
	}
	if event.RequestID == 0 {
		event.RequestID = event.Snapshot.RequestID
	}
	if event.From.IsZero() && timeCount > 0 {
		event.From = times[0]
	}
	if event.To.IsZero() {
		if timeCount > 1 {
			event.To = times[1]
		} else {
			event.To = event.From
		}
	}
	if event.From.IsZero() {
		event.From = event.To
	}
	if event.From.IsZero() {
		event.From = time.Now().UTC()
		event.To = event.From
	}
	if event.To.Before(event.From) {
		event.From, event.To = event.To, event.From
	}
	if event.Count == 0 {
		event.Count = 1
	}
	return event, true
}

func gapSaturatingAdd(left, right uint64) uint64 {
	if math.MaxUint64-left < right {
		return math.MaxUint64
	}
	return left + right
}

func saturatingInc(value uint64) uint64 {
	if value == math.MaxUint64 {
		return value
	}
	return value + 1
}

func snapshotFromEvent(event OutcomeGapEvent, source OutcomeGapSource, scope HealthScope, generation uint64) OutcomeGapSnapshot {
	reason := normalizeGapReason(event.Reason, source)
	if reason == "" {
		reason = string(source)
	}
	first := OutcomeGapCorrelation{SessionHash: normalizeOutcomeSessionHash(event.SessionHash), RequestID: event.RequestID}
	last := first
	return OutcomeGapSnapshot{
		Source: source, Scope: scope, Generation: generation, Count: event.Count,
		First: first, Last: last,
		FirstSessionHash: first.SessionHash, FirstRequestID: first.RequestID,
		LastSessionHash: last.SessionHash, LastRequestID: last.RequestID,
		From: event.From.UTC(), To: event.To.UTC(), Reason: reason, StableReason: reason,
		SourceMask: uint8(1 << uint(sourceIndexForScope(scope))),
	}
}

func sourceIndexForScope(scope HealthScope) int {
	if index, _, ok := outcomeGapSourceIndex(OutcomeGapSource(scope)); ok {
		return index
	}
	return 0
}

func mergeOutcomeGapSnapshot(dst *OutcomeGapSnapshot, src OutcomeGapSnapshot) {
	if dst == nil || src.Count == 0 {
		return
	}
	if dst.Count == 0 {
		*dst = src
		return
	}
	dst.Count = gapSaturatingAdd(dst.Count, src.Count)
	if src.From.Before(dst.From) || (src.From.Equal(dst.From) && src.First.RequestID < dst.First.RequestID) {
		dst.From = src.From
		dst.First = src.First
		dst.FirstSessionHash = src.First.SessionHash
		dst.FirstRequestID = src.First.RequestID
	}
	if src.To.After(dst.To) || (src.To.Equal(dst.To) && src.Last.RequestID > dst.Last.RequestID) {
		dst.To = src.To
		dst.Last = src.Last
		dst.LastSessionHash = src.Last.SessionHash
		dst.LastRequestID = src.Last.RequestID
	}
	if dst.Reason == "" {
		dst.Reason = src.Reason
	} else if src.Reason != "" && dst.Reason != src.Reason {
		dst.Reason = string(OutcomeGapReasonMixed)
	}
	dst.StableReason = dst.Reason
	dst.SourceMask |= src.SourceMask
}

// Accumulate 原子累计一个缺口并返回该 source 的最新 generation。
func (a *OutcomeGapAccumulator) Accumulate(args ...any) uint64 {
	if a == nil {
		return 0
	}
	event, ok := parseOutcomeGapEvent(args...)
	if !ok {
		return 0
	}
	source, _ := normalizeOutcomeGapSource(event.Source)
	index, scope, _ := outcomeGapSourceIndex(source)
	a.mu.Lock()
	slot := &a.slots[index]
	slot.generation = saturatingInc(slot.generation)
	addition := snapshotFromEvent(event, source, scope, slot.generation)
	if slot.hasCurrent {
		mergeOutcomeGapSnapshot(&slot.current, addition)
		slot.current.Generation = slot.generation
	} else {
		slot.current = addition
		slot.hasCurrent = true
	}
	generation := slot.generation
	a.mu.Unlock()
	return generation
}

// AccumulateEvent 是 typed convenience API。
func (a *OutcomeGapAccumulator) AccumulateEvent(event OutcomeGapEvent) uint64 {
	return a.Accumulate(event)
}

// Snapshot 返回指定 source 的 current 摘要；省略 source 时返回一个只含 source mask
// 的聚合视图，具体 slot 仍应通过显式 source 读取。
func (a *OutcomeGapAccumulator) Snapshot(sources ...any) OutcomeGapSnapshot {
	if a == nil {
		return OutcomeGapSnapshot{}
	}
	source := OutcomeGapSource("")
	if len(sources) > 0 {
		source, _ = gapSourceFromAny(sources[0])
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if source != "" {
		index, _, ok := outcomeGapSourceIndex(source)
		if ok && a.slots[index].hasCurrent {
			return a.slots[index].current
		}
		return OutcomeGapSnapshot{Source: source, Scope: HealthScope(source)}
	}
	var combined OutcomeGapSnapshot
	for index := range a.slots {
		if !a.slots[index].hasCurrent {
			continue
		}
		mergeOutcomeGapSnapshot(&combined, a.slots[index].current)
		combined.SourceMask |= a.slots[index].current.SourceMask
	}
	return combined
}

func (a *OutcomeGapAccumulator) SnapshotFor(source OutcomeGapSource) OutcomeGapSnapshot {
	return a.Snapshot(source)
}

// Claim 固化两个非空 current slot，并清空 current aggregate 标记；generation
// counter 不回退。重复 Claim 在已有 in-flight claim 时返回同一份 claim。
func (a *OutcomeGapAccumulator) Claim() OutcomeGapClaim {
	if a == nil {
		return OutcomeGapClaim{}
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.inFlight {
		return a.claimed
	}
	claim := OutcomeGapClaim{}
	a.nextID = saturatingInc(a.nextID)
	claim.ID = a.nextID
	for index := range a.slots {
		slot := &a.slots[index]
		if !slot.hasCurrent || slot.current.Count == 0 {
			continue
		}
		claim.Slots[index] = slot.current
		if index == 0 {
			claim.Queue, claim.HasQueue = slot.current, true
		} else {
			claim.Sink, claim.HasSink = slot.current, true
		}
		slot.current = OutcomeGapSnapshot{Source: slot.current.Source, Scope: slot.current.Scope, Generation: slot.generation}
		slot.hasCurrent = false
	}
	if !claim.HasQueue && !claim.HasSink {
		return OutcomeGapClaim{}
	}
	a.claimed = claim
	a.inFlight = true
	return claim
}

func claimFromAny(value any) (OutcomeGapClaim, bool) {
	switch typed := value.(type) {
	case OutcomeGapClaim:
		return typed, true
	case *OutcomeGapClaim:
		if typed != nil {
			return *typed, true
		}
	}
	return OutcomeGapClaim{}, false
}

// Commit 只恢复 claim 后没有同 scope 新 current 的 slot；结果计算后消费 claim。
func (a *OutcomeGapAccumulator) Commit(value any) OutcomeGapCommitResult {
	claim, ok := claimFromAny(value)
	if a == nil || !ok {
		return OutcomeGapCommitResult{}
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.inFlight || claim.ID == 0 || claim.ID != a.claimed.ID {
		return OutcomeGapCommitResult{}
	}
	result := OutcomeGapCommitResult{ClaimID: claim.ID}
	for index := range a.slots {
		claimed := claim.Slots[index]
		if claimed.Count == 0 {
			continue
		}
		indexSource := claimed.Source
		_, scope, valid := outcomeGapSourceIndex(indexSource)
		if !valid {
			continue
		}
		if !a.slots[index].hasCurrent && a.slots[index].generation == claimed.Generation {
			result.RecoverableScopes = append(result.RecoverableScopes, scope)
			result.Committed[index] = OutcomeGapCommittedScope{Scope: scope, Generation: claimed.Generation}
			result.CommittedGenerations[index] = claimed.Generation
		}
	}
	a.inFlight = false
	a.claimed = OutcomeGapClaim{}
	return result
}

// Merge 在写入失败时把 claim 逐 scope 合并回 current；它不递增 generation。
func (a *OutcomeGapAccumulator) Merge(value any) {
	claim, ok := claimFromAny(value)
	if a == nil || !ok {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.inFlight || claim.ID == 0 || claim.ID != a.claimed.ID {
		return
	}
	for index := range a.slots {
		claimed := claim.Slots[index]
		if claimed.Count == 0 {
			continue
		}
		slot := &a.slots[index]
		if !slot.hasCurrent {
			slot.current = claimed
			slot.current.Generation = slot.generation
			if slot.generation < claimed.Generation {
				slot.generation = claimed.Generation
				slot.current.Generation = slot.generation
			}
			slot.hasCurrent = true
			continue
		}
		mergeOutcomeGapSnapshot(&slot.current, claimed)
		if slot.generation < claimed.Generation {
			slot.generation = claimed.Generation
		}
		slot.current.Generation = slot.generation
	}
	a.inFlight = false
	a.claimed = OutcomeGapClaim{}
}

func (a *OutcomeGapAccumulator) HasCurrent(source OutcomeGapSource) bool {
	if a == nil {
		return false
	}
	index, _, ok := outcomeGapSourceIndex(source)
	if !ok {
		return false
	}
	a.mu.Lock()
	has := a.slots[index].hasCurrent
	a.mu.Unlock()
	return has
}

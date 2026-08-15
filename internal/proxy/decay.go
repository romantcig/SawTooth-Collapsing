package proxy

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
)

// DecayPhase 衰减阶段枚举（保留向后兼容 proxy.go）。
type DecayPhase int

const (
	DecayFresh     DecayPhase = 0 // 无损——消息原样保留
	DecayMiddle    DecayPhase = 1 // 轻度——thinking 移除 + tool stub
	DecayOld       DecayPhase = 2 // 中度——增加截断（阈值减半）
	DecayCompacted DecayPhase = 3 // 重度——最大化压缩
)

// decayPersisted 是 DecayTracker 的可 JSON 序列化形式。
// 对标 YesMem decay.go:35-39。
type decayPersisted struct {
	StubbedAt map[string]int     `json:"stubbed_at"`
	Intensity map[string]float64 `json:"intensity"`
	FilePaths map[string]string  `json:"file_paths"`
}

// decayLoadPollution 是加载一条记录时被忽略条目的稳定计数。
// 它只保留类别与数量：被忽略的 state key 本身既不进入内存，也不进入诊断。
type decayLoadPollution struct {
	// ForeignPrefix 是属于其他 session/epoch 的记录数。
	ForeignPrefix int
	// BareMessage 是无法归属任何 session/epoch 的裸 msg_N 记录数。
	BareMessage int
}

// PinnedPathSnapshot 是一次请求使用的路径快照。它的底层 slice 只在构造时
// 写入，生产管线不会把它交回 DecayTracker，因此不同请求之间不会共享可变
// pinned state。命名 slice 仍可与 []string 互相传递，保留旧调用方的便利性。
type PinnedPathSnapshot []string

// NewPinnedPathSnapshot 对路径去重、排序并做防御性复制。
func NewPinnedPathSnapshot(paths []string) PinnedPathSnapshot {
	seen := make(map[string]struct{}, len(paths))
	copyPaths := make(PinnedPathSnapshot, 0, len(paths))
	for _, path := range paths {
		if path == "" {
			continue
		}
		if _, exists := seen[path]; exists {
			continue
		}
		seen[path] = struct{}{}
		copyPaths = append(copyPaths, path)
	}
	sort.Strings(copyPaths)
	return copyPaths
}

func (p PinnedPathSnapshot) clone() PinnedPathSnapshot {
	return append(PinnedPathSnapshot(nil), p...)
}

func (p PinnedPathSnapshot) contains(filePath string) bool {
	if filePath == "" || len(p) == 0 {
		return false
	}
	for _, pinned := range p {
		if filePath == pinned || strings.HasSuffix(filePath, pinned) || strings.HasSuffix(pinned, filePath) {
			return true
		}
	}
	return false
}

// DecayEvaluationSnapshot 是 stubify 登记完成后，在同一把 tracker 读锁下
// 复制出的请求级事实。所有 map/slice 都是副本；后续 tracker 写入不会改变
// 本请求的阶段判定。
type DecayEvaluationSnapshot struct {
	StateKey    string
	RequestIdx  int
	ThreadLen   int
	Pressure    float64
	Generation  uint64
	PinnedPaths PinnedPathSnapshot
	StubbedAt   map[string]int
	Intensity   map[string]float64
	FilePaths   map[string]string
}

func (s DecayEvaluationSnapshot) clone() DecayEvaluationSnapshot {
	clone := DecayEvaluationSnapshot{
		StateKey:    s.StateKey,
		RequestIdx:  s.RequestIdx,
		ThreadLen:   s.ThreadLen,
		Pressure:    s.Pressure,
		Generation:  s.Generation,
		PinnedPaths: s.PinnedPaths.clone(),
		StubbedAt:   make(map[string]int, len(s.StubbedAt)),
		Intensity:   make(map[string]float64, len(s.Intensity)),
		FilePaths:   make(map[string]string, len(s.FilePaths)),
	}
	for key, value := range s.StubbedAt {
		clone.StubbedAt[key] = value
	}
	for key, value := range s.Intensity {
		clone.Intensity[key] = value
	}
	for key, value := range s.FilePaths {
		clone.FilePaths[key] = value
	}
	return clone
}

func (s DecayEvaluationSnapshot) SnapshotPinned(path string) bool {
	return s.PinnedPaths.contains(path)
}

func (s DecayEvaluationSnapshot) stateKeyForIndex(index int) string {
	return fmt.Sprintf("%s:msg_%d", s.StateKey, index)
}

func (s DecayEvaluationSnapshot) stageAt(index int) DecayPhase {
	key := s.stateKeyForIndex(index)
	stubbedAt, exists := s.StubbedAt[key]
	if !exists {
		return DecayFresh
	}
	boost := int(s.Intensity[key] * 20)
	if s.PinnedPaths.contains(s.FilePaths[key]) {
		boost += 30
	}
	age := s.RequestIdx - stubbedAt
	s0end, s1end, s2end := decayBoundaries(s.ThreadLen, s.Pressure)
	switch {
	case age < s0end+boost:
		return DecayFresh
	case age < s1end+boost:
		return DecayMiddle
	case age < s2end+boost:
		return DecayOld
	default:
		return DecayCompacted
	}
}

// DecayTracker 追踪每条消息的桩化时间和关联数据，实现逐消息渐进式衰减。
// 对标 YesMem decay.go:19-32。
type DecayTracker struct {
	mu sync.RWMutex
	// messageKey（格式 "msg_N"）→ 首次桩化时的请求序号
	stubbedAt map[string]int
	// messageKey → 桩化时的情绪强度（0.0-1.0）
	intensity map[string]float64
	// messageKey → 关联文件路径（从 tool_use.input.file_path 提取）
	filePaths map[string]string
	// legacyPinnedPaths 仅供旧测试/旧 API 兼容；生产请求不读取它。
	// 新管线把 pinned paths 固定在 DecayEvaluationSnapshot 中。
	legacyPinnedPaths map[string]bool
	// generation 只用于诊断快照的一致性证明，不参与跨请求决策。
	generation   map[string]uint64
	persistFn    PersistFunc
	loadFn       LoadFunc
	stateLoader  StateLoader    // 生产：error-aware 状态读取合同
	submitter    StateSubmitter // 生产：锁外 result-bearing 写入提交
	loadedFromDB map[string]bool
	stateFoundDB map[string]bool
	currentAlias map[string]string
	// persistLocks 让同一 stateKey 的"快照 + 提交"整体串行，从而在不持有
	// d.mu 的前提下保持磁盘顺序与内存顺序一致。
	persistLocks map[string]*sync.Mutex
	// loadPollution 记录每个 stateKey 加载时被忽略的条目数量。
	loadPollution map[string]decayLoadPollution
}

// NewDecayTracker 创建新的衰减追踪器。
func NewDecayTracker() *DecayTracker {
	return &DecayTracker{
		stubbedAt:         make(map[string]int),
		intensity:         make(map[string]float64),
		filePaths:         make(map[string]string),
		legacyPinnedPaths: make(map[string]bool),
		generation:        make(map[string]uint64),
		loadedFromDB:      make(map[string]bool),
		stateFoundDB:      make(map[string]bool),
		currentAlias:      make(map[string]string),
		persistLocks:      make(map[string]*sync.Mutex),
		loadPollution:     make(map[string]decayLoadPollution),
	}
}

// SetCurrentAlias 让旧 session 级观察调用解析到当前 epoch key。
// 主管线自身始终传入显式 stateKey。
func (d *DecayTracker) SetCurrentAlias(sessionID, stateKey string) {
	if d == nil || sessionID == "" || stateKey == "" {
		return
	}
	d.mu.Lock()
	d.currentAlias[sessionID] = stateKey
	d.mu.Unlock()
}

func (d *DecayTracker) resolveStateKeyLocked(sessionID string) string {
	if alias := d.currentAlias[sessionID]; alias != "" {
		return alias
	}
	return sessionID
}

func (d *DecayTracker) hasSessionStateLocked(sessionID string) bool {
	prefix := sessionID + ":msg_"
	for key := range d.stubbedAt {
		if strings.HasPrefix(key, prefix) {
			return true
		}
	}
	for key := range d.intensity {
		if strings.HasPrefix(key, prefix) {
			return true
		}
	}
	for key := range d.filePaths {
		if strings.HasPrefix(key, prefix) {
			return true
		}
	}
	return false
}

// MigrateLegacyState 一次性把裸 session 前缀的衰减状态复制到 epoch 1 key。
// 显式目标 key 已存在时目标优先，绝不再读取旧裸 key。
// 目标 key 读取失败时同样立即终止：查询错误不是"目标没有状态"，回退读 legacy
// 会让上一个 epoch 的衰减状态复活。
func (d *DecayTracker) MigrateLegacyState(sessionID, stateKey string) StateLoadFailure {
	if d == nil || sessionID == "" || stateKey == "" || sessionID == stateKey {
		return StateLoadFailure{}
	}
	if failure := d.LoadFromDB(stateKey); failure.Failed {
		return failure
	}
	d.mu.RLock()
	targetExists := d.hasSessionStateLocked(stateKey)
	targetFound := d.stateFoundDB[stateKey]
	d.mu.RUnlock()
	if targetExists || targetFound {
		return StateLoadFailure{}
	}

	d.LoadFromDB(sessionID)
	persistLock := d.decayPersistLock(stateKey)
	persistLock.Lock()
	defer persistLock.Unlock()

	d.mu.Lock()
	if d.hasSessionStateLocked(stateKey) || d.stateFoundDB[stateKey] || !d.hasSessionStateLocked(sessionID) {
		d.mu.Unlock()
		return StateLoadFailure{}
	}
	legacyPrefix := sessionID + ":msg_"
	targetPrefix := stateKey + ":msg_"
	for key, value := range d.stubbedAt {
		if strings.HasPrefix(key, legacyPrefix) {
			d.stubbedAt[targetPrefix+strings.TrimPrefix(key, legacyPrefix)] = value
		}
	}
	for key, value := range d.intensity {
		if strings.HasPrefix(key, legacyPrefix) {
			d.intensity[targetPrefix+strings.TrimPrefix(key, legacyPrefix)] = value
		}
	}
	for key, value := range d.filePaths {
		if strings.HasPrefix(key, legacyPrefix) {
			d.filePaths[targetPrefix+strings.TrimPrefix(key, legacyPrefix)] = value
		}
	}
	d.loadedFromDB[stateKey] = true
	snapshot := d.snapshotStateLocked(stateKey)
	submitter, persistFn := d.submitter, d.persistFn
	shouldPersist := submitter != nil || persistFn != nil
	if shouldPersist {
		d.stateFoundDB[stateKey] = true
	}
	d.mu.Unlock()

	if shouldPersist {
		d.submitDecayState(submitter, persistFn, stateKey, snapshot, nil)
	}
	return StateLoadFailure{}
}

// SetPersistFunc 设置持久化回调。
func (d *DecayTracker) SetPersistFunc(fn PersistFunc) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.persistFn = fn
}

// SetLoadFunc 是 bool-only 加载回调的兼容入口，只供尚未迁移的测试使用。
// 它把回调包装成永远不产生 Err 的 StateLoader；生产 wiring 必须用
// SetStateLoader，否则查询失败会被当成"状态不存在"。
func (d *DecayTracker) SetLoadFunc(fn LoadFunc) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.loadFn = fn
	d.stateLoader = stateLoaderFromLoadFunc(fn)
}

// SetStateLoader 设置生产使用的 error-aware 状态读取合同。
func (d *DecayTracker) SetStateLoader(loader StateLoader) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.stateLoader = loader
}

// SetStateSubmitter 设置锁外 result-bearing 写入提交器。
func (d *DecayTracker) SetStateSubmitter(submitter StateSubmitter) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.submitter = submitter
}

// decayPersistLock 返回 stateKey 的持久化串行锁。它自己会短暂获取 d.mu，
// 因此必须在进入组件锁之前调用。
func (d *DecayTracker) decayPersistLock(stateKey string) *sync.Mutex {
	d.mu.Lock()
	lock := d.persistLocks[stateKey]
	if lock == nil {
		lock = &sync.Mutex{}
		d.persistLocks[stateKey] = lock
	}
	d.mu.Unlock()
	return lock
}

// snapshotStateLocked 是三张全局 map 唯一的持久化取样口：只复制
// "stateKey:msg_" 前缀的记录，其他 session/epoch 与裸 msg_N 一律不进入快照。
// 调用方必须持有 d.mu（读锁或写锁）。
func (d *DecayTracker) snapshotStateLocked(stateKey string) decayPersisted {
	prefix := stateKey + ":msg_"
	dp := decayPersisted{
		StubbedAt: make(map[string]int),
		Intensity: make(map[string]float64),
		FilePaths: make(map[string]string),
	}
	for key, value := range d.stubbedAt {
		if strings.HasPrefix(key, prefix) {
			dp.StubbedAt[key] = value
		}
	}
	for key, value := range d.intensity {
		if strings.HasPrefix(key, prefix) {
			dp.Intensity[key] = value
		}
	}
	for key, value := range d.filePaths {
		if strings.HasPrefix(key, prefix) {
			dp.FilePaths[key] = value
		}
	}
	return dp
}

// submitDecayState 是唯一的外部写入口，必须在 d.mu 之外调用。
func (d *DecayTracker) submitDecayState(submitter StateSubmitter, persistFn PersistFunc, stateKey string, snapshot decayPersisted, completion *outcomeCompletion) PersistenceReceipt {
	data, err := json.Marshal(snapshot)
	if err != nil {
		return completeStateOp(completion, persistenceStateNotAttempted, persistenceFailureNone)
	}
	key := "decay:" + stateKey
	value := string(data)
	var legacy func()
	if persistFn != nil {
		legacy = func() { persistFn(key, value) }
	}
	return submitStateOp(submitter, legacy, PersistenceOp{
		Kind: PersistenceOpPut, OrderingKey: key, Key: key, Value: value, Completion: completion,
	})
}

// MarkStubbed 记录一条消息在指定请求序号被桩化。
// 仅记录首次桩化，后续调用不做更新。
// emotionalIntensity 是桩化时的情绪强度（0.0-1.0），影响衰减速度。
// key 格式: "sessionID:msg_N"（session-scoped，多 session 并发安全）。
func (d *DecayTracker) MarkStubbed(sessionID string, msgIndex, requestIdx int, emotionalIntensity float64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	sessionID = d.resolveStateKeyLocked(sessionID)
	key := fmt.Sprintf("%s:msg_%d", sessionID, msgIndex)
	if _, exists := d.stubbedAt[key]; !exists {
		d.stubbedAt[key] = requestIdx
		d.intensity[key] = emotionalIntensity
		d.generation[sessionID]++
	}
}

// SetFilePath 记录桩化消息关联的文件路径。
// key 格式: "sessionID:msg_N"。
func (d *DecayTracker) SetFilePath(sessionID string, msgIndex int, path string) {
	if path == "" {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	sessionID = d.resolveStateKeyLocked(sessionID)
	key := fmt.Sprintf("%s:msg_%d", sessionID, msgIndex)
	d.filePaths[key] = path
	d.generation[sessionID]++
}

// SetPinnedPaths 更新"活跃路径"集合——引用这些路径的消息衰减更慢。
// 在每次 stubify 之前调用。无 daemon 替代方案：从当前请求的 tool_use 块提取。
func (d *DecayTracker) SetPinnedPaths(paths []string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.legacyPinnedPaths = make(map[string]bool, len(paths))
	for _, p := range NewPinnedPathSnapshot(paths) {
		d.legacyPinnedPaths[p] = true
	}
}

// isPinnedPath 检查文件路径是否匹配任意活跃路径。
// 双向后缀匹配：短路径可匹配长路径的尾部和反向。
func (d *DecayTracker) isPinnedPath(filePath string) bool {
	if filePath == "" || len(d.legacyPinnedPaths) == 0 {
		return false
	}
	if d.legacyPinnedPaths[filePath] {
		return true
	}
	for pp := range d.legacyPinnedPaths {
		if strings.HasSuffix(filePath, pp) || strings.HasSuffix(pp, filePath) {
			return true
		}
	}
	return false
}

// BuildDecayEvaluationSnapshot 在一次读锁内复制指定 stateKey 的全部衰减
// 事实和 request-local pinned paths。调用方可在返回后并发更新 tracker，已
// 返回的 snapshot/plan 仍保持不变。
func (d *DecayTracker) BuildDecayEvaluationSnapshot(stateKey string, pinned PinnedPathSnapshot, requestIdx, threadLen int, pressure float64) DecayEvaluationSnapshot {
	snapshot := DecayEvaluationSnapshot{
		StateKey:    stateKey,
		RequestIdx:  requestIdx,
		ThreadLen:   threadLen,
		Pressure:    pressure,
		PinnedPaths: NewPinnedPathSnapshot(pinned),
		StubbedAt:   make(map[string]int),
		Intensity:   make(map[string]float64),
		FilePaths:   make(map[string]string),
	}
	if d == nil {
		return snapshot
	}
	d.mu.RLock()
	resolved := d.resolveStateKeyLocked(stateKey)
	snapshot.StateKey = resolved
	snapshot.Generation = d.generation[resolved]
	prefix := resolved + ":msg_"
	for key, value := range d.stubbedAt {
		if strings.HasPrefix(key, prefix) {
			snapshot.StubbedAt[key] = value
		}
	}
	for key, value := range d.intensity {
		if strings.HasPrefix(key, prefix) {
			snapshot.Intensity[key] = value
		}
	}
	for key, value := range d.filePaths {
		if strings.HasPrefix(key, prefix) {
			snapshot.FilePaths[key] = value
		}
	}
	d.mu.RUnlock()
	return snapshot
}

// Snapshot 是较短的兼容别名，供后续计划消费稳定接口。
func (d *DecayTracker) Snapshot(stateKey string, pinned PinnedPathSnapshot, requestIdx, threadLen int, pressure float64) DecayEvaluationSnapshot {
	return d.BuildDecayEvaluationSnapshot(stateKey, pinned, requestIdx, threadLen, pressure)
}

// BuildCompactionPlan 是 tracker 侧的便捷入口，返回的 plan 与 tracker
// 生命周期解耦；后续阶段只应消费返回值。
func (d *DecayTracker) BuildCompactionPlan(stateKey string, pinned PinnedPathSnapshot, messages, original []Message, requestIdx, totalTokens, threshold, keepRecent int, compactEnabled bool) *CompactionPlan {
	return BuildCompactionPlanFromTracker(d, stateKey, pinned, messages, original, requestIdx, totalTokens, threshold, keepRecent, compactEnabled)
}

// decayBoundaries 根据线程长度和 token 压力计算自适应阶段边界。
// 对标 YesMem decay.go:90-119。
// pressure = totalTokens / threshold。低压力 → 拉伸边界（衰减更慢）。
func decayBoundaries(threadLen int, pressure float64) (s0end, s1end, s2end int) {
	var base0, base1, base2 int
	switch {
	case threadLen < 500:
		base0, base1, base2 = 5, 15, 50
	case threadLen < 2000:
		base0, base1, base2 = 5, 12, 40
	default:
		base0, base1, base2 = 4, 10, 30
	}

	// 压力缩放：pressure 1.0 时拉伸 3x，pressure 2.5+ 时不拉伸。
	stretch := 1.0
	if pressure < 2.5 {
		stretch = 1.0 + (2.5-pressure)/0.75 // [1.0, 4.33]
		if stretch > 4.0 {
			stretch = 4.0
		}
		if stretch < 1.0 {
			stretch = 1.0
		}
	}

	return int(float64(base0) * stretch),
		int(float64(base1) * stretch),
		int(float64(base2) * stretch)
}

// GetStage 返回指定消息的衰减阶段（0-3）。
// currentRequestIdx 是当前请求序号，threadLen 是原始线程长度。
// key 格式: "sessionID:msg_N"。
func (d *DecayTracker) GetStage(sessionID string, msgIndex, currentRequestIdx, threadLen int, pressure float64) DecayPhase {
	d.mu.RLock()
	sessionID = d.resolveStateKeyLocked(sessionID)
	key := fmt.Sprintf("%s:msg_%d", sessionID, msgIndex)
	stubbedAt, exists := d.stubbedAt[key]
	emotionalIntensity := d.intensity[key]
	filePath := d.filePaths[key]
	isPinned := d.isPinnedPath(filePath)
	d.mu.RUnlock()

	if !exists {
		return DecayFresh
	}

	age := currentRequestIdx - stubbedAt
	boost := int(emotionalIntensity * 20) // 0-20 extra requests before decay

	// 活跃路径 +30 额外请求保护
	if isPinned {
		boost += 30
	}

	s0end, s1end, s2end := decayBoundaries(threadLen, pressure)

	if age < s0end+boost {
		return DecayFresh
	}
	if age < s1end+boost {
		return DecayMiddle
	}
	if age < s2end+boost {
		return DecayOld
	}
	return DecayCompacted
}

// Persist 将当前衰减状态持久化到 DB。
func (d *DecayTracker) Persist(threadID string) {
	d.PersistWithResult(threadID, nil)
}

// PersistWithResult 是 result-bearing 的持久化入口：快照在组件锁内取样，
// 外部提交在锁外完成，磁盘结果通过 completion 回到调用方的 request outcome。
func (d *DecayTracker) PersistWithResult(threadID string, completion *outcomeCompletion) PersistenceReceipt {
	if d == nil {
		return completeStateOp(completion, persistenceStateNotAttempted, persistenceFailureNone)
	}
	d.mu.RLock()
	stateKey := d.resolveStateKeyLocked(threadID)
	hasSink := d.submitter != nil || d.persistFn != nil
	d.mu.RUnlock()
	if !hasSink {
		return completeStateOp(completion, persistenceStateNotAttempted, persistenceFailureNone)
	}

	lock := d.decayPersistLock(stateKey)
	lock.Lock()
	defer lock.Unlock()

	d.mu.RLock()
	snapshot := d.snapshotStateLocked(stateKey)
	submitter, persistFn := d.submitter, d.persistFn
	d.mu.RUnlock()

	return d.submitDecayState(submitter, persistFn, stateKey, snapshot, completion)
}

// LoadFromDB 从 DB 恢复衰减状态。只有 ErrNoRows（Found=false 且 Err=nil）才算
// missing；读取失败不设置任何已加载标记、不应用任何状态，允许下一次请求重试。
// 加载只接受当前 stateKey 前缀的记录：污染进来的其他 session/epoch 与无法归属
// 的裸 msg_N 一律忽略，只留下稳定的数量计数。
func (d *DecayTracker) LoadFromDB(threadID string) StateLoadFailure {
	d.mu.Lock()
	stateKey := d.resolveStateKeyLocked(threadID)
	if d.loadedFromDB[stateKey] {
		d.mu.Unlock()
		return StateLoadFailure{}
	}
	loader := d.stateLoader
	d.mu.Unlock()

	if loader == nil {
		d.mu.Lock()
		d.loadedFromDB[stateKey] = true
		d.mu.Unlock()
		return StateLoadFailure{}
	}

	result := loader.LoadStateResult("decay:" + stateKey)
	if result.Err != nil {
		// 查询失败不是"状态不存在"：不设置 loaded，也不写入任何状态。
		return stateLoadFailureFrom(result)
	}
	if !result.Found || result.Value == "" {
		d.mu.Lock()
		d.loadedFromDB[stateKey] = true
		d.mu.Unlock()
		return StateLoadFailure{}
	}
	d.mu.Lock()
	d.stateFoundDB[stateKey] = true
	d.loadedFromDB[stateKey] = true
	d.mu.Unlock()

	var dp decayPersisted
	if err := json.Unmarshal([]byte(result.Value), &dp); err != nil {
		return StateLoadFailure{}
	}

	prefix := stateKey + ":msg_"
	var pollution decayLoadPollution
	d.mu.Lock()
	defer d.mu.Unlock()
	for k, v := range dp.StubbedAt {
		if !strings.HasPrefix(k, prefix) {
			pollution.observe(k)
			continue
		}
		if _, exists := d.stubbedAt[k]; !exists {
			d.stubbedAt[k] = v
		}
	}
	for k, v := range dp.Intensity {
		if !strings.HasPrefix(k, prefix) {
			pollution.observe(k)
			continue
		}
		if _, exists := d.intensity[k]; !exists {
			d.intensity[k] = v
		}
	}
	for k, v := range dp.FilePaths {
		if !strings.HasPrefix(k, prefix) {
			pollution.observe(k)
			continue
		}
		if _, exists := d.filePaths[k]; !exists {
			d.filePaths[k] = v
		}
	}
	if pollution.ForeignPrefix > 0 || pollution.BareMessage > 0 {
		total := d.loadPollution[stateKey]
		total.ForeignPrefix += pollution.ForeignPrefix
		total.BareMessage += pollution.BareMessage
		d.loadPollution[stateKey] = total
	}
	return StateLoadFailure{}
}

// observe 把一个被拒绝的 key 归入稳定类别，只累加数量、不保存 key 本身。
func (p *decayLoadPollution) observe(key string) {
	if strings.HasPrefix(key, "msg_") {
		p.BareMessage++
		return
	}
	p.ForeignPrefix++
}

// ClearSession 清空指定 session 的全部衰减追踪状态。
// collapse 重建消息数组后调用——旧 key 不再有效。
// key 格式为 "sessionID:msg_N"，ClearSession 按前缀匹配删除。
func (d *DecayTracker) ClearSession(sessionID string) {
	d.ClearSessionWithResult(sessionID, nil)
}

// ClearSessionWithResult 是 result-bearing 的清理入口：删除与快照在组件锁内完成，
// 外部提交在锁外完成。
func (d *DecayTracker) ClearSessionWithResult(sessionID string, completion *outcomeCompletion) PersistenceReceipt {
	if d == nil {
		return completeStateOp(completion, persistenceStateNotAttempted, persistenceFailureNone)
	}
	d.mu.RLock()
	stateKey := d.resolveStateKeyLocked(sessionID)
	d.mu.RUnlock()

	lock := d.decayPersistLock(stateKey)
	lock.Lock()
	defer lock.Unlock()

	prefix := stateKey + ":msg_"
	d.mu.Lock()
	for k := range d.stubbedAt {
		if strings.HasPrefix(k, prefix) {
			delete(d.stubbedAt, k)
		}
	}
	for k := range d.intensity {
		if strings.HasPrefix(k, prefix) {
			delete(d.intensity, k)
		}
	}
	for k := range d.filePaths {
		if strings.HasPrefix(k, prefix) {
			delete(d.filePaths, k)
		}
	}
	_, snapshot := d.PersistUnlocked(stateKey)
	submitter, persistFn := d.submitter, d.persistFn
	d.mu.Unlock()

	// 三张 map 的前缀条目已删除，内存状态更新已落定。
	completion.noteMemorySaved()

	if submitter == nil && persistFn == nil {
		return completeStateOp(completion, persistenceStateNotAttempted, persistenceFailureNone)
	}
	return d.submitDecayState(submitter, persistFn, stateKey, snapshot, completion)
}

// PersistUnlocked 在调用方已持有 d.mu 时生成待持久化快照。它复用
// snapshotStateLocked，因此与 Persist 走同一套前缀过滤；并且刻意不执行外部
// 提交——组件锁内调用 SQLite 会阻塞其他请求，提交必须由调用方在释放锁之后
// 用 submitDecayState 完成。
func (d *DecayTracker) PersistUnlocked(threadID string) (string, decayPersisted) {
	stateKey := d.resolveStateKeyLocked(threadID)
	return stateKey, d.snapshotStateLocked(stateKey)
}

// ---- estimateIntensity ----

// estimateIntensity 基于最近消息的结构信号估算情绪强度（0.0-1.0）。
// 对标 YesMem intensity.go:7-30。语言无关。
func estimateIntensity(messages []Message) float64 {
	intensity := 0.0
	recent := lastN(messages, 10)

	// 错误 tool_result 计数
	errors := countErrors(recent)
	intensity += float64(errors) * 0.15

	// 工具调用密度（> 5 = 密集工作）
	toolCalls := countToolUses(recent)
	if toolCalls > 5 {
		intensity += 0.2
	}

	// 长用户消息（> 500 rune = 复杂请求）
	if lastUserMsgLen(messages) > 500 {
		intensity += 0.15
	}

	if intensity > 1.0 {
		return 1.0
	}
	return intensity
}

func countErrors(messages []Message) int {
	count := 0
	for _, msg := range messages {
		blocks, _ := parseContent(msg.Content)
		for _, block := range blocks {
			if block.Type == "tool_result" && block.IsError {
				count++
			}
		}
	}
	return count
}

func countToolUses(messages []Message) int {
	count := 0
	for _, msg := range messages {
		blocks, _ := parseContent(msg.Content)
		for _, block := range blocks {
			if block.Type == "tool_use" {
				count++
			}
		}
	}
	return count
}

func lastUserMsgLen(messages []Message) int {
	for i := len(messages) - 1; i >= 0; i-- {
		if messages[i].Role != "user" {
			continue
		}
		blocks, _ := parseContent(messages[i].Content)
		for _, b := range blocks {
			if b.Type == "text" {
				return len([]rune(b.Text))
			}
		}
	}
	return 0
}

func lastN(messages []Message, n int) []Message {
	if len(messages) <= n {
		return messages
	}
	return messages[len(messages)-n:]
}

// ---- 阶段感知衰减（对标 YesMem decay.go:160-237） ----

// ApplyDecay 根据衰减阶段压缩文本桩。
func ApplyDecay(stub string, stage DecayPhase, role string) string {
	switch stage {
	case DecayFresh:
		return stub
	case DecayMiddle:
		return decayToStage1(stub, role)
	case DecayOld:
		return decayToStage2(stub, role)
	case DecayCompacted:
		return "" // 内容存在于 compacted block 中
	default:
		return stub
	}
}

func decayToStage1(stub string, role string) string {
	if role == "user" || role == "assistant" {
		r := []rune(stub)
		limit := 120
		if role == "user" {
			limit = 200
		}
		if len(r) > limit {
			return string(r[:limit]) + "..."
		}
	}
	return stub
}

func decayToStage2(stub string, role string) string {
	if role == "user" || role == "assistant" {
		r := []rune(stub)
		limit := 50
		if role == "user" {
			limit = 80
		}
		if len(r) > limit {
			return string(r[:limit]) + "..."
		}
	}
	return stub
}

// ApplyDecayToToolStub 根据衰减阶段压缩工具桩。
func ApplyDecayToToolStub(stub string, stage DecayPhase) string {
	switch stage {
	case DecayFresh:
		return stub
	case DecayMiddle:
		// 移除注释，保留前 3 词
		if idx := findAnnotationSep(stub); idx >= 0 {
			ann := stub[idx+len(" — "):]
			words := firstNWords(ann, 3)
			if words != "" {
				return stub[:idx] + " — " + words
			}
			return stub[:idx]
		}
		return stub
	case DecayOld:
		// 完全移除注释
		if idx := findAnnotationSep(stub); idx >= 0 {
			return stub[:idx]
		}
		return stub
	case DecayCompacted:
		return ""
	default:
		return stub
	}
}

func findAnnotationSep(s string) int {
	return strings.Index(s, " — ")
}

func firstNWords(s string, n int) string {
	words := 0
	for i, ch := range s {
		if ch == ' ' {
			words++
			if words >= n {
				return s[:i]
			}
		}
	}
	return s
}

// ---- ApplyDecayBatch（批量逐消息衰减，供 proxy.go 使用） ----

// ApplyDecayBatch 对消息执行逐消息阶段感知衰减处理。
// 可选 plan 参数是生产管线的唯一阶段/覆盖来源；未被 plan 覆盖的 Stage-3
// 消息统一退回 Stage-2，绝不先清空再等待另一次 tracker 扫描。
// 不带 plan 的调用保留旧 API 兼容，但同样采用安全的 Stage-2 fallback。
func (d *DecayTracker) ApplyDecayBatch(messages []Message, sessionID string, totalTokens int, threshold int, tc *TokenCounter, pivotText string, requestIdx int, plans ...*CompactionPlan) ([]Message, DecayPhase) {
	pressure := 0.0
	if threshold > 0 {
		pressure = float64(totalTokens) / float64(threshold)
	}
	var plan *CompactionPlan
	if len(plans) > 0 {
		plan = plans[0]
	}

	// 计算整体阶段（基于压力，供 collapse 决策）
	var overallPhase DecayPhase
	switch {
	case pressure >= 3.0:
		overallPhase = DecayCompacted
	case pressure >= 2.0:
		overallPhase = DecayOld
	case pressure >= 1.0:
		overallPhase = DecayMiddle
	default:
		overallPhase = DecayFresh
	}

	// 对 messages 应用逐消息衰减
	result := make([]Message, 0, len(messages))
	threadLen := len(messages)
	activeAssistant, activeResult := activeToolPairIndices(messages)
	for i, msg := range messages {
		if i == activeAssistant || i == activeResult {
			result = append(result, msg)
			continue
		}
		stage := DecayFresh
		fallbackStage2 := false
		if plan != nil {
			stage = plan.StageAt(i)
		} else if d != nil {
			stage = d.GetStage(sessionID, i, requestIdx, threadLen, pressure)
		}
		// Snapshot/plan 是 immutable 的；若旧调用没有 plan，也不能产生无
		// replacement 的空正文，因此 Stage-3 默认先降级到 Stage-2。
		stage3Covered := plan != nil && plan.IsCovered(i)
		if stage == DecayCompacted && !stage3Covered {
			stage = DecayOld
			fallbackStage2 = true
		}
		if plan != nil && fallbackStage2 {
			if fallback, ok := plan.Stage2Message(i); ok {
				msg.Content = append(json.RawMessage(nil), fallback.Content...)
				result = append(result, msg)
				continue
			}
		}
		blocks, isArray := parseContent(msg.Content)
		changed := false

		for j := range blocks {
			switch blocks[j].Type {
			case "text":
				// 文本桩：应用 ApplyDecay
				original := blocks[j].Text
				decayed := ApplyDecay(blocks[j].Text, stage, msg.Role)
				blocks[j].Text = decayed
				if decayed != original {
					changed = true
				}
			case "tool_use":
				// 工具桩格式: "[→] ToolName args — annotation"
				original := blocks[j].Text
				if original == "" {
					// 未桩化的 tool_use — 跳过衰减
					continue
				}
				decayed := ApplyDecayToToolStub(original, stage)
				blocks[j].Text = decayed
				if decayed != original {
					changed = true
				}
			case "tool_result":
				// tool_result 桩格式: "[tool result archived]"
				original := blocks[j].Text
				if original == "" {
					continue
				}
				decayed := ApplyDecayToToolStub(original, stage)
				blocks[j].Text = decayed
				if decayed != original {
					changed = true
				}
			}
		}

		if changed {
			msg.Content = rebuildContent(blocks, isArray)
		}
		result = append(result, msg)
	}

	// 持久化衰减状态（graceful: 失败不影响请求）
	if d != nil {
		d.Persist(sessionID)
	}

	return result, overallPhase
}

// ApplyDecayBatchWithPlan 是显式 plan-aware 入口，便于后续计划避免使用
// variadic 兼容签名。
func (d *DecayTracker) ApplyDecayBatchWithPlan(messages []Message, sessionID string, totalTokens, threshold int, tc *TokenCounter, pivotText string, requestIdx int, plan *CompactionPlan) ([]Message, DecayPhase) {
	return d.ApplyDecayBatch(messages, sessionID, totalTokens, threshold, tc, pivotText, requestIdx, plan)
}

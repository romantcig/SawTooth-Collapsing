package proxy

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"
)

// PersistFunc 将 key-value 状态持久化到外部存储（如 SQLite frozen_state 表）。
type PersistFunc func(key, value string)

// LoadFunc 从外部存储读取 key 对应的状态值。
// 返回 value 和是否找到。
type LoadFunc func(key string) (string, bool)

// DeleteFunc 删除外部存储中的指定状态键。
type DeleteFunc func(key string)

const (
	frozenRawPrefixModeFull   = "full"
	frozenRawPrefixModeLegacy = "legacy_boundary"
)

// FrozenStubs 存储每个 thread 的冻结桩化消息前缀，用于缓存优化。
// 桩化周期之间，冻结前缀被逐字节复用，使 API 缓存在前缀部分可命中。
type FrozenStubs struct {
	mu            sync.RWMutex
	ttl           time.Duration        // eviction TTL —— 默认 30 分钟
	messages      map[string][]Message // threadID → 深拷贝的桩化消息
	cutoff        map[string]int       // threadID → Store 时的原始消息总数
	boundaryHash  map[string]string    // threadID → messages[cutoff-1] 的稳定 hash
	prefixHash    map[string]string    // threadID → 序列化后 frozen prefix 的 SHA-256 hash
	rawPrefixHash map[string]string    // threadID → raw history[:cutoff] 的完整 reuse hash
	rawPrefixMode map[string]string    // threadID → full 或 legacy_boundary（仅兼容旧内存调用）
	stubTime      map[string]time.Time
	tokens        map[string]int // threadID → frozen stubs 的 token 估算
	rawTokens     map[string]int // threadID → Store 时原始 token 估算（压缩前）
	lastAccess    map[string]time.Time
	persistFn     PersistFunc                 // 可选：持久化 frozen 状态到 DB
	loadFn        LoadFunc                    // 兼容：旧 bool-only 加载回调（仅测试）
	stateLoader   StateLoader                 // 生产：error-aware 状态读取合同
	submitter     StateSubmitter              // 生产：锁外 result-bearing 写入提交
	deleteFn      DeleteFunc                  // 可选：失效时删除 DB 中的 frozen 状态
	loadedFromDB  map[string]bool             // threadID → 已尝试从 DB 加载
	loadingFromDB map[string]chan struct{}    // stateKey → 正在进行的 DB 加载完成信号
	loadFailures  map[string]StateLoadFailure // stateKey → 最近一次读取失败事实
	persistLocks  map[string]*sync.Mutex      // stateKey → 锁外持久化串行锁
	stateFoundDB  map[string]bool             // threadID → DB 中曾存在显式状态（即使内容无效）
	currentAlias  map[string]string           // legacy session key → 当前 epoch state key
}

// frozenPersisted 是 frozen 桩化状态的可 JSON 序列化形式。
type frozenPersisted struct {
	Messages      []Message `json:"messages"`
	Cutoff        int       `json:"cutoff"`
	BoundaryHash  string    `json:"boundary_hash"`
	PrefixHash    string    `json:"prefix_hash"`
	RawPrefixHash string    `json:"raw_prefix_hash"`
	RawPrefixMode string    `json:"raw_prefix_mode,omitempty"`
	Tokens        int       `json:"tokens"`
	RawTokens     int       `json:"raw_tokens,omitempty"`
}

// FrozenResult 包含一个已验证的 frozen prefix 及其元数据。
type FrozenResult struct {
	Messages  []Message // 冻结桩化消息（深拷贝，可安全修改）
	Cutoff    int       // Store 时的原始消息总数
	Tokens    int       // token 估算
	RawTokens int       // Store 时原始 token 估算（压缩前）
}

// NewFrozenStubs 创建使用默认 30 分钟 TTL 的 FrozenStubs 存储。
func NewFrozenStubs() *FrozenStubs {
	return NewFrozenStubsWithTTL(30 * time.Minute)
}

// NewFrozenStubsWithTTL 创建使用自定义 eviction TTL 的 FrozenStubs 存储。
func NewFrozenStubsWithTTL(ttl time.Duration) *FrozenStubs {
	return &FrozenStubs{
		ttl:           ttl,
		messages:      make(map[string][]Message),
		cutoff:        make(map[string]int),
		boundaryHash:  make(map[string]string),
		prefixHash:    make(map[string]string),
		rawPrefixHash: make(map[string]string),
		rawPrefixMode: make(map[string]string),
		stubTime:      make(map[string]time.Time),
		tokens:        make(map[string]int),
		rawTokens:     make(map[string]int),
		lastAccess:    make(map[string]time.Time),
		loadedFromDB:  make(map[string]bool),
		loadingFromDB: make(map[string]chan struct{}),
		loadFailures:  make(map[string]StateLoadFailure),
		persistLocks:  make(map[string]*sync.Mutex),
		stateFoundDB:  make(map[string]bool),
		currentAlias:  make(map[string]string),
	}
}

// SetCurrentAlias 保留旧的 session 级查询兼容性，但实际 epoch 状态仍使用
// 独立 state key。主管线在每次 epoch gate 后更新 alias；旧 epoch key不解析。
func (f *FrozenStubs) SetCurrentAlias(sessionID, stateKey string) {
	if f == nil || sessionID == "" || stateKey == "" {
		return
	}
	f.mu.Lock()
	f.currentAlias[sessionID] = stateKey
	f.mirrorCurrentAliasesLocked(stateKey)
	f.mu.Unlock()
}

func (f *FrozenStubs) resolveStateKeyLocked(threadID string) string {
	if alias := f.currentAlias[threadID]; alias != "" {
		return alias
	}
	return threadID
}

func (f *FrozenStubs) mirrorCurrentAliasesLocked(stateKey string) {
	for sessionID, current := range f.currentAlias {
		if current != stateKey || sessionID == stateKey {
			continue
		}
		if _, exists := f.messages[stateKey]; !exists {
			delete(f.messages, sessionID)
			delete(f.cutoff, sessionID)
			delete(f.boundaryHash, sessionID)
			delete(f.prefixHash, sessionID)
			delete(f.rawPrefixHash, sessionID)
			delete(f.rawPrefixMode, sessionID)
			delete(f.stubTime, sessionID)
			delete(f.tokens, sessionID)
			delete(f.rawTokens, sessionID)
			delete(f.lastAccess, sessionID)
			f.loadedFromDB[sessionID] = true
			continue
		}
		f.messages[sessionID] = f.messages[stateKey]
		f.cutoff[sessionID] = f.cutoff[stateKey]
		f.boundaryHash[sessionID] = f.boundaryHash[stateKey]
		f.prefixHash[sessionID] = f.prefixHash[stateKey]
		f.rawPrefixHash[sessionID] = f.rawPrefixHash[stateKey]
		f.rawPrefixMode[sessionID] = f.rawPrefixMode[stateKey]
		f.stubTime[sessionID] = f.stubTime[stateKey]
		f.tokens[sessionID] = f.tokens[stateKey]
		f.rawTokens[sessionID] = f.rawTokens[stateKey]
		f.lastAccess[sessionID] = f.lastAccess[stateKey]
		f.loadedFromDB[sessionID] = true
		f.stateFoundDB[sessionID] = f.stateFoundDB[stateKey]
	}
}

// MigrateLegacyState 一次性把旧裸 session key 的 Frozen 快照复制到明确的 epoch key。
// 迁移后主管线只访问 stateKey；旧键仅作为兼容性观察镜像，不再作为当前读取键。
// 返回 target key 的读取失败事实：读取失败时既不迁移也不回退 legacy key。
func (f *FrozenStubs) MigrateLegacyState(logger *slog.Logger, sessionID, stateKey string) StateLoadFailure {
	if f == nil || sessionID == "" || stateKey == "" || sessionID == stateKey {
		return StateLoadFailure{}
	}
	if logger == nil {
		logger = slog.Default()
	}
	// 目标 key 永远优先。即使持久化内容无效并已 fail-closed 删除，也不能
	// 再退回裸 key，否则旧状态可能在显式 epoch 状态之后重新进入主管线。
	if failure := f.loadFrozenFromDB(logger, stateKey); failure.Failed {
		// 读取失败不是"目标状态不存在"：此时读 legacy 会破坏 epoch 隔离。
		return failure
	}
	f.mu.RLock()
	_, targetExists := f.messages[stateKey]
	targetFound := f.stateFoundDB[stateKey]
	f.mu.RUnlock()
	if targetExists || targetFound {
		return StateLoadFailure{}
	}

	f.loadFrozenFromDB(logger, sessionID)
	f.mu.RLock()
	legacy, exists := f.messages[sessionID]
	if !exists {
		f.mu.RUnlock()
		return StateLoadFailure{}
	}
	messages := deepCopyMessages(legacy)
	cutoff := f.cutoff[sessionID]
	boundaryHash := f.boundaryHash[sessionID]
	prefixHash := f.prefixHash[sessionID]
	rawPrefixHash := f.rawPrefixHash[sessionID]
	rawPrefixMode := f.rawPrefixMode[sessionID]
	stubTime := f.stubTime[sessionID]
	tokens := f.tokens[sessionID]
	rawTokens := f.rawTokens[sessionID]
	lastAccess := f.lastAccess[sessionID]
	f.mu.RUnlock()
	if messages == nil {
		return StateLoadFailure{}
	}
	persisted, err := json.Marshal(frozenPersisted{
		Messages: messages, Cutoff: cutoff, BoundaryHash: boundaryHash,
		PrefixHash: prefixHash, RawPrefixHash: rawPrefixHash, RawPrefixMode: rawPrefixMode,
		Tokens: tokens, RawTokens: rawTokens,
	})
	if err != nil {
		return StateLoadFailure{}
	}

	persistLock := f.frozenPersistLock(stateKey)
	persistLock.Lock()
	defer persistLock.Unlock()

	f.mu.Lock()
	if _, exists := f.messages[stateKey]; exists || f.stateFoundDB[stateKey] {
		f.mu.Unlock()
		return StateLoadFailure{}
	}
	f.messages[stateKey] = messages
	f.cutoff[stateKey] = cutoff
	f.boundaryHash[stateKey] = boundaryHash
	f.prefixHash[stateKey] = prefixHash
	f.rawPrefixHash[stateKey] = rawPrefixHash
	f.rawPrefixMode[stateKey] = rawPrefixMode
	f.stubTime[stateKey] = stubTime
	f.tokens[stateKey] = tokens
	f.rawTokens[stateKey] = rawTokens
	f.lastAccess[stateKey] = lastAccess
	f.loadedFromDB[stateKey] = true
	submitter, persistFn := f.submitter, f.persistFn
	if submitter != nil || persistFn != nil {
		f.stateFoundDB[stateKey] = true
	}
	f.mu.Unlock()

	// 外部提交在组件锁之外完成；同 key 的顺序由 persistLock 保证。
	f.submitFrozenState(submitter, persistFn, stateKey, string(persisted), nil)
	return StateLoadFailure{}
}

// SetPersistFunc 设置持久化 frozen 状态到 DB 的回调函数。
func (f *FrozenStubs) SetPersistFunc(fn PersistFunc) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.persistFn = fn
}

// SetLoadFunc 是 bool-only 加载回调的兼容入口，只供尚未迁移的测试使用。
// 它把回调包装成永远不产生 Err 的 StateLoader；生产 wiring 必须用
// SetStateLoader，否则查询失败会被当成"状态不存在"。
func (f *FrozenStubs) SetLoadFunc(fn LoadFunc) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.loadFn = fn
	f.stateLoader = stateLoaderFromLoadFunc(fn)
}

// SetStateLoader 设置生产使用的 error-aware 状态读取合同。
func (f *FrozenStubs) SetStateLoader(loader StateLoader) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stateLoader = loader
}

// SetStateSubmitter 设置锁外 result-bearing 写入提交器。
func (f *FrozenStubs) SetStateSubmitter(submitter StateSubmitter) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.submitter = submitter
}

// frozenPersistLock 让同一 stateKey 的"内存更新 + 提交"整体串行，
// 从而在不持有组件 mutex 的前提下保持磁盘顺序与内存顺序一致。
func (f *FrozenStubs) frozenPersistLock(stateKey string) *sync.Mutex {
	f.mu.Lock()
	lock := f.persistLocks[stateKey]
	if lock == nil {
		lock = &sync.Mutex{}
		f.persistLocks[stateKey] = lock
	}
	f.mu.Unlock()
	return lock
}

// submitFrozenState 与 submitFrozenDelete 是仅有的两个外部写入口，
// 两者都必须在 f.mu 之外调用。
func (f *FrozenStubs) submitFrozenState(submitter StateSubmitter, persistFn PersistFunc, stateKey, value string, completion *outcomeCompletion) PersistenceReceipt {
	key := "frozen:" + stateKey
	var legacy func()
	if persistFn != nil {
		legacy = func() { persistFn(key, value) }
	}
	return submitStateOp(submitter, legacy, PersistenceOp{
		Kind: PersistenceOpPut, OrderingKey: key, Key: key, Value: value, Completion: completion,
	})
}

func (f *FrozenStubs) submitFrozenDelete(submitter StateSubmitter, deleteFn DeleteFunc, stateKey string, completion *outcomeCompletion) PersistenceReceipt {
	key := "frozen:" + stateKey
	var legacy func()
	if deleteFn != nil {
		legacy = func() { deleteFn(key) }
	}
	return submitStateOp(submitter, legacy, PersistenceOp{
		Kind: PersistenceOpDelete, OrderingKey: key, Key: key, Completion: completion,
	})
}

// SetDeleteFunc 设置 frozen 状态失效时的持久化删除回调。
func (f *FrozenStubs) SetDeleteFunc(fn DeleteFunc) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleteFn = fn
}

// Store 冻结指定 thread 的 detached historical prefix。
// 深拷贝消息并计算 boundary/prefix hash 用于验证。
// cutoff 是 detached history 总数（第一条未桩化历史消息的索引）。
// boundaryMsg 是 historicalMessages[cutoff-1]，用于 boundary 验证。
func (f *FrozenStubs) Store(threadID string, stubbed []Message, cutoff int, boundaryMsg Message, tokenEstimate int, rawTokenEstimate int, rawHistory ...[]Message) {
	f.StoreWithLogger(slog.Default(), threadID, stubbed, cutoff, boundaryMsg, tokenEstimate, rawTokenEstimate, rawHistory...)
}

func (f *FrozenStubs) StoreWithLogger(logger *slog.Logger, threadID string, stubbed []Message, cutoff int, boundaryMsg Message, tokenEstimate int, rawTokenEstimate int, rawHistory ...[]Message) {
	f.StoreWithResult(logger, threadID, stubbed, cutoff, boundaryMsg, tokenEstimate, rawTokenEstimate, nil, rawHistory...)
}

// StoreWithResult 是 result-bearing 的存储入口：内存与快照在组件锁内更新，
// 外部提交在锁外完成，磁盘结果通过 completion 回到调用方的 request outcome。
func (f *FrozenStubs) StoreWithResult(logger *slog.Logger, threadID string, stubbed []Message, cutoff int, boundaryMsg Message, tokenEstimate int, rawTokenEstimate int, completion *outcomeCompletion, rawHistory ...[]Message) PersistenceReceipt {
	if logger == nil {
		logger = slog.Default()
	}
	if cutoff <= 0 || tokenEstimate < 0 || rawTokenEstimate < 0 {
		logger.Debug("frozen 状态未存储", "reason", "metadata_invalid")
		return completeStateOp(completion, persistenceStateNotAttempted, persistenceFailureNone)
	}
	if ExtractPersistentUserContext(stubbed) != nil {
		logger.Debug("frozen 状态未存储", "reason", "persistent_context")
		return completeStateOp(completion, persistenceStateNotAttempted, persistenceFailureNone)
	}
	frozen := deepCopyMessages(stubbed)
	if frozen == nil {
		logger.Debug("frozen 状态未存储", "reason", "copy_failed")
		return completeStateOp(completion, persistenceStateNotAttempted, persistenceFailureNone)
	}

	frozenJSON, _ := json.Marshal(frozen)
	pHash := sha256hex(frozenJSON)

	bHash := stableBoundaryHash(boundaryMsg)
	rawHash := legacyRawPrefixHash(cutoff, boundaryMsg)
	rawMode := frozenRawPrefixModeLegacy
	if len(rawHistory) > 0 {
		rawHash = reuseSafetyPrefixHash(rawHistory[0], cutoff)
		if rawHash == "" {
			logger.Debug("frozen 状态未存储", "reason", "raw_prefix_unavailable")
			return completeStateOp(completion, persistenceStateNotAttempted, persistenceFailureNone)
		}
		rawMode = frozenRawPrefixModeFull
	}

	now := time.Now()
	fp := frozenPersisted{
		Messages: frozen, Cutoff: cutoff, BoundaryHash: bHash, PrefixHash: pHash, RawPrefixHash: rawHash, RawPrefixMode: rawMode,
		Tokens: tokenEstimate, RawTokens: rawTokenEstimate,
	}
	persisted, _ := json.Marshal(fp)

	persistLock := f.frozenPersistLock(threadID)
	persistLock.Lock()
	defer persistLock.Unlock()

	f.mu.Lock()
	f.messages[threadID] = frozen
	f.cutoff[threadID] = cutoff
	f.boundaryHash[threadID] = bHash
	f.prefixHash[threadID] = pHash
	f.rawPrefixHash[threadID] = rawHash
	f.rawPrefixMode[threadID] = rawMode
	f.stubTime[threadID] = now
	f.tokens[threadID] = tokenEstimate
	f.rawTokens[threadID] = rawTokenEstimate
	f.lastAccess[threadID] = now
	f.loadedFromDB[threadID] = true // 内存中的是最新权威数据
	submitter, persistFn := f.submitter, f.persistFn
	if submitter != nil || persistFn != nil {
		f.stateFoundDB[threadID] = true
	}
	f.mirrorCurrentAliasesLocked(threadID)
	f.mu.Unlock()

	// 内存已落定，磁盘结果仍由 completion 异步补齐，两个维度自此分离。
	completion.noteMemorySaved()

	// 提交在组件锁外执行；persistLock 保证同一 thread 的内存与磁盘顺序一致。
	if persisted == nil {
		return completeStateOp(completion, persistenceStateNotAttempted, persistenceFailureNone)
	}
	receipt := f.submitFrozenState(submitter, persistFn, threadID, string(persisted), completion)

	logger.Info("frozen prefix 已存储（frozen 消息估算）", "cutoff", cutoff, "tokens", tokenEstimate)
	return receipt
}

// Get 返回指定 thread 的已验证 frozen stubs（若存在且有效）。
// 验证：(1) 当前消息数不小于 cutoff；
//
//	(2) frozen prefix 在内存中未被意外修改（SHA-256 hash 验证）。
func (f *FrozenStubs) Get(threadID string, currentMessages []Message) *FrozenResult {
	return f.GetWithLogger(slog.Default(), threadID, currentMessages)
}

func (f *FrozenStubs) GetWithLogger(logger *slog.Logger, threadID string, currentMessages []Message) *FrozenResult {
	result, _ := f.GetWithLoadResult(logger, threadID, currentMessages)
	return result
}

// GetWithLoadResult 在读取失败时返回 nil frozen prefix 与稳定失败事实：
// 查询错误既不会被当成"没有 frozen 状态"，也不会让本请求复用任何旧前缀。
func (f *FrozenStubs) GetWithLoadResult(logger *slog.Logger, threadID string, currentMessages []Message) (*FrozenResult, StateLoadFailure) {
	if logger == nil {
		logger = slog.Default()
	}
	f.mu.RLock()
	stateKey := f.resolveStateKeyLocked(threadID)
	_, ok := f.messages[stateKey]
	loaded := f.loadedFromDB[stateKey]
	f.mu.RUnlock()

	// 冷启动 lazy-load：首次访问时尝试从 DB 恢复
	if !ok && !loaded {
		if failure := f.loadFrozenFromDB(logger, stateKey); failure.Failed {
			logger.Debug("frozen prefix 未命中", "reason", "state_load_failed")
			return nil, failure
		}
	}

	f.mu.RLock()
	stateKey = f.resolveStateKeyLocked(threadID)
	msgs, ok := f.messages[stateKey]
	if !ok {
		f.mu.RUnlock()
		logger.Debug("frozen prefix 未命中", "reason", "not_found")
		return nil, StateLoadFailure{}
	}
	cutoff := f.cutoff[stateKey]
	pHash := f.prefixHash[stateKey]
	bHash := f.boundaryHash[stateKey]
	rawHash := f.rawPrefixHash[stateKey]
	rawMode := f.rawPrefixMode[stateKey]
	tokens := f.tokens[stateKey]
	rawTokens := f.rawTokens[stateKey]
	f.mu.RUnlock()

	// 验证 1：持久化元数据与当前消息边界必须可安全切片。
	// 有状态却未命中的各验证分支升 Info：这是排查"frozen 只存不命中"
	// 反复折叠的关键信号，not_found（从未存过）才是常态噪音。
	if cutoff <= 0 || cutoff > len(currentMessages) || bHash == "" || !validPressureFingerprint(rawHash) || (rawMode != frozenRawPrefixModeFull && rawMode != frozenRawPrefixModeLegacy) || tokens < 0 || rawTokens < 0 {
		logger.Info("frozen prefix 未命中", "reason", "metadata_invalid", "cutoff", cutoff, "raw_prefix_mode", rawMode, "message_count", len(currentMessages))
		f.InvalidateWithLogger(logger, stateKey)
		return nil, StateLoadFailure{}
	}

	// 验证 2：prefix hash 不匹配（内存被意外修改）
	frozenJSON, _ := json.Marshal(msgs)
	if sha256hex(frozenJSON) != pHash {
		logger.Info("frozen prefix 未命中", "reason", "stored_prefix_changed", "cutoff", cutoff, "frozen_messages", len(msgs))
		f.InvalidateWithLogger(logger, stateKey)
		return nil, StateLoadFailure{}
	}

	// 验证 3：完整 raw cutoff 前缀 reuse-safety 证明。
	// 单条 boundary 相同不足以证明 cutoff 前的早期消息仍连续。
	if rawMode == frozenRawPrefixModeFull {
		if currentHash := reuseSafetyPrefixHash(currentMessages, cutoff); currentHash == "" || currentHash != rawHash {
			logger.Info("frozen prefix 未命中", "reason", "raw_prefix_changed", "cutoff", cutoff, "message_count", len(currentMessages))
			f.InvalidateWithLogger(logger, stateKey)
			return nil, StateLoadFailure{}
		}
	} else if legacyRawPrefixHash(cutoff, currentMessages[cutoff-1]) != rawHash {
		logger.Info("frozen prefix 未命中", "reason", "boundary_changed", "cutoff", cutoff, "raw_prefix_mode", rawMode)
		f.InvalidateWithLogger(logger, stateKey)
		return nil, StateLoadFailure{}
	}

	// 验证 4：保留 boundary 双坐标检查，便于检测元数据损坏。
	// sawtooth-proxy 在 Get 之前运行 StripReminders，CC 注入不会误触发
	if cutoff > 0 && cutoff <= len(currentMessages) {
		currentBHash := stableBoundaryHash(currentMessages[cutoff-1])
		if currentBHash != bHash {
			logger.Info("frozen prefix 未命中", "reason", "boundary_changed", "cutoff", cutoff, "raw_prefix_mode", rawMode)
			f.InvalidateWithLogger(logger, stateKey)
			return nil, StateLoadFailure{}
		}
	}

	// 深拷贝——防止下游（cache_control inject 等）原地修改 frozen 数据
	copied := deepCopyMessages(msgs)
	if copied == nil {
		logger.Info("frozen prefix 未命中", "reason", "copy_failed", "cutoff", cutoff, "frozen_messages", len(msgs))
		f.InvalidateWithLogger(logger, stateKey)
		return nil, StateLoadFailure{}
	}

	// 更新最后访问时间
	f.mu.Lock()
	f.lastAccess[stateKey] = time.Now()
	f.mu.Unlock()

	logger.Info("frozen prefix 命中", "cutoff", cutoff, "frozen_tokens", tokens)

	return &FrozenResult{
		Messages:  copied,
		Cutoff:    cutoff,
		Tokens:    tokens,
		RawTokens: rawTokens,
	}, StateLoadFailure{}
}

// LengthFor 返回 threadID 对应的 frozen prefix 消息数；条目不存在时返回 0。
func (f *FrozenStubs) LengthFor(threadID string) int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return len(f.messages[f.resolveStateKeyLocked(threadID)])
}

// UpdateMessages 用 newMsgs 覆盖已存储的 frozen prefix 并刷新 prefix hash。
// 要求 newMsgs 长度与已有条目一致（防止同一管线内二次 sawtooth 触发）。
// 返回是否成功更新。
func (f *FrozenStubs) UpdateMessages(threadID string, newMsgs []Message) bool {
	updated, _ := f.UpdateMessagesWithResult(threadID, newMsgs, nil)
	return updated
}

// UpdateMessagesWithResult 是 result-bearing 的覆盖入口：内存更新在锁内，
// 外部提交在锁外，磁盘结果通过 completion 回到调用方。
func (f *FrozenStubs) UpdateMessagesWithResult(threadID string, newMsgs []Message, completion *outcomeCompletion) (bool, PersistenceReceipt) {
	if ExtractPersistentUserContext(newMsgs) != nil {
		return false, completeStateOp(completion, persistenceStateNotAttempted, persistenceFailureNone)
	}
	fresh := deepCopyMessages(newMsgs)
	if fresh == nil {
		return false, completeStateOp(completion, persistenceStateNotAttempted, persistenceFailureNone)
	}

	freshJSON, _ := json.Marshal(fresh)
	pHash := sha256hex(freshJSON)

	f.mu.RLock()
	lockKey := f.resolveStateKeyLocked(threadID)
	f.mu.RUnlock()
	persistLock := f.frozenPersistLock(lockKey)
	persistLock.Lock()
	defer persistLock.Unlock()

	f.mu.Lock()
	stateKey := f.resolveStateKeyLocked(threadID)
	existing, ok := f.messages[stateKey]
	if !ok || len(fresh) != len(existing) {
		f.mu.Unlock()
		return false, completeStateOp(completion, persistenceStateNotAttempted, persistenceFailureNone)
	}
	f.messages[stateKey] = fresh
	f.prefixHash[stateKey] = pHash
	f.lastAccess[stateKey] = time.Now()
	f.mirrorCurrentAliasesLocked(stateKey)
	cutoff := f.cutoff[stateKey]
	bHash := f.boundaryHash[stateKey]
	rawHash := f.rawPrefixHash[stateKey]
	rawMode := f.rawPrefixMode[stateKey]
	tokens := f.tokens[stateKey]
	rawTokens := f.rawTokens[stateKey]
	submitter, persistFn := f.submitter, f.persistFn
	f.mu.Unlock()

	if submitter == nil && persistFn == nil {
		return true, completeStateOp(completion, persistenceStateNotAttempted, persistenceFailureNone)
	}
	data, err := json.Marshal(frozenPersisted{
		Messages:      fresh,
		Cutoff:        cutoff,
		BoundaryHash:  bHash,
		PrefixHash:    pHash,
		RawPrefixHash: rawHash,
		RawPrefixMode: rawMode,
		Tokens:        tokens,
		RawTokens:     rawTokens,
	})
	if err != nil {
		return true, completeStateOp(completion, persistenceStateNotAttempted, persistenceFailureNone)
	}
	return true, f.submitFrozenState(submitter, persistFn, stateKey, string(data), completion)
}

// Invalidate 删除指定 thread 的 frozen stubs。
func (f *FrozenStubs) Invalidate(threadID string) {
	f.InvalidateWithLogger(slog.Default(), threadID)
}

func (f *FrozenStubs) InvalidateWithLogger(logger *slog.Logger, threadID string) {
	f.InvalidateWithResult(logger, threadID, nil)
}

// InvalidateWithResult 是 result-bearing 的失效入口：删除同样锁外提交。
func (f *FrozenStubs) InvalidateWithResult(logger *slog.Logger, threadID string, completion *outcomeCompletion) PersistenceReceipt {
	if logger == nil {
		logger = slog.Default()
	}
	f.mu.RLock()
	lockKey := f.resolveStateKeyLocked(threadID)
	f.mu.RUnlock()
	// 与 StoreWithResult 使用同一把 persistLock，保证 put/delete 的提交顺序
	// 与内存顺序一致；加锁顺序始终是 persistLock → f.mu。
	persistLock := f.frozenPersistLock(lockKey)
	persistLock.Lock()
	defer persistLock.Unlock()

	f.mu.Lock()
	stateKey := f.resolveStateKeyLocked(threadID)
	delete(f.messages, stateKey)
	delete(f.cutoff, stateKey)
	delete(f.boundaryHash, stateKey)
	delete(f.prefixHash, stateKey)
	delete(f.rawPrefixHash, stateKey)
	delete(f.rawPrefixMode, stateKey)
	delete(f.stubTime, stateKey)
	delete(f.tokens, stateKey)
	delete(f.rawTokens, stateKey)
	delete(f.lastAccess, stateKey)
	for sessionID, alias := range f.currentAlias {
		if alias == stateKey {
			delete(f.messages, sessionID)
			delete(f.cutoff, sessionID)
			delete(f.boundaryHash, sessionID)
			delete(f.prefixHash, sessionID)
			delete(f.rawPrefixHash, sessionID)
			delete(f.rawPrefixMode, sessionID)
			delete(f.stubTime, sessionID)
			delete(f.tokens, sessionID)
			delete(f.rawTokens, sessionID)
			delete(f.lastAccess, sessionID)
		}
	}
	// 已知坏状态失效后保留“本进程已加载”标记，防止删除失败时反复恢复。
	f.loadedFromDB[stateKey] = true
	submitter, deleteFn := f.submitter, f.deleteFn
	f.mu.Unlock()

	// 失效同样是一次成功的内存状态更新，按 D-11 与写入路径同权。
	completion.noteMemorySaved()

	logger.Debug("frozen prefix 已失效", "reason", "state_invalidated")
	if submitter == nil && deleteFn == nil {
		return completeStateOp(completion, persistenceStateNotAttempted, persistenceFailureNone)
	}
	return f.submitFrozenDelete(submitter, deleteFn, stateKey, completion)
}

// UpdateTTL 动态更新 FrozenStubs 的 eviction TTL。
// 用于 Cache TTL 自适应：检测到 1h 断点时升至 65min，默认 ephemeral 保持 30min。
func (f *FrozenStubs) UpdateTTL(ttl time.Duration) {
	f.mu.Lock()
	f.ttl = ttl
	f.mu.Unlock()
}

// Evict 清理超过 TTL 未访问的条目，返回清理数量。
func (f *FrozenStubs) Evict() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	cutoff := time.Now().Add(-f.ttl)
	evicted := 0
	for tid, t := range f.lastAccess {
		if t.Before(cutoff) {
			delete(f.messages, tid)
			delete(f.cutoff, tid)
			delete(f.boundaryHash, tid)
			delete(f.prefixHash, tid)
			delete(f.rawPrefixHash, tid)
			delete(f.rawPrefixMode, tid)
			delete(f.stubTime, tid)
			delete(f.tokens, tid)
			delete(f.rawTokens, tid)
			delete(f.lastAccess, tid)
			delete(f.loadedFromDB, tid)
			delete(f.stateFoundDB, tid)
			delete(f.loadFailures, tid)
			evicted++
		}
	}
	if evicted > 0 {
		slog.Info("frozen stubs eviction 完成", "evicted", evicted)
	}
	return evicted
}

// loadFrozenFromDB 尝试从 DB 恢复指定 thread 的 frozen stubs。
// 每个 thread 在冷启动时仅调用一次（由 Get 触发 lazy-load）：并发调用方等待
// 已在进行的加载，不重复查询，并共享同一次读取结果。比照同文件 loadSawtoothFromDB。
// 只有 ErrNoRows（Found=false 且 Err=nil）才算 missing；读取失败不设置任何
// 已加载标记，允许下一次请求重试。
func (f *FrozenStubs) loadFrozenFromDB(logger *slog.Logger, threadID string) (failure StateLoadFailure) {
	f.mu.Lock()
	stateKey := f.resolveStateKeyLocked(threadID)
	if f.loadedFromDB[stateKey] {
		f.mu.Unlock()
		return StateLoadFailure{}
	}
	if loading := f.loadingFromDB[stateKey]; loading != nil {
		f.mu.Unlock()
		<-loading
		f.mu.RLock()
		shared := f.loadFailures[stateKey]
		f.mu.RUnlock()
		return shared
	}
	loading := make(chan struct{})
	f.loadingFromDB[stateKey] = loading
	loader := f.stateLoader
	f.mu.Unlock()

	// 本函数有多个出口（含三处 InvalidateWithLogger 分支），清理只能用 defer。
	// 只碰 loadingFromDB 与共享失败事实：loadedFromDB 由各分支在读取结果已知
	// 之后自行置位（WR-03 特意后移的），InvalidateWithLogger 还会刻意把它置
	// true，在这里覆盖会推翻那条设计。
	defer func() {
		f.mu.Lock()
		delete(f.loadingFromDB, stateKey)
		if failure.Failed {
			f.loadFailures[stateKey] = failure
		} else {
			delete(f.loadFailures, stateKey)
		}
		f.mu.Unlock()
		close(loading)
	}()

	if loader == nil {
		f.mu.Lock()
		f.loadedFromDB[stateKey] = true
		f.mu.Unlock()
		return StateLoadFailure{}
	}

	result := loader.LoadStateResult("frozen:" + stateKey)
	if result.Err != nil {
		// 查询失败不是"状态不存在"：不设置 loaded/found，也不写入任何状态。
		return stateLoadFailureFrom(result)
	}
	if !result.Found || result.Value == "" {
		f.mu.Lock()
		f.loadedFromDB[stateKey] = true
		f.mu.Unlock()
		return StateLoadFailure{}
	}
	f.mu.Lock()
	f.stateFoundDB[stateKey] = true
	f.loadedFromDB[stateKey] = true
	f.mu.Unlock()

	var fp frozenPersisted
	if err := json.Unmarshal([]byte(result.Value), &fp); err != nil {
		f.InvalidateWithLogger(logger, stateKey)
		return StateLoadFailure{}
	}
	if len(fp.Messages) == 0 || fp.Cutoff <= 0 || fp.BoundaryHash == "" || fp.PrefixHash == "" || !validPressureFingerprint(fp.RawPrefixHash) || (fp.RawPrefixMode != frozenRawPrefixModeFull && fp.RawPrefixMode != frozenRawPrefixModeLegacy) || fp.Tokens < 0 || fp.RawTokens < 0 || ExtractPersistentUserContext(fp.Messages) != nil {
		f.InvalidateWithLogger(logger, stateKey)
		return StateLoadFailure{}
	}

	// 验证 prefix hash 与存储的消息一致
	frozenJSON, _ := json.Marshal(fp.Messages)
	if sha256hex(frozenJSON) != fp.PrefixHash {
		logger.Debug("frozen prefix 未命中", "reason", "stored_prefix_changed")
		f.InvalidateWithLogger(logger, threadID)
		return StateLoadFailure{}
	}

	now := time.Now()
	f.mu.Lock()
	defer f.mu.Unlock()
	// 仅在仍为空时写入（Store() 可能在并发中被调用）
	if _, exists := f.messages[stateKey]; exists {
		return StateLoadFailure{}
	}
	f.messages[stateKey] = fp.Messages
	f.cutoff[stateKey] = fp.Cutoff
	f.boundaryHash[stateKey] = fp.BoundaryHash
	f.prefixHash[stateKey] = fp.PrefixHash
	f.rawPrefixHash[stateKey] = fp.RawPrefixHash
	f.rawPrefixMode[stateKey] = fp.RawPrefixMode
	f.tokens[stateKey] = fp.Tokens
	f.rawTokens[stateKey] = fp.RawTokens
	f.stubTime[stateKey] = now
	f.lastAccess[stateKey] = now
	f.mirrorCurrentAliasesLocked(stateKey)

	logger.Info("从 SQLite 恢复 frozen 状态", "cutoff", fp.Cutoff, "tokens", fp.Tokens)
	return StateLoadFailure{}
}

// deepCopyMessages 通过 JSON round-trip 创建消息切片的深拷贝。
func deepCopyMessages(msgs []Message) []Message {
	data, err := json.Marshal(msgs)
	if err != nil {
		return nil
	}
	var out []Message
	if json.Unmarshal(data, &out) != nil {
		return nil
	}
	return out
}

// stableBoundaryHash 对 StripReminders 后的完整 boundary message 计算规范 JSON hash。
func stableBoundaryHash(msg Message) string {
	canonicalMessage, err := canonicalMessageForHash(msg, normalizeBoundaryContent)
	if err != nil {
		data, _ := json.Marshal(msg)
		return sha256hex(data)
	}
	canonical, _ := json.Marshal(canonicalMessage)
	return sha256hex(canonical)
}

// legacyRawPrefixHash 为没有传入 raw history 的旧内存 API 保留受限兼容证明。
// 它只绑定 cutoff 与 boundary，生产主管线始终传入完整 raw history 并使用 full 模式。
func legacyRawPrefixHash(cutoff int, boundary Message) string {
	payload := struct {
		Cutoff   int    `json:"cutoff"`
		Boundary string `json:"boundary"`
	}{Cutoff: cutoff, Boundary: stableBoundaryHash(boundary)}
	data, _ := json.Marshal(payload)
	return sha256hex(data)
}

// canonicalMessageForHash 将完整消息解码为可确定性序列化的 JSON 对象。
// normalizeContent 只能修改 content；未知消息级字段的 absent/null/value 状态
// 全部保留并参与完整性指纹。
func canonicalMessageForHash(msg Message, normalizeContent func(any) any) (map[string]any, error) {
	data, err := json.Marshal(msg)
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var canonical map[string]any
	if err := decoder.Decode(&canonical); err != nil {
		return nil, err
	}
	if content, ok := canonical["content"]; ok && normalizeContent != nil {
		canonical["content"] = normalizeContent(content)
	}
	return canonical, nil
}

// normalizeBoundaryContent 只消除两类已确证的非语义差异：text/thinking/tool_result
// 块本身不使用 input，但 ContentBlock.Input 缺少 omitempty 会把省略字段重编码为
// input:null；已知 Anthropic content block 的直接 cache_control 是缓存元数据，
// 与 normalizeHistoryContent 的 reuse 规范化同源。未知 block 类型的 cache_control、
// 未知字段、数组 null 和 tool_use 的 input 均保持原样，避免把未来协议中的显式
// null 与 absent 混同。
func normalizeBoundaryContent(content any) any {
	blocks, ok := content.([]any)
	if !ok {
		return content
	}
	for _, block := range blocks {
		object, ok := block.(map[string]any)
		if !ok {
			continue
		}
		typeName, _ := object["type"].(string)
		if knownHistoryContentBlockType(typeName) {
			delete(object, "cache_control")
		}
		if object["input"] == nil {
			switch typeName {
			case "text", "thinking", "tool_result":
				delete(object, "input")
			}
		}
	}
	return blocks
}

// sha256hex 返回 data 的十六进制编码 SHA-256 hash。
func sha256hex(data []byte) string {
	h := sha256.Sum256(data)
	return fmt.Sprintf("%x", h[:])
}

// ==== SawtoothTrigger ====

// TriggerReason 表示桩化周期触发的原因。
type TriggerReason string

const (
	TriggerNone      TriggerReason = ""          // 无需触发
	TriggerTokens    TriggerReason = "tokens"    // 超过 token 阈值
	TriggerPause     TriggerReason = "pause"     // 暂停超时 + token 足够
	TriggerEmergency TriggerReason = "emergency" // 原始估算超过紧急阈值
)

// TriggerEvaluation 是一次 Sawtooth 触发判定使用的不可变事实快照。
// reason、等待时间与压力阈值均在同一次读锁内取得，后续 TTL 更新不会改写它。
type TriggerEvaluation struct {
	Reason             TriggerReason
	RequiredWait       time.Duration
	ActualWait         time.Duration
	ActualWaitKnown    bool
	SelectedPressure   int
	EmergencyThreshold int
	TokenThreshold     int
	TokenMinimum       int
}

// persistedState 是持久化到 proxy_state 的 JSON 结构。
type persistedState struct {
	Tokens                    int    `json:"tokens"`
	MsgCount                  int    `json:"msg_count"`
	SystemFingerprint         string `json:"system_fingerprint,omitempty"`
	ToolsFingerprint          string `json:"tools_fingerprint,omitempty"`
	MessagesPrefixFingerprint string `json:"messages_prefix_fingerprint,omitempty"`
	Conservative              bool   `json:"conservative,omitempty"`
}

// baselineResetReason 表示 pressure baseline 不能沿用时的受限原因。
// 状态层只暴露事实，不在此处决定是否执行 Collapse。
type baselineResetReason string

const (
	baselineResetNone             baselineResetReason = "none"
	baselineResetNoActual         baselineResetReason = "no_actual"
	baselineResetStateLoadFailed  baselineResetReason = "state_load_failed"
	baselineResetLegacyUnverified baselineResetReason = "legacy_unverified"
	baselineResetMessageShrink    baselineResetReason = "message_shrink"
	baselineResetMessagesChanged  baselineResetReason = "messages_changed"
	baselineResetSystemChanged    baselineResetReason = "system_changed"
	baselineResetToolsChanged     baselineResetReason = "tools_changed"
)

// pressureBaseline 是后续 pressure 决策所需的单次原子快照。
// 其中只包含数字、受限枚举和固定长度 SHA-256 十六进制指纹。
type pressureBaseline struct {
	ActualTokens              int
	MessageCount              int
	SystemFingerprint         string
	ToolsFingerprint          string
	MessagesPrefixFingerprint string
	Conservative              bool
	Available                 bool
	ResetReason               baselineResetReason
}

// 校准系数的滚动实测口径：本地估算 → 真实计费 token 的转换率。
// 样本只存内存（受 st.mu 保护），不放入 persistedState——ResetPressureBaseline
// 的 epoch 清零不波及它（校准比率与会话历史无关），进程重启由冷启动常数兜底。
const (
	// defaultCalibrationRatio 是冷启动/样本不足时的常数：真实会话 101 个
	// 非折叠请求的中位实测值（本地估算系统性低估约 1.5 倍）。
	defaultCalibrationRatio = 1.50
	// calibrationSampleWindow 是每个 stateKey 保留的 (actual, estimate) 比值数。
	calibrationSampleWindow = 8
	// calibrationMinRatio / calibrationMaxRatio 钳制滚动中位数的漂移范围。
	// 下限必须允许显著 <1：加权字符估算器对文本密集 wire 可能系统性高估
	// （折叠后 wire 实测 0.5~0.7），只允许放大（旧下限 1.10）会把高估永久
	// 钉死，令 local_full 兜底轮每轮误报。
	calibrationMinRatio = 0.50
	calibrationMaxRatio = 1.80
)

// SawtoothTrigger 根据 token 使用量和时间判断是否执行桩化周期。
type SawtoothTrigger struct {
	mu                          sync.RWMutex
	calibrationSamples          map[string][]float64        // stateKey → 最近 N 个 actual/estimate 比值
	lastTotalTokens             map[string]int              // threadID → 上次 API 响应 input tokens
	lastMessageCount            map[string]int              // threadID → 上次响应时的消息数
	systemFingerprints          map[string]string           // threadID → 上次主请求 system 的 SHA-256 指纹
	toolsFingerprints           map[string]string           // threadID → 上次主请求 tools 的 SHA-256 指纹
	messagesPrefixFingerprints  map[string]string           // threadID → 上次主请求消息前缀的 SHA-256 指纹
	conservativeBaselines       map[string]bool             // threadID → actual 仅作为不得压低的 pressure floor
	lastRequestTime             map[string]time.Time        // threadID → 上次 API 响应时间
	loadedFromDB                map[string]bool             // threadID → DB 加载已完成
	stateFoundDB                map[string]bool             // threadID → DB 中曾存在显式状态（即使内容无效）
	loadingFromDB               map[string]chan struct{}    // threadID → 正在进行的 DB 加载完成信号
	loadFailures                map[string]StateLoadFailure // threadID → 最近一次读取失败事实
	baselineGeneration          map[string]uint64           // threadID → 响应写回版本，防止慢加载覆盖新状态
	baselineRequestGeneration   map[string]uint64           // threadID → 请求进入有状态主管线时分配的代际
	baselineCommittedGeneration map[string]uint64           // threadID → 最近接受的响应请求代际
	baselinePersistLocks        map[string]*sync.Mutex      // threadID → 锁外持久化串行锁
	requestSeq                  map[string]int              // threadID → 当前请求序号（Phase B: DecayTracker 用）
	currentAlias                map[string]string           // legacy session key → 当前 epoch state key
	pauseThreshold              time.Duration               // 暂停检测阈值（cache TTL - 安全边距）
	tokenThreshold              int                         // 超过此值触发桩化周期（来自配置）
	tokenMinimum                int                         // 桩化下限（来自配置）
	persistFn                   PersistFunc                 // 可选：更新时持久化 token 状态到 DB
	loadFn                      LoadFunc                    // 兼容：旧 bool-only 加载回调（仅测试）
	stateLoader                 StateLoader                 // 生产：error-aware 状态读取合同
	submitter                   StateSubmitter              // 生产：锁外 result-bearing 写入提交
}

// NewSawtoothTrigger 创建新的触发状态跟踪器。
func NewSawtoothTrigger(pauseThreshold time.Duration, tokenThreshold, tokenMinimum int) *SawtoothTrigger {
	return &SawtoothTrigger{
		calibrationSamples:          make(map[string][]float64),
		lastTotalTokens:             make(map[string]int),
		lastMessageCount:            make(map[string]int),
		systemFingerprints:          make(map[string]string),
		toolsFingerprints:           make(map[string]string),
		messagesPrefixFingerprints:  make(map[string]string),
		conservativeBaselines:       make(map[string]bool),
		lastRequestTime:             make(map[string]time.Time),
		loadedFromDB:                make(map[string]bool),
		stateFoundDB:                make(map[string]bool),
		loadingFromDB:               make(map[string]chan struct{}),
		loadFailures:                make(map[string]StateLoadFailure),
		baselineGeneration:          make(map[string]uint64),
		baselineRequestGeneration:   make(map[string]uint64),
		baselineCommittedGeneration: make(map[string]uint64),
		baselinePersistLocks:        make(map[string]*sync.Mutex),
		requestSeq:                  make(map[string]int),
		currentAlias:                make(map[string]string),
		pauseThreshold:              pauseThreshold,
		tokenThreshold:              tokenThreshold,
		tokenMinimum:                tokenMinimum,
	}
}

// SetCurrentAlias 让旧的 session 级观察 API 指向当前 epoch key。
// 主管线始终传入显式 stateKey；alias 不会把旧 epoch key 重定向。
func (st *SawtoothTrigger) SetCurrentAlias(sessionID, stateKey string) {
	if st == nil || sessionID == "" || stateKey == "" {
		return
	}
	st.mu.Lock()
	st.currentAlias[sessionID] = stateKey
	st.mirrorCurrentAliasesLocked(stateKey)
	st.mu.Unlock()
}

func (st *SawtoothTrigger) resolveStateKeyLocked(threadID string) string {
	if alias := st.currentAlias[threadID]; alias != "" {
		return alias
	}
	return threadID
}

func (st *SawtoothTrigger) hasStateLocked(stateKey string) bool {
	_, hasTokens := st.lastTotalTokens[stateKey]
	_, hasTime := st.lastRequestTime[stateKey]
	_, hasSeq := st.requestSeq[stateKey]
	return hasTokens || hasTime || hasSeq || st.baselineGeneration[stateKey] != 0 ||
		st.baselineRequestGeneration[stateKey] != 0 || st.baselineCommittedGeneration[stateKey] != 0
}

func (st *SawtoothTrigger) copyStateLocked(sourceKey, targetKey string) {
	if value, ok := st.lastTotalTokens[sourceKey]; ok {
		st.lastTotalTokens[targetKey] = value
	} else {
		delete(st.lastTotalTokens, targetKey)
	}
	if value, ok := st.lastMessageCount[sourceKey]; ok {
		st.lastMessageCount[targetKey] = value
	} else {
		delete(st.lastMessageCount, targetKey)
	}
	if value, ok := st.systemFingerprints[sourceKey]; ok {
		st.systemFingerprints[targetKey] = value
	} else {
		delete(st.systemFingerprints, targetKey)
	}
	if value, ok := st.toolsFingerprints[sourceKey]; ok {
		st.toolsFingerprints[targetKey] = value
	} else {
		delete(st.toolsFingerprints, targetKey)
	}
	if value, ok := st.messagesPrefixFingerprints[sourceKey]; ok {
		st.messagesPrefixFingerprints[targetKey] = value
	} else {
		delete(st.messagesPrefixFingerprints, targetKey)
	}
	if value, ok := st.conservativeBaselines[sourceKey]; ok {
		st.conservativeBaselines[targetKey] = value
	} else {
		delete(st.conservativeBaselines, targetKey)
	}
	if value, ok := st.lastRequestTime[sourceKey]; ok {
		st.lastRequestTime[targetKey] = value
	} else {
		delete(st.lastRequestTime, targetKey)
	}
	if value, ok := st.baselineGeneration[sourceKey]; ok {
		st.baselineGeneration[targetKey] = value
	} else {
		delete(st.baselineGeneration, targetKey)
	}
	if value, ok := st.baselineRequestGeneration[sourceKey]; ok {
		st.baselineRequestGeneration[targetKey] = value
	} else {
		delete(st.baselineRequestGeneration, targetKey)
	}
	if value, ok := st.baselineCommittedGeneration[sourceKey]; ok {
		st.baselineCommittedGeneration[targetKey] = value
	} else {
		delete(st.baselineCommittedGeneration, targetKey)
	}
	if value, ok := st.requestSeq[sourceKey]; ok {
		st.requestSeq[targetKey] = value
	} else {
		delete(st.requestSeq, targetKey)
	}
}

func (st *SawtoothTrigger) mirrorCurrentAliasesLocked(stateKey string) {
	for sessionID, current := range st.currentAlias {
		if current != stateKey || sessionID == stateKey {
			continue
		}
		st.copyStateLocked(stateKey, sessionID)
		st.loadedFromDB[sessionID] = true
		st.stateFoundDB[sessionID] = st.stateFoundDB[stateKey]
	}
}

// MigrateLegacyState 一次性把旧裸 session key 的 Sawtooth 状态复制到 epoch 1 key。
// 若显式目标 key 已存在（包括无效后 fail-closed 的持久状态），绝不回退读取裸 key。
// 目标 key 读取失败时同样立即终止：查询错误不是"目标没有状态"。
func (st *SawtoothTrigger) MigrateLegacyState(sessionID, stateKey string) StateLoadFailure {
	if st == nil || sessionID == "" || stateKey == "" || sessionID == stateKey {
		return StateLoadFailure{}
	}
	if failure := st.loadSawtoothFromDB(stateKey); failure.Failed {
		return failure
	}
	st.mu.RLock()
	targetExists := st.hasStateLocked(stateKey)
	targetFound := st.stateFoundDB[stateKey]
	st.mu.RUnlock()
	if targetExists || targetFound {
		return StateLoadFailure{}
	}

	st.loadSawtoothFromDB(sessionID)
	persistLock := st.pressurePersistLock(stateKey)
	persistLock.Lock()
	defer persistLock.Unlock()

	st.mu.Lock()
	if st.hasStateLocked(stateKey) || st.stateFoundDB[stateKey] || !st.hasStateLocked(sessionID) {
		st.mu.Unlock()
		return StateLoadFailure{}
	}
	st.copyStateLocked(sessionID, stateKey)
	st.loadedFromDB[stateKey] = true
	state := persistedState{
		Tokens:                    st.lastTotalTokens[stateKey],
		MsgCount:                  st.lastMessageCount[stateKey],
		SystemFingerprint:         st.systemFingerprints[stateKey],
		ToolsFingerprint:          st.toolsFingerprints[stateKey],
		MessagesPrefixFingerprint: st.messagesPrefixFingerprints[stateKey],
		Conservative:              st.conservativeBaselines[stateKey],
	}
	submitter, persistFn := st.submitter, st.persistFn
	shouldPersist := (submitter != nil || persistFn != nil) && state.Tokens > 0 && state.MsgCount >= 0
	if shouldPersist {
		st.stateFoundDB[stateKey] = true
	}
	st.mu.Unlock()

	if shouldPersist {
		if data, err := json.Marshal(state); err == nil {
			st.submitSawtoothState(submitter, persistFn, stateKey, string(data), nil)
		}
	}
	return StateLoadFailure{}
}

// PressureBaseline 返回指定 thread 的完整 pressure baseline 单次快照。
// 首次访问会在不持锁的情况下尝试从 SQLite lazy-load，然后整体重读。
func (st *SawtoothTrigger) PressureBaseline(threadID string) pressureBaseline {
	baseline, _ := st.PressureBaselineWithLoadResult(threadID)
	return baseline
}

// PressureBaselineWithLoadResult 在读取失败时明确 fail closed：
// Available=false 且 ResetReason=state_load_failed，调用方因此会选择
// local_full 而不是被误导成"这个 thread 从来没有 actual"。
func (st *SawtoothTrigger) PressureBaselineWithLoadResult(threadID string) (pressureBaseline, StateLoadFailure) {
	var failure StateLoadFailure
	for {
		st.mu.RLock()
		stateKey := st.resolveStateKeyLocked(threadID)
		_, hasActual := st.lastTotalTokens[stateKey]
		loaded := st.loadedFromDB[stateKey]
		loading := st.loadingFromDB[stateKey]
		st.mu.RUnlock()
		if hasActual || loaded {
			threadID = stateKey
			break
		}
		if loading != nil {
			<-loading
			// 等待方共享同一次读取结果，不把 error 折叠成"状态不存在"。
			st.mu.RLock()
			failure = st.loadFailures[stateKey]
			st.mu.RUnlock()
			if failure.Failed {
				break
			}
			continue
		}
		if failure = st.loadSawtoothFromDB(stateKey); failure.Failed {
			break
		}
	}
	if failure.Failed {
		return pressureBaseline{Available: false, ResetReason: baselineResetStateLoadFailed}, failure
	}

	st.mu.RLock()
	baseline := pressureBaseline{
		ActualTokens:              st.lastTotalTokens[threadID],
		MessageCount:              st.lastMessageCount[threadID],
		SystemFingerprint:         st.systemFingerprints[threadID],
		ToolsFingerprint:          st.toolsFingerprints[threadID],
		MessagesPrefixFingerprint: st.messagesPrefixFingerprints[threadID],
		Conservative:              st.conservativeBaselines[threadID],
		ResetReason:               baselineResetNoActual,
	}
	baseline.Available = baseline.ActualTokens > 0 && baseline.MessageCount >= 0 &&
		validPressureFingerprint(baseline.SystemFingerprint) &&
		validPressureFingerprint(baseline.ToolsFingerprint) &&
		validPressureFingerprint(baseline.MessagesPrefixFingerprint)
	if baseline.Available {
		baseline.ResetReason = baselineResetNone
	}
	st.mu.RUnlock()
	return baseline, StateLoadFailure{}
}

func validPressureFingerprint(fingerprint string) bool {
	if len(fingerprint) != sha256.Size*2 {
		return false
	}
	for _, char := range fingerprint {
		if (char < '0' || char > '9') && (char < 'a' || char > 'f') {
			return false
		}
	}
	return true
}

// SetPersistFunc 设置持久化 sawtooth 状态到 DB 的回调函数。
func (st *SawtoothTrigger) SetPersistFunc(fn PersistFunc) {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.persistFn = fn
}

// SetLoadFunc 是 bool-only 加载回调的兼容入口，只供尚未迁移的测试使用。
// 生产 wiring 必须用 SetStateLoader，否则查询失败会被当成"状态不存在"。
func (st *SawtoothTrigger) SetLoadFunc(fn LoadFunc) {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.loadFn = fn
	st.stateLoader = stateLoaderFromLoadFunc(fn)
}

// SetStateLoader 设置生产使用的 error-aware 状态读取合同。
func (st *SawtoothTrigger) SetStateLoader(loader StateLoader) {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.stateLoader = loader
}

// SetStateSubmitter 设置锁外 result-bearing 写入提交器。
func (st *SawtoothTrigger) SetStateSubmitter(submitter StateSubmitter) {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.submitter = submitter
}

// submitSawtoothState 是 Sawtooth 唯一的外部写入口，必须在 st.mu 之外调用。
func (st *SawtoothTrigger) submitSawtoothState(submitter StateSubmitter, persistFn PersistFunc, stateKey, value string, completion *outcomeCompletion) PersistenceReceipt {
	key := "sawtooth:" + stateKey
	var legacy func()
	if persistFn != nil {
		legacy = func() { persistFn(key, value) }
	}
	return submitStateOp(submitter, legacy, PersistenceOp{
		Kind: PersistenceOpPut, OrderingKey: key, Key: key, Value: value, Completion: completion,
	})
}

// RecordCalibrationSample 记录一次完整响应的 (actual, estimate) 比值样本。
// estimate 是与 actual 同点采集的转发 wire 本地全量估算（messages + system +
// tools）；调用方只在响应完整（SSE 完整 / JSON 2xx 且 usage 可解析）且坐标已
// 绑定时才到达这里，中断/非 2xx/乱序路径不产生样本，配对天然不错位。
func (st *SawtoothTrigger) RecordCalibrationSample(stateKey string, actual, estimate int) {
	if st == nil || stateKey == "" || actual <= 0 || estimate <= 0 {
		return
	}
	ratio := float64(actual) / float64(estimate)
	st.mu.Lock()
	defer st.mu.Unlock()
	stateKey = st.resolveStateKeyLocked(stateKey)
	samples := append(st.calibrationSamples[stateKey], ratio)
	if excess := len(samples) - calibrationSampleWindow; excess > 0 {
		copy(samples, samples[excess:])
		samples = samples[:calibrationSampleWindow]
	}
	st.calibrationSamples[stateKey] = samples
}

// CalibrationRatio 返回该 stateKey 的滚动校准系数（锁内只读）。
// 冷启动/样本不足返回常数 1.50；否则取窗口内中位数并 clamp 到
// [calibrationMinRatio, calibrationMaxRatio]。
func (st *SawtoothTrigger) CalibrationRatio(stateKey string) float64 {
	if st == nil {
		return defaultCalibrationRatio
	}
	st.mu.RLock()
	defer st.mu.RUnlock()
	stateKey = st.resolveStateKeyLocked(stateKey)
	samples := st.calibrationSamples[stateKey]
	if len(samples) == 0 {
		return defaultCalibrationRatio
	}
	sorted := append([]float64(nil), samples...)
	sort.Float64s(sorted)
	mid := len(sorted) / 2
	median := sorted[mid]
	if len(sorted)%2 == 0 {
		median = (sorted[mid-1] + sorted[mid]) / 2
	}
	return clampCalibrationRatio(median)
}

func clampCalibrationRatio(ratio float64) float64 {
	if ratio < calibrationMinRatio {
		return calibrationMinRatio
	}
	if ratio > calibrationMaxRatio {
		return calibrationMaxRatio
	}
	return ratio
}

// Evaluate 返回本次触发判定实际使用的同锁快照。
// selectedPressure 是调用方已经在 local_full 与 actual_plus_delta 中唯一选定的压力值；
// 历史 actual 不在此处再次参与 token 判定，它只应通过 pressureDecision 进入。
func (st *SawtoothTrigger) Evaluate(threadID string, selectedPressure int, now time.Time) TriggerEvaluation {
	st.mu.RLock()
	defer st.mu.RUnlock()

	threadID = st.resolveStateKeyLocked(threadID)
	emergencyThreshold := st.tokenThreshold + 10_000 // 比阈值多 10k 安全边距
	lastTime, hasTime := st.lastRequestTime[threadID]
	evaluation := TriggerEvaluation{
		Reason:             TriggerNone,
		RequiredWait:       st.pauseThreshold,
		SelectedPressure:   selectedPressure,
		EmergencyThreshold: emergencyThreshold,
		TokenThreshold:     st.tokenThreshold,
		TokenMinimum:       st.tokenMinimum,
	}
	if hasTime {
		evaluation.ActualWait = now.Sub(lastTime)
		evaluation.ActualWaitKnown = true
	}

	// 紧急制动 —— 当前选定压力明显过高。
	if selectedPressure > emergencyThreshold {
		evaluation.Reason = TriggerEmergency
		return evaluation
	}

	// 正常 token 线只使用当前选定压力，不叠加或重读旧 actual。
	if selectedPressure > evaluation.TokenThreshold {
		evaluation.Reason = TriggerTokens
		return evaluation
	}

	// 暂停检测保留既有时间语义，但最低压力也使用同一 selectedPressure。
	if evaluation.ActualWaitKnown &&
		selectedPressure > evaluation.TokenMinimum &&
		evaluation.ActualWait > evaluation.RequiredWait {
		evaluation.Reason = TriggerPause
	}

	return evaluation
}

// ShouldTrigger 判断是否应为此 thread 执行桩化周期。
// 兼容调用方只读取 reason；需要解释本次决定时应直接保存 Evaluate 返回的快照。
func (st *SawtoothTrigger) ShouldTrigger(threadID string, selectedPressure int) TriggerReason {
	return st.Evaluate(threadID, selectedPressure, time.Now()).Reason
}

// UpdateAfterResponse 是三参数 legacy 兼容入口。
// 它保留 actual 与消息坐标，但主动清空上下文指纹，强制下一轮完整重基线。
func (st *SawtoothTrigger) UpdateAfterResponse(threadID string, totalInputTokens, messageCount int) {
	st.UpdatePressureBaseline(threadID, totalInputTokens, messageCount, "", "", "")
}

// ResetPressureBaseline 清除当前 epoch 的精确/保守 pressure 复用事实。
// 用于 input-history 仍连续、但 reuse-safety 指纹变化的已知 transport 差异。
func (st *SawtoothTrigger) ResetPressureBaseline(threadID string) {
	generation := st.BeginPressureRequest(threadID)
	st.UpdatePressureBaselineForRequest(threadID, generation, 0, 0, "", "", "")
}

// BeginPressureRequest 在请求进入有状态主管线时分配单调代际。
// 响应写回携带该代际，迟到的旧响应因此不能覆盖更新请求的 baseline。
func (st *SawtoothTrigger) BeginPressureRequest(threadID string) uint64 {
	st.mu.Lock()
	defer st.mu.Unlock()
	threadID = st.resolveStateKeyLocked(threadID)
	st.baselineRequestGeneration[threadID]++
	st.mirrorCurrentAliasesLocked(threadID)
	return st.baselineRequestGeneration[threadID]
}

// UpdatePressureBaseline 是测试与 legacy 调用使用的同步入口。
// 它先分配一个新请求代际，再复用生产响应写回协议。
// 指纹只接受固定 64 位小写 SHA-256 十六进制；非法值按空值持久化。
func (st *SawtoothTrigger) UpdatePressureBaseline(threadID string, totalInputTokens, messageCount int, systemFingerprint, toolsFingerprint, messagesPrefixFingerprint string) {
	generation := st.BeginPressureRequest(threadID)
	st.UpdatePressureBaselineForRequest(threadID, generation, totalInputTokens, messageCount, systemFingerprint, toolsFingerprint, messagesPrefixFingerprint)
}

// UpdatePressureBaselineForRequest 在成功主响应后按请求代际原子写回完整 pressure baseline。
// generation 为零时兼容直接构造 requestMeta 的测试/旧调用，并在写回时分配新代际。
func (st *SawtoothTrigger) UpdatePressureBaselineForRequest(threadID string, generation uint64, totalInputTokens, messageCount int, systemFingerprint, toolsFingerprint, messagesPrefixFingerprint string) bool {
	accepted, _ := st.updatePressureBaselineForRequest(threadID, generation, totalInputTokens, messageCount, systemFingerprint, toolsFingerprint, messagesPrefixFingerprint, false, nil)
	return accepted
}

// UpdatePressureBaselineWithResult 是 result-bearing 的 baseline 写回入口：
// 内存在锁内更新，SQLite 提交在锁外完成，磁盘结果通过 completion 回到 outcome。
func (st *SawtoothTrigger) UpdatePressureBaselineWithResult(threadID string, generation uint64, totalInputTokens, messageCount int, systemFingerprint, toolsFingerprint, messagesPrefixFingerprint string, completion *outcomeCompletion) (bool, PersistenceReceipt) {
	return st.updatePressureBaselineForRequest(threadID, generation, totalInputTokens, messageCount, systemFingerprint, toolsFingerprint, messagesPrefixFingerprint, false, completion)
}

// UpdatePressureFloorForRequest 保留旧版本 conservative baseline 的兼容入口。
// 当前生产 response 路径不再调用它：forwarded body 会先绑定真实 wire 坐标，
// 再通过 UpdatePressureBaselineForRequest 写入 exact baseline。旧数据库中的
// Conservative=true 记录只用于兼容读取，并会在 pressure 决策中被丢弃。
func (st *SawtoothTrigger) UpdatePressureFloorForRequest(threadID string, generation uint64, pressureFloor, messageCount int, systemFingerprint, toolsFingerprint, messagesPrefixFingerprint string) bool {
	accepted, _ := st.updatePressureBaselineForRequest(threadID, generation, pressureFloor, messageCount, systemFingerprint, toolsFingerprint, messagesPrefixFingerprint, true, nil)
	return accepted
}

func (st *SawtoothTrigger) updatePressureBaselineForRequest(threadID string, generation uint64, totalInputTokens, messageCount int, systemFingerprint, toolsFingerprint, messagesPrefixFingerprint string, conservative bool, completion *outcomeCompletion) (bool, PersistenceReceipt) {
	st.mu.RLock()
	threadID = st.resolveStateKeyLocked(threadID)
	st.mu.RUnlock()
	systemFingerprint = sanitizePressureFingerprint(systemFingerprint)
	toolsFingerprint = sanitizePressureFingerprint(toolsFingerprint)
	messagesPrefixFingerprint = sanitizePressureFingerprint(messagesPrefixFingerprint)
	if totalInputTokens <= 0 || messageCount < 0 {
		totalInputTokens = 0
		messageCount = 0
		systemFingerprint = ""
		toolsFingerprint = ""
		messagesPrefixFingerprint = ""
		conservative = false
	}

	state := persistedState{
		Tokens:                    totalInputTokens,
		MsgCount:                  messageCount,
		SystemFingerprint:         systemFingerprint,
		ToolsFingerprint:          toolsFingerprint,
		MessagesPrefixFingerprint: messagesPrefixFingerprint,
		Conservative:              conservative,
	}
	data, marshalErr := json.Marshal(state)
	persistLock := st.pressurePersistLock(threadID)
	persistLock.Lock()
	defer persistLock.Unlock()

	st.mu.Lock()
	if generation == 0 {
		st.baselineRequestGeneration[threadID]++
		generation = st.baselineRequestGeneration[threadID]
	} else if generation > st.baselineRequestGeneration[threadID] {
		st.baselineRequestGeneration[threadID] = generation
	}
	if generation < st.baselineCommittedGeneration[threadID] {
		st.mu.Unlock()
		return false, completeStateOp(completion, persistenceStateNotAttempted, persistenceFailureNone)
	}
	st.baselineCommittedGeneration[threadID] = generation
	if totalInputTokens > 0 {
		st.lastTotalTokens[threadID] = totalInputTokens
		st.lastMessageCount[threadID] = messageCount
		st.systemFingerprints[threadID] = systemFingerprint
		st.toolsFingerprints[threadID] = toolsFingerprint
		st.messagesPrefixFingerprints[threadID] = messagesPrefixFingerprint
		st.conservativeBaselines[threadID] = conservative
		st.lastRequestTime[threadID] = time.Now()
	} else {
		delete(st.lastTotalTokens, threadID)
		delete(st.lastMessageCount, threadID)
		delete(st.systemFingerprints, threadID)
		delete(st.toolsFingerprints, threadID)
		delete(st.messagesPrefixFingerprints, threadID)
		delete(st.conservativeBaselines, threadID)
		delete(st.lastRequestTime, threadID)
	}
	st.loadedFromDB[threadID] = true
	submitter, persistFn := st.submitter, st.persistFn
	if submitter != nil || persistFn != nil {
		st.stateFoundDB[threadID] = true
	}
	st.baselineGeneration[threadID]++
	if st.loadingFromDB[threadID] != nil {
		st.loadedFromDB[threadID] = false
	}
	st.mirrorCurrentAliasesLocked(threadID)
	st.mu.Unlock()
	if marshalErr != nil {
		return true, completeStateOp(completion, persistenceStateNotAttempted, persistenceFailureNone)
	}
	// 提交在 st.mu 之外执行；persistLock 保证同 thread 的磁盘顺序。
	return true, st.submitSawtoothState(submitter, persistFn, threadID, string(data), completion)
}

func (st *SawtoothTrigger) pressurePersistLock(threadID string) *sync.Mutex {
	st.mu.Lock()
	threadID = st.resolveStateKeyLocked(threadID)
	lock := st.baselinePersistLocks[threadID]
	if lock == nil {
		lock = &sync.Mutex{}
		st.baselinePersistLocks[threadID] = lock
	}
	st.mu.Unlock()
	return lock
}

func sanitizePressureFingerprint(fingerprint string) string {
	if validPressureFingerprint(fingerprint) {
		return fingerprint
	}
	return ""
}

// IncrementRequestSeq 递增指定 thread 的请求序号并返回新值。
// 每次 HandleMessages 调用时递增一次（Phase B: DecayTracker 用）。
func (st *SawtoothTrigger) IncrementRequestSeq(threadID string) int {
	st.mu.Lock()
	defer st.mu.Unlock()
	threadID = st.resolveStateKeyLocked(threadID)
	st.requestSeq[threadID]++
	st.mirrorCurrentAliasesLocked(threadID)
	return st.requestSeq[threadID]
}

// GetRequestSeq 返回当前请求序号（不递增）。
func (st *SawtoothTrigger) GetRequestSeq(threadID string) int {
	st.mu.RLock()
	defer st.mu.RUnlock()
	threadID = st.resolveStateKeyLocked(threadID)
	return st.requestSeq[threadID]
}

// SetPauseThreshold 动态更新 SawtoothTrigger 的 pause 检测阈值。
// 用于 Cache TTL 自适应：检测到 1h 断点时升至 61min，默认 ephemeral 保持 4min。
func (st *SawtoothTrigger) SetPauseThreshold(pauseThreshold time.Duration) {
	st.mu.Lock()
	st.pauseThreshold = pauseThreshold
	st.mu.Unlock()
}

// loadSawtoothFromDB 从 DB 加载指定 thread 的持久化 sawtooth 状态。
// 每个 thread 在冷启动时仅调用一次（由 ShouldTrigger 触发 lazy-load）。
// 只有 ErrNoRows 才算 missing：查询失败既不设置 loaded/found，也不应用任何
// 状态，下一次请求可以重试。
func (st *SawtoothTrigger) loadSawtoothFromDB(threadID string) StateLoadFailure {
	st.mu.Lock()
	threadID = st.resolveStateKeyLocked(threadID)
	if st.loadedFromDB[threadID] {
		st.mu.Unlock()
		return StateLoadFailure{}
	}
	if loading := st.loadingFromDB[threadID]; loading != nil {
		st.mu.Unlock()
		<-loading
		st.mu.RLock()
		shared := st.loadFailures[threadID]
		st.mu.RUnlock()
		return shared
	}
	loading := make(chan struct{})
	st.loadingFromDB[threadID] = loading
	startGeneration := st.baselineGeneration[threadID]
	loader := st.stateLoader
	st.mu.Unlock()

	var state persistedState
	valid := false
	var failure StateLoadFailure
	found := false
	if loader != nil {
		result := loader.LoadStateResult("sawtooth:" + threadID)
		switch {
		case result.Err != nil:
			failure = stateLoadFailureFrom(result)
		case result.Found && result.Value != "":
			found = true
			if json.Unmarshal([]byte(result.Value), &state) == nil && state.Tokens > 0 && state.MsgCount >= 0 {
				state.SystemFingerprint = sanitizePressureFingerprint(state.SystemFingerprint)
				state.ToolsFingerprint = sanitizePressureFingerprint(state.ToolsFingerprint)
				state.MessagesPrefixFingerprint = sanitizePressureFingerprint(state.MessagesPrefixFingerprint)
				valid = true
			}
		}
	}

	st.mu.Lock()
	applied := false
	if failure.Failed {
		st.loadFailures[threadID] = failure
	} else {
		delete(st.loadFailures, threadID)
		if found {
			st.stateFoundDB[threadID] = true
		}
		if valid && st.baselineGeneration[threadID] == startGeneration {
			if _, exists := st.lastTotalTokens[threadID]; !exists {
				st.lastTotalTokens[threadID] = state.Tokens
				st.lastMessageCount[threadID] = state.MsgCount
				st.systemFingerprints[threadID] = state.SystemFingerprint
				st.toolsFingerprints[threadID] = state.ToolsFingerprint
				st.messagesPrefixFingerprints[threadID] = state.MessagesPrefixFingerprint
				st.conservativeBaselines[threadID] = state.Conservative
				st.mirrorCurrentAliasesLocked(threadID)
				applied = true
			}
		}
		st.loadedFromDB[threadID] = true
	}
	delete(st.loadingFromDB, threadID)
	close(loading)
	st.mu.Unlock()
	// 不设置 lastRequestTime —— 保持零值，ShouldTrigger 中 hasTime=false 会跳过 Pause 检查。
	// 下次 API 响应后 UpdateAfterResponse 才会设置真实时间。

	if applied {
		slog.Info("从 SQLite 恢复 Sawtooth 状态",
			"tokens", state.Tokens,
			"msg_count", state.MsgCount,
		)
	}
	return failure
}

// ── Cache TTL 自适应辅助函数 ──

// CacheGapForTTL 返回缓存过期前的"等待窗口"（即最后一次 API 调用后缓存还活多久）。
// 1h TTL → 61min（剩余 1min 安全边距），默认 ephemeral → 4min。
func CacheGapForTTL(cacheTTL string) time.Duration {
	switch cacheTTL {
	case "1h":
		return 61 * time.Minute
	default:
		return 4 * time.Minute
	}
}

// SawtoothTTLForCacheTTL 返回 FrozenStubs 的 TTL（应略大于 cache TTL，确保缓存未过期时 prefix 不被释放）。
// 1h TTL → 65min，默认 ephemeral → 30min。
func SawtoothTTLForCacheTTL(cacheTTL string) time.Duration {
	switch cacheTTL {
	case "1h":
		return 65 * time.Minute
	default:
		return 30 * time.Minute
	}
}

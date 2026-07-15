package proxy

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"log/slog"
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

// FrozenStubs 存储每个 thread 的冻结桩化消息前缀，用于缓存优化。
// 桩化周期之间，冻结前缀被逐字节复用，使 API 缓存在前缀部分可命中。
type FrozenStubs struct {
	mu           sync.RWMutex
	ttl          time.Duration        // eviction TTL —— 默认 30 分钟
	messages     map[string][]Message // threadID → 深拷贝的桩化消息
	cutoff       map[string]int       // threadID → Store 时的原始消息总数
	boundaryHash map[string]string    // threadID → messages[cutoff-1] 的稳定 hash
	prefixHash   map[string]string    // threadID → 序列化后 frozen prefix 的 SHA-256 hash
	stubTime     map[string]time.Time
	tokens       map[string]int // threadID → frozen stubs 的 token 估算
	rawTokens    map[string]int // threadID → Store 时原始 token 估算（压缩前）
	lastAccess   map[string]time.Time
	persistFn    PersistFunc     // 可选：持久化 frozen 状态到 DB
	loadFn       LoadFunc        // 可选：冷启动时从 DB 加载 frozen 状态
	deleteFn     DeleteFunc      // 可选：失效时删除 DB 中的 frozen 状态
	loadedFromDB map[string]bool // threadID → 已尝试从 DB 加载
}

// frozenPersisted 是 frozen 桩化状态的可 JSON 序列化形式。
type frozenPersisted struct {
	Messages     []Message `json:"messages"`
	Cutoff       int       `json:"cutoff"`
	BoundaryHash string    `json:"boundary_hash"`
	PrefixHash   string    `json:"prefix_hash"`
	Tokens       int       `json:"tokens"`
	RawTokens    int       `json:"raw_tokens,omitempty"`
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
		ttl:          ttl,
		messages:     make(map[string][]Message),
		cutoff:       make(map[string]int),
		boundaryHash: make(map[string]string),
		prefixHash:   make(map[string]string),
		stubTime:     make(map[string]time.Time),
		tokens:       make(map[string]int),
		rawTokens:    make(map[string]int),
		lastAccess:   make(map[string]time.Time),
		loadedFromDB: make(map[string]bool),
	}
}

// SetPersistFunc 设置持久化 frozen 状态到 DB 的回调函数。
func (f *FrozenStubs) SetPersistFunc(fn PersistFunc) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.persistFn = fn
}

// SetLoadFunc 设置冷启动时从 DB 加载 frozen 状态的回调函数。
func (f *FrozenStubs) SetLoadFunc(fn LoadFunc) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.loadFn = fn
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
func (f *FrozenStubs) Store(threadID string, stubbed []Message, cutoff int, boundaryMsg Message, tokenEstimate int, rawTokenEstimate int) {
	f.StoreWithLogger(slog.Default(), threadID, stubbed, cutoff, boundaryMsg, tokenEstimate, rawTokenEstimate)
}

func (f *FrozenStubs) StoreWithLogger(logger *slog.Logger, threadID string, stubbed []Message, cutoff int, boundaryMsg Message, tokenEstimate int, rawTokenEstimate int) {
	if logger == nil {
		logger = slog.Default()
	}
	if cutoff <= 0 || tokenEstimate < 0 || rawTokenEstimate < 0 {
		logger.Warn("frozen 状态元数据非法，跳过存储", "thread_id", threadID, "cutoff", cutoff)
		return
	}
	if ExtractPersistentUserContext(stubbed) != nil {
		logger.Warn("frozen prefix 包含 persistent user context，拒绝存储", "thread_id", threadID)
		return
	}
	frozen := deepCopyMessages(stubbed)
	if frozen == nil {
		logger.Warn("frozen prefix 深拷贝失败，跳过存储", "thread_id", threadID)
		return
	}

	frozenJSON, _ := json.Marshal(frozen)
	pHash := sha256hex(frozenJSON)

	bHash := stableBoundaryHash(boundaryMsg)

	now := time.Now()
	fp := frozenPersisted{
		Messages: frozen, Cutoff: cutoff, BoundaryHash: bHash, PrefixHash: pHash,
		Tokens: tokenEstimate, RawTokens: rawTokenEstimate,
	}
	persisted, _ := json.Marshal(fp)

	f.mu.Lock()
	f.messages[threadID] = frozen
	f.cutoff[threadID] = cutoff
	f.boundaryHash[threadID] = bHash
	f.prefixHash[threadID] = pHash
	f.stubTime[threadID] = now
	f.tokens[threadID] = tokenEstimate
	f.rawTokens[threadID] = rawTokenEstimate
	f.lastAccess[threadID] = now
	f.loadedFromDB[threadID] = true // 内存中的是最新权威数据
	// 持久化在同一临界区内执行，保证同一 thread 的内存与 SQLite 顺序一致。
	if f.persistFn != nil && persisted != nil {
		f.persistFn("frozen:"+threadID, string(persisted))
	}
	f.mu.Unlock()

	logger.Info("frozen prefix 已存储",
		"thread_id", threadID,
		"cutoff", cutoff,
		"prefix_hash", pHash[:min(16, len(pHash))],
		"tokens", tokenEstimate,
	)
}

// Get 返回指定 thread 的已验证 frozen stubs（若存在且有效）。
// 验证：(1) 当前消息数不小于 cutoff；
//
//	(2) frozen prefix 在内存中未被意外修改（SHA-256 hash 验证）。
func (f *FrozenStubs) Get(threadID string, currentMessages []Message) *FrozenResult {
	return f.GetWithLogger(slog.Default(), threadID, currentMessages)
}

func (f *FrozenStubs) GetWithLogger(logger *slog.Logger, threadID string, currentMessages []Message) *FrozenResult {
	if logger == nil {
		logger = slog.Default()
	}
	f.mu.RLock()
	_, ok := f.messages[threadID]
	loaded := f.loadedFromDB[threadID]
	f.mu.RUnlock()

	// 冷启动 lazy-load：首次访问时尝试从 DB 恢复
	if !ok && !loaded {
		f.loadFrozenFromDB(logger, threadID)
	}

	f.mu.RLock()
	msgs, ok := f.messages[threadID]
	if !ok {
		f.mu.RUnlock()
		logger.Debug("frozen prefix 未命中", "thread_id", threadID)
		return nil
	}
	cutoff := f.cutoff[threadID]
	pHash := f.prefixHash[threadID]
	bHash := f.boundaryHash[threadID]
	tokens := f.tokens[threadID]
	rawTokens := f.rawTokens[threadID]
	f.mu.RUnlock()

	// 验证 1：持久化元数据与当前消息边界必须可安全切片。
	if cutoff <= 0 || cutoff > len(currentMessages) || bHash == "" || tokens < 0 || rawTokens < 0 {
		logger.Warn("frozen prefix 验证失败：状态元数据非法",
			"thread_id", threadID,
			"current", len(currentMessages),
			"cutoff", cutoff,
		)
		f.InvalidateWithLogger(logger, threadID)
		return nil
	}

	// 验证 2：prefix hash 不匹配（内存被意外修改）
	frozenJSON, _ := json.Marshal(msgs)
	if sha256hex(frozenJSON) != pHash {
		logger.Warn("frozen prefix 验证失败：hash 不匹配",
			"thread_id", threadID,
		)
		f.InvalidateWithLogger(logger, threadID)
		return nil
	}

	// 验证 3：boundary hash 不匹配（用户编辑了 frozen 范围内的消息）
	// sawtooth-proxy 在 Get 之前运行 StripReminders，CC 注入不会误触发
	if cutoff > 0 && cutoff <= len(currentMessages) {
		currentBHash := stableBoundaryHash(currentMessages[cutoff-1])
		if currentBHash != bHash {
			logger.Warn("frozen prefix 验证失败：boundary 已变化（用户可能编辑了消息）",
				"thread_id", threadID,
				"cutoff", cutoff,
			)
			f.InvalidateWithLogger(logger, threadID)
			return nil
		}
	}

	// 深拷贝——防止下游（cache_control inject 等）原地修改 frozen 数据
	copied := deepCopyMessages(msgs)
	if copied == nil {
		logger.Warn("frozen prefix 验证失败：深拷贝失败",
			"thread_id", threadID,
		)
		f.InvalidateWithLogger(logger, threadID)
		return nil
	}

	// 更新最后访问时间
	f.mu.Lock()
	f.lastAccess[threadID] = time.Now()
	f.mu.Unlock()

	logger.Info("frozen prefix 命中",
		"thread_id", threadID,
		"cutoff", cutoff,
		"frozen_tokens", tokens,
	)

	return &FrozenResult{
		Messages:  copied,
		Cutoff:    cutoff,
		Tokens:    tokens,
		RawTokens: rawTokens,
	}
}

// LengthFor 返回 threadID 对应的 frozen prefix 消息数；条目不存在时返回 0。
func (f *FrozenStubs) LengthFor(threadID string) int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return len(f.messages[threadID])
}

// UpdateMessages 用 newMsgs 覆盖已存储的 frozen prefix 并刷新 prefix hash。
// 要求 newMsgs 长度与已有条目一致（防止同一管线内二次 sawtooth 触发）。
// 返回是否成功更新。
func (f *FrozenStubs) UpdateMessages(threadID string, newMsgs []Message) bool {
	if ExtractPersistentUserContext(newMsgs) != nil {
		return false
	}
	fresh := deepCopyMessages(newMsgs)
	if fresh == nil {
		return false
	}

	freshJSON, _ := json.Marshal(fresh)
	pHash := sha256hex(freshJSON)

	f.mu.Lock()
	existing, ok := f.messages[threadID]
	if !ok || len(fresh) != len(existing) {
		f.mu.Unlock()
		return false
	}
	f.messages[threadID] = fresh
	f.prefixHash[threadID] = pHash
	f.lastAccess[threadID] = time.Now()
	cutoff := f.cutoff[threadID]
	bHash := f.boundaryHash[threadID]
	tokens := f.tokens[threadID]
	rawTokens := f.rawTokens[threadID]
	if f.persistFn != nil {
		fp := frozenPersisted{
			Messages:     fresh,
			Cutoff:       cutoff,
			BoundaryHash: bHash,
			PrefixHash:   pHash,
			Tokens:       tokens,
			RawTokens:    rawTokens,
		}
		if data, err := json.Marshal(fp); err == nil {
			f.persistFn("frozen:"+threadID, string(data))
		}
	}
	f.mu.Unlock()

	return true
}

// Invalidate 删除指定 thread 的 frozen stubs。
func (f *FrozenStubs) Invalidate(threadID string) {
	f.InvalidateWithLogger(slog.Default(), threadID)
}

func (f *FrozenStubs) InvalidateWithLogger(logger *slog.Logger, threadID string) {
	if logger == nil {
		logger = slog.Default()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.messages, threadID)
	delete(f.cutoff, threadID)
	delete(f.boundaryHash, threadID)
	delete(f.prefixHash, threadID)
	delete(f.stubTime, threadID)
	delete(f.tokens, threadID)
	delete(f.rawTokens, threadID)
	delete(f.lastAccess, threadID)
	// 已知坏状态失效后保留“本进程已加载”标记，防止删除失败时反复恢复。
	f.loadedFromDB[threadID] = true
	if f.deleteFn != nil {
		f.deleteFn("frozen:" + threadID)
	}
	logger.Warn("frozen prefix 已失效", "thread_id", threadID)
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
			delete(f.stubTime, tid)
			delete(f.tokens, tid)
			delete(f.rawTokens, tid)
			delete(f.lastAccess, tid)
			delete(f.loadedFromDB, tid)
			evicted++
		}
	}
	if evicted > 0 {
		slog.Info("frozen stubs eviction 完成", "evicted", evicted)
	}
	return evicted
}

// loadFrozenFromDB 尝试从 DB 恢复指定 thread 的 frozen stubs。
// 每个 thread 在冷启动时仅调用一次（由 Get 触发 lazy-load）。
func (f *FrozenStubs) loadFrozenFromDB(logger *slog.Logger, threadID string) {
	f.mu.Lock()
	if f.loadedFromDB[threadID] {
		f.mu.Unlock()
		return
	}
	f.loadedFromDB[threadID] = true
	loadFn := f.loadFn
	f.mu.Unlock()

	if loadFn == nil {
		return
	}

	raw, ok := loadFn("frozen:" + threadID)
	if !ok || raw == "" {
		return
	}

	var fp frozenPersisted
	if err := json.Unmarshal([]byte(raw), &fp); err != nil {
		f.InvalidateWithLogger(logger, threadID)
		return
	}
	if len(fp.Messages) == 0 || fp.Cutoff <= 0 || fp.BoundaryHash == "" || fp.PrefixHash == "" || fp.Tokens < 0 || fp.RawTokens < 0 || ExtractPersistentUserContext(fp.Messages) != nil {
		f.InvalidateWithLogger(logger, threadID)
		return
	}

	// 验证 prefix hash 与存储的消息一致
	frozenJSON, _ := json.Marshal(fp.Messages)
	if sha256hex(frozenJSON) != fp.PrefixHash {
		logger.Warn("从 DB 恢复的 frozen 状态 hash 不匹配，丢弃", "thread_id", threadID)
		f.InvalidateWithLogger(logger, threadID)
		return
	}

	now := time.Now()
	f.mu.Lock()
	defer f.mu.Unlock()
	// 仅在仍为空时写入（Store() 可能在并发中被调用）
	if _, exists := f.messages[threadID]; exists {
		return
	}
	f.messages[threadID] = fp.Messages
	f.cutoff[threadID] = fp.Cutoff
	f.boundaryHash[threadID] = fp.BoundaryHash
	f.prefixHash[threadID] = fp.PrefixHash
	f.tokens[threadID] = fp.Tokens
	f.rawTokens[threadID] = fp.RawTokens
	f.stubTime[threadID] = now
	f.lastAccess[threadID] = now

	logger.Info("从 SQLite 恢复 frozen 状态",
		"thread_id", threadID,
		"cutoff", fp.Cutoff,
		"tokens", fp.Tokens,
	)
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

// normalizeBoundaryContent 只消除 ContentBlock typed round-trip 已确证产生的
// 非语义差异：text/thinking/tool_result 块本身不使用 input，但 ContentBlock.Input
// 缺少 omitempty 会把省略字段重编码为 input:null。未知字段、数组 null 和 tool_use
// 的 input 均保持原样，避免把未来协议中的显式 null 与 absent 混同。
func normalizeBoundaryContent(content any) any {
	blocks, ok := content.([]any)
	if !ok {
		return content
	}
	for _, block := range blocks {
		object, ok := block.(map[string]any)
		if !ok || object["input"] != nil {
			continue
		}
		typeName, _ := object["type"].(string)
		switch typeName {
		case "text", "thinking", "tool_result":
			delete(object, "input")
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

// persistedState 是持久化到 proxy_state 的 JSON 结构。
type persistedState struct {
	Tokens                    int    `json:"tokens"`
	MsgCount                  int    `json:"msg_count"`
	SystemFingerprint         string `json:"system_fingerprint,omitempty"`
	ToolsFingerprint          string `json:"tools_fingerprint,omitempty"`
	MessagesPrefixFingerprint string `json:"messages_prefix_fingerprint,omitempty"`
}

// baselineResetReason 表示 pressure baseline 不能沿用时的受限原因。
// 状态层只暴露事实，不在此处决定是否执行 Collapse。
type baselineResetReason string

const (
	baselineResetNone            baselineResetReason = "none"
	baselineResetNoActual        baselineResetReason = "no_actual"
	baselineResetMessageShrink   baselineResetReason = "message_shrink"
	baselineResetMessagesChanged baselineResetReason = "messages_changed"
	baselineResetSystemChanged   baselineResetReason = "system_changed"
	baselineResetToolsChanged    baselineResetReason = "tools_changed"
)

// pressureBaseline 是后续 pressure 决策所需的单次原子快照。
// 其中只包含数字、受限枚举和固定长度 SHA-256 十六进制指纹。
type pressureBaseline struct {
	ActualTokens              int
	MessageCount              int
	SystemFingerprint         string
	ToolsFingerprint          string
	MessagesPrefixFingerprint string
	Available                 bool
	ResetReason               baselineResetReason
}

// SawtoothTrigger 根据 token 使用量和时间判断是否执行桩化周期。
type SawtoothTrigger struct {
	mu                          sync.RWMutex
	lastTotalTokens             map[string]int           // threadID → 上次 API 响应 input tokens
	lastMessageCount            map[string]int           // threadID → 上次响应时的消息数
	systemFingerprints          map[string]string        // threadID → 上次主请求 system 的 SHA-256 指纹
	toolsFingerprints           map[string]string        // threadID → 上次主请求 tools 的 SHA-256 指纹
	messagesPrefixFingerprints  map[string]string        // threadID → 上次主请求消息前缀的 SHA-256 指纹
	lastRequestTime             map[string]time.Time     // threadID → 上次 API 响应时间
	loadedFromDB                map[string]bool          // threadID → DB 加载已完成
	loadingFromDB               map[string]chan struct{} // threadID → 正在进行的 DB 加载完成信号
	baselineGeneration          map[string]uint64        // threadID → 响应写回版本，防止慢加载覆盖新状态
	baselineRequestGeneration   map[string]uint64        // threadID → 请求进入有状态主管线时分配的代际
	baselineCommittedGeneration map[string]uint64        // threadID → 最近接受的响应请求代际
	requestSeq                  map[string]int           // threadID → 当前请求序号（Phase B: DecayTracker 用）
	pauseThreshold              time.Duration            // 暂停检测阈值（cache TTL - 安全边距）
	tokenThreshold              int                      // 超过此值触发桩化周期（来自配置）
	tokenMinimum                int                      // 桩化下限（来自配置）
	persistFn                   PersistFunc              // 可选：更新时持久化 token 状态到 DB
	loadFn                      LoadFunc                 // 可选：冷启动时从 DB 加载 token 状态
}

// NewSawtoothTrigger 创建新的触发状态跟踪器。
func NewSawtoothTrigger(pauseThreshold time.Duration, tokenThreshold, tokenMinimum int) *SawtoothTrigger {
	return &SawtoothTrigger{
		lastTotalTokens:             make(map[string]int),
		lastMessageCount:            make(map[string]int),
		systemFingerprints:          make(map[string]string),
		toolsFingerprints:           make(map[string]string),
		messagesPrefixFingerprints:  make(map[string]string),
		lastRequestTime:             make(map[string]time.Time),
		loadedFromDB:                make(map[string]bool),
		loadingFromDB:               make(map[string]chan struct{}),
		baselineGeneration:          make(map[string]uint64),
		baselineRequestGeneration:   make(map[string]uint64),
		baselineCommittedGeneration: make(map[string]uint64),
		requestSeq:                  make(map[string]int),
		pauseThreshold:              pauseThreshold,
		tokenThreshold:              tokenThreshold,
		tokenMinimum:                tokenMinimum,
	}
}

// PressureBaseline 返回指定 thread 的完整 pressure baseline 单次快照。
// 首次访问会在不持锁的情况下尝试从 SQLite lazy-load，然后整体重读。
func (st *SawtoothTrigger) PressureBaseline(threadID string) pressureBaseline {
	for {
		st.mu.RLock()
		_, hasActual := st.lastTotalTokens[threadID]
		loaded := st.loadedFromDB[threadID]
		loading := st.loadingFromDB[threadID]
		st.mu.RUnlock()
		if hasActual || loaded {
			break
		}
		if loading != nil {
			<-loading
			continue
		}
		st.loadSawtoothFromDB(threadID)
	}

	st.mu.RLock()
	baseline := pressureBaseline{
		ActualTokens:              st.lastTotalTokens[threadID],
		MessageCount:              st.lastMessageCount[threadID],
		SystemFingerprint:         st.systemFingerprints[threadID],
		ToolsFingerprint:          st.toolsFingerprints[threadID],
		MessagesPrefixFingerprint: st.messagesPrefixFingerprints[threadID],
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
	return baseline
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

// SetLoadFunc 设置冷启动时从 DB 加载 sawtooth 状态的回调函数。
func (st *SawtoothTrigger) SetLoadFunc(fn LoadFunc) {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.loadFn = fn
}

// ShouldTrigger 判断是否应为此 thread 执行桩化周期。
// selectedPressure 是调用方已经在 local_full 与 actual_plus_delta 中唯一选定的压力值。
// 历史 actual 不在此处再次参与 token 判定；它只应通过 pressureDecision 进入。
func (st *SawtoothTrigger) ShouldTrigger(threadID string, selectedPressure int) TriggerReason {
	st.mu.RLock()
	emergencyThreshold := st.tokenThreshold + 10_000 // 比阈值多 10k 安全边距
	tokenThreshold := st.tokenThreshold
	tokenMinimum := st.tokenMinimum
	pauseThreshold := st.pauseThreshold
	lastTime, hasTime := st.lastRequestTime[threadID]
	st.mu.RUnlock()

	// 紧急制动 —— 当前选定压力明显过高。
	if selectedPressure > emergencyThreshold {
		return TriggerEmergency
	}

	// 正常 token 线只使用当前选定压力，不叠加或重读旧 actual。
	if selectedPressure > tokenThreshold {
		return TriggerTokens
	}

	// 暂停检测保留既有时间语义，但最低压力也使用同一 selectedPressure。
	if hasTime && selectedPressure > tokenMinimum {
		if time.Since(lastTime) > pauseThreshold {
			return TriggerPause
		}
	}

	return TriggerNone
}

// UpdateAfterResponse 是三参数 legacy 兼容入口。
// 它保留 actual 与消息坐标，但主动清空上下文指纹，强制下一轮完整重基线。
func (st *SawtoothTrigger) UpdateAfterResponse(threadID string, totalInputTokens, messageCount int) {
	st.UpdatePressureBaseline(threadID, totalInputTokens, messageCount, "", "", "")
}

// BeginPressureRequest 在请求进入有状态主管线时分配单调代际。
// 响应写回携带该代际，迟到的旧响应因此不能覆盖更新请求的 baseline。
func (st *SawtoothTrigger) BeginPressureRequest(threadID string) uint64 {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.baselineRequestGeneration[threadID]++
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
	systemFingerprint = sanitizePressureFingerprint(systemFingerprint)
	toolsFingerprint = sanitizePressureFingerprint(toolsFingerprint)
	messagesPrefixFingerprint = sanitizePressureFingerprint(messagesPrefixFingerprint)
	if totalInputTokens <= 0 || messageCount < 0 {
		totalInputTokens = 0
		messageCount = 0
		systemFingerprint = ""
		toolsFingerprint = ""
		messagesPrefixFingerprint = ""
	}

	state := persistedState{
		Tokens:                    totalInputTokens,
		MsgCount:                  messageCount,
		SystemFingerprint:         systemFingerprint,
		ToolsFingerprint:          toolsFingerprint,
		MessagesPrefixFingerprint: messagesPrefixFingerprint,
	}

	st.mu.Lock()
	if generation == 0 {
		st.baselineRequestGeneration[threadID]++
		generation = st.baselineRequestGeneration[threadID]
	} else if generation > st.baselineRequestGeneration[threadID] {
		st.baselineRequestGeneration[threadID] = generation
	}
	if generation < st.baselineCommittedGeneration[threadID] {
		st.mu.Unlock()
		return false
	}
	st.baselineCommittedGeneration[threadID] = generation
	if totalInputTokens > 0 {
		st.lastTotalTokens[threadID] = totalInputTokens
		st.lastMessageCount[threadID] = messageCount
		st.systemFingerprints[threadID] = systemFingerprint
		st.toolsFingerprints[threadID] = toolsFingerprint
		st.messagesPrefixFingerprints[threadID] = messagesPrefixFingerprint
		st.lastRequestTime[threadID] = time.Now()
	} else {
		delete(st.lastTotalTokens, threadID)
		delete(st.lastMessageCount, threadID)
		delete(st.systemFingerprints, threadID)
		delete(st.toolsFingerprints, threadID)
		delete(st.messagesPrefixFingerprints, threadID)
		delete(st.lastRequestTime, threadID)
	}
	st.loadedFromDB[threadID] = true
	st.baselineGeneration[threadID]++
	if st.loadingFromDB[threadID] != nil {
		st.loadedFromDB[threadID] = false
	}
	persistFn := st.persistFn
	if persistFn != nil {
		if data, err := json.Marshal(state); err == nil {
			persistFn("sawtooth:"+threadID, string(data))
		}
	}
	st.mu.Unlock()
	return true
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
	st.requestSeq[threadID]++
	return st.requestSeq[threadID]
}

// GetRequestSeq 返回当前请求序号（不递增）。
func (st *SawtoothTrigger) GetRequestSeq(threadID string) int {
	st.mu.RLock()
	defer st.mu.RUnlock()
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
func (st *SawtoothTrigger) loadSawtoothFromDB(threadID string) {
	st.mu.Lock()
	if st.loadedFromDB[threadID] {
		st.mu.Unlock()
		return
	}
	if loading := st.loadingFromDB[threadID]; loading != nil {
		st.mu.Unlock()
		<-loading
		return
	}
	loading := make(chan struct{})
	st.loadingFromDB[threadID] = loading
	startGeneration := st.baselineGeneration[threadID]
	loadFn := st.loadFn
	st.mu.Unlock()

	var state persistedState
	valid := false
	if loadFn != nil {
		raw, ok := loadFn("sawtooth:" + threadID)
		if ok && raw != "" && json.Unmarshal([]byte(raw), &state) == nil && state.Tokens > 0 && state.MsgCount >= 0 {
			state.SystemFingerprint = sanitizePressureFingerprint(state.SystemFingerprint)
			state.ToolsFingerprint = sanitizePressureFingerprint(state.ToolsFingerprint)
			state.MessagesPrefixFingerprint = sanitizePressureFingerprint(state.MessagesPrefixFingerprint)
			valid = true
		}
	}

	st.mu.Lock()
	applied := false
	if valid && st.baselineGeneration[threadID] == startGeneration {
		if _, exists := st.lastTotalTokens[threadID]; !exists {
			st.lastTotalTokens[threadID] = state.Tokens
			st.lastMessageCount[threadID] = state.MsgCount
			st.systemFingerprints[threadID] = state.SystemFingerprint
			st.toolsFingerprints[threadID] = state.ToolsFingerprint
			st.messagesPrefixFingerprints[threadID] = state.MessagesPrefixFingerprint
			applied = true
		}
	}
	st.loadedFromDB[threadID] = true
	delete(st.loadingFromDB, threadID)
	close(loading)
	st.mu.Unlock()
	// 不设置 lastRequestTime —— 保持零值，ShouldTrigger 中 hasTime=false 会跳过 Pause 检查。
	// 下次 API 响应后 UpdateAfterResponse 才会设置真实时间。

	if applied {
		slog.Info("从 SQLite 恢复 Sawtooth 状态",
			"thread_id", threadID,
			"tokens", state.Tokens,
			"msg_count", state.MsgCount,
		)
	}
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

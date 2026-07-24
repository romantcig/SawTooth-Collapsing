package proxy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"sync"
)

const (
	historyEpochStateVersion      = 1
	historyMismatchDigestLimit    = 32
	historyMissingContentSentinel = "missing"
	historyEpochPersistencePrefix = "history_epoch:"
	historyEpochStateKeySeparator = ":history_epoch:"
)

// HistoryEpochReason 是历史连续性判定允许进入日志与 facts 的受限原因。
// 它只描述可证明的结构事实，不推断撤回、人工编辑或分支来源。
type HistoryEpochReason string

const (
	HistoryEpochReasonInitial      HistoryEpochReason = "initial"
	HistoryEpochReasonUnchanged    HistoryEpochReason = "unchanged"
	HistoryEpochReasonAppended     HistoryEpochReason = "appended"
	HistoryEpochReasonReuseChanged HistoryEpochReason = "reuse_changed"
	HistoryEpochReasonShrunk       HistoryEpochReason = "history_shrunk"
	HistoryEpochReasonChanged      HistoryEpochReason = "history_changed"
	HistoryEpochReasonInvalid      HistoryEpochReason = "invalid_history"
)

// historyFingerprints 分离两个用途不同的历史证明：
//   - Reuse：Frozen/pressure 对 raw wire 前缀的完整复用安全证明；
//   - Input：只用于判断用户可见输入历史是否仍连续。
//
// 两组均只保存逐消息 SHA-256 与其聚合 SHA-256，不保存正文。
type historyFingerprints struct {
	ReuseMessageHashes []string
	InputMessageHashes []string
	ReuseHash          string
	InputHash          string
}

// HistoryEpochDecision 固定一次请求进入有状态管线时的本地 epoch 与状态键。
type HistoryEpochDecision struct {
	Epoch         uint64
	StateKey      string
	CommonPrefix  int
	ReuseSafe     bool
	EpochChanged  bool
	FirstMismatch bool
	Reason        HistoryEpochReason
}

type historyEpochPersisted struct {
	Version               int      `json:"version"`
	Epoch                 uint64   `json:"epoch"`
	Valid                 bool     `json:"valid"`
	ReuseMessageHashes    []string `json:"reuse_message_hashes,omitempty"`
	InputMessageHashes    []string `json:"input_message_hashes,omitempty"`
	ReuseHash             string   `json:"reuse_hash,omitempty"`
	InputHash             string   `json:"input_hash,omitempty"`
	RecentMismatchDigests []string `json:"recent_mismatch_digests,omitempty"`
}

type historyEpochSession struct {
	mu     sync.Mutex
	loaded bool
	state  historyEpochPersisted
}

// HistoryEpochManager 为每个 session 串行提交一份权威 history 状态。
// 不同 session 使用独立锁，慢 SQLite 写入不会阻塞其他会话。
type HistoryEpochManager struct {
	mu        sync.Mutex
	sessions  map[string]*historyEpochSession
	configMu  sync.RWMutex
	persistFn PersistFunc
	loadFn    LoadFunc
}

func NewHistoryEpochManager() *HistoryEpochManager {
	return &HistoryEpochManager{sessions: make(map[string]*historyEpochSession)}
}

func (m *HistoryEpochManager) SetPersistFunc(fn PersistFunc) {
	if m == nil {
		return
	}
	m.configMu.Lock()
	m.persistFn = fn
	m.configMu.Unlock()
}

func (m *HistoryEpochManager) SetLoadFunc(fn LoadFunc) {
	if m == nil {
		return
	}
	m.configMu.Lock()
	m.loadFn = fn
	m.configMu.Unlock()
}

// Begin 在单个 session 锁内完成 load、比较、epoch 推进与持久化。
// 无法规范化的输入不会沿用任何旧状态；它会建立新的 fail-closed epoch。
func (m *HistoryEpochManager) Begin(sessionID string, messages []Message) HistoryEpochDecision {
	if m == nil {
		return HistoryEpochDecision{
			Epoch:        1,
			StateKey:     historyEpochStateKey(sessionID, 1),
			ReuseSafe:    false,
			EpochChanged: true,
			Reason:       HistoryEpochReasonInvalid,
		}
	}

	session := m.session(sessionID)
	session.mu.Lock()
	defer session.mu.Unlock()
	m.loadSessionLocked(sessionID, session)

	current, canonicalErr := canonicalizeHistory(messages)
	previous := session.state
	decision := HistoryEpochDecision{ReuseSafe: true}

	if previous.Epoch == 0 {
		decision.Epoch = 1
		decision.EpochChanged = true
		decision.Reason = HistoryEpochReasonInitial
		if canonicalErr != nil {
			decision.Reason = HistoryEpochReasonInvalid
			decision.ReuseSafe = false
			decision.FirstMismatch = true
		}
	} else if canonicalErr != nil || !previous.Valid {
		decision.Epoch = previous.Epoch + 1
		decision.EpochChanged = true
		decision.Reason = HistoryEpochReasonInvalid
		decision.ReuseSafe = false
		decision.FirstMismatch = true
	} else {
		decision.Epoch = previous.Epoch
		decision.CommonPrefix = commonHashPrefix(previous.InputMessageHashes, current.InputMessageHashes)
		inputContinuous := decision.CommonPrefix == len(previous.InputMessageHashes) &&
			len(current.InputMessageHashes) >= len(previous.InputMessageHashes)
		sameLength := len(current.InputMessageHashes) == len(previous.InputMessageHashes)

		if inputContinuous {
			decision.ReuseSafe = prefixHashesEqual(previous.ReuseMessageHashes, current.ReuseMessageHashes, len(previous.ReuseMessageHashes))
			switch {
			case !decision.ReuseSafe:
				decision.Reason = HistoryEpochReasonReuseChanged
			case sameLength:
				decision.Reason = HistoryEpochReasonUnchanged
			default:
				decision.Reason = HistoryEpochReasonAppended
			}
		} else {
			decision.Epoch++
			decision.EpochChanged = true
			decision.ReuseSafe = false
			if len(current.InputMessageHashes) < len(previous.InputMessageHashes) &&
				decision.CommonPrefix == len(current.InputMessageHashes) {
				decision.Reason = HistoryEpochReasonShrunk
			} else {
				decision.Reason = HistoryEpochReasonChanged
			}
		}
	}

	decision.StateKey = historyEpochStateKey(sessionID, decision.Epoch)
	next := historyEpochPersisted{
		Version: historyEpochStateVersion,
		Epoch:   decision.Epoch,
		Valid:   canonicalErr == nil,
	}
	if canonicalErr == nil {
		next.ReuseMessageHashes = append([]string(nil), current.ReuseMessageHashes...)
		next.InputMessageHashes = append([]string(nil), current.InputMessageHashes...)
		next.ReuseHash = current.ReuseHash
		next.InputHash = current.InputHash
	}
	next.RecentMismatchDigests = append([]string(nil), previous.RecentMismatchDigests...)
	if decision.Reason == HistoryEpochReasonReuseChanged || decision.EpochChanged && previous.Epoch != 0 {
		digest := historyMismatchDigest(previous, next, decision.CommonPrefix, decision.Reason)
		decision.FirstMismatch = !containsHistoryDigest(next.RecentMismatchDigests, digest)
		if decision.FirstMismatch {
			next.RecentMismatchDigests = append(next.RecentMismatchDigests, digest)
			if len(next.RecentMismatchDigests) > historyMismatchDigestLimit {
				next.RecentMismatchDigests = append([]string(nil), next.RecentMismatchDigests[len(next.RecentMismatchDigests)-historyMismatchDigestLimit:]...)
			}
		}
	}

	session.state = next
	m.persistSessionLocked(sessionID, next)
	return decision
}

// IsCurrent 是响应写回前的外层 epoch 门禁。
func (m *HistoryEpochManager) IsCurrent(sessionID string, epoch uint64) bool {
	if m == nil || epoch == 0 {
		return false
	}
	session := m.session(sessionID)
	session.mu.Lock()
	defer session.mu.Unlock()
	m.loadSessionLocked(sessionID, session)
	return session.state.Epoch == epoch
}

func (m *HistoryEpochManager) session(sessionID string) *historyEpochSession {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.sessions == nil {
		m.sessions = make(map[string]*historyEpochSession)
	}
	session := m.sessions[sessionID]
	if session == nil {
		session = &historyEpochSession{}
		m.sessions[sessionID] = session
	}
	return session
}

func (m *HistoryEpochManager) loadSessionLocked(sessionID string, session *historyEpochSession) {
	if session.loaded {
		return
	}
	session.loaded = true
	m.configMu.RLock()
	loadFn := m.loadFn
	m.configMu.RUnlock()
	if loadFn == nil {
		return
	}
	raw, ok := loadFn(historyEpochPersistenceKey(sessionID))
	if !ok || raw == "" {
		return
	}
	var persisted historyEpochPersisted
	if json.Unmarshal([]byte(raw), &persisted) != nil || persisted.Epoch == 0 {
		return
	}
	if persisted.Version != historyEpochStateVersion || !validHistoryEpochState(persisted) {
		// 保留已持久化的单调 epoch，但把不可证明的旧内容标记为无效。
		persisted = historyEpochPersisted{
			Version: historyEpochStateVersion,
			Epoch:   persisted.Epoch,
			Valid:   false,
		}
	}
	session.state = persisted
}

func (m *HistoryEpochManager) persistSessionLocked(sessionID string, state historyEpochPersisted) {
	m.configMu.RLock()
	persistFn := m.persistFn
	m.configMu.RUnlock()
	if persistFn == nil {
		return
	}
	data, err := json.Marshal(state)
	if err != nil {
		return
	}
	persistFn(historyEpochPersistenceKey(sessionID), string(data))
}

func validHistoryEpochState(state historyEpochPersisted) bool {
	for _, digest := range state.RecentMismatchDigests {
		if !validPressureFingerprint(digest) {
			return false
		}
	}
	if len(state.RecentMismatchDigests) > historyMismatchDigestLimit {
		return false
	}
	if !state.Valid {
		return len(state.ReuseMessageHashes) == 0 && len(state.InputMessageHashes) == 0 && state.ReuseHash == "" && state.InputHash == ""
	}
	if len(state.ReuseMessageHashes) != len(state.InputMessageHashes) ||
		!validPressureFingerprint(state.ReuseHash) || !validPressureFingerprint(state.InputHash) {
		return false
	}
	for _, hash := range append(append([]string(nil), state.ReuseMessageHashes...), state.InputMessageHashes...) {
		if !validPressureFingerprint(hash) {
			return false
		}
	}
	return hashStringList(state.ReuseMessageHashes) == state.ReuseHash &&
		hashStringList(state.InputMessageHashes) == state.InputHash
}

func historyEpochPersistenceKey(sessionID string) string {
	return historyEpochPersistencePrefix + sessionID
}

func historyEpochStateKey(sessionID string, epoch uint64) string {
	// epoch 1 复用既有 session key 作为无损冷启动兼容别名；
	// 一旦发生真实分叉，epoch 2+ 使用独立命名空间，旧状态永不回流。
	if epoch == 1 {
		return sessionID
	}
	return sessionID + historyEpochStateKeySeparator + strconv.FormatUint(epoch, 10)
}

func historyMismatchDigest(previous, current historyEpochPersisted, commonPrefix int, reason HistoryEpochReason) string {
	payload := struct {
		PreviousInput string             `json:"previous_input"`
		CurrentInput  string             `json:"current_input"`
		PreviousReuse string             `json:"previous_reuse"`
		CurrentReuse  string             `json:"current_reuse"`
		CommonPrefix  int                `json:"common_prefix"`
		Reason        HistoryEpochReason `json:"reason"`
	}{
		PreviousInput: previous.InputHash,
		CurrentInput:  current.InputHash,
		PreviousReuse: previous.ReuseHash,
		CurrentReuse:  current.ReuseHash,
		CommonPrefix:  commonPrefix,
		Reason:        reason,
	}
	data, _ := json.Marshal(payload)
	return sha256hex(data)
}

func containsHistoryDigest(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func commonHashPrefix(left, right []string) int {
	limit := len(left)
	if len(right) < limit {
		limit = len(right)
	}
	for index := 0; index < limit; index++ {
		if left[index] != right[index] {
			return index
		}
	}
	return limit
}

func prefixHashesEqual(previous, current []string, count int) bool {
	if count < 0 || count > len(previous) || count > len(current) {
		return false
	}
	for index := 0; index < count; index++ {
		if previous[index] != current[index] {
			return false
		}
	}
	return true
}

// canonicalizeHistory 对每条 Message 仅在明确的 Anthropic schema 位置做规范化。
func canonicalizeHistory(messages []Message) (historyFingerprints, error) {
	result := historyFingerprints{
		ReuseMessageHashes: make([]string, 0, len(messages)),
		InputMessageHashes: make([]string, 0, len(messages)),
	}
	for index, message := range messages {
		reuseMessage, err := canonicalHistoryMessage(message, false)
		if err != nil {
			return historyFingerprints{}, fmt.Errorf("规范化历史消息 %d reuse 指纹: %w", index, err)
		}
		inputMessage, err := canonicalHistoryMessage(message, true)
		if err != nil {
			return historyFingerprints{}, fmt.Errorf("规范化历史消息 %d input 指纹: %w", index, err)
		}
		reuseBytes, err := json.Marshal(reuseMessage)
		if err != nil {
			return historyFingerprints{}, err
		}
		inputBytes, err := json.Marshal(inputMessage)
		if err != nil {
			return historyFingerprints{}, err
		}
		result.ReuseMessageHashes = append(result.ReuseMessageHashes, sha256hex(reuseBytes))
		result.InputMessageHashes = append(result.InputMessageHashes, sha256hex(inputBytes))
	}
	result.ReuseHash = hashStringList(result.ReuseMessageHashes)
	result.InputHash = hashStringList(result.InputMessageHashes)
	return result, nil
}

// reuseSafetyPrefixHash 返回 current raw history[:count] 的完整复用安全证明。
func reuseSafetyPrefixHash(messages []Message, count int) string {
	if count < 0 || count > len(messages) {
		return ""
	}
	fingerprints, err := canonicalizeHistory(messages[:count])
	if err != nil {
		return ""
	}
	return fingerprints.ReuseHash
}

func canonicalHistoryMessage(message Message, inputHistory bool) (map[string]any, error) {
	result := make(map[string]any, len(message.extraFields)+2)
	result["role"] = message.Role

	trimmed := bytes.TrimSpace(message.Content)
	if len(trimmed) == 0 {
		result["content"] = map[string]any{"$history_state": historyMissingContentSentinel}
	} else {
		content, err := decodeHistoryJSON(trimmed)
		if err != nil {
			return nil, fmt.Errorf("content: %w", err)
		}
		result["content"] = normalizeHistoryContent(content, message.Role, inputHistory)
	}

	for key, raw := range message.extraFields {
		value, err := decodeHistoryJSON(raw)
		if err != nil {
			return nil, fmt.Errorf("未知字段 %q: %w", key, err)
		}
		result[key] = value
	}
	return result, nil
}

func normalizeHistoryContent(content any, role string, inputHistory bool) any {
	blocks, ok := content.([]any)
	if !ok {
		return content
	}
	normalized := make([]any, len(blocks))
	for index, block := range blocks {
		object, isObject := block.(map[string]any)
		if !isObject {
			normalized[index] = block
			continue
		}
		copyObject := cloneHistoryObject(object)
		typeName, _ := copyObject["type"].(string)
		// 只有已知 Anthropic content block 的直接 cache_control 是缓存元数据。
		// 未知 block 类型保持 fail-closed，避免把未来业务字段误删。
		if knownHistoryContentBlockType(typeName) {
			delete(copyObject, "cache_control")
		}
		if inputHistory {
			if role == "assistant" && typeName == "thinking" {
				delete(copyObject, "thinking")
				delete(copyObject, "signature")
			}
			if value, exists := copyObject["input"]; exists && value == nil {
				switch typeName {
				case "text", "thinking", "tool_result":
					delete(copyObject, "input")
				}
			}
		}
		normalized[index] = copyObject
	}

	if len(normalized) == 1 {
		if block, ok := normalized[0].(map[string]any); ok && len(block) == 2 && block["type"] == "text" {
			if text, ok := block["text"].(string); ok {
				return text
			}
		}
	}
	return normalized
}

func knownHistoryContentBlockType(typeName string) bool {
	switch typeName {
	case "text", "image", "document", "thinking", "redacted_thinking", "tool_use", "tool_result":
		return true
	default:
		return false
	}
}

func cloneHistoryObject(source map[string]any) map[string]any {
	result := make(map[string]any, len(source))
	for key, value := range source {
		result[key] = cloneHistoryValue(value)
	}
	return result
}

func cloneHistoryValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		return cloneHistoryObject(typed)
	case []any:
		result := make([]any, len(typed))
		for index, item := range typed {
			result[index] = cloneHistoryValue(item)
		}
		return result
	default:
		return value
	}
}

func decodeHistoryJSON(raw []byte) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("包含多个 JSON 值")
		}
		return nil, err
	}
	return value, nil
}

func hashStringList(values []string) string {
	data, _ := json.Marshal(values)
	return sha256hex(data)
}

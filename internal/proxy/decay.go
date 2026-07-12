package proxy

import (
	"encoding/json"
	"fmt"
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
	// 当前活跃路径集合——引用这些路径的消息衰减更慢
	pinnedPaths  map[string]bool
	persistFn    PersistFunc
	loadFn       LoadFunc
	loadedFromDB map[string]bool
}

// NewDecayTracker 创建新的衰减追踪器。
func NewDecayTracker() *DecayTracker {
	return &DecayTracker{
		stubbedAt:    make(map[string]int),
		intensity:    make(map[string]float64),
		filePaths:    make(map[string]string),
		pinnedPaths:  make(map[string]bool),
		loadedFromDB: make(map[string]bool),
	}
}

// SetPersistFunc 设置持久化回调。
func (d *DecayTracker) SetPersistFunc(fn PersistFunc) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.persistFn = fn
}

// SetLoadFunc 设置冷启动加载回调。
func (d *DecayTracker) SetLoadFunc(fn LoadFunc) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.loadFn = fn
}

// MarkStubbed 记录一条消息在指定请求序号被桩化。
// 仅记录首次桩化，后续调用不做更新。
// emotionalIntensity 是桩化时的情绪强度（0.0-1.0），影响衰减速度。
// key 格式: "sessionID:msg_N"（session-scoped，多 session 并发安全）。
func (d *DecayTracker) MarkStubbed(sessionID string, msgIndex, requestIdx int, emotionalIntensity float64) {
	key := fmt.Sprintf("%s:msg_%d", sessionID, msgIndex)
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, exists := d.stubbedAt[key]; !exists {
		d.stubbedAt[key] = requestIdx
		d.intensity[key] = emotionalIntensity
	}
}

// SetFilePath 记录桩化消息关联的文件路径。
// key 格式: "sessionID:msg_N"。
func (d *DecayTracker) SetFilePath(sessionID string, msgIndex int, path string) {
	if path == "" {
		return
	}
	key := fmt.Sprintf("%s:msg_%d", sessionID, msgIndex)
	d.mu.Lock()
	defer d.mu.Unlock()
	d.filePaths[key] = path
}

// SetPinnedPaths 更新"活跃路径"集合——引用这些路径的消息衰减更慢。
// 在每次 stubify 之前调用。无 daemon 替代方案：从当前请求的 tool_use 块提取。
func (d *DecayTracker) SetPinnedPaths(paths []string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.pinnedPaths = make(map[string]bool, len(paths))
	for _, p := range paths {
		d.pinnedPaths[p] = true
	}
}

// isPinnedPath 检查文件路径是否匹配任意活跃路径。
// 双向后缀匹配：短路径可匹配长路径的尾部和反向。
func (d *DecayTracker) isPinnedPath(filePath string) bool {
	if filePath == "" || len(d.pinnedPaths) == 0 {
		return false
	}
	if d.pinnedPaths[filePath] {
		return true
	}
	for pp := range d.pinnedPaths {
		if strings.HasSuffix(filePath, pp) || strings.HasSuffix(pp, filePath) {
			return true
		}
	}
	return false
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
	key := fmt.Sprintf("%s:msg_%d", sessionID, msgIndex)
	d.mu.RLock()
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
	d.mu.RLock()
	fn := d.persistFn
	if fn == nil {
		d.mu.RUnlock()
		return
	}
	dp := decayPersisted{
		StubbedAt: make(map[string]int),
		Intensity: make(map[string]float64),
		FilePaths: make(map[string]string),
	}
	for k, v := range d.stubbedAt {
		dp.StubbedAt[k] = v
	}
	for k, v := range d.intensity {
		dp.Intensity[k] = v
	}
	for k, v := range d.filePaths {
		dp.FilePaths[k] = v
	}
	d.mu.RUnlock()

	if data, err := json.Marshal(dp); err == nil {
		fn("decay:"+threadID, string(data))
	}
}

// LoadFromDB 从 DB 恢复衰减状态。每个 thread 仅加载一次。
func (d *DecayTracker) LoadFromDB(threadID string) {
	d.mu.Lock()
	if d.loadedFromDB[threadID] {
		d.mu.Unlock()
		return
	}
	d.loadedFromDB[threadID] = true
	loadFn := d.loadFn
	d.mu.Unlock()

	if loadFn == nil {
		return
	}

	raw, ok := loadFn("decay:" + threadID)
	if !ok || raw == "" {
		return
	}

	var dp decayPersisted
	if err := json.Unmarshal([]byte(raw), &dp); err != nil {
		return
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	for k, v := range dp.StubbedAt {
		if _, exists := d.stubbedAt[k]; !exists {
			d.stubbedAt[k] = v
		}
	}
	for k, v := range dp.Intensity {
		if _, exists := d.intensity[k]; !exists {
			d.intensity[k] = v
		}
	}
	for k, v := range dp.FilePaths {
		if _, exists := d.filePaths[k]; !exists {
			d.filePaths[k] = v
		}
	}
}

// ClearSession 清空指定 session 的全部衰减追踪状态。
// collapse 重建消息数组后调用——旧 key 不再有效。
// key 格式为 "sessionID:msg_N"，ClearSession 按前缀匹配删除。
func (d *DecayTracker) ClearSession(sessionID string) {
	d.mu.Lock()
	defer d.mu.Unlock()

	prefix := sessionID + ":"
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
	d.PersistUnlocked(sessionID)
}

// PersistUnlocked 在已持有锁的情况下持久化到 DB。
func (d *DecayTracker) PersistUnlocked(threadID string) {
	if d.persistFn == nil {
		return
	}
	dp := decayPersisted{
		StubbedAt: d.stubbedAt,
		Intensity: d.intensity,
		FilePaths: d.filePaths,
	}
	if data, err := json.Marshal(dp); err == nil {
		d.persistFn("decay:"+threadID, string(data))
	}
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
// 先估算 intensity、提取 pinnedPaths、计算压力，然后对每条消息按
// DecayTracker 记录的历史阶段应用衰减。
// 返回处理后的消息和基于压力的整体阶段（供下游 collapse 决策使用）。
func (d *DecayTracker) ApplyDecayBatch(messages []Message, sessionID string, totalTokens int, threshold int, tc *TokenCounter, pivotText string, requestIdx int) ([]Message, DecayPhase) {
	pressure := float64(totalTokens) / float64(threshold)

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
		stage := d.GetStage(sessionID, i, requestIdx, threadLen, pressure)
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
	d.Persist(sessionID)

	return result, overallPhase
}

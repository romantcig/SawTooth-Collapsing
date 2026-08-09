package proxy

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
)

const minCompactableRun = 50

// compactableRun 表示一段可合并的连续 Stage-3 消息区间（[start, end] 闭区间）。
type compactableRun struct {
	start int
	end   int
}

// compactionStats 保存被合并消息区间的统计摘要。
type compactionStats struct {
	fileStats string
	toolStats string
}

// CompactedBlock 保存一次合并操作的元数据，供调用方（DB 持久化/日志）使用。
// Phase C v1 不持久化，仅供 future phases 扩展。
type CompactedBlock struct {
	StartIdx int
	EndIdx   int
	Content  string
	Role     string
}

// CompactionReplacement 是一次已经通过角色、坐标和正文布局预检的替换。
// Content 在 plan 构建时复制；物化阶段不再读取 tracker 或重新发现 run。
type CompactionReplacement struct {
	RunID    int
	StartIdx int
	EndIdx   int
	Role     string
	SplitIdx int
	Content  json.RawMessage
}

// CompactionRun 是 immutable plan 中的一段连续 Stage-3 区间。
type CompactionRun struct {
	StartIdx       int
	EndIdx         int
	ReplacementIDs []int
}

// CompactionPlan 固定一次请求的阶段评估、保护集合和 replacement layout。
// 构造完成后生产代码不修改其字段；所有 map/slice 都由 builder 深拷贝，
// Stage2Messages 作为物化失败时的恢复材料保存。
type CompactionPlan struct {
	EffectiveEnabled bool
	MessageCount     int
	ScanStart        int
	ScanEnd          int
	Protected        map[int]struct{}
	Stages           []DecayPhase
	Stage2Messages   []Message
	Runs             []CompactionRun
	Replacements     []CompactionReplacement
	Snapshot         DecayEvaluationSnapshot
}

// ReplacementCount 返回已预检 replacement 数量。
func (p *CompactionPlan) ReplacementCount() int {
	if p == nil {
		return 0
	}
	return len(p.Replacements)
}

func (p *CompactionPlan) IsProtected(index int) bool {
	if p == nil {
		return false
	}
	_, ok := p.Protected[index]
	return ok
}

// IsCovered 只依据 plan 中的 replacement 区间判断，不读取 tracker。
func (p *CompactionPlan) IsCovered(index int) bool {
	if p == nil || !p.EffectiveEnabled {
		return false
	}
	for _, replacement := range p.Replacements {
		if index >= replacement.StartIdx && index <= replacement.EndIdx {
			return true
		}
	}
	return false
}

func (p *CompactionPlan) StageAt(index int) DecayPhase {
	if p == nil || index < 0 || index >= len(p.Stages) {
		return DecayFresh
	}
	return p.Stages[index]
}

func (p *CompactionPlan) Stage2Message(index int) (Message, bool) {
	if p == nil || index < 0 || index >= len(p.Stage2Messages) {
		return Message{}, false
	}
	return cloneMessageForPlan(p.Stage2Messages[index]), true
}

func (p *CompactionPlan) SnapshotPinned(path string) bool {
	if p == nil {
		return false
	}
	return p.Snapshot.SnapshotPinned(path)
}

func cloneMessageForPlan(message Message) Message {
	clone := message
	clone.Content = append(json.RawMessage(nil), message.Content...)
	if message.extraFields != nil {
		clone.extraFields = make(map[string]json.RawMessage, len(message.extraFields))
		for key, raw := range message.extraFields {
			clone.extraFields[key] = append(json.RawMessage(nil), raw...)
		}
	}
	return clone
}

func cloneMessagesForPlan(messages []Message) []Message {
	if messages == nil {
		return nil
	}
	result := make([]Message, len(messages))
	for i, message := range messages {
		result[i] = cloneMessageForPlan(message)
	}
	return result
}

// compactionProtectedIndices 是唯一的保护集合 helper。messages[0]、keepRecent、
// active tool pair、pinned path 和高 intensity 决策项都在这里合并，避免 proxy
// 与 compaction 各自维护不同的 scan boundary。
func compactionProtectedIndices(messages []Message, snapshot DecayEvaluationSnapshot, keepRecent int) map[int]struct{} {
	protected := make(map[int]struct{})
	if len(messages) == 0 {
		return protected
	}
	protected[0] = struct{}{}
	if keepRecent < 0 {
		keepRecent = 0
	}
	if keepRecent > len(messages) {
		keepRecent = len(messages)
	}
	for i := len(messages) - keepRecent; i < len(messages); i++ {
		if i >= 0 {
			protected[i] = struct{}{}
		}
	}
	activeAssistant, activeResult := activeToolPairIndices(messages)
	if activeAssistant >= 0 {
		protected[activeAssistant] = struct{}{}
	}
	if activeResult >= 0 {
		protected[activeResult] = struct{}{}
	}
	for i := range messages {
		key := snapshot.stateKeyForIndex(i)
		if snapshot.PinnedPaths.contains(snapshot.FilePaths[key]) || snapshot.Intensity[key] >= 0.9 {
			protected[i] = struct{}{}
		}
	}
	return protected
}

func stage2MessageForPlan(message Message) Message {
	result := cloneMessageForPlan(message)
	blocks, isArray := parseContent(result.Content)
	changed := false
	for i := range blocks {
		switch blocks[i].Type {
		case "text":
			decayed := decayToStage2(blocks[i].Text, result.Role)
			if decayed != blocks[i].Text {
				changed = true
				blocks[i].Text = decayed
			}
		case "tool_use", "tool_result":
			if blocks[i].Text != "" {
				decayed := ApplyDecayToToolStub(blocks[i].Text, DecayOld)
				if decayed != blocks[i].Text {
					changed = true
					blocks[i].Text = decayed
				}
			}
		}
	}
	if changed {
		result.Content = rebuildContent(blocks, isArray)
	}
	return result
}

func stage2MessagesForPlan(messages []Message) []Message {
	result := cloneMessagesForPlan(messages)
	for i := range result {
		result[i] = stage2MessageForPlan(result[i])
	}
	return result
}

func appendPlanRun(plan *CompactionPlan, run compactableRun, replacements []CompactionReplacement) {
	runID := len(plan.Runs)
	planRun := CompactionRun{StartIdx: run.start, EndIdx: run.end}
	for i := range replacements {
		replacements[i].RunID = runID
		planRun.ReplacementIDs = append(planRun.ReplacementIDs, len(plan.Replacements))
		plan.Replacements = append(plan.Replacements, replacements[i])
	}
	plan.Runs = append(plan.Runs, planRun)
}

// BuildCompactionPlan 以 snapshot 为唯一阶段输入，一次性完成保护集合、run
// 发现和 replacement layout 预检。只有可实际物化到 original/decayed 坐标的
// replacement 才会进入 plan；其他 Stage-3 候选保留 Stage-2 恢复材料。
func BuildCompactionPlan(snapshot DecayEvaluationSnapshot, messages, original []Message, keepRecent int, compactEnabled bool) *CompactionPlan {
	snapshot = snapshot.clone()
	plan := &CompactionPlan{
		EffectiveEnabled: compactEnabled,
		MessageCount:     len(messages),
		ScanStart:        1,
		ScanEnd:          -1,
		Protected:        compactionProtectedIndices(messages, snapshot, keepRecent),
		Stages:           make([]DecayPhase, len(messages)),
		Stage2Messages:   stage2MessagesForPlan(messages),
		Snapshot:         snapshot,
	}
	for i := range messages {
		plan.Stages[i] = snapshot.stageAt(i)
		if plan.IsProtected(i) && plan.Stages[i] == DecayCompacted {
			plan.Stages[i] = DecayOld
		}
	}
	if len(messages) == 0 {
		return plan
	}
	if keepRecent < 0 {
		keepRecent = 0
	}
	if keepRecent > len(messages) {
		keepRecent = len(messages)
	}
	plan.ScanEnd = len(messages) - keepRecent - 1
	if plan.ScanEnd < plan.ScanStart || !compactEnabled {
		return plan
	}

	var runStart = -1
	flushRun := func(end int) {
		if runStart < 0 {
			return
		}
		run := compactableRun{start: runStart, end: end}
		if run.end-run.start+1 < minCompactableRun {
			runStart = -1
			return
		}
		leftRole := ""
		if run.start > 0 && run.start-1 < len(messages) {
			leftRole = messages[run.start-1].Role
		}
		rightRole := ""
		if run.end+1 >= 0 && run.end+1 < len(messages) {
			rightRole = messages[run.end+1].Role
		}
		roles := compactedRolesForNeighbors(leftRole, rightRole)
		if len(roles) == 0 || run.start < 0 || run.end >= len(messages) || run.end >= len(original) {
			runStart = -1
			return
		}
		var replacements []CompactionReplacement
		switch len(roles) {
		case 1:
			message := buildCompactedMessage(original, run.start, run.end, roles[0])
			if len(message.Content) == 0 || !json.Valid(message.Content) {
				runStart = -1
				return
			}
			replacements = append(replacements, CompactionReplacement{StartIdx: run.start, EndIdx: run.end, Role: roles[0], SplitIdx: -1, Content: append(json.RawMessage(nil), message.Content...)})
		case 2:
			mid := run.start + (run.end-run.start)/2
			if mid < run.start || mid >= run.end {
				runStart = -1
				return
			}
			first := buildCompactedMessage(original, run.start, mid, roles[0])
			second := buildCompactedMessage(original, mid+1, run.end, roles[1])
			if len(first.Content) == 0 || len(second.Content) == 0 || !json.Valid(first.Content) || !json.Valid(second.Content) {
				runStart = -1
				return
			}
			replacements = append(replacements,
				CompactionReplacement{StartIdx: run.start, EndIdx: mid, Role: roles[0], SplitIdx: mid, Content: append(json.RawMessage(nil), first.Content...)},
				CompactionReplacement{StartIdx: mid + 1, EndIdx: run.end, Role: roles[1], SplitIdx: mid, Content: append(json.RawMessage(nil), second.Content...)},
			)
		}
		appendPlanRun(plan, run, replacements)
		runStart = -1
	}

	for i := plan.ScanStart; i <= plan.ScanEnd; i++ {
		if plan.Stages[i] == DecayCompacted && !plan.IsProtected(i) {
			if runStart < 0 {
				runStart = i
			}
			continue
		}
		flushRun(i - 1)
	}
	flushRun(plan.ScanEnd)
	return plan
}

// BuildCompactionPlanFromTracker 是生产链可直接使用的 convenience API；它
// 先复制 snapshot，再构建 plan，调用方不需要暴露 tracker 给 CompactMessages。
func BuildCompactionPlanFromTracker(tracker *DecayTracker, stateKey string, pinned PinnedPathSnapshot, messages, original []Message, requestIdx int, totalTokens, threshold, keepRecent int, compactEnabled bool) *CompactionPlan {
	pressure := 0.0
	if threshold > 0 {
		pressure = float64(totalTokens) / float64(threshold)
	}
	snapshot := DecayEvaluationSnapshot{StateKey: stateKey, RequestIdx: requestIdx, ThreadLen: len(messages), Pressure: pressure, PinnedPaths: NewPinnedPathSnapshot(pinned), StubbedAt: make(map[string]int), Intensity: make(map[string]float64), FilePaths: make(map[string]string)}
	if tracker != nil {
		snapshot = tracker.BuildDecayEvaluationSnapshot(stateKey, pinned, requestIdx, len(messages), pressure)
	}
	return BuildCompactionPlan(snapshot, messages, original, keepRecent, compactEnabled)
}

// findCompactableRuns 扫描 DecayTracker，找出所有长度 ≥ minCompactableRun 的
// 连续 Stage-3（DecayCompacted）消息区间。
// scanStart/scanEnd 定义扫描范围（闭区间）。
func findCompactableRuns(dt *DecayTracker, sessionID string, msgCount, scanStart, scanEnd, requestIdx int, pressure float64) []compactableRun {
	if dt == nil {
		return nil
	}

	var runs []compactableRun
	runStart := -1

	for i := scanStart; i <= scanEnd; i++ {
		stage := dt.GetStage(sessionID, i, requestIdx, msgCount, pressure)
		if stage == DecayCompacted {
			if runStart == -1 {
				runStart = i
			}
		} else {
			if runStart != -1 && (i-1)-runStart+1 >= minCompactableRun {
				runs = append(runs, compactableRun{start: runStart, end: i - 1})
			}
			runStart = -1
		}
	}
	// 收尾：最后一个 run 延续到 scanEnd
	if runStart != -1 && scanEnd-runStart+1 >= minCompactableRun {
		runs = append(runs, compactableRun{start: runStart, end: scanEnd})
	}

	return runs
}

// extractCompactionStats 从原始消息中提取文件和工具使用统计。
// 使用 original messages（桩化前）以获取完整的 tool_use.Input 信息。
func extractCompactionStats(messages []Message, start, end int) compactionStats {
	toolCounts := make(map[string]int)
	fileCounts := make(map[string]int)

	for i := start; i <= end && i < len(messages); i++ {
		blocks, _ := parseContent(messages[i].Content)
		for _, b := range blocks {
			if b.Type != "tool_use" {
				continue
			}
			if b.Name != "" {
				toolCounts[b.Name]++
			}
			// 提取文件路径（对齐 YesMem: "file_path", "path"）
			for _, key := range []string{"file_path", "path"} {
				if v, ok := b.Input[key].(string); ok && v != "" {
					fileCounts[filepath.Base(v)]++
				}
			}
		}
	}

	return compactionStats{
		fileStats: formatCompactionCounts(fileCounts),
		toolStats: formatCompactionCounts(toolCounts),
	}
}

// buildCompactedContent 生成合并块的替换文本。
// 格式对齐 YesMem compaction.go:123-141：
//
//	[Compacted: Messages X-Y (N msgs)]
//	Files: main.go(5), proxy.go(3)
//	Tools: Edit(8), Read(3), Bash(2)
func buildCompactedContent(start, end int, stats compactionStats) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "[Compacted: Messages %d-%d (%d msgs)]", start, end, end-start+1)

	if stats.fileStats != "" {
		fmt.Fprintf(&sb, "\nFiles: %s", stats.fileStats)
	}
	if stats.toolStats != "" {
		fmt.Fprintf(&sb, "\nTools: %s", stats.toolStats)
	}

	return sb.String()
}

// formatCompactionCounts 将计数 map 格式化为 "key1(N), key2(N)" 字符串。
// 按计数降序排列，同计数按 key 字母序（确定性输出）。
// 最多展示前 5 名，超出则追加 "+N more"。
func formatCompactionCounts(counts map[string]int) string {
	if len(counts) == 0 {
		return ""
	}

	type kv struct {
		key   string
		count int
	}
	var pairs []kv
	for k, v := range counts {
		pairs = append(pairs, kv{k, v})
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].count != pairs[j].count {
			return pairs[i].count > pairs[j].count
		}
		return pairs[i].key < pairs[j].key
	})

	limit := 5
	if len(pairs) < limit {
		limit = len(pairs)
	}

	var parts []string
	for _, p := range pairs[:limit] {
		parts = append(parts, fmt.Sprintf("%s(%d)", p.key, p.count))
	}
	if len(pairs) > 5 {
		parts = append(parts, fmt.Sprintf("+%d more", len(pairs)-5))
	}
	return strings.Join(parts, ", ")
}

// compactedRolesForNeighbors 依据 run 两侧邻居的实际角色，决定该 run 应替换成
// 几条合并消息、各是什么角色。
//
// 非 user/assistant 的邻居不参与交替约束，规范化为 "" 表示无约束——实测
// role:"system" 占 14.7% 且是 CC 原生就发在 messages 数组里的，任何
// 「严格 user/assistant 交替」的推断都不成立。
//
//	lc == "" 且 rc == "" → ["user"]
//	lc == ""             → [opposite(rc)]
//	rc == ""             → [opposite(lc)]
//	lc == rc             → [opposite(lc)]
//	lc != rc             → [opposite(lc), opposite(rc)]
//
// 返回前自检序列合法性（内部无相邻同角色、首元素 ≠ lc、末元素 ≠ rc）。
// 任一不满足返回 nil，调用方据此放弃合并该 run——宁可少合并一次，
// 也不向上游发出非法角色序列。
func compactedRolesForNeighbors(leftRole, rightRole string) []string {
	normalize := func(r string) string {
		if r == "user" || r == "assistant" {
			return r
		}
		return ""
	}
	opposite := func(r string) string {
		if r == "user" {
			return "assistant"
		}
		return "user"
	}

	lc := normalize(leftRole)
	rc := normalize(rightRole)

	var roles []string
	switch {
	case lc == "" && rc == "":
		roles = []string{"user"}
	case lc == "":
		roles = []string{opposite(rc)}
	case rc == "":
		roles = []string{opposite(lc)}
	case lc == rc:
		roles = []string{opposite(lc)}
	default:
		roles = []string{opposite(lc), opposite(rc)}
	}

	for i := 1; i < len(roles); i++ {
		if roles[i] == roles[i-1] {
			return nil
		}
	}
	if lc != "" && roles[0] == lc {
		return nil
	}
	if rc != "" && roles[len(roles)-1] == rc {
		return nil
	}
	return roles
}

// CompactMessagesWithPlan 只物化传入 immutable plan，绝不访问 DecayTracker，
// 也不重新调用 findCompactableRuns。任何布局/坐标/插入失败都会恢复该 run
// 的 pre-decay Stage-2 内容，并且不伪造 CompactedBlock。
func CompactMessagesWithPlan(decayed, original []Message, plan *CompactionPlan) ([]Message, []CompactedBlock) {
	if plan == nil || !plan.EffectiveEnabled || len(plan.Replacements) == 0 {
		return decayed, nil
	}
	if len(decayed) != plan.MessageCount || len(original) < plan.MessageCount {
		return restorePlanStage2(decayed, plan, 0, plan.MessageCount-1), nil
	}

	result := make([]Message, 0, len(decayed))
	var blocks []CompactedBlock
	lastEnd := 0
	for runID, run := range plan.Runs {
		if run.StartIdx < lastEnd || run.StartIdx < 0 || run.EndIdx < run.StartIdx || run.EndIdx >= len(decayed) {
			return restoreAllPlanStage2(decayed, plan), blocks
		}
		result = append(result, decayed[lastEnd:run.StartIdx]...)
		runResultStart := len(result)
		runBlockStart := len(blocks)
		runOK := true
		prevEnd := run.StartIdx - 1
		for _, replacementID := range run.ReplacementIDs {
			if replacementID < 0 || replacementID >= len(plan.Replacements) {
				runOK = false
				break
			}
			replacement := plan.Replacements[replacementID]
			if replacement.RunID != runID || replacement.StartIdx != prevEnd+1 || replacement.EndIdx < replacement.StartIdx || replacement.EndIdx > run.EndIdx || replacement.Role == "" || len(replacement.Content) == 0 || !json.Valid(replacement.Content) {
				runOK = false
				break
			}
			result = append(result, Message{Role: replacement.Role, Content: append(json.RawMessage(nil), replacement.Content...)})
			blocks = append(blocks, CompactedBlock{
				StartIdx: replacement.StartIdx,
				EndIdx:   replacement.EndIdx,
				Content:  extractTextFromContent(replacement.Content),
				Role:     replacement.Role,
			})
			prevEnd = replacement.EndIdx
		}
		if !runOK || prevEnd != run.EndIdx {
			// 丢弃本 run 已经追加的 replacement，并用 Stage-2 内容恢复。
			result = result[:runResultStart]
			blocks = blocks[:runBlockStart]
			result = append(result, stage2Range(plan, decayed, run.StartIdx, run.EndIdx)...)
		}
		lastEnd = run.EndIdx + 1
	}
	result = append(result, decayed[lastEnd:]...)
	return result, blocks
}

func stage2Range(plan *CompactionPlan, decayed []Message, start, end int) []Message {
	if plan != nil && start >= 0 && end >= start && end < len(plan.Stage2Messages) {
		return cloneMessagesForPlan(plan.Stage2Messages[start : end+1])
	}
	if start < 0 {
		start = 0
	}
	if end >= len(decayed) {
		end = len(decayed) - 1
	}
	if end < start {
		return nil
	}
	return cloneMessagesForPlan(decayed[start : end+1])
}

func restorePlanStage2(decayed []Message, plan *CompactionPlan, start, end int) []Message {
	result := cloneMessagesForPlan(decayed)
	if plan == nil {
		return result
	}
	if start < 0 {
		start = 0
	}
	if end >= len(result) {
		end = len(result) - 1
	}
	for i := start; i <= end; i++ {
		if i < len(plan.Stage2Messages) {
			result[i] = cloneMessageForPlan(plan.Stage2Messages[i])
		}
	}
	return result
}

func restoreAllPlanStage2(decayed []Message, plan *CompactionPlan) []Message {
	return restorePlanStage2(decayed, plan, 0, len(decayed)-1)
}

// CompactMessages 是兼容入口：
//
//	CompactMessages(decayed, original, plan)
//
// 是新的 plan-only 形态；旧的
//
//	CompactMessages(decayed, original, tracker, sessionID, requestIdx, pressure)
//
// 仍可编译，但只在兼容边界构建一次 plan。生产请求路径直接调用
// CompactMessagesWithPlan，绝不把 tracker 交给物化器。
func CompactMessages(decayed, original []Message, args ...any) ([]Message, []CompactedBlock) {
	if len(args) == 1 {
		switch plan := args[0].(type) {
		case *CompactionPlan:
			return CompactMessagesWithPlan(decayed, original, plan)
		case CompactionPlan:
			return CompactMessagesWithPlan(decayed, original, &plan)
		}
		return decayed, nil
	}
	if len(args) != 4 {
		return decayed, nil
	}
	dt, ok := args[0].(*DecayTracker)
	if !ok || dt == nil {
		return decayed, nil
	}
	sessionID, ok := args[1].(string)
	if !ok {
		return decayed, nil
	}
	requestIdx, ok := args[2].(int)
	if !ok {
		return decayed, nil
	}
	pressure, ok := args[3].(float64)
	if !ok {
		return decayed, nil
	}
	snapshot := dt.BuildDecayEvaluationSnapshot(sessionID, nil, requestIdx, len(decayed), pressure)
	// 旧兼容 API 的历史扫描边界是 len-5（即保留最后四个索引）；生产
	// 请求路径使用配置 KeepRecent，并不经过这个 wrapper。
	plan := BuildCompactionPlan(snapshot, decayed, original, 4, true)
	return CompactMessagesWithPlan(decayed, original, plan)
}

// countRoleInRange 统计 [start, end] 范围内指定角色的消息数（闭区间）。
func countRoleInRange(messages []Message, start, end int, role string) int {
	count := 0
	for i := start; i <= end && i < len(messages); i++ {
		if messages[i].Role == role {
			count++
		}
	}
	return count
}

// buildCompactedMessage 为指定角色创建一条合并消息。
// 统计信息从 original（桩化前）消息中提取，确保 Files/Tools 摘要不因桩化丢失。
func buildCompactedMessage(original []Message, start, end int, role string) Message {
	stats := extractCompactionStats(original, start, end)
	content := buildCompactedContent(start, end, stats)
	contentJSON, _ := json.Marshal(content)
	return Message{Role: role, Content: contentJSON}
}

// extractTextFromContent 从 Message.Content（json.RawMessage）提取纯文本字符串。
func extractTextFromContent(raw json.RawMessage) string {
	var text string
	if err := json.Unmarshal(raw, &text); err == nil {
		return text
	}
	// 可能是数组格式
	var blocks []ContentBlock
	if err := json.Unmarshal(raw, &blocks); err == nil {
		for _, b := range blocks {
			if b.Type == "text" {
				return b.Text
			}
		}
	}
	return ""
}

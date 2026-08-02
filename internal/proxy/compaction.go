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

// CompactMessages 将连续 50+ 条 Stage-3（DecayCompacted）消息合并为摘要消息，
// 并保持发往上游的 user/assistant 交替。
//
// Role Alternation 保护协议：
//
// 核心约束：合并后的消息序列必须满足：
//   - 第一条合并消息的角色 ≠ 左邻居（decayed[run.start-1]）的角色
//   - 最后一条合并消息的角色 ≠ 右邻居（decayed[run.end+1]）的角色
//
// 合并策略由两侧邻居的实际角色决定（判定表见 compactedRolesForNeighbors）：
//
//   - 两侧规范化后的角色不同 → 2 条合并消息，分别覆盖前半区间
//     [run.start, mid] 与后半区间 [mid+1, run.end]，因此两条摘要的
//     区间标题与统计内容都不同
//
//   - 其余情形 → 1 条合并消息，覆盖整个 [run.start, run.end]
//
//   - 非 user/assistant 的邻居视为无交替约束；算出的序列若仍与任一邻居冲突，
//     放弃合并该 run（原样保留已衰减消息）
//
// run 的长度门槛只有一道，位于 findCompactableRuns（run 总长 ≥ minCompactableRun）。
// 这里不做第二道设限——decay.go 的 `case DecayCompacted: return ""` 已把内容清空，
// 任何「不合并」都意味着内容已空、摘要从未建立的信息净损失。
//
// 参数:
//   - decayed: 已桩化+衰减的消息（用于确定角色和当前状态）
//   - original: 桩化前的原始消息（用于提取统计信息）
//   - dt: 衰减追踪器（nil 时直接返回 decayed）
//   - requestIdx: 当前请求序号
//   - pressure: token 压力（totalTokens/threshold）
func CompactMessages(decayed, original []Message, dt *DecayTracker, sessionID string, requestIdx int, pressure float64) ([]Message, []CompactedBlock) {
	if dt == nil || len(decayed) < minCompactableRun {
		return decayed, nil
	}

	// 保护区：messages[0] 始终保护（frozen prefix 缓存稳定性）
	//         最后 5 条作为 keepRecent 保护（对齐 YesMem compaction.go:197-199）
	scanStart := 1
	scanEnd := len(decayed) - 5
	if scanEnd < scanStart {
		return decayed, nil
	}

	runs := findCompactableRuns(dt, sessionID, len(decayed), scanStart, scanEnd, requestIdx, pressure)
	if len(runs) == 0 {
		return decayed, nil
	}

	var blocks []CompactedBlock
	result := make([]Message, 0, len(decayed))
	lastEnd := 0

	for _, run := range runs {
		// 复制 run 之前的未受影响消息
		result = append(result, decayed[lastEnd:run.start]...)

		// 邻居的实际角色；右邻居越界时按无交替约束处理
		leftRole := decayed[run.start-1].Role
		rightRole := ""
		if run.end+1 < len(decayed) {
			rightRole = decayed[run.end+1].Role
		}

		roles := compactedRolesForNeighbors(leftRole, rightRole)
		switch len(roles) {
		case 1:
			// 只能放一条 → 覆盖整个 run 区间
			cm := buildCompactedMessage(original, run.start, run.end, roles[0])
			result = append(result, cm)
			blocks = append(blocks,
				CompactedBlock{StartIdx: run.start, EndIdx: run.end, Content: extractTextFromContent(cm.Content), Role: roles[0]},
			)

		case 2:
			// 两条各覆盖半个区间 → 坐标不相交、文本不重复
			mid := run.start + (run.end-run.start)/2
			cm1 := buildCompactedMessage(original, run.start, mid, roles[0])
			cm2 := buildCompactedMessage(original, mid+1, run.end, roles[1])
			result = append(result, cm1, cm2)
			blocks = append(blocks,
				CompactedBlock{StartIdx: run.start, EndIdx: mid, Content: extractTextFromContent(cm1.Content), Role: roles[0]},
				CompactedBlock{StartIdx: mid + 1, EndIdx: run.end, Content: extractTextFromContent(cm2.Content), Role: roles[1]},
			)

		default:
			// fail-safe：算不出合法序列 → 放弃合并，原样保留已衰减消息
			result = append(result, decayed[run.start:run.end+1]...)
		}

		lastEnd = run.end + 1
	}

	// 复制最后一段 run 之后的消息
	result = append(result, decayed[lastEnd:]...)
	return result, blocks
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

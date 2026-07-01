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

// CompactMessages 将连续 50+ 条 Stage-3（DecayCompacted）消息合并为摘要消息，
// 严格保持 user/assistant 角色交替。
//
// Role Alternation 保护协议（Phase C 关键修复，经探索验证）：
//
// 核心约束：合并后的消息序列必须满足：
//   - 第一条合并消息的角色 ≠ 左邻居（msg[run.start-1]）的角色
//   - 最后一条合并消息的角色 ≠ 右邻居（msg[run.end+1]）的角色
//
// Run 长度 L 的奇偶性决定邻居关系：
//   - L 为偶数：左邻居 ≠ 右邻居 → 可放置 2 条合并消息 [rightRole, leftRole]
//   - L 为奇数：左邻居 == 右邻居 → 只能放 1 条合并消息 [opposite(leftRole)]
//
// 计数阈值决定合并策略：
//
//   - 两个角色都 ≥ 50（必然 L ≥ 100 偶数）：
//     生成 [rightRole, leftRole] 共 2 条合并消息
//     例（左=user, 右=assistant）：[assistant, user] → user→asst→user→asst ✓
//
//   - 仅一个角色 ≥ 50（必然 L=99 奇数）：
//     生成 1 条合并消息，角色 = opposite(左邻居)
//     例（左右都是 user）：[assistant] → user→asst→user ✓
//
//   - 两个角色都 < 50：不合并该区间（保留原始消息）
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

		userCount := countRoleInRange(decayed, run.start, run.end, "user")
		asstCount := countRoleInRange(decayed, run.start, run.end, "assistant")
		runLen := run.end - run.start + 1

		// 确定邻居角色，用于保证交替
		leftRole := decayed[run.start-1].Role
		rightRole := "user" // 默认
		if run.end+1 < len(decayed) {
			rightRole = decayed[run.end+1].Role
		}

		// oppositeRole 辅助
		oppositeRole := func(r string) string {
			if r == "user" {
				return "assistant"
			}
			return "user"
		}

		switch {
		case userCount >= minCompactableRun && asstCount >= minCompactableRun:
			// 情况 A：两个角色都满足阈值（L ≥ 100）
			if runLen%2 == 0 {
				// L 为偶数 → leftRole ≠ rightRole → 2 条合并消息 [rightRole, leftRole]
				cm1 := buildCompactedMessage(original, run.start, run.end, rightRole)
				cm2 := buildCompactedMessage(original, run.start, run.end, leftRole)
				result = append(result, cm1, cm2)
				blocks = append(blocks,
					CompactedBlock{StartIdx: run.start, EndIdx: run.end, Content: extractTextFromContent(cm1.Content), Role: rightRole},
					CompactedBlock{StartIdx: run.start, EndIdx: run.end, Content: extractTextFromContent(cm2.Content), Role: leftRole},
				)
			} else {
				// L 为奇数 → leftRole == rightRole → 1 条合并消息 [opposite(leftRole)]
				role := oppositeRole(leftRole)
				cm := buildCompactedMessage(original, run.start, run.end, role)
				result = append(result, cm)
				blocks = append(blocks,
					CompactedBlock{StartIdx: run.start, EndIdx: run.end, Content: extractTextFromContent(cm.Content), Role: role},
				)
			}

		case userCount >= minCompactableRun:
			// 情况 B：仅 user 满足阈值（必然 L=99 奇数，leftRole == rightRole）
			// → 1 条合并消息，角色 = opposite(leftRole)
			role := oppositeRole(leftRole)
			cm := buildCompactedMessage(original, run.start, run.end, role)
			result = append(result, cm)
			blocks = append(blocks,
				CompactedBlock{StartIdx: run.start, EndIdx: run.end, Content: extractTextFromContent(cm.Content), Role: role},
			)

		case asstCount >= minCompactableRun:
			// 情况 C：仅 assistant 满足阈值（必然 L=99 奇数，leftRole == rightRole）
			// → 1 条合并消息，角色 = opposite(leftRole)
			role := oppositeRole(leftRole)
			cm := buildCompactedMessage(original, run.start, run.end, role)
			result = append(result, cm)
			blocks = append(blocks,
				CompactedBlock{StartIdx: run.start, EndIdx: run.end, Content: extractTextFromContent(cm.Content), Role: role},
			)

		default:
			// 情况 D：两个角色都不满足阈值 → 保留原始消息（已衰减的）
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

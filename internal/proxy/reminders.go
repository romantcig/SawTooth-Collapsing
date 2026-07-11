package proxy

import (
	"regexp"
	"strings"
)

// reminderPattern 匹配 <system-reminder>...</system-reminder> 块（含换行，dotall）。
// 内层捕获组提取 body，用于持久/临时分类。
// 正则取自 YesMem reminders.go:10。
var reminderPattern = regexp.MustCompile(`(?s)<system-reminder>\s*(.*?)\s*</system-reminder>`)

// skillHintPattern 匹配 [skill-hint]...[/skill-hint] 块（含可选前导换行，dotall）。
// 直接 ReplaceAllString 删空即可。正则取自 YesMem skill_hints.go:19。
var skillHintPattern = regexp.MustCompile(`(?s)\n?\[skill-hint\].*?\[/skill-hint\]`)

// isSessionStart 检测 system-reminder body 是否为 SessionStart 类型。
// 对标 YesMem reminders.go:187-189。
func isSessionStart(body string) bool {
	return strings.Contains(body, "SessionStart:")
}

// isExplicitTemporaryReminder 只识别已有协议证据和回归测试覆盖的临时提醒。
// 未知 reminder 采用 fail-safe 保留，避免删除未来 Claude Code 指令。
func isExplicitTemporaryReminder(body string) bool {
	normalized := strings.ToLower(strings.TrimSpace(body))
	if normalized == "noise" {
		return true
	}
	for _, marker := range []string{
		"your task tools haven't been used recently",
		"task tools haven't been used recently",
		"this is just a gentle reminder",
		"this is ambient context",
		"this may or may not be related",
	} {
		if strings.Contains(normalized, marker) {
			return true
		}
	}
	return false
}

// stripRemindersFromText 移除文本中过期的 system-reminder / skill-hint 块。
//
// 简化版（CONTEXT.md 决策，无 requestIdx）：
//   - system-reminder：SessionStart 和持久 user context 原样保留。
//   - 仅明确的临时 reminder 被移除；未知语义 fail-safe 保留。
//   - skill-hint：一律移除。
//   - 安全默认：正则未匹配的内容原样保留，避免误删真实对话。
//
// 实际发生删除时 TrimSpace，防止留下空白堆积；未改变时逐字节返回原文。
func stripRemindersFromText(text string) string {
	changed := false
	// 1. system-reminder 分类替换：持久/SessionStart/未知内容保留。
	t := reminderPattern.ReplaceAllStringFunc(text, func(match string) string {
		inner := reminderPattern.FindStringSubmatch(match)
		if len(inner) < 2 {
			// 安全默认：无法提取 body 时原样保留。
			return match
		}
		body := inner[1]
		if isSessionStart(body) || hasPersistentUserContextHeading(body) {
			return match
		}
		if isExplicitTemporaryReminder(body) {
			changed = true
			return ""
		}
		return match
	})

	// 2. skill-hint 移除。
	if skillHintPattern.MatchString(t) {
		changed = true
	}
	t = skillHintPattern.ReplaceAllString(t, "")

	if !changed {
		return text
	}
	return strings.TrimSpace(t)
}

// stripRemindersFromBlocks 对 content block 数组执行 reminder 移除。
//
// 仿 stubify.go 纯函数三件套的 copy-then-mutate 范式：
//   - 仅改写 block.Type == "text" 的 .Text；
//   - text block 被清空（trim 后为空）→ 整块丢弃；
//   - 非 text block（tool_use / tool_result / thinking 等）一律原样保留，
//     保证 tool_use/tool_result 配对不被破坏（REMIND-04）。
//
// 绝不原地修改入参，返回新切片。
func stripRemindersFromBlocks(blocks []ContentBlock) []ContentBlock {
	result, _ := stripRemindersFromBlocksWithChange(blocks)
	return result
}

func stripRemindersFromBlocksWithChange(blocks []ContentBlock) ([]ContentBlock, bool) {
	result := make([]ContentBlock, 0, len(blocks))
	changed := false
	for _, block := range blocks {
		if block.Type != "text" {
			// 非 text block 原样保留（含 tool_use/tool_result/thinking）。
			result = append(result, block)
			continue
		}
		stripped := stripRemindersFromText(block.Text)
		if stripped != block.Text {
			changed = true
		}
		if stripped == "" {
			// text block 被整块清空 → 丢弃该 block。
			continue
		}
		newBlock := block
		newBlock.Text = stripped
		result = append(result, newBlock)
	}
	return result, changed
}

// StripReminders 移除旧消息中过期的 system-reminder / skill-hint 标签（REMIND-01/02/03）。
//
// 行为约定：
//   - 最后一条 user 消息完整保留，不被改写（REMIND-03）。
//   - 其余消息走 parseContent → stripRemindersFromBlocks → rebuildContent。
//   - 不增删消息条数；tool_use/tool_result block 数与配对不变（REMIND-04）。
//   - SessionStart 与四种持久 user context 原样保留。
//   - 只删除明确临时 reminder，未知或畸形内容 fail-safe 保留。
//   - 若 strip 后某条消息所有 text block 均被清空（content 会变成空数组），
//     则整条消息保持原样不动，防止空 content 触发 Anthropic API 400 错误。
//
// copy-then-mutate：先复制入参切片，绝不原地修改调用方的 messages。
func StripReminders(messages []Message) []Message {
	// 倒序扫描定位最后一条 user 消息（对标 proxy.go extractLatestUserText idiom）。
	lastUserIdx := -1
	for i := len(messages) - 1; i >= 0; i-- {
		if messages[i].Role == "user" {
			lastUserIdx = i
			break
		}
	}

	result := make([]Message, len(messages))
	copy(result, messages)

	for i := range result {
		if i == lastUserIdx {
			continue // 末条 user 完整保留（REMIND-03）
		}
		blocks, isArray := parseContent(result[i].Content)
		stripped, changed := stripRemindersFromBlocksWithChange(blocks)
		if !changed {
			continue
		}
		// 空内容防护：若 strip 后所有 text block 均被清空，整条消息保持原样，
		// 防止产出空 content [] 或 "" 触发 Anthropic API 400 错误。
		// 参考：anthropics/claude-code #54314 — 空 text block 会永久破坏 session。
		if len(stripped) == 0 {
			continue
		}
		result[i].Content = rebuildContent(stripped, isArray)
	}

	return result
}

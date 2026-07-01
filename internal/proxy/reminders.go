package proxy

import (
	"regexp"
	"strings"
)

// reminderPattern 匹配 <system-reminder>...</system-reminder> 块（含换行，dotall）。
// 内层捕获组提取 body，用于 isSessionStart 分类。
// 正则取自 YesMem reminders.go:10。
var reminderPattern = regexp.MustCompile(`(?s)<system-reminder>\s*(.*?)\s*</system-reminder>`)

// skillHintPattern 匹配 [skill-hint]...[/skill-hint] 块（含可选前导换行，dotall）。
// 直接 ReplaceAllString 删空即可。正则取自 YesMem skill_hints.go:19。
var skillHintPattern = regexp.MustCompile(`(?s)\n?\[skill-hint\].*?\[/skill-hint\]`)

// isSessionStart 检测 system-reminder body 是否为 SessionStart 类型。
// 这是唯一必须保留的 system-reminder 类型；其余非 SessionStart 块一律移除。
// 对标 YesMem reminders.go:187-189。
func isSessionStart(body string) bool {
	return strings.Contains(body, "SessionStart:")
}

// stripRemindersFromText 移除文本中过期的 system-reminder / skill-hint 块。
//
// 简化版（CONTEXT.md 决策，无 requestIdx）：
//   - system-reminder：body 含 SessionStart: → 原样保留；其余 → 移除。
//   - skill-hint：一律移除。
//   - 安全默认：正则未匹配的内容原样保留，避免误删真实对话。
//
// 处理后 TrimSpace，防止移除留下的空白堆积。
func stripRemindersFromText(text string) string {
	// 1. system-reminder 分类替换：SessionStart 保留，其余移除。
	t := reminderPattern.ReplaceAllStringFunc(text, func(match string) string {
		inner := reminderPattern.FindStringSubmatch(match)
		if len(inner) < 2 {
			// 安全默认：无法提取 body 时原样保留。
			return match
		}
		body := inner[1]
		if isSessionStart(body) {
			return match // SessionStart 原样保回
		}
		return "" // 非 SessionStart 块移除
	})

	// 2. skill-hint 移除。
	t = skillHintPattern.ReplaceAllString(t, "")

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
	result := make([]ContentBlock, 0, len(blocks))
	for _, block := range blocks {
		if block.Type != "text" {
			// 非 text block 原样保留（含 tool_use/tool_result/thinking）。
			result = append(result, block)
			continue
		}
		stripped := stripRemindersFromText(block.Text)
		if stripped == "" {
			// text block 被整块清空 → 丢弃该 block。
			continue
		}
		newBlock := block
		newBlock.Text = stripped
		result = append(result, newBlock)
	}
	return result
}

// StripReminders 移除旧消息中过期的 system-reminder / skill-hint 标签（REMIND-01/02/03）。
//
// 行为约定：
//   - 最后一条 user 消息完整保留，不被改写（REMIND-03）。
//   - 其余消息走 parseContent → stripRemindersFromBlocks → rebuildContent。
//   - 不增删消息条数；tool_use/tool_result block 数与配对不变（REMIND-04）。
//   - body 含 SessionStart: 的 system-reminder 原样保留。
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
		stripped := stripRemindersFromBlocks(blocks)
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

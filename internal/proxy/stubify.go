package proxy

import (
	"encoding/json"
	"fmt"
	"strings"
	"unicode/utf8"
)

// ContentBlock 映射 Anthropic API content block JSON 结构。
// 支持 text、thinking、tool_use、tool_result 四种类型。
type ContentBlock struct {
	Type      string         `json:"type"`
	Text      string         `json:"text,omitempty"`
	Thinking  string         `json:"thinking,omitempty"`
	Signature string         `json:"signature,omitempty"`
	ID        string         `json:"id,omitempty"`
	Name      string         `json:"name,omitempty"`
	Input     map[string]any `json:"input"`
	ToolUseID string         `json:"tool_use_id,omitempty"`
	Content   any            `json:"content,omitempty"`
	IsError   bool           `json:"is_error,omitempty"`
}

// Message 表示 Anthropic API 中的一条消息。
// Content 可以是纯文本字符串或 ContentBlock 数组。
type Message struct {
	Role    string          `json:"role"`
	Content json.RawMessage `json:"content"`
}

// StubStats 记录 stub 操作前后的统计信息。
type StubStats struct {
	OriginalTokens  int
	StubbedTokens   int
	MessagesStubbed int
	ThinkingRemoved int
	ToolsProcessed  int
	IsDecision      bool
}

// parseContent 解析 Message.Content 字段。
// 若为 JSON 数组则返回 []ContentBlock；若为字符串则包装为单元素 text block。
// 返回 isArray 指示原始格式，用于 rebuildContent。
func parseContent(raw json.RawMessage) (blocks []ContentBlock, isArray bool) {
	if len(raw) == 0 {
		return []ContentBlock{}, false
	}

	// 尝试解析为数组
	if raw[0] == '[' {
		var parsed []ContentBlock
		if err := json.Unmarshal(raw, &parsed); err != nil {
			return []ContentBlock{{Type: "text", Text: ""}}, true
		}
		return parsed, true
	}

	// 解析为字符串
	var text string
	if err := json.Unmarshal(raw, &text); err != nil {
		return []ContentBlock{{Type: "text", Text: ""}}, false
	}
	return []ContentBlock{{Type: "text", Text: text}}, false
}

// rebuildContent 将 ContentBlock 数组重建为 JSON。
// 若原始为字符串格式且只有单个 text block，还原为字符串。
func rebuildContent(blocks []ContentBlock, wasArray bool) json.RawMessage {
	// 防御性清理：过滤掉 thinking 字段为空的 thinking block。
	// ContentBlock.Thinking 标注了 json:"thinking,omitempty"，空 thinking 在 marshal
	// 时会导致 "thinking" 字段缺失，Anthropic API 返回 400。
	blocks = sanitizeBlocks(blocks)

	if !wasArray && len(blocks) == 1 && blocks[0].Type == "text" {
		data, err := json.Marshal(blocks[0].Text)
		if err != nil {
			return json.RawMessage(`""`)
		}
		return data
	}

	data, err := json.Marshal(blocks)
	if err != nil {
		return json.RawMessage(`[]`)
	}
	return data
}

// sanitizeBlocks 过滤掉无效的 content block（如 thinking 文本为空的 thinking block）。
// 这是防御性措施，防止因 omitempty 导致 API 400 错误。
func sanitizeBlocks(blocks []ContentBlock) []ContentBlock {
	n := 0
	for _, b := range blocks {
		// thinking block 必须有非空 thinking 字段，否则 Anthropic API 拒绝
		if b.Type == "thinking" && b.Thinking == "" {
			continue
		}
		// tool_use block 必须有非 nil input 字段，否则 json.Marshal 序列化后 Anthropic API 返回 400
		if b.Type == "tool_use" && b.Input == nil {
			b.Input = map[string]any{}
		}
		blocks[n] = b
		n++
	}
	return blocks[:n]
}

// stubifyMessages 对 messages 数组执行桩化处理（STUB-01 至 STUB-07）。
// 按顺序执行：thinking 移除 → pivot 保护检查 → 决策检测 → 截断 → tool stub。
// pivotText 为空时跳过 pivot 保护。
// keepRecent 指定尾部不桩化的消息数（保护最近上下文，防止缓存/质量退化）。
// keepThinking 为 true 时保留 thinking blocks（1M 上下文模型建议开启）。
// dt 非 nil 时在每条消息桩化后调用 MarkStubbed 记录衰减状态（Phase B）。
// sessionID 用于 DecayTracker 的 session-scoped key（Phase F）。
func stubifyMessages(messages []Message, tc *TokenCounter, pivotText string, keepRecent int, keepThinking bool, dt *DecayTracker, sessionID string, requestIdx int, intensity float64, threshold int) ([]Message, StubStats) {
	// T-02-01: 防止恶意超大消息数组导致内存耗尽
	const maxMessages = 10000
	if len(messages) > maxMessages {
		messages = messages[len(messages)-maxMessages:]
	}

	// 保护上限：至少保留 1 条（messages[0]），最多不超过消息总数
	if keepRecent < 0 {
		keepRecent = 0
	}
	protectedTail := len(messages) - keepRecent
	if protectedTail < 1 {
		protectedTail = 1 // 始终保护 messages[0]
	}

	stats := StubStats{}
	stats.OriginalTokens = tc.CountMessagesTokens(messages)

	// 阈值门控：token 数不超阈值则跳过整个 stub 管线（对标 YesMem stubify.go threshold guard）
	if threshold > 0 && stats.OriginalTokens <= threshold {
		stats.StubbedTokens = stats.OriginalTokens
		return messages, stats
	}

	stubbed := make([]Message, 0, len(messages))
	// messages[0] 始终保护，保证 frozen prefix 缓存前缀稳定性
	stubbed = append(stubbed, messages[0])

	// Phase D: 构建 tool_use_id → 关键词映射（供 stubToolResults 追加 deep_search 提示）
	toolKWMap := buildToolUseKeywordMap(messages)

	// prevHadToolUse 跟踪前一条消息是否包含被桩化的 tool_use block。
	// 用于工具配对强制桩化（F-9）：若前一条 assistant 消息的 tool_use 被桩化，
	// 则当前 user 消息的 tool_result 也必须桩化，防止 Anthropic API 400 错误。
	prevHadToolUse := false

	for i := 1; i < len(messages); i++ {
		msg := messages[i]
		blocks, isArray := parseContent(msg.Content)

		// 80-token 硬底：太短且无 tool 交互的消息跳过 stub（stub 后反而更大）
		// 对标 YesMem stubify.go: estimateMessageContentTokens ≤ 80
		// 含 tool_use 或 tool_result 的消息仍需 stub——tool stub 始终更短
		msgTokens := tc.CountMessageTokens(msg)
		if msgTokens <= 80 && !hasToolUseBlocks(msg.Content) && !hasToolResultBlocks(msg.Content) {
			stubbed = append(stubbed, msg)
			continue
		}

		// 工具配对检测：前一条消息有 tool_use 且当前有 tool_result → 强制桩化
		forceStub := prevHadToolUse && hasToolResultBlocks(msg.Content)

		// STUB-01: 移除 thinking 块（keepThinking 为 true 时跳过）。
		// 必须在尾部保护之前执行——即使受保护的消息也应移除 thinking，
		// 否则 ContentBlock.Thinking 的 omitempty 标签在 re-marshal 时
		// 可能丢弃空 thinking 字段，导致 Anthropic API 400。
		var thinkingRemoved int
		if !keepThinking {
			beforeThink := len(blocks)
			blocks = removeThinking(blocks)
			thinkingRemoved = beforeThink - len(blocks)
			stats.ThinkingRemoved += thinkingRemoved
		}

		// 尾部保护区：最近 keepRecent 条消息跳过文本截断和工具桩化。
		// 例外：工具配对强制桩化（F-9）。
		// 注意：若 thinking blocks 在上方被移除，需重建 content 再追加。
		if i >= protectedTail && !forceStub {
			if thinkingRemoved > 0 {
				msg.Content = rebuildContent(blocks, isArray)
			}
			stubbed = append(stubbed, msg)
			continue
		}

		// STUB-07: pivot + debug + task list 扩展保护
		// 工具配对强制桩化时不做保护跳过
		if isProtectedExtended(msg.Content, pivotText) && !forceStub {
			if thinkingRemoved > 0 {
				msg.Content = rebuildContent(blocks, isArray)
			}
			stubbed = append(stubbed, msg)
			continue
		}

		// STUB-06: 决策消息检测
		if msg.Role == "assistant" && isDecisionMessage(msg.Content, tc) {
			stats.IsDecision = true
		}

		// STUB-04/05: 文本截断
		blocks = truncateTextBlocks(blocks, msg.Role)

		// STUB-02/03: tool 桩化
		toolBlocksBefore := 0
		for _, b := range blocks {
			if b.Type == "tool_use" || b.Type == "tool_result" {
				toolBlocksBefore++
			}
		}
		blocks = stubToolResults(blocks, toolKWMap)

		// Phase B: 桩化前提取文件路径（供 DecayTracker.SetFilePath 用）
		var stubbedFilePath string
		for _, b := range blocks {
			if b.Type == "tool_use" && b.Input != nil {
				if fp, ok := b.Input["file_path"].(string); ok && fp != "" {
					stubbedFilePath = fp
					break
				}
			}
		}
		blocks = stubToolUses(blocks)
		stats.ToolsProcessed += toolBlocksBefore

		// 追踪本消息是否包含 tool_use（用于下一条消息的配对检测）
		prevHadToolUse = hasToolUseBlocks(msg.Content)

		// 重建 content
		msg.Content = rebuildContent(blocks, isArray)
		stubbed = append(stubbed, msg)
		stats.MessagesStubbed++

		// Phase B: 记录衰减状态（DecayTracker 非 nil 时）
		if dt != nil {
			dt.MarkStubbed(sessionID, i, requestIdx, intensity)
			if stubbedFilePath != "" {
				dt.SetFilePath(sessionID, i, stubbedFilePath)
			}
		}
	}

	stats.StubbedTokens = tc.CountMessagesTokens(stubbed)
	return stubbed, stats
}

// removeThinking 过滤掉所有 thinking 类型的 content block（STUB-01）。
func removeThinking(blocks []ContentBlock) []ContentBlock {
	filtered := make([]ContentBlock, 0, len(blocks))
	for _, block := range blocks {
		if block.Type != "thinking" {
			filtered = append(filtered, block)
		}
	}
	return filtered
}

// stubToolResults 将 tool_result block 替换为桩化文本（STUB-02）。
// Phase D: 通过 toolKWMap 查找匹配 tool_use 的关键词，追加 deep_search 提示。
func stubToolResults(blocks []ContentBlock, toolKWMap map[string]string) []ContentBlock {
	result := make([]ContentBlock, 0, len(blocks))
	for _, block := range blocks {
		if block.Type == "tool_result" {
			stub := "[tool result archived]"
			// Phase D: 追加 deep_search 提示
			if kw, ok := toolKWMap[block.ToolUseID]; ok && kw != "" {
				stub += fmt.Sprintf(" → deep_search('%s')", sanitizeDSKeyword(kw))
			}
			result = append(result, ContentBlock{Type: "text", Text: stub})
		} else {
			result = append(result, block)
		}
	}
	return result
}

// stubToolUses 将 tool_use block 替换为格式化的 stub 文本。
// 按工具类型区分格式（对标 YesMem buildEagerStub），未知类型 fallback 到通用格式。
func stubToolUses(blocks []ContentBlock) []ContentBlock {
	result := make([]ContentBlock, 0, len(blocks))
	for _, block := range blocks {
		if block.Type != "tool_use" {
			result = append(result, block)
			continue
		}
		var text string
		switch block.Name {
		case "Read":
			path, _ := block.Input["file_path"].(string)
			text = fmt.Sprintf("[→] Read %s", path)
		case "Edit":
			path, _ := block.Input["file_path"].(string)
			text = fmt.Sprintf("[→] Edit %s -- file on disk is current", path)
		case "Write":
			path, _ := block.Input["file_path"].(string)
			text = fmt.Sprintf("[→] Write %s -- file on disk is current", path)
		case "Bash":
			cmd, _ := block.Input["command"].(string)
			text = fmt.Sprintf("[→] Bash: %s", truncateRunes(cmd, 80))
		case "Grep":
			pattern, _ := block.Input["pattern"].(string)
			path, _ := block.Input["path"].(string)
			text = fmt.Sprintf("[→] Grep '%s' in %s", truncateRunes(pattern, 40), path)
		case "Glob":
			pattern, _ := block.Input["pattern"].(string)
			text = fmt.Sprintf("[→] Glob '%s'", pattern)
		case "Agent":
			desc, _ := block.Input["description"].(string)
			text = fmt.Sprintf("[→] Agent: %s", truncateRunes(desc, 60))
		case "WebSearch":
			query, _ := block.Input["query"].(string)
			text = fmt.Sprintf("[→] WebSearch '%s'", truncateRunes(query, 60))
		case "WebFetch":
			url, _ := block.Input["url"].(string)
			text = fmt.Sprintf("[→] WebFetch %s", truncateRunes(url, 60))
		default:
			// 未知工具类型 fallback 到现有通用格式
			annotation := extractAnnotation(block.Input)
			if annotation != "" {
				text = fmt.Sprintf("[→] %s %s — %s", block.Name, formatArgsSummary(block.Input), annotation)
			} else {
				text = fmt.Sprintf("[→] %s %s", block.Name, formatArgsSummary(block.Input))
			}
		}
		// deep_search 提示（对标 YesMem stubify.go:345-347）
		if kw := extractToolKeywords(block); kw != "" {
			text += fmt.Sprintf(" → deep_search('%s')", sanitizeDSKeyword(kw))
		}
		result = append(result, ContentBlock{Type: "text", Text: text})
	}
	return result
}

// buildToolUseKeywordMap 扫描所有消息的 tool_use block，构建 tool_use_id → 关键词映射。
// 对标 YesMem stubify.go:417-443 buildToolUseInfo。
// 用于 stubToolResults 在桩化时追加 deep_search 提示。
func buildToolUseKeywordMap(messages []Message) map[string]string {
	info := make(map[string]string)
	for _, msg := range messages {
		blocks, _ := parseContent(msg.Content)
		for _, b := range blocks {
			if b.Type == "tool_use" && b.ID != "" {
				kw := extractToolKeywords(b)
				if kw != "" {
					info[b.ID] = kw
				}
			}
		}
	}
	return info
}

// extractAnnotation 从 tool_use input 中提取文件路径等关键参数作为注解（STUB-03）。
// 匹配常见的文件相关 key，截断到约 100 runes。
func extractAnnotation(input map[string]any) string {
	if len(input) == 0 {
		return ""
	}

	fileKeys := []string{"file_path", "path", "filename", "file", "dir", "directory", "filePath"}
	var parts []string
	for _, key := range fileKeys {
		if val, ok := input[key]; ok {
			if s, ok := val.(string); ok && s != "" {
				parts = append(parts, key+"="+s)
			}
		}
	}

	if len(parts) == 0 {
		return ""
	}

	annotation := strings.Join(parts, ", ")
	return truncateRunes(annotation, 100)
}

// formatArgsSummary 生成 tool_use 参数的简短摘要。
// 截断到约 80 runes。
func formatArgsSummary(input map[string]any) string {
	if len(input) == 0 {
		return ""
	}

	var parts []string
	for key, val := range input {
		// 跳过已通过 extractAnnotation 处理的文件路径
		if isFileKey(key) {
			continue
		}
		parts = append(parts, fmt.Sprintf("%s=%v", key, val))
	}

	summary := strings.Join(parts, ", ")
	return truncateRunes(summary, 80)
}

// isFileKey 判断 key 是否为文件路径相关的键。
func isFileKey(key string) bool {
	fileKeys := []string{"file_path", "path", "filename", "file", "dir", "directory", "filePath"}
	for _, fk := range fileKeys {
		if key == fk {
			return true
		}
	}
	return false
}

// truncateTextBlocks 对文本块执行 rune 感知截断（STUB-04/05）。
// 用户消息：< 800 保留，>= 800 截断到 500 + "…"
// Assistant 消息：< 400 保留，>= 400 截断到 300 + "…"
func truncateTextBlocks(blocks []ContentBlock, role string) []ContentBlock {
	var maxRunes, truncatedRunes int
	switch role {
	case "user":
		maxRunes = 800
		truncatedRunes = 500
	case "assistant":
		maxRunes = 400
		truncatedRunes = 300
	default:
		return blocks
	}

	result := make([]ContentBlock, len(blocks))
	copy(result, blocks)
	for i, block := range result {
		if block.Type == "text" {
			if countRunes(block.Text) > maxRunes {
				result[i].Text = truncateRunes(block.Text, truncatedRunes)
			}
		}
	}
	return result
}

// countRunes 返回字符串的 Unicode 字符数。
func countRunes(s string) int {
	return utf8.RuneCountInString(s)
}

// truncateRunes 按 rune 位置截断字符串，并追加 "…"。
// 若字符串 rune 数不超过 maxRunes，原样返回。
// 截断可能落在代码块中间，未闭合围栏会吞掉下游对话内容
// （经 reexpand.go 的 SummaryText 注入路径级联污染），
// 故截断后围栏计数为奇数时补一个闭合围栏止血。
func truncateRunes(s string, maxRunes int) string {
	runes := []rune(s)
	if len(runes) <= maxRunes {
		return s
	}
	truncated := string(runes[:maxRunes]) + "…"
	if strings.Count(truncated, "```")%2 == 1 {
		// 闭合围栏须独占一行（Markdown 围栏在行首才生效）
		truncated += "\n```"
	}
	return truncated
}

// isDecisionMessage 检测消息是否为决策消息（STUB-06，对应 D-03）。
// 决策检测启发式（语言无关，来自 YesMem 算法）：
//   a. 短确认：总 token < 50 且 role 为 assistant → 可能为决策
//
// 条件 a 即足以捕获真实决策消息。移除了旧版条件 c（末尾 200 字符内短句检测），
// 因其将大量以句号结尾的常态消息误判为决策（如 "Done."、"Here's the fix."），
// 导致 decay 推进速度慢 3 倍。
func isDecisionMessage(content json.RawMessage, tc *TokenCounter) bool {
	blocks, _ := parseContent(content)

	// 提取所有文本
	var allText strings.Builder
	for _, block := range blocks {
		if block.Type == "text" {
			allText.WriteString(block.Text)
		}
	}
	text := allText.String()

	// 条件 a: 短确认 —— total tokens < 50
	tokenCount := tc.CountTokens(text)
	return tokenCount < 50
}

// isTaskList 检查文本是否包含 TODO 项或 checklist。
func isTaskList(text string) bool {
	if text == "" {
		return false
	}
	return strings.Contains(text, "- [ ]") ||
		strings.Contains(text, "- [x]") ||
		strings.Contains(text, "TODO:") ||
		strings.Contains(text, "- [X]")
}

// isProtectedExtended 扩展保护检测：pivot 文本 + task list。
func isProtectedExtended(content json.RawMessage, pivotText string) bool {
	blocks, _ := parseContent(content)
	var allText strings.Builder
	for _, block := range blocks {
		if block.Type == "text" {
			allText.WriteString(block.Text)
		}
	}
	text := allText.String()

	// pivot 文本重叠检测
	if isPivotMessage(content, pivotText) {
		return true
	}

	// task list 保护（TODO / checklist）
	if isTaskList(text) {
		return true
	}

	return false
}

// hasToolUseBlocks 检查消息的 content block 中是否包含 tool_use 类型。
func hasToolUseBlocks(content json.RawMessage) bool {
	blocks, _ := parseContent(content)
	for _, b := range blocks {
		if b.Type == "tool_use" {
			return true
		}
	}
	return false
}

// hasToolResultBlocks 检查消息的 content block 中是否包含 tool_result 类型。
func hasToolResultBlocks(content json.RawMessage) bool {
	blocks, _ := parseContent(content)
	for _, b := range blocks {
		if b.Type == "tool_result" {
			return true
		}
	}
	return false
}

// isPivotMessage 检测消息是否与 pivot 文本高度重叠（STUB-07，对应 D-08）。
// 若 pivotText 为空，返回 false。
// 将 content 文本和 pivotText 分别分词（空格分割 + 小写），
// 统计重叠单词数，>= 3 即为 pivot 消息。
// 长度 < 3 的单词不计入重叠（噪声过滤）。
func isPivotMessage(content json.RawMessage, pivotText string) bool {
	if pivotText == "" {
		return false
	}

	blocks, _ := parseContent(content)
	var allText strings.Builder
	for _, block := range blocks {
		if block.Type == "text" {
			allText.WriteString(block.Text)
		}
	}

	contentWords := tokenizeWords(allText.String())
	pivotWords := tokenizeWords(pivotText)

	// 构建 pivot 词集合
	pivotSet := make(map[string]bool, len(pivotWords))
	for _, w := range pivotWords {
		pivotSet[w] = true
	}

	// 统计重叠
	overlap := 0
	seen := make(map[string]bool)
	for _, w := range contentWords {
		if pivotSet[w] && !seen[w] {
			overlap++
			seen[w] = true
		}
	}

	return overlap >= 3
}

// tokenizeWords 将文本按空格分割为小写单词，过滤短词。
func tokenizeWords(text string) []string {
	parts := strings.Fields(strings.ToLower(text))
	words := make([]string, 0, len(parts))
	for _, p := range parts {
		// 去除标点
		cleaned := strings.Trim(p, ".,;:!?\"'()[]{}…—")
		if utf8.RuneCountInString(cleaned) >= 3 {
			words = append(words, cleaned)
		}
	}
	return words
}

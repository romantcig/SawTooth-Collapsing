package proxy

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/google/uuid"
)

// ── 公开纯函数 ──

// CalcCollapseCutoff 从后往前累积 token 到 tokenFloor，返回 cutoff 索引或 -1。
// COLLAPSE-01, COLLAPSE-02, D-03
// keepRecent 保护最近 N 条消息不被折叠。
func CalcCollapseCutoff(messages []Message, tokenFloor int, tc *TokenCounter, keepRecent int) int {
	n := len(messages)
	// 守卫：至少需要第一条消息 + 一条可折叠消息
	if n < 2 {
		return -1
	}

	// T-03-04: 防止恶意超大消息数组导致内存耗尽
	const maxMessages = 10000
	if n > maxMessages {
		messages = messages[:maxMessages]
		n = maxMessages
	}

	// 从末尾向前遍历到索引 1（跳过第一条消息）
	kept := 0
	cutoff := -1
	for i := n - 1; i >= 1; i-- {
		kept += tc.CountMessageTokens(messages[i])
		if kept >= tokenFloor {
			cutoff = i
			break
		}
	}

	// cutoff=1 只会 blank 首条消息并再插入它的 archive，消息数反而增加，
	// 且摘要范围退化为 1–0；至少需要归档首条之外的一条历史消息。
	if cutoff < 2 {
		return -1
	}

	// keepRecent 保护：确保至少保留 keepRecent 条最近消息不被折叠
	if keepRecent > 0 {
		maxCutoff := n - keepRecent
		if maxCutoff < 1 {
			return -1
		}
		if cutoff > maxCutoff {
			cutoff = maxCutoff
		}
	}

	maxCutoff := n
	if keepRecent > 0 {
		maxCutoff = n - keepRecent
	}

	// Orphan safety (COLLAPSE-02, D-03, T-03-05):
	// 若 cutoff 落在含 tool_result 的 user 消息上，优先将整对纳入折叠；
	// 若这会侵入 recent window，则后退保留整对。
	if cutoff < n && messages[cutoff].Role == "user" && hasToolResultContent(messages[cutoff].Content) {
		if cutoff+1 <= maxCutoff {
			cutoff++
		} else if cutoff > 1 && messages[cutoff-1].Role == "assistant" && hasToolUseContent(messages[cutoff-1].Content) {
			cutoff--
		}
	}
	if cutoff < 2 {
		return -1
	}

	return cutoff
}

// CollapseOldMessages 将 cutoffIdx 前的消息折叠为单一 archive block。
// 保留 cutoffIdx 及之后的消息，第一条消息 blanking。
// modified 为已桩化/衰减的消息（用于 blankFirst + recent tail），
// original 为桩化前的原始消息（用于 buildArchiveBlock 提取完整摘要）。
// 对标 YesMem: func CollapseOldMessages(modified, original []any, ...)
// 返回 (折叠后的消息数组, 构建的 archive block)，不修改输入。
// COLLAPSE-03, COLLAPSE-05, D-04, D-05, ABLOCK-01
func CollapseOldMessages(modified, original []Message, cutoffIdx int, tc *TokenCounter, sessionID string) ([]Message, ArchiveBlock) {
	// modified/original 必须保持一一对应；异常输入 fail closed，避免错误归档或越界。
	if len(modified) != len(original) {
		return modified, ArchiveBlock{}
	}

	// T-03-04: bounds check — 两组输入同步限制上限。
	const maxMessages = 10000
	if len(modified) > maxMessages {
		modified = modified[:maxMessages]
		original = original[:maxMessages]
	}

	if cutoffIdx < 1 || cutoffIdx >= len(modified) || cutoffIdx >= len(original) {
		return modified, ArchiveBlock{}
	}

	// 步骤 1: blankFirstMessage——将第一条消息的 content 替换为占位文本（用 modified，ABLOCK-01）
	blanked := blankFirstMessage(modified, tc)

	// ABLOCK-01: 从原始消息提取摘要，确保 Tools/Files/Commits/Timeline/Gotchas 不因桩化丢失
	block := buildArchiveBlock(original[:cutoffIdx], cutoffIdx, tc, sessionID)

	// 步骤 4+5: 组装结果（tail 用 modified）
	archiveMsgContent, _ := json.Marshal(block.SummaryText)
	archiveMsg := Message{Role: "user", Content: archiveMsgContent}

	result := make([]Message, 0, 2+len(modified)-cutoffIdx)
	result = append(result, blanked[0]) // 只取 blanked 的第一条消息
	result = append(result, archiveMsg)
	result = append(result, modified[cutoffIdx:]...)

	return result, block
}

// buildArchiveBlock 从消息数组创建 ArchiveBlock 结构，不格式化文本。
// COLLAPSE-04, D-04
func buildArchiveBlock(messages []Message, cutoffIdx int, tc *TokenCounter, sessionID string) ArchiveBlock {
	// 提取折叠消息中的各部分数据用于生成摘要文本
	var allTimeline []string
	var allCommits []string
	var allTools []string
	var allFiles = make(map[string]bool)
	var allGotchas []string
	var conclusionText string

	for i, msg := range messages {
		blocks, _ := parseContent(msg.Content)
		allTimeline = append(allTimeline, extractTimeline(blocks, i, msg.Role)...)
		allCommits = append(allCommits, extractGitCommits(blocks)...)
		allTools = append(allTools, extractToolEvents(blocks)...)
		for _, f := range extractFileList(blocks) {
			allFiles[f] = true
		}
		allGotchas = append(allGotchas, extractGotchas(blocks)...)
		if msg.Role == "assistant" {
			if conc := extractConclusion(blocks); conc != "" {
				conclusionText = conc
			}
		}
	}

	estimatedTokens := tc.CountMessagesTokens(messages)
	contentHash, _ := archiveContentHash(messages)

	summaryText := formatArchiveBlockText(
		1, cutoffIdx-1,
		cutoffIdx, estimatedTokens,
		allTools, allFiles, allCommits, allTimeline, allGotchas, conclusionText,
	)

	block := ArchiveBlock{
		ID:              uuid.Must(uuid.NewRandom()).String(),
		SessionID:       sessionID,
		ContentHash:     contentHash,
		BlockRangeStart: 1,
		BlockRangeEnd:   cutoffIdx - 1,
		MessageCount:    cutoffIdx,
		EstimatedTokens: estimatedTokens,
		Messages:        messages,
		Keywords:        extractKeywords(messages),
		SummaryText:     summaryText,
		CreatedAt:       "",
	}
	return block
}

// archiveContentHash 对项目稳定的 []Message JSON 表示计算 SHA-256 指纹。
func archiveContentHash(messages []Message) (string, error) {
	canonicalMessages := make([]struct {
		Role    string `json:"role"`
		Content any    `json:"content"`
	}, len(messages))
	for i, message := range messages {
		var content any
		if err := json.Unmarshal(message.Content, &content); err != nil {
			return "", fmt.Errorf("解析 archive message %d content 失败: %w", i, err)
		}
		canonicalMessages[i].Role = message.Role
		canonicalMessages[i].Content = content
	}
	canonical, err := json.Marshal(canonicalMessages)
	if err != nil {
		return "", fmt.Errorf("序列化 archive canonical messages 失败: %w", err)
	}
	sum := sha256.Sum256(canonical)
	return fmt.Sprintf("%x", sum), nil
}

// blankFirstMessage 创建 messages 的 shallow copy，将第一条消息的 content
// 替换为占位文本字符串。
// D-05
func blankFirstMessage(messages []Message, tc *TokenCounter) []Message {
	result := make([]Message, len(messages))
	copy(result, messages)

	if len(result) == 0 {
		return result
	}

	// 计算折叠消息的 N 和 X
	archivedCount := len(messages) - 1
	archivedTokens := 0
	for i := 1; i < len(messages); i++ {
		archivedTokens += tc.CountMessageTokens(messages[i])
	}

	placeholder := fmt.Sprintf(
		"[Earlier conversation archived — %d messages, ~%d tokens. Operations: file reads, edits, tool calls, commits. Use reexpand to retrieve specific details.]",
		archivedCount, archivedTokens,
	)

	// 保留第一条消息的原始 Role，只替换 Content
	contentJSON, _ := json.Marshal(placeholder)
	result[0].Content = contentJSON

	return result
}

// ── 私有辅助函数 ──

// hasToolResultContent 检查消息的 content 中是否包含 tool_result 类型的 block。
func hasToolResultContent(content json.RawMessage) bool {
	blocks, _ := parseContent(content)
	for _, b := range blocks {
		if b.Type == "tool_result" {
			return true
		}
	}
	return false
}

// hasToolUseContent 检查消息的 content 中是否包含 tool_use 类型的 block。
func hasToolUseContent(content json.RawMessage) bool {
	blocks, _ := parseContent(content)
	for _, b := range blocks {
		if b.Type == "tool_use" {
			return true
		}
	}
	return false
}

// extractTimeline 从 content blocks 提取时间线条目。
// 扫描 tool_use 块中操作类 tool（read/write/edit/bash/execute 等）。
// role 为 "user" 时提取用户消息摘要作为 steering signal。
func extractTimeline(blocks []ContentBlock, msgIdx int, role string) []string {
	var events []string

	// 用户消息作为时间线条目（对话的方向信号）
	if role == "user" {
		text := extractTextFromBlocks(blocks)
		if text != "" {
			summary := truncateRunes(text, 120)
			events = append(events, fmt.Sprintf("- [%d] U: %s", msgIdx, summary))
		}
	}

	for _, b := range blocks {
		if b.Type != "tool_use" {
			continue
		}
		name := strings.ToLower(b.Name)

		// 跳过纯搜索/查询类 tool
		if name == "grep" || name == "glob" || name == "read" {
			continue
		}

		desc := formatTimelineEvent(b.Name, b.Input, msgIdx)
		if desc != "" {
			events = append(events, desc)
		}
	}
	return events
}

// extractTextFromBlocks 从 content blocks 中提取所有文本内容。
func extractTextFromBlocks(blocks []ContentBlock) string {
	var sb strings.Builder
	for _, b := range blocks {
		if b.Type == "text" && b.Text != "" {
			sb.WriteString(b.Text)
			sb.WriteString(" ")
		}
	}
	return strings.TrimSpace(sb.String())
}

// formatTimelineEvent 生成单个时间线条目的可读描述。
func formatTimelineEvent(toolName string, input map[string]any, msgIdx int) string {
	switch toolName {
	case "Edit":
		if fp := getFilePath(input); fp != "" {
			return fmt.Sprintf("- [%d] Edit: %s", msgIdx, basePath(fp))
		}
		return fmt.Sprintf("- [%d] Edit", msgIdx)
	case "Write":
		if fp := getFilePath(input); fp != "" {
			return fmt.Sprintf("- [%d] Write: %s", msgIdx, basePath(fp))
		}
		return fmt.Sprintf("- [%d] Write", msgIdx)
	case "Bash":
		cmd, _ := input["command"].(string)
		return formatBashTimelineEvent(cmd, msgIdx)
	case "Skill":
		skill, _ := input["skill"].(string)
		if skill != "" {
			return fmt.Sprintf("- [%d] Skill: %s", msgIdx, skill)
		}
		return fmt.Sprintf("- [%d] Skill", msgIdx)
	default:
		return fmt.Sprintf("- [%d] %s", msgIdx, toolName)
	}
}

// formatBashTimelineEvent 从 bash 命令提取有意义的时间线条目。
func formatBashTimelineEvent(cmd string, msgIdx int) string {
	cmd = strings.TrimSpace(cmd)

	if strings.Contains(cmd, "git commit") {
		return fmt.Sprintf("- [%d] git commit", msgIdx)
	}
	if strings.Contains(cmd, "go build") || strings.Contains(cmd, "make build") {
		return fmt.Sprintf("- [%d] build", msgIdx)
	}
	if strings.Contains(cmd, "go test") {
		return fmt.Sprintf("- [%d] test", msgIdx)
	}

	// 其他 bash：截断到 80 字符
	short := truncateRunes(strings.ReplaceAll(cmd, "\n", " "), 80)
	return fmt.Sprintf("- [%d] Bash: %s", msgIdx, short)
}

// extractGitCommits 从 content blocks 提取 git commit 信息。
func extractGitCommits(blocks []ContentBlock) []string {
	var commits []string
	for _, b := range blocks {
		if b.Type != "tool_use" || b.Name != "Bash" {
			continue
		}
		cmd, _ := b.Input["command"].(string)
		if !strings.Contains(cmd, "git commit") {
			continue
		}
		msg := extractCommitMessage(cmd)
		if msg != "" {
			commits = append(commits, msg)
		}
	}
	return commits
}

// extractCommitMessage 从 git commit 命令中提取 commit message。
func extractCommitMessage(cmd string) string {
	// -m "message" 格式
	if idx := findFlagValue(cmd, "-m"); idx >= 0 {
		rest := strings.TrimSpace(cmd[idx+2:])
		if len(rest) > 1 && rest[0] == '"' {
			if end := strings.IndexByte(rest[1:], '"'); end >= 0 {
				return rest[1 : end+1]
			}
		}
		if len(rest) > 1 && rest[0] == '\'' {
			if end := strings.IndexByte(rest[1:], '\''); end >= 0 {
				return rest[1 : end+1]
			}
		}
		return truncateRunes(rest, 200)
	}

	// heredoc 格式（简化处理）
	if strings.Contains(cmd, "<<") {
		return "commit (heredoc format)"
	}

	return ""
}

// findFlagValue 在命令字符串中查找 flag 的位置。
func findFlagValue(cmd, flag string) int {
	idx := strings.Index(cmd, flag+" ")
	if idx < 0 {
		return -1
	}
	return idx + len(flag)
}

// formatToolStub 将单个 tool_use block 格式化为 stub 文本。
// 对标 stubify.go:stubToolUses。Phase D: 追加 deep_search 提示。
func formatToolStub(name string, input map[string]any) string {
	annotation := extractAnnotation(input)
	block := ContentBlock{Type: "tool_use", Name: name, Input: input}
	keywords := extractToolKeywords(block)
	var text string
	if annotation != "" {
		text = fmt.Sprintf("[→] %s %s — %s", name, formatArgsSummary(input), annotation)
	} else {
		text = fmt.Sprintf("[→] %s %s", name, formatArgsSummary(input))
	}
	if keywords != "" {
		text += " → deep_search('" + keywords + "')"
	}
	return text
}

// extractToolEvents 从 content blocks 提取 tool 名 + args 摘要。
func extractToolEvents(blocks []ContentBlock) []string {
	var events []string
	for _, b := range blocks {
		if b.Type != "tool_use" {
			continue
		}
		stub := formatToolStub(b.Name, b.Input)
		events = append(events, "- "+stub)
	}
	return events
}

// extractFileList 从 content blocks 提取文件路径并去重。
func extractFileList(blocks []ContentBlock) []string {
	seen := make(map[string]bool)
	var files []string
	fileKeys := []string{"file_path", "path", "filename", "file", "dir", "directory", "filePath"}

	for _, b := range blocks {
		if b.Type != "tool_use" {
			continue
		}
		for _, key := range fileKeys {
			if val, ok := b.Input[key]; ok {
				if s, ok := val.(string); ok && s != "" {
					if !seen[s] {
						seen[s] = true
						files = append(files, s)
					}
				}
			}
		}
	}
	return files
}

// extractGotchas 从 content blocks 提取错误和警告信息。
func extractGotchas(blocks []ContentBlock) []string {
	var gotchas []string
	for _, b := range blocks {
		if b.Type == "tool_result" && b.IsError {
			// tool_result 标记为 error
			gotchas = append(gotchas, "[tool error]")
			continue
		}

		// 检查文本块中的错误关键词
		if b.Type == "text" {
			lower := strings.ToLower(b.Text)
			if strings.Contains(lower, "error") || strings.Contains(lower, "fail") {
				// 截取上下文
				short := truncateRunes(strings.TrimSpace(b.Text), 200)
				gotchas = append(gotchas, short)
			}
		}
	}
	return gotchas
}

// extractConclusion 从 content blocks 提取最后 1-2 句作为结论摘要。
func extractConclusion(blocks []ContentBlock) string {
	// 收集所有 text block 的文本
	var allText strings.Builder
	for _, b := range blocks {
		if b.Type == "text" {
			allText.WriteString(b.Text)
		}
	}
	text := strings.TrimSpace(allText.String())
	if text == "" {
		return ""
	}

	// 按句末标点分割，取最后 1-2 句
	sentences := splitSentences(text)
	if len(sentences) == 0 {
		return ""
	}

	// 取最后 1-2 句，总长度不超过 300 runes
	count := 1
	if len(sentences) >= 2 {
		count = 2
	}
	start := len(sentences) - count
	if start < 0 {
		start = 0
	}
	conclusion := strings.TrimSpace(strings.Join(sentences[start:], " "))
	return truncateRunes(conclusion, 300)
}

// splitSentences 按句末标点分割文本。
func splitSentences(text string) []string {
	var sentences []string
	runes := []rune(text)
	start := 0
	for i, r := range runes {
		if r == '.' || r == '!' || r == '?' || r == '。' || r == '！' || r == '？' {
			s := strings.TrimSpace(string(runes[start : i+1]))
			if s != "" {
				sentences = append(sentences, s)
			}
			start = i + 1
		}
	}
	// 处理剩余部分
	if start < len(runes) {
		s := strings.TrimSpace(string(runes[start:]))
		if s != "" {
			sentences = append(sentences, s)
		}
	}
	return sentences
}

// extractKeywords 从消息数组提取关键词。
// 提取三类：文件路径、tool 名、用户消息实词。
func extractKeywords(messages []Message) []KeywordEntry {
	seen := make(map[string]bool)
	var keywords []KeywordEntry

	addKW := func(word, source string) {
		if word == "" {
			return
		}
		key := word + "|" + source
		if !seen[key] {
			seen[key] = true
			keywords = append(keywords, KeywordEntry{Word: word, Source: source})
		}
	}

	for _, msg := range messages {
		blocks, _ := parseContent(msg.Content)

		// 提取文件路径
		for _, f := range extractFileList(blocks) {
			addKW(f, "file_path")
		}

		// 提取 tool 名
		for _, b := range blocks {
			if b.Type == "tool_use" && b.Name != "" {
				addKW(b.Name, "tool_name")
			}
		}

		// 从用户消息提取实词
		if msg.Role == "user" {
			var text string
			for _, b := range blocks {
				if b.Type == "text" {
					text += b.Text + " "
				}
			}
			words := tokenizeWords(strings.TrimSpace(text))
			for _, w := range words {
				if !isStopWord(w) {
					addKW(w, "user_message")
				}
			}
		}
	}

	return keywords
}

// isStopWord 判断词是否为停用词（大小写不敏感，词已由 tokenizeWords 转为小写）。
func isStopWord(word string) bool {
	stopWords := map[string]bool{
		"the": true, "a": true, "an": true, "is": true, "are": true, "was": true,
		"were": true, "be": true, "been": true, "being": true, "have": true,
		"has": true, "had": true, "do": true, "does": true, "did": true,
		"will": true, "would": true, "shall": true, "should": true, "may": true,
		"might": true, "must": true, "can": true, "could": true, "i": true,
		"you": true, "he": true, "she": true, "it": true, "we": true, "they": true,
		"me": true, "him": true, "her": true, "us": true, "them": true,
		"my": true, "your": true, "his": true, "its": true, "our": true, "their": true,
		"this": true, "that": true, "these": true, "those": true, "in": true,
		"on": true, "at": true, "to": true, "for": true, "of": true, "with": true,
		"by": true, "from": true, "up": true, "about": true, "into": true,
		"through": true, "during": true, "before": true, "after": true,
		"above": true, "below": true, "between": true, "and": true, "but": true,
		"or": true, "nor": true, "not": true, "so": true, "yet": true,
		"both": true, "either": true, "neither": true, "each": true, "every": true,
		"all": true, "any": true, "few": true, "more": true, "most": true,
		"other": true, "some": true, "such": true, "no": true, "only": true,
		"own": true, "same": true, "than": true, "too": true, "very": true,
		"just": true, "because": true, "as": true, "until": true, "while": true,
		"if": true, "when": true, "where": true, "how": true, "what": true,
		"which": true, "who": true, "whom": true, "here": true, "there": true,
		"then": true, "now": true,
	}
	return stopWords[word]
}

// getFilePath 从 tool_use input 中提取文件路径。
func getFilePath(input map[string]any) string {
	fileKeys := []string{"file_path", "path", "filename", "file", "filePath"}
	for _, key := range fileKeys {
		if val, ok := input[key]; ok {
			if s, ok := val.(string); ok && s != "" {
				return s
			}
		}
	}
	return ""
}

// basePath 返回路径的最后 2 个组件以提高可读性。
func basePath(fp string) string {
	if fp == "" {
		return "?"
	}
	parts := strings.Split(strings.ReplaceAll(fp, "\\", "/"), "/")
	if len(parts) <= 2 {
		return fp
	}
	return strings.Join(parts[len(parts)-2:], "/")
}

// ── Archive block 文本格式化 ──

// formatArchiveBlockText 生成 7 部分 archive block 的格式化文本。
func formatArchiveBlockText(
	rangeStart, rangeEnd, msgCount, tokens int,
	tools []string,
	files map[string]bool,
	commits, timeline, gotchas []string,
	conclusion string,
) string {
	var sb strings.Builder

	// 第 1 部分: 消息范围
	fmt.Fprintf(&sb, "Archived messages %d–%d（共 %d 条，约 %d tokens）",
		rangeStart, rangeEnd, msgCount, tokens)

	// 第 2 部分: Tool 摘要
	if len(tools) > 0 {
		sb.WriteString("\n\n### Tools Used\n")
		for _, t := range tools {
			fmt.Fprintf(&sb, "%s\n", t)
		}
	}

	// 第 3 部分: 文件列表
	if len(files) > 0 {
		sb.WriteString("\n### Files\n")
		// 排序以保证输出稳定
		sorted := make([]string, 0, len(files))
		for f := range files {
			sorted = append(sorted, f)
		}
		sortStrings(sorted)
		for _, f := range sorted {
			fmt.Fprintf(&sb, "- %s\n", f)
		}
	}

	// 第 4 部分: Commits
	if len(commits) > 0 {
		sb.WriteString("\n### Commits\n")
		for _, c := range commits {
			fmt.Fprintf(&sb, "%s\n", c)
		}
	}

	// 第 5 部分: Timeline
	if len(timeline) > 0 {
		sb.WriteString("\n### Timeline\n")
		// 预算控制：最多 120 条
		tl := timeline
		if len(tl) > 120 {
			kept := make([]string, 0, 121)
			kept = append(kept, tl[:20]...)
			kept = append(kept, fmt.Sprintf("  [...%d events omitted...]", len(tl)-120))
			kept = append(kept, tl[len(tl)-100:]...)
			tl = kept
		}
		for _, t := range tl {
			fmt.Fprintf(&sb, "%s\n", t)
		}
	}

	// 第 6 部分: Gotchas
	if len(gotchas) > 0 {
		sb.WriteString("\n### Gotchas\n")
		for _, g := range gotchas {
			fmt.Fprintf(&sb, "- %s\n", g)
		}
	}

	// 第 7 部分: Conclusion
	if conclusion != "" {
		sb.WriteString("\n### Conclusion\n")
		sb.WriteString(conclusion)
		sb.WriteString("\n")
	}

	return sb.String()
}

// sortStrings 对字符串 slice 排序。
func sortStrings(s []string) {
	sort.Strings(s)
}

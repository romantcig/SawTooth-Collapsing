package proxy

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
)

// ── 重展开引擎 ──

// ExtractKeywords 从最新用户消息提取关键词。
// 提取三类：文件路径、tool 名、用户消息实词（过滤停用词）。
// 返回去重后的关键词列表（保持顺序：文件路径 → tool 名 → 实词）。
// D-06, D-07
func ExtractKeywords(messages []Message) []string {
	// 获取最新用户消息文本
	userText := getLatestUserText(messages)
	if userText == "" && len(messages) == 0 {
		return nil
	}

	seen := make(map[string]bool)
	var keywords []string

	addKW := func(w string) {
		if w == "" || seen[w] {
			return
		}
		seen[w] = true
		keywords = append(keywords, w)
	}

	// 第 1 类: 文件路径（从最新用户消息文本中提取）
	for _, fp := range extractFilePaths(userText) {
		addKW(fp)
	}

	// 第 2 类: tool 名（从所有消息的 tool_use block 提取）
	toolNames := make(map[string]bool)
	for _, msg := range messages {
		blocks, _ := parseContent(msg.Content)
		for _, b := range blocks {
			if b.Type == "tool_use" && b.Name != "" {
				if !toolNames[b.Name] {
					toolNames[b.Name] = true
					addKW(b.Name)
				}
			}
		}
	}

	// 第 3 类: 用户消息实词（tokenizeWords + 停用词过滤）
	if userText != "" {
		words := tokenizeWords(userText)
		for _, w := range words {
			if !isStopWord(w) {
				addKW(w)
			}
		}
	}

	return keywords
}

// SearchAndExpand 搜索 SQLite 中的匹配 archive blocks 并注入到消息中。
// 预算控制：总注入 <= budget.ReExpansion（若 budget 非 nil），否则回退 <= 10% tokenThreshold。
// 每条 <= 2000 chars。不修改输入 messages，返回新 slice。
// D-06, D-08 / Phase E: Budget 集成。
func SearchAndExpand(messages []Message, store *SQLiteStore, tokenThreshold int, tc *TokenCounter, budget *Budget) []Message {
	// Guard 1: store == nil → 不做任何处理
	if store == nil {
		return messages
	}

	// Guard 2: messages 为空
	if len(messages) == 0 {
		return messages
	}

	// 提取关键词
	keywords := ExtractKeywords(messages)
	// Phase D: 从已桩化的消息中提取 deep_search 提示关键词
	hintKeywords := extractSearchHintsFromStubs(messages)
	keywords = append(keywords, hintKeywords...)
	// 跨来源去重：ExtractKeywords 和 extractSearchHintsFromStubs 可能重叠
	//（如文件路径、工具名同时出现在两者中），浪费 FTS5 的 20 槽限制
	keywords = dedupKeywords(keywords)
	if len(keywords) == 0 {
		return messages
	}

	// 构建 FTS5 查询：关键词用 OR 连接，双引号转义，最多 20 个关键词
	// T-03-06: 关键词注入防护——双引号转义 + 限制数量
	fts5Query := buildFTS5Query(keywords)
	if fts5Query == "" {
		return messages
	}
	// 执行搜索
	limit := 5
	if len(keywords) > limit {
		limit = len(keywords)
	}
	summaries, err := store.SearchArchives(fts5Query, limit)
	if err != nil {
		slog.Warn("archive 搜索失败", "query", fts5Query, "error", err)
		return messages
	}
	if len(summaries) == 0 {
		return messages
	}

	// 预算控制过滤（D-08）
	// T-03-08: 跨 session archive 通过 bm25 相关性 + 预算控制防止无关注入
	// Phase E: 动态预算——使用 Budget 的 ReExpansion，否则回退到 tokenThreshold 的 10%
	maxBudget := tokenThreshold / 10
	if budget != nil {
		maxBudget = budget.ReExpansion
	}

	// Phase E: 预算门控——若无可用预算则跳过重展开
	if budget != nil && !budget.CanSpendReExpansion(1) {
		return messages
	}
	tokenUsed := 0
	var injected []Message

	for i, summary := range summaries {
		// 分段感知截断到 2000 字符——优先保留 Gotchas/Conclusion
		truncated := truncateSummaryText(summary.SummaryText, 2000)
		if truncated == "" {
			continue
		}

		// 估算 token 消耗（含前缀 "[Retrieved archive #n — ...]\n\n"）
		prefix := fmt.Sprintf("[Retrieved archive #%d — %d-%d, ~%d tokens]\n\n",
			i+1, summary.BlockRangeStart, summary.BlockRangeEnd, summary.EstimatedTokens)
		cost := tc.CountTokens(prefix + truncated)

		// Budget 门控：超出预算则停止注入后续 block
		if budget != nil && tokenUsed+cost > maxBudget {
			slog.Info("archive 注入预算耗尽",
				"injected_count", i,
				"total_summaries", len(summaries),
				"token_used", tokenUsed,
				"max_budget", maxBudget,
			)
			break
		}
		tokenUsed += cost

		// 构造注入消息
		contentJSON, _ := json.Marshal(prefix + truncated)
		injected = append(injected, Message{
			Role:    "user",
			Content: contentJSON,
		})

		slog.Info("注入存档块",
			"session_id", summary.SessionID,
			"block_id", summary.ID,
			"range", fmt.Sprintf("%d-%d", summary.BlockRangeStart, summary.BlockRangeEnd),
		)
	}

	if len(injected) == 0 {
		return messages
	}

	// 在 messages 开头（索引 0 之后、索引 1 之前）插入 archive blocks
	result := make([]Message, 0, 1+len(injected)+len(messages)-1)
	result = append(result, messages[0])            // 保留第一条消息
	result = append(result, injected...)             // 插入 archive blocks
	result = append(result, messages[1:]...)         // 其余消息

	slog.Info("archive 注入完成",
		"injected_blocks", len(injected),
		"token_cost", tokenUsed,
		"budget_used_pct", fmt.Sprintf("%.1f%%", float64(tokenUsed)/float64(maxBudget)*100),
	)
	// Phase E: 记录重展开 token 支出
	if budget != nil {
		budget.SpendReExpansion(tokenUsed)
	}

	return result
}

// ── 私有辅助函数 ──

// getLatestUserText 从消息数组末尾向前遍历，返回第一条 user 消息的文本内容。
// 复用 extractLatestUserText 的模式（proxy.go:261-281）。
func getLatestUserText(messages []Message) string {
	for i := len(messages) - 1; i >= 0; i-- {
		if messages[i].Role != "user" {
			continue
		}

		// 尝试解析为纯字符串
		var text string
		if err := json.Unmarshal(messages[i].Content, &text); err == nil {
			return text
		}

		// 解析为 ContentBlock 数组
		blocks, _ := parseContent(messages[i].Content)
		for _, b := range blocks {
			if b.Type == "text" && b.Text != "" {
				return b.Text
			}
		}
		return ""
	}
	return ""
}

// extractFilePaths 从文本中提取文件路径。
// 匹配绝对路径（盘符或 / 开头）和相对路径（含 / 且含扩展名）。
func extractFilePaths(text string) []string {
	seen := make(map[string]bool)
	var paths []string

	// 按空白和标点分割，检查每个 token 是否像路径
	tokens := strings.FieldsFunc(text, func(r rune) bool {
		return r == ' ' || r == '\n' || r == '\t' || r == ',' || r == ';' || r == '"' || r == '\''
	})

	for _, token := range tokens {
		// 至少 3 个字符
		if len(token) < 3 {
			continue
		}

		// 绝对路径：盘符开头 (如 C:\...) 或 / 开头
		// 相对路径：含 / 且含 .
		isAbsPath := (len(token) >= 2 && token[1] == ':' && (token[2] == '\\' || token[2] == '/')) ||
			strings.HasPrefix(token, "/")
		isRelPath := strings.Contains(token, "/") && strings.Contains(token, ".")

		if (isAbsPath || isRelPath) && !seen[token] {
			seen[token] = true
			paths = append(paths, token)
		}
	}

	return paths
}

// buildFTS5Query 将关键词列表构建为 FTS5 MATCH 查询。
// 用 OR 连接，双引号转义，最多 20 个关键词。
// T-03-06: 关键词注入防护——双引号转义 + 限制数量。
// 所有 token 用双引号包裹（phrase literal 策略），使 FTS5 保留字符（* : ( ) + - 等）被视为字面量。
func buildFTS5Query(keywords []string) string {
	var parts []string
	for _, kw := range keywords {
		// FTS5 phrase literal 转义：双引号加倍（FTS5 规定双引号内 "" 才是字面 "）
		escaped := strings.ReplaceAll(kw, `"`, `""`)
		// 空关键词跳过
		if escaped == "" {
			continue
		}
		// 始终用双引号包裹——确保 FTS5 保留字符（* : ( ) + - 等）被视为字面量而非操作符
		parts = append(parts, `"`+escaped+`"`)
	}

	// 限制最多 20 个关键词（先过滤再截断，防止空词浪费槽位）
	if len(parts) > 20 {
		parts = parts[:20]
	}

	if len(parts) == 0 {
		return ""
	}

	return strings.Join(parts, " OR ")
}

// extractSearchHintsFromStubs 扫描所有消息的文本内容，提取已有的 deep_search 提示中的关键词。
// 对标 YesMem reexpand.go:127-138 extractSearchHint。
// Phase D: 让之前桩化/压缩周期生成的 deep_search 提示也能被 SearchAndExpand 检索。
func extractSearchHintsFromStubs(messages []Message) []string {
	seen := make(map[string]bool)
	var keywords []string

	addKW := func(w string) {
		if w == "" || seen[w] {
			return
		}
		seen[w] = true
		keywords = append(keywords, w)
	}

	for _, msg := range messages {
		// 尝试字符串格式
		var textStr string
		if err := json.Unmarshal(msg.Content, &textStr); err == nil {
			extractDSKeywords(textStr, addKW)
			continue
		}

		// ContentBlock 数组格式
		blocks, _ := parseContent(msg.Content)
		for _, b := range blocks {
			if b.Type == "text" && b.Text != "" {
				extractDSKeywords(b.Text, addKW)
			}
		}
	}

	return keywords
}

// dedupKeywords 去除关键词列表中的重复项，保持原始顺序。
func dedupKeywords(keywords []string) []string {
	seen := make(map[string]bool, len(keywords))
	result := make([]string, 0, len(keywords))
	for _, kw := range keywords {
		if kw == "" || seen[kw] {
			continue
		}
		seen[kw] = true
		result = append(result, kw)
	}
	return result
}

// truncateSummaryText 对 archive SummaryText 做分段感知截断：
// 超限时优先保留头部行与 Gotchas/Conclusion 尾段，剩余 rune 预算按原顺序
// 贪心装填中间段（整段装或整段弃），被省略的段在原位置插入一行标注。
// 不含任何已知段标题的文本回退 truncateRunes。
func truncateSummaryText(s string, maxRunes int) string {
	if countRunes(s) <= maxRunes {
		return s
	}

	// 段定位：按 formatArchiveBlockText 的段序精确查找各标题一次。
	// token 以 "\n### " 开头——生成器对 Tools Used 写 "\n\n###"，恰命中其第二个换行。
	titles := []string{"Tools Used", "Files", "Commits", "Timeline", "Gotchas", "Conclusion"}
	positions := make([]int, len(titles))
	searchFrom := 0
	for i, title := range titles {
		token := "\n### " + title + "\n"
		idx := strings.Index(s[searchFrom:], token)
		if idx < 0 {
			positions[i] = -1
			continue
		}
		positions[i] = searchFrom + idx
		searchFrom = positions[i] + len(token)
	}

	// 6 个标题全部未命中 → 非 7 段结构，防御回退
	firstHit := -1
	for _, pos := range positions {
		if pos >= 0 {
			firstHit = pos
			break
		}
	}
	if firstHit < 0 {
		return truncateRunes(s, maxRunes)
	}

	// 切分：head 为头部行；chunk 以自身 "\n### 标题\n" 开头、含该段全部内容
	head := s[:firstHit]
	chunks := make([]string, len(titles))
	for i, pos := range positions {
		if pos < 0 {
			continue
		}
		end := len(s)
		for j := i + 1; j < len(titles); j++ {
			if positions[j] >= 0 {
				end = positions[j]
				break
			}
		}
		chunks[i] = s[pos:end]
	}

	// 必保集：头部行 + Gotchas + Conclusion；自身超限则对拼接结果整体兜底
	mustKeep := head + chunks[4] + chunks[5]
	if countRunes(mustKeep) > maxRunes {
		return truncateRunes(mustKeep, maxRunes)
	}

	// 贪心装填：按原顺序遍历 4 个中间段，装不下则标记省略并继续试下一段
	remaining := maxRunes - countRunes(mustKeep)
	kept := make([]bool, len(titles))
	kept[4] = positions[4] >= 0
	kept[5] = positions[5] >= 0
	for i := 0; i < 4; i++ {
		if positions[i] < 0 {
			continue
		}
		n := countRunes(chunks[i])
		if n <= remaining {
			kept[i] = true
			remaining -= n
		}
	}

	// 组装：按原始段序输出保留段；每处连续省略段 run 在原位置插一行标注
	//（标注行不计入 rune 预算，真实预算控制在下游 CountTokens）
	var sb strings.Builder
	sb.WriteString(head)
	var omitted []string
	flushOmitted := func() {
		if len(omitted) == 0 {
			return
		}
		sb.WriteString("\n[...omitted: " + strings.Join(omitted, ", ") + "...]\n")
		omitted = omitted[:0]
	}
	for i := range titles {
		if positions[i] < 0 {
			continue
		}
		if kept[i] {
			flushOmitted()
			sb.WriteString(chunks[i])
		} else {
			omitted = append(omitted, titles[i])
		}
	}
	flushOmitted()
	return sb.String()
}

// extractDSKeywords 从文本中提取所有 deep_search('...') 提示中的关键词。
func extractDSKeywords(text string, addKW func(string)) {
	const marker = "deep_search('"
	for {
		idx := strings.Index(text, marker)
		if idx < 0 {
			return
		}
		start := idx + len(marker)
		end := strings.Index(text[start:], "')")
		if end < 0 {
			return
		}
		hint := text[start : start+end]
		// 按空白分词，每个词作为独立关键词
		for _, word := range strings.Fields(hint) {
			word = strings.Trim(word, ".,;:!?\"'()[]{}…—")
			if len(word) >= 3 && !isStopWord(word) {
				addKW(word)
			}
		}
		text = text[start+end+2:] // 跳过 "')"
	}
}

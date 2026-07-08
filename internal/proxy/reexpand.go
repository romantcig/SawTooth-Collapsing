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

	// 构建 FTS5 查询：关键词用 OR 连接，单引号转义，最多 20 个关键词
	// T-03-06: 关键词注入防护——单引号转义 + 限制数量
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
	} // 10% of threshold
	tokenUsed := 0
	var injected []Message

	for i, summary := range summaries {
		// 截断到 2000 字符
		truncated := truncateRunes(summary.SummaryText, 2000)
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

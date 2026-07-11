package proxy

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"strings"
)

// ── 重展开引擎 ──

type RecallSignalKind string

const (
	RecallSignalDeepSearch RecallSignalKind = "deep_search"
	RecallSignalExactPath  RecallSignalKind = "exact_path"
	RecallSignalRecovery   RecallSignalKind = "recovery_intent"
	maxRecallSignals                        = 3
)

// RecallSignal 是一次明确、可审计的 Archive 恢复线索。
type RecallSignal struct {
	Kind       RecallSignalKind
	Query      string
	Terms      []string
	ExactPath  string
	MessageIdx int
	StubText   string
}

var recallIntentPhrases = []string{
	"recall archive", "restore archive", "retrieve archive", "search archive",
	"recall earlier", "restore earlier", "find earlier", "from earlier",
	"恢复存档", "找回存档", "恢复之前", "找回之前", "搜索存档",
}

var recallCommonWords = map[string]bool{
	"archive": true, "recall": true, "restore": true, "retrieve": true,
	"search": true, "earlier": true, "previous": true, "before": true,
	"request": true, "tool": true, "user": true, "assistant": true,
	"bash": true, "read": true, "write": true, "edit": true,
	"grep": true, "glob": true, "mcp": true, "agent": true,
	"恢复": true, "找回": true, "之前": true, "存档": true, "搜索": true,
}

// extractRecallSignals 只接受 deep_search、精确路径和窄恢复意图。
func extractRecallSignals(messages []Message) []RecallSignal {
	latest := getLatestUserText(messages)
	if latest == "" {
		return nil
	}
	queryTerms := effectiveRecallTerms(latest)
	var signals []RecallSignal
	seen := make(map[string]bool)
	add := func(signal RecallSignal) {
		key := string(signal.Kind) + "\x00" + strings.ToLower(signal.Query)
		if signal.Query == "" || seen[key] || len(signals) >= maxRecallSignals {
			return
		}
		seen[key] = true
		signals = append(signals, signal)
	}

	for _, path := range extractFilePaths(latest) {
		add(RecallSignal{Kind: RecallSignalExactPath, Query: path, Terms: []string{path}, ExactPath: path, MessageIdx: -1})
	}

	for msgIdx, msg := range messages {
		var texts []string
		var plain string
		if err := json.Unmarshal(msg.Content, &plain); err == nil {
			texts = append(texts, plain)
		} else {
			blocks, _ := parseContent(msg.Content)
			for _, block := range blocks {
				if block.Type == "text" && block.Text != "" {
					texts = append(texts, block.Text)
				}
			}
		}
		for _, text := range texts {
			for _, hint := range extractDeepSearchHints(text) {
				hintTerms := effectiveRecallTerms(hint)
				path := exactPathInText(hint)
				if path == "" && countTermOverlap(queryTerms, hintTerms) < 2 {
					continue
				}
				if path != "" && !containsPath(latest, path) && countTermOverlap(queryTerms, hintTerms) < 2 {
					continue
				}
				add(RecallSignal{Kind: RecallSignalDeepSearch, Query: hint, Terms: hintTerms, ExactPath: path, MessageIdx: msgIdx, StubText: text})
			}
		}
	}

	lower := strings.ToLower(latest)
	for _, phrase := range recallIntentPhrases {
		if !strings.Contains(lower, phrase) {
			continue
		}
		if len(queryTerms) > 0 {
			add(RecallSignal{Kind: RecallSignalRecovery, Query: strings.Join(queryTerms, " "), Terms: queryTerms, MessageIdx: -1})
		}
		break
	}
	return signals
}

func effectiveRecallTerms(text string) []string {
	seen := make(map[string]bool)
	var terms []string
	for _, word := range tokenizeWords(strings.ToLower(text)) {
		word = strings.Trim(word, ".,;:!?\"'()[]{}…—")
		if len([]rune(word)) < 3 || isStopWord(word) || recallCommonWords[word] || seen[word] {
			continue
		}
		seen[word] = true
		terms = append(terms, word)
		if len(terms) == 8 {
			break
		}
	}
	return terms
}

func countTermOverlap(a, b []string) int {
	set := make(map[string]bool, len(a))
	for _, term := range a {
		set[strings.ToLower(term)] = true
	}
	count := 0
	for _, term := range b {
		if set[strings.ToLower(term)] {
			count++
		}
	}
	return count
}

// RecallOutcome 描述一次请求最终保留的 archive 召回结果。
type RecallOutcome struct {
	Messages        []Message
	Attempted       bool
	Candidates      int
	Selected        int
	Injected        int
	Discarded       int
	TokenCost       int
	BudgetLimit     int
	BudgetRemaining int
}

type recallCandidate struct {
	Signal      RecallSignal
	Summary     ArchiveSummary
	SameSession bool
}

// SearchAndExpand 搜索 SQLite 中的匹配 archive blocks 并注入到消息中。
// 每个候选只在实际注入前按真实 token cost 门控并立即扣费。
// budget 为 nil 时仍受 tokenThreshold/10 的本地硬上限约束。
// 每条 <= 2000 chars。不修改输入 messages。
// D-06, D-08 / Phase E: Budget 集成。
func SearchAndExpand(messages []Message, store *SQLiteStore, tokenThreshold int, tc *TokenCounter, budget *Budget) RecallOutcome {
	return searchAndExpandForSession(messages, store, tokenThreshold, tc, budget, "")
}

func searchAndExpandForSession(messages []Message, store *SQLiteStore, tokenThreshold int, tc *TokenCounter, budget *Budget, requestSessionID string) RecallOutcome {
	return searchAndExpandWithMeta(messages, store, tokenThreshold, tc, budget, newRequestMeta(0, requestSessionID))
}

func searchAndExpandWithMeta(messages []Message, store *SQLiteStore, tokenThreshold int, tc *TokenCounter, budget *Budget, meta *requestMeta) (outcome RecallOutcome) {
	logger := slog.Default()
	requestSessionID := ""
	if meta != nil {
		logger = meta.Logger
		requestSessionID = meta.RequestSessionID
	}
	maxBudget := tokenThreshold / 10
	if maxBudget < 0 {
		maxBudget = 0
	}
	outcome = RecallOutcome{
		Messages:        messages,
		BudgetLimit:     maxBudget,
		BudgetRemaining: maxBudget,
	}
	if budget != nil {
		maxBudget = budget.RemainingReExpansion()
		outcome.BudgetLimit = budget.ReExpansion
		outcome.BudgetRemaining = maxBudget
	}

	// Guard 1: store == nil → 不做任何处理
	if store == nil {
		return outcome
	}

	// Guard 2: messages 为空
	if len(messages) == 0 {
		return outcome
	}

	signals := extractRecallSignals(messages)
	if len(signals) == 0 {
		return outcome
	}
	outcome.Attempted = true
	defer func() {
		logger.Info("Archive 召回汇总",
			"candidates", outcome.Candidates,
			"selected", outcome.Selected,
			"injected", outcome.Injected,
			"discarded", outcome.Discarded,
			"token_cost", outcome.TokenCost,
			"budget_limit", outcome.BudgetLimit,
			"budget_remaining", outcome.BudgetRemaining,
		)
	}()
	seenCandidates := make(map[string]bool)
	var candidates []recallCandidate
	for _, signal := range signals {
		terms := signal.Terms
		if signal.ExactPath != "" {
			terms = []string{signal.ExactPath}
		}
		query := buildFTS5Query(terms)
		if query == "" {
			continue
		}
		summaries, err := store.SearchArchives(query, 1)
		if err != nil {
			logger.Warn("archive 搜索失败", "query", query, "error", err)
			continue
		}
		for _, summary := range summaries {
			if seenCandidates[summary.ID] {
				continue
			}
			seenCandidates[summary.ID] = true
			sameSession := requestSessionID != "" && summary.SessionID == requestSessionID
			if !candidateMeetsRecallThreshold(signal, summary, sameSession) {
				continue
			}
			candidates = append(candidates, recallCandidate{Signal: signal, Summary: summary, SameSession: sameSession})
		}
	}
	outcome.Candidates = len(seenCandidates)
	if len(candidates) == 0 {
		return outcome
	}
	sort.SliceStable(candidates, func(i, j int) bool { return recallCandidateLess(candidates[i], candidates[j]) })
	candidates = dedupeDominatedCandidates(candidates)
	if len(candidates) > maxRecallSignals {
		candidates = candidates[:maxRecallSignals]
	}

	localSpent := 0
	canSpend := func(cost int) bool {
		if cost < 0 {
			return false
		}
		if budget != nil {
			return budget.CanSpendReExpansion(cost)
		}
		return localSpent+cost <= maxBudget
	}
	spend := func(cost int) {
		if budget != nil {
			budget.SpendReExpansion(cost)
			outcome.BudgetRemaining = budget.RemainingReExpansion()
		} else {
			localSpent += cost
			outcome.BudgetRemaining = maxBudget - localSpent
		}
		outcome.TokenCost += cost
	}

	// 同 session 完整展开去重：一个 session 只完整展开一次，预算留给不同 session 的块
	expandedSessions := make(map[string]bool)
	fullExpansionCount := 0
	result := messages

	for i, candidate := range candidates {
		summary := candidate.Summary
		// 分段感知截断到 2000 字符——优先保留 Gotchas/Conclusion
		truncated := truncateSummaryText(summary.SummaryText, 2000)
		if truncated == "" {
			continue
		}
		outcome.Selected++

		// 估算 token 消耗（含前缀 "[Retrieved archive #n — ...]\n\n"）
		prefix := fmt.Sprintf("[Retrieved archive #%d — source=%s, range=%d-%d, ~%d tokens]\n\n",
			i+1, summary.SessionID, summary.BlockRangeStart, summary.BlockRangeEnd, summary.EstimatedTokens)

		// 完整展开尝试：messages_json 可用且剩余预算装得下时，摘要 header 后附
		// 完整原始消息（LLM 先看摘要判断相关性，需要细节再往下翻）；
		// JSON 损坏、预算不足、同 session 已展开——任一不满足即落回 summary_text 路径。
		// 与 summary 路径不同，本分支在 budget == nil 时也按 maxBudget 门控——
		// 完整消息可达数万 token，无约束注入会挤爆上下文。
		payload, payloadCost, fullExpanded := "", 0, false
		rawHistoryStillPresent := candidate.SameSession && summary.BlockRangeEnd < len(messages)
		if summary.MessagesJSON != "" && fullExpansionCount == 0 && !expandedSessions[summary.SessionID] && !rawHistoryStillPresent {
			headCost := tc.CountTokens(prefix + truncated)
			remaining := outcome.BudgetRemaining - headCost
			if remaining > 0 {
				// rune 预算 ≈ 2×token 预算：粗剪防格式化浪费，真门控靠下方 CountTokens 实测
				if full, ok := formatFullMessages(summary.MessagesJSON, remaining*2); ok {
					candidate := prefix + truncated + "\n\n--- Full messages ---\n" + full
					if cost := tc.CountTokens(candidate); canSpend(cost) {
						payload = candidate
						payloadCost = cost
						fullExpanded = true
					}
				}
			}
		}

		if payload == "" {
			cost := tc.CountTokens(prefix + truncated)

			// Budget 门控：超出预算则先按 1000→500 减半降级重试，降到底仍装不下才停止注入
			if !canSpend(cost) {
				fits := false
				for _, level := range []int{1000, 500} {
					truncated = truncateSummaryText(summary.SummaryText, level)
					cost = tc.CountTokens(prefix + truncated)
					if canSpend(cost) {
						fits = true
						break
					}
				}
				if !fits {
					outcome.Discarded++
					logger.Debug("archive 注入预算耗尽",
						"injected_count", outcome.Injected,
						"total_summaries", len(candidates),
						"token_used", outcome.TokenCost,
						"budget_remaining", outcome.BudgetRemaining,
						"degraded_levels", "1000,500",
					)
					continue
				}
			}
			payload = prefix + truncated
			payloadCost = cost
		}

		var applied bool
		result, applied = applyRecallPayload(result, candidate.Signal, payload)
		if !applied {
			outcome.Discarded++
			continue
		}
		spend(payloadCost)
		if fullExpanded {
			expandedSessions[summary.SessionID] = true
			fullExpansionCount++
		}
		outcome.Injected++

		logger.Debug("注入存档块",
			"source_session_id", summary.SessionID,
			"block_id", summary.ID,
			"range", fmt.Sprintf("%d-%d", summary.BlockRangeStart, summary.BlockRangeEnd),
			"full_expansion", fullExpanded,
		)
	}

	outcome.Messages = result
	return outcome
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

func extractDeepSearchHints(text string) []string {
	const marker = "deep_search('"
	var hints []string
	for {
		idx := strings.Index(text, marker)
		if idx < 0 {
			return hints
		}
		start := idx + len(marker)
		end := strings.Index(text[start:], "')")
		if end < 0 {
			return hints
		}
		if hint := strings.TrimSpace(text[start : start+end]); hint != "" {
			hints = append(hints, hint)
		}
		text = text[start+end+2:]
	}
}

func exactPathInText(text string) string {
	paths := extractFilePaths(text)
	if len(paths) == 0 {
		return ""
	}
	return paths[0]
}

func containsPath(text, path string) bool {
	return strings.Contains(normalizeRecallPath(text), normalizeRecallPath(path))
}

func normalizeRecallPath(path string) string {
	return strings.ToLower(strings.ReplaceAll(path, `\`, "/"))
}

func matchedExactPath(summary ArchiveSummary, path string) bool {
	want := normalizeRecallPath(path)
	for _, term := range summary.MatchedTerms {
		if normalizeRecallPath(term) == want {
			return true
		}
	}
	return false
}

func candidateMeetsRecallThreshold(signal RecallSignal, summary ArchiveSummary, sameSession bool) bool {
	if signal.ExactPath != "" {
		return matchedExactPath(summary, signal.ExactPath)
	}
	required := 3
	if sameSession {
		required = 2
	}
	return summary.MatchedTermCount >= required
}

func recallCandidateLess(a, b recallCandidate) bool {
	if a.SameSession != b.SameSession {
		return a.SameSession
	}
	if a.Summary.MatchedTermCount != b.Summary.MatchedTermCount {
		return a.Summary.MatchedTermCount > b.Summary.MatchedTermCount
	}
	if a.Summary.Rank != b.Summary.Rank {
		return a.Summary.Rank < b.Summary.Rank
	}
	if a.Summary.CreatedAt != b.Summary.CreatedAt {
		return a.Summary.CreatedAt > b.Summary.CreatedAt
	}
	aSpan := a.Summary.BlockRangeEnd - a.Summary.BlockRangeStart
	bSpan := b.Summary.BlockRangeEnd - b.Summary.BlockRangeStart
	if aSpan != bSpan {
		return aSpan > bSpan
	}
	return a.Summary.ID < b.Summary.ID
}

func dedupeDominatedCandidates(candidates []recallCandidate) []recallCandidate {
	seenHashes := make(map[string]bool)
	result := make([]recallCandidate, 0, len(candidates))
	for _, candidate := range candidates {
		if hash := candidate.Summary.ContentHash; hash != "" && seenHashes[hash] {
			continue
		}
		dominated := false
		for _, kept := range result {
			if candidate.Summary.SessionID != kept.Summary.SessionID {
				continue
			}
			if rangesHighlyOverlap(candidate.Summary, kept.Summary) {
				dominated = true
				break
			}
		}
		if dominated {
			continue
		}
		if candidate.Summary.ContentHash != "" {
			seenHashes[candidate.Summary.ContentHash] = true
		}
		result = append(result, candidate)
	}
	return result
}

func rangesHighlyOverlap(a, b ArchiveSummary) bool {
	start := a.BlockRangeStart
	if b.BlockRangeStart > start {
		start = b.BlockRangeStart
	}
	end := a.BlockRangeEnd
	if b.BlockRangeEnd < end {
		end = b.BlockRangeEnd
	}
	if end < start {
		return false
	}
	intersection := end - start + 1
	aLen := a.BlockRangeEnd - a.BlockRangeStart + 1
	bLen := b.BlockRangeEnd - b.BlockRangeStart + 1
	shorter := aLen
	if bLen < shorter {
		shorter = bLen
	}
	return shorter > 0 && intersection*5 >= shorter*4
}

func applyRecallPayload(messages []Message, signal RecallSignal, payload string) ([]Message, bool) {
	if signal.Kind == RecallSignalDeepSearch {
		if result, ok := replaceRecallStub(messages, signal, payload); ok {
			return result, true
		}
	}
	return appendRecallToLatestUser(messages, payload)
}

func replaceRecallStub(messages []Message, signal RecallSignal, payload string) ([]Message, bool) {
	if signal.MessageIdx < 0 || signal.MessageIdx >= len(messages) || signal.StubText == "" {
		return messages, false
	}
	result := append([]Message(nil), messages...)
	message := result[signal.MessageIdx]
	var text string
	if err := json.Unmarshal(message.Content, &text); err == nil {
		if text != signal.StubText {
			return messages, false
		}
		message.Content, _ = json.Marshal(payload)
		result[signal.MessageIdx] = message
		return result, true
	}
	var blocks []ContentBlock
	if err := json.Unmarshal(message.Content, &blocks); err != nil {
		return messages, false
	}
	for i := range blocks {
		if blocks[i].Type != "text" || blocks[i].Text != signal.StubText {
			continue
		}
		blocks[i] = ContentBlock{Type: "text", Text: payload}
		message.Content, _ = json.Marshal(blocks)
		result[signal.MessageIdx] = message
		return result, true
	}
	return messages, false
}

func appendRecallToLatestUser(messages []Message, payload string) ([]Message, bool) {
	for i := len(messages) - 1; i >= 0; i-- {
		if messages[i].Role != "user" {
			continue
		}
		result := append([]Message(nil), messages...)
		message := result[i]
		var blocks []ContentBlock
		if err := json.Unmarshal(message.Content, &blocks); err != nil {
			var text string
			if err := json.Unmarshal(message.Content, &text); err != nil {
				return messages, false
			}
			blocks = []ContentBlock{{Type: "text", Text: text}}
		}
		blocks = append(blocks, ContentBlock{Type: "text", Text: payload})
		message.Content, _ = json.Marshal(blocks)
		result[i] = message
		return result, true
	}
	return messages, false
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
		token = strings.Trim(token, ".,;:!?()[]{}<>\"")
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

// 完整展开的消息级截断上限。
const (
	fullExpandMaxMsgRunes     = 2000 // 单条消息文本上限
	fullExpandToolResultRunes = 200  // 单个 tool_result 上限——常含整页文件内容，狠截防止吃光预算
)

// formatFullMessages 将 archive 的 messages_json 反序列化为折叠前的原始消息，
// 格式化为纯文本（"[role]: ..." 逐条拼接）。原始消息含 tool_use/tool_result
// 配对结构，不能按原样插回消息数组（API 400），只能文本化后随注入消息携带。
// 从后往前贪心装填 rune 预算（最近的对话最重要），输出按时间正序，
// 装不下的头部消息以一行省略标注代替（标注行不计预算，真门控在调用方 CountTokens）。
// JSON 损坏、"null" 或无有效文本时返回 ("", false)，调用方降级 summary_text。
func formatFullMessages(messagesJSON string, maxRunes int) (string, bool) {
	var msgs []Message
	if err := json.Unmarshal([]byte(messagesJSON), &msgs); err != nil {
		return "", false
	}
	if len(msgs) == 0 {
		return "", false
	}

	var parts []string
	used := 0
	omitted := 0
	for i := len(msgs) - 1; i >= 0; i-- {
		text := formatMessageText(msgs[i])
		if text == "" {
			continue
		}
		line := "[" + msgs[i].Role + "]: " + truncateRunes(text, fullExpandMaxMsgRunes)
		n := countRunes(line) + 1 // +1 为拼接换行
		if used+n > maxRunes {
			omitted = i + 1
			break
		}
		used += n
		parts = append(parts, line)
	}
	if len(parts) == 0 {
		return "", false
	}

	// 反转回时间正序
	for l, r := 0, len(parts)-1; l < r; l, r = l+1, r-1 {
		parts[l], parts[r] = parts[r], parts[l]
	}
	result := strings.Join(parts, "\n")
	if omitted > 0 {
		result = fmt.Sprintf("[...%d earlier messages omitted...]\n", omitted) + result
	}
	return result, true
}

// formatMessageText 将单条消息的 content blocks 压成纯文本。
// text 原样保留，tool_use 压成一行 "[tool: Name]"，tool_result 截断后带 "[result]" 前缀。
// thinking 跳过——内部推理文本冗长且对召回无价值。
func formatMessageText(msg Message) string {
	blocks, _ := parseContent(msg.Content)
	var sb strings.Builder
	for _, b := range blocks {
		var piece string
		switch b.Type {
		case "text":
			piece = b.Text
		case "tool_use":
			piece = "[tool: " + b.Name + "]"
		case "tool_result":
			if t := toolResultText(b.Content); t != "" {
				piece = "[result] " + truncateRunes(t, fullExpandToolResultRunes)
			}
		}
		if piece == "" {
			continue
		}
		if sb.Len() > 0 {
			sb.WriteString("\n")
		}
		sb.WriteString(piece)
	}
	return sb.String()
}

// toolResultText 从 tool_result 的 Content(any) 提取文本。
// API 形态两种：字符串，或嵌套 content blocks（[]any 内 map，取 type=="text" 的 text）。
func toolResultText(content any) string {
	switch c := content.(type) {
	case string:
		return c
	case []any:
		var sb strings.Builder
		for _, item := range c {
			m, ok := item.(map[string]any)
			if !ok || m["type"] != "text" {
				continue
			}
			if txt, ok := m["text"].(string); ok && txt != "" {
				if sb.Len() > 0 {
					sb.WriteString("\n")
				}
				sb.WriteString(txt)
			}
		}
		return sb.String()
	}
	return ""
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

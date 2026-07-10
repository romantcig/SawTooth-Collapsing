package proxy

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

// ---- truncateSummaryText 分段感知截断测试 ----

// buildBigFiles 生成 n 个长路径条目的 files 集合（每条目行约 63 runes）。
func buildBigFiles(n int) map[string]bool {
	files := make(map[string]bool, n)
	for i := 0; i < n; i++ {
		files[fmt.Sprintf("src/pkg%02d/%s.go", i, strings.Repeat("f", 47))] = true
	}
	return files
}

// buildBigTimeline 生成 n 条长事件行（每条约 61 runes）。
func buildBigTimeline(n int) []string {
	timeline := make([]string, n)
	for i := range timeline {
		timeline[i] = fmt.Sprintf("event %03d %s", i, strings.Repeat("y", 50))
	}
	return timeline
}

// (a) 超限时巨型中间段 Files/Timeline 整段省略且各自原位置有一行标注，
// 头部行、小中间段与必保的 Gotchas/Conclusion 按原顺序保留。
func TestTruncateSummaryTextPreservesGotchasConclusion(t *testing.T) {
	// Gotchas/Conclusion 内容避开 Files、Timeline 字样，以免干扰省略断言
	text := formatArchiveBlockText(1, 8, 8, 1200,
		[]string{"Bash", "Read"},
		buildBigFiles(50),
		[]string{"abc1234 fix parser"},
		buildBigTimeline(55),
		[]string{"beware of rune boundaries"},
		"all sections verified",
	)
	got := truncateSummaryText(text, 500)

	for _, want := range []string{
		"Archived messages",
		"### Tools Used",
		"### Commits",
		"### Gotchas",
		"### Conclusion",
		"[...omitted:",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("output should contain %q, got:\n%s", want, got)
		}
	}
	for _, absent := range []string{"### Files", "### Timeline"} {
		if strings.Contains(got, absent) {
			t.Errorf("output should not contain %q, got:\n%s", absent, got)
		}
	}
	// 两处省略非相邻（中间隔着保留的 Commits），各自成 run 各插一行标注
	if !strings.Contains(got, "[...omitted: Files...]") {
		t.Errorf("output should contain Files omission marker, got:\n%s", got)
	}
	if !strings.Contains(got, "[...omitted: Timeline...]") {
		t.Errorf("output should contain Timeline omission marker, got:\n%s", got)
	}
}

// (b) 未超限的完整 7 段文本原样返回（字节相等）。
func TestTruncateSummaryTextUnderLimitUnchanged(t *testing.T) {
	text := formatArchiveBlockText(1, 4, 4, 300,
		[]string{"Bash"},
		map[string]bool{"main.go": true},
		[]string{"abc1234 init"},
		[]string{"event one"},
		[]string{"watch the edge case"},
		"done",
	)
	if got := truncateSummaryText(text, 5000); got != text {
		t.Errorf("under-limit input should be returned unchanged, got:\n%s", got)
	}
}

// (c) 不含任何已知段标题的文本回退 truncateRunes（继承围栏补闭合语义）。
func TestTruncateSummaryTextNonSectionedFallback(t *testing.T) {
	text := strings.Repeat("plain narrative without any section header. ", 15)
	got := truncateSummaryText(text, 200)
	want := truncateRunes(text, 200)
	if got != want {
		t.Errorf("non-sectioned input should fall back to truncateRunes:\ngot:  %q\nwant: %q", got, want)
	}
}

// (d) 仅 Gotchas+Conclusion 且必保集自身超限 → 对拼接结果整体 truncateRunes 兜底。
func TestTruncateSummaryTextMustKeepOverflowFallback(t *testing.T) {
	gotchas := make([]string, 40)
	for i := range gotchas {
		gotchas[i] = fmt.Sprintf("lesson %02d: %s", i, strings.Repeat("g", 70))
	}
	text := formatArchiveBlockText(1, 8, 8, 900, nil, nil, nil, nil, gotchas, "keep it short")
	got := truncateSummaryText(text, 200)
	want := truncateRunes(text, 200)
	if got != want {
		t.Errorf("must-keep overflow should fall back to whole-text truncateRunes:\ngot:  %q\nwant: %q", got, want)
	}
}

// ---- SearchAndExpand 预算降级测试 ----

// seedBudgetStore 播种单个含巨型中间段 7 段 SummaryText 的 archive block。
// Files 段约 1600 runes：2000 档装得下、1000/500 档装不下；Timeline 段约
// 3000 runes：任何档都装不下——保证 2000 档与 500 档截断产物 token 数拉开差距。
// 返回 store、SummaryText、触发检索的 messages 与 TokenCounter。
func seedBudgetStore(t *testing.T) (*SQLiteStore, string, []Message, *TokenCounter) {
	t.Helper()

	tc, err := NewTokenCounter()
	if err != nil {
		t.Fatalf("NewTokenCounter: %v", err)
	}

	store, err := NewSQLiteStore(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("NewSQLiteStore failed: %v", err)
	}
	t.Cleanup(func() { store.Close() })

	summaryText := formatArchiveBlockText(1, 8, 8, 1200,
		[]string{"Bash", "Read"},
		buildBigFiles(25),
		[]string{"abc1234 fix flimflam parser"},
		buildBigTimeline(50),
		[]string{"flimflam requires quoted input"},
		"flimflam pipeline verified",
	)
	block := ArchiveBlock{
		ID: "block-budget", SessionID: "s1",
		BlockRangeStart: 1, BlockRangeEnd: 8,
		MessageCount: 8, EstimatedTokens: 1200,
		SummaryText: summaryText,
		Keywords:    []KeywordEntry{{Word: "flimflam", Source: "user_message"}},
	}
	if err := store.SaveArchive(block); err != nil {
		t.Fatalf("SaveArchive failed: %v", err)
	}

	messages := []Message{
		{Role: "user", Content: json.RawMessage(`"tell me about flimflam"`)},
	}
	return store, summaryText, messages, tc
}

// (e1) 预算介于 500 档与 2000 档 cost 之间：2000 档必超、降级后必进，
// 注入的是降级后的更短形态而非整条丢弃。
func TestSearchAndExpandBudgetDegradesTruncation(t *testing.T) {
	store, summaryText, messages, tc := seedBudgetStore(t)

	// 复刻注入循环的 prefix（archive 序号 1 + 播种块的 Range 与 EstimatedTokens）
	prefix := fmt.Sprintf("[Retrieved archive #%d — %d-%d, ~%d tokens]\n\n", 1, 1, 8, 1200)
	cost2000 := tc.CountTokens(prefix + truncateSummaryText(summaryText, 2000))
	cost500 := tc.CountTokens(prefix + truncateSummaryText(summaryText, 500))
	if cost500 >= cost2000 {
		t.Fatalf("test construction broken: cost500=%d should be < cost2000=%d", cost500, cost2000)
	}
	budget := &Budget{ReExpansion: (cost2000 + cost500) / 2}

	outcome := SearchAndExpand(messages, store, 100000, tc, budget)
	result := outcome.Messages
	if len(result) != len(messages)+1 {
		t.Fatalf("expected %d messages after degraded injection, got %d", len(messages)+1, len(result))
	}
	var injectedText string
	if err := json.Unmarshal(result[1].Content, &injectedText); err != nil {
		t.Fatalf("unmarshal injected content: %v", err)
	}
	if !strings.Contains(injectedText, "[Retrieved archive #1") {
		t.Errorf("injected message should contain archive prefix, got: %q", injectedText)
	}
	// 注入文本必须严格短于 2000 档形态——证明降级实际生效而非 2000 档原样进入
	full2000 := countRunes(prefix + truncateSummaryText(summaryText, 2000))
	if countRunes(injectedText) >= full2000 {
		t.Errorf("injected text (%d runes) should be shorter than 2000-level form (%d runes)",
			countRunes(injectedText), full2000)
	}
}

// (e2) ReExpansion=1 通过 CanSpendReExpansion(1) 前置门，但 1000/500 两档
// 均装不下 → 停止注入，返回原 messages。
func TestSearchAndExpandBudgetExhaustedStopsInjection(t *testing.T) {
	store, _, messages, tc := seedBudgetStore(t)

	budget := &Budget{ReExpansion: 1}
	outcome := SearchAndExpand(messages, store, 100000, tc, budget)
	result := outcome.Messages
	if len(result) != len(messages) {
		t.Fatalf("expected no injection with exhausted budget, got %d messages (want %d)", len(result), len(messages))
	}
}

// ---- formatFullMessages 完整展开格式化测试 ----

// mustJSON 将 []Message 序列化为 messages_json 字符串（复刻 SaveArchive 的写入形态）。
func mustJSON(t *testing.T, msgs []Message) string {
	t.Helper()
	data, err := json.Marshal(msgs)
	if err != nil {
		t.Fatalf("marshal messages: %v", err)
	}
	return string(data)
}

// 正常路径：string content 与 blocks content 混合，输出 "[role]: 文本" 按时间正序。
func TestFormatFullMessagesBasic(t *testing.T) {
	msgs := []Message{
		{Role: "user", Content: json.RawMessage(`"first question"`)},
		{Role: "assistant", Content: json.RawMessage(`[{"type":"text","text":"second answer"}]`)},
	}
	got, ok := formatFullMessages(mustJSON(t, msgs), 100000)
	if !ok {
		t.Fatal("expected ok=true for valid messages")
	}
	wantUser := "[user]: first question"
	wantAsst := "[assistant]: second answer"
	if !strings.Contains(got, wantUser) || !strings.Contains(got, wantAsst) {
		t.Errorf("missing formatted lines, got: %q", got)
	}
	if strings.Index(got, wantUser) > strings.Index(got, wantAsst) {
		t.Errorf("messages should be in chronological order, got: %q", got)
	}
}

// JSON 损坏、"null"（Messages 为 nil 入库形态）、空数组均返回 ok=false。
func TestFormatFullMessagesInvalidInputs(t *testing.T) {
	for _, tc := range []struct{ name, input string }{
		{"corrupt", `{not valid json`},
		{"null", `null`},
		{"empty", `[]`},
	} {
		if got, ok := formatFullMessages(tc.input, 100000); ok {
			t.Errorf("%s: expected ok=false, got ok=true with %q", tc.name, got)
		}
	}
}

// tool_use 压成一行，tool_result 两种形态（string / 嵌套 blocks）均提取，超长截到 200。
func TestFormatFullMessagesToolBlocks(t *testing.T) {
	longResult := strings.Repeat("x", 500)
	msgs := []Message{
		{Role: "assistant", Content: json.RawMessage(`[{"type":"tool_use","id":"t1","name":"Read","input":{"file_path":"a.go"}}]`)},
		{Role: "user", Content: json.RawMessage(`[{"type":"tool_result","tool_use_id":"t1","content":"` + longResult + `"}]`)},
		{Role: "user", Content: json.RawMessage(`[{"type":"tool_result","tool_use_id":"t2","content":[{"type":"text","text":"nested result text"}]}]`)},
	}
	got, ok := formatFullMessages(mustJSON(t, msgs), 100000)
	if !ok {
		t.Fatal("expected ok=true")
	}
	if !strings.Contains(got, "[tool: Read]") {
		t.Errorf("tool_use should compress to [tool: Name], got: %q", got)
	}
	if !strings.Contains(got, "nested result text") {
		t.Errorf("nested tool_result text should be extracted, got: %q", got)
	}
	if strings.Contains(got, longResult) {
		t.Error("500-rune tool_result should be truncated to 200")
	}
	if !strings.Contains(got, strings.Repeat("x", 200)+"…") {
		t.Errorf("truncated tool_result should keep first 200 runes + ellipsis, got: %q", got)
	}
}

// rune 预算不足时从后往前装填：最新消息保留，最旧消息省略并有标注，输出仍为正序。
func TestFormatFullMessagesBudgetKeepsRecent(t *testing.T) {
	pad := strings.Repeat("p", 90)
	msgs := []Message{
		{Role: "user", Content: json.RawMessage(`"oldest ` + pad + `"`)},
		{Role: "assistant", Content: json.RawMessage(`"middle ` + pad + `"`)},
		{Role: "user", Content: json.RawMessage(`"newest ` + pad + `"`)},
	}
	// 每行约 105 runes：预算 250 装下 newest+middle，oldest 装不下
	got, ok := formatFullMessages(mustJSON(t, msgs), 250)
	if !ok {
		t.Fatal("expected ok=true")
	}
	if strings.Contains(got, "oldest") {
		t.Errorf("oldest message should be dropped under budget, got: %q", got)
	}
	if !strings.Contains(got, "middle") || !strings.Contains(got, "newest") {
		t.Errorf("recent messages should be kept, got: %q", got)
	}
	if !strings.Contains(got, "[...1 earlier messages omitted...]") {
		t.Errorf("expected omission marker, got: %q", got)
	}
	if strings.Index(got, "middle") > strings.Index(got, "newest") {
		t.Errorf("output should stay chronological, got: %q", got)
	}
}

// ---- SearchAndExpand 完整展开集成测试 ----

// seedFullExpandStore 播种一个带原始 Messages 的 archive block。
// 原始消息含独特文本 "quokka"（summary 中不出现），用于断言完整展开确实注入了原文。
func seedFullExpandStore(t *testing.T) (*SQLiteStore, []Message, *TokenCounter) {
	t.Helper()

	tc, err := NewTokenCounter()
	if err != nil {
		t.Fatalf("NewTokenCounter: %v", err)
	}
	store, err := NewSQLiteStore(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("NewSQLiteStore failed: %v", err)
	}
	t.Cleanup(func() { store.Close() })

	block := ArchiveBlock{
		ID: "block-full", SessionID: "s1",
		BlockRangeStart: 1, BlockRangeEnd: 2,
		MessageCount: 2, EstimatedTokens: 80,
		Messages: []Message{
			{Role: "user", Content: json.RawMessage(`"how does the quokka module parse flimflam input"`)},
			{Role: "assistant", Content: json.RawMessage(`"the quokka module requires quoted flimflam everywhere"`)},
		},
		SummaryText: "archive summary about flimflam parsing",
		Keywords:    []KeywordEntry{{Word: "flimflam", Source: "user_message"}},
	}
	if err := store.SaveArchive(block); err != nil {
		t.Fatalf("SaveArchive failed: %v", err)
	}

	messages := []Message{
		{Role: "user", Content: json.RawMessage(`"tell me about flimflam"`)},
	}
	return store, messages, tc
}

// injectedTextAt 取注入结果第 idx 条消息的字符串内容。
func injectedTextAt(t *testing.T, result []Message, idx int) string {
	t.Helper()
	var text string
	if err := json.Unmarshal(result[idx].Content, &text); err != nil {
		t.Fatalf("unmarshal injected content at %d: %v", idx, err)
	}
	return text
}

// 预算充足 → 注入完整原始消息，summary header 与全文分隔符同时在场。
func TestSearchAndExpandFullExpansion(t *testing.T) {
	store, messages, tc := seedFullExpandStore(t)

	budget := &Budget{ReExpansion: 100000}
	outcome := SearchAndExpand(messages, store, 100000, tc, budget)
	result := outcome.Messages
	if len(result) != len(messages)+1 {
		t.Fatalf("expected 1 injected message, got %d total (want %d)", len(result), len(messages)+1)
	}
	text := injectedTextAt(t, result, 1)
	for _, want := range []string{
		"[Retrieved archive #1",
		"archive summary about flimflam parsing",
		"--- Full messages ---",
		"[user]: how does the quokka module parse flimflam input",
		"[assistant]: the quokka module requires quoted flimflam everywhere",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("injected text missing %q, got: %q", want, text)
		}
	}
}

// 预算只够 summary → 完整展开被门控拒绝，降级注入纯 summary。
func TestSearchAndExpandFullExpansionBudgetFallsBack(t *testing.T) {
	store, messages, tc := seedFullExpandStore(t)

	// 复刻注入循环的 summary 形态成本，预算设为略高于它——完整版必装不下
	prefix := fmt.Sprintf("[Retrieved archive #%d — %d-%d, ~%d tokens]\n\n", 1, 1, 2, 80)
	summaryCost := tc.CountTokens(prefix + truncateSummaryText("archive summary about flimflam parsing", 2000))
	budget := &Budget{ReExpansion: summaryCost + 5}

	outcome := SearchAndExpand(messages, store, 100000, tc, budget)
	result := outcome.Messages
	if len(result) != len(messages)+1 {
		t.Fatalf("expected 1 injected message, got %d total (want %d)", len(result), len(messages)+1)
	}
	text := injectedTextAt(t, result, 1)
	if strings.Contains(text, "--- Full messages ---") {
		t.Errorf("full expansion should be rejected under tight budget, got: %q", text)
	}
	if !strings.Contains(text, "archive summary about flimflam parsing") {
		t.Errorf("summary fallback should still inject, got: %q", text)
	}
}

// messages_json 损坏 → 该块降级 summary，注入不中断。
func TestSearchAndExpandFullExpansionCorruptJSONFallsBack(t *testing.T) {
	store, messages, tc := seedFullExpandStore(t)

	if _, err := store.db.Exec(`UPDATE archive_blocks SET messages_json = '{corrupt' WHERE id = 'block-full'`); err != nil {
		t.Fatalf("corrupt messages_json: %v", err)
	}

	budget := &Budget{ReExpansion: 100000}
	outcome := SearchAndExpand(messages, store, 100000, tc, budget)
	result := outcome.Messages
	if len(result) != len(messages)+1 {
		t.Fatalf("expected 1 injected message, got %d total (want %d)", len(result), len(messages)+1)
	}
	text := injectedTextAt(t, result, 1)
	if strings.Contains(text, "--- Full messages ---") {
		t.Errorf("corrupt JSON should degrade to summary, got: %q", text)
	}
	if !strings.Contains(text, "archive summary about flimflam parsing") {
		t.Errorf("summary fallback should still inject, got: %q", text)
	}
}

// 同 session 两块命中 → 只有排名第一的块完整展开，第二块走 summary。
func TestSearchAndExpandFullExpansionSameSessionOnce(t *testing.T) {
	tc, err := NewTokenCounter()
	if err != nil {
		t.Fatalf("NewTokenCounter: %v", err)
	}
	store, err := NewSQLiteStore(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("NewSQLiteStore failed: %v", err)
	}
	t.Cleanup(func() { store.Close() })

	// block-first 匹配 flimflam+warbler 两词排前，block-second 只匹配 flimflam；
	// 噪声块保证目标词 IDF 为正（同 store_test.go 的排序测试构造）
	blocks := []ArchiveBlock{
		{
			ID: "block-first", SessionID: "s1",
			BlockRangeStart: 1, BlockRangeEnd: 2,
			MessageCount: 2, EstimatedTokens: 50,
			Messages: []Message{
				{Role: "user", Content: json.RawMessage(`"first block original text"`)},
			},
			SummaryText: "first summary",
			Keywords: []KeywordEntry{
				{Word: "flimflam", Source: "user_message"},
				{Word: "warbler", Source: "user_message"},
			},
		},
		{
			ID: "block-second", SessionID: "s1",
			BlockRangeStart: 3, BlockRangeEnd: 4,
			MessageCount: 2, EstimatedTokens: 50,
			Messages: []Message{
				{Role: "user", Content: json.RawMessage(`"second block original text"`)},
			},
			SummaryText: "second summary",
			Keywords:    []KeywordEntry{{Word: "flimflam", Source: "user_message"}},
		},
		{
			ID: "block-noise", SessionID: "s2",
			BlockRangeStart: 5, BlockRangeEnd: 6,
			MessageCount: 2, EstimatedTokens: 50,
			SummaryText: "noise",
			Keywords: []KeywordEntry{
				{Word: "gamma", Source: "user_message"},
				{Word: "delta", Source: "user_message"},
				{Word: "epsilon", Source: "user_message"},
			},
		},
	}
	for _, b := range blocks {
		if err := store.SaveArchive(b); err != nil {
			t.Fatalf("SaveArchive(%s) failed: %v", b.ID, err)
		}
	}

	messages := []Message{
		{Role: "user", Content: json.RawMessage(`"tell me about flimflam warbler"`)},
	}
	budget := &Budget{ReExpansion: 100000}
	outcome := SearchAndExpand(messages, store, 100000, tc, budget)
	result := outcome.Messages
	if len(result) != len(messages)+2 {
		t.Fatalf("expected 2 injected messages, got %d total (want %d)", len(result), len(messages)+2)
	}

	first := injectedTextAt(t, result, 1)
	second := injectedTextAt(t, result, 2)
	if !strings.Contains(first, "--- Full messages ---") || !strings.Contains(first, "first block original text") {
		t.Errorf("top-ranked block should fully expand, got: %q", first)
	}
	if strings.Contains(second, "--- Full messages ---") {
		t.Errorf("same-session second block should stay summary-only, got: %q", second)
	}
	if !strings.Contains(second, "second summary") {
		t.Errorf("second block should inject its summary, got: %q", second)
	}
}

func seedBudgetCandidates(t *testing.T, count int) (*SQLiteStore, []Message, *TokenCounter, string) {
	t.Helper()

	tc, err := NewTokenCounter()
	if err != nil {
		t.Fatalf("NewTokenCounter: %v", err)
	}
	store, err := NewSQLiteStore(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("NewSQLiteStore failed: %v", err)
	}
	t.Cleanup(func() { store.Close() })

	summary := "flimflam archive summary with enough detail for budget accounting"
	for i := 0; i < count; i++ {
		block := ArchiveBlock{
			ID: fmt.Sprintf("budget-candidate-%d", i), SessionID: fmt.Sprintf("session-%d", i),
			BlockRangeStart: 1, BlockRangeEnd: 2,
			MessageCount: 2, EstimatedTokens: 80,
			SummaryText: summary,
			Keywords:    []KeywordEntry{{Word: "flimflam", Source: "user_message"}},
		}
		if err := store.SaveArchive(block); err != nil {
			t.Fatalf("SaveArchive(%s): %v", block.ID, err)
		}
	}

	messages := []Message{{Role: "user", Content: json.RawMessage(`"tell me about flimflam"`)}}
	return store, messages, tc, summary
}

func TestSearchAndExpandBudgetTracksOnlyInjectedPayload(t *testing.T) {
	store, messages, tc, summary := seedBudgetCandidates(t, 2)
	prefix := fmt.Sprintf("[Retrieved archive #%d — %d-%d, ~%d tokens]\n\n", 1, 1, 2, 80)
	oneCost := tc.CountTokens(prefix + summary)
	budget := &Budget{ReExpansion: oneCost}

	outcome := SearchAndExpand(messages, store, 100000, tc, budget)
	if outcome.Candidates != 2 || outcome.Selected != 2 {
		t.Fatalf("candidates/selected = %d/%d, want 2/2", outcome.Candidates, outcome.Selected)
	}
	if outcome.Injected != 1 || outcome.Discarded != 1 {
		t.Fatalf("injected/discarded = %d/%d, want 1/1", outcome.Injected, outcome.Discarded)
	}
	if len(outcome.Messages) != len(messages)+1 {
		t.Fatalf("message count = %d, want %d", len(outcome.Messages), len(messages)+1)
	}
	injected := injectedTextAt(t, outcome.Messages, 1)
	actualCost := tc.CountTokens(injected)
	if outcome.TokenCost != actualCost {
		t.Fatalf("token cost = %d, want actual injected cost %d", outcome.TokenCost, actualCost)
	}
	if budget.RemainingReExpansion() != 0 || outcome.BudgetRemaining != 0 {
		t.Fatalf("remaining budget = %d/%d, want 0/0", budget.RemainingReExpansion(), outcome.BudgetRemaining)
	}
}

func TestSearchAndExpandBudgetNilUsesHardLimit(t *testing.T) {
	store, messages, tc, summary := seedBudgetCandidates(t, 2)
	prefix := fmt.Sprintf("[Retrieved archive #%d — %d-%d, ~%d tokens]\n\n", 1, 1, 2, 80)
	oneCost := tc.CountTokens(prefix + summary)

	outcome := SearchAndExpand(messages, store, oneCost*10, tc, nil)
	if outcome.BudgetLimit != oneCost {
		t.Fatalf("budget limit = %d, want %d", outcome.BudgetLimit, oneCost)
	}
	if outcome.Injected != 1 || outcome.Discarded != 1 {
		t.Fatalf("injected/discarded = %d/%d, want 1/1", outcome.Injected, outcome.Discarded)
	}
	if outcome.TokenCost > outcome.BudgetLimit {
		t.Fatalf("token cost %d exceeds hard limit %d", outcome.TokenCost, outcome.BudgetLimit)
	}
}

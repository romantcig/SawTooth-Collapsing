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

	result := SearchAndExpand(messages, store, 100000, tc, budget)
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
	result := SearchAndExpand(messages, store, 100000, tc, budget)
	if len(result) != len(messages) {
		t.Fatalf("expected no injection with exhausted budget, got %d messages (want %d)", len(result), len(messages))
	}
}

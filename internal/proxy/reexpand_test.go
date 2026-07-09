package proxy

import (
	"fmt"
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

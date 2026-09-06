package proxy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// ---- findCompactableRuns tests ----

func TestFindCompactableRunsExactly50(t *testing.T) {
	dt := NewDecayTracker()
	// 50 consecutive Stage 3 messages → should form 1 run
	// threadLen=200 < 500, boundaries (5,15,50) × stretch 1.0 = (5,15,50)
	// age = 200 - 1 = 199 > 50 → Stage 3
	for i := 10; i <= 59; i++ {
		dt.MarkStubbed("test", i, 1, 0.0)
	}
	runs := findCompactableRuns(dt, "test", 200, 1, 180, 200, 3.0) // pressure=3.0 → stretch=1.0
	if len(runs) != 1 {
		t.Fatalf("expected 1 run for exactly 50 stubs, got %d", len(runs))
	}
	if runs[0].start != 10 || runs[0].end != 59 {
		t.Errorf("expected run 10-59, got %d-%d", runs[0].start, runs[0].end)
	}
}

func TestFindCompactableRuns49NotEnough(t *testing.T) {
	dt := NewDecayTracker()
	// 49 messages → NOT enough
	for i := 10; i <= 58; i++ {
		dt.MarkStubbed("test", i, 1, 0.0)
	}
	runs := findCompactableRuns(dt, "test", 200, 1, 180, 200, 3.0)
	if len(runs) != 0 {
		t.Fatalf("expected 0 runs for 49 stubs, got %d", len(runs))
	}
}

func TestFindCompactableRunsMultiple(t *testing.T) {
	dt := NewDecayTracker()
	// Two runs of 50+ separated by a gap at index 70
	for i := 10; i <= 69; i++ {
		dt.MarkStubbed("test", i, 1, 0.0)
	}
	// gap at 70 (not stubbed → DecayFresh → not Stage 3)
	for i := 71; i <= 130; i++ {
		dt.MarkStubbed("test", i, 1, 0.0)
	}

	runs := findCompactableRuns(dt, "test", 200, 1, 180, 200, 3.0)
	if len(runs) != 2 {
		t.Fatalf("expected 2 runs, got %d", len(runs))
	}
	if runs[0].start != 10 || runs[0].end != 69 {
		t.Errorf("run 0: expected 10-69, got %d-%d", runs[0].start, runs[0].end)
	}
	if runs[1].start != 71 || runs[1].end != 130 {
		t.Errorf("run 1: expected 71-130, got %d-%d", runs[1].start, runs[1].end)
	}
}

func TestFindCompactableRunsTrailingRun(t *testing.T) {
	dt := NewDecayTracker()
	// Trailing run that extends to scanEnd
	for i := 30; i <= 100; i++ {
		dt.MarkStubbed("test", i, 1, 0.0)
	}
	runs := findCompactableRuns(dt, "test", 200, 20, 100, 200, 3.0)
	if len(runs) != 1 {
		t.Fatalf("expected 1 trailing run, got %d", len(runs))
	}
	if runs[0].start != 30 || runs[0].end != 100 {
		t.Errorf("expected run 30-100, got %d-%d", runs[0].start, runs[0].end)
	}
}

func TestFindCompactableRunsRespectsScanRange(t *testing.T) {
	dt := NewDecayTracker()
	// Stubs outside scan range should be ignored
	for i := 1; i <= 120; i++ {
		dt.MarkStubbed("test", i, 1, 0.0)
	}
	// Only scan 50-100
	runs := findCompactableRuns(dt, "test", 200, 50, 100, 200, 3.0)
	if len(runs) != 1 {
		t.Fatalf("expected 1 run in scan range, got %d", len(runs))
	}
	if runs[0].start != 50 || runs[0].end != 100 {
		t.Errorf("expected run 50-100, got %d-%d", runs[0].start, runs[0].end)
	}
}

func TestFindCompactableRunsNilDT(t *testing.T) {
	runs := findCompactableRuns(nil, "test", 200, 1, 100, 200, 3.0)
	if runs != nil {
		t.Errorf("expected nil runs for nil dt, got %v", runs)
	}
}

func TestFindCompactableRunsWithLongStretch(t *testing.T) {
	dt := NewDecayTracker()
	// With pressure=0 → stretch=4.0, boundaries=(20,60,200)
	// age=150: > 60 (stage 2) but < 200 (not stage 3)
	for i := 10; i <= 70; i++ {
		dt.MarkStubbed("test", i, 1, 0.0)
	}
	// requestIdx=151 → age=150, not enough for Stage 3 with stretch 4.0
	runs := findCompactableRuns(dt, "test", 200, 1, 180, 151, 0.0)
	if len(runs) != 0 {
		t.Errorf("expected 0 runs with long stretch (age 150 < boundary 200), got %d", len(runs))
	}

	// requestIdx=250 → age=249, now enough for Stage 3
	runs = findCompactableRuns(dt, "test", 200, 1, 180, 250, 0.0)
	if len(runs) != 1 {
		t.Fatalf("expected 1 run with age 249 > boundary 200, got %d", len(runs))
	}
}

// ---- extractCompactionStats tests ----

func TestExtractCompactionStatsToolsAndFiles(t *testing.T) {
	messages := buildTestMessages(
		withToolUse("Read", map[string]any{"file_path": "/app/main.go"}, "assistant"),
		withToolResult("contents...", "user"),
		withToolUse("Edit", map[string]any{"file_path": "/app/main.go"}, "assistant"),
		withToolResult("edited", "user"),
		withToolUse("Read", map[string]any{"file_path": "/app/config.go"}, "assistant"),
		withToolResult("config", "user"),
		withToolUse("Bash", map[string]any{"command": "go build"}, "assistant"),
		withToolResult("built", "user"),
	)

	stats := extractCompactionStats(messages, 0, len(messages)-1)

	// Read(2), Bash(1), Edit(1) — sorted by count desc, then key asc
	expectedTools := "Read(2), Bash(1), Edit(1)"
	if stats.toolStats != expectedTools {
		t.Errorf("tool stats mismatch\n  expected: %s\n  got:      %s", expectedTools, stats.toolStats)
	}

	// main.go(2), config.go(1) — sorted by count desc, then key asc
	expectedFiles := "main.go(2), config.go(1)"
	if stats.fileStats != expectedFiles {
		t.Errorf("file stats mismatch\n  expected: %s\n  got:      %s", expectedFiles, stats.fileStats)
	}
}

func TestExtractCompactionStatsEmptyMessages(t *testing.T) {
	stats := extractCompactionStats(nil, 0, 10)
	if stats.fileStats != "" || stats.toolStats != "" {
		t.Error("expected empty stats for nil messages")
	}
}

func TestExtractCompactionStatsTextOnly(t *testing.T) {
	messages := buildTestMessages(
		withText("hello", "user"),
		withText("world", "assistant"),
	)
	stats := extractCompactionStats(messages, 0, len(messages)-1)
	if stats.fileStats != "" || stats.toolStats != "" {
		t.Error("expected empty stats for text-only messages")
	}
}

func TestExtractCompactionStatsPathBasename(t *testing.T) {
	messages := buildTestMessages(
		withToolUse("Read", map[string]any{"file_path": "/very/deep/nested/dir/main.go"}, "assistant"),
		withToolUse("Read", map[string]any{"path": "/another/deep/path/helper.go"}, "assistant"),
	)

	stats := extractCompactionStats(messages, 0, len(messages)-1)

	// Should use basename only
	if !strings.Contains(stats.fileStats, "main.go") {
		t.Errorf("file stats should contain 'main.go' (basename), got: %s", stats.fileStats)
	}
	if !strings.Contains(stats.fileStats, "helper.go") {
		t.Errorf("file stats should contain 'helper.go' (basename), got: %s", stats.fileStats)
	}
}

// ---- buildCompactedContent tests ----

func TestBuildCompactedContentBasic(t *testing.T) {
	stats := compactionStats{
		fileStats: "main.go(2), config.go(1)",
		toolStats: "Read(2), Edit(1)",
	}
	content := buildCompactedContent(10, 59, stats)
	if content == "" {
		t.Fatal("expected non-empty content")
	}
	// Should contain range header
	if !strings.Contains(content, "[Compacted: Messages 10-59 (50 msgs)]") {
		t.Errorf("expected header, got: %s", content)
	}
	if !strings.Contains(content, "Files: main.go(2), config.go(1)") {
		t.Errorf("expected file line, got: %s", content)
	}
	if !strings.Contains(content, "Tools: Read(2), Edit(1)") {
		t.Errorf("expected tool line, got: %s", content)
	}
}

func TestBuildCompactedContentEmptyStats(t *testing.T) {
	stats := compactionStats{}
	content := buildCompactedContent(0, 99, stats)
	// Should only have header, no Files: or Tools: lines
	expected := "[Compacted: Messages 0-99 (100 msgs)]"
	if content != expected {
		t.Errorf("expected %q, got %q", expected, content)
	}
}

// ---- formatCompactionCounts tests ----

func TestFormatCompactionCountsBasic(t *testing.T) {
	counts := map[string]int{
		"Read": 3,
		"Edit": 1,
		"Bash": 2,
	}
	result := formatCompactionCounts(counts)
	// Should be sorted by count desc: Read(3), Bash(2), Edit(1)
	if !strings.HasPrefix(result, "Read(3)") {
		t.Errorf("expected Read(3) first, got: %s", result)
	}
}

func TestFormatCompactionCountsTop5(t *testing.T) {
	counts := map[string]int{
		"a": 1, "b": 2, "c": 3, "d": 4, "e": 5, "f": 6, "g": 7,
	}
	result := formatCompactionCounts(counts)
	// Should contain top 5 + "+2 more"
	if !strings.Contains(result, "+2 more") {
		t.Errorf("expected '+2 more' overflow, got: %s", result)
	}
	// "g" is not in top 5 by count (7 entries, top 5: g,f,e,d,c)
	parts := strings.Split(result, ", ")
	if len(parts) > 6 { // 5 entries + "+N more" = max 6 parts
		t.Errorf("expected ≤6 parts, got %d: %s", len(parts), result)
	}
}

func TestFormatCompactionCountsDeterministic(t *testing.T) {
	counts := map[string]int{
		"Read":  5,
		"Write": 5,
		"Edit":  3,
	}
	// Same count → sorted by key: "Read" < "Write"
	result := formatCompactionCounts(counts)
	// Read should come before Write (same count, alphabetical)
	readIdx := strings.Index(result, "Read")
	writeIdx := strings.Index(result, "Write")
	if readIdx < 0 || writeIdx < 0 {
		t.Fatalf("expected 'Read' and 'Write' in output: %s", result)
	}
	if readIdx > writeIdx {
		t.Errorf("expected Read before Write (same count), got: %s", result)
	}
}

func TestFormatCompactionCountsEmpty(t *testing.T) {
	result := formatCompactionCounts(nil)
	if result != "" {
		t.Errorf("expected empty string for nil map, got: %s", result)
	}
	result = formatCompactionCounts(map[string]int{})
	if result != "" {
		t.Errorf("expected empty string for empty map, got: %s", result)
	}
}

// ---- compactedRolesForNeighbors tests ----

// TestCompactedRolesForNeighborsMatrix 遍历 {user, assistant, system, "", tool}
// 的全部 25 组有序对，断言角色决策只产出合法的 user/assistant 序列，
// 且首/末元素不与对应的 user/assistant 邻居冲突。
// 非 user/assistant 的邻居（真实录制里 system 占 14.7%）按无交替约束处理。
func TestCompactedRolesForNeighborsMatrix(t *testing.T) {
	candidates := []string{"user", "assistant", "system", "", "tool"}

	// 五组显式期望值，覆盖判定表的每一个分支。
	explicit := map[[2]string][]string{
		{"user", "user"}:        {"assistant"},         // lc == rc
		{"user", "assistant"}:   {"assistant", "user"}, // lc != rc
		{"system", "assistant"}: {"user"},              // 左侧无约束
		{"user", "system"}:      {"assistant"},         // 右侧无约束
		{"system", "system"}:    {"user"},              // 双侧无约束
	}

	// 与生产实现同构的规范化，用于独立复算约束（不调用被测函数的内部逻辑）。
	normalize := func(r string) string {
		if r == "user" || r == "assistant" {
			return r
		}
		return ""
	}

	pairs := 0
	for _, left := range candidates {
		for _, right := range candidates {
			pairs++
			got := compactedRolesForNeighbors(left, right)

			if len(got) < 1 || len(got) > 2 {
				t.Errorf("(%q,%q): 返回长度 = %d, want 1 或 2 (got=%v)", left, right, len(got), got)
				continue
			}
			for i, r := range got {
				if r != "user" && r != "assistant" {
					t.Errorf("(%q,%q): got[%d] = %q, 只允许 user/assistant", left, right, i, r)
				}
			}
			for i := 1; i < len(got); i++ {
				if got[i] == got[i-1] {
					t.Errorf("(%q,%q): 序列内部相邻同角色 %v", left, right, got)
				}
			}
			if lc := normalize(left); lc != "" && got[0] == lc {
				t.Errorf("(%q,%q): 首元素 %q 与左邻居冲突 (got=%v)", left, right, got[0], got)
			}
			if rc := normalize(right); rc != "" && got[len(got)-1] == rc {
				t.Errorf("(%q,%q): 末元素 %q 与右邻居冲突 (got=%v)", left, right, got[len(got)-1], got)
			}

			want, ok := explicit[[2]string{left, right}]
			if !ok {
				continue
			}
			if len(got) != len(want) {
				t.Errorf("(%q,%q): got %v, want %v", left, right, got, want)
				continue
			}
			for i := range want {
				if got[i] != want[i] {
					t.Errorf("(%q,%q): got %v, want %v", left, right, got, want)
					break
				}
			}
		}
	}
	if pairs != 25 {
		t.Errorf("覆盖的有序对 = %d, want 25", pairs)
	}
}

// ---- CompactMessages tests ----

// TestCompactMessagesNeighborsEqualProducesSingleBlock 验证：run 左右邻居的实际角色
// 相同（这里同为 user）时，中间只能放一条 opposite(user)=assistant 的摘要，
// 覆盖整个 run 区间——放两条必然让其中一条与某侧邻居撞角色。
// 106 条消息、标记 [0,99] 为已桩化 → run [1,99]，roles[0] 与 roles[100] 都是 user。
func TestCompactMessagesNeighborsEqualProducesSingleBlock(t *testing.T) {
	dt := NewDecayTracker()
	n := 106
	for i := 0; i <= 99; i++ {
		dt.MarkStubbed("test", i, 1, 0.0)
	}

	original := make([]Message, n)
	decayed := make([]Message, n)
	for i := 0; i < n; i++ {
		role := "assistant"
		if i%2 == 0 {
			role = "user"
		}
		original[i] = Message{Role: role, Content: json.RawMessage(fmt.Sprintf(`"msg %d"`, i))}
		decayed[i] = Message{Role: role, Content: json.RawMessage(fmt.Sprintf(`"msg %d"`, i))}
	}

	runs := findCompactableRuns(dt, "test", n, 1, n-5, 200, 3.0)
	if len(runs) != 1 {
		t.Fatalf("期望 1 个 run, got %d", len(runs))
	}
	run := runs[0]
	if decayed[run.start-1].Role != decayed[run.end+1].Role {
		t.Fatalf("前提不成立：左邻居 %q ≠ 右邻居 %q",
			decayed[run.start-1].Role, decayed[run.end+1].Role)
	}

	result, blocks := CompactMessages(decayed, original, dt, "test", 200, 3.0)

	if len(blocks) != 1 {
		t.Fatalf("邻居相同 → 期望 1 条 block, got %d", len(blocks))
	}
	if blocks[0].StartIdx != run.start || blocks[0].EndIdx != run.end {
		t.Errorf("block 区间 = [%d,%d], want 整个 run [%d,%d]",
			blocks[0].StartIdx, blocks[0].EndIdx, run.start, run.end)
	}
	if blocks[0].Role != "assistant" {
		t.Errorf("block Role = %q, want assistant（= opposite(user)）", blocks[0].Role)
	}
	for i := 1; i < len(result); i++ {
		if result[i].Role == result[i-1].Role {
			t.Errorf("相邻同角色 at %d: %s → %s", i, result[i-1].Role, result[i].Role)
		}
	}
}

func TestCompactMessagesNoRuns(t *testing.T) {
	dt := NewDecayTracker()
	// Only 10 messages, all Fresh (no stubs)
	original, decayed := buildTestThread(10, nil)

	result, blocks := CompactMessages(decayed, original, dt, "test", 5, 3.0)

	if len(result) != len(decayed) {
		t.Errorf("expected same length, got %d (was %d)", len(result), len(decayed))
	}
	if len(blocks) != 0 {
		t.Errorf("expected 0 blocks, got %d", len(blocks))
	}
}

func TestCompactMessagesNilDT(t *testing.T) {
	msgs := make([]Message, 100)
	for i := range msgs {
		msgs[i] = Message{Role: "user", Content: json.RawMessage(`"msg"`)}
	}
	result, blocks := CompactMessages(msgs, msgs, nil, "test", 0, 0)
	if len(result) != len(msgs) {
		t.Errorf("expected unchanged for nil dt, got %d", len(result))
	}
	if blocks != nil {
		t.Errorf("expected nil blocks for nil dt, got %d", len(blocks))
	}
}

func TestCompactMessagesTooFewMessages(t *testing.T) {
	dt := NewDecayTracker()
	msgs := make([]Message, 30)
	for i := range msgs {
		msgs[i] = Message{Role: "user", Content: json.RawMessage(`"msg"`)}
	}
	result, blocks := CompactMessages(msgs, msgs, dt, "test", 100, 3.0)
	// < 50 messages → early return
	if len(result) != len(msgs) {
		t.Errorf("expected unchanged for <50 total msgs, got %d", len(result))
	}
	if len(blocks) != 0 {
		t.Errorf("expected 0 blocks, got %d", len(blocks))
	}
}

func TestCompactMessagesProtectsFirstMessage(t *testing.T) {
	dt := NewDecayTracker()
	original, decayed := buildTestThread(110, dt) // enough for compaction

	result, blocks := CompactMessages(decayed, original, dt, "test", 200, 3.0)

	if len(result) == 0 {
		t.Fatal("result is empty")
	}
	// messages[0] should be preserved (same as original[0])
	content := string(result[0].Content)
	if strings.Contains(content, "[Compacted:") {
		t.Error("messages[0] should not be a compacted block")
	}
	_ = blocks
}

func TestCompactMessagesProtectsRecentTail(t *testing.T) {
	dt := NewDecayTracker()
	// 120 messages: run [1, 115] compacted, tail [116-119] protected
	n := 120
	original := make([]Message, n)
	decayed := make([]Message, n)
	for i := 0; i < n; i++ {
		role := "assistant"
		if i%2 == 0 {
			role = "user"
		}
		original[i] = Message{Role: role, Content: json.RawMessage(fmt.Sprintf(`"msg %d"`, i))}
		decayed[i] = Message{Role: role, Content: json.RawMessage(fmt.Sprintf(`"msg %d"`, i))}
		dt.MarkStubbed("test", i, 1, 0.0)
	}

	result, blocks := CompactMessages(decayed, original, dt, "test", 200, 3.0)

	// Verify fewer messages
	if len(result) >= len(decayed) {
		t.Errorf("expected fewer messages after compaction: %d → %d", len(decayed), len(result))
	}
	// Verify role alternation
	for i := 1; i < len(result); i++ {
		if result[i].Role == result[i-1].Role {
			t.Errorf("role violation at index %d: %s → %s", i, result[i-1].Role, result[i])
		}
	}
	// Verify tail messages (116-119) are preserved with correct content
	tailStart := n - 4 // index 116
	expectedTail := decayed[tailStart:]
	for i, expected := range expectedTail {
		ri := len(result) - len(expectedTail) + i
		if ri < 0 || ri >= len(result) {
			t.Errorf("tail msg[%d] missing from result", tailStart+i)
			continue
		}
		actual := result[ri]
		if actual.Role != expected.Role {
			t.Errorf("tail msg[%d] role mismatch: expected %s, got %s", tailStart+i, expected.Role, actual.Role)
		}
		if string(actual.Content) != string(expected.Content) {
			t.Errorf("tail msg[%d] content mismatch: expected %s, got %s", tailStart+i, string(expected.Content), string(actual.Content))
		}
	}
	_ = blocks
}

func TestCompactMessagesContentContainsStats(t *testing.T) {
	dt := NewDecayTracker()
	original, decayed := buildTestThreadWithTools(110, dt)

	result, blocks := CompactMessages(decayed, original, dt, "test", 200, 3.0)

	if len(blocks) == 0 {
		t.Fatal("expected compacted blocks")
	}

	// At least one compacted message should contain Files: and Tools:
	foundFiles := false
	foundTools := false
	for _, msg := range result {
		content := string(msg.Content)
		if strings.Contains(content, "Files:") {
			foundFiles = true
		}
		if strings.Contains(content, "Tools:") {
			foundTools = true
		}
	}
	if !foundFiles {
		t.Error("expected 'Files:' in at least one compacted message")
	}
	if !foundTools {
		t.Error("expected 'Tools:' in at least one compacted message")
	}
}

func TestCompactMessagesAdjacentRuns(t *testing.T) {
	dt := NewDecayTracker()
	// Two runs separated by a single Fresh message.
	// Run1: [10, 108] = 99 msgs, user=50(asst=49) → single role
	// Gap:  msg[109] = Fresh (not stubbed)
	// Run2: [110, 210] = 101 msgs, both roles ≥50 → 2 blocks
	// Protected tail: messages[211-219]
	n := 220
	original := make([]Message, n)
	decayed := make([]Message, n)

	for i := 0; i < n; i++ {
		role := "assistant"
		if i%2 == 0 {
			role = "user"
		}
		original[i] = Message{Role: role, Content: json.RawMessage(fmt.Sprintf(`"msg %d"`, i))}
		decayed[i] = Message{Role: role, Content: json.RawMessage(fmt.Sprintf(`"msg %d"`, i))}
		// Mark as stubbed except the gap
		if i != 109 {
			dt.MarkStubbed("test", i, 1, 0.0)
		}
	}

	result, blocks := CompactMessages(decayed, original, dt, "test", 200, 3.0)

	// Should have 2 runs → at least 2 blocks (Run1=1, Run2=2)
	if len(blocks) < 2 {
		t.Fatalf("expected ≥2 blocks for 2 runs, got %d", len(blocks))
	}
	// Verify role alternation across entire result
	for i := 1; i < len(result); i++ {
		if result[i].Role == result[i-1].Role {
			t.Errorf("role violation at index %d: %s → %s", i, result[i-1].Role, result[i])
		}
	}
	// The gap message (msg[109]) should still exist in the result
	foundGap := false
	gapContent := fmt.Sprintf(`"msg %d"`, 109)
	for _, msg := range result {
		if string(msg.Content) == gapContent {
			foundGap = true
			break
		}
	}
	if !foundGap {
		t.Error("gap message (msg[109]) missing from result")
	}
	// Verify fewer messages
	if len(result) >= len(decayed) {
		t.Errorf("expected fewer messages after compaction: %d → %d", len(decayed), len(result))
	}
}

// ---- CompactMessages 真实 role 序列回归 ----
//
// 以下测试全部由 testdata/compaction/real-role-sequence.json 驱动：
// 177 条 role（user 78 / assistant 77 / system 22，0 处相邻重复），
// 取自真实录制的 body.messages[*].role。合成的严格交替数据把
// 「run 长度奇偶 ⇒ 邻居关系」这个错误前提固化了，正是 W1-1/W1-2
// 长期测不出来的原因，故角色相关的回归一律改用这份真实序列。

func TestCompactMessagesShortRunWithSystemProducesSummary(t *testing.T) {
	dt := NewDecayTracker()
	roles := loadRealRoleSequence(t)
	// run = [1,60]，L=60，含 10 条 system，右邻居 roles[61]="assistant"。
	// 旧逻辑 user 25 / assistant 25 两侧计数都 < 50 → 落 default 分支完全不合并，
	// 而 decay.go 的 `case DecayCompacted: return ""` 已把这 60 条内容清空。
	original, decayed := buildThreadFromRoles(roles, dt, 60)

	result, blocks := CompactMessages(decayed, original, dt, "test", 200, 3.0)

	if len(blocks) < 1 {
		t.Fatalf("L=60 的 run 必须产出摘要 block, got %d (run[1,60] 内 user=%d assistant=%d)",
			len(blocks),
			countRoleInRange(decayed, 1, 60, "user"),
			countRoleInRange(decayed, 1, 60, "assistant"))
	}
	if len(result) >= len(decayed) {
		t.Errorf("合并后消息数 = %d, 应少于输入的 %d", len(result), len(decayed))
	}
	// run 内的原始消息必须已被摘要替换掉
	const replaced = `"msg 30"`
	for i, msg := range result {
		if string(msg.Content) == replaced {
			t.Errorf("result[%d] 仍是 run 内的原始消息 %s——未被合并", i, replaced)
		}
	}
}

func TestCompactMessagesRealRoleSequenceNoAdjacentSameRole(t *testing.T) {
	// 四个切点由实算挑出，是旧逻辑的失效现场：
	//   118 → 旧代码插入 role="system" 的合并消息
	//   119 → 旧代码插入的 assistant 与右邻居 roles[120]="assistant" 相邻
	//   120 → 旧代码插入 ["user","user"]，双违规
	//   172 → 最长 run（撞上 scanEnd）
	// 输入序列本身 0 处相邻重复，任何违规都只可能由合并引入。
	for _, stubbedThrough := range []int{118, 119, 120, 172} {
		t.Run(fmt.Sprintf("stubbedThrough=%d", stubbedThrough), func(t *testing.T) {
			dt := NewDecayTracker()
			roles := loadRealRoleSequence(t)
			original, decayed := buildThreadFromRoles(roles, dt, stubbedThrough)

			result, blocks := CompactMessages(decayed, original, dt, "test", 200, 3.0)

			if len(blocks) == 0 {
				t.Fatalf("期望至少 1 个 block, got 0")
			}
			for i := 1; i < len(result); i++ {
				if result[i].Role == result[i-1].Role {
					t.Errorf("相邻同角色 at %d: %s → %s", i, result[i-1].Role, result[i].Role)
				}
			}
		})
	}
}

func TestCompactMessagesRealRoleSequenceNeverInsertsSystemRole(t *testing.T) {
	for _, stubbedThrough := range []int{118, 119, 120, 172} {
		t.Run(fmt.Sprintf("stubbedThrough=%d", stubbedThrough), func(t *testing.T) {
			dt := NewDecayTracker()
			roles := loadRealRoleSequence(t)
			original, decayed := buildThreadFromRoles(roles, dt, stubbedThrough)

			result, blocks := CompactMessages(decayed, original, dt, "test", 200, 3.0)

			if len(blocks) == 0 {
				t.Fatalf("期望至少 1 个 block, got 0")
			}
			compactedSeen := 0
			for i, msg := range result {
				if !strings.HasPrefix(extractTextFromContent(msg.Content), "[Compacted:") {
					continue
				}
				compactedSeen++
				if msg.Role != "user" && msg.Role != "assistant" {
					t.Errorf("合并消息 result[%d] 的 Role = %q, 只允许 user/assistant", i, msg.Role)
				}
			}
			if compactedSeen != len(blocks) {
				t.Errorf("结果里的合并消息数 = %d, blocks = %d", compactedSeen, len(blocks))
			}
			for i, b := range blocks {
				if b.Role != "user" && b.Role != "assistant" {
					t.Errorf("blocks[%d].Role = %q, 只允许 user/assistant", i, b.Role)
				}
			}
		})
	}
}

func TestCompactMessagesEveryRunIndexCoveredByBlock(t *testing.T) {
	dt := NewDecayTracker()
	roles := loadRealRoleSequence(t)
	original, decayed := buildThreadFromRoles(roles, dt, 172)

	// 与 CompactMessages 内部一致的扫描参数（compaction.go 的 scanStart/scanEnd）
	scanStart := 1
	scanEnd := len(decayed) - 5
	runs := findCompactableRuns(dt, "test", len(decayed), scanStart, scanEnd, 200, 3.0)
	if len(runs) == 0 {
		t.Fatal("期望至少 1 个 run")
	}

	_, blocks := CompactMessages(decayed, original, dt, "test", 200, 3.0)

	for _, run := range runs {
		for idx := run.start; idx <= run.end; idx++ {
			covered := false
			for _, b := range blocks {
				if idx >= b.StartIdx && idx <= b.EndIdx {
					covered = true
					break
				}
			}
			if !covered {
				t.Fatalf("run [%d,%d] 的索引 %d 未被任何 CompactedBlock 覆盖："+
					"内容已被 decay 清空而摘要从未建立（或 fail-safe 被静默触发）",
					run.start, run.end, idx)
			}
		}
	}
}

// TestCompactMessagesHalvesAreDisjointAndDistinct 用真实 role 序列钉住 W1-3：
// stubbedThrough=119 → run [1,119]，左邻居 roles[0]="user"、右邻居
// roles[120]="assistant"，必然走两条 block 分支。旧代码两条 buildCompactedMessage
// 都传整个 (run.start, run.end)，两条摘要文本完全相同。
func TestCompactMessagesHalvesAreDisjointAndDistinct(t *testing.T) {
	dt := NewDecayTracker()
	roles := loadRealRoleSequence(t)
	original, decayed := buildThreadFromRoles(roles, dt, 119)

	runs := findCompactableRuns(dt, "test", len(decayed), 1, len(decayed)-5, 200, 3.0)
	if len(runs) != 1 {
		t.Fatalf("期望 1 个 run, got %d", len(runs))
	}
	run := runs[0]

	_, blocks := CompactMessages(decayed, original, dt, "test", 200, 3.0)
	if len(blocks) != 2 {
		t.Fatalf("期望 2 条 block, got %d", len(blocks))
	}

	// 坐标：不相交、无空洞、并集恰好等于 run
	if blocks[0].StartIdx != run.start {
		t.Errorf("blocks[0].StartIdx = %d, want %d", blocks[0].StartIdx, run.start)
	}
	if blocks[1].EndIdx != run.end {
		t.Errorf("blocks[1].EndIdx = %d, want %d", blocks[1].EndIdx, run.end)
	}
	if blocks[1].StartIdx != blocks[0].EndIdx+1 {
		t.Errorf("前后半区间不衔接: [%d,%d] 与 [%d,%d]",
			blocks[0].StartIdx, blocks[0].EndIdx, blocks[1].StartIdx, blocks[1].EndIdx)
	}

	// 文本：两条 [Compacted: Messages X-Y 标题必须不同
	title := func(content string) string {
		if i := strings.IndexByte(content, '\n'); i >= 0 {
			return content[:i]
		}
		return content
	}
	firstTitle, secondTitle := title(blocks[0].Content), title(blocks[1].Content)
	if firstTitle == secondTitle {
		t.Errorf("两条摘要标题相同（W1-3 回归）: %q", firstTitle)
	}
	wantFirst := fmt.Sprintf("[Compacted: Messages %d-%d ", blocks[0].StartIdx, blocks[0].EndIdx)
	if !strings.HasPrefix(firstTitle, wantFirst) {
		t.Errorf("blocks[0] 标题 = %q, want 前缀 %q", firstTitle, wantFirst)
	}
	wantSecond := fmt.Sprintf("[Compacted: Messages %d-%d ", blocks[1].StartIdx, blocks[1].EndIdx)
	if !strings.HasPrefix(secondTitle, wantSecond) {
		t.Errorf("blocks[1] 标题 = %q, want 前缀 %q", secondTitle, wantSecond)
	}

	// 角色：两条摘要 Role 互异
	if blocks[0].Role == blocks[1].Role {
		t.Errorf("两条摘要角色相同: %q", blocks[0].Role)
	}
}

// ---- countRoleInRange tests ----

func TestCountRoleInRange(t *testing.T) {
	msgs := []Message{
		{Role: "user"},
		{Role: "assistant"},
		{Role: "user"},
		{Role: "assistant"},
		{Role: "user"},
	}
	if n := countRoleInRange(msgs, 0, 4, "user"); n != 3 {
		t.Errorf("expected 3 user msgs in [0,4], got %d", n)
	}
	if n := countRoleInRange(msgs, 0, 4, "assistant"); n != 2 {
		t.Errorf("expected 2 assistant msgs in [0,4], got %d", n)
	}
	if n := countRoleInRange(msgs, 1, 3, "user"); n != 1 {
		t.Errorf("expected 1 user msg in [1,3], got %d", n)
	}
}

func TestCountRoleInRangeOutOfBounds(t *testing.T) {
	msgs := []Message{{Role: "user"}, {Role: "assistant"}}
	// start beyond length
	if n := countRoleInRange(msgs, 5, 10, "user"); n != 0 {
		t.Errorf("expected 0 for out-of-bounds, got %d", n)
	}
}

// ---- extractTextFromContent tests ----

func TestExtractTextFromContentString(t *testing.T) {
	raw := json.RawMessage(`"hello world"`)
	text := extractTextFromContent(raw)
	if text != "hello world" {
		t.Errorf("expected 'hello world', got %q", text)
	}
}

func TestExtractTextFromContentArray(t *testing.T) {
	raw := json.RawMessage(`[{"type":"text","text":"from array"}]`)
	text := extractTextFromContent(raw)
	if text != "from array" {
		t.Errorf("expected 'from array', got %q", text)
	}
}

func TestExtractTextFromContentInvalid(t *testing.T) {
	raw := json.RawMessage(`not json`)
	text := extractTextFromContent(raw)
	if text != "" {
		t.Errorf("expected empty for invalid JSON, got %q", text)
	}
}

// ---- helpers for building test data ----

// loadRealRoleSequence 加载从真实录制提取的 177 条 role 序列。
// fixture 只含角色字符串，不含消息正文、session id 或时间戳。
// 不在测试运行时读原始录制目录（data/ 在 .gitignore 里，干净检出下不存在）。
func loadRealRoleSequence(t *testing.T) []string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", "compaction", "real-role-sequence.json"))
	if err != nil {
		t.Fatalf("读取 real-role-sequence.json 失败: %v", err)
	}
	var roles []string
	if err := json.Unmarshal(data, &roles); err != nil {
		t.Fatalf("解析 real-role-sequence.json 失败: %v", err)
	}
	return roles
}

// buildThreadFromRoles 按给定 role 序列构造 original/decayed，
// 并把 [0, stubbedThrough] 标记为已桩化，使 Stage-3 run 恰好终止于 stubbedThrough。
func buildThreadFromRoles(roles []string, dt *DecayTracker, stubbedThrough int) (original, decayed []Message) {
	for i, role := range roles {
		msg := Message{Role: role, Content: json.RawMessage(fmt.Sprintf(`"msg %d"`, i))}
		original = append(original, msg)
		decayed = append(decayed, msg)
		if dt != nil && i <= stubbedThrough {
			dt.MarkStubbed("test", i, 1, 0.0)
		}
	}
	return
}

// buildTestThread creates a thread of N messages with alternating roles.
// All messages are marked as stubbed at request 1.
// Returns (original, decayed) — identical for simple tests.
func buildTestThread(n int, dt *DecayTracker) (original, decayed []Message) {
	for i := 0; i < n; i++ {
		role := "assistant"
		if i%2 == 0 {
			role = "user"
		}
		msg := Message{
			Role:    role,
			Content: json.RawMessage(fmt.Sprintf(`"msg %d"`, i)),
		}
		original = append(original, msg)
		decayed = append(decayed, msg)
		if dt != nil {
			dt.MarkStubbed("test", i, 1, 0.0)
		}
	}
	return
}

// buildTestThreadWithTools creates a thread with tool_use/tool_result pairs.
func buildTestThreadWithTools(n int, dt *DecayTracker) (original, decayed []Message) {
	fileNames := []string{"/app/main.go", "/app/config.go", "/app/handler.go", "/app/store.go", "/app/proxy.go"}
	for i := 0; i < n; i++ {
		if i%2 == 0 {
			// user message with text
			content := fmt.Sprintf(`"user msg %d"`, i)
			msg := Message{Role: "user", Content: json.RawMessage(content)}
			original = append(original, msg)
			decayed = append(decayed, msg)
		} else {
			// assistant message with tool_use + text
			fn := fileNames[i%len(fileNames)]
			blockJSON := fmt.Sprintf(`[{"type":"text","text":"thinking..."},{"type":"tool_use","name":"Read","input":{"file_path":"%s"}}]`, fn)
			originalMsg := Message{Role: "assistant", Content: json.RawMessage(blockJSON)}
			original = append(original, originalMsg)
			// decayed version: just the stubbed text
			stubText := fmt.Sprintf(`"[→] Read %s"`, fn)
			decayed = append(decayed, Message{Role: "assistant", Content: json.RawMessage(stubText)})
		}
		if dt != nil {
			dt.MarkStubbed("test", i, 1, 0.0)
		}
	}
	return
}

// buildTestMessages creates []Message from variadic message specs.
// Each spec is a function that returns a Message.
func buildTestMessages(specs ...Message) []Message {
	msgs := make([]Message, len(specs))
	copy(msgs, specs)
	return msgs
}

// Helper functions to create test messages
func withToolUse(name string, input map[string]any, role string) Message {
	blocks := []ContentBlock{
		{Type: "tool_use", Name: name, Input: input},
	}
	data, _ := json.Marshal(blocks)
	return Message{Role: role, Content: data}
}

func withToolResult(content string, role string) Message {
	blocks := []ContentBlock{
		{Type: "tool_result", Content: content},
	}
	data, _ := json.Marshal(blocks)
	return Message{Role: role, Content: data}
}

func withText(text string, role string) Message {
	data, _ := json.Marshal(text)
	return Message{Role: role, Content: data}
}

// 这些测试刻意覆盖 request-local plan 接缝，而不是旧 tracker 兼容包装层；
// 它们共同封闭 CompactionPlan/DecayEvaluationSnapshot 的信息安全边界。

func TestCompactionPlanRunLengths(t *testing.T) {
	for _, tc := range []struct {
		name          string
		runLength     int
		wantReplaces  int
		wantBlocks    int
		wantStage2Run bool
	}{
		{name: "one", runLength: 1, wantReplaces: 0, wantBlocks: 0, wantStage2Run: true},
		{name: "forty-nine", runLength: 49, wantReplaces: 0, wantBlocks: 0, wantStage2Run: true},
		{name: "fifty", runLength: 50, wantReplaces: 2, wantBlocks: 2, wantStage2Run: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			messages := planFixtureMessages(tc.runLength + 2)
			tracker := NewDecayTracker()
			for i := 1; i <= tc.runLength; i++ {
				tracker.MarkStubbed("plan", i, 1, 0)
			}
			snapshot := tracker.BuildDecayEvaluationSnapshot("plan", []string(nil), 200, len(messages), 3)
			plan := BuildCompactionPlan(snapshot, messages, messages, 0, true)
			if got := plan.ReplacementCount(); got != tc.wantReplaces {
				t.Fatalf("replacement count=%d, want %d", got, tc.wantReplaces)
			}
			decayed, _ := tracker.ApplyDecayBatch(messages, "plan", 300, 100, nil, "", 200, plan)
			result, blocks := CompactMessagesWithPlan(decayed, messages, plan)
			if len(blocks) != tc.wantBlocks {
				t.Fatalf("blocks=%d, want %d", len(blocks), tc.wantBlocks)
			}
			if tc.wantStage2Run {
				for i := 1; i <= tc.runLength; i++ {
					if got := extractTextFromContent(result[i].Content); got == "" {
						t.Fatalf("index %d lost Stage-2 fallback content", i)
					}
				}
			}
		})
	}
}

func TestCompactionPlanProtectedUnion(t *testing.T) {
	messages := planFixtureMessages(70)
	tracker := NewDecayTracker()
	for i := 1; i < len(messages); i++ {
		intensity := 0.0
		if i == 20 {
			intensity = 0.95
		}
		tracker.MarkStubbed("protected", i, 1, intensity)
	}
	tracker.SetFilePath("protected", 10, "src/pinned.go")
	pinned := NewPinnedPathSnapshot([]string{"pinned.go"})
	snapshot := tracker.BuildDecayEvaluationSnapshot("protected", pinned, 200, len(messages), 3)
	plan := BuildCompactionPlan(snapshot, messages, messages, 8, true)
	for _, idx := range []int{0, 10, 20, len(messages) - 1, len(messages) - 8} {
		if !plan.IsProtected(idx) {
			t.Fatalf("index %d not in protected union", idx)
		}
	}
}

func TestDecayCompactionIntegrationCoverage(t *testing.T) {
	t.Run("六十条候选被 pinned 与 intensity 切成短 run", func(t *testing.T) {
		messages := planFixtureMessages(64)
		tracker := NewDecayTracker()
		for i := 1; i <= 60; i++ {
			intensity := 0.0
			if i == 40 {
				intensity = 0.95
			}
			tracker.MarkStubbed("split", i, 0, intensity)
		}
		tracker.SetFilePath("split", 20, "C:/repo/pinned.go")
		snapshot := tracker.BuildDecayEvaluationSnapshot("split", NewPinnedPathSnapshot([]string{"pinned.go"}), 1_000, len(messages), 3)
		plan := BuildCompactionPlan(snapshot, messages, messages, 3, true)
		if !plan.IsProtected(20) || !plan.IsProtected(40) {
			t.Fatalf("保护并集缺少 pinned/intensity 边界: protected=%v", plan.Protected)
		}
		if plan.ReplacementCount() != 0 || len(plan.Runs) != 0 {
			t.Fatalf("被保护项切开的短 run 不应进入 replacement plan: runs=%v replacements=%v", plan.Runs, plan.Replacements)
		}
		decayed, _ := tracker.ApplyDecayBatch(messages, "split", 300, 100, nil, "", 1_000, plan)
		result, blocks := CompactMessagesWithPlan(decayed, messages, plan)
		if len(blocks) != 0 || len(result) != len(messages) {
			t.Fatalf("短 run 意外物化: result=%d blocks=%d", len(result), len(blocks))
		}
		for i := 1; i <= 60; i++ {
			if extractTextFromContent(result[i].Content) == "" {
				t.Fatalf("plan 外 Stage-3 候选在索引 %d 未降级到 Stage-2", i)
			}
		}
	})

	t.Run("两个五十条 run 与 system 邻居", func(t *testing.T) {
		messages := planFixtureMessages(103)
		messages[0].Role = "system"
		messages[25].Role = "system"
		messages[51].Role = "system"
		messages[77].Role = "system"
		messages[102].Role = "system"
		tracker := NewDecayTracker()
		for i := 1; i <= 101; i++ {
			intensity := 0.0
			if i == 51 {
				intensity = 0.95
			}
			tracker.MarkStubbed("multi", i, 0, intensity)
		}
		snapshot := tracker.BuildDecayEvaluationSnapshot("multi", nil, 1_000, len(messages), 3)
		plan := BuildCompactionPlan(snapshot, messages, messages, 0, true)
		if len(plan.Runs) != 2 || plan.ReplacementCount() != 2 {
			t.Fatalf("多 run plan=%v replacements=%v，want 2/2", plan.Runs, plan.Replacements)
		}
		decayed, _ := tracker.ApplyDecayBatch(messages, "multi", 300, 100, nil, "", 1_000, plan)
		result, blocks := CompactMessagesWithPlan(decayed, messages, plan)
		if len(blocks) != 2 || len(result) >= len(messages) {
			t.Fatalf("多 run 物化失败: result=%d blocks=%v", len(result), blocks)
		}
		for _, block := range blocks {
			if block.Role != "user" && block.Role != "assistant" {
				t.Fatalf("system role 泄漏到 CompactedBlock: %+v", block)
			}
			for i := block.StartIdx; i <= block.EndIdx; i++ {
				if !plan.IsCovered(i) {
					t.Fatalf("CompactedBlock 覆盖了 plan 外坐标 %d: %+v", i, block)
				}
			}
		}
	})

	t.Run("左右角色冲突生成两条 replacement", func(t *testing.T) {
		messages := planFixtureMessages(52)
		messages[0].Role = "user"
		messages[51].Role = "assistant"
		tracker := NewDecayTracker()
		for i := 1; i <= 50; i++ {
			tracker.MarkStubbed("roles", i, 0, 0)
		}
		snapshot := tracker.BuildDecayEvaluationSnapshot("roles", nil, 1_000, len(messages), 3)
		plan := BuildCompactionPlan(snapshot, messages, messages, 0, true)
		if plan.ReplacementCount() != 2 || plan.Replacements[0].Role != "assistant" || plan.Replacements[1].Role != "user" {
			t.Fatalf("role conflict replacement=%+v", plan.Replacements)
		}
	})

	for _, keepRecent := range []int{0, 1, 5, 8} {
		t.Run("keep-recent-"+string(rune('0'+keepRecent)), func(t *testing.T) {
			messages := planFixtureMessages(62)
			tracker := NewDecayTracker()
			for i := 1; i < len(messages); i++ {
				tracker.MarkStubbed("recent", i, 0, 0)
			}
			snapshot := tracker.BuildDecayEvaluationSnapshot("recent", nil, 1_000, len(messages), 3)
			plan := BuildCompactionPlan(snapshot, messages, messages, keepRecent, true)
			if plan.ReplacementCount() == 0 {
				t.Fatalf("KeepRecent=%d 意外消除了全部 eligible run", keepRecent)
			}
			for i := len(messages) - keepRecent; i < len(messages); i++ {
				if i >= 0 && !plan.IsProtected(i) {
					t.Fatalf("KeepRecent=%d 未保护索引 %d", keepRecent, i)
				}
			}
		})
	}

	t.Run("活动 tool pair 不进入 replacement", func(t *testing.T) {
		messages := planFixtureMessages(60)
		messages[58] = Message{Role: "assistant", Content: json.RawMessage(`[{"type":"tool_use","id":"active-plan","name":"Read","input":{"file_path":"active.go"}}]`)}
		messages[59] = Message{Role: "user", Content: json.RawMessage(`[{"type":"tool_result","tool_use_id":"active-plan","content":"still active"}]`)}
		tracker := NewDecayTracker()
		for i := 1; i < len(messages); i++ {
			tracker.MarkStubbed("tool-pair", i, 0, 0)
		}
		snapshot := tracker.BuildDecayEvaluationSnapshot("tool-pair", nil, 1_000, len(messages), 3)
		plan := BuildCompactionPlan(snapshot, messages, messages, 0, true)
		if !plan.IsProtected(58) || !plan.IsProtected(59) || plan.IsCovered(58) || plan.IsCovered(59) {
			t.Fatalf("活动 tool pair 保护失败: protected=%v replacements=%v", plan.Protected, plan.Replacements)
		}
		decayed, _ := tracker.ApplyDecayBatch(messages, "tool-pair", 300, 100, nil, "", 1_000, plan)
		result, _ := CompactMessagesWithPlan(decayed, messages, plan)
		useIndex, resultIndex := -1, -1
		for i, message := range result {
			blocks, _ := parseContent(message.Content)
			for _, block := range blocks {
				if block.Type == "tool_use" && block.ID == "active-plan" {
					useIndex = i
				}
				if block.Type == "tool_result" && block.ToolUseID == "active-plan" {
					resultIndex = i
				}
			}
		}
		if useIndex < 0 || resultIndex != useIndex+1 {
			t.Fatalf("活动 tool pair 次序被破坏: use=%d result=%d", useIndex, resultIndex)
		}
	})
}

func TestCompactDisabledDegradesStage3ToStage2(t *testing.T) {
	messages := planFixtureMessages(55)
	tracker := NewDecayTracker()
	for i := 1; i < 51; i++ {
		tracker.MarkStubbed("disabled", i, 1, 0)
	}
	snapshot := tracker.BuildDecayEvaluationSnapshot("disabled", nil, 200, len(messages), 3)
	plan := BuildCompactionPlan(snapshot, messages, messages, 0, false)
	if plan.EffectiveEnabled {
		t.Fatal("compact-disabled plan is effective")
	}
	decayed, _ := tracker.ApplyDecayBatch(messages, "disabled", 300, 100, nil, "", 200, plan)
	result, blocks := CompactMessagesWithPlan(decayed, messages, plan)
	if len(blocks) != 0 {
		t.Fatalf("compact-disabled produced %d blocks", len(blocks))
	}
	for i := 1; i < 51; i++ {
		if extractTextFromContent(result[i].Content) == "" {
			t.Fatalf("compact-disabled index %d lost content", i)
		}
	}
}

func TestCompactionPlanMaterializationFallback(t *testing.T) {
	messages := planFixtureMessages(55)
	tracker := NewDecayTracker()
	for i := 1; i < 51; i++ {
		tracker.MarkStubbed("failure", i, 1, 0)
	}
	snapshot := tracker.BuildDecayEvaluationSnapshot("failure", nil, 200, len(messages), 3)
	plan := BuildCompactionPlan(snapshot, messages, messages, 0, true)
	if plan.ReplacementCount() == 0 {
		t.Fatal("fixture did not create a replacement")
	}
	// 故意破坏 immutable layout 副本，证明物化器会恢复衰减前 Stage-2
	// 内容，而不是输出空 marker 掩盖信息丢失。
	plan.Replacements[0].StartIdx = len(messages) + 10
	decayed, _ := tracker.ApplyDecayBatch(messages, "failure", 300, 100, nil, "", 200, plan)
	result, blocks := CompactMessagesWithPlan(decayed, messages, plan)
	if len(blocks) != 0 {
		t.Fatalf("failed materialization reported %d blocks", len(blocks))
	}
	for i := 1; i < 51; i++ {
		if extractTextFromContent(result[i].Content) == "" {
			t.Fatalf("materialization failure left empty content at %d", i)
		}
	}
}

func TestCompactionPlanPinnedSnapshotConcurrent(t *testing.T) {
	tracker := NewDecayTracker()
	tracker.MarkStubbed("same", 1, 1, 0)
	tracker.SetFilePath("same", 1, "old.go")
	paths := []string{"old.go"}
	pinned := NewPinnedPathSnapshot(paths)
	paths[0] = "mutated-after-snapshot.go"
	snapshot := tracker.BuildDecayEvaluationSnapshot("same", pinned, 200, 2, 3)
	start := make(chan struct{})
	done := make(chan struct{})
	go func() {
		<-start
		tracker.SetFilePath("same", 1, "new.go")
		tracker.SetPinnedPaths([]string{"new.go"})
		close(done)
	}()
	close(start)
	<-done
	plan := BuildCompactionPlan(snapshot, planFixtureMessages(2), planFixtureMessages(2), 0, true)
	if !plan.SnapshotPinned("old.go") {
		t.Fatal("request-local pinned snapshot changed after concurrent tracker update")
	}
}

func TestCompactionPlanCoordinatesStable(t *testing.T) {
	messages := planFixtureMessages(60)
	tracker := NewDecayTracker()
	for i := 1; i < 51; i++ {
		tracker.MarkStubbed("coords", i, 1, 0)
	}
	snapshot := tracker.BuildDecayEvaluationSnapshot("coords", nil, 200, len(messages), 3)
	plan := BuildCompactionPlan(snapshot, messages, messages, 0, true)
	decayed, _ := tracker.ApplyDecayBatch(messages, "coords", 300, 100, nil, "", 200, plan)
	result, blocks := CompactMessagesWithPlan(decayed, messages, plan)
	if len(blocks) == 0 || len(result) >= len(messages) {
		t.Fatalf("expected compacted result, result=%d blocks=%d", len(result), len(blocks))
	}
	for _, block := range blocks {
		if block.StartIdx < 0 || block.EndIdx < block.StartIdx || block.EndIdx >= len(messages) {
			t.Fatalf("invalid block coordinates: %+v", block)
		}
	}
}

func TestCompactionPlanStage2FallbackFrozenPrefixBytesStable(t *testing.T) {
	const sessionID = "stage2-fallback-frozen"
	var forwarded []Message
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Messages []Message `json:"messages"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("解析实际上游请求失败: %v", err)
			return
		}
		forwarded = deepCopyMessages(body.Messages)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"type":"message","usage":{"input_tokens":100,"output_tokens":1}}`)
	}))
	defer upstream.Close()

	server := newPipelineTestServer(t, upstream.URL)
	server.Config.Stubify.KeepRecent = 8
	server.Config.Collapse.CompactEnabled = true
	history := planFixtureMessages(58)
	history[0].Content = mustMarshal("受保护的超大首消息 " + strings.Repeat("heavy-context ", 12_000))
	threshold := server.Config.Stubify.TokenThreshold
	if total := server.TokenCounter.CountMessagesTokens(history); total <= threshold {
		t.Fatalf("fixture 总 token=%d，必须高于压缩阈值 %d", total, threshold)
	}
	tokenFloor := threshold / 2
	if tokenFloor < 10_000 {
		tokenFloor = 10_000
	}
	if cutoff := CalcCollapseCutoff(history, tokenFloor, server.TokenCounter, server.Config.Stubify.KeepRecent); cutoff > 0 {
		t.Fatalf("fixture 意外进入 collapse 主路径，cutoff=%d", cutoff)
	}

	stateKey := historyEpochStateKey(sessionID, 1)
	for i := 1; i <= 49; i++ {
		server.DecayTracker.MarkStubbed(stateKey, i, -1_000, 0)
	}
	var persistErr error
	server.Frozen.SetPersistFunc(func(key, value string) {
		if persistErr == nil {
			persistErr = server.Store.PersistState(key, value)
		}
	})
	server.Frozen.SetLoadFunc(server.Store.LoadState)

	servePipelineRequest(t, server, sessionID, history)
	if persistErr != nil {
		t.Fatalf("持久化 Frozen 失败: %v", persistErr)
	}
	if len(forwarded) != len(history) {
		t.Fatalf("49 条短 run 不应生成 replacement：forwarded=%d original=%d", len(forwarded), len(history))
	}
	for i := 1; i <= 49; i++ {
		text := extractTextFromContent(forwarded[i].Content)
		if text == "" {
			t.Fatalf("Stage-2 fallback 在索引 %d 丢失正文", i)
		}
		if bytes.Equal(forwarded[i].Content, history[i].Content) {
			t.Fatalf("索引 %d 未实际进入 Stage-2 fallback", i)
		}
		if strings.Contains(text, "[Compacted:") {
			t.Fatalf("49 条短 run 在索引 %d 伪造了 CompactedBlock", i)
		}
	}

	stored := server.Frozen.Get(stateKey, history)
	if stored == nil {
		t.Fatal("当轮 Frozen 内存快照不存在")
	}
	wireBytes, err := json.Marshal(forwarded)
	if err != nil {
		t.Fatal(err)
	}
	storedBytes, err := json.Marshal(stored.Messages)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(wireBytes, storedBytes) {
		t.Fatalf("实际上游与当轮 Frozen prefix bytes 不一致\nwire:   %s\nstored: %s", wireBytes, storedBytes)
	}

	coldFrozen := NewFrozenStubs()
	coldFrozen.SetLoadFunc(server.Store.LoadState)
	cold := coldFrozen.Get(stateKey, history)
	if cold == nil {
		t.Fatal("SQLite 冷恢复未返回 Frozen prefix")
	}
	coldBytes, err := json.Marshal(cold.Messages)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(wireBytes, coldBytes) {
		t.Fatalf("实际上游与 SQLite 冷恢复 prefix bytes 不一致\nwire: %s\ncold: %s", wireBytes, coldBytes)
	}
}

func planFixtureMessages(n int) []Message {
	messages := make([]Message, n)
	for i := range messages {
		role := "assistant"
		if i%2 == 0 {
			role = "user"
		}
		content, _ := json.Marshal("stage-two payload " + string(rune('a'+i%26)) + " " + string(bytes.Repeat([]byte("x"), 120)))
		messages[i] = Message{Role: role, Content: content}
	}
	return messages
}

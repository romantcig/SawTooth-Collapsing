package proxy

import (
	"encoding/json"
	"fmt"
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

// ---- CompactMessages tests ----

func TestCompactMessagesBothRolesEnough(t *testing.T) {
	dt := NewDecayTracker()
	// 111 messages: run [1, 106] (L=106 even), assistant=53 user=53, both ≥ 50
	// leftRole=user(0), rightRole=asst(107) → 2 compacted [asst, user]
	// Alternation: user→asst→user→asst ✓
	original, decayed := buildTestThread(111, dt)

	result, blocks := CompactMessages(decayed, original, dt, "test", 200, 3.0)

	if len(blocks) == 0 {
		t.Fatalf("expected at least 1 block, got 0 (user=%d asst=%d in run [1,106])",
			countRoleInRange(decayed, 1, 106, "user"),
			countRoleInRange(decayed, 1, 106, "assistant"))
	}
	// Verify role alternation in result
	roles := make([]string, 0, len(result))
	for _, msg := range result {
		roles = append(roles, msg.Role)
	}
	for i := 1; i < len(roles); i++ {
		if roles[i] == roles[i-1] {
			t.Errorf("role violation at index %d: %s → %s", i, roles[i-1], roles[i])
		}
	}
	// Verify fewer messages
	if len(result) >= len(decayed) {
		t.Errorf("expected fewer messages after compaction: %d → %d", len(decayed), len(result))
	}
	// With 111 msgs and even run → should produce exactly 2 blocks
	if len(blocks) != 2 {
		t.Errorf("got %d blocks (expected 2 for even-length run)", len(blocks))
	}
}

func TestCompactMessagesSingleRoleEnough(t *testing.T) {
	dt := NewDecayTracker()
	// 106 messages, mark [0, 99] as stubbed → run [1, 99] = 99 msgs
	// assistant at 1,3,...,99 = 50; user at 2,4,...,98 = 49
	// Only assistant reaches 50 → 1 compacted message (absorb entire range)
	n := 106
	// Mark stubs only — message construction happens below
	for i := 0; i <= 99; i++ {
		dt.MarkStubbed("test", i, 1, 0.0)
	}

	// Build original and decayed slices
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

	result, blocks := CompactMessages(decayed, original, dt, "test", 200, 3.0)

	if len(blocks) == 0 {
		// Debug
		for i := 1; i <= 100; i++ {
			stage := dt.GetStage("test", i, 200, n, 3.0)
			if stage == DecayCompacted {
				t.Logf("msg[%d]=%s is Stage 3", i, decayed[i].Role)
			}
		}
		t.Fatalf("expected at least 1 block for run with 50 assistant + 49 user")
	}
	// Verify role alternation preserved
	roles := make([]string, 0, len(result))
	for _, msg := range result {
		roles = append(roles, msg.Role)
	}
	for i := 1; i < len(roles); i++ {
		if roles[i] == roles[i-1] {
			t.Errorf("role violation at index %d: %s → %s", i, roles[i-1], roles[i])
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

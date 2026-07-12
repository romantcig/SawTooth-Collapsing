package proxy

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

// ----- buildToolUseInfoExtended -----

func TestBuildToolUseInfoExtended(t *testing.T) {
	toolUse := ContentBlock{
		Type: "tool_use",
		ID:   "tool_01",
		Name: "Read",
		Input: map[string]any{
			"file_path": "/app/main.go",
		},
	}
	msg := Message{
		Role:    "assistant",
		Content: rebuildContent([]ContentBlock{toolUse}, true),
	}

	info := buildToolUseInfoExtended([]Message{msg})

	if len(info) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(info))
	}
	if info["tool_01"].Name != "Read" {
		t.Errorf("expected Name=Read, got %s", info["tool_01"].Name)
	}
	if info["tool_01"].Keywords != "Read /app/main.go" {
		t.Errorf("expected Keywords='Read /app/main.go', got '%s'", info["tool_01"].Keywords)
	}
}

func TestBuildToolUseInfoExtendedEmpty(t *testing.T) {
	info := buildToolUseInfoExtended(nil)
	if len(info) != 0 {
		t.Errorf("expected empty map for nil input, got %d entries", len(info))
	}
}

func TestBuildToolUseInfoExtendedNoID(t *testing.T) {
	// tool_use without an ID should be skipped
	toolUse := ContentBlock{
		Type: "tool_use",
		ID:   "", // no ID
		Name: "Read",
		Input: map[string]any{
			"file_path": "/app/main.go",
		},
	}
	msg := Message{
		Role:    "assistant",
		Content: rebuildContent([]ContentBlock{toolUse}, true),
	}

	info := buildToolUseInfoExtended([]Message{msg})
	// Should be empty because no ID
	if len(info) != 0 {
		t.Errorf("expected empty map for tool_use with no ID, got %d entries", len(info))
	}
}

func TestBuildToolUseInfoExtendedMultipleTools(t *testing.T) {
	messages := []Message{
		{
			Role: "assistant",
			Content: rebuildContent([]ContentBlock{
				{Type: "tool_use", ID: "t1", Name: "Read", Input: map[string]any{"file_path": "a.go"}},
				{Type: "tool_use", ID: "t2", Name: "Edit", Input: map[string]any{"file_path": "b.go"}},
			}, true),
		},
	}

	info := buildToolUseInfoExtended(messages)

	if len(info) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(info))
	}
	if info["t1"].Keywords != "Read a.go" {
		t.Errorf("t1 keywords: got '%s'", info["t1"].Keywords)
	}
	if info["t2"].Keywords != "Edit b.go" {
		t.Errorf("t2 keywords: got '%s'", info["t2"].Keywords)
	}
}

func TestBuildToolUseInfoExtendedDuplicateID(t *testing.T) {
	// Same ID appears twice — last write wins
	messages := []Message{
		{
			Role: "assistant",
			Content: rebuildContent([]ContentBlock{
				{Type: "tool_use", ID: "t1", Name: "Read", Input: map[string]any{"file_path": "a.go"}},
			}, true),
		},
		{
			Role: "assistant",
			Content: rebuildContent([]ContentBlock{
				{Type: "tool_use", ID: "t1", Name: "Edit", Input: map[string]any{"file_path": "b.go"}},
			}, true),
		},
	}

	info := buildToolUseInfoExtended(messages)
	if info["t1"].Name != "Edit" {
		t.Errorf("expected last-write-wins, got Name=%s", info["t1"].Name)
	}
}

// ----- CompressContext -----

func TestCompressContextEmpty(t *testing.T) {
	messages, result := CompressContext(nil, 4, nil)
	if len(messages) != 0 {
		t.Errorf("expected empty slice, got %d", len(messages))
	}
	if result.ThinkingCompressed != 0 {
		t.Errorf("expected 0 compressed, got %d", result.ThinkingCompressed)
	}
}

func TestCompressContextShortMessages(t *testing.T) {
	// 3 messages total, keepRecent=4 → all protected
	messages := []Message{
		{Role: "system", Content: mustMarshal("system prompt")},
		{Role: "user", Content: mustMarshal("hello")},
		{Role: "assistant", Content: mustMarshal("hi")},
	}

	compressed, _ := CompressContext(messages, 4, nil)

	if len(compressed) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(compressed))
	}
	// All should be untouched
	for i := range messages {
		if !jsonEqual(messages[i].Content, compressed[i].Content) {
			t.Errorf("message[%d] was modified unexpectedly", i)
		}
	}
}

func TestCompressContextProtectsSystemMessage(t *testing.T) {
	// messages[0] should never be touched even when within scan range
	thinking := strings.Repeat("x", 2500) // > 500 tokens (nil tc: len/4)

	messages := []Message{
		{Role: "system", Content: rebuildContent([]ContentBlock{
			{Type: "thinking", Thinking: thinking},
		}, true)},
		{Role: "user", Content: mustMarshal("hello")},
	}

	compressed, _ := CompressContext(messages, 0, nil)
	// messages[0] thinking block should NOT be compressed
	blocks, _ := parseContent(compressed[0].Content)
	if len(blocks) == 0 {
		t.Fatal("unexpected empty blocks")
	}
	if blocks[0].Type != "thinking" {
		t.Errorf("system message thinking should NOT be compressed, got type=%s", blocks[0].Type)
	}
}

func TestCompressContextThinkingBlocks(t *testing.T) {
	shortThinking := strings.Repeat("a", 200)
	// need > 500 tokens → with nil tc fallback len/4 > 500 → len > 2000
	longThinking := strings.Repeat("b", 2500)

	messages := []Message{
		{Role: "system", Content: mustMarshal("system")},
		{Role: "assistant", Content: rebuildContent([]ContentBlock{
			{Type: "text", Text: "ok"},
			{Type: "thinking", Thinking: shortThinking}, // < 500 tokens, stays
			{Type: "thinking", Thinking: longThinking},  // > 500 tokens, compressed
		}, true)},
		{Role: "user", Content: mustMarshal("next")},
	}

	compressed, result := CompressContext(messages, 1, nil)

	if result.ThinkingCompressed != 1 {
		t.Errorf("expected 1 thinking compressed, got %d", result.ThinkingCompressed)
	}

	blocks, _ := parseContent(compressed[1].Content)
	if len(blocks) != 3 {
		t.Fatalf("expected 3 blocks, got %d", len(blocks))
	}

	// Block 0: text, untouched
	if blocks[0].Type != "text" || blocks[0].Text != "ok" {
		t.Errorf("block 0: expected text='ok', got type=%s text=%s", blocks[0].Type, blocks[0].Text)
	}
	// Block 1: thinking, untouched (short)
	if blocks[1].Type != "thinking" || len(blocks[1].Thinking) != 200 {
		t.Errorf("block 1: expected thinking with 200 chars, got type=%s len=%d", blocks[1].Type, len(blocks[1].Thinking))
	}
	// Block 2: compressed — still type "thinking" with placeholder text
	if blocks[2].Type != "thinking" || blocks[2].Thinking != "[context compressed: thinking block]" {
		t.Errorf("block 2: expected type=thinking thinking='[context compressed: thinking block]', got type=%s thinking=%s", blocks[2].Type, blocks[2].Thinking)
	}
}

func TestCompressContextOmitsOldSignedThinking(t *testing.T) {
	longThinking := strings.Repeat("signed reasoning ", 800)
	originalContent := json.RawMessage(`[{"type":"thinking","thinking":` + string(mustMarshal(longThinking)) + `,"signature":"sig-protected"},{"type":"text","text":"answer"}]`)
	messages := []Message{
		{Role: "user", Content: mustMarshal("start")},
		{Role: "assistant", Content: originalContent},
		{Role: "user", Content: mustMarshal("next")},
	}

	compressed, result := CompressContext(messages, 1, nil)

	if result.ThinkingCompressed != 1 {
		t.Fatalf("旧带签名 thinking 应被省略，实际 compressed=%d", result.ThinkingCompressed)
	}
	blocks, _ := parseContent(compressed[1].Content)
	if len(blocks) != 1 || blocks[0].Type != "text" || blocks[0].Text != "answer" {
		t.Fatalf("省略旧 signed thinking 后内容异常: %s", compressed[1].Content)
	}
	if bytes.Contains(compressed[1].Content, []byte("sig-protected")) || bytes.Contains(compressed[1].Content, []byte("context compressed: thinking")) {
		t.Fatalf("旧 signed thinking 不应保留签名或伪造 thinking 占位块: %s", compressed[1].Content)
	}
}

func TestCompressContextPreservesRecentSignedThinkingByteForByte(t *testing.T) {
	longThinking := strings.Repeat("active signed reasoning ", 800)
	originalContent := json.RawMessage(`[{"type":"thinking","thinking":` + string(mustMarshal(longThinking)) + `,"signature":"sig-active","cache_control":{"type":"ephemeral"}},{"type":"tool_use","id":"toolu_active","name":"Read","input":{"file_path":"main.go"}}]`)
	messages := []Message{
		{Role: "user", Content: mustMarshal("start")},
		{Role: "assistant", Content: originalContent},
		{Role: "user", Content: mustMarshal("tool result")},
	}

	compressed, result := CompressContext(messages, 2, nil)

	if result.ThinkingCompressed != 0 {
		t.Fatalf("keepRecent 内的 signed thinking 不得压缩，实际 compressed=%d", result.ThinkingCompressed)
	}
	if !bytes.Equal(compressed[1].Content, originalContent) {
		t.Fatalf("活动 tool-use thinking 未原样保留:\n got: %s\nwant: %s", compressed[1].Content, originalContent)
	}
}

func TestCompressContextProtectsActiveToolThinkingWhenKeepRecentZero(t *testing.T) {
	longThinking := strings.Repeat("active tool reasoning ", 800)
	originalContent := json.RawMessage(`[{"type":"thinking","thinking":` + string(mustMarshal(longThinking)) + `,"signature":"sig-active-zero","cache_control":{"type":"ephemeral"}},{"type":"redacted_thinking","data":"opaque-redacted"},{"type":"tool_use","id":"toolu_active_zero","name":"Read","input":{"file_path":"main.go"},"future_field":{"keep":true}}]`)
	messages := []Message{
		{Role: "user", Content: mustMarshal("start")},
		{Role: "assistant", Content: originalContent},
		{Role: "user", Content: mustMarshalBlocks([]ContentBlock{{Type: "tool_result", ToolUseID: "toolu_active_zero", Content: "result"}})},
	}

	compressed, result := CompressContext(messages, 0, nil)

	if result.ThinkingCompressed != 0 {
		t.Fatalf("活动工具轮次不得压缩 thinking，实际 compressed=%d", result.ThinkingCompressed)
	}
	if !bytes.Equal(compressed[1].Content, originalContent) {
		t.Fatalf("keepRecent=0 时活动工具 assistant 未原样保留:\n got: %s\nwant: %s", compressed[1].Content, originalContent)
	}
}

func TestCompressContextPreservesUnknownBlockFieldsWhenRebuilding(t *testing.T) {
	longThinking := strings.Repeat("old signed reasoning ", 800)
	content := json.RawMessage(`[{"type":"thinking","thinking":` + string(mustMarshal(longThinking)) + `,"signature":"old"},{"type":"redacted_thinking","data":"opaque","future":{"keep":true}},{"type":"text","text":"visible","cache_control":{"type":"ephemeral"}}]`)
	messages := []Message{
		{Role: "user", Content: mustMarshal("start")},
		{Role: "assistant", Content: content},
		{Role: "user", Content: mustMarshal("next turn")},
	}

	compressed, result := CompressContext(messages, 1, nil)
	if result.ThinkingCompressed != 1 {
		t.Fatalf("signed thinking compressed=%d, want 1", result.ThinkingCompressed)
	}
	var blocks []map[string]any
	if err := json.Unmarshal(compressed[1].Content, &blocks); err != nil {
		t.Fatal(err)
	}
	if got := blocks[0]["type"]; got != "redacted_thinking" {
		t.Fatalf("first retained block type=%v", got)
	}
	if got := blocks[0]["data"]; got != "opaque" {
		t.Fatalf("redacted_thinking.data=%v, want opaque", got)
	}
	if _, ok := blocks[0]["future"]; !ok {
		t.Fatal("未来 block 字段在重建时丢失")
	}
	if _, ok := blocks[0]["input"]; ok {
		t.Fatal("非 tool_use block 不应凭空出现 input:null")
	}
	if _, ok := blocks[1]["cache_control"]; !ok {
		t.Fatal("未修改 text block 的 cache_control 丢失")
	}
}

func TestActiveToolAssistantIndexRequiresAdjacentMatchingIDs(t *testing.T) {
	assistant := Message{Role: "assistant", Content: mustMarshalBlocks([]ContentBlock{{Type: "tool_use", ID: "toolu_match", Name: "Read"}})}
	matchingResult := Message{Role: "user", Content: mustMarshalBlocks([]ContentBlock{{Type: "tool_result", ToolUseID: "toolu_match", Content: "ok"}})}
	unmatchedResult := Message{Role: "user", Content: mustMarshalBlocks([]ContentBlock{{Type: "tool_result", ToolUseID: "toolu_other", Content: "ok"}})}

	if got := activeToolAssistantIndex([]Message{{Role: "user", Content: mustMarshal("start")}, assistant, matchingResult}); got != 1 {
		t.Fatalf("匹配工具轮次索引=%d, want 1", got)
	}
	if got := activeToolAssistantIndex([]Message{{Role: "user", Content: mustMarshal("start")}, assistant, unmatchedResult}); got != -1 {
		t.Fatalf("不匹配 tool ID 不应保护 assistant，got=%d", got)
	}
	if got := activeToolAssistantIndex([]Message{assistant, matchingResult, {Role: "user", Content: mustMarshal("new turn")}}); got != -1 {
		t.Fatalf("已结束的旧工具轮次不应视为活动轮次，got=%d", got)
	}
}

func TestCompressContextToolResults(t *testing.T) {
	tc, err := NewTokenCounter()
	if err != nil {
		t.Fatalf("NewTokenCounter: %v", err)
	}
	// Build a long tool result (> 500 tokens)
	longText := strings.Repeat("line of output\n", 200) // 200 lines

	toolUseMsg := Message{
		Role: "assistant",
		Content: rebuildContent([]ContentBlock{
			{Type: "tool_use", ID: "tu_1", Name: "Bash", Input: map[string]any{"command": "git log --oneline"}},
		}, true),
	}
	toolResultMsg := Message{
		Role: "user",
		Content: rebuildContent([]ContentBlock{
			{Type: "tool_result", ToolUseID: "tu_1", IsError: false, Content: longText},
		}, true),
	}

	messages := []Message{
		{Role: "system", Content: mustMarshal("system")},
		toolUseMsg,
		toolResultMsg,
		{Role: "assistant", Content: mustMarshal("latest")},
	}

	compressed, result := CompressContext(messages, 1, tc)

	if result.ToolResultsCompressed != 1 {
		t.Errorf("expected 1 tool_result compressed, got %d (tokensSaved=%d)", result.ToolResultsCompressed, result.TokensSaved)
	}
	if result.TokensSaved <= 0 {
		t.Logf("warning: TokensSaved=%d (may be 0 if text is short enough)", result.TokensSaved)
	}

	// Verify the tool_result was compressed but kept required fields
	blocks, _ := parseContent(compressed[2].Content)
	if len(blocks) != 1 {
		t.Fatalf("expected 1 block in tool_result, got %d", len(blocks))
	}
	if blocks[0].Type != "tool_result" {
		t.Errorf("expected type=tool_result, got %s", blocks[0].Type)
	}
	if blocks[0].ToolUseID != "tu_1" {
		t.Errorf("expected ToolUseID=tu_1, got %s", blocks[0].ToolUseID)
	}
	if blocks[0].IsError != false {
		t.Errorf("expected IsError=false")
	}
	// Content should be the summary string
	summary, ok := blocks[0].Content.(string)
	if !ok {
		t.Errorf("expected Content to be string, got %T", blocks[0].Content)
	}
	if !strings.HasPrefix(summary, "[context compressed:") {
		t.Errorf("expected summary to start with '[context compressed:', got '%s'", summary)
	}
}

func TestCompressContextToolResultPreservesIsError(t *testing.T) {
	tc, err := NewTokenCounter()
	if err != nil {
		t.Fatalf("NewTokenCounter: %v", err)
	}
	longText := strings.Repeat("ERROR: something went wrong\n", 100)

	messages := []Message{
		{Role: "system", Content: mustMarshal("system")},
		{Role: "assistant", Content: rebuildContent([]ContentBlock{
			{Type: "tool_use", ID: "tu_err", Name: "Bash", Input: map[string]any{"command": "bad"}},
		}, true)},
		{Role: "user", Content: rebuildContent([]ContentBlock{
			{Type: "tool_result", ToolUseID: "tu_err", IsError: true, Content: longText},
		}, true)},
	}

	compressed, _ := CompressContext(messages, 0, tc)
	blocks, _ := parseContent(compressed[2].Content)
	if blocks[0].IsError != true {
		t.Errorf("IsError should be preserved as true, got %v", blocks[0].IsError)
	}
}

func TestCompressContextKeepRecent(t *testing.T) {
	// keepRecent=2 means last 2 messages are untouched
	longThinking := strings.Repeat("x", 2500)

	messages := []Message{
		{Role: "system", Content: mustMarshal("system")},
		{Role: "assistant", Content: rebuildContent([]ContentBlock{
			{Type: "thinking", Thinking: longThinking}, // should be compressed
		}, true)},
		{Role: "user", Content: mustMarshal("msg2")},      // keepRecent protected
		{Role: "assistant", Content: mustMarshal("msg3")}, // keepRecent protected
	}

	compressed, result := CompressContext(messages, 2, nil)

	if result.ThinkingCompressed != 1 {
		t.Errorf("expected 1 thinking compressed, got %d", result.ThinkingCompressed)
	}

	// messages[3] (keepRecent) should be untouched
	blocks, _ := parseContent(compressed[3].Content)
	if blocks[0].Text != "msg3" {
		t.Errorf("keepRecent message should not be modified")
	}
}

func TestCompressContextNoTokenCounter(t *testing.T) {
	// Should not panic with nil TokenCounter, uses rough estimates
	longThinking := strings.Repeat("y", 2500)

	messages := []Message{
		{Role: "system", Content: mustMarshal("system")},
		{Role: "assistant", Content: rebuildContent([]ContentBlock{
			{Type: "thinking", Thinking: longThinking},
		}, true)},
		{Role: "user", Content: mustMarshal("next")},
	}

	compressed, result := CompressContext(messages, 1, nil)

	if result.ThinkingCompressed != 1 {
		t.Errorf("expected 1 thinking compressed with nil tc, got %d", result.ThinkingCompressed)
	}
	if result.TokensSaved != 0 {
		t.Logf("TokensSaved=%d (0 expected with nil tc)", result.TokensSaved)
	}
	_ = compressed
}

func TestCompressContextIsArrayPreserved(t *testing.T) {
	// When content is a bare string (isArray=false) and no blocks are modified,
	// it should stay as a bare string.
	longThinking := strings.Repeat("z", 2500)

	messages := []Message{
		{Role: "system", Content: mustMarshal("system")},
		{Role: "assistant", Content: rebuildContent([]ContentBlock{
			{Type: "thinking", Thinking: longThinking},
			{Type: "text", Text: "after thinking"},
		}, true)},
		{Role: "user", Content: mustMarshal("next")},
	}

	compressed, _ := CompressContext(messages, 1, nil)

	// The assistant message had isArray=true originally, should stay as array after compression
	blocks, isArray := parseContent(compressed[1].Content)
	if !isArray {
		t.Errorf("expected isArray=true after compression, got false")
	}
	if len(blocks) != 2 {
		t.Errorf("expected 2 blocks, got %d", len(blocks))
	}
	if blocks[0].Type != "thinking" || blocks[0].Thinking != "[context compressed: thinking block]" {
		t.Errorf("block 0: expected thinking placeholder, got type=%s thinking=%s", blocks[0].Type, blocks[0].Text)
	}
}

func TestCompressContextToolResultWithMissingToolUse(t *testing.T) {
	tc, err := NewTokenCounter()
	if err != nil {
		t.Fatalf("NewTokenCounter: %v", err)
	}
	// tool_result references a tool_use_id that doesn't exist in messages
	longText := strings.Repeat("this is a line of output text\n", 200) // > 500 tokens

	messages := []Message{
		{Role: "system", Content: mustMarshal("system")},
		{Role: "user", Content: rebuildContent([]ContentBlock{
			{Type: "tool_result", ToolUseID: "nonexistent", Content: longText},
		}, true)},
	}

	compressed, result := CompressContext(messages, 0, tc)
	if result.ToolResultsCompressed != 1 {
		t.Errorf("expected compression even with missing tool_use, got %d", result.ToolResultsCompressed)
	}
	blocks, _ := parseContent(compressed[1].Content)
	if blocks[0].Type != "tool_result" {
		t.Errorf("type should stay tool_result, got %s", blocks[0].Type)
	}
}

// ----- truncateMiddle -----

func TestTruncateMiddleShort(t *testing.T) {
	s := "hello"
	result := truncateMiddle(s, 10, 10)
	if result != "hello" {
		t.Errorf("short string should not be truncated, got '%s'", result)
	}
}

func TestTruncateMiddleLong(t *testing.T) {
	s := "abcde" + strings.Repeat("m", 100) + "vwxyz"
	result := truncateMiddle(s, 5, 5)
	if !strings.Contains(result, "...") {
		t.Errorf("expected truncation placeholder, got '%s'", result)
	}
	// Should start with "abcde" and end with "vwxyz"
	if !strings.HasPrefix(result, "abcde") {
		t.Errorf("expected prefix 'abcde', got '%s'", result)
	}
	if !strings.HasSuffix(result, "vwxyz") {
		t.Errorf("expected suffix 'vwxyz', got '%s'", result)
	}
}

func TestTruncateMiddleExact(t *testing.T) {
	s := "abcdefghij" // 10 runes
	result := truncateMiddle(s, 5, 5)
	if result != s {
		t.Errorf("string equal to head+tail should not be truncated, got '%s'", result)
	}
}

// ----- truncateToolResult -----

func TestTruncateToolResultShort(t *testing.T) {
	text := "line1\nline2\nline3"
	result := truncateToolResult(text)
	if result != text {
		t.Errorf("short text should not be truncated, got '%s'", result)
	}
}

func TestTruncateToolResultLong(t *testing.T) {
	// 35 lines (>30 threshold) to trigger truncation
	lines := make([]string, 35)
	for i := range lines {
		lines[i] = "line " + string(rune('a'+i%26))
	}
	text := strings.Join(lines, "\n")

	result := truncateToolResult(text)

	if !strings.Contains(result, "lines compressed") {
		t.Errorf("expected 'lines compressed' placeholder, got '%s'", result)
	}
	if !strings.HasPrefix(result, "line a") {
		t.Errorf("expected first 15 lines at start, got prefix: '%s'", truncateMiddle(result, 30, 0))
	}
	if !strings.HasSuffix(result, lines[34]) {
		t.Errorf("expected last line preserved, got suffix mismatch")
	}
}

// ----- buildToolResultSummary -----

func TestBuildToolResultSummary(t *testing.T) {
	text := "package main\n\nfunc main() {\n\tfmt.Println(\"hello\")\n}\n"
	text += strings.Repeat("// padding\n", 30) // make it long enough

	block := ContentBlock{
		Content: text,
	}
	info := toolUseInfoExtended{
		Name:     "Read",
		Keywords: "Read main.go",
	}

	summary := buildToolResultSummary(block, info)

	if !strings.HasPrefix(summary, "[context compressed: Read,") {
		t.Errorf("expected prefix '[context compressed: Read,', got '%s'", truncateMiddle(summary, 40, 0))
	}
	if !strings.Contains(summary, "func main") {
		t.Errorf("expected 'func main' signature, got '%s'", summary)
	}
	if !strings.Contains(summary, "→ deep_search('Read main.go')") {
		t.Errorf("expected deep_search hint, got '%s'", summary)
	}
}

func TestBuildToolResultSummaryNoKeywords(t *testing.T) {
	text := "some output\n" + strings.Repeat("pad\n", 30)

	block := ContentBlock{
		Content: text,
	}
	info := toolUseInfoExtended{
		Name:     "Bash",
		Keywords: "",
	}

	summary := buildToolResultSummary(block, info)

	if strings.Contains(summary, "→ deep_search") {
		t.Errorf("no deep_search hint when keywords are empty, got '%s'", summary)
	}
}

// ----- extractToolKeywords -----

func TestExtractToolKeywordsRead(t *testing.T) {
	block := ContentBlock{Name: "Read", Input: map[string]any{"file_path": "/app/main.go"}}
	kw := extractToolKeywords(block)
	if kw != "Read /app/main.go" {
		t.Errorf("expected 'Read /app/main.go', got '%s'", kw)
	}
}

func TestExtractToolKeywordsBashLong(t *testing.T) {
	longCmd := strings.Repeat("x", 100)
	block := ContentBlock{Name: "Bash", Input: map[string]any{"command": longCmd}}
	kw := extractToolKeywords(block)
	// Should be truncated to 40 runes + "..."
	if len([]rune(kw)) > 48 { // "Bash " + 40 runes + "..."
		t.Errorf("command too long: %d runes: '%s'", len([]rune(kw)), kw)
	}
	if !strings.HasPrefix(kw, "Bash ") {
		t.Errorf("expected 'Bash ' prefix, got '%s'", kw)
	}
}

func TestExtractToolKeywordsGrep(t *testing.T) {
	block := ContentBlock{Name: "Grep", Input: map[string]any{
		"pattern": "func.*Test",
		"path":    "/app/",
	}}
	kw := extractToolKeywords(block)
	if kw != "Grep func.*Test /app/" {
		t.Errorf("expected 'Grep func.*Test /app/', got '%s'", kw)
	}
}

func TestExtractToolKeywordsGlob(t *testing.T) {
	block := ContentBlock{Name: "Glob", Input: map[string]any{"pattern": "**/*.go"}}
	kw := extractToolKeywords(block)
	if kw != "Glob **/*.go" {
		t.Errorf("expected 'Glob **/*.go', got '%s'", kw)
	}
}

func TestExtractToolKeywordsAgent(t *testing.T) {
	block := ContentBlock{Name: "Agent", Input: map[string]any{"description": "find bugs in this code"}}
	kw := extractToolKeywords(block)
	if kw != "Agent find bugs in this code" {
		t.Errorf("expected 'Agent find bugs in this code', got '%s'", kw)
	}
}

func TestExtractToolKeywordsWebSearch(t *testing.T) {
	block := ContentBlock{Name: "WebSearch", Input: map[string]any{"query": "golang generics tutorial"}}
	kw := extractToolKeywords(block)
	if kw != "golang generics tutorial" {
		t.Errorf("expected 'golang generics tutorial', got '%s'", kw)
	}
}

func TestExtractToolKeywordsWebFetch(t *testing.T) {
	block := ContentBlock{Name: "WebFetch", Input: map[string]any{"url": "https://pkg.go.dev/fmt"}}
	kw := extractToolKeywords(block)
	if kw != "https://pkg.go.dev/fmt" {
		t.Errorf("expected 'https://pkg.go.dev/fmt', got '%s'", kw)
	}
}

func TestExtractToolKeywordsUnknown(t *testing.T) {
	block := ContentBlock{Name: "CustomTool", Input: map[string]any{}}
	kw := extractToolKeywords(block)
	if kw != "CustomTool" {
		t.Errorf("expected 'CustomTool' for unknown tool, got '%s'", kw)
	}
}

// ----- countBlockTokens -----

func TestCountBlockTokensThinking(t *testing.T) {
	tc, err := NewTokenCounter()
	if err != nil {
		t.Fatalf("NewTokenCounter: %v", err)
	}
	block := ContentBlock{Type: "thinking", Thinking: strings.Repeat("hello world ", 50)}

	count := countBlockTokens(block, tc)
	if count <= 0 {
		t.Errorf("expected positive token count, got %d", count)
	}
}

func TestCountBlockTokensThinkingNilTC(t *testing.T) {
	block := ContentBlock{Type: "thinking", Thinking: strings.Repeat("a", 400)}
	count := countBlockTokens(block, nil)
	// Rough estimate: 400/4 = 100
	if count != 100 {
		t.Errorf("expected ~100 tokens (400/4), got %d", count)
	}
}

func TestCountBlockTokensToolResult(t *testing.T) {
	tc, err := NewTokenCounter()
	if err != nil {
		t.Fatalf("NewTokenCounter: %v", err)
	}
	block := ContentBlock{
		Type:    "tool_result",
		Content: strings.Repeat("line\n", 100),
	}
	count := countBlockTokens(block, tc)
	if count <= 0 {
		t.Errorf("expected positive token count, got %d", count)
	}
}

func TestCountBlockTokensUnknownType(t *testing.T) {
	tc, err := NewTokenCounter()
	if err != nil {
		t.Fatalf("NewTokenCounter: %v", err)
	}
	block := ContentBlock{Type: "text", Text: "hello world"}
	count := countBlockTokens(block, tc)
	if count != 0 {
		t.Errorf("expected 0 for unknown block type, got %d", count)
	}
}

// ----- helpers -----

func mustMarshal(s string) json.RawMessage {
	data, err := json.Marshal(s)
	if err != nil {
		panic(err)
	}
	return json.RawMessage(data)
}

func jsonEqual(a, b json.RawMessage) bool {
	aa, _ := json.Marshal(a)
	bb, _ := json.Marshal(b)
	return string(aa) == string(bb)
}

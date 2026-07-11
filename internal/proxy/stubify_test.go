package proxy

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func TestMessageUnknownFieldsRoundTrip(t *testing.T) {
	raw := []byte(`{"role":"user","content":"before","isMeta":true,"agent_id":"agent-7","future_object":{"enabled":true,"limit":17},"future_array":[1,null,{"mode":"strict"}],"future_null":null}`)

	var message Message
	if err := json.Unmarshal(raw, &message); err != nil {
		t.Fatalf("unmarshal message: %v", err)
	}
	assertJSONEquivalent(t, mustMarshalJSON(t, message), raw)

	message.Content = json.RawMessage(`"after"`)
	got := mustMarshalJSON(t, message)
	want := []byte(`{"role":"user","content":"after","isMeta":true,"agent_id":"agent-7","future_object":{"enabled":true,"limit":17},"future_array":[1,null,{"mode":"strict"}],"future_null":null}`)
	assertJSONEquivalent(t, got, want)
}

func TestMessageUnknownNullRemainsDistinctFromAbsent(t *testing.T) {
	var withNull Message
	if err := json.Unmarshal([]byte(`{"role":"user","content":"same","future":null}`), &withNull); err != nil {
		t.Fatalf("unmarshal explicit null: %v", err)
	}
	var absent Message
	if err := json.Unmarshal([]byte(`{"role":"user","content":"same"}`), &absent); err != nil {
		t.Fatalf("unmarshal absent field: %v", err)
	}

	var withNullObject map[string]json.RawMessage
	if err := json.Unmarshal(mustMarshalJSON(t, withNull), &withNullObject); err != nil {
		t.Fatalf("decode explicit-null result: %v", err)
	}
	if value, ok := withNullObject["future"]; !ok || string(value) != "null" {
		t.Fatalf("explicit null not preserved: %s", mustMarshalJSON(t, withNull))
	}

	var absentObject map[string]json.RawMessage
	if err := json.Unmarshal(mustMarshalJSON(t, absent), &absentObject); err != nil {
		t.Fatalf("decode absent result: %v", err)
	}
	if _, ok := absentObject["future"]; ok {
		t.Fatalf("absent field was synthesized: %s", mustMarshalJSON(t, absent))
	}
}

func TestMessageUnknownFieldsSurviveDeepCopyAndAnyRoundTrip(t *testing.T) {
	raw := []byte(`[{"role":"user","content":[{"type":"text","text":"keep"}],"isMeta":true,"agent_id":null,"future":{"items":[1,2,3]}}]`)
	var messages []Message
	if err := json.Unmarshal(raw, &messages); err != nil {
		t.Fatalf("unmarshal messages: %v", err)
	}

	copied := deepCopyMessages(messages)
	if copied == nil {
		t.Fatal("deepCopyMessages returned nil")
	}
	assertJSONEquivalent(t, mustMarshalJSON(t, copied), raw)

	roundTripped := anyToMessages(messagesToAny(messages))
	assertJSONEquivalent(t, mustMarshalJSON(t, roundTripped), raw)
}

func TestMessageUnknownFieldsFrozenRoundTripAndHashSensitivity(t *testing.T) {
	decode := func(raw string) Message {
		t.Helper()
		var message Message
		if err := json.Unmarshal([]byte(raw), &message); err != nil {
			t.Fatalf("unmarshal message: %v", err)
		}
		return message
	}
	withMetadata := decode(`{"role":"user","content":"prefix","isMeta":true,"future":null}`)
	withoutMetadata := decode(`{"role":"user","content":"prefix"}`)
	withJSON := mustMarshalJSON(t, []Message{withMetadata})
	withoutJSON := mustMarshalJSON(t, []Message{withoutMetadata})
	if sha256hex(withJSON) == sha256hex(withoutJSON) {
		t.Fatal("frozen prefix hash ignored unknown message-level fields")
	}

	current := []Message{
		withMetadata,
		{Role: "assistant", Content: json.RawMessage(`"boundary"`)},
	}
	frozen := NewFrozenStubs()
	frozen.Store("thread", current[:1], 2, current[1], 10, 20)
	result := frozen.Get("thread", current)
	if result == nil {
		t.Fatal("expected frozen round-trip result")
	}
	assertJSONEquivalent(t, mustMarshalJSON(t, result.Messages), withJSON)
}

func mustMarshalJSON(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal JSON: %v", err)
	}
	return data
}

func assertJSONEquivalent(t *testing.T, got, want []byte) {
	t.Helper()
	var gotValue any
	if err := json.Unmarshal(got, &gotValue); err != nil {
		t.Fatalf("decode got JSON: %v\n%s", err, got)
	}
	var wantValue any
	if err := json.Unmarshal(want, &wantValue); err != nil {
		t.Fatalf("decode want JSON: %v\n%s", err, want)
	}
	if !reflect.DeepEqual(gotValue, wantValue) {
		t.Fatalf("JSON differs\ngot:  %s\nwant: %s", got, want)
	}
}

// ---- buildToolUseKeywordMap tests ----

func TestBuildToolUseKeywordMapBasic(t *testing.T) {
	messages := []Message{
		{Role: "assistant", Content: mustMarshalBlocks([]ContentBlock{
			{Type: "tool_use", ID: "tool_1", Name: "Read", Input: map[string]any{"file_path": "/app/main.go"}},
		})},
		{Role: "assistant", Content: mustMarshalBlocks([]ContentBlock{
			{Type: "tool_use", ID: "tool_2", Name: "Edit", Input: map[string]any{"file_path": "/app/config.go"}},
		})},
	}

	m := buildToolUseKeywordMap(messages)

	if len(m) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(m))
	}
	if m["tool_1"] != "Read /app/main.go" {
		t.Errorf("tool_1: expected 'Read /app/main.go', got %q", m["tool_1"])
	}
	if m["tool_2"] != "Edit /app/config.go" {
		t.Errorf("tool_2: expected 'Edit /app/config.go', got %q", m["tool_2"])
	}
}

func TestBuildToolUseKeywordMapEmptyMessages(t *testing.T) {
	m := buildToolUseKeywordMap(nil)
	if len(m) != 0 {
		t.Errorf("expected empty map for nil messages, got %d entries", len(m))
	}
}

func TestBuildToolUseKeywordMapNoTools(t *testing.T) {
	messages := []Message{
		{Role: "user", Content: json.RawMessage(`"just text"`)},
		{Role: "assistant", Content: json.RawMessage(`"reply"`)},
	}
	m := buildToolUseKeywordMap(messages)
	if len(m) != 0 {
		t.Errorf("expected empty map for no tools, got %d entries", len(m))
	}
}

// ---- stubToolResults with deep_search tests ----

func TestStubToolResultsWithDeepSearch(t *testing.T) {
	toolKWMap := map[string]string{
		"tool_1": "Read /app/main.go",
		"tool_2": "Bash go build",
	}
	blocks := []ContentBlock{
		{Type: "tool_result", ToolUseID: "tool_1", Content: "file content..."},
		{Type: "tool_result", ToolUseID: "tool_2", Content: "build output"},
		{Type: "tool_result", ToolUseID: "tool_3", Content: "no match"},
		{Type: "text", Text: "plain text"},
	}

	result := stubToolResults(blocks, toolKWMap)

	if len(result) != 4 {
		t.Fatalf("expected 4 blocks, got %d", len(result))
	}
	// Block 0: matched tool_1 → deep_search hint
	if result[0].Type != "text" {
		t.Error("block 0 should be text")
	}
	if !strings.Contains(result[0].Text, "[tool result archived]") {
		t.Errorf("block 0 should be tool result stub, got: %s", result[0].Text)
	}
	if !strings.Contains(result[0].Text, "deep_search('Read /app/main.go')") {
		t.Errorf("block 0 should have deep_search hint, got: %s", result[0].Text)
	}
	// Block 1: matched tool_2 → deep_search hint
	if !strings.Contains(result[1].Text, "deep_search('Bash go build')") {
		t.Errorf("block 1 should have deep_search hint, got: %s", result[1].Text)
	}
	// Block 2: no match → no deep_search hint
	if result[2].Text != "[tool result archived]" {
		t.Errorf("block 2 should be plain stub (no match), got: %s", result[2].Text)
	}
	// Block 3: plain text → unchanged
	if result[3].Type != "text" || result[3].Text != "plain text" {
		t.Error("block 3 should be unchanged plain text")
	}
}

func TestStubToolResultsNilMap(t *testing.T) {
	blocks := []ContentBlock{
		{Type: "tool_result", ToolUseID: "tool_1", Content: "data"},
	}
	result := stubToolResults(blocks, nil)
	if !strings.Contains(result[0].Text, "[tool result archived]") {
		t.Errorf("should work with nil map, got: %s", result[0].Text)
	}
	if strings.Contains(result[0].Text, "deep_search") {
		t.Errorf("should NOT have deep_search hint with nil map, got: %s", result[0].Text)
	}
}

// ---- stubToolUses with deep_search tests ----

func TestStubToolUsesWithDeepSearch(t *testing.T) {
	blocks := []ContentBlock{
		{Type: "tool_use", Name: "Read", Input: map[string]any{"file_path": "/app/main.go"}},
		{Type: "tool_use", Name: "Bash", Input: map[string]any{"command": "go build ./..."}},
		{Type: "tool_use", Name: "UnknownTool", Input: nil},
		{Type: "text", Text: "plain text"},
	}

	result := stubToolUses(blocks)

	if len(result) != 4 {
		t.Fatalf("expected 4 blocks, got %d", len(result))
	}
	// Block 0: Read → deep_search hint
	if !strings.Contains(result[0].Text, "[→]") {
		t.Errorf("block 0 should be tool_use stub, got: %s", result[0].Text)
	}
	if !strings.Contains(result[0].Text, "deep_search('Read /app/main.go')") {
		t.Errorf("block 0 should have deep_search hint, got: %s", result[0].Text)
	}
	// Block 1: Bash → deep_search hint
	if !strings.Contains(result[1].Text, "deep_search('Bash go build") {
		t.Errorf("block 1 should have deep_search hint, got: %s", result[1].Text)
	}
	// Block 2: UnknownTool → deep_search hint (just tool name)
	if !strings.Contains(result[2].Text, "deep_search('UnknownTool')") {
		t.Errorf("block 2 should have deep_search hint with tool name, got: %s", result[2].Text)
	}
	// Block 3: plain text → unchanged
	if result[3].Type != "text" || result[3].Text != "plain text" {
		t.Error("block 3 should be unchanged")
	}
}

func TestStubToolUsesWithoutKeywords(t *testing.T) {
	// Tool with no extractable keywords (empty name)
	blocks := []ContentBlock{
		{Type: "tool_use", Name: "", Input: map[string]any{}},
	}
	result := stubToolUses(blocks)
	// Should still produce a stub, but no deep_search hint since extractToolKeywords returns ""
	if len(result) != 1 {
		t.Fatalf("expected 1 block, got %d", len(result))
	}
	if strings.Contains(result[0].Text, "deep_search") {
		t.Errorf("should NOT have deep_search hint for empty keywords, got: %s", result[0].Text)
	}
}

// ---- extractSearchHintsFromStubs tests ----

func TestExtractSearchHintsFromStubs(t *testing.T) {
	messages := []Message{
		{Role: "assistant", Content: json.RawMessage(`"some text"`)},
		{Role: "user", Content: json.RawMessage(`"[→] Read /app/main.go → deep_search('Read /app/main.go')"`)},
		{Role: "assistant", Content: mustMarshalBlocks([]ContentBlock{
			{Type: "text", Text: "[tool result archived] → deep_search('Bash go build')"},
		})},
	}

	keywords := extractSearchHintsFromStubs(messages)

	if len(keywords) == 0 {
		t.Fatal("expected keywords from deep_search hints")
	}
	// Should contain words from both hints
	hasRead := false
	hasBash := false
	for _, kw := range keywords {
		if strings.Contains(kw, "Read") {
			hasRead = true
		}
		if strings.Contains(kw, "Bash") {
			hasBash = true
		}
	}
	if !hasRead {
		t.Errorf("expected 'Read' keyword, got: %v", keywords)
	}
	if !hasBash {
		t.Errorf("expected 'Bash' keyword, got: %v", keywords)
	}
}

func TestExtractSearchHintsNoHints(t *testing.T) {
	messages := []Message{
		{Role: "user", Content: json.RawMessage(`"plain text without hints"`)},
	}
	keywords := extractSearchHintsFromStubs(messages)
	if len(keywords) != 0 {
		t.Errorf("expected 0 keywords for no hints, got %d: %v", len(keywords), keywords)
	}
}

func TestExtractSearchHintsDeduplication(t *testing.T) {
	messages := []Message{
		{Role: "user", Content: json.RawMessage(`"→ deep_search('Read main.go')"`)},
		{Role: "assistant", Content: json.RawMessage(`"→ deep_search('Read main.go')"`)},
	}
	keywords := extractSearchHintsFromStubs(messages)
	// Count occurrences of "Read"
	count := 0
	for _, kw := range keywords {
		if kw == "Read" {
			count++
		}
	}
	if count > 1 {
		t.Errorf("expected deduplicated 'Read', got %d occurrences", count)
	}
}

// ---- extractFallbackKeywords tests ----

func TestExtractFallbackKeywords(t *testing.T) {
	input := map[string]any{
		"description": "a very long description that should be included if under 50 chars",
		"thinking":    "skip me",
		"signature":   "skip me too",
		"command":     "go build",
		"count":       42,
	}
	result := extractFallbackKeywords("CustomTool", input)
	if !strings.HasPrefix(result, "CustomTool ") {
		t.Errorf("should start with tool name, got: %s", result)
	}
	// "go build" should be included (string, 3-50 chars)
	if !strings.Contains(result, "go build") {
		t.Errorf("should contain 'go build', got: %s", result)
	}
	// "skip me" should NOT be included (thinking key is filtered)
	if strings.Contains(result, "skip me") {
		t.Errorf("should NOT contain thinking value, got: %s", result)
	}
}

func TestExtractFallbackKeywordsEmptyInput(t *testing.T) {
	result := extractFallbackKeywords("EmptyTool", nil)
	if result != "EmptyTool" {
		t.Errorf("expected just tool name for nil input, got: %s", result)
	}
}

func TestExtractFallbackKeywordsAllFiltered(t *testing.T) {
	// 所有字段都被过滤 → 只返回 tool name
	input := map[string]any{
		"thinking":      "thought content",
		"signature":     "sig123",
		"cache_control": "ephemeral",
		"short":         "ab",                    // < 3 chars, filtered
		"toolong":       strings.Repeat("x", 51), // > 50 chars, filtered
	}
	result := extractFallbackKeywords("MyTool", input)
	if result != "MyTool" {
		t.Errorf("expected just 'MyTool' when all fields filtered, got: %s", result)
	}
}

// ---- stubifyMessages end-to-end with deep_search ----

func TestStubifyMessagesDeepSearchIntegration(t *testing.T) {
	tc, _ := NewTokenCounter()
	if tc == nil {
		t.Skip("token counter not available")
	}

	// 构造真实消息序列：assistant 发起 Read，user 返回 tool_result
	messages := []Message{
		{Role: "user", Content: json.RawMessage(`"hello"`)},
		{Role: "assistant", Content: mustMarshalBlocks([]ContentBlock{
			{Type: "tool_use", ID: "toolu_001", Name: "Read", Input: map[string]any{"file_path": "/app/main.go"}},
		})},
		{Role: "user", Content: mustMarshalBlocks([]ContentBlock{
			{Type: "tool_result", ToolUseID: "toolu_001", Content: "package main\n\nfunc main() {\n\tfmt.Println(\"hello\")\n}"},
		})},
	}

	// stubify with small threshold to force stubbing
	stubbed, _ := stubifyMessages(messages, tc, "", 0, false, nil, "test", 1, 0.0, 0)

	// 验证 tool_use stub 包含 deep_search 提示
	toolUseFound := false
	toolResultFound := false
	for _, msg := range stubbed {
		blocks, _ := parseContent(msg.Content)
		for _, b := range blocks {
			if b.Type == "text" {
				if strings.Contains(b.Text, "[→] Read") {
					toolUseFound = true
					if !strings.Contains(b.Text, "deep_search('Read /app/main.go')") {
						t.Errorf("tool_use stub missing deep_search hint: %s", b.Text)
					}
				}
				if strings.Contains(b.Text, "[tool result archived]") {
					toolResultFound = true
					if !strings.Contains(b.Text, "deep_search('Read /app/main.go')") {
						t.Errorf("tool_result stub missing deep_search hint: %s", b.Text)
					}
				}
			}
		}
	}
	if !toolUseFound {
		t.Error("expected tool_use stub in output")
	}
	if !toolResultFound {
		t.Error("expected tool_result stub in output")
	}
}

// ---- sanitizeDSKeyword tests ----

func TestSanitizeDSKeyword(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"Bash echo test", "Bash echo test"},           // 无特殊字符
		{"Bash echo \"')\"", "Bash echo \" )\""},       // '') 被替换为 ' )'
		{"grep ')' pattern ')", "grep  )' pattern  )"}, // 多处 '')
		{"", ""}, // 空字符串
		{"Read /app/main.go", "Read /app/main.go"}, // 正常路径
	}
	for _, tt := range tests {
		result := sanitizeDSKeyword(tt.input)
		if result != tt.expected {
			t.Errorf("sanitizeDSKeyword(%q) = %q, want %q", tt.input, result, tt.expected)
		}
	}
}

// ---- dedupKeywords tests ----

func TestDedupKeywords(t *testing.T) {
	input := []string{"Read", "Read", "Bash", "", "Edit", "Bash"}
	result := dedupKeywords(input)
	expected := []string{"Read", "Bash", "Edit"}
	if len(result) != len(expected) {
		t.Fatalf("expected %d items, got %d: %v", len(expected), len(result), result)
	}
	for i, kw := range expected {
		if result[i] != kw {
			t.Errorf("index %d: expected %q, got %q", i, kw, result[i])
		}
	}
}

func TestDedupKeywordsEmpty(t *testing.T) {
	result := dedupKeywords(nil)
	if len(result) != 0 {
		t.Errorf("expected empty for nil, got %d", len(result))
	}
	result = dedupKeywords([]string{})
	if len(result) != 0 {
		t.Errorf("expected empty for empty slice, got %d", len(result))
	}
}

// ---- helpers ----

func mustMarshalBlocks(blocks []ContentBlock) json.RawMessage {
	data, err := json.Marshal(blocks)
	if err != nil {
		panic(fmt.Sprintf("failed to marshal blocks: %v", err))
	}
	return data
}

// ---- Gap B: 阈值门控测试 ----

func TestStubifyThresholdGuardZero(t *testing.T) {
	// threshold=0 时不触发守卫（向后兼容）
	tc, _ := NewTokenCounter()
	if tc == nil {
		t.Skip("token counter not available")
	}
	messages := []Message{
		{Role: "user", Content: json.RawMessage(`"hello"`)},
		{Role: "assistant", Content: json.RawMessage(`"hi there, this is a response with enough tokens to be meaningful"`)},
	}
	stubbed, _ := stubifyMessages(messages, tc, "", 0, false, nil, "test", 1, 0.0, 0)
	if len(stubbed) != len(messages) {
		t.Errorf("stubify with threshold=0 should not change message count: got %d, want %d", len(stubbed), len(messages))
	}
}

func TestStubifyThresholdGuardActive(t *testing.T) {
	// threshold > tokens → 提前返回（不 stub）
	tc, _ := NewTokenCounter()
	if tc == nil {
		t.Skip("token counter not available")
	}
	messages := []Message{
		{Role: "user", Content: json.RawMessage(`"hello"`)},
	}
	originalTokens := tc.CountMessagesTokens(messages)
	// threshold 远大于实际 token
	stubbed, stats := stubifyMessages(messages, tc, "", 0, false, nil, "test", 1, 0.0, 100000)
	if stats.StubbedTokens != stats.OriginalTokens {
		t.Errorf("guard should return early with StubbedTokens == OriginalTokens")
	}
	if stats.OriginalTokens != originalTokens {
		t.Errorf("OriginalTokens should be %d, got %d", originalTokens, stats.OriginalTokens)
	}
	if len(stubbed) != len(messages) {
		t.Errorf("messages should be unchanged, got %d want %d", len(stubbed), len(messages))
	}
}

func TestStubifyThresholdGuardInactive(t *testing.T) {
	// threshold < tokens → 正常 stub
	tc, _ := NewTokenCounter()
	if tc == nil {
		t.Skip("token counter not available")
	}
	messages := []Message{
		{Role: "user", Content: json.RawMessage(`"hello"`)},
		{Role: "assistant", Content: json.RawMessage(`"a longer response that will exceed the threshold"`)},
	}
	// threshold=1 远小于实际 token → 不触发守卫
	_, stats := stubifyMessages(messages, tc, "", 0, false, nil, "test", 1, 0.0, 1)
	// 消息数 > 1，至少 messages[0] 之后的会被 stub
	if stats.OriginalTokens <= 1 {
		t.Skip("test messages too short for meaningful threshold test")
	}
	// 守卫不应触发
}

// ---- Gap C: 80-token 硬底测试 ----

func TestStubify80TokenFloorShortText(t *testing.T) {
	// 短纯文本消息不应被 stub
	tc, _ := NewTokenCounter()
	if tc == nil {
		t.Skip("token counter not available")
	}
	// 构造一条短消息（"ok" 远小于 80 tokens）
	messages := []Message{
		{Role: "user", Content: json.RawMessage(`"hello"`)}, // messages[0] 受保护
		{Role: "assistant", Content: json.RawMessage(`"ok"`)},
		{Role: "user", Content: json.RawMessage(`"yes"`)},
	}
	stubbed, _ := stubifyMessages(messages, tc, "", 0, false, nil, "test", 1, 0.0, 0)
	// "ok" 消息（~8 tokens）不应被 stub——应原样保留
	found := false
	for _, msg := range stubbed {
		if msg.Role == "assistant" {
			blocks, _ := parseContent(msg.Content)
			for _, b := range blocks {
				if b.Type == "text" && strings.Contains(b.Text, "ok") {
					found = true
				}
			}
		}
	}
	if !found {
		t.Error("short 'ok' message should be preserved by 80-token guard")
	}
}

func TestStubify80TokenFloorShortWithToolUse(t *testing.T) {
	// 短但含 tool_use 的消息仍需 stub
	tc, _ := NewTokenCounter()
	if tc == nil {
		t.Skip("token counter not available")
	}
	messages := []Message{
		{Role: "user", Content: json.RawMessage(`"hello"`)},
		{Role: "assistant", Content: mustMarshalBlocks([]ContentBlock{
			{Type: "tool_use", ID: "tu_1", Name: "Read", Input: map[string]any{"file_path": "/x.go"}},
		})},
	}
	stubbed, _ := stubifyMessages(messages, tc, "", 0, false, nil, "test", 1, 0.0, 0)
	// tool_use 消息即使短也应被 stub
	toolStubbed := false
	for _, msg := range stubbed {
		blocks, _ := parseContent(msg.Content)
		for _, b := range blocks {
			if b.Type == "text" && strings.Contains(b.Text, "[→] Read") {
				toolStubbed = true
			}
		}
	}
	if !toolStubbed {
		t.Error("short tool_use message should still be stubbed despite 80-token guard")
	}
}

// ---- Gap A: Tool stub 格式测试 ----

func TestStubToolUsesKnownTools(t *testing.T) {
	tests := []struct {
		name    string
		block   ContentBlock
		contain string
	}{
		{"Read", ContentBlock{Type: "tool_use", Name: "Read", Input: map[string]any{"file_path": "/app/main.go"}}, "[→] Read /app/main.go"},
		{"Edit", ContentBlock{Type: "tool_use", Name: "Edit", Input: map[string]any{"file_path": "/app/config.go"}}, "[→] Edit /app/config.go -- file on disk is current"},
		{"Write", ContentBlock{Type: "tool_use", Name: "Write", Input: map[string]any{"file_path": "/app/out.go"}}, "[→] Write /app/out.go -- file on disk is current"},
		{"Bash", ContentBlock{Type: "tool_use", Name: "Bash", Input: map[string]any{"command": "go test ./..."}}, "[→] Bash: go test ./..."},
		{"Grep", ContentBlock{Type: "tool_use", Name: "Grep", Input: map[string]any{"pattern": "TODO", "path": "."}}, "[→] Grep 'TODO' in ."},
		{"Glob", ContentBlock{Type: "tool_use", Name: "Glob", Input: map[string]any{"pattern": "*.go"}}, "[→] Glob '*.go'"},
		{"Agent", ContentBlock{Type: "tool_use", Name: "Agent", Input: map[string]any{"description": "fix bugs"}}, "[→] Agent: fix bugs"},
		{"WebSearch", ContentBlock{Type: "tool_use", Name: "WebSearch", Input: map[string]any{"query": "golang errors"}}, "[→] WebSearch 'golang errors'"},
		{"WebFetch", ContentBlock{Type: "tool_use", Name: "WebFetch", Input: map[string]any{"url": "https://pkg.go.dev"}}, "[→] WebFetch https://pkg.go.dev"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := stubToolUses([]ContentBlock{tt.block})
			if len(result) != 1 {
				t.Fatalf("expected 1 block, got %d", len(result))
			}
			if result[0].Type != "text" {
				t.Errorf("expected text block, got %s", result[0].Type)
			}
			if !strings.Contains(result[0].Text, tt.contain) {
				t.Errorf("expected %q in stub text, got: %s", tt.contain, result[0].Text)
			}
		})
	}
}

func TestStubToolUsesUnknownToolFallback(t *testing.T) {
	// 未知工具类型 fallback 到通用格式
	block := ContentBlock{Type: "tool_use", Name: "CustomTool", Input: map[string]any{"key": "value"}}
	result := stubToolUses([]ContentBlock{block})
	if len(result) != 1 {
		t.Fatalf("expected 1 block, got %d", len(result))
	}
	if !strings.Contains(result[0].Text, "[→] CustomTool") {
		t.Errorf("unknown tool should use generic format, got: %s", result[0].Text)
	}
}

func TestStubToolUsesPreservesNonToolBlocks(t *testing.T) {
	blocks := []ContentBlock{
		{Type: "text", Text: "keep me"},
		{Type: "tool_use", Name: "Read", Input: map[string]any{"file_path": "/x.go"}},
		{Type: "text", Text: "keep me too"},
	}
	result := stubToolUses(blocks)
	if len(result) != 3 {
		t.Fatalf("expected 3 blocks, got %d", len(result))
	}
	if result[0].Type != "text" || result[0].Text != "keep me" {
		t.Error("non-tool_use block should be preserved at index 0")
	}
	if result[2].Type != "text" || result[2].Text != "keep me too" {
		t.Error("non-tool_use block should be preserved at index 2")
	}
}

func TestStubToolUsesDeepSearchSuffix(t *testing.T) {
	// Read → 应有 deep_search 提示
	block := ContentBlock{Type: "tool_use", Name: "Read", Input: map[string]any{"file_path": "/app/main.go"}}
	result := stubToolUses([]ContentBlock{block})
	if !strings.Contains(result[0].Text, "deep_search") {
		t.Errorf("tool_use stub should have deep_search hint, got: %s", result[0].Text)
	}
}

// ---- truncateRunes 围栏截断测试 ----

func TestTruncateRunesFenceClosedAfterCut(t *testing.T) {
	// 截断点落在代码块中间 → 输出围栏计数应为偶数，且以闭合围栏结尾
	input := "前导文字\n```go\n" + strings.Repeat("x", 100) + "\n```\n结尾"
	result := truncateRunes(input, 50)
	if strings.Count(result, "```")%2 != 0 {
		t.Errorf("expected even fence count after cut inside code block, got %d in: %q",
			strings.Count(result, "```"), result)
	}
	if !strings.HasSuffix(result, "\n```") {
		t.Errorf("expected result to end with closing fence on its own line, got: %q", result)
	}
}

func TestTruncateRunesNoFenceUnchanged(t *testing.T) {
	// 不含围栏的文本截断行为与修复前逐字节一致
	input := strings.Repeat("a", 200)
	result := truncateRunes(input, 50)
	expected := string([]rune(input)[:50]) + "…"
	if result != expected {
		t.Errorf("no-fence truncation changed: expected %q, got %q", expected, result)
	}
}

func TestTruncateRunesNoCutReturnsAsIs(t *testing.T) {
	// 未触发截断 → 原样返回，即使自带奇数个围栏也不修改
	input := "孤立围栏 ```"
	result := truncateRunes(input, 50)
	if result != input {
		t.Errorf("untruncated input should be returned as-is, got: %q", result)
	}
}

func TestTruncateRunesCutAfterClosedBlockNoAppend(t *testing.T) {
	// 完整闭合代码块之后截断 → 围栏计数已为偶数，不追加围栏
	input := "```go\ncode\n```\n" + strings.Repeat("t", 100)
	result := truncateRunes(input, 50)
	if !strings.HasSuffix(result, "…") {
		t.Errorf("cut in plain tail should end with ellipsis, got: %q", result)
	}
	if strings.Count(result, "```")%2 != 0 {
		t.Errorf("expected even fence count (no append needed), got %d in: %q",
			strings.Count(result, "```"), result)
	}
}

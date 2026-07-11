package proxy

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func TestCalcCollapseCutoffRespectsTokenFloorAndKeepRecent(t *testing.T) {
	tc := mustTokenCounter(t)
	messages := collapseTextMessages(12, 80)
	keepRecent := 4
	tokenFloor := tc.CountMessagesTokens(messages[8:])

	cutoff := CalcCollapseCutoff(messages, tokenFloor, tc, keepRecent)
	if cutoff < 1 {
		t.Fatalf("cutoff = %d, want a collapsible prefix", cutoff)
	}
	if cutoff > len(messages)-keepRecent {
		t.Fatalf("cutoff = %d preserves fewer than %d recent messages", cutoff, keepRecent)
	}
	if got := tc.CountMessagesTokens(messages[cutoff:]); got < tokenFloor {
		t.Fatalf("retained tail tokens = %d, want at least %d", got, tokenFloor)
	}
}

func TestCalcCollapseCutoffKeepsToolPairWithoutViolatingKeepRecent(t *testing.T) {
	tc := mustTokenCounter(t)
	messages := collapseTextMessages(8, 40)
	messages[4] = Message{
		Role: "assistant",
		Content: rebuildContent([]ContentBlock{{
			Type:  "tool_use",
			ID:    "tool-1",
			Name:  "Read",
			Input: map[string]any{"file_path": "internal/proxy/frozen.go"},
		}}, true),
	}
	messages[5] = Message{
		Role: "user",
		Content: rebuildContent([]ContentBlock{{
			Type:      "tool_result",
			ToolUseID: "tool-1",
			Content:   "ok",
		}}, true),
	}
	keepRecent := 3
	tokenFloor := tc.CountMessagesTokens(messages[5:])

	cutoff := CalcCollapseCutoff(messages, tokenFloor, tc, keepRecent)
	if cutoff > len(messages)-keepRecent {
		t.Fatalf("cutoff = %d violates keepRecent=%d", cutoff, keepRecent)
	}
	if cutoff > 4 && cutoff <= 5 {
		t.Fatalf("cutoff = %d splits tool_use/tool_result pair at indexes 4-5", cutoff)
	}
}

func TestCollapseOldMessagesUsesOriginalArchiveAndModifiedRecentTail(t *testing.T) {
	tc := mustTokenCounter(t)
	modified := collapseTextMessages(10, 20)
	original := deepCopyMessages(modified)
	original[2] = Message{
		Role: "assistant",
		Content: rebuildContent([]ContentBlock{{
			Type:  "tool_use",
			ID:    "tool-original",
			Name:  "Read",
			Input: map[string]any{"file_path": "internal/proxy/original-only.go"},
		}}, true),
	}
	modified[2].Content = mustMarshal("[stubbed tool call]")

	result, block := CollapseOldMessages(modified, original, 6, tc, "session")
	if len(result) != 2+len(modified)-6 {
		t.Fatalf("collapsed message count = %d, want %d", len(result), 2+len(modified)-6)
	}
	if result[0].Role != modified[0].Role {
		t.Fatalf("blanked first role = %q, want %q", result[0].Role, modified[0].Role)
	}
	var blanked string
	if err := json.Unmarshal(result[0].Content, &blanked); err != nil {
		t.Fatalf("decode blanked first content: %v", err)
	}
	if !strings.Contains(blanked, "Earlier conversation archived") {
		t.Fatalf("blanked first content = %q", blanked)
	}
	if !strings.Contains(block.SummaryText, "original-only.go") {
		t.Fatalf("archive summary did not use original messages: %s", block.SummaryText)
	}
	if strings.Contains(block.SummaryText, "stubbed tool call") {
		t.Fatalf("archive summary used modified stub content: %s", block.SummaryText)
	}
	for i := 6; i < len(modified); i++ {
		got, _ := json.Marshal(result[2+i-6])
		want, _ := json.Marshal(modified[i])
		if string(got) != string(want) {
			t.Fatalf("recent tail message %d changed\ngot:  %s\nwant: %s", i, got, want)
		}
	}
}

func TestCollapseOldMessagesLargeSessionReducesMessagesAndTokens(t *testing.T) {
	tc := mustTokenCounter(t)
	messages := collapseTextMessages(320, 240)
	beforeTokens := tc.CountMessagesTokens(messages)

	result, _ := CollapseOldMessages(messages, messages, 300, tc, "session")
	afterTokens := tc.CountMessagesTokens(result)
	if len(result) != 22 {
		t.Fatalf("collapsed message count = %d, want 22", len(result))
	}
	if afterTokens*2 >= beforeTokens {
		t.Fatalf("token reduction insufficient: before=%d after=%d", beforeTokens, afterTokens)
	}
	for i := 300; i < 320; i++ {
		got, _ := json.Marshal(result[2+i-300])
		want, _ := json.Marshal(messages[i])
		if string(got) != string(want) {
			t.Fatalf("large-session recent tail message %d changed", i)
		}
	}
}

func TestCollapseOldMessagesRejectsMismatchedInputs(t *testing.T) {
	tc := mustTokenCounter(t)
	modified := collapseTextMessages(10001, 1)
	original := collapseTextMessages(3, 1)
	result, block := CollapseOldMessages(modified, original, 2, tc, "session")
	if len(result) != len(modified) {
		t.Fatalf("异常输入应原样返回 modified，got=%d want=%d", len(result), len(modified))
	}
	if block.ID != "" {
		t.Fatalf("异常输入不应创建 archive: %+v", block)
	}
}

func TestCollapseOldMessagesRejectsCutoffBeyondOriginal(t *testing.T) {
	tc := mustTokenCounter(t)
	modified := collapseTextMessages(5, 1)
	original := collapseTextMessages(2, 1)
	result, block := CollapseOldMessages(modified, original, 4, tc, "session")
	if len(result) != len(modified) || block.ID != "" {
		t.Fatalf("越界 cutoff 未 fail closed: len=%d block=%+v", len(result), block)
	}
}

func TestArchiveContentHashIsStableAndContentSensitive(t *testing.T) {
	messages := []Message{
		{Role: "user", Content: mustMarshal("same content")},
		{Role: "assistant", Content: mustMarshal("same answer")},
	}
	first, err := archiveContentHash(messages)
	if err != nil {
		t.Fatalf("archiveContentHash 第一次计算失败: %v", err)
	}
	second, err := archiveContentHash(deepCopyMessages(messages))
	if err != nil {
		t.Fatalf("archiveContentHash 第二次计算失败: %v", err)
	}
	if first == "" || first != second {
		t.Fatalf("相同消息 hash 不稳定: first=%q second=%q", first, second)
	}

	changed := deepCopyMessages(messages)
	changed[1].Content = mustMarshal("different answer")
	different, err := archiveContentHash(changed)
	if err != nil {
		t.Fatalf("archiveContentHash 变更正文计算失败: %v", err)
	}
	if different == first {
		t.Fatalf("不同正文得到相同 hash: %q", first)
	}

	block := buildArchiveBlock(messages, len(messages), mustTokenCounter(t), "session-hash")
	if block.ContentHash != first {
		t.Fatalf("buildArchiveBlock ContentHash=%q, want %q", block.ContentHash, first)
	}
}

func TestArchiveContentHashCanonicalizesJSONObjects(t *testing.T) {
	first := []Message{{Role: "assistant", Content: json.RawMessage(`[{"type":"tool_use","input":{"b":2,"a":1},"name":"Edit"}]`)}}
	second := []Message{{Role: "assistant", Content: json.RawMessage(` [ { "name":"Edit", "input": { "a":1, "b":2 }, "type":"tool_use" } ] `)}}
	firstHash, err := archiveContentHash(first)
	if err != nil {
		t.Fatal(err)
	}
	secondHash, err := archiveContentHash(second)
	if err != nil {
		t.Fatal(err)
	}
	if firstHash != secondHash {
		t.Fatalf("语义相同 JSON 的 hash 不一致: %s != %s", firstHash, secondHash)
	}
}

func mustTokenCounter(t *testing.T) *TokenCounter {
	t.Helper()
	tc, err := NewTokenCounter()
	if err != nil {
		t.Fatalf("NewTokenCounter: %v", err)
	}
	return tc
}

func collapseTextMessages(count, words int) []Message {
	messages := make([]Message, count)
	for i := range messages {
		role := "user"
		if i%2 == 1 {
			role = "assistant"
		}
		text := fmt.Sprintf("message-%03d %s", i, strings.Repeat("context ", words))
		messages[i] = Message{Role: role, Content: mustMarshal(text)}
	}
	return messages
}

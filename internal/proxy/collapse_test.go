package proxy

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"image/png"
	"os"
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

func TestCalcCollapseCutoffRejectsDegenerateFirstMessageOnlyCollapse(t *testing.T) {
	tc := mustTokenCounter(t)
	messages := collapseTextMessages(3, 20)
	tokenFloor := tc.CountMessagesTokens(messages[1:])

	if cutoff := CalcCollapseCutoff(messages, tokenFloor, tc, 0); cutoff != -1 {
		t.Fatalf("cutoff = %d, want -1；仅折叠首条消息不会缩短消息数组", cutoff)
	}
}

func TestCalcCollapseCutoffFinalGuardAfterBoundaryAdjustments(t *testing.T) {
	tc := mustTokenCounter(t)

	t.Run("keepRecent 将 cutoff 压回 1", func(t *testing.T) {
		messages := collapseTextMessages(4, 20)
		tokenFloor := tc.CountMessagesTokens(messages[2:])
		if cutoff := CalcCollapseCutoff(messages, tokenFloor, tc, 3); cutoff != -1 {
			t.Fatalf("cutoff = %d, want -1 after keepRecent adjustment", cutoff)
		}
	})

	t.Run("tool pair 后退将 cutoff 压回 1", func(t *testing.T) {
		messages := collapseTextMessages(4, 20)
		messages[1] = Message{Role: "assistant", Content: rebuildContent([]ContentBlock{{
			Type: "tool_use", ID: "tool-1", Name: "Read", Input: map[string]any{},
		}}, true)}
		messages[2] = Message{Role: "user", Content: rebuildContent([]ContentBlock{{
			Type: "tool_result", ToolUseID: "tool-1", Content: "ok",
		}}, true)}
		tokenFloor := tc.CountMessagesTokens(messages[2:])
		if cutoff := CalcCollapseCutoff(messages, tokenFloor, tc, 2); cutoff != -1 {
			t.Fatalf("cutoff = %d, want -1 after tool-pair retreat", cutoff)
		}
	})
}

func TestCalcCollapseCutoffNeverCrossesActiveToolPair(t *testing.T) {
	tc, err := NewTokenCounter()
	if err != nil {
		t.Fatal(err)
	}
	activeAssistant := Message{Role: "assistant", Content: json.RawMessage(`[{"type":"thinking","thinking":"signed","signature":"sig"},{"type":"tool_use","id":"active","name":"Read","input":{"file_path":"main.go"}}]`)}
	largeResult := Message{Role: "user", Content: mustMarshalBlocks([]ContentBlock{{Type: "tool_result", ToolUseID: "active", Content: strings.Repeat("large current result ", 2000)}})}

	noHistory := []Message{{Role: "user", Content: mustMarshal("start")}, activeAssistant, largeResult}
	if cutoff := CalcCollapseCutoff(noHistory, 100, tc, 0); cutoff != -1 {
		t.Fatalf("无安全历史前缀时 cutoff=%d, want -1", cutoff)
	}

	withHistory := []Message{
		{Role: "user", Content: mustMarshal("start")},
		{Role: "assistant", Content: mustMarshal(strings.Repeat("old history ", 200))},
		activeAssistant,
		largeResult,
	}
	if cutoff := CalcCollapseCutoff(withHistory, 100, tc, 0); cutoff != 2 {
		t.Fatalf("cutoff=%d, want active assistant index 2", cutoff)
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

func TestCollapseOldMessagesPersistentContextExcludedFromArchive(t *testing.T) {
	tc := mustTokenCounter(t)
	contextSentinel := "FICTIONAL_CONTEXT_SENTINEL"
	var contextMessage Message
	contextRaw := fmt.Sprintf(`{"role":"user","content":[{"type":"text","text":%q}],"isMeta":true,"agent_id":null}`, persistentReminder("claudeMd", contextSentinel))
	if err := json.Unmarshal([]byte(contextRaw), &contextMessage); err != nil {
		t.Fatalf("unmarshal context message: %v", err)
	}
	raw := append([]Message{contextMessage}, collapseTextMessages(8, 20)...)
	history, context := DetachPersistentUserContext(raw)
	if context == nil {
		t.Fatal("expected detached context")
	}

	const cutoff = 5
	collapsed, block := CollapseOldMessages(history, history, cutoff, tc, "session")
	result := PrependPersistentUserContext(collapsed, context)
	if len(result) != 1+2+len(history)-cutoff {
		t.Fatalf("assembled message count = %d", len(result))
	}
	if !strings.Contains(string(result[0].Content), contextSentinel) {
		t.Fatal("context is not first after assembly")
	}
	var blanked string
	if err := json.Unmarshal(result[1].Content, &blanked); err != nil || !strings.Contains(blanked, "Earlier conversation archived") {
		t.Fatalf("second message is not historical blank marker: %s", result[1].Content)
	}
	if persistentContextCount(result) != 1 {
		t.Fatalf("context was not attached exactly once: %s", mustMarshalJSON(t, result))
	}

	archiveJSON := string(mustMarshalJSON(t, block.Messages))
	if strings.Contains(block.SummaryText, contextSentinel) || strings.Contains(archiveJSON, contextSentinel) {
		t.Fatal("persistent context leaked into archive summary or messages")
	}
	for _, keyword := range block.Keywords {
		if strings.Contains(keyword.Word, contextSentinel) {
			t.Fatal("persistent context leaked into archive keywords")
		}
	}
	if block.MessageCount != cutoff || block.BlockRangeStart != 1 || block.BlockRangeEnd != cutoff-1 {
		t.Fatalf("archive coordinates include detached context: %+v", block)
	}
	wantHash, err := archiveContentHash(history[:cutoff])
	if err != nil {
		t.Fatal(err)
	}
	if block.ContentHash != wantHash {
		t.Fatalf("archive hash = %s, want detached-history hash %s", block.ContentHash, wantHash)
	}
	for i := cutoff; i < len(history); i++ {
		assertJSONEquivalent(t, mustMarshalJSON(t, result[3+i-cutoff]), mustMarshalJSON(t, history[i]))
	}
}

func TestCollapseOldMessagesUnknownFieldsRemainInRecentTail(t *testing.T) {
	tc := mustTokenCounter(t)
	messages := collapseTextMessages(8, 20)
	for i := 5; i < len(messages); i++ {
		raw := fmt.Sprintf(`{"role":%q,"content":%q,"isMeta":true,"future_index":%d,"future_null":null}`, messages[i].Role, fmt.Sprintf("tail-%d", i), i)
		if err := json.Unmarshal([]byte(raw), &messages[i]); err != nil {
			t.Fatalf("unmarshal tail message %d: %v", i, err)
		}
	}

	result, _ := CollapseOldMessages(messages, deepCopyMessages(messages), 5, tc, "session")
	for i := 5; i < len(messages); i++ {
		assertJSONEquivalent(t, mustMarshalJSON(t, result[2+i-5]), mustMarshalJSON(t, messages[i]))
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

func TestTokenCounterLargeScreenshotFixtureIsSanitizedAndVisualScale(t *testing.T) {
	tc := mustTokenCounter(t)
	message, imageData := loadLargeScreenshotFixture(t)
	if len(imageData) != 491776 {
		t.Fatalf("fixture base64 chars=%d, want 491776", len(imageData))
	}
	raw, err := base64.StdEncoding.DecodeString(imageData)
	if err != nil {
		t.Fatalf("fixture base64 无效: %v", err)
	}
	config, err := png.DecodeConfig(bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("fixture PNG 无效: %v", err)
	}
	if config.Width != 1920 || config.Height != 897 {
		t.Fatalf("fixture dimensions=%dx%d, want 1920x897", config.Width, config.Height)
	}

	got := tc.CountMessageTokens(message)
	if got < 2277 || got > 2500 {
		t.Fatalf("大截图估算=%d，期望视觉量级而非旧文本估算 344931", got)
	}
}

func TestCalcCollapseCutoffLargeScreenshotRetainsSemanticTokenFloor(t *testing.T) {
	tc := mustTokenCounter(t)
	messages := collapseTextMessages(40, 5000)
	messages[len(messages)-1], _ = loadLargeScreenshotFixture(t)

	const threshold = 150000
	const tokenFloor = 75000
	const keepRecent = 8
	if total := tc.CountMessagesTokens(messages); total <= threshold {
		t.Fatalf("回归会话总 token=%d，必须超过 threshold=%d", total, threshold)
	}

	cutoff := CalcCollapseCutoff(messages, tokenFloor, tc, keepRecent)
	if cutoff < 2 {
		t.Fatalf("cutoff=%d，期望可折叠前缀", cutoff)
	}
	keepRecentBoundary := len(messages) - keepRecent
	if cutoff >= keepRecentBoundary {
		t.Fatalf("cutoff=%d 退化为 keep_recent 边界=%d，截图再次主导折叠", cutoff, keepRecentBoundary)
	}
	if retained := tc.CountMessagesTokens(messages[cutoff:]); retained < tokenFloor {
		t.Fatalf("retained tail tokens=%d, want >=%d", retained, tokenFloor)
	}
}

func TestCalcCollapseCutoffUsesTokenCounterSingleEntryPoint(t *testing.T) {
	source, err := os.ReadFile("collapse.go")
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(source, []byte("func countMessageTokens(")) {
		t.Fatal("collapse.go 仍保留第二套 countMessageTokens 实现")
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

func TestArchiveContentHashIncludesMessageUnknownFieldStates(t *testing.T) {
	decode := func(raw string) Message {
		t.Helper()
		var message Message
		if err := json.Unmarshal([]byte(raw), &message); err != nil {
			t.Fatalf("unmarshal archive message: %v", err)
		}
		return message
	}
	base := []Message{decode(`{"role":"user","content":"same"}`)}
	baseHash, err := archiveContentHash(base)
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name    string
		message Message
	}{
		{name: "explicit null", message: decode(`{"role":"user","content":"same","future":null}`)},
		{name: "non-null value", message: decode(`{"role":"user","content":"same","future":{"mode":"strict"}}`)},
		{name: "known metadata", message: decode(`{"role":"user","content":"same","isMeta":true}`)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := archiveContentHash([]Message{tt.message})
			if err != nil {
				t.Fatal(err)
			}
			if got == baseHash {
				t.Fatal("message-level archive field state did not affect hash")
			}
		})
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

func loadLargeScreenshotFixture(t *testing.T) (Message, string) {
	t.Helper()
	raw, err := os.ReadFile("testdata/multimodal/large-screenshot-tool-result.json")
	if err != nil {
		t.Fatal(err)
	}
	var block map[string]any
	if err := json.Unmarshal(raw, &block); err != nil {
		t.Fatal(err)
	}
	nested, ok := block["content"].([]any)
	if !ok {
		t.Fatal("fixture tool_result.content 不是数组")
	}
	var imageData string
	for _, item := range nested {
		semantic, ok := item.(map[string]any)
		if !ok || semantic["type"] != "image" {
			continue
		}
		source, _ := semantic["source"].(map[string]any)
		imageData, _ = source["data"].(string)
	}
	if imageData == "" {
		t.Fatal("fixture 缺少 image source.data")
	}
	content, err := json.Marshal([]any{block})
	if err != nil {
		t.Fatal(err)
	}
	return Message{Role: "user", Content: content}, imageData
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

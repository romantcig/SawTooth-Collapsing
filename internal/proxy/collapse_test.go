package proxy

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"image/png"
	"os"
	"regexp"
	"strconv"
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

func TestExtractTimelineSkipsBracketPrefixedUserText(t *testing.T) {
	// CC 每轮注入的 system-reminder / task-notification 都以方括号开头，
	// 它们不是对话方向信号，不应写进归档 Timeline。对齐 YesMem collapse.go:277-279。
	const noiseText = "[Request interrupted by user]"

	t.Run("括号开头的合成文本不产生 U 条目", func(t *testing.T) {
		events := extractTimeline([]ContentBlock{{Type: "text", Text: noiseText}}, 3, "user")
		if len(events) != 0 {
			t.Fatalf("events = %v, want 0 条", events)
		}
	})

	t.Run("空文本仍不产生条目", func(t *testing.T) {
		events := extractTimeline([]ContentBlock{{Type: "text", Text: ""}}, 3, "user")
		if len(events) != 0 {
			t.Fatalf("events = %v, want 0 条", events)
		}
	})

	t.Run("被过滤的 user 消息里 tool 事件照常提取", func(t *testing.T) {
		blocks := []ContentBlock{
			{Type: "text", Text: noiseText},
			{Type: "tool_use", ID: "edit-1", Name: "Edit", Input: map[string]any{"file_path": "a/b.go"}},
		}
		events := extractTimeline(blocks, 3, "user")
		if len(events) != 1 {
			t.Fatalf("events = %v, want 仅 1 条 tool 事件", events)
		}
		if !strings.HasPrefix(events[0], "- [") || !strings.Contains(events[0], "Edit:") {
			t.Fatalf("tool 事件格式异常: %q", events[0])
		}
	})

	t.Run("普通 user 文本照旧产生条目", func(t *testing.T) {
		events := extractTimeline([]ContentBlock{{Type: "text", Text: "把折叠阈值调到 80%"}}, 3, "user")
		if len(events) != 1 {
			t.Fatalf("events = %v, want 1 条", events)
		}
		if !strings.Contains(events[0], "U: ") {
			t.Fatalf("普通 user 文本条目缺少 U: 标记: %q", events[0])
		}
	})
}

// collapsePlaceholderRe 抓取 blankFirstMessage 占位符里的两个数字。
var collapsePlaceholderRe = regexp.MustCompile(`(\d+) messages, ~(\d+) tokens`)

// archiveSummaryRangeRe / archiveSummaryCountRe 分别抓取归档摘要首行的区间与条数。
// 区间分隔符用 \D 匹配，避免测试依赖模板里的具体破折号字面量。
var (
	archiveSummaryRangeRe = regexp.MustCompile(`Archived messages (\d+)\D(\d+)`)
	archiveSummaryCountRe = regexp.MustCompile(`共 (\d+) 条`)
)

func parseCollapsePlaceholder(t *testing.T, content json.RawMessage) (msgCount, tokens int) {
	t.Helper()
	var placeholder string
	if err := json.Unmarshal(content, &placeholder); err != nil {
		t.Fatalf("decode 占位符失败: %v", err)
	}
	m := collapsePlaceholderRe.FindStringSubmatch(placeholder)
	if m == nil {
		t.Fatalf("占位符格式不可解析: %q", placeholder)
	}
	msgCount, _ = strconv.Atoi(m[1])
	tokens, _ = strconv.Atoi(m[2])
	return msgCount, tokens
}

func TestCollapseOldMessagesPlaceholderCountsArchivedRangeOnly(t *testing.T) {
	tc := mustTokenCounter(t)
	const cutoff = 6
	messages := collapseTextMessages(20, 20)

	result, _ := CollapseOldMessages(messages, messages, cutoff, tc, "session")
	gotCount, gotTokens := parseCollapsePlaceholder(t, result[0].Content)
	if gotCount != cutoff-1 {
		t.Fatalf("占位符消息数 = %d, want %d（只统计归档区间 [1,%d)）", gotCount, cutoff-1, cutoff)
	}
	wantTokens := 0
	for i := 1; i < cutoff; i++ {
		wantTokens += tc.CountMessageTokens(messages[i])
	}
	if gotTokens != wantTokens {
		t.Fatalf("占位符 token 数 = %d, want %d", gotTokens, wantTokens)
	}

	// 保留的 recent tail 换成超大消息后，占位符两个数字必须纹丝不动。
	fattened := deepCopyMessages(messages)
	for i := cutoff; i < len(fattened); i++ {
		fattened[i].Content = mustMarshal(strings.Repeat("huge tail content ", 500))
	}
	fatResult, _ := CollapseOldMessages(fattened, fattened, cutoff, tc, "session")
	fatCount, fatTokens := parseCollapsePlaceholder(t, fatResult[0].Content)
	if fatCount != gotCount || fatTokens != gotTokens {
		t.Fatalf("保留的 tail 被计入占位符: count %d→%d, tokens %d→%d", gotCount, fatCount, gotTokens, fatTokens)
	}
}

func TestBuildArchiveBlockSummaryRangeMatchesMessageCount(t *testing.T) {
	tc := mustTokenCounter(t)
	const cutoff = 6
	messages := collapseTextMessages(20, 20)

	block := buildArchiveBlock(messages[:cutoff], cutoff, tc, "session")
	firstLine := strings.SplitN(block.SummaryText, "\n", 2)[0]

	rangeMatch := archiveSummaryRangeRe.FindStringSubmatch(firstLine)
	countMatch := archiveSummaryCountRe.FindStringSubmatch(firstLine)
	if rangeMatch == nil || countMatch == nil {
		t.Fatalf("摘要首行不可解析: %q", firstLine)
	}
	start, _ := strconv.Atoi(rangeMatch[1])
	end, _ := strconv.Atoi(rangeMatch[2])
	count, _ := strconv.Atoi(countMatch[1])
	if start != 0 || end != cutoff-1 || count != cutoff {
		t.Fatalf("摘要首行数字 = [%d,%d] 共 %d 条, want [0,%d] 共 %d 条: %q", start, end, count, cutoff-1, cutoff, firstLine)
	}
	if end-start+1 != count {
		t.Fatalf("摘要首行范围与条数不自洽: [%d,%d] 共 %d 条", start, end, count)
	}

	// 显示层坐标改动不得外溢到 DB 幂等边界 idx_archive_blocks_content_identity。
	if block.BlockRangeStart != 1 || block.BlockRangeEnd != cutoff-1 || block.MessageCount != cutoff {
		t.Fatalf("archive 字段层坐标被改动: start=%d end=%d count=%d", block.BlockRangeStart, block.BlockRangeEnd, block.MessageCount)
	}
}

// archiveSectionBodyLines 返回归档摘要中指定段标题之后、下一个段标题之前的非空行。
// 段定位方式与 reexpand.go 的 truncateSummaryText 一致（"\n### <title>\n"）。
func archiveSectionBodyLines(t *testing.T, text, title string) []string {
	t.Helper()
	token := "\n### " + title + "\n"
	idx := strings.Index(text, token)
	if idx < 0 {
		t.Fatalf("摘要缺少段标题 %q:\n%s", token, text)
	}
	body := text[idx+len(token):]
	if next := strings.Index(body, "\n### "); next >= 0 {
		body = body[:next]
	}
	var lines []string
	for _, line := range strings.Split(body, "\n") {
		if strings.TrimSpace(line) != "" {
			lines = append(lines, line)
		}
	}
	return lines
}

// W2-1：Tools Used 段聚合成一行计数，不再逐个列 tool stub。
func TestFormatArchiveBlockTextAggregatesTools(t *testing.T) {
	tools := []string{"Read", "Bash", "Read", "Edit", "Bash", "Read"}
	text := formatArchiveBlockText(0, 5, 6, 400, tools, nil, nil, nil, nil, "")

	// 段标题字面量是 truncateSummaryText 的解析锚点，必须原样保留。
	if !strings.Contains(text, "### Tools Used") {
		t.Fatalf("Tools Used 段标题丢失:\n%s", text)
	}
	lines := archiveSectionBodyLines(t, text, "Tools Used")
	if len(lines) != 1 {
		t.Fatalf("Tools Used 段正文 = %d 行, want 1 行:\n%v", len(lines), lines)
	}
	// 计数降序、同计数按 key 字母序（formatCompactionCounts 的既有语义）。
	if want := "Read(3), Bash(2), Edit(1)"; lines[0] != want {
		t.Fatalf("Tools Used 聚合行 = %q, want %q", lines[0], want)
	}
}

// W2-2：Files 段有条数上限，超出部分以一行省略标注收尾。
func TestFormatArchiveBlockTextCapsFileLines(t *testing.T) {
	buildFiles := func(n int) map[string]bool {
		files := make(map[string]bool, n)
		for i := 0; i < n; i++ {
			files[fmt.Sprintf("src/pkg/f%03d.go", i)] = true
		}
		return files
	}

	// 45 > maxArchiveFileLines(40)：40 条正文 + 1 行省略标注。
	over := formatArchiveBlockText(0, 5, 6, 400, nil, buildFiles(45), nil, nil, nil, "")
	overLines := archiveSectionBodyLines(t, over, "Files")
	if len(overLines) != 41 {
		t.Fatalf("45 个文件的 Files 段 = %d 行, want 41 行", len(overLines))
	}
	last := overLines[len(overLines)-1]
	if !strings.Contains(last, "5 more files omitted") {
		t.Fatalf("Files 段末行未标注省略数量: %q", last)
	}
	for _, line := range overLines[:40] {
		if strings.Contains(line, "omitted") {
			t.Fatalf("省略标注出现在正文区: %q", line)
		}
	}

	// 恰好 40 条：与改动前行为完全一致，不得多出任何行。
	exact := formatArchiveBlockText(0, 5, 6, 400, nil, buildFiles(40), nil, nil, nil, "")
	exactLines := archiveSectionBodyLines(t, exact, "Files")
	if len(exactLines) != 40 {
		t.Fatalf("40 个文件的 Files 段 = %d 行, want 40 行", len(exactLines))
	}
	if strings.Contains(exact, "files omitted") {
		t.Fatalf("未超限时不应出现省略标注:\n%s", exact)
	}
}

// W2-1 + W2-2：归档体积不随折叠量线性膨胀。
func TestBuildArchiveBlockVolumeBoundedForManyToolUses(t *testing.T) {
	tc := mustTokenCounter(t)
	const toolUseCount = 300
	const blocksPerMessage = 30

	var messages []Message
	for start := 0; start < toolUseCount; start += blocksPerMessage {
		blocks := make([]ContentBlock, 0, blocksPerMessage)
		for i := start; i < start+blocksPerMessage; i++ {
			blocks = append(blocks, ContentBlock{
				Type:  "tool_use",
				ID:    fmt.Sprintf("edit-%03d", i),
				Name:  "Edit",
				Input: map[string]any{"file_path": fmt.Sprintf("src/f%03d.go", i)},
			})
		}
		messages = append(messages, Message{Role: "assistant", Content: rebuildContent(blocks, true)})
	}

	block := buildArchiveBlock(messages, len(messages), tc, "session-volume")

	if !strings.Contains(block.SummaryText, "### Tools Used") {
		t.Fatalf("Tools Used 段标题丢失:\n%s", block.SummaryText)
	}
	toolLines := archiveSectionBodyLines(t, block.SummaryText, "Tools Used")
	if len(toolLines) != 1 {
		t.Fatalf("300 个 tool_use 的 Tools Used 段 = %d 行, want 1 行", len(toolLines))
	}
	fileLines := archiveSectionBodyLines(t, block.SummaryText, "Files")
	if len(fileLines) > 41 {
		t.Fatalf("300 个文件的 Files 段 = %d 行, want ≤ 41 行", len(fileLines))
	}
	if got := countRunes(block.SummaryText); got >= 8000 {
		t.Fatalf("300 个 tool_use 的归档 = %d runes, want < 8000", got)
	}
}

// W3-2：classifyEvent 的 key 推导矩阵。"" 表示该条目永不折叠。
func TestClassifyEventKeys(t *testing.T) {
	cases := []struct {
		event string
		want  string
	}{
		{"- [3] U: 改一下折叠阈值", ""},
		{"- [4] git commit", ""},
		{"- [5] Bash: rm -rf x", ""},
		{"- [6] build", "build"},
		{"- [7] test", "test"},
		{"- [8] Edit: a/b.go", "edit:a/b.go"},
		{"- [9] Skill: web-research", "skill:web-research"},
		{"- [10] TodoWrite", "todowrite"},
		{"event without index prefix", ""},
	}
	for _, tt := range cases {
		if got := classifyEvent(tt.event); got != tt.want {
			t.Errorf("classifyEvent(%q) = %q, want %q", tt.event, got, tt.want)
		}
	}
}

// W3-2：连续同 key 的条目 ≥3 折成一条，≤2 逐条保留，非连续互不影响。
func TestDeduplicateEventsCollapsesConsecutiveSameType(t *testing.T) {
	t.Run("连续 3 条折成一条", func(t *testing.T) {
		got := deduplicateEvents([]string{"- [1] test", "- [2] test", "- [3] test"})
		if len(got) != 1 {
			t.Fatalf("got = %v, want 1 条", got)
		}
		if !strings.Contains(got[0], "(3x)") {
			t.Fatalf("折叠行缺少计数标注: %q", got[0])
		}
	})

	t.Run("连续 2 条原样保留", func(t *testing.T) {
		in := []string{"- [1] test", "- [2] test"}
		got := deduplicateEvents(in)
		if len(got) != 2 || got[0] != in[0] || got[1] != in[1] {
			t.Fatalf("got = %v, want %v", got, in)
		}
	})

	t.Run("非连续同类不折叠", func(t *testing.T) {
		in := []string{"- [1] test", "- [2] build", "- [3] test"}
		got := deduplicateEvents(in)
		if len(got) != 3 {
			t.Fatalf("got = %v, want 3 条", got)
		}
	})

	t.Run("user steering 条目永不折叠", func(t *testing.T) {
		in := []string{"- [1] U: 继续", "- [2] U: 继续", "- [3] U: 继续"}
		got := deduplicateEvents(in)
		if len(got) != 3 {
			t.Fatalf("got = %v, want 3 条", got)
		}
	})

	t.Run("连续 3 条 git commit 永不折叠", func(t *testing.T) {
		in := []string{"- [1] git commit", "- [2] git commit", "- [3] git commit"}
		got := deduplicateEvents(in)
		if len(got) != 3 {
			t.Fatalf("got = %v, want 3 条", got)
		}
	})
}

// W3-2：连续同类事件在进入 120 条预算之前就被折成 "(Nx)"。
func TestBuildArchiveBlockTimelineCollapsesRepeatedEvents(t *testing.T) {
	tc := mustTokenCounter(t)
	const eventCount = 300
	const blocksPerMessage = 30

	var messages []Message
	for start := 0; start < eventCount; start += blocksPerMessage {
		blocks := make([]ContentBlock, 0, blocksPerMessage)
		for i := start; i < start+blocksPerMessage; i++ {
			blocks = append(blocks, ContentBlock{
				Type:  "tool_use",
				ID:    fmt.Sprintf("bash-%03d", i),
				Name:  "Bash",
				Input: map[string]any{"command": "go test ./..."},
			})
		}
		messages = append(messages, Message{Role: "assistant", Content: rebuildContent(blocks, true)})
	}

	block := buildArchiveBlock(messages, len(messages), tc, "session-timeline")
	lines := archiveSectionBodyLines(t, block.SummaryText, "Timeline")
	if len(lines) != 1 {
		t.Fatalf("300 条同类事件的 Timeline 段 = %d 行, want 1 行:\n%v", len(lines), lines)
	}
	if !strings.Contains(lines[0], "(300x)") {
		t.Fatalf("Timeline 折叠行缺少计数标注: %q", lines[0])
	}
	// 折叠必须发生在 120 条预算之前，否则会先被截断再折叠出 "events omitted"。
	if strings.Contains(block.SummaryText, "events omitted") {
		t.Fatalf("折叠发生在 120 条预算之后:\n%s", block.SummaryText)
	}
}

// W3-3：Gotchas 只收录 tool_result 且 IsError 的条目，并附工具名与错误首行。
func TestExtractGotchasOnlyToolResultErrors(t *testing.T) {
	// CC 的工具提示文本含 "fail" 字样，是提示而非经验教训，不得写进归档。
	noiseMsg := Message{Role: "user", Content: mustMarshalBlocks([]ContentBlock{{
		Type: "text",
		Text: "Calling this tool without the required parameter will fail with InputValidationError.",
	}})}
	if got := extractGotchas([]Message{noiseMsg}); len(got) != 0 {
		t.Fatalf("文本关键词仍被当成 gotcha: %v", got)
	}

	// 无配对 tool_use 时退化为 "tool"，有配对时带工具名与错误首行。
	loneError := Message{Role: "user", Content: mustMarshalBlocks([]ContentBlock{{
		Type: "tool_result", ToolUseID: "t1", IsError: true,
	}})}
	got := extractGotchas([]Message{loneError})
	if len(got) != 1 || got[0] != "tool" {
		t.Fatalf("孤立错误条目 = %v, want [tool]", got)
	}

	paired := []Message{
		{Role: "assistant", Content: mustMarshalBlocks([]ContentBlock{{
			Type: "tool_use", ID: "t1", Name: "Bash",
			Input: map[string]any{"command": "go test ./..."},
		}})},
		{Role: "user", Content: mustMarshalBlocks([]ContentBlock{{
			Type: "tool_result", ToolUseID: "t1", IsError: true,
			Content: "\nFAIL example.com/pkg\n\nstack trace continues",
		}})},
	}
	got = extractGotchas(paired)
	if len(got) != 1 || got[0] != "Bash: FAIL example.com/pkg" {
		t.Fatalf("配对错误条目 = %v, want [Bash: FAIL example.com/pkg]", got)
	}
}

// Commits 段跨消息配对：tool_use 与 tool_result 通过 id 关联，提取短哈希。
func TestExtractGitCommitsWithHash(t *testing.T) {
	withHash := []Message{
		{Role: "assistant", Content: mustMarshalBlocks([]ContentBlock{{
			Type: "tool_use", ID: "c1", Name: "Bash",
			Input: map[string]any{"command": `git commit -m "fix: boundary hash"`},
		}})},
		{Role: "user", Content: mustMarshalBlocks([]ContentBlock{{
			Type: "tool_result", ToolUseID: "c1",
			Content: "[main abc1234def] fix: boundary hash\n 2 files changed",
		}})},
	}
	got := extractGitCommits(withHash)
	if len(got) != 1 || got[0] != "abc1234def fix: boundary hash" {
		t.Fatalf("commit 条目 = %v, want [abc1234def fix: boundary hash]", got)
	}

	noResult := withHash[:1]
	got = extractGitCommits(noResult)
	if len(got) != 1 || got[0] != "fix: boundary hash" {
		t.Fatalf("无 result 时 commit 条目 = %v, want [fix: boundary hash]", got)
	}
}

// 时间线必须剥离 <system-reminder> 标签段：reminder 是当轮播报，
// 不是用户方向信号（CC 源码 K2e 剥离正则对应的标签形态）。
func TestExtractTimelineStripsSystemReminder(t *testing.T) {
	reminderOnly := []ContentBlock{{
		Type: "text",
		Text: "<system-reminder>Background task completed</system-reminder>",
	}}
	if got := extractTimeline(reminderOnly, 3, "user"); len(got) != 0 {
		t.Fatalf("reminder 独占消息未被过滤: %v", got)
	}

	mixed := []ContentBlock{{
		Type: "text",
		Text: "<system-reminder>Background task completed</system-reminder>\n请修复崩溃问题",
	}}
	got := extractTimeline(mixed, 3, "user")
	if len(got) != 1 || !strings.Contains(got[0], "请修复崩溃问题") || strings.Contains(got[0], "Background task") {
		t.Fatalf("混合消息应只保留用户文本: %v", got)
	}

	interrupted := []ContentBlock{{Type: "text", Text: "[Request interrupted by user]"}}
	if got := extractTimeline(interrupted, 3, "user"); len(got) != 0 {
		t.Fatalf("中断消息未被过滤: %v", got)
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

// ── Plan 06 Task 2：Collapse 只消费已提交的 canonical Archive ID ──

func TestCollapseArchiveCoverage(t *testing.T) {
	tc := mustTokenCounter(t)
	messages := collapseTextMessages(20, 80)
	original := deepCopyMessages(messages)
	cutoff := 12

	plan := PlanCollapse(messages, original, cutoff, tc, "collapse-archive-session")
	if !plan.Valid() {
		t.Fatal("合法 cutoff 应产生 collapse 意图")
	}
	if plan.Block.ID == "" || len(plan.Block.Messages) != cutoff {
		t.Fatalf("archive intent 无效: id=%q messages=%d", plan.Block.ID, len(plan.Block.Messages))
	}

	if _, ok := plan.Materialize(""); ok {
		t.Fatal("canonical ID 为空时不得物化折叠")
	}

	collapsed, ok := plan.Materialize("canonical-collapse-id")
	if !ok {
		t.Fatal("已落库 canonical ID 应可物化折叠")
	}
	if len(collapsed) != 2+len(messages)-cutoff {
		t.Fatalf("折叠后消息数=%d, want %d", len(collapsed), 2+len(messages)-cutoff)
	}
	marker := allText(t, collapsed[1])
	if !strings.Contains(marker, "recover('canonical-collapse-id')") {
		t.Fatalf("归档摘要缺少 canonical 恢复引用: %s", marker)
	}
	// marker 必须落在 Stage-1/2 截断裁不掉的受保护位置（archive 摘要消息本体）。
	if !strings.Contains(marker, "Archived messages") {
		t.Fatalf("恢复引用未与归档摘要同处一条受保护消息: %s", marker)
	}
	// 恢复引用只暴露不透明 ID，不得泄漏 session、路径或原文。
	for _, secret := range []string{"collapse-archive-session", "collapse-text-"} {
		if strings.Contains(marker, secret) {
			t.Fatalf("恢复引用泄漏 %q: %s", secret, marker)
		}
	}
}

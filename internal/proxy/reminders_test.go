package proxy

import (
	"encoding/json"
	"strings"
	"testing"
)

// countBlockType 解析消息 Content 并统计指定 Type 的 block 数量。
func countBlockType(t *testing.T, msg Message, blockType string) int {
	t.Helper()
	blocks, _ := parseContent(msg.Content)
	n := 0
	for _, b := range blocks {
		if b.Type == blockType {
			n++
		}
	}
	return n
}

// allText 解析消息 Content 并拼接所有 text block 的文本。
func allText(t *testing.T, msg Message) string {
	t.Helper()
	blocks, _ := parseContent(msg.Content)
	var sb strings.Builder
	for _, b := range blocks {
		if b.Type == "text" {
			sb.WriteString(b.Text)
		}
	}
	return sb.String()
}

func TestStripReminders(t *testing.T) {
	// 用例 1：旧消息含非 SessionStart 的 system-reminder → 被移除。
	t.Run("移除非SessionStart的system-reminder", func(t *testing.T) {
		input := []Message{
			{Role: "assistant", Content: json.RawMessage(`[{"type":"text","text":"前置回答\n<system-reminder>\nYour task tools haven't been used recently.\n</system-reminder>"}]`)},
			{Role: "user", Content: json.RawMessage(`"最新提问"`)},
		}
		out := StripReminders(input)
		got := allText(t, out[0])
		if strings.Contains(got, "<system-reminder>") {
			t.Errorf("旧消息的 system-reminder 应被移除，got=%q", got)
		}
		if !strings.Contains(got, "前置回答") {
			t.Errorf("真实文本应保留，got=%q", got)
		}
	})

	// 用例 2：旧 user 消息含 [skill-hint] → 被移除。
	t.Run("移除skill-hint", func(t *testing.T) {
		input := []Message{
			{Role: "user", Content: json.RawMessage(`[{"type":"text","text":"问题正文\n[skill-hint]建议使用某 skill[/skill-hint]"}]`)},
			{Role: "assistant", Content: json.RawMessage(`"回答"`)},
			{Role: "user", Content: json.RawMessage(`"最新提问"`)},
		}
		out := StripReminders(input)
		got := allText(t, out[0])
		if strings.Contains(got, "[skill-hint]") {
			t.Errorf("旧 user 的 skill-hint 应被移除，got=%q", got)
		}
		if !strings.Contains(got, "问题正文") {
			t.Errorf("真实文本应保留，got=%q", got)
		}
	})

	// 用例 3：末条 user 消息含 reminder → 内容逐字节完整保留。
	t.Run("末条user完整保留", func(t *testing.T) {
		lastContent := json.RawMessage(`[{"type":"text","text":"最新提问\n<system-reminder>task tools haven't been used recently</system-reminder>\n[skill-hint]hint[/skill-hint]"}]`)
		input := []Message{
			{Role: "assistant", Content: json.RawMessage(`"回答"`)},
			{Role: "user", Content: lastContent},
		}
		out := StripReminders(input)
		if string(out[1].Content) != string(lastContent) {
			t.Errorf("末条 user 应逐字节保留\n want=%s\n got =%s", lastContent, out[1].Content)
		}
	})

	// 用例 4：body 含 SessionStart: 的 system-reminder → 原样保留。
	t.Run("SessionStart保留", func(t *testing.T) {
		input := []Message{
			{Role: "user", Content: json.RawMessage(`[{"type":"text","text":"<system-reminder>\nSessionStart: 会话开始上下文\n</system-reminder>"}]`)},
			{Role: "assistant", Content: json.RawMessage(`"回答"`)},
			{Role: "user", Content: json.RawMessage(`"最新提问"`)},
		}
		out := StripReminders(input)
		got := allText(t, out[0])
		if !strings.Contains(got, "SessionStart:") {
			t.Errorf("SessionStart 块应保留 body，got=%q", got)
		}
		if !strings.Contains(got, "<system-reminder>") {
			t.Errorf("SessionStart 块应原样保留 system-reminder 标签，got=%q", got)
		}
	})

	// 用例 5：tool_use + tool_result 配对完整 + 消息条数不变。
	t.Run("tool配对完整且消息条数不变", func(t *testing.T) {
		input := []Message{
			{Role: "assistant", Content: json.RawMessage(`[{"type":"text","text":"调用工具\n<system-reminder>noise</system-reminder>"},{"type":"tool_use","id":"t1","name":"Read","input":{"file_path":"a.go"}}]`)},
			{Role: "user", Content: json.RawMessage(`[{"type":"tool_result","tool_use_id":"t1","content":"file content"}]`)},
			{Role: "user", Content: json.RawMessage(`"最新提问"`)},
		}
		toolUseBefore := countBlockType(t, input[0], "tool_use")
		toolResultBefore := countBlockType(t, input[1], "tool_result")

		out := StripReminders(input)

		if len(out) != len(input) {
			t.Fatalf("消息条数应不变，want=%d got=%d", len(input), len(out))
		}
		if got := countBlockType(t, out[0], "tool_use"); got != toolUseBefore {
			t.Errorf("tool_use 数应不变，want=%d got=%d", toolUseBefore, got)
		}
		if got := countBlockType(t, out[1], "tool_result"); got != toolResultBefore {
			t.Errorf("tool_result 数应不变，want=%d got=%d", toolResultBefore, got)
		}
		// system-reminder 仍应被移除，tool_use 仍在。
		if strings.Contains(allText(t, out[0]), "<system-reminder>") {
			t.Errorf("system-reminder 应被移除")
		}
	})

	// 用例 6：含 reminder 的输入经 strip 后 token 严格下降。
	t.Run("token下降", func(t *testing.T) {
		tc, err := NewTokenCounter()
		if err != nil {
			t.Fatal(err)
		}
		input := []Message{
			{Role: "assistant", Content: json.RawMessage(`[{"type":"text","text":"回答正文\n<system-reminder>\nYour task tools haven't been used recently. Lengthy reminder body that wastes a meaningful number of context tokens across many turns.\n</system-reminder>\n[skill-hint]another lengthy skill hint suggesting tools that no longer apply[/skill-hint]"}]`)},
			{Role: "user", Content: json.RawMessage(`"最新提问"`)},
		}
		before := tc.CountMessagesTokens(input)
		after := tc.CountMessagesTokens(StripReminders(input))
		if after >= before {
			t.Errorf("strip 后 token 应严格下降，before=%d after=%d", before, after)
		}
	})

	// 用例 7：纯 reminder 消息 strip 后内容为空 → 整条消息原样保留（防 Anthropic 400）。
	t.Run("空内容防护-纯reminder消息保持原样", func(t *testing.T) {
		reminderOnly := json.RawMessage(`[{"type":"text","text":"<system-reminder>\nHere is the new session start reminder.\n</system-reminder>"}]`)
		normalMsg := json.RawMessage(`[{"type":"text","text":"正常正文\n<system-reminder>\nYour task tools haven't been used recently.\n</system-reminder>"}]`)

		input := []Message{
			{Role: "user", Content: reminderOnly},              // 旧消息：整页只有便利贴，撕完会空 → 应原样保留
			{Role: "assistant", Content: normalMsg},            // 旧消息：有正文+便利贴 → 应正常清理
			{Role: "user", Content: json.RawMessage(`"最新提问"`)}, // 末条 user → 保留
		}
		out := StripReminders(input)

		// 断言 1：纯 reminder 消息原样保留（未因 strip 变空触发 400）
		if string(out[0].Content) != string(reminderOnly) {
			t.Errorf("纯 reminder 消息应为原样保留（空 content 会触发 Anthropic 400），got=%s", string(out[0].Content))
		}

		// 断言 2：其他消息仍正常清理
		if strings.Contains(allText(t, out[1]), "<system-reminder>") {
			t.Errorf("正常消息的 system-reminder 应被移除")
		}

		// 断言 3：消息条数不变
		if len(out) != 3 {
			t.Errorf("消息条数不应变化，got=%d", len(out))
		}
	})

	// 用例 8：持久 user context 位于旧首 user，且后续已有工具结果时仍保留。
	t.Run("旧首user持久context保留", func(t *testing.T) {
		for _, key := range []string{"claudeMd", "currentDate", "userEmail", "attachedProject"} {
			t.Run(key, func(t *testing.T) {
				input := []Message{
					persistentContextMessage(key, "fictional-value"),
					{Role: "assistant", Content: json.RawMessage(`[{"type":"tool_use","id":"t1","name":"Read","input":{}}]`)},
					{Role: "user", Content: json.RawMessage(`[{"type":"tool_result","tool_use_id":"t1","content":"ok"}]`)},
					{Role: "user", Content: json.RawMessage(`"最新提问"`)},
				}
				out := StripReminders(input)
				if !strings.Contains(allText(t, out[0]), "# "+key) {
					t.Fatalf("持久 key #%s 被 StripReminders 删除", key)
				}
				if countBlockType(t, out[1], "tool_use") != 1 || countBlockType(t, out[2], "tool_result") != 1 {
					t.Fatal("持久 context 分类改变了工具配对")
				}
			})
		}
	})

	// 用例 9：未知或畸形 reminder 采用 fail-safe 保留。
	t.Run("不确定reminder保留", func(t *testing.T) {
		input := []Message{
			{Role: "assistant", Content: json.RawMessage(`"before <system-reminder>future semantic reminder</system-reminder> after"`)},
			{Role: "assistant", Content: json.RawMessage(`"before <system-reminder>unclosed reminder after"`)},
			{Role: "user", Content: json.RawMessage(`"最新提问"`)},
		}
		out := StripReminders(input)
		if !strings.Contains(allText(t, out[0]), "future semantic reminder") {
			t.Fatal("未知完整 reminder 不应被删除")
		}
		if !strings.Contains(allText(t, out[1]), "unclosed reminder") {
			t.Fatal("畸形 reminder 不应被删除")
		}
	})

	t.Run("实际删除保留content block未知字段", func(t *testing.T) {
		original := json.RawMessage(`[
			{"type":"text","text":"ordinary before\n<system-reminder>noise</system-reminder>\nordinary after","future_text":{"mode":"strict"}},
			{"type":"tool_result","tool_use_id":"tool-1","content":"result","future_tool":null}
		]`)
		input := []Message{
			{Role: "assistant", Content: original},
			{Role: "user", Content: json.RawMessage(`"latest"`)},
		}
		out := StripReminders(input)
		if strings.Contains(allText(t, out[0]), "<system-reminder>") {
			t.Fatal("explicit temporary reminder was not removed")
		}
		var blocks []map[string]json.RawMessage
		if err := json.Unmarshal(out[0].Content, &blocks); err != nil {
			t.Fatalf("decode stripped blocks: %v", err)
		}
		if len(blocks) != 2 {
			t.Fatalf("block count = %d, want 2", len(blocks))
		}
		if string(blocks[0]["future_text"]) != `{"mode":"strict"}` {
			t.Fatalf("modified text block lost unknown field: %s", out[0].Content)
		}
		if value, ok := blocks[1]["future_tool"]; !ok || string(value) != "null" {
			t.Fatalf("unmodified tool_result lost unknown field: %s", out[0].Content)
		}
	})
}

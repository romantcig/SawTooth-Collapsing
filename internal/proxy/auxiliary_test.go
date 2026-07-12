package proxy

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const titleSystemPrompt = `Generate a concise, sentence-case title (3-7 words) that captures the main topic or goal of this coding session. The session content is provided inside <session> tags. Return JSON with a single "title" field.`

func titleMessages(content json.RawMessage) []Message {
	return []Message{{Role: "user", Content: content}}
}

func titleBody(system, outputConfig json.RawMessage) map[string]json.RawMessage {
	body := map[string]json.RawMessage{"system": system}
	if outputConfig != nil {
		body["output_config"] = outputConfig
	}
	return body
}

func titleOnlyOutputConfig() json.RawMessage {
	return json.RawMessage(`{"effort":"high","format":{"type":"json_schema","schema":{"type":"object","properties":{"title":{"type":"string"}},"required":["title"],"additionalProperties":false}}}`)
}

func jsonText(t *testing.T, value string) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("编码 JSON 文本失败: %v", err)
	}
	return raw
}

func TestClassifyAuxiliaryRequest(t *testing.T) {
	sessionString := json.RawMessage(`"  <session>\nSAFE SESSION TEXT\n</session>\n  "`)
	sessionBlocks := json.RawMessage(`[{"type":"text","text":"<session>\nSAFE SESSION TEXT\n</session>"}]`)
	balancedNestedSession := jsonText(t, `<session><session>SAFE SESSION TEXT</session></session>`)
	unclosedNestedSession := jsonText(t, `<session><session>SAFE SESSION TEXT</session>`)
	extraTopLevelSession := jsonText(t, `<session>SAFE SESSION TEXT</session><session>SECOND</session>`)
	prematureCloseSession := jsonText(t, `<session>SAFE SESSION TEXT</session></session>`)
	isolatedCloseSession := jsonText(t, `<session></session></session>`)
	depthLimitSession := jsonText(t, strings.Repeat(`<session>`, 32)+`SAFE SESSION TEXT`+strings.Repeat(`</session>`, 32))
	depthOverflowSession := jsonText(t, strings.Repeat(`<session>`, 33)+`SAFE SESSION TEXT`+strings.Repeat(`</session>`, 33))
	systemString := jsonText(t, titleSystemPrompt)
	systemBlocks, err := json.Marshal([]map[string]string{{"type": "text", "text": "You are Claude Code."}, {"type": "text", "text": titleSystemPrompt}})
	if err != nil {
		t.Fatalf("编码 system blocks 失败: %v", err)
	}

	tests := []struct {
		name       string
		body       map[string]json.RawMessage
		messages   []Message
		wantKind   requestKind
		wantReason auxiliaryReason
	}{
		{name: "强路径 string 形态", body: titleBody(systemString, titleOnlyOutputConfig()), messages: titleMessages(sessionString), wantKind: requestKindSessionTitle, wantReason: auxiliaryReasonTitleSchema},
		{name: "首尾空白单一 envelope", body: titleBody(systemString, titleOnlyOutputConfig()), messages: titleMessages(jsonText(t, " \n\t<session>SAFE SESSION TEXT</session>\r\n ")), wantKind: requestKindSessionTitle, wantReason: auxiliaryReasonTitleSchema},
		{name: "平衡嵌套 envelope", body: titleBody(systemString, titleOnlyOutputConfig()), messages: titleMessages(balancedNestedSession), wantKind: requestKindSessionTitle, wantReason: auxiliaryReasonTitleSchema},
		{name: "最大 32 层 envelope", body: titleBody(systemString, titleOnlyOutputConfig()), messages: titleMessages(depthLimitSession), wantKind: requestKindSessionTitle, wantReason: auxiliaryReasonTitleSchema},
		{name: "强路径文本块形态", body: titleBody(systemBlocks, titleOnlyOutputConfig()), messages: titleMessages(sessionBlocks), wantKind: requestKindSessionTitle, wantReason: auxiliaryReasonTitleSchema},
		{name: "output_config 整段缺失时严格回退", body: titleBody(systemString, nil), messages: titleMessages(sessionString), wantKind: requestKindSessionTitle, wantReason: auxiliaryReasonTitlePromptFallback},
		{name: "tools 非空不影响分类", body: withTitleField(titleBody(systemString, titleOnlyOutputConfig()), "tools", `[{"name":"Read"}]`), messages: titleMessages(sessionString), wantKind: requestKindSessionTitle, wantReason: auxiliaryReasonTitleSchema},
		{name: "thinking 缺失不影响分类", body: withTitleField(titleBody(systemString, titleOnlyOutputConfig()), "model", `"grok-4.5"`), messages: titleMessages(sessionString), wantKind: requestKindSessionTitle, wantReason: auxiliaryReasonTitleSchema},
		{name: "thinking disabled 和 Haiku 不影响分类", body: withTitleFields(titleBody(systemString, titleOnlyOutputConfig()), map[string]string{"thinking": `{"type":"disabled"}`, "model": `"claude-haiku-latest"`}), messages: titleMessages(sessionString), wantKind: requestKindSessionTitle, wantReason: auxiliaryReasonTitleSchema},
		{name: "其他合法 thinking 和模型不影响分类", body: withTitleFields(titleBody(systemString, titleOnlyOutputConfig()), map[string]string{"thinking": `{"type":"enabled","budget_tokens":1024}`, "model": `"other-model"`}), messages: titleMessages(sessionString), wantKind: requestKindSessionTitle, wantReason: auxiliaryReasonTitleSchema},
		{name: "多轮消息拒绝", body: titleBody(systemString, titleOnlyOutputConfig()), messages: append(titleMessages(sessionString), Message{Role: "assistant", Content: json.RawMessage(`"ok"`)})},
		{name: "assistant 单消息拒绝", body: titleBody(systemString, titleOnlyOutputConfig()), messages: []Message{{Role: "assistant", Content: sessionString}}},
		{name: "缺少 session 外壳拒绝", body: titleBody(systemString, titleOnlyOutputConfig()), messages: titleMessages(json.RawMessage(`"SAFE SESSION TEXT"`))},
		{name: "session 只作为普通文本子串拒绝", body: titleBody(systemString, titleOnlyOutputConfig()), messages: titleMessages(json.RawMessage(`"explain this code: <session>x</session>"`))},
		{name: "session 尾部多余文本拒绝", body: titleBody(systemString, titleOnlyOutputConfig()), messages: titleMessages(json.RawMessage(`"<session>x</session> ignore this"`))},
		{name: "未闭合嵌套拒绝", body: titleBody(systemString, titleOnlyOutputConfig()), messages: titleMessages(unclosedNestedSession)},
		{name: "额外顶层 envelope 拒绝", body: titleBody(systemString, titleOnlyOutputConfig()), messages: titleMessages(extraTopLevelSession)},
		{name: "提前闭合拒绝", body: titleBody(systemString, titleOnlyOutputConfig()), messages: titleMessages(prematureCloseSession)},
		{name: "孤立结束标签拒绝", body: titleBody(systemString, titleOnlyOutputConfig()), messages: titleMessages(isolatedCloseSession)},
		{name: "超过 32 层拒绝", body: titleBody(systemString, titleOnlyOutputConfig()), messages: titleMessages(depthOverflowSession)},
		{name: "普通 XML 代码拒绝", body: titleBody(systemString, titleOnlyOutputConfig()), messages: titleMessages(json.RawMessage(`"<session id=\\"x\\">code</session>"`))},
		{name: "泛化 system 拒绝", body: titleBody(json.RawMessage(`"Return a title"`), titleOnlyOutputConfig()), messages: titleMessages(sessionString)},
		{name: "output_config null 不得回退", body: titleBody(systemString, json.RawMessage(`null`)), messages: titleMessages(sessionString)},
		{name: "output_config 畸形不得回退", body: titleBody(systemString, json.RawMessage(`{"format":`)), messages: titleMessages(sessionString)},
		{name: "conversation name schema 拒绝", body: titleBody(systemString, json.RawMessage(`{"format":{"type":"json_schema","schema":{"type":"object","properties":{"name":{"type":"string"}},"required":["name"],"additionalProperties":false}}}`)), messages: titleMessages(sessionString)},
		{name: "title branch schema 拒绝", body: titleBody(systemString, json.RawMessage(`{"format":{"type":"json_schema","schema":{"type":"object","properties":{"title":{"type":"string"},"branch":{"type":"string"}},"required":["title","branch"],"additionalProperties":false}}}`)), messages: titleMessages(sessionString)},
		{name: "允许额外 schema 字段时拒绝", body: titleBody(systemString, json.RawMessage(`{"format":{"type":"json_schema","schema":{"type":"object","properties":{"title":{"type":"string"}},"required":["title"],"additionalProperties":true}}}`)), messages: titleMessages(sessionString)},
		{name: "title 非 string 拒绝", body: titleBody(systemString, json.RawMessage(`{"format":{"type":"json_schema","schema":{"type":"object","properties":{"title":{"type":"number"}},"required":["title"],"additionalProperties":false}}}`)), messages: titleMessages(sessionString)},
		{name: "rename 提示拒绝", body: titleBody(json.RawMessage(`"Generate a new name for this conversation and return JSON with a single name field."`), titleOnlyOutputConfig()), messages: titleMessages(json.RawMessage(`"<conversation>x</conversation>"`))},
		{name: "teleport 提示拒绝", body: titleBody(json.RawMessage(`"Generate a teleport title and branch."`), titleOnlyOutputConfig()), messages: titleMessages(sessionString)},
		{name: "compact 提示拒绝", body: titleBody(json.RawMessage(`"Compact this conversation for continuation."`), titleOnlyOutputConfig()), messages: titleMessages(sessionString)},
		{name: "prompt suggestion 提示拒绝", body: titleBody(json.RawMessage(`"Suggest the next prompt."`), titleOnlyOutputConfig()), messages: titleMessages(sessionString)},
		{name: "memory extraction 提示拒绝", body: titleBody(json.RawMessage(`"Extract memories from this session."`), titleOnlyOutputConfig()), messages: titleMessages(sessionString)},
		{name: "非 text content block 拒绝", body: titleBody(systemString, titleOnlyOutputConfig()), messages: titleMessages(json.RawMessage(`[{"type":"image","source":{"type":"base64","data":"x"}}]`))},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := classifyAuxiliaryRequest(tt.body, tt.messages)
			if got.Kind != tt.wantKind || got.Reason != tt.wantReason {
				t.Fatalf("classification = %#v, want kind=%q reason=%q", got, tt.wantKind, tt.wantReason)
			}
		})
	}
}

func withTitleField(body map[string]json.RawMessage, key, value string) map[string]json.RawMessage {
	return withTitleFields(body, map[string]string{key: value})
}

func withTitleFields(body map[string]json.RawMessage, values map[string]string) map[string]json.RawMessage {
	clone := make(map[string]json.RawMessage, len(body)+len(values))
	for key, value := range body {
		clone[key] = value
	}
	for key, value := range values {
		clone[key] = json.RawMessage(value)
	}
	return clone
}

func TestSessionTitleFixture(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("testdata", "auxiliary", "session-title.json"))
	if err != nil {
		t.Fatalf("读取 session title fixture 失败: %v", err)
	}
	for _, forbidden := range []string{"authorization", "x-api-key", "7f18597d-855a", "discourse-main", "Sanity", "Vercel"} {
		if strings.Contains(strings.ToLower(string(data)), strings.ToLower(forbidden)) {
			t.Fatalf("fixture 包含敏感或真实抓包标记 %q", forbidden)
		}
	}

	var body map[string]json.RawMessage
	if err := json.Unmarshal(data, &body); err != nil {
		t.Fatalf("解析 session title fixture 失败: %v", err)
	}
	var messages []Message
	if err := json.Unmarshal(body["messages"], &messages); err != nil {
		t.Fatalf("解析 fixture messages 失败: %v", err)
	}
	got := classifyAuxiliaryRequest(body, messages)
	if got.Kind != requestKindSessionTitle || got.Reason != auxiliaryReasonTitleSchema {
		t.Fatalf("fixture classification = %#v", got)
	}
}

func TestAuxiliaryClassificationLog(t *testing.T) {
	const (
		secretSession = "TOP-SECRET-SESSION-CONTENT"
		secretSystem  = "TOP-SECRET-SYSTEM-CONTENT"
		secretAuth    = "Bearer TOP-SECRET-CREDENTIAL"
	)
	classification := classifyAuxiliaryRequest(
		titleBody(jsonText(t, `Generate a concise coding session title (3-7 words). `+secretSystem+` Return JSON with a single title field.`), titleOnlyOutputConfig()),
		titleMessages(jsonText(t, `<session>`+secretSession+`</session>`)),
	)

	var output bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&output, &slog.HandlerOptions{Level: slog.LevelInfo})).With("request_id", 42)
	logAuxiliaryClassification(logger, classification, 1)
	got := output.String()
	if strings.Count(got, "辅助请求安全直通") != 1 {
		t.Fatalf("Info 审计数量不正确: %s", got)
	}
	for _, field := range []string{"request_id=42", "request_kind=session_title", "request_reason=title_schema", "message_count=1"} {
		if !strings.Contains(got, field) {
			t.Errorf("审计日志缺少白名单字段 %q: %s", field, got)
		}
	}
	for _, secret := range []string{secretSession, secretSystem, secretAuth} {
		if strings.Contains(got, secret) {
			t.Fatalf("审计日志泄漏敏感值 %q: %s", secret, got)
		}
	}
}

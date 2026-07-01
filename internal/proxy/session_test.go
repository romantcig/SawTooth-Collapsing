package proxy

import (
	"encoding/json"
	"testing"
)

func TestIsSubagent_EmptyMessages(t *testing.T) {
	// 边缘情况：空消息返回 false
	bodyMap := map[string]json.RawMessage{
		"model": json.RawMessage(`"claude-sonnet-4-20250514"`),
	}
	if isSubagent(bodyMap, nil) {
		t.Error("空 messages 应返回 false")
	}
	if isSubagent(bodyMap, []Message{}) {
		t.Error("空 messages 切片应返回 false")
	}
}

func TestIsSubagent_CheckA_SystemArray(t *testing.T) {
	// Check A: system 数组格式包含 cc_entrypoint=sdk-ts
	bodyMap := map[string]json.RawMessage{
		"system": json.RawMessage(`[{"type": "text", "text": "cc_entrypoint=sdk-ts custom-system-prompt"}]`),
		"model":  json.RawMessage(`"claude-sonnet-4-20250514"`),
	}
	messages := []Message{{Role: "user", Content: json.RawMessage(`"hello"`)}}
	if !isSubagent(bodyMap, messages) {
		t.Error("system 数组含 cc_entrypoint=sdk-ts 应返回 true")
	}
}

func TestIsSubagent_CheckA_SystemString(t *testing.T) {
	// Check A: system 字符串格式包含 cc_entrypoint=sdk-ts
	bodyMap := map[string]json.RawMessage{
		"system": json.RawMessage(`"cc_entrypoint=sdk-ts custom-system-prompt"`),
		"model":  json.RawMessage(`"claude-sonnet-4-20250514"`),
	}
	messages := []Message{{Role: "user", Content: json.RawMessage(`"hello"`)}}
	if !isSubagent(bodyMap, messages) {
		t.Error("system 字符串含 cc_entrypoint=sdk-ts 应返回 true")
	}
}

func TestIsSubagent_CheckA_SystemArrayFirstElement(t *testing.T) {
	// Check A: 只检查第一个元素即可
	bodyMap := map[string]json.RawMessage{
		"system": json.RawMessage(`[
			{"type": "text", "text": "cc_entrypoint=sdk-ts first"},
			{"type": "text", "text": "other content"}
		]`),
		"model": json.RawMessage(`"claude-sonnet-4-20250514"`),
	}
	messages := []Message{{Role: "user", Content: json.RawMessage(`"hello"`)}}
	if !isSubagent(bodyMap, messages) {
		t.Error("system 数组首元素含 cc_entrypoint=sdk-ts 应返回 true")
	}
}

func TestIsSubagent_CheckB_Haiku(t *testing.T) {
	// Check B: haiku 模型
	bodyMap := map[string]json.RawMessage{
		"model":    json.RawMessage(`"claude-3-haiku-20240307"`),
		"thinking": json.RawMessage(`{"type": "enabled", "budget_tokens": 500}`),
	}
	messages := []Message{{Role: "user", Content: json.RawMessage(`"hello"`)}}
	if !isSubagent(bodyMap, messages) {
		t.Error("haiku 模型应返回 true")
	}
}

func TestIsSubagent_CheckB_HaikuSubstring(t *testing.T) {
	// Check B: 模型名包含 "haiku" 即可（不区分大小写场景验证）
	bodyMap := map[string]json.RawMessage{
		"model":    json.RawMessage(`"claude-3-5-haiku-20241022"`),
		"thinking": json.RawMessage(`{"type": "enabled", "budget_tokens": 500}`),
	}
	messages := []Message{{Role: "user", Content: json.RawMessage(`"hello"`)}}
	if !isSubagent(bodyMap, messages) {
		t.Error("claude-3-5-haiku 应返回 true")
	}
}

func TestIsSubagent_CheckC_NoThinking(t *testing.T) {
	// Check C: 缺少 thinking 字段
	bodyMap := map[string]json.RawMessage{
		"model": json.RawMessage(`"claude-sonnet-4-20250514"`),
	}
	messages := []Message{{Role: "user", Content: json.RawMessage(`"hello"`)}}
	if !isSubagent(bodyMap, messages) {
		t.Error("无 thinking 字段应返回 true")
	}
}

func TestIsSubagent_NormalSonnetWithThinking(t *testing.T) {
	// 正常 Sonnet 请求（有 thinking 字段）应返回 false
	bodyMap := map[string]json.RawMessage{
		"model":    json.RawMessage(`"claude-sonnet-4-20250514"`),
		"thinking": json.RawMessage(`{"type": "enabled", "budget_tokens": 1000}`),
	}
	messages := []Message{{Role: "user", Content: json.RawMessage(`"hello"`)}}
	if isSubagent(bodyMap, messages) {
		t.Error("正常 Sonnet 请求（有 thinking）应返回 false")
	}
}

func TestIsSubagent_SystemWithoutEntrypoint(t *testing.T) {
	// system 字段存在但不包含 cc_entrypoint=sdk-ts — 不应触发 Check A
	bodyMap := map[string]json.RawMessage{
		"system":    json.RawMessage(`"custom system prompt"`),
		"model":     json.RawMessage(`"claude-sonnet-4-20250514"`),
		"thinking":  json.RawMessage(`{"type": "enabled", "budget_tokens": 1000}`),
	}
	messages := []Message{{Role: "user", Content: json.RawMessage(`"hello"`)}}
	if isSubagent(bodyMap, messages) {
		t.Error("system 字段无 cc_entrypoint 且有 thinking 应返回 false")
	}
}

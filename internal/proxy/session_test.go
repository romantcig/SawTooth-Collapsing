package proxy

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestAgentRequestFeatures(t *testing.T) {
	tests := []struct {
		name     string
		bodyMap  map[string]json.RawMessage
		headers  map[string]string
		messages []Message
		want     agentRequestFeatures
	}{
		{
			name: "DeepSeek 主请求只保留枚举和存在性",
			bodyMap: map[string]json.RawMessage{
				"model":    json.RawMessage(`"deepseek-v4-pro"`),
				"thinking": json.RawMessage(`{"type":"enabled"}`),
				"system":   json.RawMessage(`"private system prompt"`),
				"metadata": json.RawMessage(`{"user_id":"secret-user"}`),
			},
			headers:  map[string]string{"X-Claude-Code-Session-Id": "secret-session"},
			messages: []Message{{Role: "user", Content: json.RawMessage(`"private message"`)}},
			want: agentRequestFeatures{
				ModelFamily:          agentModelFamilyDeepSeek,
				ThinkingPresent:      true,
				SystemPresent:        true,
				SystemShape:          agentSystemShapeString,
				MetadataPresent:      true,
				SessionHeaderPresent: true,
				ParentRelation:       agentParentRelationUnavailable,
				MessagesPresent:      true,
			},
		},
		{
			name: "Claude Haiku 与 sdk-ts marker",
			bodyMap: map[string]json.RawMessage{
				"model":  json.RawMessage(`"claude-3-5-haiku-20241022"`),
				"system": json.RawMessage(`[{"type":"text","text":"cc_entrypoint=sdk-ts secret"}]`),
			},
			messages: []Message{{Role: "user", Content: json.RawMessage(`"hello"`)}},
			want: agentRequestFeatures{
				ModelFamily:        agentModelFamilyClaudeHaiku,
				SystemPresent:      true,
				SystemShape:        agentSystemShapeArray,
				SDKTSMarkerPresent: true,
				ParentRelation:     agentParentRelationUnavailable,
				MessagesPresent:    true,
			},
		},
		{
			name: "畸形字段安全降级",
			bodyMap: map[string]json.RawMessage{
				"model":    json.RawMessage(`123`),
				"system":   json.RawMessage(`{"text":`),
				"metadata": json.RawMessage(`{"broken":`),
			},
			want: agentRequestFeatures{
				ModelFamily:     agentModelFamilyUnknown,
				SystemPresent:   true,
				SystemShape:     agentSystemShapeUnknown,
				MetadataPresent: true,
				ParentRelation:  agentParentRelationUnavailable,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("POST", "/v1/messages", nil)
			for key, value := range tt.headers {
				req.Header.Set(key, value)
			}
			got := extractAgentRequestFeatures(req, tt.bodyMap, tt.messages)
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("extractAgentRequestFeatures() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestAgentDiagnosticRedaction(t *testing.T) {
	const (
		secretSystem  = "TOP-SECRET-SYSTEM-PROMPT"
		secretMessage = "TOP-SECRET-MESSAGE"
		secretAuth    = "Bearer TOP-SECRET-KEY"
		secretSession = "TOP-SECRET-SESSION"
	)
	bodyMap := map[string]json.RawMessage{
		"model":    json.RawMessage(`"deepseek-v4-pro"`),
		"thinking": json.RawMessage(`{"type":"enabled"}`),
		"system":   json.RawMessage(`"` + secretSystem + `"`),
	}
	req := httptest.NewRequest("POST", "/v1/messages", nil)
	req.Header.Set("Authorization", secretAuth)
	req.Header.Set("X-Claude-Code-Session-Id", secretSession)
	messages := []Message{{Role: "user", Content: json.RawMessage(`"` + secretMessage + `"`)}}

	var output bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&output, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previous) })

	logAgentRequestFeatures(extractAgentRequestFeatures(req, bodyMap, messages))
	got := output.String()
	for _, secret := range []string{secretSystem, secretMessage, secretAuth, secretSession} {
		if strings.Contains(got, secret) {
			t.Fatalf("诊断日志泄漏敏感值 %q: %s", secret, got)
		}
	}
	for _, field := range []string{
		"agent_features", "model_family=deepseek", "thinking_present=true",
		"system_present=true", "session_header_present=true", "parent_relation=unavailable",
	} {
		if !strings.Contains(got, field) {
			t.Errorf("诊断日志缺少白名单字段 %q: %s", field, got)
		}
	}
}

type agentFeatureFixture struct {
	SchemaVersion int                       `json:"schema_version"`
	Evidence      string                    `json:"evidence"`
	Cases         []agentFeatureFixtureCase `json:"cases"`
}

type agentFeatureFixtureCase struct {
	ExpectedRole string               `json:"expected_role"`
	Features     agentRequestFeatures `json:"features"`
}

func loadAgentFeatureFixture(t *testing.T, name string) agentFeatureFixture {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", "agent_features", name))
	if err != nil {
		t.Fatalf("读取 fixture %s 失败: %v", name, err)
	}
	var fixture agentFeatureFixture
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatalf("解析 fixture %s 失败: %v", name, err)
	}
	return fixture
}

func TestAgentFeatureFixtures(t *testing.T) {
	tests := []struct {
		name      string
		caseCount int
		wantRoles []string
	}{
		{"deepseek-main.json", 2, []string{"main", "main"}},
		{"deepseek-subagent.json", 1, []string{"subagent"}},
		{"ambiguous.json", 2, []string{"unknown", "unknown"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fixture := loadAgentFeatureFixture(t, tt.name)
			if fixture.SchemaVersion != 1 {
				t.Fatalf("schema_version = %d, want 1", fixture.SchemaVersion)
			}
			if len(fixture.Cases) != tt.caseCount {
				t.Fatalf("cases = %d, want %d", len(fixture.Cases), tt.caseCount)
			}
			for i, wantRole := range tt.wantRoles {
				if fixture.Cases[i].ExpectedRole != wantRole {
					t.Errorf("case %d expected_role = %q, want %q", i, fixture.Cases[i].ExpectedRole, wantRole)
				}
			}
		})
	}

	main := loadAgentFeatureFixture(t, "deepseek-main.json")
	if !main.Cases[0].Features.ThinkingPresent || main.Cases[1].Features.ThinkingPresent {
		t.Fatal("DeepSeek main fixture 必须同时覆盖 thinking present 与 absent")
	}
	if main.Cases[1].ExpectedRole != "main" {
		t.Fatal("missing-thinking 的 main 变体不得被标记为 subagent")
	}

	subagent := loadAgentFeatureFixture(t, "deepseek-subagent.json")
	features := subagent.Cases[0].Features
	if !features.ThinkingPresent || !features.SDKTSMarkerPresent {
		t.Fatal("DeepSeek subagent fixture 必须包含 thinking 和已知 sdk-ts marker")
	}

	ambiguous := loadAgentFeatureFixture(t, "ambiguous.json")
	for i, fixtureCase := range ambiguous.Cases {
		if fixtureCase.Features.ParentRelation != agentParentRelationUnavailable {
			t.Errorf("ambiguous case %d parent_relation = %q, want unavailable", i, fixtureCase.Features.ParentRelation)
		}
	}
}

func TestAgentFixtureSchemaRejectsSensitivePayload(t *testing.T) {
	allowedTopLevel := map[string]bool{"schema_version": true, "evidence": true, "cases": true}
	allowedCase := map[string]bool{"expected_role": true, "features": true}
	allowedFeatures := map[string]bool{
		"model_family": true, "thinking_present": true, "system_present": true,
		"system_shape": true, "sdk_ts_marker_present": true, "metadata_present": true,
		"session_header_present": true, "parent_marker_present": true,
		"parent_relation": true, "messages_present": true,
	}
	for _, name := range []string{"deepseek-main.json", "deepseek-subagent.json", "ambiguous.json"} {
		data, err := os.ReadFile(filepath.Join("testdata", "agent_features", name))
		if err != nil {
			t.Fatalf("读取 fixture %s 失败: %v", name, err)
		}
		lower := strings.ToLower(string(data))
		for _, forbidden := range []string{"authorization", "api_key", "prompt", "messages\"", "body\"", "system\""} {
			if strings.Contains(lower, forbidden) {
				t.Fatalf("fixture %s 包含敏感载荷键 %q", name, forbidden)
			}
		}

		var raw map[string]json.RawMessage
		if err := json.Unmarshal(data, &raw); err != nil {
			t.Fatalf("解析 fixture %s 失败: %v", name, err)
		}
		assertAllowedJSONKeys(t, name, raw, allowedTopLevel)
		var cases []map[string]json.RawMessage
		if err := json.Unmarshal(raw["cases"], &cases); err != nil {
			t.Fatalf("解析 fixture %s cases 失败: %v", name, err)
		}
		for i, fixtureCase := range cases {
			assertAllowedJSONKeys(t, name, fixtureCase, allowedCase)
			var features map[string]json.RawMessage
			if err := json.Unmarshal(fixtureCase["features"], &features); err != nil {
				t.Fatalf("解析 fixture %s case %d features 失败: %v", name, i, err)
			}
			assertAllowedJSONKeys(t, name, features, allowedFeatures)
		}
	}
}

func assertAllowedJSONKeys(t *testing.T, name string, values map[string]json.RawMessage, allowed map[string]bool) {
	t.Helper()
	for key := range values {
		if !allowed[key] {
			t.Fatalf("fixture %s 包含非白名单字段 %q", name, key)
		}
	}
}

func TestDeepSeekAgentMatrix(t *testing.T) {
	for _, name := range []string{"deepseek-main.json", "deepseek-subagent.json", "ambiguous.json"} {
		fixture := loadAgentFeatureFixture(t, name)
		for i, fixtureCase := range fixture.Cases {
			got := classifyAgentFeatures(fixtureCase.Features)
			if string(got.Role) != fixtureCase.ExpectedRole {
				t.Errorf("%s case %d role = %q, want %q (reason=%q)", name, i, got.Role, fixtureCase.ExpectedRole, got.Reason)
			}
			if got.Reason == "" {
				t.Errorf("%s case %d reason 为空", name, i)
			}
		}
	}
}

func TestAgentClassificationCompatibilityPrecedence(t *testing.T) {
	tests := []struct {
		name     string
		features agentRequestFeatures
		wantRole agentRole
		wantWhy  agentClassificationReason
	}{
		{
			name: "sdk-ts marker 优先识别子代理",
			features: agentRequestFeatures{
				ModelFamily:        agentModelFamilyDeepSeek,
				SDKTSMarkerPresent: true,
				MessagesPresent:    true,
			},
			wantRole: agentRoleSubagent,
			wantWhy:  agentReasonSDKTSCompatibility,
		},
		{
			name: "DeepSeek 主代理缺 thinking 仍为 main",
			features: agentRequestFeatures{
				ModelFamily:     agentModelFamilyDeepSeek,
				MessagesPresent: true,
			},
			wantRole: agentRoleMain,
			wantWhy:  agentReasonDeepSeekModel,
		},
		{
			name: "Haiku 仅作为兼容 fallback",
			features: agentRequestFeatures{
				ModelFamily:     agentModelFamilyClaudeHaiku,
				ThinkingPresent: true,
				MessagesPresent: true,
			},
			wantRole: agentRoleSubagent,
			wantWhy:  agentReasonHaikuCompatibility,
		},
		{
			name: "无稳定 marker 保持 unknown",
			features: agentRequestFeatures{
				ModelFamily:     agentModelFamilyUnknown,
				MessagesPresent: true,
			},
			wantRole: agentRoleUnknown,
			wantWhy:  agentReasonNoVerifiedMarker,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := classifyAgentFeatures(tt.features)
			if got.Role != tt.wantRole || got.Reason != tt.wantWhy {
				t.Fatalf("classification = %#v, want role=%q reason=%q", got, tt.wantRole, tt.wantWhy)
			}
		})
	}
}

func TestAgentParentRelation(t *testing.T) {
	const (
		childID  = "11111111-1111-4111-8111-111111111111"
		parentID = "22222222-2222-4222-8222-222222222222"
	)
	bodyMap := map[string]json.RawMessage{
		"model":    json.RawMessage(`"deepseek-v4-pro"`),
		"thinking": json.RawMessage(`{"type":"enabled"}`),
	}
	messages := []Message{{Role: "user", Content: json.RawMessage(`"hello"`)}}

	tests := []struct {
		name          string
		parentHeader  string
		wantAvailable bool
		wantParentID  string
	}{
		{name: "显式 parent marker", parentHeader: parentID, wantAvailable: true, wantParentID: parentID},
		{name: "缺少 parent marker"},
		{name: "空白 parent marker", parentHeader: "   "},
		{name: "parent 与 child 相同不可用", parentHeader: childID},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("POST", "/v1/messages", nil)
			req.Header.Set("X-Claude-Code-Session-Id", childID)
			if tt.parentHeader != "" {
				req.Header.Set("X-Claude-Code-Parent-Session-Id", tt.parentHeader)
			}
			got := classifyAgentRequest(req, bodyMap, messages)
			if got.ParentAvailable != tt.wantAvailable || got.ParentSessionID != tt.wantParentID {
				t.Fatalf("parent classification = %#v, want available=%v parent=%q", got, tt.wantAvailable, tt.wantParentID)
			}
		})
	}
}

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

func TestIsSubagent_NoThinkingDoesNotDecideRole(t *testing.T) {
	// 缺少 thinking 字段不能单独决定子代理身份。
	bodyMap := map[string]json.RawMessage{
		"model": json.RawMessage(`"claude-sonnet-4-20250514"`),
	}
	messages := []Message{{Role: "user", Content: json.RawMessage(`"hello"`)}}
	if isSubagent(bodyMap, messages) {
		t.Error("无 thinking 字段不得单独判为 subagent")
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
		"system":   json.RawMessage(`"custom system prompt"`),
		"model":    json.RawMessage(`"claude-sonnet-4-20250514"`),
		"thinking": json.RawMessage(`{"type": "enabled", "budget_tokens": 1000}`),
	}
	messages := []Message{{Role: "user", Content: json.RawMessage(`"hello"`)}}
	if isSubagent(bodyMap, messages) {
		t.Error("system 字段无 cc_entrypoint 且有 thinking 应返回 false")
	}
}

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
				AgentContextType:     agentContextTypeMissing,
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
				AgentContextType:   agentContextTypeMissing,
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
				ModelFamily:      agentModelFamilyUnknown,
				SystemPresent:    true,
				SystemShape:      agentSystemShapeUnknown,
				MetadataPresent:  true,
				ParentRelation:   agentParentRelationUnavailable,
				AgentContextType: agentContextTypeMissing,
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
		"model":        json.RawMessage(`"deepseek-v4-pro"`),
		"thinking":     json.RawMessage(`{"type":"enabled"}`),
		"system":       json.RawMessage(`"` + secretSystem + `"`),
		"agentContext": json.RawMessage(`{"parentSessionId":"` + secretSession + `"}`),
	}
	req := httptest.NewRequest("POST", "/v1/messages", nil)
	req.Header.Set("Authorization", secretAuth)
	req.Header.Set("X-Claude-Code-Session-Id", secretSession)
	messages := []Message{{Role: "user", Content: json.RawMessage(`"` + secretMessage + `"`)}}

	var output bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&output, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previous) })

	logAgentRequestFeatures(slog.Default(), extractAgentRequestFeatures(req, bodyMap, messages))
	got := output.String()
	for _, secret := range []string{secretSystem, secretMessage, secretAuth, secretSession} {
		if strings.Contains(got, secret) {
			t.Fatalf("诊断日志泄漏敏感值 %q: %s", secret, got)
		}
	}
	for _, field := range []string{
		"agent_features", "model_family=deepseek", "thinking_present=true",
		"system_present=true", "session_header_present=true", "parent_relation=unavailable",
		"agent_context_type=missing", "parent_session_present=true",
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
		{"ambiguous.json", 2, []string{"subagent", "main"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fixture := loadAgentFeatureFixture(t, tt.name)
			if fixture.SchemaVersion != 2 {
				t.Fatalf("schema_version = %d, want 2", fixture.SchemaVersion)
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

	mainFeatures := main.Cases[0].Features
	subagentFeatures := loadAgentFeatureFixture(t, "deepseek-subagent.json").Cases[0].Features
	if mainFeatures.ModelFamily != subagentFeatures.ModelFamily ||
		mainFeatures.ThinkingPresent != subagentFeatures.ThinkingPresent ||
		mainFeatures.SDKTSMarkerPresent != subagentFeatures.SDKTSMarkerPresent {
		t.Fatal("main/subagent fixture 必须保持相同 model、thinking 和 sdk-ts，只由强特征区分")
	}
}

func TestAgentFixtureSchemaRejectsSensitivePayload(t *testing.T) {
	allowedTopLevel := map[string]bool{"schema_version": true, "evidence": true, "cases": true}
	allowedCase := map[string]bool{"expected_role": true, "features": true}
	allowedFeatures := map[string]bool{
		"model_family": true, "thinking_present": true, "sdk_ts_marker_present": true,
		"billing_subagent_marker": true, "agent_context_type": true,
		"parent_session_present": true, "messages_present": true,
	}
	for _, name := range []string{"deepseek-main.json", "deepseek-subagent.json", "ambiguous.json"} {
		data, err := os.ReadFile(filepath.Join("testdata", "agent_features", name))
		if err != nil {
			t.Fatalf("读取 fixture %s 失败: %v", name, err)
		}
		lower := strings.ToLower(string(data))
		for _, forbidden := range []string{
			"authorization", "api_key", "prompt", "\"headers\"", "\"body\"", "\"messages\"",
			"\"parent_session_id\"", "\"session_id\"", "\"system\"",
		} {
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
			name: "sdk-ts marker 不再识别子代理",
			features: agentRequestFeatures{
				ModelFamily:        agentModelFamilyDeepSeek,
				SDKTSMarkerPresent: true,
				MessagesPresent:    true,
			},
			wantRole: agentRoleMain,
			wantWhy:  agentClassificationReason("no_subagent_marker"),
		},
		{
			name: "DeepSeek 主代理缺 thinking 仍为 main",
			features: agentRequestFeatures{
				ModelFamily:     agentModelFamilyDeepSeek,
				MessagesPresent: true,
			},
			wantRole: agentRoleMain,
			wantWhy:  agentClassificationReason("no_subagent_marker"),
		},
		{
			name: "Haiku 不再识别子代理",
			features: agentRequestFeatures{
				ModelFamily:     agentModelFamilyClaudeHaiku,
				ThinkingPresent: true,
				MessagesPresent: true,
			},
			wantRole: agentRoleMain,
			wantWhy:  agentClassificationReason("no_subagent_marker"),
		},
		{
			name: "无强特征按 main",
			features: agentRequestFeatures{
				ModelFamily:     agentModelFamilyUnknown,
				MessagesPresent: true,
			},
			wantRole: agentRoleMain,
			wantWhy:  agentClassificationReason("no_subagent_marker"),
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

func TestAgentClassificationStrongFeaturePrecedence(t *testing.T) {
	req := httptest.NewRequest("POST", "/v1/messages", nil)
	req.Header.Set("x-anthropic-billing-header", "cc_is_subagent=true")
	bodyMap := map[string]json.RawMessage{
		"model":        json.RawMessage(`"deepseek-v4-pro"`),
		"thinking":     json.RawMessage(`{"type":"enabled"}`),
		"system":       json.RawMessage(`"cc_entrypoint=sdk-ts"`),
		"agentContext": json.RawMessage(`{"agentType":"subagent","parentSessionId":"parent-secret"}`),
	}
	got := classifyAgentRequest(req, bodyMap, []Message{{Role: "user", Content: json.RawMessage(`"hello"`)}})
	if got.Role != agentRoleSubagent || got.Reason != agentReasonBillingMarker {
		t.Fatalf("组合强特征 classification = %#v, want billing marker precedence", got)
	}
}

func TestAgentBillingMarker(t *testing.T) {
	tests := []struct {
		name   string
		header string
		role   agentRole
	}{
		{name: "单 token", header: "cc_is_subagent=true", role: agentRoleSubagent},
		{name: "大小写和空白", header: " Cc_Is_SubAgent = TRUE ", role: agentRoleSubagent},
		{name: "逗号多 token", header: "cch=12345, cc_is_subagent=true, cc_version=2.1.199", role: agentRoleSubagent},
		{name: "分号多 token", header: "cch=12345;cc_is_subagent = true", role: agentRoleSubagent},
		{name: "false", header: "cc_is_subagent=false", role: agentRoleMain},
		{name: "相似键", header: "not_cc_is_subagent=true", role: agentRoleMain},
		{name: "畸形", header: "cc_is_subagent", role: agentRoleMain},
		{name: "缺失", role: agentRoleMain},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("POST", "/v1/messages", nil)
			if tt.header != "" {
				req.Header.Set("x-anthropic-billing-header", tt.header)
			}
			bodyMap := map[string]json.RawMessage{"model": json.RawMessage(`"deepseek-v4-pro"`)}
			got := classifyAgentRequest(req, bodyMap, []Message{{Role: "user", Content: json.RawMessage(`"hello"`)}})
			if got.Role != tt.role {
				t.Fatalf("role = %q, want %q (reason=%q)", got.Role, tt.role, got.Reason)
			}
		})
	}
}

func TestAgentContextStrongFeatures(t *testing.T) {
	tests := []struct {
		name    string
		context json.RawMessage
		role    agentRole
		reason  string
	}{
		{name: "agentType subagent", context: json.RawMessage(`{"agentType":"subagent"}`), role: agentRoleSubagent, reason: "agent_context_type"},
		{name: "agentType 大小写空白", context: json.RawMessage(`{"agentType":" SubAgent "}`), role: agentRoleSubagent, reason: "agent_context_type"},
		{name: "parentSessionId", context: json.RawMessage(`{"parentSessionId":"parent-secret"}`), role: agentRoleSubagent, reason: "agent_context_parent"},
		{name: "空 parent", context: json.RawMessage(`{"parentSessionId":"   "}`), role: agentRoleMain, reason: "no_subagent_marker"},
		{name: "main", context: json.RawMessage(`{"agentType":"main"}`), role: agentRoleMain, reason: "no_subagent_marker"},
		{name: "畸形对象", context: json.RawMessage(`{"agentType":`), role: agentRoleMain, reason: "no_subagent_marker"},
		{name: "非对象", context: json.RawMessage(`"subagent"`), role: agentRoleMain, reason: "no_subagent_marker"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bodyMap := map[string]json.RawMessage{
				"model":        json.RawMessage(`"same-model"`),
				"thinking":     json.RawMessage(`{"type":"enabled"}`),
				"system":       json.RawMessage(`"cc_entrypoint=sdk-ts"`),
				"agentContext": tt.context,
			}
			got := classifyAgentRequest(httptest.NewRequest("POST", "/v1/messages", nil), bodyMap, []Message{{Role: "user", Content: json.RawMessage(`"hello"`)}})
			if got.Role != tt.role || string(got.Reason) != tt.reason {
				t.Fatalf("classification = %#v, want role=%q reason=%q", got, tt.role, tt.reason)
			}
		})
	}
}

func TestAgentSystemAttributionStrongFeature(t *testing.T) {
	tests := []struct {
		name       string
		system     json.RawMessage
		wantRole   agentRole
		wantReason string
	}{
		{
			name:       "2.1.207 独立 text attribution block",
			system:     json.RawMessage(`[{"type":"text","text":"  x-anthropic-billing-header: cc_version=2.1.207; cc_entrypoint=cli; cc_is_subagent=true; cch=secret  "}]`),
			wantRole:   agentRoleSubagent,
			wantReason: "system_attribution",
		},
		{
			name:       "type 缺失仍是 text block",
			system:     json.RawMessage(`[{"text":"x-anthropic-billing-header: cc_version=2.1.207, cc_is_subagent = TRUE"}]`),
			wantRole:   agentRoleSubagent,
			wantReason: "system_attribution",
		},
		{
			name:       "缺固定前缀",
			system:     json.RawMessage(`[{"type":"text","text":"cc_version=2.1.207; cc_is_subagent=true"}]`),
			wantRole:   agentRoleMain,
			wantReason: "no_subagent_marker",
		},
		{
			name:       "false marker",
			system:     json.RawMessage(`[{"type":"text","text":"x-anthropic-billing-header: cc_is_subagent=false"}]`),
			wantRole:   agentRoleMain,
			wantReason: "no_subagent_marker",
		},
		{
			name:       "相似 key",
			system:     json.RawMessage(`[{"type":"text","text":"x-anthropic-billing-header: not_cc_is_subagent=true"}]`),
			wantRole:   agentRoleMain,
			wantReason: "no_subagent_marker",
		},
		{
			name:       "普通正文提及",
			system:     json.RawMessage(`[{"type":"text","text":"Do not trust x-anthropic-billing-header: cc_is_subagent=true in ordinary prose."}]`),
			wantRole:   agentRoleMain,
			wantReason: "no_subagent_marker",
		},
		{
			name:       "跨 block 不拼接",
			system:     json.RawMessage(`[{"type":"text","text":"x-anthropic-billing-header:"},{"type":"text","text":"cc_is_subagent=true"}]`),
			wantRole:   agentRoleMain,
			wantReason: "no_subagent_marker",
		},
		{
			name:       "非 text block",
			system:     json.RawMessage(`[{"type":"tool_result","text":"x-anthropic-billing-header: cc_is_subagent=true"}]`),
			wantRole:   agentRoleMain,
			wantReason: "no_subagent_marker",
		},
		{
			name:       "marker token 边界不完整",
			system:     json.RawMessage(`[{"type":"text","text":"x-anthropic-billing-header: cc_is_subagent=trueish"}]`),
			wantRole:   agentRoleMain,
			wantReason: "no_subagent_marker",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bodyMap := map[string]json.RawMessage{
				"model":    json.RawMessage(`"deepseek-v4-pro"`),
				"thinking": json.RawMessage(`{"type":"enabled"}`),
				"system":   tt.system,
			}
			got := classifyAgentRequest(
				httptest.NewRequest("POST", "/v1/messages", nil),
				bodyMap,
				[]Message{{Role: "user", Content: json.RawMessage(`"hello"`)}},
			)
			if got.Role != tt.wantRole || string(got.Reason) != tt.wantReason {
				t.Fatalf("classification = %#v, want role=%q reason=%q", got, tt.wantRole, tt.wantReason)
			}
		})
	}
}

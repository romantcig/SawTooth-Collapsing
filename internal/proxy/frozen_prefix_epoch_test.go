package proxy

import (
	"encoding/json"
	"testing"
)

func TestFrozenFullRawPrefixRejectsEarlyEditWithStableBoundary(t *testing.T) {
	raw := []Message{
		{Role: "user", Content: mustMarshal("early-original")},
		{Role: "assistant", Content: mustMarshal("middle-original")},
		{Role: "user", Content: mustMarshal("boundary")},
		{Role: "assistant", Content: mustMarshal("tail")},
	}
	frozen := NewFrozenStubs()
	frozen.StoreWithLogger(nil, "full-prefix", []Message{{Role: "assistant", Content: mustMarshal("stubbed")}}, 3, raw[2], 10, 100, raw)

	changed := deepCopyMessages(raw)
	changed[0].Content = mustMarshal("early-edited")
	if got := frozen.Get("full-prefix", changed); got != nil {
		t.Fatal("cutoff 前早期消息变化且 boundary 不变时不应命中 Frozen")
	}
}

func TestFrozenFullRawPrefixAcceptsKnownWireEquivalent(t *testing.T) {
	raw := []Message{
		{Role: "user", Content: mustMarshal("hello")},
		{Role: "assistant", Content: json.RawMessage(`[{"type":"text","text":"answer","cache_control":{"type":"ephemeral"}}]`)},
		{Role: "user", Content: mustMarshal("boundary")},
	}
	frozen := NewFrozenStubs()
	frozen.Store("wire-equivalent", []Message{{Role: "assistant", Content: mustMarshal("stubbed")}}, 3, raw[2], 10, 100, raw)

	equivalent := deepCopyMessages(raw)
	equivalent[1].Content = json.RawMessage(`"answer"`)
	if got := frozen.Get("wire-equivalent", equivalent); got == nil {
		t.Fatal("纯字符串与单 text block 等价形态不应使 Frozen 失效")
	}
}

func TestFrozenFullRawPrefixRejectsBusinessPayloadChanges(t *testing.T) {
	raw := []Message{
		{Role: "user", Content: json.RawMessage(`[{"type":"tool_use","id":"tool-1","name":"read","input":{"path":"a"}}]`)},
		{Role: "user", Content: json.RawMessage(`[{"type":"tool_result","tool_use_id":"tool-1","content":"same"}]`)},
		{Role: "assistant", Content: mustMarshal("boundary")},
	}
	frozen := NewFrozenStubs()
	frozen.Store("business-payload", []Message{{Role: "assistant", Content: mustMarshal("stubbed")}}, 3, raw[2], 10, 100, raw)

	cases := []struct {
		name   string
		mutate func([]Message)
	}{
		{name: "tool input", mutate: func(messages []Message) {
			messages[0].Content = json.RawMessage(`[{"type":"tool_use","id":"tool-1","name":"read","input":{"path":"b"}}]`)
		}},
		{name: "tool result", mutate: func(messages []Message) {
			messages[1].Content = json.RawMessage(`[{"type":"tool_result","tool_use_id":"tool-1","content":"changed"}]`)
		}},
		{name: "unknown field", mutate: func(messages []Message) {
			messages[0].Content = json.RawMessage(`[{"type":"tool_use","id":"tool-1","name":"read","input":{"path":"a"},"future_semantic":true}]`)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			current := deepCopyMessages(raw)
			tc.mutate(current)
			if got := frozen.Get("business-payload", current); got != nil {
				t.Fatal("业务 payload 变化后 Frozen 仍命中")
			}
		})
	}
}

func TestFrozenLegacyPersistedStateWithoutRawPrefixHashFailsClosed(t *testing.T) {
	raw := frozenTestMessages(3)
	prefix := deepCopyMessages(raw[:1])
	prefixJSON, err := json.Marshal(prefix)
	if err != nil {
		t.Fatal(err)
	}
	persisted, err := json.Marshal(frozenPersisted{
		Messages: prefix, Cutoff: 2,
		BoundaryHash: stableBoundaryHash(raw[1]),
		PrefixHash:   sha256hex(prefixJSON), Tokens: 10, RawTokens: 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	deleted := 0
	frozen := NewFrozenStubs()
	frozen.SetLoadFunc(func(string) (string, bool) { return string(persisted), true })
	frozen.SetDeleteFunc(func(string) { deleted++ })
	if got := frozen.Get("legacy-prefix", raw); got != nil {
		t.Fatal("缺少完整 raw prefix hash 的旧状态不得命中")
	}
	if deleted != 1 {
		t.Fatalf("旧 schema 失效后删除次数=%d，want 1", deleted)
	}
}

func TestFrozenThinkingSignatureChangeMissesButDoesNotNeedBoundaryGuess(t *testing.T) {
	raw := []Message{
		{Role: "assistant", Content: json.RawMessage(`[{"type":"thinking","thinking":"one","signature":"sig-1"}]`)},
		{Role: "user", Content: mustMarshal("boundary")},
	}
	frozen := NewFrozenStubs()
	frozen.Store("thinking-signature", []Message{{Role: "assistant", Content: mustMarshal("stubbed")}}, 2, raw[1], 10, 100, raw)
	changed := deepCopyMessages(raw)
	changed[0].Content = json.RawMessage(`[{"type":"thinking","thinking":"two","signature":"sig-2"}]`)
	if got := frozen.Get("thinking-signature", changed); got != nil {
		t.Fatal("thinking/signature 变化后 Frozen 不应仅凭 boundary 命中")
	}
}

func TestFrozenFullRawPrefixPersistsAndRestores(t *testing.T) {
	raw := frozenTestMessages(4)
	persisted := make(map[string]string)
	source := NewFrozenStubs()
	source.SetPersistFunc(func(key, value string) { persisted[key] = value })
	source.Store("persisted-full", raw[:1], 3, raw[2], 10, 100, raw)

	var state frozenPersisted
	if err := json.Unmarshal([]byte(persisted["frozen:persisted-full"]), &state); err != nil {
		t.Fatalf("解析 Frozen 持久状态: %v", err)
	}
	if state.RawPrefixMode != frozenRawPrefixModeFull || !validPressureFingerprint(state.RawPrefixHash) {
		t.Fatalf("持久化 raw prefix proof 不完整: mode=%q hash=%q", state.RawPrefixMode, state.RawPrefixHash)
	}

	restored := NewFrozenStubs()
	restored.SetLoadFunc(func(key string) (string, bool) {
		value, ok := persisted[key]
		return value, ok
	})
	if got := restored.Get("persisted-full", raw); got == nil {
		t.Fatal("完整 raw prefix proof 冷启动后未命中")
	}
}

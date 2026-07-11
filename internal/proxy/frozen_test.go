package proxy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
)

func TestFrozenStoreKeepsRawCutoffSeparateFromFrozenPrefixLength(t *testing.T) {
	frozen := NewFrozenStubs()
	raw := frozenTestMessages(302)
	prefix := deepCopyMessages(raw[:5])

	frozen.Store("thread", prefix, 300, raw[299], 120, 6000)

	if got := frozen.LengthFor("thread"); got != len(prefix) {
		t.Fatalf("frozen prefix length = %d, want %d", got, len(prefix))
	}
	result := frozen.Get("thread", raw)
	if result == nil {
		t.Fatal("expected frozen result for matching raw boundary message")
	}
	if result.Cutoff != 300 {
		t.Fatalf("raw cutoff = %d, want 300", result.Cutoff)
	}
	if len(result.Messages) != len(prefix) {
		t.Fatalf("stored frozen prefix length = %d, want %d", len(result.Messages), len(prefix))
	}
	if result.Tokens != 120 || result.RawTokens != 6000 {
		t.Fatalf("token metadata = (%d, %d), want (120, 6000)", result.Tokens, result.RawTokens)
	}
}

func TestFrozenUpdateMessagesPersistsBytesAndPreservesRawMetadata(t *testing.T) {
	persisted := make(map[string]string)
	frozen := NewFrozenStubs()
	frozen.SetPersistFunc(func(key, value string) {
		persisted[key] = value
	})

	raw := frozenTestMessages(302)
	prefix := deepCopyMessages(raw[:4])
	frozen.Store("thread", prefix, 300, raw[299], 120, 6000)

	updated := deepCopyMessages(prefix)
	updated[1].Content = mustMarshal("updated frozen bytes")
	if !frozen.UpdateMessages("thread", updated) {
		t.Fatal("UpdateMessages should accept an equal-length frozen prefix")
	}
	if frozen.UpdateMessages("thread", updated[:len(updated)-1]) {
		t.Fatal("UpdateMessages should reject a different frozen prefix length")
	}
	if frozen.UpdateMessages("missing", updated) {
		t.Fatal("UpdateMessages should reject a missing frozen entry")
	}

	restored := NewFrozenStubs()
	restored.SetLoadFunc(func(key string) (string, bool) {
		value, ok := persisted[key]
		return value, ok
	})
	result := restored.Get("thread", raw)
	if result == nil {
		t.Fatal("expected cold-start load of updated frozen prefix")
	}
	if result.Cutoff != 300 || result.Tokens != 120 || result.RawTokens != 6000 {
		t.Fatalf("cold-start metadata = cutoff %d, tokens %d, raw tokens %d", result.Cutoff, result.Tokens, result.RawTokens)
	}
	want, err := json.Marshal(updated)
	if err != nil {
		t.Fatalf("marshal updated prefix: %v", err)
	}
	got, err := json.Marshal(result.Messages)
	if err != nil {
		t.Fatalf("marshal restored prefix: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("cold-start bytes differ\ngot:  %s\nwant: %s", got, want)
	}
}

func TestFrozenBoundaryHashCoversCompleteMessage(t *testing.T) {
	tests := []struct {
		name   string
		before json.RawMessage
		after  json.RawMessage
	}{
		{
			name:   "第 201 字节后文本变化",
			before: mustMarshal(strings.Repeat("a", 220) + "before"),
			after:  mustMarshal(strings.Repeat("a", 220) + "after"),
		},
		{
			name:   "第二个文本块变化",
			before: json.RawMessage(`[{"type":"text","text":"same"},{"type":"text","text":"before"}]`),
			after:  json.RawMessage(`[{"type":"text","text":"same"},{"type":"text","text":"after"}]`),
		},
		{
			name:   "tool_result 内容变化",
			before: json.RawMessage(`[{"type":"tool_result","tool_use_id":"tool-1","content":"before"}]`),
			after:  json.RawMessage(`[{"type":"tool_result","tool_use_id":"tool-1","content":"after"}]`),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			frozen := NewFrozenStubs()
			current := frozenTestMessages(3)
			current[1].Content = tt.before
			frozen.Store("thread", current[:1], 2, current[1], 10, 20)
			current[1].Content = tt.after
			if got := frozen.Get("thread", current); got != nil {
				t.Fatal("完整 boundary 内容变化后仍命中 frozen")
			}
		})
	}
}

func TestFrozenBoundaryIgnoresNonSemanticContentBlockShapeChanges(t *testing.T) {
	t.Run("省略字段与显式 null 等价", func(t *testing.T) {
		frozen := NewFrozenStubs()
		current := frozenTestMessages(3)
		current[1] = Message{Role: "user", Content: json.RawMessage(`[{"type":"tool_result","tool_use_id":"tool-1","content":"same"}]`)}
		frozen.Store("thread", current[:1], 2, current[1], 10, 20)

		current[1].Content = json.RawMessage(`[{"type":"tool_result","tool_use_id":"tool-1","input":null,"content":"same"}]`)
		if got := frozen.Get("thread", current); got == nil {
			t.Fatal("仅出现无语义的 input:null 后 frozen boundary 不应失效")
		}
	})

	t.Run("真实 tool_result 内容编辑仍失效", func(t *testing.T) {
		frozen := NewFrozenStubs()
		current := frozenTestMessages(3)
		current[1] = Message{Role: "user", Content: json.RawMessage(`[{"type":"tool_result","tool_use_id":"tool-1","content":"before"}]`)}
		frozen.Store("thread", current[:1], 2, current[1], 10, 20)

		current[1].Content = json.RawMessage(`[{"type":"tool_result","tool_use_id":"tool-1","input":null,"content":"after"}]`)
		if got := frozen.Get("thread", current); got != nil {
			t.Fatal("真实 tool_result 内容编辑后 frozen boundary 仍命中")
		}
	})

	t.Run("未知扩展字段显式 null 保持语义敏感", func(t *testing.T) {
		frozen := NewFrozenStubs()
		current := frozenTestMessages(3)
		current[1] = Message{Role: "user", Content: json.RawMessage(`[{"type":"tool_result","tool_use_id":"tool-1","content":"same"}]`)}
		frozen.Store("thread", current[:1], 2, current[1], 10, 20)

		current[1].Content = json.RawMessage(`[{"type":"tool_result","tool_use_id":"tool-1","content":"same","future_semantic":null}]`)
		if got := frozen.Get("thread", current); got != nil {
			t.Fatal("未知扩展字段从 absent 变为显式 null 后 frozen boundary 仍命中")
		}
	})
}

func TestStableBoundaryHashPreservesUnprovenNullAndUnknownFields(t *testing.T) {
	base := Message{Role: "user", Content: json.RawMessage(`[{"type":"tool_result","content":["same",null]}]`)}
	withArrayValue := Message{Role: "user", Content: json.RawMessage(`[{"type":"tool_result","content":["same","changed"]}]`)}
	if stableBoundaryHash(base) == stableBoundaryHash(withArrayValue) {
		t.Fatal("数组中的 null 被错误规范化或忽略")
	}

	withUnknownNonNull := Message{Role: "user", Content: json.RawMessage(`[{"type":"tool_result","content":["same",null],"future_semantic":"value"}]`)}
	if stableBoundaryHash(base) == stableBoundaryHash(withUnknownNonNull) {
		t.Fatal("未知非 null 扩展字段被错误规范化或忽略")
	}

	toolUseAbsentInput := Message{Role: "assistant", Content: json.RawMessage(`[{"type":"tool_use","id":"tool-1","name":"Read"}]`)}
	toolUseNullInput := Message{Role: "assistant", Content: json.RawMessage(`[{"type":"tool_use","id":"tool-1","name":"Read","input":null}]`)}
	if stableBoundaryHash(toolUseAbsentInput) == stableBoundaryHash(toolUseNullInput) {
		t.Fatal("tool_use 的显式 input:null 被错误视为字段省略")
	}
}

func TestFrozenColdStartRejectsInvalidCutoff(t *testing.T) {
	current := frozenTestMessages(3)
	prefix := deepCopyMessages(current[:1])
	prefixJSON, err := json.Marshal(prefix)
	if err != nil {
		t.Fatal(err)
	}
	for _, cutoff := range []int{-1, 0, 4} {
		t.Run(fmt.Sprintf("cutoff=%d", cutoff), func(t *testing.T) {
			persisted, err := json.Marshal(frozenPersisted{
				Messages: prefix, Cutoff: cutoff,
				BoundaryHash: stableBoundaryHash(current[1]),
				PrefixHash:   sha256hex(prefixJSON), Tokens: 10, RawTokens: 20,
			})
			if err != nil {
				t.Fatal(err)
			}
			frozen := NewFrozenStubs()
			frozen.SetLoadFunc(func(string) (string, bool) { return string(persisted), true })
			if got := frozen.Get("thread", current); got != nil {
				t.Fatalf("非法 cutoff=%d 不应恢复 frozen", cutoff)
			}
			if got := frozen.LengthFor("thread"); got != 0 {
				t.Fatalf("非法状态进入内存，长度=%d", got)
			}
		})
	}
}

func TestFrozenConcurrentPersistenceKeepsStateOrder(t *testing.T) {
	frozen := NewFrozenStubs()
	raw := frozenTestMessages(4)
	firstPersistStarted := make(chan struct{})
	releaseFirstPersist := make(chan struct{})
	var once sync.Once
	var persisted string
	frozen.SetPersistFunc(func(_ string, value string) {
		once.Do(func() {
			close(firstPersistStarted)
			<-releaseFirstPersist
		})
		persisted = value
	})

	firstDone := make(chan struct{})
	go func() {
		frozen.Store("thread", raw[:1], 2, raw[1], 10, 20)
		close(firstDone)
	}()
	<-firstPersistStarted
	secondDone := make(chan struct{})
	go func() {
		frozen.Store("thread", raw[:2], 3, raw[2], 20, 30)
		close(secondDone)
	}()
	close(releaseFirstPersist)
	<-firstDone
	<-secondDone

	var got frozenPersisted
	if err := json.Unmarshal([]byte(persisted), &got); err != nil {
		t.Fatalf("解析最终持久化状态: %v", err)
	}
	if got.Cutoff != 3 || len(got.Messages) != 2 {
		t.Fatalf("最终持久化状态过期: cutoff=%d messages=%d", got.Cutoff, len(got.Messages))
	}
	result := frozen.Get("thread", raw)
	if result == nil || result.Cutoff != got.Cutoff || len(result.Messages) != len(got.Messages) {
		t.Fatalf("内存与持久化状态不一致: result=%+v persisted=%+v", result, got)
	}
}

func TestFrozenBoundaryInvalidationDeletesPersistedStateOnce(t *testing.T) {
	persisted := make(map[string]string)
	source := NewFrozenStubs()
	source.SetPersistFunc(func(key, value string) { persisted[key] = value })
	current := frozenTestMessages(3)
	source.Store("thread", current[:1], 2, current[1], 10, 20)

	loadCalls := 0
	deleteCalls := 0
	restored := NewFrozenStubs()
	restored.SetLoadFunc(func(key string) (string, bool) {
		loadCalls++
		value, ok := persisted[key]
		return value, ok
	})
	restored.SetDeleteFunc(func(key string) {
		deleteCalls++
		delete(persisted, key)
	})
	current[1].Content = mustMarshal("edited boundary")
	if got := restored.Get("thread", current); got != nil {
		t.Fatal("boundary 不匹配时不应返回 frozen")
	}
	if _, ok := persisted["frozen:thread"]; ok {
		t.Fatal("boundary 不匹配后持久化 frozen 状态未删除")
	}
	if got := restored.Get("thread", current); got != nil {
		t.Fatal("重复 Get 不应恢复已失效 frozen")
	}
	if loadCalls != 1 || deleteCalls != 1 {
		t.Fatalf("失效状态重复加载或删除: load=%d delete=%d", loadCalls, deleteCalls)
	}
}

func frozenTestMessages(count int) []Message {
	messages := make([]Message, count)
	for i := range messages {
		role := "user"
		if i%2 == 1 {
			role = "assistant"
		}
		messages[i] = Message{Role: role, Content: mustMarshal(fmt.Sprintf("message-%03d", i))}
	}
	return messages
}

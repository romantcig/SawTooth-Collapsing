package proxy

import (
	"bytes"
	"encoding/json"
	"fmt"
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

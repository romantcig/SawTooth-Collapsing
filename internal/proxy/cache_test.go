package proxy

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestCacheFrozenFreezeRestorePrefixJSONBytesMatch(t *testing.T) {
	raw := frozenTestMessages(302)
	prefix := deepCopyMessages(raw[:5])
	prefix[2].Content = json.RawMessage(`[{"type":"text","text":"embedded","cache_control":{"type":"ephemeral","ttl":"1h"}}]`)

	freezePrefix := deepCopyMessages(prefix)
	if err := StripMessagesCacheControl(freezePrefix); err != nil {
		t.Fatalf("strip freeze prefix cache_control: %v", err)
	}
	frozen := NewFrozenStubs()
	frozen.Store("thread", freezePrefix, 300, raw[299], 120, 6000)
	if err := InjectFrozenBoundaryBreakpoint(freezePrefix, frozen.LengthFor("thread")); err != nil {
		t.Fatalf("inject freeze boundary breakpoint: %v", err)
	}
	freezeBytes, err := json.Marshal(freezePrefix)
	if err != nil {
		t.Fatalf("marshal freeze prefix: %v", err)
	}

	result := frozen.Get("thread", raw)
	if result == nil {
		t.Fatal("expected frozen result for restore turn")
	}
	restorePrefix := result.Messages
	if err := StripMessagesCacheControl(restorePrefix); err != nil {
		t.Fatalf("strip restore prefix cache_control: %v", err)
	}
	if err := InjectFrozenBoundaryBreakpoint(restorePrefix, len(result.Messages)); err != nil {
		t.Fatalf("inject restore boundary breakpoint: %v", err)
	}
	restoreBytes, err := json.Marshal(restorePrefix)
	if err != nil {
		t.Fatalf("marshal restore prefix: %v", err)
	}

	if !bytes.Equal(freezeBytes, restoreBytes) {
		t.Fatalf("freeze and restore frozen prefix bytes differ\nfreeze:  %s\nrestore: %s", freezeBytes, restoreBytes)
	}
	if got := countBreakpoints(restorePrefix); got != 1 {
		t.Fatalf("restore breakpoint count = %d, want 1", got)
	}
}

func TestCacheFrozenSnapshotExcludesPrependedExternalMessages(t *testing.T) {
	raw := frozenTestMessages(302)
	prefix := deepCopyMessages(raw[:5])
	frozen := NewFrozenStubs()
	frozen.Store("thread", prefix, 300, raw[299], 120, 6000)

	external := []Message{
		{Role: "user", Content: mustMarshal("external briefing")},
		{Role: "assistant", Content: mustMarshal("external acknowledgement")},
	}
	outgoing := append(deepCopyMessages(external), prefix...)
	if len(outgoing) != len(external)+len(prefix) {
		t.Fatalf("outgoing length = %d, want %d", len(outgoing), len(external)+len(prefix))
	}

	result := frozen.Get("thread", raw)
	if result == nil {
		t.Fatal("expected stored pre-insert frozen snapshot")
	}
	stored, err := json.Marshal(result.Messages)
	if err != nil {
		t.Fatalf("marshal stored prefix: %v", err)
	}
	want, err := json.Marshal(prefix)
	if err != nil {
		t.Fatalf("marshal expected prefix: %v", err)
	}
	if !bytes.Equal(stored, want) {
		t.Fatalf("stored frozen snapshot includes or lost external messages\nstored: %s\nwant:   %s", stored, want)
	}
	if bytes.Contains(stored, []byte("external briefing")) {
		t.Fatal("external prepended message entered frozen snapshot")
	}
}

package proxy

import (
	"bytes"
	"encoding/json"
	"sync"
	"testing"
	"time"
)

func TestApplyCacheControlConcurrentTTLUpdates(t *testing.T) {
	s := NewServer(DefaultConfig())
	s.Frozen = NewFrozenStubs()
	s.Sawtooth = NewSawtoothTrigger(time.Minute, 1000, 500)
	messages := []Message{{Role: "user", Content: json.RawMessage(`[{"type":"text","text":"cached"}]`)}}

	var wg sync.WaitGroup
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.applyCacheControl(deepCopyMessages(messages), 1, "session")
		}()
	}
	wg.Wait()
	if s.cachedTTL == "" {
		t.Fatal("并发 cache TTL 更新后未记录生效值")
	}
}

func TestValidateConfigNormalizesCacheTTL(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Cache.CacheTTL = " 1h "
	validateConfig(&cfg)
	if cfg.Cache.CacheTTL != "1h" {
		t.Fatalf("cache_ttl=%q, want 1h", cfg.Cache.CacheTTL)
	}

	cfg.Cache.CacheTTL = "1hr"
	validateConfig(&cfg)
	if cfg.Cache.CacheTTL != "ephemeral" {
		t.Fatalf("非法 cache_ttl 未回退: %q", cfg.Cache.CacheTTL)
	}
}

func TestNormalizeCacheTTLRejectsUnsupportedValue(t *testing.T) {
	messages := []Message{{Role: "user", Content: json.RawMessage(`[{"type":"text","text":"cached","cache_control":{"type":"ephemeral"}}]`)}}
	if err := NormalizeCacheTTL(messages, "5m"); err == nil {
		t.Fatal("非法 cache TTL 应返回错误")
	}
}

func TestApplyCacheControlDoesNotMutateActiveToolPair(t *testing.T) {
	s := NewServer(DefaultConfig())
	s.Frozen = NewFrozenStubs()
	assistantContent := json.RawMessage(`[{"type":"thinking","thinking":"signed","signature":"sig","cache_control":{"type":"ephemeral","ttl":"1h"}},{"type":"tool_use","id":"active","name":"Read","input":{"file_path":"main.go"},"future":true}]`)
	resultContent := json.RawMessage(`[{"type":"tool_result","tool_use_id":"active","content":"current","cache_control":{"type":"ephemeral"},"future_result":true}]`)
	messages := []Message{
		{Role: "user", Content: json.RawMessage(`[{"type":"text","text":"stable history","cache_control":{"type":"ephemeral"}}]`)},
		{Role: "assistant", Content: assistantContent},
		{Role: "user", Content: resultContent},
	}

	s.applyCacheControl(messages, len(messages), "session")
	if !bytes.Equal(messages[1].Content, assistantContent) {
		t.Fatalf("cache 管理改写活动 assistant:\n got: %s\nwant: %s", messages[1].Content, assistantContent)
	}
	if !bytes.Equal(messages[2].Content, resultContent) {
		t.Fatalf("cache 管理改写活动 tool_result:\n got: %s\nwant: %s", messages[2].Content, resultContent)
	}
	if countBreakpoints(messages[:1]) != 1 {
		t.Fatal("稳定历史 boundary 未保留单一 breakpoint")
	}
}

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

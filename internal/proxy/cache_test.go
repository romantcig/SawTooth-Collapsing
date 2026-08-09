package proxy

import (
	"bytes"
	"encoding/json"
	"sync"
	"testing"
	"time"
)

func TestTriggerEvaluation(t *testing.T) {
	now := time.Now()
	tests := []struct {
		name             string
		selectedPressure int
		lastRequestAgo   time.Duration
		wantReason       TriggerReason
		wantActualKnown  bool
	}{
		{
			name:             "emergency 优先于 token 与 pause",
			selectedPressure: 11_001,
			lastRequestAgo:   5 * time.Minute,
			wantReason:       TriggerEmergency,
			wantActualKnown:  true,
		},
		{
			name:             "tokens 优先于 pause",
			selectedPressure: 1_001,
			lastRequestAgo:   5 * time.Minute,
			wantReason:       TriggerTokens,
			wantActualKnown:  true,
		},
		{
			name:             "pause 使用本次等待快照",
			selectedPressure: 501,
			lastRequestAgo:   5 * time.Minute,
			wantReason:       TriggerPause,
			wantActualKnown:  true,
		},
		{
			name:             "minimum 相等时不触发 pause",
			selectedPressure: 500,
			lastRequestAgo:   5 * time.Minute,
			wantReason:       TriggerNone,
			wantActualKnown:  true,
		},
		{
			name:             "没有历史时间时 actual wait unavailable",
			selectedPressure: 501,
			wantReason:       TriggerNone,
			wantActualKnown:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			trigger := NewSawtoothTrigger(CacheGapForTTL("ephemeral"), 1_000, 500)
			if tt.lastRequestAgo > 0 {
				trigger.mu.Lock()
				trigger.lastRequestTime["thread"] = now.Add(-tt.lastRequestAgo)
				trigger.mu.Unlock()
			}

			evaluation := trigger.Evaluate("thread", tt.selectedPressure, now)
			if evaluation.Reason != tt.wantReason {
				t.Fatalf("Evaluate reason=%q, want %q", evaluation.Reason, tt.wantReason)
			}
			if evaluation.RequiredWait != 4*time.Minute {
				t.Fatalf("RequiredWait=%s, want 4m", evaluation.RequiredWait)
			}
			if evaluation.ActualWaitKnown != tt.wantActualKnown {
				t.Fatalf("ActualWaitKnown=%v, want %v", evaluation.ActualWaitKnown, tt.wantActualKnown)
			}
			if tt.wantActualKnown {
				if evaluation.ActualWait != tt.lastRequestAgo {
					t.Fatalf("ActualWait=%s, want %s", evaluation.ActualWait, tt.lastRequestAgo)
				}
			} else if evaluation.ActualWait != 0 {
				t.Fatalf("unknown ActualWait=%s, want zero", evaluation.ActualWait)
			}
			if evaluation.SelectedPressure != tt.selectedPressure {
				t.Fatalf("SelectedPressure=%d, want %d", evaluation.SelectedPressure, tt.selectedPressure)
			}
			if evaluation.EmergencyThreshold != 11_000 || evaluation.TokenThreshold != 1_000 || evaluation.TokenMinimum != 500 {
				t.Fatalf("threshold snapshot=%+v", evaluation)
			}
			if got := trigger.ShouldTrigger("thread", tt.selectedPressure); got != tt.wantReason {
				t.Fatalf("ShouldTrigger=%q, want %q", got, tt.wantReason)
			}
		})
	}
}

func TestTriggerEvaluationStrictPriority(t *testing.T) {
	now := time.Now()
	tests := []struct {
		name             string
		selectedPressure int
		lastRequestAgo   time.Duration
		wantReason       TriggerReason
	}{
		{name: "超过 emergency", selectedPressure: 11_001, lastRequestAgo: 5 * time.Minute, wantReason: TriggerEmergency},
		{name: "等于 emergency 后落到 tokens", selectedPressure: 11_000, lastRequestAgo: 5 * time.Minute, wantReason: TriggerTokens},
		{name: "超过 tokens", selectedPressure: 1_001, lastRequestAgo: 5 * time.Minute, wantReason: TriggerTokens},
		{name: "等于 tokens 后落到 pause", selectedPressure: 1_000, lastRequestAgo: 5 * time.Minute, wantReason: TriggerPause},
		{name: "等于 minimum", selectedPressure: 500, lastRequestAgo: 5 * time.Minute, wantReason: TriggerNone},
		{name: "等于 required wait", selectedPressure: 501, lastRequestAgo: 4 * time.Minute, wantReason: TriggerNone},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			trigger := NewSawtoothTrigger(4*time.Minute, 1_000, 500)
			trigger.mu.Lock()
			trigger.lastRequestTime["thread"] = now.Add(-tt.lastRequestAgo)
			trigger.mu.Unlock()

			if got := trigger.Evaluate("thread", tt.selectedPressure, now).Reason; got != tt.wantReason {
				t.Fatalf("reason=%q, want %q", got, tt.wantReason)
			}
		})
	}
}

func TestTriggerEvaluationCurrentAndNextTTL(t *testing.T) {
	now := time.Now()
	trigger := NewSawtoothTrigger(CacheGapForTTL("ephemeral"), 1_000, 500)
	trigger.mu.Lock()
	trigger.lastRequestTime["thread"] = now.Add(-5 * time.Minute)
	trigger.mu.Unlock()

	current := trigger.Evaluate("thread", 501, now)
	trigger.SetPauseThreshold(CacheGapForTTL("1h"))
	next := trigger.Evaluate("thread", 501, now)

	if current.Reason != TriggerPause || current.RequiredWait != 4*time.Minute || current.ActualWait != 5*time.Minute || !current.ActualWaitKnown {
		t.Fatalf("current evaluation=%+v", current)
	}
	if next.Reason != TriggerNone || next.RequiredWait != 61*time.Minute || next.ActualWait != 5*time.Minute || !next.ActualWaitKnown {
		t.Fatalf("next evaluation=%+v", next)
	}
	if current.RequiredWait != 4*time.Minute {
		t.Fatalf("TTL 更新污染已完成 evaluation: %+v", current)
	}

	trigger.mu.Lock()
	trigger.lastRequestTime["thread"] = now.Add(-62 * time.Minute)
	trigger.mu.Unlock()
	oneHour := trigger.Evaluate("thread", 501, now)
	if oneHour.Reason != TriggerPause || oneHour.RequiredWait != 61*time.Minute || oneHour.ActualWait != 62*time.Minute {
		t.Fatalf("1h evaluation=%+v", oneHour)
	}
}

func TestCacheGapForTTL(t *testing.T) {
	tests := []struct {
		cacheTTL string
		want     time.Duration
	}{
		{cacheTTL: "ephemeral", want: 4 * time.Minute},
		{cacheTTL: "1h", want: 61 * time.Minute},
		{cacheTTL: "", want: 4 * time.Minute},
	}

	for _, tt := range tests {
		t.Run(tt.cacheTTL, func(t *testing.T) {
			if got := CacheGapForTTL(tt.cacheTTL); got != tt.want {
				t.Fatalf("CacheGapForTTL(%q)=%s, want %s", tt.cacheTTL, got, tt.want)
			}
		})
	}
}

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

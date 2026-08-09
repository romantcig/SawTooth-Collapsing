package proxy

import (
	"bytes"
	"encoding/json"
	"testing"
)

// These tests intentionally exercise the request-local plan seam rather than
// the legacy tracker compatibility wrappers.  They are the RED gate for the
// CompactionPlan/DecayEvaluationSnapshot implementation.

func TestCompactionPlanRunLengths(t *testing.T) {
	for _, tc := range []struct {
		name          string
		runLength     int
		wantReplaces  int
		wantBlocks    int
		wantStage2Run bool
	}{
		{name: "one", runLength: 1, wantReplaces: 0, wantBlocks: 0, wantStage2Run: true},
		{name: "forty-nine", runLength: 49, wantReplaces: 0, wantBlocks: 0, wantStage2Run: true},
		{name: "fifty", runLength: 50, wantReplaces: 2, wantBlocks: 2, wantStage2Run: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			messages := planFixtureMessages(tc.runLength + 2)
			tracker := NewDecayTracker()
			for i := 1; i <= tc.runLength; i++ {
				tracker.MarkStubbed("plan", i, 1, 0)
			}
			snapshot := tracker.BuildDecayEvaluationSnapshot("plan", []string(nil), 200, len(messages), 3)
			plan := BuildCompactionPlan(snapshot, messages, messages, 0, true)
			if got := plan.ReplacementCount(); got != tc.wantReplaces {
				t.Fatalf("replacement count=%d, want %d", got, tc.wantReplaces)
			}
			decayed, _ := tracker.ApplyDecayBatch(messages, "plan", 300, 100, nil, "", 200, plan)
			result, blocks := CompactMessagesWithPlan(decayed, messages, plan)
			if len(blocks) != tc.wantBlocks {
				t.Fatalf("blocks=%d, want %d", len(blocks), tc.wantBlocks)
			}
			if tc.wantStage2Run {
				for i := 1; i <= tc.runLength; i++ {
					if got := extractTextFromContent(result[i].Content); got == "" {
						t.Fatalf("index %d lost Stage-2 fallback content", i)
					}
				}
			}
		})
	}
}

func TestCompactionPlanProtectedUnion(t *testing.T) {
	messages := planFixtureMessages(70)
	tracker := NewDecayTracker()
	for i := 1; i < len(messages); i++ {
		intensity := 0.0
		if i == 20 {
			intensity = 0.95
		}
		tracker.MarkStubbed("protected", i, 1, intensity)
	}
	tracker.SetFilePath("protected", 10, "src/pinned.go")
	pinned := NewPinnedPathSnapshot([]string{"pinned.go"})
	snapshot := tracker.BuildDecayEvaluationSnapshot("protected", pinned, 200, len(messages), 3)
	plan := BuildCompactionPlan(snapshot, messages, messages, 8, true)
	for _, idx := range []int{0, 10, 20, len(messages) - 1, len(messages) - 8} {
		if !plan.IsProtected(idx) {
			t.Fatalf("index %d not in protected union", idx)
		}
	}
}

func TestCompactDisabledDegradesStage3ToStage2(t *testing.T) {
	messages := planFixtureMessages(55)
	tracker := NewDecayTracker()
	for i := 1; i < 51; i++ {
		tracker.MarkStubbed("disabled", i, 1, 0)
	}
	snapshot := tracker.BuildDecayEvaluationSnapshot("disabled", nil, 200, len(messages), 3)
	plan := BuildCompactionPlan(snapshot, messages, messages, 0, false)
	if plan.EffectiveEnabled {
		t.Fatal("compact-disabled plan is effective")
	}
	decayed, _ := tracker.ApplyDecayBatch(messages, "disabled", 300, 100, nil, "", 200, plan)
	result, blocks := CompactMessagesWithPlan(decayed, messages, plan)
	if len(blocks) != 0 {
		t.Fatalf("compact-disabled produced %d blocks", len(blocks))
	}
	for i := 1; i < 51; i++ {
		if extractTextFromContent(result[i].Content) == "" {
			t.Fatalf("compact-disabled index %d lost content", i)
		}
	}
}

func TestCompactionPlanMaterializationFallback(t *testing.T) {
	messages := planFixtureMessages(55)
	tracker := NewDecayTracker()
	for i := 1; i < 51; i++ {
		tracker.MarkStubbed("failure", i, 1, 0)
	}
	snapshot := tracker.BuildDecayEvaluationSnapshot("failure", nil, 200, len(messages), 3)
	plan := BuildCompactionPlan(snapshot, messages, messages, 0, true)
	if plan.ReplacementCount() == 0 {
		t.Fatal("fixture did not create a replacement")
	}
	// Deliberately corrupt the immutable-layout copy to prove the materializer
	// restores pre-decay Stage-2 content instead of emitting an empty marker.
	plan.Replacements[0].StartIdx = len(messages) + 10
	decayed, _ := tracker.ApplyDecayBatch(messages, "failure", 300, 100, nil, "", 200, plan)
	result, blocks := CompactMessagesWithPlan(decayed, messages, plan)
	if len(blocks) != 0 {
		t.Fatalf("failed materialization reported %d blocks", len(blocks))
	}
	for i := 1; i < 51; i++ {
		if extractTextFromContent(result[i].Content) == "" {
			t.Fatalf("materialization failure left empty content at %d", i)
		}
	}
}

func TestCompactionPlanPinnedSnapshotConcurrent(t *testing.T) {
	tracker := NewDecayTracker()
	tracker.MarkStubbed("same", 1, 1, 0)
	tracker.SetFilePath("same", 1, "old.go")
	paths := []string{"old.go"}
	pinned := NewPinnedPathSnapshot(paths)
	paths[0] = "mutated-after-snapshot.go"
	snapshot := tracker.BuildDecayEvaluationSnapshot("same", pinned, 200, 2, 3)
	start := make(chan struct{})
	done := make(chan struct{})
	go func() {
		<-start
		tracker.SetFilePath("same", 1, "new.go")
		tracker.SetPinnedPaths([]string{"new.go"})
		close(done)
	}()
	close(start)
	<-done
	plan := BuildCompactionPlan(snapshot, planFixtureMessages(2), planFixtureMessages(2), 0, true)
	if !plan.SnapshotPinned("old.go") {
		t.Fatal("request-local pinned snapshot changed after concurrent tracker update")
	}
}

func TestCompactionPlanCoordinatesStable(t *testing.T) {
	messages := planFixtureMessages(60)
	tracker := NewDecayTracker()
	for i := 1; i < 51; i++ {
		tracker.MarkStubbed("coords", i, 1, 0)
	}
	snapshot := tracker.BuildDecayEvaluationSnapshot("coords", nil, 200, len(messages), 3)
	plan := BuildCompactionPlan(snapshot, messages, messages, 0, true)
	decayed, _ := tracker.ApplyDecayBatch(messages, "coords", 300, 100, nil, "", 200, plan)
	result, blocks := CompactMessagesWithPlan(decayed, messages, plan)
	if len(blocks) == 0 || len(result) >= len(messages) {
		t.Fatalf("expected compacted result, result=%d blocks=%d", len(result), len(blocks))
	}
	for _, block := range blocks {
		if block.StartIdx < 0 || block.EndIdx < block.StartIdx || block.EndIdx >= len(messages) {
			t.Fatalf("invalid block coordinates: %+v", block)
		}
	}
}

func planFixtureMessages(n int) []Message {
	messages := make([]Message, n)
	for i := range messages {
		role := "assistant"
		if i%2 == 0 {
			role = "user"
		}
		content, _ := json.Marshal("stage-two payload " + string(rune('a'+i%26)) + " " + string(bytes.Repeat([]byte("x"), 120)))
		messages[i] = Message{Role: role, Content: content}
	}
	return messages
}

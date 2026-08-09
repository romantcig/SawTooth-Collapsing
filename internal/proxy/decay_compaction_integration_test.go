package proxy

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// 这些测试刻意覆盖 request-local plan 接缝，而不是旧 tracker 兼容包装层；
// 它们共同封闭 CompactionPlan/DecayEvaluationSnapshot 的信息安全边界。

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

func TestDecayCompactionIntegrationCoverage(t *testing.T) {
	t.Run("六十条候选被 pinned 与 intensity 切成短 run", func(t *testing.T) {
		messages := planFixtureMessages(64)
		tracker := NewDecayTracker()
		for i := 1; i <= 60; i++ {
			intensity := 0.0
			if i == 40 {
				intensity = 0.95
			}
			tracker.MarkStubbed("split", i, 0, intensity)
		}
		tracker.SetFilePath("split", 20, "C:/repo/pinned.go")
		snapshot := tracker.BuildDecayEvaluationSnapshot("split", NewPinnedPathSnapshot([]string{"pinned.go"}), 1_000, len(messages), 3)
		plan := BuildCompactionPlan(snapshot, messages, messages, 3, true)
		if !plan.IsProtected(20) || !plan.IsProtected(40) {
			t.Fatalf("保护并集缺少 pinned/intensity 边界: protected=%v", plan.Protected)
		}
		if plan.ReplacementCount() != 0 || len(plan.Runs) != 0 {
			t.Fatalf("被保护项切开的短 run 不应进入 replacement plan: runs=%v replacements=%v", plan.Runs, plan.Replacements)
		}
		decayed, _ := tracker.ApplyDecayBatch(messages, "split", 300, 100, nil, "", 1_000, plan)
		result, blocks := CompactMessagesWithPlan(decayed, messages, plan)
		if len(blocks) != 0 || len(result) != len(messages) {
			t.Fatalf("短 run 意外物化: result=%d blocks=%d", len(result), len(blocks))
		}
		for i := 1; i <= 60; i++ {
			if extractTextFromContent(result[i].Content) == "" {
				t.Fatalf("plan 外 Stage-3 候选在索引 %d 未降级到 Stage-2", i)
			}
		}
	})

	t.Run("两个五十条 run 与 system 邻居", func(t *testing.T) {
		messages := planFixtureMessages(103)
		messages[0].Role = "system"
		messages[25].Role = "system"
		messages[51].Role = "system"
		messages[77].Role = "system"
		messages[102].Role = "system"
		tracker := NewDecayTracker()
		for i := 1; i <= 101; i++ {
			intensity := 0.0
			if i == 51 {
				intensity = 0.95
			}
			tracker.MarkStubbed("multi", i, 0, intensity)
		}
		snapshot := tracker.BuildDecayEvaluationSnapshot("multi", nil, 1_000, len(messages), 3)
		plan := BuildCompactionPlan(snapshot, messages, messages, 0, true)
		if len(plan.Runs) != 2 || plan.ReplacementCount() != 2 {
			t.Fatalf("多 run plan=%v replacements=%v，want 2/2", plan.Runs, plan.Replacements)
		}
		decayed, _ := tracker.ApplyDecayBatch(messages, "multi", 300, 100, nil, "", 1_000, plan)
		result, blocks := CompactMessagesWithPlan(decayed, messages, plan)
		if len(blocks) != 2 || len(result) >= len(messages) {
			t.Fatalf("多 run 物化失败: result=%d blocks=%v", len(result), blocks)
		}
		for _, block := range blocks {
			if block.Role != "user" && block.Role != "assistant" {
				t.Fatalf("system role 泄漏到 CompactedBlock: %+v", block)
			}
			for i := block.StartIdx; i <= block.EndIdx; i++ {
				if !plan.IsCovered(i) {
					t.Fatalf("CompactedBlock 覆盖了 plan 外坐标 %d: %+v", i, block)
				}
			}
		}
	})

	t.Run("左右角色冲突生成两条 replacement", func(t *testing.T) {
		messages := planFixtureMessages(52)
		messages[0].Role = "user"
		messages[51].Role = "assistant"
		tracker := NewDecayTracker()
		for i := 1; i <= 50; i++ {
			tracker.MarkStubbed("roles", i, 0, 0)
		}
		snapshot := tracker.BuildDecayEvaluationSnapshot("roles", nil, 1_000, len(messages), 3)
		plan := BuildCompactionPlan(snapshot, messages, messages, 0, true)
		if plan.ReplacementCount() != 2 || plan.Replacements[0].Role != "assistant" || plan.Replacements[1].Role != "user" {
			t.Fatalf("role conflict replacement=%+v", plan.Replacements)
		}
	})

	for _, keepRecent := range []int{0, 1, 5, 8} {
		t.Run("keep-recent-"+string(rune('0'+keepRecent)), func(t *testing.T) {
			messages := planFixtureMessages(62)
			tracker := NewDecayTracker()
			for i := 1; i < len(messages); i++ {
				tracker.MarkStubbed("recent", i, 0, 0)
			}
			snapshot := tracker.BuildDecayEvaluationSnapshot("recent", nil, 1_000, len(messages), 3)
			plan := BuildCompactionPlan(snapshot, messages, messages, keepRecent, true)
			if plan.ReplacementCount() == 0 {
				t.Fatalf("KeepRecent=%d 意外消除了全部 eligible run", keepRecent)
			}
			for i := len(messages) - keepRecent; i < len(messages); i++ {
				if i >= 0 && !plan.IsProtected(i) {
					t.Fatalf("KeepRecent=%d 未保护索引 %d", keepRecent, i)
				}
			}
		})
	}

	t.Run("活动 tool pair 不进入 replacement", func(t *testing.T) {
		messages := planFixtureMessages(60)
		messages[58] = Message{Role: "assistant", Content: json.RawMessage(`[{"type":"tool_use","id":"active-plan","name":"Read","input":{"file_path":"active.go"}}]`)}
		messages[59] = Message{Role: "user", Content: json.RawMessage(`[{"type":"tool_result","tool_use_id":"active-plan","content":"still active"}]`)}
		tracker := NewDecayTracker()
		for i := 1; i < len(messages); i++ {
			tracker.MarkStubbed("tool-pair", i, 0, 0)
		}
		snapshot := tracker.BuildDecayEvaluationSnapshot("tool-pair", nil, 1_000, len(messages), 3)
		plan := BuildCompactionPlan(snapshot, messages, messages, 0, true)
		if !plan.IsProtected(58) || !plan.IsProtected(59) || plan.IsCovered(58) || plan.IsCovered(59) {
			t.Fatalf("活动 tool pair 保护失败: protected=%v replacements=%v", plan.Protected, plan.Replacements)
		}
		decayed, _ := tracker.ApplyDecayBatch(messages, "tool-pair", 300, 100, nil, "", 1_000, plan)
		result, _ := CompactMessagesWithPlan(decayed, messages, plan)
		useIndex, resultIndex := -1, -1
		for i, message := range result {
			blocks, _ := parseContent(message.Content)
			for _, block := range blocks {
				if block.Type == "tool_use" && block.ID == "active-plan" {
					useIndex = i
				}
				if block.Type == "tool_result" && block.ToolUseID == "active-plan" {
					resultIndex = i
				}
			}
		}
		if useIndex < 0 || resultIndex != useIndex+1 {
			t.Fatalf("活动 tool pair 次序被破坏: use=%d result=%d", useIndex, resultIndex)
		}
	})
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
	// 故意破坏 immutable layout 副本，证明物化器会恢复衰减前 Stage-2
	// 内容，而不是输出空 marker 掩盖信息丢失。
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

func TestCompactionPlanStage2FallbackFrozenPrefixBytesStable(t *testing.T) {
	const sessionID = "stage2-fallback-frozen"
	var forwarded []Message
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Messages []Message `json:"messages"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("解析实际上游请求失败: %v", err)
			return
		}
		forwarded = deepCopyMessages(body.Messages)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"type":"message","usage":{"input_tokens":100,"output_tokens":1}}`)
	}))
	defer upstream.Close()

	server := newPipelineTestServer(t, upstream.URL)
	server.Config.Stubify.KeepRecent = 8
	server.Config.Collapse.CompactEnabled = true
	history := planFixtureMessages(58)
	history[0].Content = mustMarshal("受保护的超大首消息 " + strings.Repeat("heavy-context ", 12_000))
	threshold := server.Config.Stubify.TokenThreshold
	if total := server.TokenCounter.CountMessagesTokens(history); total <= threshold {
		t.Fatalf("fixture 总 token=%d，必须高于压缩阈值 %d", total, threshold)
	}
	tokenFloor := threshold / 2
	if tokenFloor < 10_000 {
		tokenFloor = 10_000
	}
	if cutoff := CalcCollapseCutoff(history, tokenFloor, server.TokenCounter, server.Config.Stubify.KeepRecent); cutoff > 0 {
		t.Fatalf("fixture 意外进入 collapse 主路径，cutoff=%d", cutoff)
	}

	stateKey := historyEpochStateKey(sessionID, 1)
	for i := 1; i <= 49; i++ {
		server.DecayTracker.MarkStubbed(stateKey, i, -1_000, 0)
	}
	var persistErr error
	server.Frozen.SetPersistFunc(func(key, value string) {
		if persistErr == nil {
			persistErr = server.Store.PersistState(key, value)
		}
	})
	server.Frozen.SetLoadFunc(server.Store.LoadState)

	servePipelineRequest(t, server, sessionID, history)
	if persistErr != nil {
		t.Fatalf("持久化 Frozen 失败: %v", persistErr)
	}
	if len(forwarded) != len(history) {
		t.Fatalf("49 条短 run 不应生成 replacement：forwarded=%d original=%d", len(forwarded), len(history))
	}
	for i := 1; i <= 49; i++ {
		text := extractTextFromContent(forwarded[i].Content)
		if text == "" {
			t.Fatalf("Stage-2 fallback 在索引 %d 丢失正文", i)
		}
		if bytes.Equal(forwarded[i].Content, history[i].Content) {
			t.Fatalf("索引 %d 未实际进入 Stage-2 fallback", i)
		}
		if strings.Contains(text, "[Compacted:") {
			t.Fatalf("49 条短 run 在索引 %d 伪造了 CompactedBlock", i)
		}
	}

	stored := server.Frozen.Get(stateKey, history)
	if stored == nil {
		t.Fatal("当轮 Frozen 内存快照不存在")
	}
	wireBytes, err := json.Marshal(forwarded)
	if err != nil {
		t.Fatal(err)
	}
	storedBytes, err := json.Marshal(stored.Messages)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(wireBytes, storedBytes) {
		t.Fatalf("实际上游与当轮 Frozen prefix bytes 不一致\nwire:   %s\nstored: %s", wireBytes, storedBytes)
	}

	coldFrozen := NewFrozenStubs()
	coldFrozen.SetLoadFunc(server.Store.LoadState)
	cold := coldFrozen.Get(stateKey, history)
	if cold == nil {
		t.Fatal("SQLite 冷恢复未返回 Frozen prefix")
	}
	coldBytes, err := json.Marshal(cold.Messages)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(wireBytes, coldBytes) {
		t.Fatalf("实际上游与 SQLite 冷恢复 prefix bytes 不一致\nwire: %s\ncold: %s", wireBytes, coldBytes)
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

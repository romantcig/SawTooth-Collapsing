package proxy

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

// mainStateSnapshot 汇总一次请求可能触碰到的全部主线有状态资源。
// 全部字段均为可比较值类型，便于直接用 == 做前后快照比对。
type mainStateSnapshot struct {
	FrozenLen    int
	Archives     int
	RequestSeq   int
	Baseline     pressureBaseline
	DecayEntries int
}

func snapshotMainState(t *testing.T, server *Server, sessionID string) mainStateSnapshot {
	t.Helper()

	server.DecayTracker.mu.RLock()
	decayEntries := len(server.DecayTracker.stubbedAt)
	server.DecayTracker.mu.RUnlock()

	return mainStateSnapshot{
		FrozenLen:    server.Frozen.LengthFor(sessionID),
		Archives:     archiveCount(t, server.Store),
		RequestSeq:   server.Sawtooth.GetRequestSeq(sessionID),
		Baseline:     server.Sawtooth.PressureBaseline(sessionID),
		DecayEntries: decayEntries,
	}
}

// TestHandleMessagesInterleavedAgentStreamsIsolateMainState 复现 CC 2.1.220 的真实并发
// 形态：main 与 5 个 Task subagent 共用同一个 X-Claude-Code-Session-Id，6 条互不相干的
// 历史交错到达同一个状态机。
//
// 生产 trace（FIX_PLAN.md 1.1）中，代理原有的三条 subagent 识别路径全部失效
// （billing header 被关闭、agentContext 从不进 body、无 stream:false 旁路查询），
// 152 个请求全部被判成 main，导致 114/152（75%）请求触发 epoch 变更、common_prefix
// 归零，主线的 baseline / Frozen / Decay 状态每轮被冲刷。
//
// 本测试锁定修复后的不变量：带 X-Claude-Code-Agent-Id 的流量在进入 HistoryEpoch gate
// 之前透明直通，主线状态不被任何 subagent 请求触碰。
func TestHandleMessagesInterleavedAgentStreamsIsolateMainState(t *testing.T) {
	const (
		sessionID           = "SHARED-SESSION-INTERLEAVED-9A7C2F"
		subagentSentinel    = "subagent-isolated-history"
		subagentUsageTokens = 191_000
		mainUsageTokens     = 4_000
	)
	// 取自生产 trace 的真实 agent id（FIX_PLAN.md 1.1 表格）。
	agentIDs := []string{
		"ac3d8abb21a98f939",
		"ae62648d28a17ee1a",
		"a74453d0a5cd80b06",
		"a9c7f0dda3165134a",
		"a1affcdb69ecfa2b5",
	}

	var forwardedAgentIDs []string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("读取上游请求失败: %v", err)
			return
		}
		// 按请求体内容而非 header 区分来源，使 usage 断言不依赖 header 是否透传。
		usage := mainUsageTokens
		if bytes.Contains(body, []byte(subagentSentinel)) {
			usage = subagentUsageTokens
			forwardedAgentIDs = append(forwardedAgentIDs, r.Header.Get("X-Claude-Code-Agent-Id"))
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"type":"message","usage":{"input_tokens":%d,"output_tokens":1}}`, usage)
	}))
	defer upstream.Close()

	server := newPipelineTestServer(t, upstream.URL)

	// subagent 在 HistoryEpoch gate 之前就返回，永远不会走到 Archive 召回。
	// 因此该回调的调用次数本身就是"进入有状态管线的请求数"。
	type epochObservation struct {
		Epoch   uint64
		Changed bool
	}
	var mainObservations []epochObservation
	server.searchAndExpandFn = func(messages []Message, _ *SQLiteStore, _ int, _ *TokenCounter, _ *Budget, meta *requestMeta) RecallOutcome {
		mainObservations = append(mainObservations, epochObservation{
			Epoch:   meta.HistoryEpoch,
			Changed: meta.HistoryEpochChanged,
		})
		return RecallOutcome{Messages: messages}
	}

	// main 流：足够大以触发一次真实 collapse，从而建立 Frozen / Archive / baseline。
	mainHistory := pipelineMessages(300, 80)
	appendMainTurn := func(turn int) {
		mainHistory = append(mainHistory,
			Message{Role: "user", Content: mustMarshal(fmt.Sprintf("main-stream-turn-%d follow-up question", turn))},
			Message{Role: "assistant", Content: mustMarshal(fmt.Sprintf("main-stream-answer-%d", turn))},
		)
	}

	// 每条 subagent 历史都与 main 完全不相干——若它们进入 epoch gate，
	// common prefix 必为 0，必然推进 epoch 并冲刷主线状态。
	subagentHistory := func(agentID string) []Message {
		return []Message{
			{Role: "user", Content: mustMarshal(subagentSentinel + " task brief for " + agentID)},
			{Role: "assistant", Content: mustMarshal(subagentSentinel + " working on " + agentID)},
		}
	}

	serveSubagent := func(agentID string) {
		t.Helper()
		before := snapshotMainState(t, server, sessionID)
		servePipelineRequestWith(t, server, sessionID, subagentHistory(agentID), nil, map[string]string{
			"X-Claude-Code-Agent-Id": agentID,
		})
		after := snapshotMainState(t, server, sessionID)
		if before != after {
			t.Fatalf("subagent %s 触碰了主线状态:\nbefore=%+v\nafter =%+v", agentID, before, after)
		}
	}

	// main#1 —— 建立主线状态。
	servePipelineRequest(t, server, sessionID, mainHistory)
	established := snapshotMainState(t, server, sessionID)
	if established.FrozenLen == 0 || established.Archives == 0 || established.RequestSeq == 0 {
		t.Fatalf("main 首轮未建立可观测状态，后续隔离断言将失去意义: %+v", established)
	}

	// 交错到达：每两次 main 之间至少插入一次 subagent。
	serveSubagent(agentIDs[0])
	serveSubagent(agentIDs[1])

	appendMainTurn(2)
	servePipelineRequest(t, server, sessionID, mainHistory)

	serveSubagent(agentIDs[2])
	serveSubagent(agentIDs[3])

	appendMainTurn(3)
	servePipelineRequest(t, server, sessionID, mainHistory)

	serveSubagent(agentIDs[4])
	serveSubagent(agentIDs[0]) // 同一 subagent 二次到达

	appendMainTurn(4)
	servePipelineRequest(t, server, sessionID, mainHistory)

	// 1. 只有 main 请求进入有状态管线。
	if len(mainObservations) != 4 {
		t.Fatalf("进入有状态管线的请求数=%d, want 4（仅 main）", len(mainObservations))
	}

	// 2. main 的 epoch 只在首次建立时变化一次，此后恒定。
	if !mainObservations[0].Changed {
		t.Fatalf("main 首轮未建立 epoch: %+v", mainObservations[0])
	}
	for i, observation := range mainObservations[1:] {
		if observation.Changed {
			t.Fatalf("main 第 %d 轮 epoch 被重建——subagent 冲刷了主线状态机: %+v", i+2, observation)
		}
		if observation.Epoch != mainObservations[0].Epoch {
			t.Fatalf("main 第 %d 轮 epoch=%d, want 恒定 %d", i+2, observation.Epoch, mainObservations[0].Epoch)
		}
	}

	// 3. subagent 响应的 usage 从未写入主线 pressure baseline。
	if baseline := server.Sawtooth.PressureBaseline(sessionID); baseline.ActualTokens == subagentUsageTokens {
		t.Fatalf("subagent 响应 usage 污染了主线 pressure baseline: %+v", baseline)
	}

	// 4. agent-id header 原样转发上游（FIX_PLAN.md 2026-07-28 决定 1：不剥离）。
	wantAgentIDs := []string{agentIDs[0], agentIDs[1], agentIDs[2], agentIDs[3], agentIDs[4], agentIDs[0]}
	if len(forwardedAgentIDs) != len(wantAgentIDs) {
		t.Fatalf("上游收到的 subagent 请求数=%d, want %d", len(forwardedAgentIDs), len(wantAgentIDs))
	}
	for i, want := range wantAgentIDs {
		if forwardedAgentIDs[i] != want {
			t.Fatalf("第 %d 个 subagent 请求转发的 agent-id=%q, want %q", i+1, forwardedAgentIDs[i], want)
		}
	}
}

package proxy

import (
	"path/filepath"
	"strings"
	"testing"
)

// 本文件是 D-21/D-22 的端到端闭环证据：真实压缩 → canonical marker → 同 session
// 恢复原文，并证明跨 session、旧分支与失败提交都不可能隐式召回。

// newRecoveryPipelineServer 复刻生产接线中与恢复相关的部分：Archive 提交与
// history transition 共用同一个 Store，epoch 前进时旧分支归档才会真正被隔离。
// dbPath 为空时使用测试自带的临时库。
func newRecoveryPipelineServer(t *testing.T, upstreamURL, dbPath string) (*Server, *recordingOutcomeDispatcher) {
	t.Helper()
	server, sink := newOutcomePipelineServer(t, upstreamURL)
	if dbPath != "" {
		store, err := NewSQLiteStore(dbPath)
		if err != nil {
			t.Fatalf("NewSQLiteStore(%s): %v", dbPath, err)
		}
		server.Store = store
	}
	server.HistoryEpoch.SetTransitionFunc(server.Store.CommitHistoryTransition)
	return server, sink
}

// recoveryRequestMessages 在既有历史尾部追加一条显式恢复请求，
// 文本里同时含恢复意图短语与生产 marker 常量生成的 canonical 引用。
func recoveryRequestMessages(base []Message, canonicalID string) []Message {
	messages := append([]Message(nil), base...)
	return append(messages,
		Message{Role: "assistant", Content: mustMarshal("上一轮历史已折叠")},
		Message{Role: "user", Content: mustMarshal("请恢复存档 " + formatArchiveRecoveryMarker(canonicalID))},
	)
}

// firstArchiveRecoveryID 跑一轮真实压缩并返回 wire 上的唯一 canonical 引用。
func firstArchiveRecoveryID(t *testing.T, server *Server, captured *[][]Message, sessionID string, base []Message) string {
	t.Helper()
	serveOutcomeMessages(t, server, sessionID, base)
	if len(*captured) == 0 {
		t.Fatal("首轮未发出上游请求")
	}
	ids := archiveRecoveryIDsIn(joinedMessageText(t, (*captured)[len(*captured)-1]))
	if len(ids) == 0 {
		t.Fatal("首轮真实压缩未产生 canonical 恢复引用")
	}
	summary, found, err := server.Store.GetVisibleArchiveByID(sessionID, ids[0])
	if err != nil || !found {
		t.Fatalf("canonical 引用不可按同 session 找回: found=%v err=%v", found, err)
	}
	if summary.SessionID != sessionID || summary.MessagesJSON == "" {
		t.Fatalf("canonical 引用未指向本 session 原文: %+v", summary)
	}
	return ids[0]
}

func recallPayloadCount(text string) int {
	return strings.Count(text, "[Retrieved archive #")
}

func TestArchiveRecoveryE2ESameSessionRoundTrip(t *testing.T) {
	var captured [][]Message
	upstream := capturingUpstream(t, &captured)
	server, sink := newRecoveryPipelineServer(t, upstream.URL, "")

	const sessionID = "archive-recovery-roundtrip"
	base := pipelineMessages(80, 260)
	canonicalID := firstArchiveRecoveryID(t, server, &captured, sessionID, base)

	// 第二轮只验证恢复：抬高阈值使本轮不再触发破坏性压缩。
	server.Config.Stubify.TokenThreshold = 50_000_000
	serveOutcomeMessages(t, server, sessionID, recoveryRequestMessages(base, canonicalID))

	snapshots := sink.all()
	if len(snapshots) != 2 {
		t.Fatalf("outcome 数=%d，want 2", len(snapshots))
	}
	recovery := snapshots[1]
	if recovery.RecallAttempted == 0 || recovery.RecallInjected != 1 {
		t.Fatalf("exact ID 恢复未汇入 closure: %+v", recovery)
	}
	wire := joinedMessageText(t, captured[len(captured)-1])
	if got := recallPayloadCount(wire); got != 1 {
		t.Fatalf("wire 上的恢复载荷数=%d，want 1:\n%s", got, wire)
	}
}

func TestArchiveRecoveryE2ECrossSessionIsInvisible(t *testing.T) {
	var captured [][]Message
	upstream := capturingUpstream(t, &captured)
	server, sink := newRecoveryPipelineServer(t, upstream.URL, "")

	const sessionID = "archive-recovery-owner"
	base := pipelineMessages(80, 260)
	canonicalID := firstArchiveRecoveryID(t, server, &captured, sessionID, base)

	server.Config.Stubify.TokenThreshold = 50_000_000
	serveOutcomeMessages(t, server, "archive-recovery-stranger", recoveryRequestMessages(base, canonicalID))

	snapshots := sink.all()
	stranger := snapshots[len(snapshots)-1]
	if stranger.RecallCandidates != 0 || stranger.RecallInjected != 0 {
		t.Fatalf("跨 session 隐式召回: %+v", stranger)
	}
	wire := joinedMessageText(t, captured[len(captured)-1])
	if got := recallPayloadCount(wire); got != 0 {
		t.Fatalf("跨 session wire 出现恢复载荷 %d 条:\n%s", got, wire)
	}
	if _, found, err := server.Store.GetVisibleArchiveByID("archive-recovery-stranger", canonicalID); err != nil || found {
		t.Fatalf("canonical ID 跨 session 可见: found=%v err=%v", found, err)
	}
}

func TestArchiveRecoveryE2EOldBranchIsInvisible(t *testing.T) {
	var captured [][]Message
	upstream := capturingUpstream(t, &captured)
	server, sink := newRecoveryPipelineServer(t, upstream.URL, "")

	const sessionID = "archive-recovery-branch"
	base := pipelineMessages(80, 260)
	canonicalID := firstArchiveRecoveryID(t, server, &captured, sessionID, base)

	// 用户撤回并改写第 4 条：raw history 在此分叉，权威 epoch 前进，
	// 不完整落在新公共前缀内的旧归档一律不可见。
	server.Config.Stubify.TokenThreshold = 50_000_000
	diverged := append([]Message(nil), base...)
	diverged[3] = Message{Role: "assistant", Content: mustMarshal("分支后的不同回答")}
	serveOutcomeMessages(t, server, sessionID, diverged)

	serveOutcomeMessages(t, server, sessionID, recoveryRequestMessages(diverged, canonicalID))

	snapshots := sink.all()
	branch := snapshots[len(snapshots)-1]
	if branch.RecallCandidates != 0 || branch.RecallInjected != 0 {
		t.Fatalf("旧分支归档被隐式召回: %+v", branch)
	}
	wire := joinedMessageText(t, captured[len(captured)-1])
	if got := recallPayloadCount(wire); got != 0 {
		t.Fatalf("旧分支 wire 出现恢复载荷 %d 条:\n%s", got, wire)
	}
	if _, found, err := server.Store.GetVisibleArchiveByID(sessionID, canonicalID); err != nil || found {
		t.Fatalf("旧分支 canonical ID 仍可见: found=%v err=%v", found, err)
	}
}

func TestArchiveRecoveryE2EColdStartStillRecovers(t *testing.T) {
	var captured [][]Message
	upstream := capturingUpstream(t, &captured)
	dbPath := filepath.Join(tempDirRetryCleanup(t), "archive-recovery.db")

	const sessionID = "archive-recovery-coldstart"
	base := pipelineMessages(80, 260)

	first, _ := newRecoveryPipelineServer(t, upstream.URL, dbPath)
	canonicalID := firstArchiveRecoveryID(t, first, &captured, sessionID, base)
	if err := first.Store.Close(); err != nil {
		t.Fatalf("关闭首次 store: %v", err)
	}

	// 冷启动：全新进程状态，只有磁盘上的 Archive 还在。
	second, sink := newRecoveryPipelineServer(t, upstream.URL, dbPath)
	t.Cleanup(func() { _ = second.Store.Close() })
	second.Config.Stubify.TokenThreshold = 50_000_000

	serveOutcomeMessages(t, second, sessionID, recoveryRequestMessages(base, canonicalID))
	recovery := sink.sole(t)
	if recovery.RecallInjected != 1 {
		t.Fatalf("冷启动后 exact ID 恢复失败: %+v", recovery)
	}
	if got := recallPayloadCount(joinedMessageText(t, captured[len(captured)-1])); got != 1 {
		t.Fatalf("冷启动 wire 恢复载荷数=%d，want 1", got)
	}
}

func TestArchiveRecoveryE2EFailedCommitLeavesNoRecoverableID(t *testing.T) {
	var captured [][]Message
	upstream := capturingUpstream(t, &captured)
	server, _ := newRecoveryPipelineServer(t, upstream.URL, "")
	committer := &failingArchiveCommitter{}
	server.archiveCommitter = committer

	const sessionID = "archive-recovery-failed"
	serveOutcomeMessages(t, server, sessionID, pipelineMessages(80, 260))

	if committer.count() == 0 {
		t.Fatal("未尝试同步 Archive 提交")
	}
	wire := joinedMessageText(t, captured[len(captured)-1])
	if ids := archiveRecoveryIDsIn(wire); len(ids) != 0 {
		t.Fatalf("失败提交仍留下确定性 marker: %v", ids)
	}
	if got := archiveCount(t, server.Store); got != 0 {
		t.Fatalf("失败提交写入了 %d 条 Archive", got)
	}
}

// TestArchiveRecoveryMarkerWithoutIntentDoesNotReinject 证明 marker 只是"可恢复
// 的承诺"，没有明确恢复意图时绝不自动把原文塞回上下文——否则折叠等于白做。
func TestArchiveRecoveryMarkerWithoutIntentDoesNotReinject(t *testing.T) {
	var captured [][]Message
	upstream := capturingUpstream(t, &captured)
	server, sink := newRecoveryPipelineServer(t, upstream.URL, "")

	const sessionID = "archive-recovery-no-intent"
	base := pipelineMessages(80, 260)
	canonicalID := firstArchiveRecoveryID(t, server, &captured, sessionID, base)

	server.Config.Stubify.TokenThreshold = 50_000_000
	silent := append([]Message(nil), base...)
	silent = append(silent,
		Message{Role: "assistant", Content: mustMarshal("上一轮历史已折叠 " + formatArchiveRecoveryMarker(canonicalID))},
		Message{Role: "user", Content: mustMarshal("继续实现下一个函数")},
	)
	serveOutcomeMessages(t, server, sessionID, silent)

	quiet := sink.all()[1]
	if quiet.RecallInjected != 0 {
		t.Fatalf("无恢复意图时自动重注入原文: %+v", quiet)
	}
	if got := recallPayloadCount(joinedMessageText(t, captured[len(captured)-1])); got != 0 {
		t.Fatalf("无恢复意图 wire 出现恢复载荷 %d 条", got)
	}
}

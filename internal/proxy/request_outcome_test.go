package proxy

import (
	"bytes"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

type outcomeDispatchFake struct {
	mu     sync.Mutex
	called int
	last   requestOutcomeSnapshot
}

func (f *outcomeDispatchFake) TryDispatch(snapshot requestOutcomeSnapshot) outcomeDispatchResult {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.called++
	f.last = snapshot
	return outcomeDispatchResult{Accepted: true, TerminalAccepted: true, SessionAccepted: true}
}

func (f *outcomeDispatchFake) snapshot() (int, requestOutcomeSnapshot) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.called, f.last
}

func attrsJSON(t *testing.T, attrs []slog.Attr) []byte {
	t.Helper()
	var buf bytes.Buffer
	handler := slog.NewJSONHandler(&buf, &slog.HandlerOptions{ReplaceAttr: func(_ []string, attr slog.Attr) slog.Attr {
		return attr
	}})
	record := slog.NewRecord(time.Unix(0, 0).UTC(), slog.LevelInfo, "outcome", 0)
	record.AddAttrs(attrs...)
	if err := handler.Handle(nil, record); err != nil {
		t.Fatalf("序列化 outcome attrs 失败: %v", err)
	}
	return buf.Bytes()
}

func TestRequestOutcomeSnapshotDefaults(t *testing.T) {
	started := time.Date(2026, 8, 9, 2, 0, 0, 123, time.UTC)
	fullSessionID := "PARENT-SESSION-AND-CREDENTIAL-MUST-NOT-LEAK"
	collector := newRequestOutcomeCollector(42, fullSessionID, started, nil)

	snapshot := collector.Snapshot()
	if snapshot.RequestID != 42 {
		t.Fatalf("request id=%d, want 42", snapshot.RequestID)
	}
	if snapshot.SessionHash != stableSessionHash(fullSessionID) {
		t.Fatalf("session hash=%q, want %q", snapshot.SessionHash, stableSessionHash(fullSessionID))
	}
	if !snapshot.StartedAt.Equal(started) {
		t.Fatalf("started_at=%s, want %s", snapshot.StartedAt, started)
	}
	if !snapshot.FinishedAt.IsZero() {
		t.Fatalf("未 seal 的 finished_at=%s，不应有终态", snapshot.FinishedAt)
	}
	if snapshot.Eligibility != outcomeEligibilityNotEvaluable {
		t.Fatalf("默认 AI eligibility=%q", snapshot.Eligibility)
	}
	if snapshot.TerminalEligibility != outcomeEligibilityNotApplicable {
		t.Fatalf("默认 terminal eligibility=%q", snapshot.TerminalEligibility)
	}
	if snapshot.Action != outcomeActionPassthrough {
		t.Fatalf("默认 action=%q", snapshot.Action)
	}
	if snapshot.UpstreamState != upstreamStateNotStarted {
		t.Fatalf("默认 upstream=%q", snapshot.UpstreamState)
	}
	if snapshot.MemoryState != persistenceStateNotAttempted || snapshot.DiskState != persistenceStateNotAttempted {
		t.Fatalf("默认 persistence memory=%q disk=%q", snapshot.MemoryState, snapshot.DiskState)
	}
	if snapshot.FailureClass != persistenceFailureUnknown {
		t.Fatalf("默认 failure=%q", snapshot.FailureClass)
	}
	if snapshot.Intervention != interventionNone {
		t.Fatalf("默认 intervention=%q", snapshot.Intervention)
	}

	collector.SetEligibility(outcomeEligibility("untrusted-value"))
	collector.SetTerminalEligibility(outcomeEligibility("untrusted-value"))
	collector.SetTriggerReason(TriggerReason("untrusted-value"))
	collector.SetAction(outcomeAction("untrusted-value"))
	collector.SetUpstreamState(upstreamState("untrusted-value"))
	collector.SetMemoryState(persistenceState("untrusted-value"))
	collector.SetDiskState(persistenceState("untrusted-value"))
	collector.SetFailureClass(persistenceFailureClass("untrusted-value"))
	collector.SetIntervention(interventionState("untrusted-value"))

	snapshot = collector.Snapshot()
	if snapshot.Eligibility != outcomeEligibilityUnknown || snapshot.TerminalEligibility != outcomeEligibilityUnknown {
		t.Fatalf("未知 eligibility 未归一: ai=%q terminal=%q", snapshot.Eligibility, snapshot.TerminalEligibility)
	}
	if snapshot.TriggerReason != TriggerUnknown || snapshot.Action != outcomeActionUnknown {
		t.Fatalf("未知 trigger/action 未归一: trigger=%q action=%q", snapshot.TriggerReason, snapshot.Action)
	}
	if snapshot.UpstreamState != upstreamStateUnknown || snapshot.MemoryState != persistenceStateUnknown || snapshot.DiskState != persistenceStateUnknown {
		t.Fatalf("未知 state 未归一: upstream=%q memory=%q disk=%q", snapshot.UpstreamState, snapshot.MemoryState, snapshot.DiskState)
	}
	if snapshot.FailureClass != persistenceFailureUnknown || snapshot.Intervention != interventionUnknown {
		t.Fatalf("未知 failure/intervention 未归一: failure=%q intervention=%q", snapshot.FailureClass, snapshot.Intervention)
	}
}

func TestRequestOutcomeSafeAttrs(t *testing.T) {
	fullSessionID := "SESSION-ID-MUST-NOT-LEAK"
	collector := newRequestOutcomeCollector(77, fullSessionID, time.Unix(123, 456).UTC(), nil)
	collector.SetTerminalEligibility(outcomeEligibilityTerminalEligible)
	collector.SetEligibility(outcomeEligibilityEvaluable)
	collector.SetTriggerReason(TriggerTokens)
	collector.SetPressureSource(pressureSourceActualPlusDelta)
	collector.SetWait(5*time.Minute, 2*time.Minute, true)
	collector.SetAction(outcomeActionCollapse)
	collector.SetSizes(100, 20, 24000, 6000)
	collector.SetRecallCounts(1, 4, 2, 2, 2)
	collector.SetUpstreamState(upstreamStateSuccess)
	collector.SetUpstreamStatus(200)
	collector.SetMemoryState(persistenceStateSaved)
	collector.SetDiskState(persistenceStateFailed)
	collector.SetFailureClass(persistenceFailureSQLite)
	collector.SetIntervention(interventionRequired)

	data := attrsJSON(t, collector.Snapshot().SafeLogAttrs())
	if bytes.Contains(data, []byte(fullSessionID)) {
		t.Fatalf("safe attrs 泄漏完整 session: %s", data)
	}
	if !json.Valid(data) {
		t.Fatalf("safe attrs 不是 JSON: %s", data)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		t.Fatal(err)
	}
	for key := range fields {
		lower := strings.ToLower(key)
		for _, denied := range []string{"session_id", "parent", "authorization", "credential", "body", "base64", "fingerprint", "path", "key", "raw", "error_text"} {
			if strings.Contains(lower, denied) {
				t.Fatalf("safe attrs 出现敏感 key %q: %s", key, data)
			}
		}
	}
	if !bytes.Contains(data, []byte(`"session_hash"`)) {
		t.Fatalf("safe attrs 缺少短 session hash: %s", data)
	}
	if bytes.Contains(data, []byte("AUTHORIZATION-MUST-NOT-LEAK")) ||
		bytes.Contains(data, []byte("BODY-MUST-NOT-LEAK")) ||
		bytes.Contains(data, []byte("FULL-FINGERPRINT-MUST-NOT-LEAK")) {
		t.Fatalf("safe attrs 包含敏感 sentinel: %s", data)
	}
}

func TestRequestMetaInitializesOutcome(t *testing.T) {
	fullSessionID := "request-meta-session"
	meta := newRequestMeta(101, fullSessionID)
	if meta == nil || meta.Outcome == nil {
		t.Fatal("newRequestMeta 未初始化 Outcome")
	}
	if got := meta.Outcome.Snapshot(); got.RequestID != meta.ID || got.SessionHash != meta.SessionHash {
		t.Fatalf("Outcome identity 未固定: %+v meta=%+v", got, meta)
	}
	metaWithRun := newRequestMetaWithRun(102, fullSessionID, "0123456789abcdef")
	if metaWithRun == nil || metaWithRun.Outcome == nil {
		t.Fatal("newRequestMetaWithRun 未初始化 Outcome")
	}
	if metaWithRun.Logger == nil || metaWithRun.auxiliaryLogger() == nil {
		t.Fatal("request logger 路由属性被破坏")
	}
	if metaWithRun.Outcome.Snapshot().SessionHash != stableSessionHash(fullSessionID) {
		t.Fatal("Outcome 未使用 stableSessionHash")
	}
}

func TestRequestOutcomeSealAndComplete(t *testing.T) {
	t.Run("无异步操作在 seal 时立即提交", func(t *testing.T) {
		fake := &outcomeDispatchFake{}
		collector := newRequestOutcomeCollector(1, "seal-now", fake)
		collector.SetAction(outcomeActionDirect)
		if err := collector.SealProducers(); err != nil {
			t.Fatalf("SealProducers 失败: %v", err)
		}
		called, snapshot := fake.snapshot()
		if called != 1 {
			t.Fatalf("TryDispatch 次数=%d，want 1", called)
		}
		if snapshot.Action != outcomeActionDirect || snapshot.FinishedAt.IsZero() {
			t.Fatalf("终态未复制完整: %+v", snapshot)
		}
		before := snapshot
		_ = collector.SealProducers()
		called, after := fake.snapshot()
		if called != 1 || before != after {
			t.Fatalf("重复 seal 改变 closure: calls=%d before=%+v after=%+v", called, before, after)
		}
	})

	t.Run("completion 可在 seal 后完成且重复完成幂等", func(t *testing.T) {
		fake := &outcomeDispatchFake{}
		collector := newRequestOutcomeCollector(2, "late-completion", fake)
		completion, err := collector.BeginAsyncResult(outcomeCompletionKindDisk)
		if err != nil {
			t.Fatalf("登记 completion 失败: %v", err)
		}
		if err := collector.SealProducers(); err != nil {
			t.Fatal(err)
		}
		if called, _ := fake.snapshot(); called != 0 {
			t.Fatalf("pending completion 未完成却 dispatch=%d", called)
		}
		if err := completion.Complete(outcomeCompletionResult{State: persistenceStateSaved}); err != nil {
			t.Fatalf("completion 完成失败: %v", err)
		}
		if err := completion.Complete(outcomeCompletionResult{State: persistenceStateFailed}); err != nil {
			t.Fatalf("重复 completion 不应报错: %v", err)
		}
		called, snapshot := fake.snapshot()
		if called != 1 || snapshot.DiskState != persistenceStateSaved {
			t.Fatalf("completion 未 first-wins exactly once: calls=%d snapshot=%+v", called, snapshot)
		}
	})
}

func TestRequestOutcomeConcurrentFinalize(t *testing.T) {
	const completionCount = 100
	fake := &outcomeDispatchFake{}
	collector := newRequestOutcomeCollector(3, "concurrent-finalize", fake)
	completions := make([]*outcomeCompletion, 0, completionCount)
	for i := 0; i < completionCount; i++ {
		completion, err := collector.BeginAsyncResult(outcomeCompletionKindDisk)
		if err != nil {
			t.Fatalf("登记 completion %d 失败: %v", i, err)
		}
		completions = append(completions, completion)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := collector.SealProducers(); err != nil {
			t.Errorf("并发 seal 失败: %v", err)
		}
	}()
	for i, completion := range completions {
		i, completion := i, completion
		wg.Add(1)
		go func() {
			defer wg.Done()
			collector.SetSizes(i+1, i, i*100, i*10)
			if err := completion.Complete(outcomeCompletionResult{State: persistenceStateSaved}); err != nil && err != ErrOutcomeCompletionAlreadyFinalized {
				t.Errorf("completion %d 失败: %v", i, err)
			}
			_ = completion.Complete(outcomeCompletionResult{State: persistenceStateFailed})
		}()
	}
	wg.Wait()

	called, snapshot := fake.snapshot()
	if called != 1 {
		t.Fatalf("并发 finalize dispatch 次数=%d，want 1", called)
	}
	if snapshot.FinishedAt.IsZero() || snapshot.DiskState != persistenceStateSaved {
		t.Fatalf("并发终态不完整: %+v", snapshot)
	}

	// seal 后仍有 pending 时，普通 setter 不得再改变最终 immutable 事实。
	fake2 := &outcomeDispatchFake{}
	collector2 := newRequestOutcomeCollector(4, "sealed-immutable", fake2)
	completion, err := collector2.BeginAsyncResult(outcomeCompletionKindDisk)
	if err != nil {
		t.Fatal(err)
	}
	if err := collector2.SealProducers(); err != nil {
		t.Fatal(err)
	}
	before := collector2.Snapshot()
	collector2.SetAction(outcomeActionCollapse)
	if err := completion.Complete(outcomeCompletionResult{State: persistenceStateSaved}); err != nil {
		t.Fatal(err)
	}
	_, after := fake2.snapshot()
	if before.Action != after.Action {
		t.Fatalf("seal 后 setter 修改了 immutable closure: before=%q after=%q", before.Action, after.Action)
	}
}

func TestRequestOutcomeRejectsLateProducer(t *testing.T) {
	fake := &outcomeDispatchFake{}
	collector := newRequestOutcomeCollector(5, "late-producer", fake)
	completion, err := collector.BeginAsyncResult(outcomeCompletionKindDisk)
	if err != nil {
		t.Fatal(err)
	}
	if err := collector.SealProducers(); err != nil {
		t.Fatal(err)
	}
	before := collector.Snapshot()
	late, err := collector.BeginAsyncResult(outcomeCompletionKindDisk)
	if late != nil || !errorsIsOutcomeSealed(err) {
		t.Fatalf("seal 后登记未返回稳定 lifecycle error: completion=%v err=%v", late, err)
	}
	if err := completion.Complete(outcomeCompletionResult{State: persistenceStateSaved}); err != nil {
		t.Fatal(err)
	}
	called, after := fake.snapshot()
	if called != 1 {
		t.Fatalf("已有 completion 完成后 dispatch=%d，want 1", called)
	}
	if before.Action != after.Action || before.RequestID != after.RequestID || before.SessionHash != after.SessionHash {
		t.Fatalf("late producer 改变 snapshot identity: before=%+v after=%+v", before, after)
	}
}

func errorsIsOutcomeSealed(err error) bool {
	return err == ErrOutcomeProducersSealed
}

func TestRequestOutcomeHasNoDirectSinkOrGoroutine(t *testing.T) {
	data, err := os.ReadFile("request_outcome.go")
	if err != nil {
		t.Fatal(err)
	}
	file, err := parser.ParseFile(token.NewFileSet(), "request_outcome.go", data, 0)
	if err != nil {
		t.Fatal(err)
	}
	var forbidden string
	ast.Inspect(file, func(node ast.Node) bool {
		switch typed := node.(type) {
		case *ast.GoStmt:
			forbidden = "go statement"
		case *ast.ChanType:
			forbidden = "channel type"
		case *ast.CallExpr:
			if selector, ok := typed.Fun.(*ast.SelectorExpr); ok {
				switch selector.Sel.Name {
				case "NewTimer", "NewTicker", "After", "AfterFunc":
					forbidden = selector.Sel.Name
				}
			}
		case *ast.SelectorExpr:
			switch typed.Sel.Name {
			case "LogAttrs", "Handle":
				forbidden = typed.Sel.Name
			}
		case *ast.Ident:
			if typed.Name == "LogHandler" || typed.Name == "SessionLogHandler" || typed.Name == "CombinedLogHandler" {
				forbidden = typed.Name
			}
		}
		return true
	})
	if forbidden != "" {
		t.Fatalf("collector 直接依赖了禁止资源: %s", forbidden)
	}
}

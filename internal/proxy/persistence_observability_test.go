package proxy

import (
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// ── 测试用 backend 与 reporter ──

// persistenceFakeBackend 只实现 writer 允许调用的两个 state 方法。
// 它没有 SaveArchive：Archive 在类型层面就不可能进入 writer。
type persistenceFakeBackend struct {
	mu          sync.Mutex
	order       []string
	values      map[string][]string
	deletes     int
	inFlight    int
	maxInFlight int
	failKeys    map[string]bool

	gate     chan struct{}
	gateKeys map[string]bool
	started  map[string]chan struct{}
}

func newPersistenceFakeBackend() *persistenceFakeBackend {
	return &persistenceFakeBackend{
		values:   make(map[string][]string),
		failKeys: make(map[string]bool),
		gateKeys: make(map[string]bool),
		started:  make(map[string]chan struct{}),
	}
}

// blockKey 让指定 ordering key 的操作阻塞，直到 releaseAll 关闭 gate。
func (b *persistenceFakeBackend) blockKey(key string) <-chan struct{} {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.gate == nil {
		b.gate = make(chan struct{})
	}
	b.gateKeys[key] = true
	if b.started[key] == nil {
		b.started[key] = make(chan struct{})
	}
	return b.started[key]
}

func (b *persistenceFakeBackend) releaseAll() {
	b.mu.Lock()
	gate := b.gate
	b.gate = nil
	b.gateKeys = make(map[string]bool)
	b.mu.Unlock()
	if gate != nil {
		close(gate)
	}
}

func (b *persistenceFakeBackend) failKey(key string, fail bool) {
	b.mu.Lock()
	b.failKeys[key] = fail
	b.mu.Unlock()
}

func (b *persistenceFakeBackend) apply(key, value string, deleted bool) error {
	b.mu.Lock()
	b.order = append(b.order, key)
	b.values[key] = append(b.values[key], value)
	if deleted {
		b.deletes++
	}
	b.inFlight++
	if b.inFlight > b.maxInFlight {
		b.maxInFlight = b.inFlight
	}
	gate := b.gate
	blocked := b.gateKeys[key]
	if blocked {
		if started := b.started[key]; started != nil {
			select {
			case <-started:
			default:
				close(started)
			}
		}
	}
	shouldFail := b.failKeys[key]
	b.mu.Unlock()

	if blocked && gate != nil {
		<-gate
	}

	b.mu.Lock()
	b.inFlight--
	b.mu.Unlock()
	if shouldFail {
		return errors.New("forced backend failure")
	}
	return nil
}

func (b *persistenceFakeBackend) PersistState(key, value string) error {
	return b.apply(key, value, false)
}

func (b *persistenceFakeBackend) DeleteState(key string) error {
	return b.apply(key, "", true)
}

func (b *persistenceFakeBackend) snapshotOrder() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.order...)
}

// valuesFor 返回同 ordering key 的实际执行序列，用于证明 FIFO。
func (b *persistenceFakeBackend) valuesFor(key string) []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.values[key]...)
}

func (b *persistenceFakeBackend) deleteCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.deletes
}

func (b *persistenceFakeBackend) peakInFlight() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.maxInFlight
}

// persistenceSwitchBackend 在两个真实 SQLiteStore（一个可用、一个已关闭）之间切换，
// 让 saved/failed receipt 全部来自真实 SQLite 返回值。
type persistenceSwitchBackend struct {
	mu      sync.Mutex
	current PersistenceBackend
}

func (b *persistenceSwitchBackend) set(next PersistenceBackend) {
	b.mu.Lock()
	b.current = next
	b.mu.Unlock()
}

func (b *persistenceSwitchBackend) active() PersistenceBackend {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.current
}

func (b *persistenceSwitchBackend) PersistState(key, value string) error {
	return b.active().PersistState(key, value)
}

func (b *persistenceSwitchBackend) DeleteState(key string) error {
	return b.active().DeleteState(key)
}

type persistenceTestReporter struct {
	mu          sync.Mutex
	transitions []HealthTransition
}

func (r *persistenceTestReporter) ReportHealthTransition(transition HealthTransition) {
	r.mu.Lock()
	r.transitions = append(r.transitions, transition)
	r.mu.Unlock()
}

func (r *persistenceTestReporter) count(scope HealthScope, kind HealthTransitionKind) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	total := 0
	for _, transition := range r.transitions {
		if transition.Scope == scope && transition.Kind == kind {
			total++
		}
	}
	return total
}

func (r *persistenceTestReporter) total() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.transitions)
}

// ── 通用 helper ──

type persistenceProbe struct {
	collector  *requestOutcomeCollector
	completion *outcomeCompletion
}

// newPersistenceProbe 用真实 Plan 01 collector 承接 receipt，
// 因此断言的是最终 request closure，而不是 writer 的内部字段。
func newPersistenceProbe(t *testing.T, requestID uint64) *persistenceProbe {
	t.Helper()
	collector := newRequestOutcomeCollector(requestID, "persistence-session")
	completion, err := collector.BeginAsyncResult(outcomeCompletionKindDisk)
	if err != nil {
		t.Fatalf("BeginAsyncResult: %v", err)
	}
	if err := collector.SealProducers(); err != nil {
		t.Fatalf("SealProducers: %v", err)
	}
	return &persistenceProbe{collector: collector, completion: completion}
}

func (p *persistenceProbe) diskState() persistenceState {
	return p.collector.Snapshot().DiskState
}

func (p *persistenceProbe) failureClass() persistenceFailureClass {
	return p.collector.Snapshot().FailureClass
}

func waitForPersistence(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("等待 persistence 条件超时")
}

func newPersistenceWriterTest(t *testing.T, backend PersistenceBackend) (*PersistenceWriter, *HealthTracker, *persistenceTestReporter) {
	t.Helper()
	reporter := &persistenceTestReporter{}
	tracker := NewHealthTracker(reporter)
	writer := NewPersistenceWriter(backend, tracker, reporter)
	if !writer.Configured() {
		t.Fatal("显式依赖齐备时 writer 应可用")
	}
	return writer, tracker, reporter
}

// ── Task 1 测试 ──

func TestPersistenceWriterBoundedRequiresExplicitDependencies(t *testing.T) {
	backend := newPersistenceFakeBackend()
	reporter := &persistenceTestReporter{}
	tracker := NewHealthTracker(reporter)

	for name, writer := range map[string]*PersistenceWriter{
		"nil backend":  NewPersistenceWriter(nil, tracker, reporter),
		"nil tracker":  NewPersistenceWriter(backend, nil, reporter),
		"nil reporter": NewPersistenceWriter(backend, tracker, nil),
	} {
		if writer.Configured() {
			t.Fatalf("%s 不应产生可用 production writer", name)
		}
		if writer.WorkerCount() != 0 {
			t.Fatalf("%s 启动了 %d 个 worker", name, writer.WorkerCount())
		}
		probe := newPersistenceProbe(t, 1)
		receipt, err := writer.TrySubmit(PersistenceOp{
			Target: PersistenceTargetState, Kind: PersistenceOpPut,
			Key: "frozen:x", Value: "v", Completion: probe.completion,
		})
		if !errors.Is(err, ErrPersistenceWriterNotConfigured) {
			t.Fatalf("%s TrySubmit err=%v, want ErrPersistenceWriterNotConfigured", name, err)
		}
		if receipt.Accepted {
			t.Fatalf("%s 接受了提交", name)
		}
		if probe.diskState() != persistenceStateUnavailable || probe.failureClass() != persistenceFailureLifecycle {
			t.Fatalf("%s closure=%s/%s, want unavailable/lifecycle", name, probe.diskState(), probe.failureClass())
		}
		if err := writer.CloseAndDrain(); err != nil {
			t.Fatalf("%s CloseAndDrain: %v", name, err)
		}
	}

	if _, err := NewPersistenceWriterChecked(backend, nil, reporter); !errors.Is(err, ErrPersistenceWriterNotConfigured) {
		t.Fatalf("Checked 构造函数 err=%v, want ErrPersistenceWriterNotConfigured", err)
	}

	writer, gotTracker, gotReporter := newPersistenceWriterTest(t, backend)
	defer writer.CloseAndDrain()
	if writer.HealthTracker() != gotTracker {
		t.Fatal("writer 内部创建了第二套 health tracker")
	}
	if writer.Reporter() != HealthTransitionReporter(gotReporter) {
		t.Fatal("writer 未保留调用方传入的 reporter")
	}
}

func TestPersistenceWriterBoundedResources(t *testing.T) {
	if PersistenceWriterWorkers != 4 || PersistenceWriterQueueCapacity != 256 {
		t.Fatalf("固定资源常量=%d/%d, want 4/256", PersistenceWriterWorkers, PersistenceWriterQueueCapacity)
	}
	backend := newPersistenceFakeBackend()
	writer, _, _ := newPersistenceWriterTest(t, backend)
	if writer.WorkerCount() != PersistenceWriterWorkers {
		t.Fatalf("worker=%d, want %d", writer.WorkerCount(), PersistenceWriterWorkers)
	}

	baselineGoroutines := runtime.NumGoroutine()
	const submissions = 1000
	probes := make([]*persistenceProbe, submissions)
	accepted := make([]bool, submissions)
	var wg sync.WaitGroup
	for index := 0; index < submissions; index++ {
		probes[index] = newPersistenceProbe(t, uint64(index+1))
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			receipt, err := writer.TrySubmit(PersistenceOp{
				Target:      PersistenceTargetState,
				Kind:        PersistenceOpPut,
				OrderingKey: "frozen:key-" + string(rune('a'+index%8)),
				Key:         "frozen:key-" + string(rune('a'+index%8)),
				Value:       "value",
				Completion:  probes[index].completion,
			})
			if err != nil {
				t.Errorf("并发提交返回错误: %v", err)
			}
			accepted[index] = receipt.Accepted
		}(index)
	}
	wg.Wait()

	if got := writer.MaxQueueLength(); got > PersistenceWriterQueueCapacity {
		t.Fatalf("queue 峰值=%d, want <= %d", got, PersistenceWriterQueueCapacity)
	}
	if err := writer.CloseAndDrain(); err != nil {
		t.Fatalf("CloseAndDrain: %v", err)
	}
	if peak := backend.peakInFlight(); peak > PersistenceWriterWorkers {
		t.Fatalf("并发执行峰值=%d, want <= %d", peak, PersistenceWriterWorkers)
	}

	acceptedCount := 0
	for index, probe := range probes {
		state := probe.diskState()
		if accepted[index] {
			acceptedCount++
			if state != persistenceStateSaved {
				t.Fatalf("已接受 op %d 结果=%s, want saved", index, state)
			}
			continue
		}
		if state != persistenceStateUnavailable || probe.failureClass() != persistenceFailureQueueFull {
			t.Fatalf("被拒 op %d 结果=%s/%s, want unavailable/queue_full", index, state, probe.failureClass())
		}
	}
	if acceptedCount == 0 {
		t.Fatal("1000 并发提交全部被拒绝，测试无法证明有界性")
	}

	// drain 之后不应残留 per-op goroutine。
	waitForPersistence(t, func() bool { return runtime.NumGoroutine() <= baselineGoroutines+2 })
}

func TestPersistenceWriterBoundedQueueFullCompletesUnavailable(t *testing.T) {
	backend := newPersistenceFakeBackend()
	started := backend.blockKey("frozen:blocked")
	writer, _, reporter := newPersistenceWriterTest(t, backend)

	head := newPersistenceProbe(t, 1)
	if receipt, err := writer.TrySubmit(PersistenceOp{
		Target: PersistenceTargetState, Kind: PersistenceOpPut,
		OrderingKey: "frozen:blocked", Key: "frozen:blocked", Value: "v", Completion: head.completion,
	}); err != nil || !receipt.Accepted {
		t.Fatalf("首个 op receipt=%+v err=%v", receipt, err)
	}
	<-started

	for index := 0; index < PersistenceWriterQueueCapacity; index++ {
		probe := newPersistenceProbe(t, uint64(index+2))
		receipt, err := writer.TrySubmit(PersistenceOp{
			Target: PersistenceTargetState, Kind: PersistenceOpPut,
			OrderingKey: "frozen:blocked", Key: "frozen:blocked", Value: "v", Completion: probe.completion,
		})
		if err != nil || !receipt.Accepted {
			t.Fatalf("第 %d 个 op 应在容量内被接受: receipt=%+v err=%v", index, receipt, err)
		}
	}
	if got := writer.QueueLength(); got != PersistenceWriterQueueCapacity {
		t.Fatalf("queue 深度=%d, want %d", got, PersistenceWriterQueueCapacity)
	}

	overflow := newPersistenceProbe(t, 9001)
	receipt, err := writer.TrySubmit(PersistenceOp{
		Target: PersistenceTargetState, Kind: PersistenceOpPut,
		OrderingKey: "frozen:blocked", Key: "frozen:blocked", Value: "v", Completion: overflow.completion,
	})
	if err != nil {
		t.Fatalf("queue full 不是 lifecycle 错误: %v", err)
	}
	if receipt.Accepted {
		t.Fatal("超出容量的提交被接受")
	}
	if receipt.Result.State != persistenceStateUnavailable || receipt.Result.FailureClass != persistenceFailureQueueFull {
		t.Fatalf("queue full receipt=%+v, want unavailable/queue_full", receipt.Result)
	}
	if overflow.diskState() != persistenceStateUnavailable {
		t.Fatalf("queue full 未立即完成 completion: %s", overflow.diskState())
	}
	if writer.QueueFullCount() != 1 {
		t.Fatalf("queue full 计数=%d, want 1", writer.QueueFullCount())
	}
	// queue full 不是 SQLite 结果，不得触碰任一 SQLite health scope。
	if reporter.total() != 0 {
		t.Fatalf("queue full 产生了 %d 条 health transition", reporter.total())
	}

	backend.releaseAll()
	if err := writer.CloseAndDrain(); err != nil {
		t.Fatalf("CloseAndDrain: %v", err)
	}
}

func TestPersistenceWriterBoundedRejectsArchiveTarget(t *testing.T) {
	backend := newPersistenceFakeBackend()
	writer, _, reporter := newPersistenceWriterTest(t, backend)
	defer writer.CloseAndDrain()

	for _, target := range []PersistenceTarget{"archive", "sqlite_archive", "history_transition", ""} {
		probe := newPersistenceProbe(t, 1)
		receipt, err := writer.TrySubmit(PersistenceOp{
			Target: target, Kind: PersistenceOpPut,
			Key: "frozen:x", Value: "v", Completion: probe.completion,
		})
		if !errors.Is(err, ErrPersistenceWriterUnsupportedTarget) {
			t.Fatalf("target=%q err=%v, want ErrPersistenceWriterUnsupportedTarget", target, err)
		}
		if receipt.Accepted {
			t.Fatalf("target=%q 被接受进队列", target)
		}
		if receipt.Result.FailureClass == persistenceFailureQueueFull {
			t.Fatalf("target=%q 被伪装成 queue_full", target)
		}
		if receipt.Result.FailureClass != persistenceFailureInvalidInput {
			t.Fatalf("target=%q class=%s, want invalid_input", target, receipt.Result.FailureClass)
		}
		if probe.diskState() != persistenceStateUnavailable {
			t.Fatalf("target=%q closure=%s, want unavailable", target, probe.diskState())
		}
	}
	if writer.QueueLength() != 0 || writer.MaxQueueLength() != 0 {
		t.Fatalf("非法 target 占用了 queue: len=%d max=%d", writer.QueueLength(), writer.MaxQueueLength())
	}
	if writer.QueueFullCount() != 0 {
		t.Fatalf("非法 target 产生了 queue full 计数: %d", writer.QueueFullCount())
	}
	if reporter.total() != 0 {
		t.Fatalf("非法 target 改变了 SQLite health: %d 条 transition", reporter.total())
	}
}

func TestPersistenceWriterFIFOPreservesAcceptedOrder(t *testing.T) {
	backend := newPersistenceFakeBackend()
	started := backend.blockKey("frozen:ordered-a")
	writer, _, _ := newPersistenceWriterTest(t, backend)

	const rounds = 16
	wantA := make([]string, 0, rounds)
	wantB := make([]string, 0, rounds)
	for index := 0; index < rounds; index++ {
		for _, key := range []string{"frozen:ordered-a", "frozen:ordered-b"} {
			value := key + "#" + strconv.Itoa(index)
			probe := newPersistenceProbe(t, uint64(index+1))
			receipt, err := writer.TrySubmit(PersistenceOp{
				Target: PersistenceTargetState, Kind: PersistenceOpPut,
				OrderingKey: key, Key: key, Value: value, Completion: probe.completion,
			})
			if err != nil || !receipt.Accepted {
				t.Fatalf("op %s receipt=%+v err=%v", value, receipt, err)
			}
			if key == "frozen:ordered-a" {
				wantA = append(wantA, value)
				if index == 0 {
					// 确保首个 a 已进入 backend 并阻塞，后续 a 只能排队。
					<-started
				}
			} else {
				wantB = append(wantB, value)
			}
		}
	}
	// delete 与 put 共用同一条 key 序列，且必须路由到 DeleteState。
	deleteProbe := newPersistenceProbe(t, 9000)
	if receipt, err := writer.TrySubmit(PersistenceOp{
		Target: PersistenceTargetState, Kind: PersistenceOpDelete,
		OrderingKey: "frozen:ordered-b", Key: "frozen:ordered-b", Completion: deleteProbe.completion,
	}); err != nil || !receipt.Accepted {
		t.Fatalf("delete op receipt=%+v err=%v", receipt, err)
	}
	wantB = append(wantB, "")

	backend.releaseAll()
	if err := writer.CloseAndDrain(); err != nil {
		t.Fatalf("CloseAndDrain: %v", err)
	}

	if got := backend.valuesFor("frozen:ordered-a"); !reflect.DeepEqual(got, wantA) {
		t.Fatalf("same-key 执行顺序=%v\nwant %v", got, wantA)
	}
	if got := backend.valuesFor("frozen:ordered-b"); !reflect.DeepEqual(got, wantB) {
		t.Fatalf("same-key 执行顺序=%v\nwant %v", got, wantB)
	}
	if got := backend.deleteCount(); got != 1 {
		t.Fatalf("DeleteState 调用次数=%d, want 1", got)
	}
	if got := len(backend.snapshotOrder()); got != rounds*2+1 {
		t.Fatalf("执行次数=%d, want %d", got, rounds*2+1)
	}
	if peak := backend.peakInFlight(); peak > PersistenceWriterWorkers {
		t.Fatalf("in-flight 峰值=%d, want <= %d", peak, PersistenceWriterWorkers)
	}
	if deleteProbe.diskState() != persistenceStateSaved {
		t.Fatalf("delete closure=%s, want saved", deleteProbe.diskState())
	}
}

func TestPersistenceWriterCrossKeyIndependence(t *testing.T) {
	backend := newPersistenceFakeBackend()
	started := backend.blockKey("frozen:blocked")
	writer, _, _ := newPersistenceWriterTest(t, backend)

	blocked := newPersistenceProbe(t, 1)
	if receipt, err := writer.TrySubmit(PersistenceOp{
		Target: PersistenceTargetState, Kind: PersistenceOpPut,
		OrderingKey: "frozen:blocked", Key: "frozen:blocked", Value: "v", Completion: blocked.completion,
	}); err != nil || !receipt.Accepted {
		t.Fatalf("blocked op receipt=%+v err=%v", receipt, err)
	}
	<-started
	// 同 key 的后继项必须等待，不同 key 必须能立刻推进。
	queued := newPersistenceProbe(t, 2)
	if receipt, err := writer.TrySubmit(PersistenceOp{
		Target: PersistenceTargetState, Kind: PersistenceOpPut,
		OrderingKey: "frozen:blocked", Key: "frozen:blocked", Value: "v", Completion: queued.completion,
	}); err != nil || !receipt.Accepted {
		t.Fatalf("blocked 后继 op receipt=%+v err=%v", receipt, err)
	}

	free := make([]*persistenceProbe, 3)
	for index := range free {
		free[index] = newPersistenceProbe(t, uint64(index+10))
		key := "frozen:free-" + string(rune('a'+index))
		if receipt, err := writer.TrySubmit(PersistenceOp{
			Target: PersistenceTargetState, Kind: PersistenceOpPut,
			OrderingKey: key, Key: key, Value: "v", Completion: free[index].completion,
		}); err != nil || !receipt.Accepted {
			t.Fatalf("free op %d receipt=%+v err=%v", index, receipt, err)
		}
	}
	for index, probe := range free {
		waitForPersistence(t, func() bool { return probe.diskState() == persistenceStateSaved })
		if probe.diskState() != persistenceStateSaved {
			t.Fatalf("cross-key op %d 未在 blocked key 释放前完成", index)
		}
	}
	if blocked.diskState() != persistenceStateNotAttempted || queued.diskState() != persistenceStateNotAttempted {
		t.Fatalf("blocked key 提前完成: head=%s next=%s", blocked.diskState(), queued.diskState())
	}

	backend.releaseAll()
	if err := writer.CloseAndDrain(); err != nil {
		t.Fatalf("CloseAndDrain: %v", err)
	}
	if blocked.diskState() != persistenceStateSaved || queued.diskState() != persistenceStateSaved {
		t.Fatalf("释放后 blocked key 未完成: head=%s next=%s", blocked.diskState(), queued.diskState())
	}
}

func TestPersistenceReceiptOutcomeCompletionExactlyOnce(t *testing.T) {
	backend := newPersistenceFakeBackend()
	backend.failKey("frozen:bad", true)
	writer, _, _ := newPersistenceWriterTest(t, backend)

	good := newPersistenceProbe(t, 1)
	bad := newPersistenceProbe(t, 2)
	preCompleted := newPersistenceProbe(t, 3)
	if err := preCompleted.completion.CompleteResult(outcomeCompletionResult{
		State: persistenceStateFailed, FailureClass: persistenceFailureSQLite, DiskState: persistenceStateFailed,
	}); err != nil {
		t.Fatalf("预先完成 completion: %v", err)
	}

	for key, probe := range map[string]*persistenceProbe{
		"frozen:good": good,
		"frozen:bad":  bad,
		"frozen:pre":  preCompleted,
	} {
		if receipt, err := writer.TrySubmit(PersistenceOp{
			Target: PersistenceTargetState, Kind: PersistenceOpPut,
			OrderingKey: key, Key: key, Value: "v", Completion: probe.completion,
		}); err != nil || !receipt.Accepted {
			t.Fatalf("%s receipt=%+v err=%v", key, receipt, err)
		}
	}
	if err := writer.CloseAndDrain(); err != nil {
		t.Fatalf("CloseAndDrain: %v", err)
	}

	if good.diskState() != persistenceStateSaved {
		t.Fatalf("nil error 未标 saved: %s", good.diskState())
	}
	if bad.diskState() != persistenceStateFailed || bad.failureClass() != persistenceFailureSQLite {
		t.Fatalf("backend error closure=%s/%s, want failed/sqlite", bad.diskState(), bad.failureClass())
	}
	// 重复 Complete 幂等：已完成的 completion 保持首个结果，且 writer 不 panic。
	if preCompleted.diskState() != persistenceStateFailed {
		t.Fatalf("重复完成改写了首个结果: %s", preCompleted.diskState())
	}
	if err := good.completion.CompleteResult(outcomeCompletionResult{State: persistenceStateFailed}); err != nil {
		t.Fatalf("重复 Complete 应幂等成功: %v", err)
	}
	if good.diskState() != persistenceStateSaved {
		t.Fatalf("重复 Complete 改写了 writer 结果: %s", good.diskState())
	}
}

func TestPersistenceHealthTransitionsSQLiteStateAndArchive(t *testing.T) {
	dir := tempDirRetryCleanup(t)
	healthy, err := NewSQLiteStore(filepath.Join(dir, "healthy.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer healthy.Close()
	broken, err := NewSQLiteStore(filepath.Join(dir, "broken.db"))
	if err != nil {
		t.Fatal(err)
	}
	if err := broken.db.Close(); err != nil {
		t.Fatal(err)
	}

	backend := &persistenceSwitchBackend{}
	backend.set(broken)
	writer, tracker, reporter := newPersistenceWriterTest(t, backend)
	defer writer.CloseAndDrain()

	submit := func(id uint64, key string) *persistenceProbe {
		t.Helper()
		probe := newPersistenceProbe(t, id)
		receipt, err := writer.TrySubmit(PersistenceOp{
			Target: PersistenceTargetState, Kind: PersistenceOpPut,
			OrderingKey: key, Key: key, Value: "v", Completion: probe.completion,
		})
		if err != nil || !receipt.Accepted {
			t.Fatalf("提交 %s receipt=%+v err=%v", key, receipt, err)
		}
		waitForPersistence(t, func() bool { return probe.diskState() != persistenceStateNotAttempted })
		return probe
	}

	// 交错两个 scope：state 连续失败 3 次，archive 同期连续失败 3 次。
	for round := 0; round < 3; round++ {
		state := submit(uint64(round+1), "frozen:state")
		if state.diskState() != persistenceStateFailed || state.failureClass() != persistenceFailureSQLite {
			t.Fatalf("第 %d 轮 state closure=%s/%s, want failed/sqlite", round, state.diskState(), state.failureClass())
		}
		writer.ObserveArchiveCommit(ArchiveCommitResult{
			State: persistenceStateFailed, FailureClass: persistenceFailureArchive,
		})
	}
	if got := reporter.count(HealthScopeSQLiteState, HealthTransitionKindEntered); got != 1 {
		t.Fatalf("sqlite_state entered=%d, want 1", got)
	}
	if got := reporter.count(HealthScopeSQLiteArchive, HealthTransitionKindEntered); got != 1 {
		t.Fatalf("sqlite_archive entered=%d, want 1", got)
	}
	if got := reporter.count(HealthScopeSQLiteState, HealthTransitionKindOngoing); got != 0 {
		t.Fatalf("ongoing 被提交给 reporter: %d", got)
	}
	if !tracker.IsDegraded(HealthScopeSQLiteState) || !tracker.IsDegraded(HealthScopeSQLiteArchive) {
		t.Fatal("两个 SQLite scope 均应处于退化状态")
	}

	// state 真实恢复不得清除 archive；archive 恢复也不得清除 state。
	backend.set(healthy)
	for round := 0; round < 3; round++ {
		state := submit(uint64(round+100), "frozen:state")
		if state.diskState() != persistenceStateSaved {
			t.Fatalf("恢复后第 %d 轮 state closure=%s, want saved", round, state.diskState())
		}
	}
	if got := reporter.count(HealthScopeSQLiteState, HealthTransitionKindRecovered); got != 1 {
		t.Fatalf("sqlite_state recovered=%d, want 1", got)
	}
	if got := reporter.count(HealthScopeSQLiteArchive, HealthTransitionKindRecovered); got != 0 {
		t.Fatalf("state 恢复清除了 sqlite_archive: recovered=%d", got)
	}
	if !tracker.IsDegraded(HealthScopeSQLiteArchive) {
		t.Fatal("sqlite_archive 被 state 恢复误清除")
	}

	for round := 0; round < 3; round++ {
		writer.ObserveArchiveCommit(ArchiveCommitResult{
			ID: "canonical-id", State: persistenceStateSaved, FailureClass: persistenceFailureNone,
		})
	}
	if got := reporter.count(HealthScopeSQLiteArchive, HealthTransitionKindRecovered); got != 1 {
		t.Fatalf("sqlite_archive recovered=%d, want 1", got)
	}
	if tracker.IsDegraded(HealthScopeSQLiteState) || tracker.IsDegraded(HealthScopeSQLiteArchive) {
		t.Fatal("两个 scope 均应已恢复")
	}
}

func TestPersistenceHealthDoesNotReplaceRequestResult(t *testing.T) {
	dir := tempDirRetryCleanup(t)
	broken, err := NewSQLiteStore(filepath.Join(dir, "broken.db"))
	if err != nil {
		t.Fatal(err)
	}
	if err := broken.db.Close(); err != nil {
		t.Fatal(err)
	}
	backend := &persistenceSwitchBackend{}
	backend.set(broken)
	writer, tracker, reporter := newPersistenceWriterTest(t, backend)

	const requests = 5
	probes := make([]*persistenceProbe, requests)
	for index := 0; index < requests; index++ {
		probes[index] = newPersistenceProbe(t, uint64(index+1))
		receipt, err := writer.TrySubmit(PersistenceOp{
			Target: PersistenceTargetState, Kind: PersistenceOpPut,
			OrderingKey: "frozen:state", Key: "frozen:state", Value: "v", Completion: probes[index].completion,
		})
		if err != nil || !receipt.Accepted {
			t.Fatalf("提交 %d receipt=%+v err=%v", index, receipt, err)
		}
	}
	if err := writer.CloseAndDrain(); err != nil {
		t.Fatalf("CloseAndDrain: %v", err)
	}
	// health 去重只影响终端提示；每个请求仍然必须拿到自己的 failed 结果。
	for index, probe := range probes {
		if probe.diskState() != persistenceStateFailed || probe.failureClass() != persistenceFailureSQLite {
			t.Fatalf("请求 %d closure=%s/%s, want failed/sqlite", index, probe.diskState(), probe.failureClass())
		}
	}
	if got := reporter.count(HealthScopeSQLiteState, HealthTransitionKindEntered); got != 1 {
		t.Fatalf("连续失败 entered=%d, want 1", got)
	}
	if got := reporter.total(); got != 1 {
		t.Fatalf("reporter 收到 %d 条 transition, want 1", got)
	}
	if !tracker.IsDegraded(HealthScopeSQLiteState) {
		t.Fatal("连续失败后 sqlite_state 应退化")
	}

	// closing 之后的提交是 lifecycle 结果，不是磁盘失败，也不产生恢复。
	late := newPersistenceProbe(t, 999)
	receipt, err := writer.TrySubmit(PersistenceOp{
		Target: PersistenceTargetState, Kind: PersistenceOpPut,
		OrderingKey: "frozen:state", Key: "frozen:state", Value: "v", Completion: late.completion,
	})
	if !errors.Is(err, ErrPersistenceWriterClosing) {
		t.Fatalf("closing submit err=%v, want ErrPersistenceWriterClosing", err)
	}
	if receipt.Accepted {
		t.Fatal("closing 之后仍接受提交")
	}
	if late.diskState() != persistenceStateUnavailable || late.failureClass() != persistenceFailureLifecycle {
		t.Fatalf("closing closure=%s/%s, want unavailable/lifecycle", late.diskState(), late.failureClass())
	}
	if got := reporter.count(HealthScopeSQLiteState, HealthTransitionKindRecovered); got != 0 {
		t.Fatalf("closing 产生了 SQLite 恢复: %d", got)
	}
	if got := reporter.count(HealthScopeSQLiteArchive, HealthTransitionKindEntered); got != 0 {
		t.Fatalf("state 生命周期结果污染了 sqlite_archive: %d", got)
	}
}

func TestPersistenceWriterCloseAndDrainCompletesAcceptedOps(t *testing.T) {
	backend := newPersistenceFakeBackend()
	started := backend.blockKey("frozen:drain")
	writer, _, _ := newPersistenceWriterTest(t, backend)

	const ops = 40
	probes := make([]*persistenceProbe, ops)
	for index := 0; index < ops; index++ {
		probes[index] = newPersistenceProbe(t, uint64(index+1))
		key := "frozen:drain"
		if index%4 != 0 {
			key = "frozen:drain-" + string(rune('a'+index%4))
		}
		receipt, err := writer.TrySubmit(PersistenceOp{
			Target: PersistenceTargetState, Kind: PersistenceOpPut,
			OrderingKey: key, Key: key, Value: "v", Completion: probes[index].completion,
		})
		if err != nil || !receipt.Accepted {
			t.Fatalf("op %d receipt=%+v err=%v", index, receipt, err)
		}
		if index == 0 {
			<-started
		}
	}
	backend.releaseAll()
	if err := writer.CloseAndDrain(); err != nil {
		t.Fatalf("CloseAndDrain: %v", err)
	}
	for index, probe := range probes {
		if probe.diskState() != persistenceStateSaved {
			t.Fatalf("drain 返回前 op %d 未终结: %s", index, probe.diskState())
		}
	}
	if got := writer.QueueLength(); got != 0 {
		t.Fatalf("drain 后 queue 深度=%d, want 0", got)
	}
	// 重复 close 幂等。
	if err := writer.CloseAndDrain(); err != nil {
		t.Fatalf("重复 CloseAndDrain: %v", err)
	}
}

func TestPersistenceWriterCloseAndDrainConcurrentSubmit(t *testing.T) {
	backend := newPersistenceFakeBackend()
	writer, _, _ := newPersistenceWriterTest(t, backend)

	const submitters = 64
	probes := make([]*persistenceProbe, submitters)
	for index := range probes {
		probes[index] = newPersistenceProbe(t, uint64(index+1))
	}
	var wg sync.WaitGroup
	start := make(chan struct{})
	for index := 0; index < submitters; index++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			<-start
			key := "frozen:race-" + string(rune('a'+index%6))
			receipt, err := writer.TrySubmit(PersistenceOp{
				Target: PersistenceTargetState, Kind: PersistenceOpPut,
				OrderingKey: key, Key: key, Value: "v", Completion: probes[index].completion,
			})
			if err != nil && !errors.Is(err, ErrPersistenceWriterClosing) {
				t.Errorf("并发提交返回意外错误: %v", err)
			}
			if err != nil && receipt.Accepted {
				t.Error("lifecycle 错误同时报告了 accepted")
			}
		}(index)
	}
	closeDone := make(chan error, 1)
	go func() {
		<-start
		closeDone <- writer.CloseAndDrain()
	}()
	close(start)
	wg.Wait()
	if err := <-closeDone; err != nil {
		t.Fatalf("并发 CloseAndDrain: %v", err)
	}

	for index, probe := range probes {
		switch probe.diskState() {
		case persistenceStateSaved, persistenceStateUnavailable:
		default:
			t.Fatalf("并发 close 下 op %d 结果=%s, want saved 或 unavailable", index, probe.diskState())
		}
	}
}

// ── Task 2：统一 error-aware loader 的静态门禁 ──

// TestStateLoaderProductionCallersAreEnumerated 是 bool-only wrapper 的迁移门禁。
// 它不允许在已知的四个 main wiring 站点之外新增 LoadState 引用；
// 新代码必须使用 StateLoader/LoadStateResult。
func TestStateLoaderProductionCallersAreEnumerated(t *testing.T) {
	var _ StateLoader = (*SQLiteStore)(nil)

	// Plan 05/06 需要迁移的四个 production wiring 站点，逐个列举以便迁移后清零。
	wantMainCallers := 0
	mainSource, err := os.ReadFile(filepath.Join("..", "..", "cmd", "proxy", "main.go"))
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Count(string(mainSource), "store.LoadState)"); got != wantMainCallers {
		t.Fatalf("cmd/proxy/main.go 的 bool-only loader 引用=%d, want %d（新增引用必须改用 LoadStateResult）", got, wantMainCallers)
	}

	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		source, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		text := string(source)
		if name == "store.go" {
			// store.go 定义 wrapper 本身，只允许它内部调用新入口一次。
			if !strings.Contains(text, "func (s *SQLiteStore) LoadStateResult(") {
				t.Fatal("store.go 缺少 error-aware LoadStateResult")
			}
			continue
		}
		if strings.Contains(text, ".LoadState(") {
			t.Fatalf("%s 引用了 bool-only LoadState；production 必须使用 StateLoader", name)
		}
	}
}

// ── Plan 06 Task 3：production wiring、四 scope 健康与严格 shutdown ──

// countingTerminalReporter 在真实 terminal-only reporter 之外只加计数，
// 不引入任何 file-capable sink。
type countingTerminalReporter struct {
	*persistenceTestReporter
	inner *TerminalHealthReporter
}

func (r *countingTerminalReporter) ReportHealthTransition(transition HealthTransition) {
	r.persistenceTestReporter.ReportHealthTransition(transition)
	r.inner.ReportHealthTransition(transition)
}

// productionObservabilityFixture 复刻生产依赖图：一个 HealthTracker、
// 一个 terminal-only reporter、一个 state-only writer、一个 dispatcher，
// 以及共享同一份缺口证据的 session outcome writer。
type productionObservabilityFixture struct {
	tracker    *HealthTracker
	reporter   *countingTerminalReporter
	writer     *PersistenceWriter
	dispatcher *OutcomeDispatcher
	gaps       *OutcomeGapAccumulator
	sink       *fakeOutcomeSink
	session    *SessionOutcomeWriter
	terminal   *lockedBuffer
	backend    *persistenceSwitchBackend
	healthy    *SQLiteStore
	broken     *SQLiteStore
}

func newProductionObservabilityFixture(t *testing.T) *productionObservabilityFixture {
	t.Helper()
	dir := tempDirRetryCleanup(t)
	healthy, err := NewSQLiteStore(filepath.Join(dir, "healthy.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = healthy.Close() })
	broken, err := NewSQLiteStore(filepath.Join(dir, "broken.db"))
	if err != nil {
		t.Fatal(err)
	}
	if err := broken.db.Close(); err != nil {
		t.Fatal(err)
	}

	terminal := &lockedBuffer{}
	reporter := &countingTerminalReporter{
		persistenceTestReporter: &persistenceTestReporter{},
		inner:                   NewTerminalHealthReporter(NewLogHandler(terminal, slog.LevelInfo)),
	}
	tracker := NewHealthTracker(reporter)
	backend := &persistenceSwitchBackend{}
	backend.set(broken)
	writer, err := NewPersistenceWriterChecked(backend, tracker, reporter)
	if err != nil {
		t.Fatalf("NewPersistenceWriterChecked: %v", err)
	}
	gaps := NewOutcomeGapAccumulator()
	sink := &fakeOutcomeSink{}
	session := NewSessionOutcomeWriter(sink, gaps, tracker)
	dispatcher, err := NewOutcomeDispatcherChecked(OutcomeDispatcherOptions{
		SessionProjector: session,
		HealthTracker:    tracker,
		Reporter:         reporter,
		GapAccumulator:   gaps,
	})
	if err != nil {
		t.Fatalf("NewOutcomeDispatcherChecked: %v", err)
	}
	t.Cleanup(func() {
		_ = writer.CloseAndDrain()
		_ = dispatcher.CloseAndDrain()
	})
	return &productionObservabilityFixture{
		tracker: tracker, reporter: reporter, writer: writer, dispatcher: dispatcher,
		gaps: gaps, sink: sink, session: session, terminal: terminal,
		backend: backend, healthy: healthy, broken: broken,
	}
}

func (f *productionObservabilityFixture) submitState(t *testing.T, id uint64) *persistenceProbe {
	t.Helper()
	probe := newPersistenceProbe(t, id)
	receipt, err := f.writer.TrySubmit(PersistenceOp{
		Target: PersistenceTargetState, Kind: PersistenceOpPut,
		OrderingKey: "frozen:prod", Key: "frozen:prod", Value: "v", Completion: probe.completion,
	})
	if err != nil || !receipt.Accepted {
		t.Fatalf("提交 state receipt=%+v err=%v", receipt, err)
	}
	waitForPersistence(t, func() bool { return probe.diskState() != persistenceStateNotAttempted })
	return probe
}

func productionSnapshot(id uint64) requestOutcomeSnapshot {
	return requestOutcomeSnapshot{
		RequestID:           id,
		SessionHash:         "0123456789abcdef",
		StartedAt:           time.Now().UTC(),
		Eligibility:         outcomeEligibilityEvaluable,
		TerminalEligibility: outcomeEligibilityTerminalIneligible,
		TriggerReason:       TriggerNone,
		Action:              outcomeActionDirect,
		UpstreamState:       upstreamStateSuccess,
	}.normalized()
}

func TestProductionHealthTransitionWiringAllScopes(t *testing.T) {
	fixture := newProductionObservabilityFixture(t)

	// 1) sqlite_state：真实 SQLite 连续失败只 entered 一次，真实成功恢复一次。
	for round := 0; round < 3; round++ {
		probe := fixture.submitState(t, uint64(round+1))
		if probe.diskState() != persistenceStateFailed {
			t.Fatalf("第 %d 轮 state=%s, want failed", round, probe.diskState())
		}
	}
	fixture.backend.set(fixture.healthy)
	for round := 0; round < 3; round++ {
		probe := fixture.submitState(t, uint64(round+100))
		if probe.diskState() != persistenceStateSaved {
			t.Fatalf("恢复后第 %d 轮 state=%s, want saved", round, probe.diskState())
		}
	}

	// 2) sqlite_archive：同步 commit 结果由同一 tracker/reporter 驱动。
	for round := 0; round < 3; round++ {
		fixture.writer.ObserveArchiveCommit(ArchiveCommitResult{
			State: persistenceStateFailed, FailureClass: persistenceFailureArchive,
		})
	}
	for round := 0; round < 3; round++ {
		fixture.writer.ObserveArchiveCommit(ArchiveCommitResult{
			ID: "canonical", State: persistenceStateSaved, FailureClass: persistenceFailureNone,
		})
	}

	// 3) session_queue_full：真实 admission 拒绝驱动，accepted 不产生恢复。
	release := make(chan struct{})
	var once sync.Once
	fixture.sink.mu.Lock()
	fixture.sink.sessionHook = func() { <-release }
	fixture.sink.mu.Unlock()
	for index := 0; index < OutcomeDispatcherSessionQueueCapacity+8; index++ {
		fixture.dispatcher.TryDispatch(productionSnapshot(uint64(index + 1000)))
	}
	if fixture.dispatcher.SessionQueueFullCount() == 0 {
		t.Fatal("session queue 未被真实拒绝，无法证明 producer 可达")
	}
	once.Do(func() { close(release) })
	fixture.sink.mu.Lock()
	fixture.sink.sessionHook = nil
	fixture.sink.mu.Unlock()

	// 4) session_log_sink：accepted 之后的整行写失败驱动独立 scope。
	fixture.sink.mu.Lock()
	fixture.sink.sessionErr = errors.New("forced session sink failure")
	fixture.sink.mu.Unlock()
	for index := 0; index < 3; index++ {
		if err := fixture.session.WriteRequestOutcome(productionSnapshot(uint64(index + 2000))); err == nil {
			t.Fatal("注入的 sink 故障未生效")
		}
		generation := fixture.gaps.Accumulate(OutcomeGapEvent{
			Source:      OutcomeGapSourceSessionLogSink,
			SessionHash: "0123456789abcdef",
			RequestID:   uint64(index + 2000),
			Reason:      string(OutcomeGapReasonSessionLogSink),
		})
		fixture.tracker.ObserveFailure(HealthScopeSessionLogSink, HealthFailureClassSessionLogSink, generation)
	}
	fixture.sink.mu.Lock()
	fixture.sink.sessionErr = nil
	fixture.sink.mu.Unlock()

	// 5) 一次成功 closure 的 Claim → 写摘要 → generation-aware Commit 同时恢复两条 session scope。
	if err := fixture.session.WriteRequestOutcome(productionSnapshot(3000)); err != nil {
		t.Fatalf("恢复后的 closure 应成功: %v", err)
	}

	for _, scope := range HealthScopes() {
		if got := fixture.reporter.count(scope, HealthTransitionKindEntered); got != 1 {
			t.Fatalf("%s entered=%d, want 1", scope, got)
		}
		if got := fixture.reporter.count(scope, HealthTransitionKindRecovered); got != 1 {
			t.Fatalf("%s recovered=%d, want 1", scope, got)
		}
		if got := fixture.reporter.count(scope, HealthTransitionKindOngoing); got != 0 {
			t.Fatalf("%s ongoing 进入了 reporter: %d", scope, got)
		}
		if fixture.tracker.IsDegraded(scope) {
			t.Fatalf("%s 仍处于退化状态", scope)
		}
	}
	if strings.Contains(fixture.terminal.String(), "sqlite_state") {
		t.Fatalf("终端健康提示泄漏 scope 名: %s", fixture.terminal.String())
	}
}

func TestProductionHealthScopesIndependent(t *testing.T) {
	fixture := newProductionObservabilityFixture(t)

	// SQLite 与 session 两侧交错退化。
	fixture.submitState(t, 1)
	fixture.writer.ObserveArchiveCommit(ArchiveCommitResult{
		State: persistenceStateFailed, FailureClass: persistenceFailureArchive,
	})
	sessionGeneration := fixture.gaps.Accumulate(OutcomeGapEvent{
		Source: OutcomeGapSourceSessionQueueFull, SessionHash: "0123456789abcdef", RequestID: 7,
		Reason: string(OutcomeGapReasonSessionQueueFull),
	})
	fixture.tracker.ObserveFailure(HealthScopeSessionQueueFull, HealthFailureClassSessionQueueFull, sessionGeneration)

	// stale generation 不得产生恢复。
	if got := fixture.tracker.ObserveGapCommit(HealthScopeSessionQueueFull, sessionGeneration-1); got.Kind == HealthTransitionKindRecovered {
		t.Fatal("stale generation 被当成恢复证明")
	}
	if got := fixture.reporter.count(HealthScopeSessionQueueFull, HealthTransitionKindRecovered); got != 0 {
		t.Fatalf("stale generation recovered=%d, want 0", got)
	}

	// state 恢复不得清除 archive 或 session。
	fixture.backend.set(fixture.healthy)
	fixture.submitState(t, 2)
	if !fixture.tracker.IsDegraded(HealthScopeSQLiteArchive) || !fixture.tracker.IsDegraded(HealthScopeSessionQueueFull) {
		t.Fatal("sqlite_state 恢复清除了其他 scope")
	}

	// session 恢复不得清除 SQLite scope。
	fixture.tracker.ObserveGapCommit(HealthScopeSessionQueueFull, sessionGeneration)
	if !fixture.tracker.IsDegraded(HealthScopeSQLiteArchive) {
		t.Fatal("session 恢复清除了 sqlite_archive")
	}
	if fixture.tracker.IsDegraded(HealthScopeSQLiteState) {
		t.Fatal("sqlite_state 未按真实 saved 恢复")
	}
}

func TestShutdownHasZeroClosingRejections(t *testing.T) {
	fixture := newProductionObservabilityFixture(t)
	fixture.backend.set(fixture.healthy)

	const requests = 64
	probes := make([]*persistenceProbe, requests)
	for index := 0; index < requests; index++ {
		probes[index] = newPersistenceProbe(t, uint64(index+1))
		key := "frozen:shutdown-" + strconv.Itoa(index%4)
		receipt, err := fixture.writer.TrySubmit(PersistenceOp{
			Target: PersistenceTargetState, Kind: PersistenceOpPut,
			OrderingKey: key, Key: key, Value: "v", Completion: probes[index].completion,
		})
		if err != nil || !receipt.Accepted {
			t.Fatalf("提交 %d receipt=%+v err=%v", index, receipt, err)
		}
		fixture.dispatcher.TryDispatch(productionSnapshot(uint64(index + 1)))
	}

	// 正常顺序：HTTP 已停止（本用例不再提交）→ persistence drain → outcome drain。
	if err := fixture.writer.CloseAndDrain(); err != nil {
		t.Fatalf("persistence drain: %v", err)
	}
	if err := fixture.dispatcher.CloseAndDrain(); err != nil {
		t.Fatalf("outcome drain: %v", err)
	}
	if got := fixture.writer.LifecycleRejectionCount(); got != 0 {
		t.Fatalf("persistence closing rejection=%d, want 0", got)
	}
	if got := fixture.dispatcher.LifecycleRejectionCount(); got != 0 {
		t.Fatalf("outcome closing rejection=%d, want 0", got)
	}
	for index, probe := range probes {
		if probe.diskState() != persistenceStateSaved {
			t.Fatalf("drain 返回前 op %d 未终结: %s", index, probe.diskState())
		}
	}
	if got := len(fixture.sink.session("0123456789abcdef")); got != requests {
		t.Fatalf("drain 后 closure 数=%d, want %d", got, requests)
	}
}

func TestHTTPDoesNotWaitForPersistenceOrOutcomeSink(t *testing.T) {
	upstream := jsonOutcomeUpstream(t)
	server, sink := newOutcomePipelineServer(t, upstream.URL)

	backend := newBlockingStateBackend()
	backend.block()
	writer, tracker, reporter := newPersistenceWriterTest(t, backend)
	defer func() {
		backend.release()
		_ = writer.CloseAndDrain()
	}()
	server.Frozen.SetStateSubmitter(writer)
	server.DecayTracker.SetStateSubmitter(writer)
	server.archiveHealth = writer

	// terminal 与 session 两条 lane 同时被阻塞。
	blockedProjector := make(chan struct{})
	dispatcher, err := NewOutcomeDispatcherChecked(OutcomeDispatcherOptions{
		TerminalProjector: func(requestOutcomeSnapshot, outcomeDispatchResult) error {
			<-blockedProjector
			return nil
		},
		SessionProjector: func(requestOutcomeSnapshot) error {
			<-blockedProjector
			return nil
		},
		HealthTracker: tracker,
		Reporter:      reporter,
	})
	if err != nil {
		t.Fatalf("NewOutcomeDispatcherChecked: %v", err)
	}
	defer func() {
		close(blockedProjector)
		_ = dispatcher.CloseAndDrain()
	}()
	_ = sink
	server.outcomeSink = dispatcher

	baselineGoroutines := runtime.NumGoroutine()
	done := make(chan struct{})
	go func() {
		defer close(done)
		serveOutcomeMessages(t, server, "http-not-waiting", pipelineMessages(80, 260))
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("HTTP handler 被阻塞的 persistence/outcome sink 拖住")
	}
	if got := runtime.NumGoroutine(); got > baselineGoroutines+PersistenceWriterWorkers+OutcomeDispatcherWorkerCount+8 {
		t.Fatalf("goroutine 数=%d，基线=%d，出现按请求增长", got, baselineGoroutines)
	}
}

func TestThousandConcurrentRequestOutcomes(t *testing.T) {
	upstream := jsonOutcomeUpstream(t)
	server, sink := newOutcomePipelineServer(t, upstream.URL)

	const requests = 1000
	baselineGoroutines := runtime.NumGoroutine()
	var wg sync.WaitGroup
	for index := 0; index < requests; index++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			serveOutcomeMessages(t, server, "concurrent-"+strconv.Itoa(index%16), pipelineMessages(4, 3))
		}(index)
	}
	wg.Wait()

	snapshots := sink.waitFor(t, requests)
	if len(snapshots) != requests {
		t.Fatalf("dispatch 次数=%d, want %d", len(snapshots), requests)
	}
	seen := make(map[uint64]bool, requests)
	for _, snapshot := range snapshots {
		if seen[snapshot.RequestID] {
			t.Fatalf("request %d 被 dispatch 多次", snapshot.RequestID)
		}
		seen[snapshot.RequestID] = true
		if snapshot.FinishedAt.IsZero() {
			t.Fatalf("request %d 未终结", snapshot.RequestID)
		}
	}
	waitForPersistence(t, func() bool { return runtime.NumGoroutine() <= baselineGoroutines+16 })
}

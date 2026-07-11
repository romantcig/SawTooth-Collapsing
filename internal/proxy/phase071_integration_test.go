package proxy

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"
)

// TestPhase071ArchiveIdentityAndDominance 把新写入幂等边界与召回 dominance
// 放在同一个真实 SQLite 生命周期内验收，避免只验证各自的纯函数。
func TestPhase071ArchiveIdentityAndDominance(t *testing.T) {
	store, err := NewSQLiteStore(filepath.Join(t.TempDir(), "phase071.db"))
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	defer store.Close()

	messages := []Message{{Role: "user", Content: mustMarshal("restore archive about alpha beta gamma")}}
	makeBlock := func(id string, end int) ArchiveBlock {
		return ArchiveBlock{
			ID: id, SessionID: "phase071-session", BlockRangeStart: 1, BlockRangeEnd: end,
			MessageCount: end, EstimatedTokens: 100, Messages: messages,
			SummaryText: "alpha beta gamma summary",
			Keywords:    []KeywordEntry{{Word: "alpha", Source: "user_message"}},
		}
	}

	const writers = 8
	var wg sync.WaitGroup
	errs := make(chan error, writers)
	for i := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- store.SaveArchive(makeBlock(fmt.Sprintf("duplicate-%d", i), 299))
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("并发 SaveArchive: %v", err)
		}
	}
	if got := archiveCount(t, store); got != 1 {
		t.Fatalf("相同逻辑 Archive 并发保存后 count=%d, want 1", got)
	}

	long := makeBlock("long-range", 347)
	long.Messages = append(long.Messages, Message{Role: "assistant", Content: mustMarshal("longer range")})
	if err := store.SaveArchive(long); err != nil {
		t.Fatalf("SaveArchive(long): %v", err)
	}
	results, err := store.SearchArchives(`"alpha"`, 10)
	if err != nil {
		t.Fatalf("SearchArchives: %v", err)
	}
	candidates := make([]recallCandidate, 0, len(results))
	for _, result := range results {
		candidates = append(candidates, recallCandidate{Summary: result, SameSession: true})
	}
	if got := len(dedupeDominatedCandidates(candidates)); got != 1 {
		t.Fatalf("1-347 与 1-299 dominance 后 selected=%d, want 1", got)
	}
}

// TestInvariantIN01ReliableAgentBypass 保留阶段级不变式名称；真实副作用断言由
// 同包集成 helper 执行，覆盖搜索、存档与上游 body。
func TestInvariantIN01ReliableAgentBypass(t *testing.T) {
	testHandleMessagesDirectAgentBypass(t, "phase071-in01", `"deepseek-v4-pro"`, `{"type":"enabled"}`, `[{"type":"text","text":"cc_entrypoint=sdk-ts"}]`)
}

// TestInvariantIN02CorruptArchiveSingleExpansion 保留损坏 JSON 回退与单次完整展开的
// 阶段级入口；两项契约继续由生产 SearchAndExpand 的公开结果验证。
func TestInvariantIN02CorruptArchiveSingleExpansion(t *testing.T) {
	t.Run("corrupt-json-fallback", TestSearchAndExpandFullExpansionCorruptJSONFallsBack)
	t.Run("single-full-expansion", TestSearchAndExpandFullExpansionSameSessionOnce)
}

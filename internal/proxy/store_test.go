package proxy

import (
	"os"
	"path/filepath"
	"testing"
)

// ---- SearchArchives 多词排序测试 ----

func TestSearchArchivesMultiKeywordRanking(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")
	store, err := NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteStore failed: %v", err)
	}
	defer store.Close()

	// 测试数据：block-b 匹配两词（alpha+beta），block-a 只匹配 alpha，
	// block-c 为噪声块，保证目标词 IDF 为正（不被 FTS5 的 1e-6 钳制）。
	// Messages 留 nil（json.Marshal(nil)="null" 可正常入库）；
	// CreatedAt 由 SQL 端 datetime('now') 生成，无需赋值。
	blocks := []ArchiveBlock{
		{
			ID: "block-a", SessionID: "s1",
			BlockRangeStart: 0, BlockRangeEnd: 1,
			MessageCount: 2, EstimatedTokens: 100,
			SummaryText: "只含 alpha",
			Keywords:    []KeywordEntry{{Word: "alpha", Source: "user_message"}},
		},
		{
			ID: "block-b", SessionID: "s1",
			BlockRangeStart: 2, BlockRangeEnd: 3,
			MessageCount: 2, EstimatedTokens: 100,
			SummaryText: "含 alpha 与 beta",
			Keywords: []KeywordEntry{
				{Word: "alpha", Source: "user_message"},
				{Word: "beta", Source: "user_message"},
			},
		},
		{
			ID: "block-c", SessionID: "s1",
			BlockRangeStart: 4, BlockRangeEnd: 5,
			MessageCount: 2, EstimatedTokens: 100,
			SummaryText: "噪声块",
			Keywords: []KeywordEntry{
				{Word: "gamma", Source: "user_message"},
				{Word: "delta", Source: "user_message"},
				{Word: "epsilon", Source: "user_message"},
			},
		},
	}
	for _, b := range blocks {
		if err := store.SaveArchive(b); err != nil {
			t.Fatalf("SaveArchive(%s) failed: %v", b.ID, err)
		}
	}

	// 查询格式与 buildFTS5Query 输出同构（双引号包裹 + OR 连接）
	results, err := store.SearchArchives(`"alpha" OR "beta"`, 10)
	if err != nil {
		t.Fatalf("SearchArchives failed: %v", err)
	}

	// 无重复行且噪声块不出现 —— 恰好两条结果
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d: %+v", len(results), results)
	}
	seen := make(map[string]bool)
	for _, r := range results {
		if seen[r.ID] {
			t.Errorf("duplicate result ID: %s", r.ID)
		}
		seen[r.ID] = true
	}

	// 多词匹配优先：N=6 FTS 行，idf(alpha)≈0.588、idf(beta)≈1.299，
	// SUM(block-b)=-1.887 < SUM(block-a)=-0.588，升序 block-b 在前，差距悬殊排序稳健。
	if results[0].ID != "block-b" {
		t.Errorf("expected block-b (matches alpha+beta) first, got %s", results[0].ID)
	}
	if results[1].ID != "block-a" {
		t.Errorf("expected block-a (matches alpha only) second, got %s", results[1].ID)
	}
}

// ---- NewSQLiteStore 损坏自动恢复测试 ----

// 场景：主 DB 文件是纯文本假数据库（模拟磁盘损坏/外部篡改），
// NewSQLiteStore 应命中 isCorruptionError → removeDBFiles → 重试路径，
// 自动删除损坏文件并重建可用库。
// 只断言恢复结果，不断言具体错误消息字符串（驱动版本升级时措辞可能变化）。
func TestNewSQLiteStoreRecoversFromCorruptDB(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "corrupt.db")
	if err := os.WriteFile(dbPath, []byte("this is not a database"), 0644); err != nil {
		t.Fatalf("预置损坏 DB 文件失败: %v", err)
	}

	store, err := NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteStore 应自动恢复损坏 DB，却返回错误: %v", err)
	}
	defer store.Close()

	// 重建后的库可用性验证：SaveArchive + SearchArchives 一轮
	block := ArchiveBlock{
		ID: "block-r", SessionID: "s1",
		BlockRangeStart: 0, BlockRangeEnd: 1,
		MessageCount: 2, EstimatedTokens: 100,
		SummaryText: "恢复验证块",
		Keywords:    []KeywordEntry{{Word: "recovery", Source: "user_message"}},
	}
	if err := store.SaveArchive(block); err != nil {
		t.Fatalf("SaveArchive failed: %v", err)
	}

	results, err := store.SearchArchives(`"recovery"`, 5)
	if err != nil {
		t.Fatalf("SearchArchives failed: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d: %+v", len(results), results)
	}
	if results[0].ID != "block-r" {
		t.Errorf("expected block-r, got %s", results[0].ID)
	}
}

// 场景：主 DB 缺失但存在进程闪退残留的 -wal/-shm 伴生文件，
// NewSQLiteStore 的 removeStaleWALFiles 前置清理应使建库正常成功。
// 不断言 -wal/-shm 文件存在性——建库后 WAL 模式会生成新的伴生文件。
func TestNewSQLiteStoreCleansOrphanWALFiles(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "orphan.db")
	if err := os.WriteFile(dbPath+"-wal", []byte("stale wal"), 0644); err != nil {
		t.Fatalf("预置残留 -wal 文件失败: %v", err)
	}
	if err := os.WriteFile(dbPath+"-shm", []byte("stale shm"), 0644); err != nil {
		t.Fatalf("预置残留 -shm 文件失败: %v", err)
	}

	store, err := NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteStore 应清理孤儿 WAL 后正常建库，却返回错误: %v", err)
	}
	defer store.Close()

	// 建库可用性轻量验证：写读一轮状态
	if err := store.PersistState("k", "v"); err != nil {
		t.Fatalf("PersistState failed: %v", err)
	}
	got, ok := store.LoadState("k")
	if !ok {
		t.Fatalf("LoadState(%q) 未找到刚写入的键", "k")
	}
	if got != "v" {
		t.Errorf("LoadState(%q) = %q, want %q", "k", got, "v")
	}
}

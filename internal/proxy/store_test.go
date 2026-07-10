package proxy

import (
	"database/sql"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestSQLiteStoreMigrationPreservesLegacyDuplicateRows(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "legacy.db")
	legacy, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("打开旧库失败: %v", err)
	}
	_, err = legacy.Exec(`CREATE TABLE archive_blocks (
		id TEXT PRIMARY KEY,
		session_id TEXT NOT NULL,
		block_range_start INTEGER NOT NULL,
		block_range_end INTEGER NOT NULL,
		message_count INTEGER NOT NULL,
		estimated_tokens INTEGER NOT NULL,
		messages_json TEXT NOT NULL,
		summary_text TEXT NOT NULL,
		created_at TEXT NOT NULL
	);
	INSERT INTO archive_blocks VALUES
		('legacy-a', 'session-1', 1, 2, 2, 10, '[{"role":"user","content":"a"}]', 'summary', '2026-01-01'),
		('legacy-b', 'session-1', 1, 2, 2, 10, '[{"role":"user","content":"a"}]', 'summary', '2026-01-02');`)
	if err != nil {
		legacy.Close()
		t.Fatalf("构造旧 schema 失败: %v", err)
	}
	if err := legacy.Close(); err != nil {
		t.Fatalf("关闭旧库失败: %v", err)
	}

	for open := 1; open <= 2; open++ {
		store, err := NewSQLiteStore(dbPath)
		if err != nil {
			t.Fatalf("第 %d 次打开旧库失败: %v", open, err)
		}

		var count, nullHashes int
		if err := store.db.QueryRow(`SELECT COUNT(*), COUNT(*) FILTER (WHERE content_hash IS NULL) FROM archive_blocks`).Scan(&count, &nullHashes); err != nil {
			store.Close()
			t.Fatalf("读取迁移后旧行失败: %v", err)
		}
		if count != 2 || nullHashes != 2 {
			store.Close()
			t.Fatalf("迁移改变旧行: count=%d null_hashes=%d, want 2/2", count, nullHashes)
		}
		if err := store.Close(); err != nil {
			t.Fatalf("第 %d 次关闭迁移库失败: %v", open, err)
		}
	}
}

func TestNewSQLiteStoreMigrationErrorDoesNotDeleteDatabaseFiles(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "migration-error.db")
	legacy, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("打开迁移错误 fixture 失败: %v", err)
	}
	legacy.SetMaxOpenConns(1)
	if _, err := legacy.Exec(`PRAGMA journal_mode=WAL`); err != nil {
		legacy.Close()
		t.Fatalf("启用 WAL 失败: %v", err)
	}
	_, err = legacy.Exec(`CREATE TABLE archive_blocks (
		id TEXT PRIMARY KEY,
		session_id TEXT NOT NULL,
		block_range_start INTEGER NOT NULL,
		block_range_end INTEGER NOT NULL,
		message_count INTEGER NOT NULL,
		estimated_tokens INTEGER NOT NULL,
		messages_json TEXT NOT NULL,
		summary_text TEXT NOT NULL,
		created_at TEXT NOT NULL,
		content_hash TEXT
	);
	INSERT INTO archive_blocks VALUES
		('duplicate-a', 'session-1', 1, 2, 2, 10, '[]', 'summary', '2026-01-01', 'same-hash'),
		('duplicate-b', 'session-1', 1, 2, 2, 10, '[]', 'summary', '2026-01-02', 'same-hash');`)
	if err != nil {
		legacy.Close()
		t.Fatalf("构造迁移错误 fixture 失败: %v", err)
	}
	defer legacy.Close()

	for _, path := range []string{dbPath, dbPath + "-wal", dbPath + "-shm"} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("迁移前文件不存在 %s: %v", path, err)
		}
	}

	store, err := NewSQLiteStore(dbPath)
	if store != nil {
		store.Close()
	}
	if err == nil {
		t.Fatal("唯一索引迁移应因重复非空 hash 失败")
	}

	for _, path := range []string{dbPath, dbPath + "-wal", dbPath + "-shm"} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("普通迁移错误删除了 %s: %v", path, err)
		}
	}
	var count int
	if err := legacy.QueryRow(`SELECT COUNT(*) FROM archive_blocks`).Scan(&count); err != nil {
		t.Fatalf("迁移失败后读取原库失败: %v", err)
	}
	if count != 2 {
		t.Fatalf("迁移失败后原库行数=%d, want 2", count)
	}
}

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

func TestSaveArchiveIdempotent(t *testing.T) {
	store, err := NewSQLiteStore(filepath.Join(t.TempDir(), "idempotent.db"))
	if err != nil {
		t.Fatalf("NewSQLiteStore failed: %v", err)
	}
	defer store.Close()

	first := archiveTestBlock("retry-a", "same content")
	second := archiveTestBlock("retry-b", "same content")
	if err := store.SaveArchive(first); err != nil {
		t.Fatalf("第一次 SaveArchive failed: %v", err)
	}
	if err := store.SaveArchive(second); err != nil {
		t.Fatalf("第二次 SaveArchive failed: %v", err)
	}

	assertArchiveCounts(t, store, 1, len(first.Keywords))
	var storedID, contentHash string
	if err := store.db.QueryRow(`SELECT id, content_hash FROM archive_blocks`).Scan(&storedID, &contentHash); err != nil {
		t.Fatalf("读取幂等结果失败: %v", err)
	}
	if storedID != first.ID {
		t.Fatalf("重复保存覆盖了首个引用 ID: got %q want %q", storedID, first.ID)
	}
	if contentHash == "" {
		t.Fatal("SaveArchive 未防御性补算 content_hash")
	}

	different := archiveTestBlock("different", "different content")
	if err := store.SaveArchive(different); err != nil {
		t.Fatalf("保存不同正文失败: %v", err)
	}
	assertArchiveCounts(t, store, 2, len(first.Keywords)+len(different.Keywords))
}

func TestSaveArchiveConcurrent(t *testing.T) {
	store, err := NewSQLiteStore(filepath.Join(t.TempDir(), "concurrent.db"))
	if err != nil {
		t.Fatalf("NewSQLiteStore failed: %v", err)
	}
	defer store.Close()

	const writers = 12
	var wg sync.WaitGroup
	errs := make(chan error, writers)
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			block := archiveTestBlock(string(rune('a'+i)), "concurrent content")
			errs <- store.SaveArchive(block)
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("并发 SaveArchive failed: %v", err)
		}
	}

	example := archiveTestBlock("example", "concurrent content")
	assertArchiveCounts(t, store, 1, len(example.Keywords))
}

func archiveTestBlock(id, text string) ArchiveBlock {
	return ArchiveBlock{
		ID:              id,
		SessionID:       "session-idempotent",
		BlockRangeStart: 1,
		BlockRangeEnd:   4,
		MessageCount:    4,
		EstimatedTokens: 100,
		Messages:        []Message{{Role: "user", Content: mustMarshal(text)}},
		SummaryText:     text,
		Keywords:        []KeywordEntry{{Word: "archive", Source: "user_message"}, {Word: "content", Source: "user_message"}},
	}
}

func assertArchiveCounts(t *testing.T, store *SQLiteStore, blocks, keywords int) {
	t.Helper()
	var gotBlocks, gotKeywords int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM archive_blocks`).Scan(&gotBlocks); err != nil {
		t.Fatalf("统计 archive_blocks 失败: %v", err)
	}
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM archive_keywords`).Scan(&gotKeywords); err != nil {
		t.Fatalf("统计 archive_keywords 失败: %v", err)
	}
	if gotBlocks != blocks || gotKeywords != keywords {
		t.Fatalf("archive 计数=%d/%d, want %d/%d", gotBlocks, gotKeywords, blocks, keywords)
	}
}

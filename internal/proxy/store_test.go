package proxy

import (
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
)

func TestSQLiteStoreMigrationPreservesArchiveVisibilityData(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "legacy-visibility.db")
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
		created_at TEXT NOT NULL,
		content_hash TEXT
	);
	CREATE TABLE archive_keywords (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		block_id TEXT NOT NULL REFERENCES archive_blocks(id),
		keyword TEXT NOT NULL,
		source TEXT NOT NULL
	);
	INSERT INTO archive_blocks VALUES
		('legacy-visible', 'legacy-session', 1, 4, 4, 17,
		 '[{"role":"user","content":{"keep":"exact bytes"}}]',
		 'legacy summary', '2026-01-01', 'legacy-content-hash');
	INSERT INTO archive_keywords(block_id, keyword, source)
	VALUES ('legacy-visible', 'needle', 'user_message');`)
	if err != nil {
		legacy.Close()
		t.Fatalf("构造旧 Archive schema 失败: %v", err)
	}
	if err := legacy.Close(); err != nil {
		t.Fatalf("关闭旧库失败: %v", err)
	}

	for open := 1; open <= 2; open++ {
		store, err := NewSQLiteStore(dbPath)
		if err != nil {
			t.Fatalf("第 %d 次迁移旧库失败: %v", open, err)
		}

		var messagesJSON, contentHash string
		var historyEpoch uint64
		var isolated int
		if err := store.db.QueryRow(`SELECT messages_json, content_hash, history_epoch, isolated
			FROM archive_blocks WHERE id = 'legacy-visible'`).Scan(&messagesJSON, &contentHash, &historyEpoch, &isolated); err != nil {
			store.Close()
			t.Fatalf("读取迁移后 Archive 失败: %v", err)
		}
		if messagesJSON != `[{"role":"user","content":{"keep":"exact bytes"}}]` || contentHash != "legacy-content-hash" {
			store.Close()
			t.Fatalf("迁移重写旧 Archive bytes/hash: messages=%q hash=%q", messagesJSON, contentHash)
		}
		if historyEpoch != 1 || isolated != 0 {
			store.Close()
			t.Fatalf("旧 Archive 默认 visibility=%d/%d, want epoch=1 isolated=0", historyEpoch, isolated)
		}
		var keywordCount int
		if err := store.db.QueryRow(`SELECT COUNT(*) FROM archive_keywords
			WHERE block_id = 'legacy-visible' AND keyword = 'needle' AND source = 'user_message'`).Scan(&keywordCount); err != nil {
			store.Close()
			t.Fatalf("读取迁移后关键词失败: %v", err)
		}
		if keywordCount != 1 {
			store.Close()
			t.Fatalf("迁移改变关键词行数: %d", keywordCount)
		}
		if err := store.Close(); err != nil {
			t.Fatalf("第 %d 次关闭迁移库失败: %v", open, err)
		}
	}
}

func TestSQLiteStoreCommitHistoryTransitionIsolatesArchiveSuffix(t *testing.T) {
	store, err := NewSQLiteStore(filepath.Join(t.TempDir(), "history-transition.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	blocks := []ArchiveBlock{
		archiveRangeTestBlock("before", "branch-session", 0, 2, 1, "before-only"),
		archiveRangeTestBlock("cross", "branch-session", 1, 3, 1, "cross-boundary"),
		archiveRangeTestBlock("after", "branch-session", 3, 5, 1, "after-only"),
		archiveRangeTestBlock("other-session", "other-session", 3, 5, 1, "other-session"),
	}
	for _, block := range blocks {
		if err := store.SaveArchive(block); err != nil {
			t.Fatalf("SaveArchive(%s): %v", block.ID, err)
		}
	}

	const stateKey = "history_epoch:stable-session-hash"
	const stateValue = `{"version":1,"epoch":2}`
	if err := store.CommitHistoryTransition(HistoryTransition{
		SessionID:    "branch-session",
		StateKey:     stateKey,
		StateValue:   stateValue,
		CommonPrefix: 3,
	}); err != nil {
		t.Fatalf("CommitHistoryTransition: %v", err)
	}
	if got, ok := store.LoadState(stateKey); !ok || got != stateValue {
		t.Fatalf("history state 未随 transition 提交: got=%q ok=%v", got, ok)
	}

	rows, err := store.db.Query(`SELECT id, isolated FROM archive_blocks ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	got := make(map[string]int)
	for rows.Next() {
		var id string
		var isolated int
		if err := rows.Scan(&id, &isolated); err != nil {
			t.Fatal(err)
		}
		got[id] = isolated
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	want := map[string]int{"before": 0, "cross": 1, "after": 1, "other-session": 0}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Archive isolation=%v, want %v", got, want)
	}

	var beforeJSON, crossJSON, afterJSON string
	if err := store.db.QueryRow(`SELECT
		MAX(CASE WHEN id='before' THEN messages_json END),
		MAX(CASE WHEN id='cross' THEN messages_json END),
		MAX(CASE WHEN id='after' THEN messages_json END)
		FROM archive_blocks`).Scan(&beforeJSON, &crossJSON, &afterJSON); err != nil {
		t.Fatal(err)
	}
	if beforeJSON == "" || crossJSON == "" || afterJSON == "" {
		t.Fatalf("transition 删除或裁剪 Archive messages: before=%q cross=%q after=%q", beforeJSON, crossJSON, afterJSON)
	}
}

func TestSQLiteStoreCommitHistoryTransitionCommonPrefixZeroIsolatesAll(t *testing.T) {
	store, err := NewSQLiteStore(filepath.Join(t.TempDir(), "history-transition-zero.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for _, block := range []ArchiveBlock{
		archiveRangeTestBlock("first", "zero-session", 0, 0, 1, "first"),
		archiveRangeTestBlock("later", "zero-session", 1, 2, 1, "later"),
	} {
		if err := store.SaveArchive(block); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.CommitHistoryTransition(HistoryTransition{
		SessionID: "zero-session", StateKey: "history_epoch:zero", StateValue: `{}`, CommonPrefix: 0,
	}); err != nil {
		t.Fatal(err)
	}
	var visible int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM archive_blocks WHERE isolated = 0`).Scan(&visible); err != nil {
		t.Fatal(err)
	}
	if visible != 0 {
		t.Fatalf("commonPrefix=0 后仍有 %d 个可见 Archive", visible)
	}
}

func TestSQLiteStoreCommitHistoryTransitionRollsBackStateAndVisibility(t *testing.T) {
	store, err := NewSQLiteStore(filepath.Join(t.TempDir(), "history-transition-rollback.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.SaveArchive(archiveRangeTestBlock("rollback", "rollback-session", 1, 4, 1, "rollback")); err != nil {
		t.Fatal(err)
	}
	const stateKey = "history_epoch:rollback"
	if err := store.PersistState(stateKey, "old-state"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`CREATE TRIGGER fail_history_archive_isolation
		BEFORE UPDATE OF isolated ON archive_blocks
		BEGIN SELECT RAISE(ABORT, 'forced archive isolation failure'); END`); err != nil {
		t.Fatal(err)
	}

	err = store.CommitHistoryTransition(HistoryTransition{
		SessionID: "rollback-session", StateKey: stateKey, StateValue: "new-state", CommonPrefix: 2,
	})
	if err == nil {
		t.Fatal("Archive isolation 故障时 transition 应失败")
	}
	if got, ok := store.LoadState(stateKey); !ok || got != "old-state" {
		t.Fatalf("失败 transition 部分提交了 state: got=%q ok=%v", got, ok)
	}
	var isolated int
	if err := store.db.QueryRow(`SELECT isolated FROM archive_blocks WHERE id='rollback'`).Scan(&isolated); err != nil {
		t.Fatal(err)
	}
	if isolated != 0 {
		t.Fatalf("失败 transition 部分提交了 visibility: isolated=%d", isolated)
	}
}

func TestSearchArchivesVisibleGateAppliesBeforeLimit(t *testing.T) {
	store, err := NewSQLiteStore(filepath.Join(t.TempDir(), "visible-search.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	isolated := archiveRangeTestBlock("isolated-top", "branch-session", 2, 5, 1, "isolated")
	isolated.Keywords = []KeywordEntry{{Word: "quasar", Source: "user_message"}, {Word: "nebula", Source: "user_message"}}
	visible := archiveRangeTestBlock("visible-second", "branch-session", 0, 1, 2, "visible")
	visible.Keywords = []KeywordEntry{{Word: "quasar", Source: "user_message"}}
	noise := archiveRangeTestBlock("noise", "branch-session", 0, 1, 2, "noise")
	noise.Keywords = []KeywordEntry{{Word: "pulsar", Source: "user_message"}, {Word: "comet", Source: "user_message"}}
	for _, block := range []ArchiveBlock{isolated, visible, noise} {
		if err := store.SaveArchive(block); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.db.Exec(`UPDATE archive_blocks SET isolated = 1 WHERE id = 'isolated-top'`); err != nil {
		t.Fatal(err)
	}

	results, err := store.SearchArchives(`"quasar" OR "nebula"`, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].ID != "visible-second" {
		t.Fatalf("SQL visibility gate 未在 LIMIT 前生效: %+v", results)
	}
}

func TestSaveArchivePersistsHistoryEpoch(t *testing.T) {
	store, err := NewSQLiteStore(filepath.Join(t.TempDir(), "archive-epoch.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	block := archiveRangeTestBlock("epoch-seven", "epoch-session", 1, 2, 7, "epoch")
	if err := store.SaveArchive(block); err != nil {
		t.Fatal(err)
	}
	var epoch uint64
	if err := store.db.QueryRow(`SELECT history_epoch FROM archive_blocks WHERE id='epoch-seven'`).Scan(&epoch); err != nil {
		t.Fatal(err)
	}
	if epoch != 7 {
		t.Fatalf("保存 Archive epoch=%d, want 7", epoch)
	}
}

func TestSaveArchiveRejectsStaleEpochAfterTransition(t *testing.T) {
	store, err := NewSQLiteStore(filepath.Join(t.TempDir(), "stale-epoch.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// 写入 epoch 2 的 Archive
	epoch2Block := archiveRangeTestBlock("block-epoch2", "stale-session", 1, 5, 2, "epoch two")
	if err := store.SaveArchive(epoch2Block); err != nil {
		t.Fatalf("SaveArchive epoch=2 失败: %v", err)
	}

	// 模拟 epoch 切换到 3（CommitHistoryTransition）
	transition := HistoryTransition{
		SessionID:    "stale-session",
		StateKey:     "history_epoch:stale-session",
		StateValue:   `{"epoch":3,"valid":true}`,
		CommonPrefix: 3,
	}
	if err := store.CommitHistoryTransition(transition); err != nil {
		t.Fatalf("CommitHistoryTransition 失败: %v", err)
	}

	// 尝试写入迟到的 epoch 1 Archive（并发的旧请求）
	staleBlock := archiveRangeTestBlock("block-stale", "stale-session", 6, 10, 1, "stale")
	err = store.SaveArchive(staleBlock)
	if err == nil {
		t.Fatal("SaveArchive 应拒绝 epoch=1 < current=2 的迟到写入")
	}
	if !strings.Contains(err.Error(), "拒绝迟到") && !strings.Contains(err.Error(), "block epoch=1 < current max=2") {
		t.Fatalf("错误消息不符合预期: %v", err)
	}

	// 验证 epoch 2 的 Archive 仍可写入（同 epoch 内正常写入）
	epoch2Block2 := archiveRangeTestBlock("block-epoch2-second", "stale-session", 11, 15, 2, "epoch two again")
	if err := store.SaveArchive(epoch2Block2); err != nil {
		t.Fatalf("SaveArchive epoch=2（同 epoch）应成功: %v", err)
	}

	// 验证 epoch 3 的新 Archive 可写入
	epoch3Block := archiveRangeTestBlock("block-epoch3", "stale-session", 16, 20, 3, "epoch three")
	if err := store.SaveArchive(epoch3Block); err != nil {
		t.Fatalf("SaveArchive epoch=3 应成功: %v", err)
	}
}

func archiveRangeTestBlock(id, sessionID string, start, end int, epoch uint64, text string) ArchiveBlock {
	keywords := []KeywordEntry{{Word: text, Source: "user_message"}}
	return ArchiveBlock{
		ID: id, SessionID: sessionID, HistoryEpoch: epoch,
		BlockRangeStart: start, BlockRangeEnd: end,
		MessageCount: end - start + 1, EstimatedTokens: 10,
		Messages:    []Message{{Role: "user", Content: mustMarshal(text)}},
		SummaryText: text, Keywords: keywords,
	}
}

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

func TestSearchArchivesStableOrderingAndFields(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "stable.db")
	store, err := NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteStore failed: %v", err)
	}
	defer store.Close()

	blocks := []ArchiveBlock{
		{
			ID: "block-b", SessionID: "session-b", ContentHash: "hash-b",
			BlockRangeStart: 1, BlockRangeEnd: 4, MessageCount: 4, EstimatedTokens: 100,
			SummaryText: "beta summary",
			Keywords:    []KeywordEntry{{Word: "flimflam", Source: "user_message"}, {Word: "warbler", Source: "user_message"}},
		},
		{
			ID: "block-a", SessionID: "session-a", ContentHash: "hash-a",
			BlockRangeStart: 1, BlockRangeEnd: 4, MessageCount: 4, EstimatedTokens: 100,
			SummaryText: "alpha summary",
			Keywords:    []KeywordEntry{{Word: "flimflam", Source: "user_message"}, {Word: "warbler", Source: "user_message"}},
		},
	}
	for _, block := range blocks {
		if err := store.SaveArchive(block); err != nil {
			t.Fatalf("SaveArchive(%s): %v", block.ID, err)
		}
	}
	if _, err := store.db.Exec(`UPDATE archive_blocks SET created_at = '2026-01-01 00:00:00'`); err != nil {
		t.Fatalf("固定 created_at: %v", err)
	}

	var first []ArchiveSummary
	for run := 0; run < 3; run++ {
		got, err := store.SearchArchives(`"flimflam" OR "warbler"`, 99)
		if err != nil {
			t.Fatalf("SearchArchives run %d: %v", run, err)
		}
		if len(got) != 2 {
			t.Fatalf("run %d results=%d, want 2", run, len(got))
		}
		if run == 0 {
			first = got
		} else if !reflect.DeepEqual(got, first) {
			t.Fatalf("run %d ordering changed:\nfirst=%+v\ngot=%+v", run, first, got)
		}
	}

	if first[0].ID != "block-a" || first[1].ID != "block-b" {
		t.Fatalf("stable ID tie-break = [%s %s], want [block-a block-b]", first[0].ID, first[1].ID)
	}
	for _, got := range first {
		if got.ContentHash == "" {
			t.Fatalf("%s missing content_hash", got.ID)
		}
		if got.MatchedTermCount != 2 || !reflect.DeepEqual(got.MatchedTerms, []string{"flimflam", "warbler"}) {
			t.Fatalf("%s matched terms=%v/%d, want [flimflam warbler]/2", got.ID, got.MatchedTerms, got.MatchedTermCount)
		}
		if got.Rank >= 0 {
			t.Fatalf("%s rank=%f, want negative BM25 aggregate", got.ID, got.Rank)
		}
	}
}

func TestSQLiteStoreCloseRemovesWALCompanions(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "close-cleanup.db")
	store, err := NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.PersistState("key", "value"); err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	for _, suffix := range []string{"-wal", "-shm"} {
		if _, err := os.Stat(dbPath + suffix); !os.IsNotExist(err) {
			t.Fatalf("Close 后伴生文件仍存在 %s: %v", dbPath+suffix, err)
		}
	}
}

func TestSQLiteStoreCloseBusyPreservesCommittedWALData(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "close-busy.db")
	store, err := NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.PersistState("before-reader", "one"); err != nil {
		t.Fatal(err)
	}

	reader, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	reader.SetMaxOpenConns(1)
	tx, err := reader.Begin()
	if err != nil {
		reader.Close()
		t.Fatal(err)
	}
	var value string
	if err := tx.QueryRow(`SELECT value FROM frozen_state WHERE key = 'before-reader'`).Scan(&value); err != nil {
		_ = tx.Rollback()
		reader.Close()
		t.Fatal(err)
	}
	if err := store.PersistState("committed-in-wal", "must-survive"); err != nil {
		_ = tx.Rollback()
		reader.Close()
		t.Fatal(err)
	}

	if err := store.Close(); err == nil {
		_ = tx.Rollback()
		reader.Close()
		t.Fatal("活跃 reader 下 TRUNCATE checkpoint 应报告 busy")
	}
	if _, err := os.Stat(dbPath + "-wal"); err != nil {
		_ = tx.Rollback()
		reader.Close()
		t.Fatalf("busy checkpoint 后 WAL 未保留: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		reader.Close()
		t.Fatal(err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got, ok := reopened.LoadState("committed-in-wal"); !ok || got != "must-survive" {
		t.Fatalf("busy checkpoint 后已提交 WAL 数据丢失: got=%q ok=%v", got, ok)
	}
}

func TestSQLiteStoreCloseCheckpointErrorPreservesCompanions(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "close-error.db")
	store, err := NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.db.Close(); err != nil {
		t.Fatal(err)
	}
	for _, suffix := range []string{"-wal", "-shm"} {
		if err := os.WriteFile(dbPath+suffix, []byte("preserve"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Close(); err == nil {
		t.Fatal("已关闭 DB 的 checkpoint 应返回错误")
	}
	for _, suffix := range []string{"-wal", "-shm"} {
		data, err := os.ReadFile(dbPath + suffix)
		if err != nil || string(data) != "preserve" {
			t.Fatalf("checkpoint error 后伴生文件未保留 %s: data=%q err=%v", suffix, data, err)
		}
	}
}

func TestSQLiteStoreDeleteState(t *testing.T) {
	store, err := NewSQLiteStore(filepath.Join(t.TempDir(), "delete-state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.PersistState("frozen:thread", "value"); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteState("frozen:thread"); err != nil {
		t.Fatal(err)
	}
	if _, ok := store.LoadState("frozen:thread"); ok {
		t.Fatal("DeleteState 后仍能加载状态")
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

func TestSaveArchiveIgnoresForgedContentHash(t *testing.T) {
	store, err := NewSQLiteStore(filepath.Join(t.TempDir(), "forged-hash.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	first := archiveTestBlock("first", "first content")
	second := archiveTestBlock("second", "second content")
	first.ContentHash = "forged-same-hash"
	second.ContentHash = "forged-same-hash"
	if err := store.SaveArchive(first); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveArchive(second); err != nil {
		t.Fatal(err)
	}
	assertArchiveCounts(t, store, 2, len(first.Keywords)+len(second.Keywords))
	var forgedCount int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM archive_blocks WHERE content_hash = ?`, "forged-same-hash").Scan(&forgedCount); err != nil {
		t.Fatal(err)
	}
	if forgedCount != 0 {
		t.Fatalf("调用方伪造 hash 被持久化: count=%d", forgedCount)
	}
}

func TestSaveArchivePreservesDistinctMessageUnknownFields(t *testing.T) {
	store, err := NewSQLiteStore(filepath.Join(t.TempDir(), "message-fields.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	decode := func(raw string) Message {
		t.Helper()
		var message Message
		if err := json.Unmarshal([]byte(raw), &message); err != nil {
			t.Fatalf("unmarshal archive message: %v", err)
		}
		return message
	}
	first := archiveTestBlock("message-fields-a", "same content")
	second := archiveTestBlock("message-fields-b", "same content")
	first.Messages = []Message{decode(`{"role":"user","content":"same content","future":null}`)}
	second.Messages = []Message{decode(`{"role":"user","content":"same content","future":{"mode":"strict"}}`)}

	if err := store.SaveArchive(first); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveArchive(second); err != nil {
		t.Fatal(err)
	}
	assertArchiveCounts(t, store, 2, len(first.Keywords)+len(second.Keywords))
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

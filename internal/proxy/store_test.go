package proxy

import (
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

// ── 统一 error-aware StateLoader 的 missing/error 真值边界 ──

// assertStateLoadFailure 收敛四类失败断言：结果必须是失败而非 missing，
// 错误链可用 errors.Is 分类，且对外事实里没有 key 或数据库路径。
func assertStateLoadFailure(t *testing.T, result StateLoadResult, key, dbPath string, wantSentinel error, wantClass StateLoadFailureClass) {
	t.Helper()
	if result.Found {
		t.Fatalf("读取失败被当成命中: %+v", result)
	}
	if result.Value != "" {
		t.Fatalf("失败结果携带了 value: %q", result.Value)
	}
	if result.Err == nil {
		t.Fatal("读取失败被伪装成 Err=nil 的不存在")
	}
	if !errors.Is(result.Err, wantSentinel) {
		t.Fatalf("错误链无法识别 %v: %v", wantSentinel, result.Err)
	}
	if result.FailureClass != wantClass {
		t.Fatalf("failure class=%q, want %q", result.FailureClass, wantClass)
	}
	if strings.Contains(string(result.FailureClass), key) || strings.Contains(string(result.FailureClass), dbPath) {
		t.Fatalf("failure class 泄漏 key/path: %q", result.FailureClass)
	}
	if strings.Contains(result.Err.Error(), key) || strings.Contains(result.Err.Error(), dbPath) {
		t.Fatalf("错误消息泄漏 key/path: %v", result.Err)
	}
}

func TestSQLiteStoreLoadStateResultExisting(t *testing.T) {
	store, err := NewSQLiteStore(filepath.Join(t.TempDir(), "load-existing.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	var loader StateLoader = store
	if err := store.PersistState("frozen:thread", `{"epoch":1}`); err != nil {
		t.Fatal(err)
	}
	result := loader.LoadStateResult("frozen:thread")
	if !result.Found || result.Err != nil || result.Value != `{"epoch":1}` {
		t.Fatalf("命中结果=%+v", result)
	}
	if result.FailureClass != StateLoadFailureNone {
		t.Fatalf("命中 failure class=%q, want none", result.FailureClass)
	}
}

func TestSQLiteStoreLoadStateResultMissing(t *testing.T) {
	store, err := NewSQLiteStore(filepath.Join(t.TempDir(), "load-missing.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	result := store.LoadStateResult("frozen:absent")
	if result.Found {
		t.Fatalf("不存在的 key 被当成命中: %+v", result)
	}
	// 只有 sql.ErrNoRows 允许出现 Found=false 且 Err=nil。
	if result.Err != nil {
		t.Fatalf("ErrNoRows 不应带错误: %v", result.Err)
	}
	if result.FailureClass != StateLoadFailureNone {
		t.Fatalf("missing failure class=%q, want none", result.FailureClass)
	}
	if value, ok := store.LoadState("frozen:absent"); ok || value != "" {
		t.Fatalf("兼容 wrapper missing=%q/%v", value, ok)
	}
}

func TestSQLiteStoreLoadStateResultClosed(t *testing.T) {
	dbPath := filepath.Join(tempDirRetryCleanup(t), "load-closed.db")
	store, err := NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.PersistState("frozen:closed", "value"); err != nil {
		t.Fatal(err)
	}
	if err := store.db.Close(); err != nil {
		t.Fatal(err)
	}

	result := store.LoadStateResult("frozen:closed")
	assertStateLoadFailure(t, result, "frozen:closed", dbPath, ErrStateLoadClosed, StateLoadFailureSQLiteClosed)
	// 兼容 wrapper 只能降级为 false，不能把 closed 说成不存在给生产使用。
	if value, ok := store.LoadState("frozen:closed"); ok || value != "" {
		t.Fatalf("兼容 wrapper closed=%q/%v", value, ok)
	}
}

func TestSQLiteStoreLoadStateResultBusy(t *testing.T) {
	dbPath := filepath.Join(tempDirRetryCleanup(t), "load-busy.db")
	store, err := NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.PersistState("frozen:busy", "value"); err != nil {
		_ = store.Close()
		t.Fatal(err)
	}

	// 释放 store 的空闲连接，让另一个连接可以取得 EXCLUSIVE 文件锁。
	store.db.SetMaxIdleConns(0)
	deadline := time.Now().Add(5 * time.Second)
	for store.db.Stats().OpenConnections > 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := store.db.Stats().OpenConnections; got != 0 {
		_ = store.Close()
		t.Fatalf("空闲连接未释放: OpenConnections=%d", got)
	}

	locker, err := sql.Open("sqlite", dbPath)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	locker.SetMaxOpenConns(1)
	if _, err := locker.Exec("PRAGMA locking_mode=EXCLUSIVE"); err != nil {
		locker.Close()
		_ = store.Close()
		t.Fatal(err)
	}
	if _, err := locker.Exec(`INSERT OR REPLACE INTO frozen_state (key, value) VALUES ('lock-holder', 'held')`); err != nil {
		locker.Close()
		_ = store.Close()
		t.Fatalf("独占写入失败，无法构造 busy 场景: %v", err)
	}

	result := store.LoadStateResult("frozen:busy")
	locker.Close()
	_ = store.Close()
	assertStateLoadFailure(t, result, "frozen:busy", dbPath, ErrStateLoadBusy, StateLoadFailureSQLiteBusy)
}

func TestSQLiteStoreLoadStateResultQueryError(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "load-query-error.db")
	store, err := NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.PersistState("frozen:query", "value"); err != nil {
		t.Fatal(err)
	}
	// 真实 SQLite 查询失败：目标表消失，而不是注入假 error。
	if _, err := store.db.Exec(`ALTER TABLE frozen_state RENAME TO frozen_state_moved`); err != nil {
		t.Fatal(err)
	}

	result := store.LoadStateResult("frozen:query")
	assertStateLoadFailure(t, result, "frozen:query", dbPath, ErrStateLoadQueryFailed, StateLoadFailureQueryFailed)
	if errors.Is(result.Err, ErrStateLoadClosed) || errors.Is(result.Err, ErrStateLoadBusy) {
		t.Fatalf("query error 被误分类: %v", result.Err)
	}

	if _, err := store.db.Exec(`ALTER TABLE frozen_state_moved RENAME TO frozen_state`); err != nil {
		t.Fatal(err)
	}
	if got := store.LoadStateResult("frozen:query"); !got.Found || got.Value != "value" {
		t.Fatalf("恢复后读取=%+v", got)
	}
}

func TestHistoryTransitionRemainsSynchronous(t *testing.T) {
	store, err := NewSQLiteStore(filepath.Join(t.TempDir(), "transition-sync.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// history transition 不属于 PersistenceBackend 合同：writer 在类型层面
	// 就无法提交它，因此不存在"排队后再落库"的窗口。
	// 只能对接口类型本身取方法集——对接口值做断言检查的是动态类型，
	// *SQLiteStore 当然带着 CommitHistoryTransition，证明不了合同边界。
	backendType := reflect.TypeOf((*PersistenceBackend)(nil)).Elem()
	backendMethods := make([]string, 0, backendType.NumMethod())
	for index := 0; index < backendType.NumMethod(); index++ {
		backendMethods = append(backendMethods, backendType.Method(index).Name)
	}
	if !reflect.DeepEqual(backendMethods, []string{"DeleteState", "PersistState"}) {
		t.Fatalf("PersistenceBackend 方法集=%v, want [DeleteState PersistState]", backendMethods)
	}

	if err := store.SaveArchive(archiveRangeTestBlock("sync", "sync-session", 1, 4, 1, "sync")); err != nil {
		t.Fatal(err)
	}
	const stateKey = "history_epoch:sync"
	if err := store.PersistState(stateKey, "old-state"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`CREATE TRIGGER fail_sync_transition
		BEFORE UPDATE OF isolated ON archive_blocks
		BEGIN SELECT RAISE(ABORT, 'forced isolation failure'); END`); err != nil {
		t.Fatal(err)
	}
	if err := store.CommitHistoryTransition(HistoryTransition{
		SessionID: "sync-session", StateKey: stateKey, StateValue: "new-state", CommonPrefix: 2,
	}); err == nil {
		t.Fatal("transition 失败时必须同步返回错误")
	}
	// 同步 fail-closed：state 与 visibility 都必须回滚。
	if got := store.LoadStateResult(stateKey); !got.Found || got.Value != "old-state" {
		t.Fatalf("失败 transition 部分提交 state: %+v", got)
	}
	var isolated int
	if err := store.db.QueryRow(`SELECT isolated FROM archive_blocks WHERE id='sync'`).Scan(&isolated); err != nil {
		t.Fatal(err)
	}
	if isolated != 0 {
		t.Fatalf("失败 transition 部分提交 visibility: %d", isolated)
	}

	if _, err := store.db.Exec(`DROP TRIGGER fail_sync_transition`); err != nil {
		t.Fatal(err)
	}
	if err := store.CommitHistoryTransition(HistoryTransition{
		SessionID: "sync-session", StateKey: stateKey, StateValue: "new-state", CommonPrefix: 2,
	}); err != nil {
		t.Fatalf("成功 transition 应同步提交: %v", err)
	}
	if got := store.LoadStateResult(stateKey); !got.Found || got.Value != "new-state" {
		t.Fatalf("成功 transition 未同步可见: %+v", got)
	}
}

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

	// 模拟 epoch 切换到 3：水位来自权威 history state，而不是 archive_blocks。
	commitTestHistoryEpoch(t, store, "stale-session", 3, 3)

	// 尝试写入迟到的 epoch 1 Archive（并发的旧请求）
	staleBlock := archiveRangeTestBlock("block-stale", "stale-session", 6, 10, 1, "stale")
	err = store.SaveArchive(staleBlock)
	if err == nil {
		t.Fatal("SaveArchive 应拒绝 epoch=1 < current=3 的迟到写入")
	}
	if !strings.Contains(err.Error(), "拒绝迟到") {
		t.Fatalf("错误消息不符合预期: %v", err)
	}

	// transition 已提交到 epoch 3 后，epoch 2 同样是迟到写入，必须一起拒绝。
	epoch2Late := archiveRangeTestBlock("block-epoch2-late", "stale-session", 11, 15, 2, "epoch two again")
	if err := store.SaveArchive(epoch2Late); err == nil {
		t.Fatal("SaveArchive 应拒绝 epoch=2 < current=3 的迟到写入")
	}

	// 验证 epoch 3 的新 Archive 可写入
	epoch3Block := archiveRangeTestBlock("block-epoch3", "stale-session", 16, 20, 3, "epoch three")
	if err := store.SaveArchive(epoch3Block); err != nil {
		t.Fatalf("SaveArchive epoch=3 应成功: %v", err)
	}
}

// ── Task 3：权威 epoch 门禁、canonical ID 与 session-visible 读取 ──

// commitTestHistoryEpoch 用权威 state key 提交一次 epoch 切换。
func commitTestHistoryEpoch(t *testing.T, store *SQLiteStore, sessionID string, epoch uint64, commonPrefix int) {
	t.Helper()
	value, err := json.Marshal(historyEpochPersisted{
		Version: historyEpochStateVersion, Epoch: epoch, Valid: false,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CommitHistoryTransition(HistoryTransition{
		SessionID:    sessionID,
		StateKey:     historyEpochPersistenceKey(sessionID),
		StateValue:   string(value),
		CommonPrefix: commonPrefix,
	}); err != nil {
		t.Fatalf("提交 epoch %d transition: %v", epoch, err)
	}
}

func archiveRowExists(t *testing.T, store *SQLiteStore, id string) bool {
	t.Helper()
	var count int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM archive_blocks WHERE id = ?`, id).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count > 0
}

func TestSaveArchiveUsesAuthoritativeHistoryEpoch(t *testing.T) {
	store, err := NewSQLiteStore(filepath.Join(t.TempDir(), "authoritative-epoch.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// 初始 state 尚未落盘时仍允许建立 epoch 1。
	initial, err := store.SaveArchiveResult(archiveRangeTestBlock("initial", "epoch-session", 0, 1, 1, "initial"))
	if err != nil {
		t.Fatalf("初始 epoch 1 写入失败: %v", err)
	}
	if initial.State != persistenceStateSaved || initial.ID == "" {
		t.Fatalf("初始 commit result=%+v", initial)
	}

	commitTestHistoryEpoch(t, store, "epoch-session", 2, 99)
	current, err := store.SaveArchiveResult(archiveRangeTestBlock("epoch-two", "epoch-session", 2, 3, 2, "current"))
	if err != nil || current.State != persistenceStateSaved {
		t.Fatalf("epoch 2 写入=%+v err=%v", current, err)
	}

	// 水位来自权威 state：epoch 3 只存在于 state，archive_blocks 的 MAX(epoch) 仍是 2。
	commitTestHistoryEpoch(t, store, "epoch-session", 3, 99)
	var maxEpoch uint64
	if err := store.db.QueryRow(`SELECT COALESCE(MAX(history_epoch), 0) FROM archive_blocks
		WHERE session_id = 'epoch-session'`).Scan(&maxEpoch); err != nil {
		t.Fatal(err)
	}
	if maxEpoch != 2 {
		t.Fatalf("前置条件失效：archive MAX(epoch)=%d, want 2", maxEpoch)
	}
	late, err := store.SaveArchiveResult(archiveRangeTestBlock("late-epoch-two", "epoch-session", 4, 5, 2, "late"))
	if err == nil {
		t.Fatal("MAX(epoch) 仍是 2，但权威 epoch 已是 3，epoch 2 写入必须拒绝")
	}
	if late.State != persistenceStateFailed || late.FailureClass != persistenceFailureArchive {
		t.Fatalf("拒绝结果=%+v, want failed/sqlite_archive", late)
	}
	if late.ID != "" {
		t.Fatalf("失败结果携带了可用 ID: %q", late.ID)
	}
	if archiveRowExists(t, store, "late-epoch-two") {
		t.Fatal("被拒绝的迟到写入仍留下了数据库行")
	}
}

func TestSaveArchiveRejectsLateOldEpochWithoutNewArchiveRow(t *testing.T) {
	store, err := NewSQLiteStore(filepath.Join(t.TempDir(), "late-epoch.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	commitTestHistoryEpoch(t, store, "branch-session", 1, 99)
	if _, err := store.SaveArchiveResult(archiveRangeTestBlock("epoch-one", "branch-session", 0, 2, 1, "epoch one")); err != nil {
		t.Fatalf("epoch 1 写入失败: %v", err)
	}

	// 切到 epoch 2 并隔离旧分支；此时 epoch 2 尚无任何 Archive 行。
	commitTestHistoryEpoch(t, store, "branch-session", 2, 0)
	var epoch2Rows int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM archive_blocks
		WHERE session_id = 'branch-session' AND history_epoch = 2`).Scan(&epoch2Rows); err != nil {
		t.Fatal(err)
	}
	if epoch2Rows != 0 {
		t.Fatalf("前置条件失效：epoch 2 已有 %d 行", epoch2Rows)
	}

	result, err := store.SaveArchiveResult(archiveRangeTestBlock("late-epoch-one", "branch-session", 3, 4, 1, "late"))
	if err == nil {
		t.Fatal("epoch 2 尚无 Archive 时，迟到 epoch 1 写入仍必须被同步拒绝")
	}
	if result.State != persistenceStateFailed || result.ID != "" {
		t.Fatalf("迟到写入结果=%+v", result)
	}
	if archiveRowExists(t, store, "late-epoch-one") {
		t.Fatal("被拒绝的迟到写入留下了数据库行")
	}
	summaries, err := store.SearchVisibleArchives("branch-session", `"late"`, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(summaries) != 0 {
		t.Fatalf("被拒绝的迟到写入产生了可见记录: %+v", summaries)
	}
}

func TestSaveArchiveReturnsCanonicalIDOnContentConflict(t *testing.T) {
	store, err := NewSQLiteStore(filepath.Join(t.TempDir(), "canonical-id.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	first := archiveRangeTestBlock("caller-generated-a", "conflict-session", 1, 4, 1, "same content")
	second := archiveRangeTestBlock("caller-generated-b", "conflict-session", 1, 4, 1, "same content")

	firstResult, err := store.SaveArchiveResult(first)
	if err != nil {
		t.Fatal(err)
	}
	if firstResult.ID != first.ID || firstResult.State != persistenceStateSaved {
		t.Fatalf("首次 commit result=%+v", firstResult)
	}

	secondResult, err := store.SaveArchiveResult(second)
	if err != nil {
		t.Fatalf("内容幂等冲突不应报错: %v", err)
	}
	if secondResult.State != persistenceStateSaved {
		t.Fatalf("幂等冲突 state=%s, want saved", secondResult.State)
	}
	// 必须返回数据库中真实存在的既有行 ID，而不是调用方新生成、未落库的 ID。
	if secondResult.ID != first.ID {
		t.Fatalf("冲突返回 ID=%q, want 既有 canonical %q", secondResult.ID, first.ID)
	}
	if archiveRowExists(t, store, second.ID) {
		t.Fatalf("调用方新 ID %q 被写入数据库", second.ID)
	}
	assertArchiveCounts(t, store, 1, len(first.Keywords))

	got, found, err := store.GetVisibleArchiveByID("conflict-session", secondResult.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !found || got.ID != first.ID {
		t.Fatalf("canonical ID 不可查询: found=%v got=%+v", found, got)
	}
	if got.HistoryEpoch != 1 {
		t.Fatalf("ArchiveSummary.HistoryEpoch=%d, want 1", got.HistoryEpoch)
	}
}

func TestGetVisibleArchiveByIDSessionAndBranchGate(t *testing.T) {
	store, err := NewSQLiteStore(filepath.Join(t.TempDir(), "visible-exact.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	commitTestHistoryEpoch(t, store, "visible-session", 2, 99)
	for _, block := range []ArchiveBlock{
		archiveRangeTestBlock("visible", "visible-session", 0, 1, 2, "visible body"),
		archiveRangeTestBlock("old-branch", "visible-session", 2, 3, 2, "old branch body"),
		archiveRangeTestBlock("other", "other-session", 0, 1, 1, "other session body"),
	} {
		if _, err := store.SaveArchiveResult(block); err != nil {
			t.Fatalf("SaveArchiveResult(%s): %v", block.ID, err)
		}
	}
	// 未来 epoch 行只能由旧库/旧写入产生；它晚于权威水位，必须不可见。
	if _, err := store.db.Exec(`INSERT INTO archive_blocks
		(id, session_id, block_range_start, block_range_end, message_count, estimated_tokens,
		 messages_json, summary_text, created_at, content_hash, history_epoch, isolated)
		VALUES ('future', 'visible-session', 4, 5, 2, 10, '[]', 'future body', datetime('now'), 'future-hash', 9, 0)`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`UPDATE archive_blocks SET isolated = 1 WHERE id = 'old-branch'`); err != nil {
		t.Fatal(err)
	}

	if got, found, err := store.GetVisibleArchiveByID("visible-session", "visible"); err != nil || !found || got.ID != "visible" {
		t.Fatalf("同 session 当前分支记录不可见: found=%v got=%+v err=%v", found, got, err)
	}
	for name, lookup := range map[string][2]string{
		"跨 session":    {"other-session", "visible"},
		"跨 session 反向": {"visible-session", "other"},
		"旧分支":          {"visible-session", "old-branch"},
		"晚于权威 epoch":   {"visible-session", "future"},
		"缺少 session":   {"", "visible"},
		"不存在":          {"visible-session", "missing"},
	} {
		got, found, err := store.GetVisibleArchiveByID(lookup[0], lookup[1])
		if err != nil {
			t.Fatalf("%s 查询错误: %v", name, err)
		}
		if found {
			t.Fatalf("%s 越过 visibility 门禁: %+v", name, got)
		}
	}
}

func TestSearchVisibleArchivesSessionAndBranchGate(t *testing.T) {
	store, err := NewSQLiteStore(filepath.Join(t.TempDir(), "visible-search-gate.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	commitTestHistoryEpoch(t, store, "search-session", 2, 99)
	mine := archiveRangeTestBlock("mine", "search-session", 0, 1, 2, "mine")
	mine.Keywords = []KeywordEntry{{Word: "quasar", Source: "user_message"}}
	stale := archiveRangeTestBlock("stale-branch", "search-session", 2, 3, 2, "stale")
	stale.Keywords = []KeywordEntry{{Word: "quasar", Source: "user_message"}}
	foreign := archiveRangeTestBlock("foreign", "foreign-session", 0, 1, 1, "foreign")
	foreign.Keywords = []KeywordEntry{{Word: "quasar", Source: "user_message"}}
	for _, block := range []ArchiveBlock{mine, stale, foreign} {
		if _, err := store.SaveArchiveResult(block); err != nil {
			t.Fatalf("SaveArchiveResult(%s): %v", block.ID, err)
		}
	}
	if _, err := store.db.Exec(`UPDATE archive_blocks SET isolated = 1 WHERE id = 'stale-branch'`); err != nil {
		t.Fatal(err)
	}

	results, err := store.SearchVisibleArchives("search-session", `"quasar"`, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].ID != "mine" {
		t.Fatalf("session/visibility 门禁失效: %+v", results)
	}
	if results[0].HistoryEpoch != 2 {
		t.Fatalf("ArchiveSummary.HistoryEpoch=%d, want 2", results[0].HistoryEpoch)
	}
	if results[0].SessionID != "search-session" {
		t.Fatalf("返回了其他 session: %q", results[0].SessionID)
	}

	// 旧的无 session 入口仍能看到三条，证明新入口的门禁不是数据缺失造成的。
	legacy, err := store.SearchArchives(`"quasar"`, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(legacy) != 2 {
		t.Fatalf("旧入口结果=%d, want 2（isolated 之外的两条）", len(legacy))
	}

	if got, err := store.SearchVisibleArchives("", `"quasar"`, 3); err != nil || len(got) != 0 {
		t.Fatalf("缺少 session 时结果=%+v err=%v", got, err)
	}
	if got, err := store.SearchVisibleArchives("foreign-session", `"quasar"`, 3); err != nil || len(got) != 1 || got[0].ID != "foreign" {
		t.Fatalf("其他 session 只应看到自己的记录: %+v err=%v", got, err)
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

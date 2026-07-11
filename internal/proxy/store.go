package proxy

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	_ "modernc.org/sqlite" // 纯 Go SQLite 驱动，通过 init() 注册 database/sql
)

// SQLiteStore 管理 archive + decay state 的 SQLite 数据库。
type SQLiteStore struct {
	db   *sql.DB
	path string
}

// ArchiveBlock 折叠存档块——包含被折叠的原始消息及元数据。
type ArchiveBlock struct {
	ID              string         `json:"id"`
	SessionID       string         `json:"session_id"`
	ContentHash     string         `json:"content_hash"`
	BlockRangeStart int            `json:"block_range_start"`
	BlockRangeEnd   int            `json:"block_range_end"`
	MessageCount    int            `json:"message_count"`
	EstimatedTokens int            `json:"estimated_tokens"`
	Messages        []Message      `json:"messages"`
	SummaryText     string         `json:"summary_text"`
	CreatedAt       string         `json:"created_at"`
	Keywords        []KeywordEntry `json:"keywords"`
}

// ArchiveSummary SearchArchives 返回的结果——不反序列化 Messages，
// 原始 messages_json 以字符串携带，由重展开侧按需反序列化（预算允许时完整展开）。
type ArchiveSummary struct {
	ID               string   `json:"id"`
	SessionID        string   `json:"session_id"`
	ContentHash      string   `json:"content_hash"`
	BlockRangeStart  int      `json:"block_range_start"`
	BlockRangeEnd    int      `json:"block_range_end"`
	MessageCount     int      `json:"message_count"`
	EstimatedTokens  int      `json:"estimated_tokens"`
	SummaryText      string   `json:"summary_text"`
	MessagesJSON     string   `json:"messages_json"`
	CreatedAt        string   `json:"created_at"`
	MatchedTerms     []string `json:"matched_terms"`
	MatchedTermCount int      `json:"matched_term_count"`
	Rank             float64  `json:"rank"`
}

// KeywordEntry 关键词条目——关联到 archive block 的关键词及其来源。
type KeywordEntry struct {
	Word   string `json:"word"`
	Source string `json:"source"` // 取值: "file_path"、"tool_name"、"user_message"
}

// NewSQLiteStore 打开或创建指定路径的 SQLite 数据库。
// 设置 PRAGMA、创建 schema，对每一步错误进行 wrap 返回。
// 内建损坏检测与自动恢复：检测到 corruption 时删除损坏文件并重试一次。
func NewSQLiteStore(path string) (*SQLiteStore, error) {
	// 确保父目录存在（纯 Go SQLite 驱动不会自动创建）
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("创建数据目录失败 %s: %w", dir, err)
	}

	// 孤儿 WAL 清理：若主 DB 不存在，清理进程闪退残留的 -wal/-shm
	// （Windows 上残留的 WAL/SHM 可能导致下次打开报 "file is not a database"）
	removeStaleWALFiles(path)

	s, err := tryInitDB(path)
	if err != nil && isCorruptionError(err) {
		// 自动恢复：删除损坏文件（主 DB + -wal + -shm），从头重建
		removeDBFiles(path)
		s, err = tryInitDB(path)
	}
	return s, err
}

// tryInitDB 尝试打开并初始化 SQLite 数据库。
// 不含损坏恢复逻辑（由 NewSQLiteStore 外层处理）。
func tryInitDB(path string) (*SQLiteStore, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("打开 sqlite 数据库失败: %w", err)
	}
	// Pitfall 4 缓解：单写入者，避免并发写入冲突
	db.SetMaxOpenConns(1)

	pragmas := []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA busy_timeout=5000",
		"PRAGMA synchronous=NORMAL",
		"PRAGMA journal_size_limit=10485760",
		"PRAGMA cache_size=-16000",
		"PRAGMA temp_store=MEMORY",
		"PRAGMA foreign_keys=ON",
	}
	for _, p := range pragmas {
		if _, err := db.Exec(p); err != nil {
			db.Close()
			return nil, fmt.Errorf("pragma %s: %w", p, err)
		}
	}

	s := &SQLiteStore{db: db, path: path}
	if err := s.createSchema(); err != nil {
		db.Close()
		return nil, fmt.Errorf("建表失败: %w", err)
	}

	return s, nil
}

// createSchema 创建全部表、索引、FTS5 虚拟表及触发器。
// 所有 DDL 使用 IF NOT EXISTS，幂等执行。
func (s *SQLiteStore) createSchema() error {
	// 存档块主表。content_hash 对新库直接创建；旧库由下方显式迁移补列。
	if _, err := s.db.Exec(`CREATE TABLE IF NOT EXISTS archive_blocks (
			id               TEXT PRIMARY KEY,
			session_id       TEXT NOT NULL,
			block_range_start INTEGER NOT NULL,
			block_range_end   INTEGER NOT NULL,
			message_count    INTEGER NOT NULL,
			estimated_tokens  INTEGER NOT NULL,
			messages_json    TEXT NOT NULL,
			summary_text     TEXT NOT NULL,
			created_at       TEXT NOT NULL DEFAULT (datetime('now')),
			content_hash     TEXT
		)`); err != nil {
		return fmt.Errorf("执行 schema 失败: %w", err)
	}
	if err := s.ensureArchiveContentHashSchema(); err != nil {
		return err
	}

	schema := []string{
		`CREATE INDEX IF NOT EXISTS idx_archive_blocks_session
		 ON archive_blocks(session_id)`,

		// 关键词表
		`CREATE TABLE IF NOT EXISTS archive_keywords (
			id       INTEGER PRIMARY KEY AUTOINCREMENT,
			block_id TEXT NOT NULL REFERENCES archive_blocks(id),
			keyword  TEXT NOT NULL,
			source   TEXT NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_archive_keywords_block
		 ON archive_keywords(block_id)`,
		`CREATE INDEX IF NOT EXISTS idx_archive_keywords_keyword
		 ON archive_keywords(keyword)`,

		// FTS5 虚拟表——内容外部存储，自动同步
		`CREATE VIRTUAL TABLE IF NOT EXISTS archive_keywords_fts
		 USING fts5(keyword, content='archive_keywords', content_rowid='id')`,

		// INSERT 触发器：自动同步到 FTS5
		`CREATE TRIGGER IF NOT EXISTS archive_keywords_ai
		 AFTER INSERT ON archive_keywords BEGIN
			INSERT INTO archive_keywords_fts(rowid, keyword)
			VALUES (new.id, new.keyword);
		END`,

		// DELETE 触发器：自动从 FTS5 移除
		`CREATE TRIGGER IF NOT EXISTS archive_keywords_ad
		 AFTER DELETE ON archive_keywords BEGIN
			INSERT INTO archive_keywords_fts(archive_keywords_fts, rowid, keyword)
			VALUES ('delete', old.id, old.keyword);
		END`,

		// Decay 状态持久化表 (D-14)
		`CREATE TABLE IF NOT EXISTS decay_state (
			session_id       TEXT PRIMARY KEY,
			phase            INTEGER NOT NULL DEFAULT 0,
			decision_counter REAL NOT NULL DEFAULT 0.0,
			last_request     TEXT NOT NULL DEFAULT (datetime('now'))
		)`,

		// Frozen 状态持久化表 (Phase 4, D-07)
		`CREATE TABLE IF NOT EXISTS frozen_state (
			key         TEXT PRIMARY KEY,
			value       TEXT NOT NULL,
			updated_at  TEXT NOT NULL DEFAULT (datetime('now'))
		)`,
	}
	for _, ddl := range schema {
		if _, err := s.db.Exec(ddl); err != nil {
			return fmt.Errorf("执行 schema 失败: %w", err)
		}
	}
	return nil
}

// ensureArchiveContentHashSchema 为旧 archive_blocks 表幂等补充内容指纹。
// 旧行保持 NULL，不回填也不清理；partial unique index 只约束新写入的非空 hash。
func (s *SQLiteStore) ensureArchiveContentHashSchema() error {
	rows, err := s.db.Query(`PRAGMA table_info(archive_blocks)`)
	if err != nil {
		return fmt.Errorf("读取 archive_blocks schema 失败: %w", err)
	}
	hasContentHash := false
	for rows.Next() {
		var cid, notNull, primaryKey int
		var name, columnType string
		var defaultValue sql.NullString
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &primaryKey); err != nil {
			rows.Close()
			return fmt.Errorf("扫描 archive_blocks schema 失败: %w", err)
		}
		if name == "content_hash" {
			hasContentHash = true
		}
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("关闭 archive_blocks schema 查询失败: %w", err)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("遍历 archive_blocks schema 失败: %w", err)
	}

	if !hasContentHash {
		if _, err := s.db.Exec(`ALTER TABLE archive_blocks ADD COLUMN content_hash TEXT`); err != nil {
			return fmt.Errorf("迁移 archive_blocks.content_hash 失败: %w", err)
		}
	}

	if _, err := s.db.Exec(`CREATE UNIQUE INDEX IF NOT EXISTS idx_archive_blocks_content_identity
		ON archive_blocks(session_id, block_range_start, block_range_end, content_hash)
		WHERE content_hash IS NOT NULL`); err != nil {
		return fmt.Errorf("创建 archive content_hash 唯一索引失败: %w", err)
	}
	return nil
}

// SaveArchive 将 ArchiveBlock 及其关键词事务性写入 SQLite（D-10, D-15）。
// 返回 error；调用方负责 graceful degradation（记录日志但不阻断请求）。
func (s *SQLiteStore) SaveArchive(block ArchiveBlock) error {
	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("开启事务失败: %w", err)
	}
	defer func() { _ = tx.Rollback() }() // Commit 后 Rollback 为 no-op

	messagesJSON, err := json.Marshal(block.Messages)
	if err != nil {
		return fmt.Errorf("序列化消息失败: %w", err)
	}
	// 调用方提供的 hash 不可信；始终从实际正文重新计算。
	block.ContentHash, err = archiveContentHash(block.Messages)
	if err != nil {
		return err
	}

	result, err := tx.Exec(
		`INSERT INTO archive_blocks
		 (id, session_id, block_range_start, block_range_end,
		  message_count, estimated_tokens, messages_json, summary_text, created_at, content_hash)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), ?)
		 ON CONFLICT DO NOTHING`,
		block.ID, block.SessionID, block.BlockRangeStart, block.BlockRangeEnd,
		block.MessageCount, block.EstimatedTokens,
		string(messagesJSON), block.SummaryText, block.ContentHash,
	)
	if err != nil {
		return fmt.Errorf("插入 archive_block 失败: %w", err)
	}
	inserted, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("读取 archive_block 插入结果失败: %w", err)
	}
	if inserted == 0 {
		return tx.Commit()
	}

	// 逐条插入关键词（触发器自动同步 FTS5）
	for _, kw := range block.Keywords {
		_, err = tx.Exec(
			`INSERT INTO archive_keywords (block_id, keyword, source) VALUES (?, ?, ?)`,
			block.ID, kw.Word, kw.Source,
		)
		if err != nil {
			return fmt.Errorf("插入关键词失败: %w", err)
		}
	}

	return tx.Commit()
}

// PersistState 将 key-value 状态持久化到 frozen_state 表（Phase 4, D-08）。
// 使用 INSERT OR REPLACE 实现 upsert 语义。
// key 格式: "frozen:{threadID}" / "sawtooth:{threadID}" / "decay:{threadID}" (Phase B)。
func (s *SQLiteStore) PersistState(key, value string) error {
	_, err := s.db.Exec(
		`INSERT OR REPLACE INTO frozen_state (key, value, updated_at)
		 VALUES (?, ?, datetime('now'))`,
		key, value,
	)
	if err != nil {
		return fmt.Errorf("持久化状态失败 key=%s: %w", key, err)
	}
	return nil
}

// LoadState 从 frozen_state 表加载 key 对应的 JSON 值（Phase 4, D-08）。
// 返回状态字符串和 true；未找到则返回 "", false。
func (s *SQLiteStore) LoadState(key string) (string, bool) {
	var value string
	err := s.db.QueryRow(
		`SELECT value FROM frozen_state WHERE key = ?`,
		key,
	).Scan(&value)
	if err != nil {
		return "", false
	}
	return value, true
}

// DeleteState 删除持久化状态键，用于移除已确认失效或损坏的 frozen 快照。
func (s *SQLiteStore) DeleteState(key string) error {
	if _, err := s.db.Exec(`DELETE FROM frozen_state WHERE key = ?`, key); err != nil {
		return fmt.Errorf("删除状态失败 key=%s: %w", key, err)
	}
	return nil
}

// SearchArchives 通过 FTS5 MATCH + bm25() 搜索匹配的 archive 摘要（D-07, D-11）。
// query 应当已经过 buildFTS5Query 清洗——所有 token 已双引号包裹（phrase literal），
// 参数化查询 ? 防止 SQL 注入，双引号包裹防止 FTS5 语法注入。
// 排序：FTS5 索引的是单 token 关键词文档（每行一个关键词，dl=avgdl=1），
// 每匹配行 bm25() = -IDF；GROUP BY block 后 SUM = -Σ IDF(匹配词)，
// 升序即"匹配词越多、词越稀有越靠前"，恢复查询级 BM25 语义。
// bm25() 是 FTS5 辅助函数，只能在全文查询的逐匹配行上下文求值
// （聚合上下文报 "unable to use function bm25"），故经 MATERIALIZED CTE
// 物化为普通列 rank 后再在外层 SUM 聚合——MATERIALIZED 阻止查询计划器
// 将 CTE 扁平化回聚合上下文。
// GROUP BY 主键 a.id 时其余 a.* 列函数依赖于主键（组内值一致），
// SQLite 允许 bare columns，安全。
func (s *SQLiteStore) SearchArchives(query string, limit int) ([]ArchiveSummary, error) {
	if limit < 1 {
		return nil, nil
	}
	if limit > 3 {
		limit = 3
	}
	rows, err := s.db.Query(
		`WITH matched AS MATERIALIZED (
		     SELECT rowid, keyword, bm25(archive_keywords_fts) AS rank
		     FROM archive_keywords_fts
		     WHERE archive_keywords_fts MATCH ?
		 )
		 SELECT a.id, a.session_id, COALESCE(a.content_hash, ''), a.block_range_start, a.block_range_end,
		        a.message_count, a.estimated_tokens, a.summary_text, a.messages_json, a.created_at,
		        GROUP_CONCAT(DISTINCT fts.keyword), COUNT(DISTINCT fts.keyword), SUM(fts.rank)
		 FROM archive_blocks a
		 JOIN archive_keywords k ON k.block_id = a.id
		 JOIN matched fts ON fts.rowid = k.id
		 GROUP BY a.id
		 ORDER BY COUNT(DISTINCT fts.keyword) DESC, SUM(fts.rank) ASC, a.created_at DESC, a.id ASC
		 LIMIT ?`,
		query, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("搜索 archive 失败: %w", err)
	}
	defer rows.Close()

	var results []ArchiveSummary
	for rows.Next() {
		var summary ArchiveSummary
		var matchedTerms string
		if err := rows.Scan(
			&summary.ID, &summary.SessionID, &summary.ContentHash,
			&summary.BlockRangeStart, &summary.BlockRangeEnd,
			&summary.MessageCount, &summary.EstimatedTokens,
			&summary.SummaryText, &summary.MessagesJSON, &summary.CreatedAt,
			&matchedTerms, &summary.MatchedTermCount, &summary.Rank,
		); err != nil {
			return nil, fmt.Errorf("扫描搜索结果失败: %w", err)
		}
		if matchedTerms != "" {
			summary.MatchedTerms = strings.Split(matchedTerms, ",")
			sort.Strings(summary.MatchedTerms)
		}
		results = append(results, summary)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("遍历搜索结果失败: %w", err)
	}

	return results, nil
}

// Close 执行 WAL checkpoint (TRUNCATE) 后关闭数据库连接（D-15）。
func (s *SQLiteStore) Close() error {
	var busy, logFrames, checkpointed int
	checkpointErr := s.db.QueryRow("PRAGMA wal_checkpoint(TRUNCATE)").Scan(&busy, &logFrames, &checkpointed)
	if checkpointErr == nil && (busy != 0 || logFrames != checkpointed) {
		checkpointErr = fmt.Errorf("sqlite WAL checkpoint 未完成: busy=%d log=%d checkpointed=%d", busy, logFrames, checkpointed)
	}
	closeErr := s.db.Close()
	if checkpointErr == nil && closeErr == nil {
		// 只有 checkpoint 明确完成后才清理当前数据库专属的伴生文件。
		for _, suffix := range []string{"-wal", "-shm"} {
			if err := os.Remove(s.path + suffix); err != nil && !os.IsNotExist(err) {
				closeErr = errors.Join(closeErr, fmt.Errorf("清理 sqlite 伴生文件 %s: %w", s.path+suffix, err))
			}
		}
	}
	return errors.Join(checkpointErr, closeErr)
}

// ── SQLite 损坏恢复辅助函数 ──

// isCorruptionError 检测错误消息中是否包含 SQLite 损坏关键字。
// 覆盖 SQLITE_CORRUPT、SQLITE_NOTADB 及其标准人类可读描述。
func isCorruptionError(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "database disk image is malformed") ||
		strings.Contains(msg, "file is not a database") ||
		strings.Contains(msg, "SQLITE_CORRUPT") ||
		strings.Contains(msg, "SQLITE_NOTADB")
}

// removeDBFiles 删除数据库主文件及其 WAL/SHM 伴生文件。
// 忽略单个文件删除失败（文件可能不存在），确保尽力清理。
func removeDBFiles(path string) {
	for _, suffix := range []string{"", "-wal", "-shm"} {
		_ = os.Remove(path + suffix)
	}
}

// removeStaleWALFiles 当主 DB 文件不存在时，清理进程闪退残留的 -wal/-shm。
// Windows 上进程异常退出后，残留的伴生文件可能使 sqlite 将空 WAL 误判为有效数据库。
func removeStaleWALFiles(path string) {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		for _, suffix := range []string{"-wal", "-shm"} {
			_ = os.Remove(path + suffix)
		}
	}
}

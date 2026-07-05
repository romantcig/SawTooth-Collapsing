package proxy

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	_ "modernc.org/sqlite" // 纯 Go SQLite 驱动，通过 init() 注册 database/sql
)

// SQLiteStore 管理 archive + decay state 的 SQLite 数据库。
type SQLiteStore struct {
	db *sql.DB
}

// ArchiveBlock 折叠存档块——包含被折叠的原始消息及元数据。
type ArchiveBlock struct {
	ID               string        `json:"id"`
	SessionID        string        `json:"session_id"`
	BlockRangeStart  int           `json:"block_range_start"`
	BlockRangeEnd    int           `json:"block_range_end"`
	MessageCount     int           `json:"message_count"`
	EstimatedTokens  int           `json:"estimated_tokens"`
	Messages         []Message     `json:"messages"`
	SummaryText      string        `json:"summary_text"`
	CreatedAt        string        `json:"created_at"`
	Keywords         []KeywordEntry `json:"keywords"`
}

// ArchiveSummary SearchArchives 返回的轻量结果——不含 Messages 字段。
type ArchiveSummary struct {
	ID              string `json:"id"`
	SessionID       string `json:"session_id"`
	BlockRangeStart int    `json:"block_range_start"`
	BlockRangeEnd   int    `json:"block_range_end"`
	MessageCount    int    `json:"message_count"`
	EstimatedTokens int    `json:"estimated_tokens"`
	SummaryText     string `json:"summary_text"`
	CreatedAt       string `json:"created_at"`
}

// KeywordEntry 关键词条目——关联到 archive block 的关键词及其来源。
type KeywordEntry struct {
	Word   string `json:"word"`
	Source string `json:"source"` // 取值: "file_path"、"tool_name"、"user_message"
}

// NewSQLiteStore 打开或创建指定路径的 SQLite 数据库。
// 设置 PRAGMA、创建 schema，对每一步错误进行 wrap 返回。
func NewSQLiteStore(path string) (*SQLiteStore, error) {
	// 确保父目录存在（纯 Go SQLite 驱动不会自动创建）
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("创建数据目录失败 %s: %w", dir, err)
	}

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

	s := &SQLiteStore{db: db}
	if err := s.createSchema(); err != nil {
		db.Close()
		return nil, fmt.Errorf("建表失败: %w", err)
	}

	return s, nil
}

// createSchema 创建全部表、索引、FTS5 虚拟表及触发器。
// 所有 DDL 使用 IF NOT EXISTS，幂等执行。
func (s *SQLiteStore) createSchema() error {
	schema := []string{
		// 存档块主表
		`CREATE TABLE IF NOT EXISTS archive_blocks (
			id               TEXT PRIMARY KEY,
			session_id       TEXT NOT NULL,
			block_range_start INTEGER NOT NULL,
			block_range_end   INTEGER NOT NULL,
			message_count    INTEGER NOT NULL,
			estimated_tokens  INTEGER NOT NULL,
			messages_json    TEXT NOT NULL,
			summary_text     TEXT NOT NULL,
			created_at       TEXT NOT NULL DEFAULT (datetime('now'))
		)`,
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

	_, err = tx.Exec(
		`INSERT INTO archive_blocks
		 (id, session_id, block_range_start, block_range_end,
		  message_count, estimated_tokens, messages_json, summary_text, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))`,
		block.ID, block.SessionID, block.BlockRangeStart, block.BlockRangeEnd,
		block.MessageCount, block.EstimatedTokens,
		string(messagesJSON), block.SummaryText,
	)
	if err != nil {
		return fmt.Errorf("插入 archive_block 失败: %w", err)
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

// SearchArchives 通过 FTS5 MATCH + bm25() 搜索匹配的 archive 摘要（D-07, D-11）。
// 对 query 做单引号转义防止注入（T-03-01 缓解）。
func (s *SQLiteStore) SearchArchives(query string, limit int) ([]ArchiveSummary, error) {
	// T-03-01: 单引号转义——防止 FTS5 查询注入
	safeQuery := strings.ReplaceAll(query, "'", "''")

	rows, err := s.db.Query(
		`SELECT DISTINCT a.id, a.session_id, a.block_range_start, a.block_range_end,
		        a.message_count, a.estimated_tokens, a.summary_text, a.created_at
		 FROM archive_blocks a
		 JOIN archive_keywords k ON k.block_id = a.id
		 JOIN archive_keywords_fts fts ON fts.rowid = k.id
		 WHERE archive_keywords_fts MATCH ?
		 ORDER BY bm25(archive_keywords_fts)
		 LIMIT ?`,
		safeQuery, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("搜索 archive 失败: %w", err)
	}
	defer rows.Close()

	var results []ArchiveSummary
	for rows.Next() {
		var summary ArchiveSummary
		if err := rows.Scan(
			&summary.ID, &summary.SessionID,
			&summary.BlockRangeStart, &summary.BlockRangeEnd,
			&summary.MessageCount, &summary.EstimatedTokens,
			&summary.SummaryText, &summary.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("扫描搜索结果失败: %w", err)
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
	// 确保 WAL 完整落盘（忽略 error，因为 db 可能已经关闭）
	_, _ = s.db.Exec("PRAGMA wal_checkpoint(TRUNCATE)")
	return s.db.Close()
}

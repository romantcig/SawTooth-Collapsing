package proxy

import (
	"crypto/sha256"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

func TestArchiveReadOnlyAudit(t *testing.T) {
	productionPath := os.Getenv("SAWTOOTH_AUDIT_DB")
	if productionPath == "" {
		t.Skip("设置 SAWTOOTH_AUDIT_DB 后执行生产数据库只读审计")
	}
	abs, err := filepath.Abs(productionPath)
	if err != nil {
		t.Fatalf("解析审计路径: %v", err)
	}
	before := snapshotFileStates(t, abs)
	snapshotPath := filepath.Join(t.TempDir(), filepath.Base(abs))
	for _, suffix := range []string{"", "-wal", "-shm"} {
		source := abs + suffix
		if _, err := os.Stat(source); os.IsNotExist(err) {
			continue
		} else if err != nil {
			t.Fatalf("读取生产快照源 %s: %v", source, err)
		}
		copyFile(t, source, snapshotPath+suffix)
	}
	t.Cleanup(func() {
		after := snapshotFileStates(t, abs)
		if fmt.Sprint(after) != fmt.Sprint(before) {
			t.Errorf("生产 DB/WAL/SHM 在快照审计期间发生变化\nbefore=%v\nafter=%v", before, after)
		}
	})

	uriPath := strings.Replace(filepath.ToSlash(snapshotPath), ":", "%3A", 1)
	dsn := "file:///" + uriPath + "?mode=ro"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("以 mode=ro 打开审计数据库: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if err := db.Ping(); err != nil {
		t.Fatalf("只读连接 Ping: %v", err)
	}

	var total, sessions, exactDuplicateGroups, dominatedPairs, expectedRetained int
	queries := []struct {
		name string
		dst  *int
		sql  string
	}{
		{"total", &total, `SELECT COUNT(*) FROM archive_blocks`},
		{"sessions", &sessions, `SELECT COUNT(DISTINCT session_id) FROM archive_blocks`},
		{"exact_duplicate_groups", &exactDuplicateGroups, `SELECT COUNT(*) FROM (SELECT session_id, block_range_start, block_range_end, messages_json, summary_text FROM archive_blocks GROUP BY 1,2,3,4,5 HAVING COUNT(*) > 1)`},
		{"dominated_pairs", &dominatedPairs, `SELECT COUNT(*) FROM archive_blocks a JOIN archive_blocks b ON a.session_id=b.session_id AND a.id<>b.id WHERE MAX(a.block_range_start,b.block_range_start) <= MIN(a.block_range_end,b.block_range_end) AND (MIN(a.block_range_end,b.block_range_end)-MAX(a.block_range_start,b.block_range_start)+1)*5 >= MIN(a.block_range_end-a.block_range_start+1,b.block_range_end-b.block_range_start+1)*4`},
		{"expected_retained", &expectedRetained, `SELECT COUNT(*) FROM archive_blocks a WHERE NOT EXISTS (SELECT 1 FROM archive_blocks b WHERE a.session_id=b.session_id AND a.id<>b.id AND MAX(a.block_range_start,b.block_range_start) <= MIN(a.block_range_end,b.block_range_end) AND (MIN(a.block_range_end,b.block_range_end)-MAX(a.block_range_start,b.block_range_start)+1)*5 >= MIN(a.block_range_end-a.block_range_start+1,b.block_range_end-b.block_range_start+1)*4 AND (b.block_range_end-b.block_range_start) > (a.block_range_end-a.block_range_start))`},
	}
	for _, query := range queries {
		if err := db.QueryRow(query.sql).Scan(query.dst); err != nil {
			t.Fatalf("只读查询 %s: %v", query.name, err)
		}
	}
	t.Logf("AUDIT_DSN=%s", dsn)
	t.Logf("AUDIT_SNAPSHOT_SOURCE=%s", abs)
	t.Logf("AUDIT total_blocks=%d distinct_sessions=%d exact_duplicate_groups=%d dominated_pairs=%d expected_retained=%d", total, sessions, exactDuplicateGroups, dominatedPairs, expectedRetained)
	if expectedRetained > total {
		t.Fatal(fmt.Sprintf("预计保留数 %d 不应大于总块数 %d", expectedRetained, total))
	}
}

type auditFileState struct {
	Path    string
	Exists  bool
	Size    int64
	ModTime time.Time
	SHA256  [sha256.Size]byte
}

func snapshotFileStates(t *testing.T, dbPath string) []auditFileState {
	t.Helper()
	states := make([]auditFileState, 0, 3)
	for _, suffix := range []string{"", "-wal", "-shm"} {
		path := dbPath + suffix
		state := auditFileState{Path: path}
		info, err := os.Stat(path)
		if os.IsNotExist(err) {
			states = append(states, state)
			continue
		}
		if err != nil {
			t.Fatalf("stat %s: %v", path, err)
		}
		file, err := os.Open(path)
		if err != nil {
			t.Fatalf("只读打开 %s: %v", path, err)
		}
		hash := sha256.New()
		if _, err := io.Copy(hash, file); err != nil {
			file.Close()
			t.Fatalf("计算 %s SHA-256: %v", path, err)
		}
		if err := file.Close(); err != nil {
			t.Fatalf("关闭 %s: %v", path, err)
		}
		state.Exists = true
		state.Size = info.Size()
		state.ModTime = info.ModTime()
		copy(state.SHA256[:], hash.Sum(nil))
		states = append(states, state)
	}
	return states
}

func copyFile(t *testing.T, source, destination string) {
	t.Helper()
	in, err := os.Open(source)
	if err != nil {
		t.Fatalf("只读打开快照源 %s: %v", source, err)
	}
	defer in.Close()
	out, err := os.OpenFile(destination, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		t.Fatalf("创建临时快照 %s: %v", destination, err)
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		t.Fatalf("复制临时快照 %s: %v", source, err)
	}
	if err := out.Close(); err != nil {
		t.Fatalf("关闭临时快照 %s: %v", destination, err)
	}
}

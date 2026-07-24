package proxy

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"sync"
	"testing"
)

func TestDebugRunIDIsFixedPerServerAndSeparatesRestarts(t *testing.T) {
	dataDir := t.TempDir()
	first := NewServer(Config{Debug: DebugConfig{DataDir: dataDir}})
	second := NewServer(Config{Debug: DebugConfig{DataDir: dataDir}})
	hexPattern := regexp.MustCompile(`^[0-9a-f]{16}$`)
	if !hexPattern.MatchString(first.debugRunID) {
		t.Fatalf("first run ID=%q，不是 16 位小写 hex", first.debugRunID)
	}
	if !hexPattern.MatchString(second.debugRunID) {
		t.Fatalf("second run ID=%q，不是 16 位小写 hex", second.debugRunID)
	}
	if first.debugRunID == second.debugRunID {
		t.Fatalf("两个 Server 复用了 run ID: %q", first.debugRunID)
	}

	firstMeta := first.nextRequestMeta("same-session")
	secondMeta := first.nextRequestMeta("same-session")
	if firstMeta.RunID != first.debugRunID || secondMeta.RunID != first.debugRunID {
		t.Fatalf("同一 Server 请求未固定 run ID: first=%q second=%q server=%q", firstMeta.RunID, secondMeta.RunID, first.debugRunID)
	}
	if firstMeta.SessionHash != stableSessionHash("same-session") || secondMeta.SessionHash != firstMeta.SessionHash {
		t.Fatalf("request meta 未固定稳定 session hash: first=%q second=%q", firstMeta.SessionHash, secondMeta.SessionHash)
	}
}

func TestDebugRequestPathUsesStrictHierarchyAndStageEnum(t *testing.T) {
	dataDir := t.TempDir()
	server := NewServer(Config{Debug: DebugConfig{DataDir: dataDir}})
	meta := server.nextRequestMeta("session-path-secret")
	path, err := server.debugLayout.RequestPath(meta, debugArtifactRawBody)
	if err != nil {
		t.Fatalf("合法 debug path: %v", err)
	}
	want := filepath.Join(dataDir, "debug", meta.SessionHash, meta.RunID, strconv.FormatUint(meta.ID, 10), "raw.json")
	want, _ = filepath.Abs(want)
	if path != want {
		t.Fatalf("debug path=%q，want %q", path, want)
	}

	for _, stage := range []debugArtifactStage{"../escape", `raw/escape`, `raw\\escape`, "unknown"} {
		if _, err := server.debugLayout.RequestPath(meta, stage); err == nil {
			t.Fatalf("非法 stage %q 未失败关闭", stage)
		}
	}
	badMeta := *meta
	badMeta.SessionHash = "../../escape"
	if _, err := server.debugLayout.RequestPath(&badMeta, debugArtifactRawBody); err == nil {
		t.Fatal("非法 session hash 未失败关闭")
	}
}

func TestDebugRunIDSeparatesSameRequestAcrossServers(t *testing.T) {
	dataDir := t.TempDir()
	first := NewServer(Config{Debug: DebugConfig{DataDir: dataDir}})
	second := NewServer(Config{Debug: DebugConfig{DataDir: dataDir}})
	firstMeta := first.nextRequestMeta("restart-session")
	secondMeta := second.nextRequestMeta("restart-session")
	firstPath, err := first.debugLayout.RequestPath(firstMeta, debugArtifactPressure)
	if err != nil {
		t.Fatal(err)
	}
	secondPath, err := second.debugLayout.RequestPath(secondMeta, debugArtifactPressure)
	if err != nil {
		t.Fatal(err)
	}
	if firstMeta.ID != secondMeta.ID || firstPath == secondPath {
		t.Fatalf("重启隔离失败: request=%d/%d path=%q/%q", firstMeta.ID, secondMeta.ID, firstPath, secondPath)
	}
}

func TestDebugSessionMetadataCreatedOnceAndValidated(t *testing.T) {
	dataDir := t.TempDir()
	server := NewServer(Config{Debug: DebugConfig{DataDir: dataDir}})
	const sessionID = "session-metadata-secret"

	const requests = 24
	var wg sync.WaitGroup
	errs := make(chan error, requests)
	for index := 0; index < requests; index++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			meta := server.nextRequestMeta(sessionID)
			errs <- server.debugLayout.Write(meta, debugArtifactPressure, []byte(`{"ok":true}`))
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("并发 debug 写入失败: %v", err)
		}
	}

	sessionDir := filepath.Join(dataDir, "debug", stableSessionHash(sessionID))
	entries, err := os.ReadDir(sessionDir)
	if err != nil {
		t.Fatal(err)
	}
	metadataCount := 0
	for _, entry := range entries {
		if entry.Name() == "session.json" {
			metadataCount++
		}
	}
	if metadataCount != 1 {
		t.Fatalf("session metadata 数量=%d，want 1: %+v", metadataCount, entries)
	}
	metadataPath := filepath.Join(sessionDir, "session.json")
	original, err := os.ReadFile(metadataPath)
	if err != nil {
		t.Fatal(err)
	}
	meta := server.nextRequestMeta(sessionID)
	if err := server.debugLayout.EnsureSessionMetadata(meta); err != nil {
		t.Fatalf("相同 session metadata 验证失败: %v", err)
	}
	after, _ := os.ReadFile(metadataPath)
	if !bytes.Equal(after, original) {
		t.Fatal("重复 metadata 验证改写了旧字节")
	}
}

func TestDebugDuplicateStageFailsWithoutOverwrite(t *testing.T) {
	server := NewServer(Config{Debug: DebugConfig{DataDir: t.TempDir()}})
	meta := server.nextRequestMeta("duplicate-stage")
	if err := server.debugLayout.Write(meta, debugArtifactUsage, []byte("first")); err != nil {
		t.Fatalf("首次写入: %v", err)
	}
	path, err := server.debugLayout.RequestPath(meta, debugArtifactUsage)
	if err != nil {
		t.Fatal(err)
	}
	if err := server.debugLayout.Write(meta, debugArtifactUsage, []byte("second")); err == nil {
		t.Fatal("重复 stage 写入未报错")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "first" {
		t.Fatalf("重复写入覆盖旧证据: %q", data)
	}
}

func TestDebugLayoutUsesExclusive0600Writer(t *testing.T) {
	server := NewServer(Config{Debug: DebugConfig{DataDir: t.TempDir()}})
	var flags []int
	var perms []os.FileMode
	server.debugLayout.openFile = func(name string, flag int, perm os.FileMode) (debugWriteCloser, error) {
		flags = append(flags, flag)
		perms = append(perms, perm)
		return os.OpenFile(name, flag, perm)
	}
	meta := server.nextRequestMeta("permission-contract")
	if err := server.debugLayout.Write(meta, debugArtifactPressure, []byte("{}")); err != nil {
		t.Fatalf("写入 debug stage: %v", err)
	}
	if len(flags) != 2 || len(perms) != 2 {
		t.Fatalf("metadata/stage opener 调用数=%d/%d，want 2", len(flags), len(perms))
	}
	for index := range flags {
		if flags[index]&(os.O_CREATE|os.O_EXCL|os.O_WRONLY) != (os.O_CREATE | os.O_EXCL | os.O_WRONLY) {
			t.Fatalf("第 %d 次 opener flags=%#x，缺少 O_CREATE|O_EXCL|O_WRONLY", index, flags[index])
		}
		if perms[index] != 0600 {
			t.Fatalf("第 %d 次 opener perm=%#o，want 0600", index, perms[index])
		}
	}
}

func TestDebugLayoutRandomFailureIsFailClosed(t *testing.T) {
	if _, err := newDebugLayout(t.TempDir(), failingDebugRandom{}); err == nil {
		t.Fatal("随机源失败时仍创建 DebugLayout")
	}
	server, err := newServerWithDebugRandom(Config{Debug: DebugConfig{DataDir: t.TempDir()}}, failingDebugRandom{})
	if err == nil {
		t.Fatal("随机源失败时 NewServer 测试入口未返回错误")
	}
	if server.debugLayout != nil || server.debugRunID != "" {
		t.Fatalf("随机源失败后保留可预测 layout: layout=%v run=%q", server.debugLayout, server.debugRunID)
	}
}

type failingDebugRandom struct{}

func (failingDebugRandom) Read([]byte) (int, error) {
	return 0, errors.New("random unavailable")
}

var _ io.Reader = failingDebugRandom{}

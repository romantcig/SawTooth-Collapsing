package proxy

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
)

func TestSessionLogRoutesProcessAndSession(t *testing.T) {
	dataDir := tempDirRetryCleanup(t)
	logger := slog.New(NewSessionLogHandler(dataDir, slog.LevelDebug, nil))
	secret := "session-secret-must-not-be-rendered"

	logger.Info("启动代理", "component", "proxy")
	logger.With("request_session_id", secret, "request_id", uint64(27)).Debug("request_in", "status", 200)
	logger.With("request_session_id", secret, "request_id", uint64(27)).Warn(eventKeyUpstreamNon2xx, "status", 502)

	process := readSessionLogFile(t, filepath.Join(dataDir, "logs", "process.log"))
	sessionPath := filepath.Join(dataDir, "logs", stableSessionHash(secret)+".log")
	session := readSessionLogFile(t, sessionPath)

	if !strings.Contains(process, "启动代理 component=proxy") {
		t.Fatalf("process.log 缺少无 session 事件: %q", process)
	}
	if strings.Contains(process, "request_in") || strings.Contains(process, secret) {
		t.Fatalf("process.log 错误接收 session 事件或身份: %q", process)
	}
	if strings.Contains(session, secret) || strings.Contains(session, "request_session_id") {
		t.Fatalf("session.log 泄漏完整 session 身份: %q", session)
	}
	if !strings.Contains(session, "[DEBUG] #27 request_in status=200") {
		t.Fatalf("session.log 缺少紧凑 request 关联: %q", session)
	}
	if !strings.Contains(session, "[WARN] #27 upstream_non2xx status=502") {
		t.Fatalf("session.log Warn 标签格式错误: %q", session)
	}
	if strings.Contains(session, "[INFO]") || strings.Contains(session, "\033[") {
		t.Fatalf("session.log 含多余 Info 标签或 ANSI: %q", session)
	}
	linePattern := regexp.MustCompile(`(?m)^\d{2}:\d{2}:\d{2} (?:\[(?:WARN|DEBUG)\] )?.+$`)
	if !linePattern.MatchString(session) {
		t.Fatalf("session.log 时间/级别格式不是 HH:MM:SS: %q", session)
	}
}

func TestSessionLogHashIsStableShortLowerHex(t *testing.T) {
	first := stableSessionHash("same-session")
	if first != stableSessionHash("same-session") {
		t.Fatal("相同 session hash 不稳定")
	}
	if first == stableSessionHash("other-session") {
		t.Fatal("不同 session hash 未区分")
	}
	if len(first) != 16 {
		t.Fatalf("session hash 长度=%d，want 16: %q", len(first), first)
	}
	if strings.Trim(first, "0123456789abcdef") != "" {
		t.Fatalf("session hash 含非小写 hex: %q", first)
	}
}

func TestSessionLogAppendAndConcurrentLines(t *testing.T) {
	dataDir := tempDirRetryCleanup(t)
	secret := "concurrent-session"
	first := slog.New(NewSessionLogHandler(dataDir, slog.LevelInfo, nil))
	second := slog.New(NewSessionLogHandler(dataDir, slog.LevelInfo, nil))
	first.Info("重启前")
	second.Info("重启后")

	const workers = 8
	const perWorker = 20
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		worker := worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			logger := slog.New(NewSessionLogHandler(dataDir, slog.LevelInfo, nil)).With(
				"request_session_id", secret,
				"request_id", uint64(worker+1),
			)
			for index := 0; index < perWorker; index++ {
				logger.Info(fmt.Sprintf("并发-%d-%d", worker, index))
			}
		}()
	}
	wg.Wait()

	process := readSessionLogFile(t, filepath.Join(dataDir, "logs", "process.log"))
	if !strings.Contains(process, "重启前") || !strings.Contains(process, "重启后") {
		t.Fatalf("追加写入被截断: %q", process)
	}
	session := readSessionLogFile(t, filepath.Join(dataDir, "logs", stableSessionHash(secret)+".log"))
	lines := strings.Split(strings.TrimSpace(session), "\n")
	if len(lines) != workers*perWorker {
		t.Fatalf("并发 session 日志行数=%d，want %d: %q", len(lines), workers*perWorker, session)
	}
	linePattern := regexp.MustCompile(`^\d{2}:\d{2}:\d{2} #\d+ 并发-\d+-\d+$`)
	for _, line := range lines {
		if !linePattern.MatchString(line) {
			t.Fatalf("发现交错或半行: %q", line)
		}
	}
}

func TestCombinedLogHandlerPreservesTerminalContract(t *testing.T) {
	var terminal strings.Builder
	terminalHandler := NewLogHandler(&terminal, slog.LevelInfo)
	dataDir := tempDirRetryCleanup(t)
	fileHandler := NewSessionLogHandler(dataDir, slog.LevelInfo, nil)
	logger := slog.New(NewCombinedLogHandler(terminalHandler, fileHandler))
	logger.InfoContext(context.Background(), "组合事件")

	if !strings.Contains(terminal.String(), "[INFO]  组合事件") {
		t.Fatalf("组合 handler 破坏终端格式: %q", terminal.String())
	}
	filePath := filepath.Join(dataDir, "logs", "process.log")
	file := readSessionLogFile(t, filePath)
	if strings.Contains(file, "[INFO]") || strings.Contains(file, "\033[") {
		t.Fatalf("文件 handler 不应复制终端级别/ANSI: %q", file)
	}
	if !strings.Contains(file, "组合事件") {
		t.Fatalf("组合 handler 未写文件日志: %q", file)
	}
}

func readSessionLogFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("读取日志 %s: %v", path, err)
	}
	return string(data)
}

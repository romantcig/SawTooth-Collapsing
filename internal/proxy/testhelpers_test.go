package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

// countBlockType 解析消息 Content 并统计指定 Type 的 block 数量。
func countBlockType(t *testing.T, msg Message, blockType string) int {
	t.Helper()
	blocks, _ := parseContent(msg.Content)
	n := 0
	for _, b := range blocks {
		if b.Type == blockType {
			n++
		}
	}
	return n
}

// allText 解析消息 Content 并拼接所有 text block 的文本。
func allText(t *testing.T, msg Message) string {
	t.Helper()
	blocks, _ := parseContent(msg.Content)
	var sb strings.Builder
	for _, b := range blocks {
		if b.Type == "text" {
			sb.WriteString(b.Text)
		}
	}
	return sb.String()
}

// lockedBuffer 让终端 writer 在 race 检测下可被测试安全读取；
// LogHandler 自身已保证整行写入，这里只额外保护读侧。
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func (b *lockedBuffer) lines() []string {
	text := strings.TrimSuffix(b.String(), "\n")
	if text == "" {
		return nil
	}
	return strings.Split(text, "\n")
}

type healthTestReporter struct {
	mu          sync.Mutex
	transitions []HealthTransition
}

func (r *healthTestReporter) ReportHealthTransition(transition HealthTransition) {
	r.mu.Lock()
	r.transitions = append(r.transitions, transition)
	r.mu.Unlock()
}

func (r *healthTestReporter) snapshot() []HealthTransition {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]HealthTransition(nil), r.transitions...)
}

func mustMarshalJSON(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal JSON: %v", err)
	}
	return data
}

// fakeOutcomeSink 用可注入的整行 seam 替代真实文件，避免依赖平台权限行为。
type fakeOutcomeSink struct {
	mu           sync.Mutex
	sessionLines map[string][]string
	processLines []string
	sessionErr   error
	processErr   error
	processHook  func()
	sessionHook  func()
}

func (s *fakeOutcomeSink) WriteSessionLine(shortHash, line string) error {
	s.mu.Lock()
	hook := s.processHook
	if shortHash != "" {
		hook = s.sessionHook
	}
	sessionErr := s.sessionErr
	processErr := s.processErr
	s.mu.Unlock()
	if hook != nil {
		hook()
	}
	if shortHash == "" {
		if processErr != nil {
			return processErr
		}
		s.mu.Lock()
		s.processLines = append(s.processLines, line)
		s.mu.Unlock()
		return nil
	}
	if sessionErr != nil {
		return sessionErr
	}
	s.mu.Lock()
	if s.sessionLines == nil {
		s.sessionLines = map[string][]string{}
	}
	s.sessionLines[shortHash] = append(s.sessionLines[shortHash], line)
	s.mu.Unlock()
	return nil
}

func (s *fakeOutcomeSink) process() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.processLines...)
}

func (s *fakeOutcomeSink) session(shortHash string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.sessionLines[shortHash]...)
}

func (s *fakeOutcomeSink) setProcessErr(err error) {
	s.mu.Lock()
	s.processErr = err
	s.mu.Unlock()
}

func (s *fakeOutcomeSink) setSessionErr(err error) {
	s.mu.Lock()
	s.sessionErr = err
	s.mu.Unlock()
}

func (s *fakeOutcomeSink) setProcessHook(hook func()) {
	s.mu.Lock()
	s.processHook = hook
	s.mu.Unlock()
}

func (s *fakeOutcomeSink) setSessionHook(hook func()) {
	s.mu.Lock()
	s.sessionHook = hook
	s.mu.Unlock()
}

func persistentReminder(key, value string) string {
	return "<system-reminder>\nAs you answer, use this context:\n# " + key + "\n" + value + "\n</system-reminder>"
}

func persistentContextMessage(key, value string) Message {
	content, _ := json.Marshal([]map[string]any{{"type": "text", "text": persistentReminder(key, value)}})
	return Message{Role: "user", Content: content}
}

func persistentContextCount(messages []Message) int {
	total := 0
	for _, message := range messages {
		if ExtractPersistentUserContext([]Message{message}) != nil {
			total++
		}
	}
	return total
}

func setServerDebugConfigForTest(t *testing.T, server *Server, cfg DebugConfig) {
	t.Helper()
	layout, err := newDebugLayout(cfg.DataDir, nil)
	if err != nil {
		t.Fatalf("初始化测试 DebugLayout: %v", err)
	}
	server.Config.Debug = cfg
	server.debugLayout = layout
	server.debugRunID = layout.RunID()
}

func readDebugFactFiles(t *testing.T, dataDir, sessionID string) [][]byte {
	t.Helper()
	dir, ok := safeDebugSessionDir(dataDir, sessionID)
	if !ok {
		t.Fatal("debug dir invalid")
	}
	factNames := map[string]bool{
		"raw_meta.json":       true,
		"forwarded_meta.json": true,
		"pressure.json":       true,
		"usage.json":          true,
	}
	var paths []string
	if err := filepath.Walk(dir, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if !info.IsDir() && factNames[info.Name()] {
			paths = append(paths, path)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	sort.Strings(paths)
	files := make([][]byte, 0, len(paths))
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, data)
	}
	return files
}

func debugFactsByStage(t *testing.T, dataDir, sessionID string) map[debugStage]map[string]any {
	t.Helper()
	result := make(map[debugStage]map[string]any)
	for _, data := range readDebugFactFiles(t, dataDir, sessionID) {
		var fact map[string]any
		if err := json.Unmarshal(data, &fact); err != nil {
			t.Fatalf("解析 facts: %v: %s", err, data)
		}
		stage, _ := fact["stage"].(string)
		if _, exists := result[debugStage(stage)]; exists {
			t.Fatalf("stage %q 重复: %v", stage, result)
		}
		result[debugStage(stage)] = fact
	}
	return result
}

// logTestTime 测试用固定时间戳——对应输出 15:04:05。
var logTestTime = time.Date(2026, 7, 6, 15, 4, 5, 0, time.Local)

// countingHandler 统计任何 file-capable logger 是否被健康路径调用。
type countingHandler struct {
	calls int
}

func (h *countingHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *countingHandler) Handle(context.Context, slog.Record) error {
	h.calls++
	return nil
}

func (h *countingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }

func (h *countingHandler) WithGroup(string) slog.Handler { return h }

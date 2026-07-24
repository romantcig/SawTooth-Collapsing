package proxy

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

const (
	requestSessionIDAttr = "request_session_id"
	sessionIDAttr        = "session_id"
	sourceSessionIDAttr  = "source_session_id"
)

// stableSessionHash 是日志和 Debug 共用的会话路由键。
// 只保留 SHA-256 的前 64 bit，既保持稳定关联，又不把原始 session 身份写入路径。
func stableSessionHash(sessionID string) string {
	sum := sha256.Sum256([]byte(sessionID))
	return fmt.Sprintf("%x", sum[:8])
}

// SessionLogHandler 将结构化日志追加到 process.log 或会话 hash 文件。
// terminal handler 与它分离，文件格式因此不会继承颜色和日期噪声。
type SessionLogHandler struct {
	logRoot string
	level   slog.Level
	attrs   []sessionLogAttr
	group   string
	mu      *sync.Mutex
	report  func(error)
}

type sessionLogAttr struct {
	key   string
	value slog.Value
}

// NewSessionLogHandler 创建文件日志 handler。report 只用于把文件系统失败报告给终端，
// 不会再次调用文件 handler，避免错误路径递归写入日志文件。
func NewSessionLogHandler(dataDir string, level slog.Level, report func(error)) *SessionLogHandler {
	root, err := filepath.Abs(filepath.Join(dataDir, "logs"))
	if err != nil {
		root = filepath.Clean(filepath.Join(dataDir, "logs"))
	}
	return &SessionLogHandler{
		logRoot: root,
		level:   level,
		mu:      &sync.Mutex{},
		report:  report,
	}
}

func (h *SessionLogHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.level
}

func (h *SessionLogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	if len(attrs) == 0 {
		return h
	}
	h2 := *h
	h2.attrs = append([]sessionLogAttr(nil), h.attrs...)
	appendSessionLogAttrs(&h2.attrs, attrs, h.group)
	return &h2
}

func (h *SessionLogHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	h2 := *h
	h2.group = joinLogGroup(h.group, name)
	h2.attrs = append([]sessionLogAttr(nil), h.attrs...)
	return &h2
}

func (h *SessionLogHandler) Handle(_ context.Context, record slog.Record) error {
	if record.Level < h.level {
		return nil
	}
	attrs := append([]sessionLogAttr(nil), h.attrs...)
	var attrErr error
	record.Attrs(func(attr slog.Attr) bool {
		appendSessionLogAttrs(&attrs, []slog.Attr{attr}, h.group)
		return true
	})
	if attrErr != nil {
		return attrErr
	}

	sessionID := sessionIDFromLogAttrs(attrs)
	line := formatSessionLogLine(record, attrs, sessionID)
	path := h.logPath(sessionID)

	h.mu.Lock()
	defer h.mu.Unlock()
	if err := os.MkdirAll(h.logRoot, 0700); err != nil {
		h.reportFailure(fmt.Errorf("创建文件日志目录 %s: %w", h.logRoot, err))
		return nil
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		h.reportFailure(fmt.Errorf("打开文件日志 %s: %w", path, err))
		return nil
	}
	_, writeErr := io.WriteString(file, line)
	closeErr := file.Close()
	if writeErr != nil {
		h.reportFailure(fmt.Errorf("写入文件日志 %s: %w", path, writeErr))
	} else if closeErr != nil {
		h.reportFailure(fmt.Errorf("关闭文件日志 %s: %w", path, closeErr))
	}
	return nil
}

func (h *SessionLogHandler) logPath(sessionID string) string {
	if sessionID == "" {
		return filepath.Join(h.logRoot, "process.log")
	}
	return filepath.Join(h.logRoot, stableSessionHash(sessionID)+".log")
}

func (h *SessionLogHandler) reportFailure(err error) {
	if h.report != nil && err != nil {
		h.report(err)
	}
}

// CombinedLogHandler 将同一条 record 分发给终端和文件 handler。
// 两者各自保留 WithAttrs/WithGroup 状态，避免文件路由属性污染终端格式。
type CombinedLogHandler struct {
	terminal slog.Handler
	file     slog.Handler
}

func NewCombinedLogHandler(terminal, file slog.Handler) *CombinedLogHandler {
	return &CombinedLogHandler{terminal: terminal, file: file}
}

func (h *CombinedLogHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return (h.terminal != nil && h.terminal.Enabled(ctx, level)) ||
		(h.file != nil && h.file.Enabled(ctx, level))
}

func (h *CombinedLogHandler) Handle(ctx context.Context, record slog.Record) error {
	var firstErr error
	if h.terminal != nil && h.terminal.Enabled(ctx, record.Level) {
		if err := h.terminal.Handle(ctx, record); err != nil {
			firstErr = err
		}
	}
	if h.file != nil && h.file.Enabled(ctx, record.Level) {
		if err := h.file.Handle(ctx, record); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (h *CombinedLogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	clone := *h
	if h.terminal != nil {
		clone.terminal = h.terminal.WithAttrs(attrs)
	}
	if h.file != nil {
		clone.file = h.file.WithAttrs(attrs)
	}
	return &clone
}

func (h *CombinedLogHandler) WithGroup(name string) slog.Handler {
	clone := *h
	if h.terminal != nil {
		clone.terminal = h.terminal.WithGroup(name)
	}
	if h.file != nil {
		clone.file = h.file.WithGroup(name)
	}
	return &clone
}

func appendSessionLogAttrs(dst *[]sessionLogAttr, attrs []slog.Attr, group string) {
	for _, attr := range attrs {
		attr.Value = attr.Value.Resolve()
		if attr.Value.Kind() == slog.KindGroup {
			appendSessionLogAttrs(dst, attr.Value.Group(), joinLogGroup(group, attr.Key))
			continue
		}
		key := attr.Key
		if group != "" {
			key = joinLogGroup(group, key)
		}
		*dst = append(*dst, sessionLogAttr{key: key, value: attr.Value})
	}
}

func joinLogGroup(parent, child string) string {
	if parent == "" {
		return child
	}
	if child == "" {
		return parent
	}
	return parent + "." + child
}

func sessionIDFromLogAttrs(attrs []sessionLogAttr) string {
	for _, preferred := range []string{requestSessionIDAttr, sessionIDAttr, sourceSessionIDAttr} {
		for _, attr := range attrs {
			if attr.key != preferred || attr.value.Kind() != slog.KindString {
				continue
			}
			if value := attr.value.String(); value != "" {
				return value
			}
		}
	}
	return ""
}

func formatSessionLogLine(record slog.Record, attrs []sessionLogAttr, sessionID string) string {
	requestID, hasRequestID := requestIDFromLogAttrs(attrs)
	var builder strings.Builder
	builder.WriteString(record.Time.Format("15:04:05"))
	builder.WriteByte(' ')
	if label := fileLogLevelLabel(record.Level); label != "" {
		builder.WriteString(label)
		builder.WriteByte(' ')
	}
	if hasRequestID {
		builder.WriteByte('#')
		builder.WriteString(strconv.FormatUint(requestID, 10))
		builder.WriteByte(' ')
	}
	builder.WriteString(sanitizeFileLogText(record.Message, sessionID))
	for _, attr := range attrs {
		if !isVisibleFileLogAttr(attr.key) || attr.key == "request_id" {
			continue
		}
		value := sanitizeFileLogText(formatFileLogValue(attr.value), sessionID)
		if value == "" {
			continue
		}
		builder.WriteByte(' ')
		builder.WriteString(attr.key)
		builder.WriteByte('=')
		builder.WriteString(value)
	}
	builder.WriteByte('\n')
	return builder.String()
}

func fileLogLevelLabel(level slog.Level) string {
	switch {
	case level >= slog.LevelError:
		return "[ERROR]"
	case level >= slog.LevelWarn:
		return "[WARN]"
	case level < slog.LevelInfo:
		return "[DEBUG]"
	default:
		return ""
	}
}

func requestIDFromLogAttrs(attrs []sessionLogAttr) (uint64, bool) {
	for _, attr := range attrs {
		if attr.key != "request_id" {
			continue
		}
		switch attr.value.Kind() {
		case slog.KindUint64:
			return attr.value.Uint64(), true
		case slog.KindInt64:
			value := attr.value.Int64()
			if value >= 0 {
				return uint64(value), true
			}
		case slog.KindString:
			value, err := strconv.ParseUint(attr.value.String(), 10, 64)
			if err == nil {
				return value, true
			}
		}
	}
	return 0, false
}

func isVisibleFileLogAttr(key string) bool {
	if key == logColorKey || key == requestSessionIDAttr || key == sessionIDAttr || key == sourceSessionIDAttr {
		return false
	}
	lower := strings.ToLower(key)
	for _, denied := range []string{
		"authorization", "proxy-authorization", "api-key", "cookie", "set-cookie",
		"body", "messages", "system", "tools", "prompt", "payload", "base64", "fingerprint",
	} {
		if strings.Contains(lower, denied) || strings.HasSuffix(lower, "_session_id") {
			return false
		}
	}
	return key != ""
}

func formatFileLogValue(value slog.Value) string {
	value = value.Resolve()
	switch value.Kind() {
	case slog.KindString:
		return value.String()
	case slog.KindBool:
		return strconv.FormatBool(value.Bool())
	case slog.KindInt64:
		return strconv.FormatInt(value.Int64(), 10)
	case slog.KindUint64:
		return strconv.FormatUint(value.Uint64(), 10)
	case slog.KindFloat64:
		return strconv.FormatFloat(value.Float64(), 'g', -1, 64)
	case slog.KindDuration:
		return value.Duration().String()
	case slog.KindTime:
		return value.Time().Format("15:04:05")
	case slog.KindAny:
		return fmt.Sprint(value.Any())
	default:
		return ""
	}
}

func sanitizeFileLogText(value, sessionID string) string {
	if sessionID != "" {
		value = strings.ReplaceAll(value, sessionID, "[REDACTED]")
	}
	value = strings.ReplaceAll(value, "\033", "")
	value = strings.ReplaceAll(value, "\r", "\\r")
	value = strings.ReplaceAll(value, "\n", "\\n")
	return value
}

package proxy

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
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

// isSessionIdentityLogAttr 是终端与文件两个 sink 共用的 session 身份判定。
// 它是 Gap 3 的单一真源：两个 sink 各写一份判定正是这次不一致的根因。
func isSessionIdentityLogAttr(key string) bool {
	if key == requestSessionIDAttr || key == sessionIDAttr || key == sourceSessionIDAttr {
		return true
	}
	return strings.HasSuffix(strings.ToLower(key), "_session_id")
}

func isVisibleFileLogAttr(key string) bool {
	if key == logColorKey || isSessionIdentityLogAttr(key) {
		return false
	}
	lower := strings.ToLower(key)
	for _, denied := range []string{
		"authorization", "proxy-authorization", "api-key", "cookie", "set-cookie",
		"body", "messages", "system", "tools", "prompt", "payload", "base64", "fingerprint",
	} {
		if strings.Contains(lower, denied) {
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

// ---- AI 落盘的单条 request closure 与 process 级缺口摘要 ----

const (
	sessionOutcomeEvent    = "request_outcome"
	sessionOutcomeGapEvent = "outcome_gap"
)

// ErrSessionOutcomeWriterNotConfigured 是缺少文件出口时的稳定生命周期错误。
var ErrSessionOutcomeWriterNotConfigured = errors.New("session outcome writer sink not configured")

// sessionOutcomeSink 是 closure 与 process 级缺口摘要的唯一整行出口；
// shortHash 为空表示写 process 级记录。
type sessionOutcomeSink interface {
	WriteSessionLine(shortHash, line string) error
}

// WriteSessionLine 以整行为单位追加写入并返回真实错误。它与 Handle 的
// best-effort 语义不同：整条记录的成败必须由 outcome writer 判定，
// 因此这里不调用 report callback，也不吞掉失败。
func (h *SessionLogHandler) WriteSessionLine(shortHash, line string) error {
	if h == nil || line == "" {
		return nil
	}
	path := h.outcomeLogPath(shortHash)
	payload := sanitizeFileLogText(line, "") + "\n"
	h.mu.Lock()
	defer h.mu.Unlock()
	if err := os.MkdirAll(h.logRoot, 0700); err != nil {
		return fmt.Errorf("创建文件日志目录 %s: %w", h.logRoot, err)
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		return fmt.Errorf("打开文件日志 %s: %w", path, err)
	}
	if _, writeErr := io.WriteString(file, payload); writeErr != nil {
		_ = file.Close()
		return fmt.Errorf("写入文件日志 %s: %w", path, writeErr)
	}
	if closeErr := file.Close(); closeErr != nil {
		return fmt.Errorf("关闭文件日志 %s: %w", path, closeErr)
	}
	return nil
}

// outcomeLogPath 接受已归一的短 hash；非短 hash 输入再哈希一次，
// 保证完整 session 身份不会出现在文件名里。
func (h *SessionLogHandler) outcomeLogPath(shortHash string) string {
	if shortHash == "" {
		return filepath.Join(h.logRoot, "process.log")
	}
	if !isShortLowerHex(shortHash) {
		shortHash = stableSessionHash(shortHash)
	}
	return filepath.Join(h.logRoot, shortHash+".log")
}

// SessionOutcomeWriter 把 accepted snapshot 投影成目标 session 文件里的一条
// closure，并在成功后重放受控缺口：Claim → 写 process 级 outcome_gap →
// generation-aware Commit。它只在 gap 摘要自身写失败时累计 sink 缺口，
// closure 写失败由 dispatcher 统一累计，避免同一故障被计两次。
type SessionOutcomeWriter struct {
	sink    sessionOutcomeSink
	gaps    *OutcomeGapAccumulator
	tracker *HealthTracker
	now     func() time.Time
}

func NewSessionOutcomeWriter(sink sessionOutcomeSink, gaps *OutcomeGapAccumulator, tracker *HealthTracker) *SessionOutcomeWriter {
	return &SessionOutcomeWriter{sink: sink, gaps: gaps, tracker: tracker, now: time.Now}
}

// ProjectSession 实现 dispatcher 的 session lane projector 合同。
func (w *SessionOutcomeWriter) ProjectSession(snapshot requestOutcomeSnapshot) error {
	return w.WriteRequestOutcome(snapshot)
}

func (w *SessionOutcomeWriter) WriteRequestOutcome(snapshot requestOutcomeSnapshot) error {
	if w == nil || w.sink == nil {
		return ErrSessionOutcomeWriterNotConfigured
	}
	snapshot = snapshot.normalized()
	if err := w.sink.WriteSessionLine(snapshot.SessionHash, formatRequestOutcomeClosure(snapshot)); err != nil {
		// 整条 closure 缺失，不留半条因果链，也不在此重放缺口。
		return err
	}
	w.replayGaps(snapshot)
	return nil
}

// replayGaps 只在一次正常 closure 成功之后执行，且严格遵守
// Claim → write → generation-aware Commit 的顺序。
func (w *SessionOutcomeWriter) replayGaps(snapshot requestOutcomeSnapshot) {
	if w.gaps == nil {
		return
	}
	claim := w.gaps.Claim()
	if claim.ID == 0 {
		return
	}
	record := formatOutcomeGapRecord(claim, w.currentTime())
	if record == "" {
		w.gaps.Merge(claim)
		return
	}
	if err := w.sink.WriteSessionLine("", record); err != nil {
		// 先把本次摘要写失败记成新的 sink 缺口，再把原 claim 完整并回 current；
		// 两步顺序保证 Merge 之后 generation 不回退，也不会产生任何 recovered。
		generation := w.gaps.Accumulate(OutcomeGapEvent{
			Source:      OutcomeGapSourceSessionLogSink,
			SessionHash: snapshot.SessionHash,
			RequestID:   snapshot.RequestID,
			From:        snapshot.StartedAt,
			To:          snapshot.FinishedAt,
			Reason:      string(OutcomeGapReasonSessionLogSink),
		})
		if w.tracker != nil {
			w.tracker.ObserveFailure(HealthScopeSessionLogSink, HealthFailureClassSessionLogSink, generation)
		}
		w.gaps.Merge(claim)
		return
	}
	result := w.gaps.Commit(claim)
	if w.tracker == nil {
		return
	}
	// 只有 Commit 判定为可恢复的 scope 才拿到恢复证明；tracker 会再次校验 generation。
	for _, scope := range result.RecoverableScopes {
		generation, ok := result.CommittedGeneration(scope)
		if !ok {
			continue
		}
		w.tracker.ObserveGapCommit(scope, generation)
	}
}

func (w *SessionOutcomeWriter) currentTime() time.Time {
	if w.now != nil {
		return w.now()
	}
	return time.Now()
}

// formatRequestOutcomeClosure 生成 AI 可读的单条闭环：触发原因与最终结果同一行。
// 字段来自显式 allowlist，零值或无诊断价值的字段直接省略。
func formatRequestOutcomeClosure(snapshot requestOutcomeSnapshot) string {
	snapshot = snapshot.normalized()
	var builder strings.Builder
	builder.WriteString(outcomeDisplayTime(snapshot).Format("15:04:05"))
	builder.WriteString(" #")
	builder.WriteString(strconv.FormatUint(snapshot.RequestID, 10))
	builder.WriteString(" 请求结果 event=")
	builder.WriteString(sessionOutcomeEvent)
	appendOutcomeField(&builder, "session", snapshot.SessionHash)
	if snapshot.Eligibility != outcomeEligibilityEvaluable {
		appendOutcomeField(&builder, "eligibility", string(snapshot.Eligibility))
	}
	appendOutcomeField(&builder, "trigger", closureTriggerToken(snapshot.TriggerReason))
	if snapshot.PressureSource != pressureSourceUnknown {
		appendOutcomeField(&builder, "pressure_source", string(snapshot.PressureSource))
	}
	if snapshot.RequiredWait > 0 || snapshot.ActualWaitKnown {
		appendOutcomeField(&builder, "required_wait", snapshot.RequiredWait.String())
		appendOutcomeField(&builder, "actual_wait", snapshot.ActualWait.String())
		appendOutcomeField(&builder, "actual_wait_known", strconv.FormatBool(snapshot.ActualWaitKnown))
	}
	appendOutcomeField(&builder, "action", string(snapshot.Action))
	if snapshot.BeforeMessages > 0 || snapshot.AfterMessages > 0 {
		appendOutcomeField(&builder, "before_messages", strconv.Itoa(snapshot.BeforeMessages))
		appendOutcomeField(&builder, "after_messages", strconv.Itoa(snapshot.AfterMessages))
	}
	if snapshot.BeforeTokens > 0 || snapshot.AfterTokens > 0 {
		appendOutcomeField(&builder, "before_tokens", strconv.Itoa(snapshot.BeforeTokens))
		appendOutcomeField(&builder, "after_tokens", strconv.Itoa(snapshot.AfterTokens))
	}
	appendOutcomeField(&builder, "upstream", string(snapshot.UpstreamState))
	if snapshot.UpstreamStatus > 0 {
		appendOutcomeField(&builder, "upstream_status", strconv.Itoa(snapshot.UpstreamStatus))
	}
	appendOutcomeField(&builder, "memory", string(snapshot.MemoryState))
	appendOutcomeField(&builder, "disk", string(snapshot.DiskState))
	if snapshot.FailureClass != persistenceFailureUnknown && snapshot.FailureClass != persistenceFailureNone {
		appendOutcomeField(&builder, "failure", string(snapshot.FailureClass))
	}
	if snapshot.Intervention != interventionNone {
		appendOutcomeField(&builder, "intervention", string(snapshot.Intervention))
	}
	return builder.String()
}

// formatOutcomeGapRecord 把一次 claim 渲染成一条 process 级摘要：逐 scope 保留
// generation、数量、首尾短关联、时间范围与稳定原因；不含丢失明细、完整身份、
// 正文、路径或 raw error。
func formatOutcomeGapRecord(claim OutcomeGapClaim, at time.Time) string {
	var builder strings.Builder
	builder.WriteString(at.Format("15:04:05"))
	builder.WriteString(" 详细记录缺口 event=")
	builder.WriteString(sessionOutcomeGapEvent)
	wrote := false
	for index := range claim.Slots {
		slot := claim.Slots[index]
		if slot.Count == 0 {
			continue
		}
		syncOutcomeGapSnapshotAliases(&slot)
		prefix := string(slot.Scope)
		if prefix == "" {
			continue
		}
		appendOutcomeField(&builder, prefix+".generation", strconv.FormatUint(slot.Generation, 10))
		appendOutcomeField(&builder, prefix+".count", strconv.FormatUint(slot.Count, 10))
		appendOutcomeField(&builder, prefix+".first_session", slot.FirstShortHash)
		appendOutcomeField(&builder, prefix+".first_request", strconv.FormatUint(slot.FirstRequestID, 10))
		appendOutcomeField(&builder, prefix+".last_session", slot.LastShortHash)
		appendOutcomeField(&builder, prefix+".last_request", strconv.FormatUint(slot.LastRequestID, 10))
		appendOutcomeField(&builder, prefix+".from", slot.From.Local().Format("15:04:05"))
		appendOutcomeField(&builder, prefix+".to", slot.To.Local().Format("15:04:05"))
		appendOutcomeField(&builder, prefix+".reason", slot.Reason)
		wrote = true
	}
	if !wrote {
		return ""
	}
	return builder.String()
}

// closureTriggerToken 让“已评估但未触发”有稳定可读的取值，
// 不与 TriggerReason 的空零值混淆。
func closureTriggerToken(reason TriggerReason) string {
	if reason == TriggerNone {
		return "none"
	}
	return string(reason)
}

func appendOutcomeField(builder *strings.Builder, key, value string) {
	if builder == nil || key == "" || value == "" {
		return
	}
	builder.WriteByte(' ')
	builder.WriteString(key)
	builder.WriteByte('=')
	builder.WriteString(value)
}

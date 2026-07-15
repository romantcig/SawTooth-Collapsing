package proxy

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// deflateUsage 对 Anthropic API usage 字段执行衰减。
// 遍历 4 个 token 统计字段，每个乘以 factor 后 math.Floor 取整。
// 值为 0 的字段保持 0（D-06）。
// 若 usage 含 total_tokens，重新计算为前四项之和（D-05）。
func deflateUsage(usage map[string]any, factor float64) {
	if usage == nil {
		return
	}

	// 仅 deflate input_tokens 和 output_tokens。
	// cache_creation_input_tokens 和 cache_read_input_tokens 是 input_tokens 的子集，
	// 重复计算会违反 total_tokens = input_tokens + output_tokens 的 API 语义。
	tokenFields := []string{
		"input_tokens",
		"output_tokens",
	}

	var sum float64
	for _, field := range tokenFields {
		val, ok := usage[field]
		if !ok {
			continue
		}

		num := toFloat64(val)
		if num > 0 {
			deflated := math.Floor(num * factor)
			usage[field] = deflated
			sum += deflated
		}
		// num == 0: 保持 0，不加入 sum（D-06）
	}

	// total_tokens = input_tokens + output_tokens（API 标准定义）
	// 不包含 cache 子字段以避免重复计数
	if _, ok := usage["total_tokens"]; ok {
		usage["total_tokens"] = sum
	}
}

// toFloat64 将 any 类型的安全转换为 float64。
// 支持 float64、int、int64、json.Number 类型。
func toFloat64(v any) float64 {
	switch val := v.(type) {
	case float64:
		return val
	case int:
		return float64(val)
	case int64:
		return float64(val)
	case json.Number:
		f, _ := val.Float64()
		return f
	default:
		return 0
	}
}

// totalInputTokens 返回 Anthropic usage 占用的完整输入上下文。
// cache creation/read 均占据 context window，因此与未缓存 input 一并计入；
// output_tokens 属于输出空间，不参与 Sawtooth 上下文压力。
func totalInputTokens(usage map[string]any) int {
	if usage == nil {
		return 0
	}
	total := 0
	for _, field := range []string{"input_tokens", "cache_creation_input_tokens", "cache_read_input_tokens"} {
		value := nonNegativeUsageToken(usage[field])
		if value > math.MaxInt-total {
			return math.MaxInt
		}
		total += value
	}
	return total
}

func nonNegativeUsageToken(value any) int {
	maxInt := uint64(math.MaxInt)
	switch number := value.(type) {
	case int:
		if number > 0 {
			return number
		}
	case int8:
		if number > 0 {
			return int(number)
		}
	case int16:
		if number > 0 {
			return int(number)
		}
	case int32:
		if number > 0 {
			return int(number)
		}
	case int64:
		if number > int64(math.MaxInt) {
			return math.MaxInt
		}
		if number > 0 {
			return int(number)
		}
	case uint:
		if uint64(number) > maxInt {
			return math.MaxInt
		}
		return int(number)
	case uint8:
		return int(number)
	case uint16:
		return int(number)
	case uint32:
		return int(number)
	case uint64:
		if number > maxInt {
			return math.MaxInt
		}
		return int(number)
	case float32:
		return nonNegativeUsageFloat(float64(number))
	case float64:
		return nonNegativeUsageFloat(number)
	case json.Number:
		if integer, err := number.Int64(); err == nil {
			return nonNegativeUsageToken(integer)
		}
		if floating, err := number.Float64(); err == nil {
			return nonNegativeUsageFloat(floating)
		}
	}
	return 0
}

func nonNegativeUsageFloat(number float64) int {
	if math.IsNaN(number) || math.IsInf(number, 0) || number <= 0 {
		return 0
	}
	if number >= float64(math.MaxInt) {
		return math.MaxInt
	}
	return int(number)
}

func decodeJSONObjectUseNumber(raw []byte) (map[string]any, bool) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, false
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return nil, false
	}
	object, ok := value.(map[string]any)
	return object, ok
}

func strictNonNegativeUsageToken(value any) (int, bool) {
	maxInt := uint64(math.MaxInt)
	switch number := value.(type) {
	case int:
		return number, number >= 0
	case int8:
		return int(number), number >= 0
	case int16:
		return int(number), number >= 0
	case int32:
		return int(number), number >= 0
	case int64:
		if number < 0 || number > int64(math.MaxInt) {
			return 0, false
		}
		return int(number), true
	case uint:
		if uint64(number) > maxInt {
			return 0, false
		}
		return int(number), true
	case uint8:
		return int(number), true
	case uint16:
		return int(number), true
	case uint32:
		return int(number), true
	case uint64:
		if number > maxInt {
			return 0, false
		}
		return int(number), true
	case float32:
		return strictNonNegativeUsageFloat(float64(number))
	case float64:
		return strictNonNegativeUsageFloat(number)
	case json.Number:
		integer, err := number.Int64()
		if err != nil || integer < 0 || integer > int64(math.MaxInt) {
			return 0, false
		}
		return int(integer), true
	default:
		return 0, false
	}
}

func strictNonNegativeUsageFloat(number float64) (int, bool) {
	if math.IsNaN(number) || math.IsInf(number, 0) || number < 0 || math.Trunc(number) != number || number > float64(math.MaxInt) {
		return 0, false
	}
	return int(number), true
}

// parseAnthropicMessageInputUsage 是 JSON 与 SSE 共用的 actual 信任边界。
// 任一输入 token 字段畸形都会拒绝整份 usage，避免部分字段静默污染 baseline。
func parseAnthropicMessageInputUsage(message map[string]any) (map[string]any, int, bool) {
	messageType, _ := message["type"].(string)
	if messageType != "message" {
		return nil, 0, false
	}
	usage, ok := message["usage"].(map[string]any)
	if !ok {
		return nil, 0, false
	}
	total := 0
	hasPositive := false
	for _, field := range []string{"input_tokens", "cache_creation_input_tokens", "cache_read_input_tokens"} {
		value, present := usage[field]
		if !present {
			continue
		}
		tokens, valid := strictNonNegativeUsageToken(value)
		if !valid {
			return nil, 0, false
		}
		if tokens > 0 {
			hasPositive = true
		}
		if tokens > math.MaxInt-total {
			return nil, 0, false
		}
		total += tokens
	}
	if !hasPositive {
		return nil, 0, false
	}
	return usage, total, true
}

// debugEntry debug 落盘 JSON 结构（D-04）。
// metadata 字段 + body（原始 JSON 解析后的对象）。
type debugEntry struct {
	Timestamp    string          `json:"timestamp"`
	RequestID    uint64          `json:"request_id"`
	Stage        debugBodyStage  `json:"stage"`
	SessionID    string          `json:"session_id"`
	Model        string          `json:"model"`
	MessageCount int             `json:"message_count"`
	Headers      json.RawMessage `json:"headers,omitempty"`
	Body         json.RawMessage `json:"body"`
}

type debugBodyStage string

const (
	debugBodyStageRawInbound debugBodyStage = "raw_inbound"
	debugBodyStageForwarded  debugBodyStage = "forwarded"
	debugBodyStageResponse   debugBodyStage = "response"
)

type debugWriteCloser interface {
	Write([]byte) (int, error)
	Close() error
}

type debugFileOpener func(string, int, os.FileMode) (debugWriteCloser, error)

// writeDebugFile 将请求/响应落盘到 data_dir/debug/{sessionID}/{timestamp}-{direction}.json（D-03）。
// headers 参数仅在 req 方向传入（用于 redact Authorization）。
func (s *Server) writeDebugFile(sessionID string, requestID uint64, timestamp time.Time, stage debugBodyStage, body []byte, headers http.Header, model string, messageCount int) {
	debugDir, ok := safeDebugSessionDir(s.Config.Debug.DataDir, sessionID)
	if !ok {
		slog.Warn("debug session 目录校验失败")
		return
	}
	if err := os.MkdirAll(debugDir, 0755); err != nil {
		slog.Warn("无法创建 debug 目录", "path", debugDir, "error", err)
		return
	}

	// 文件安全的时间戳格式（RFC3339 中冒号替换为连字符）
	tsSafe := timestamp.Format("2006-01-02T150405.000000000")
	filename := fmt.Sprintf("%s-%d-%s.json", tsSafe, requestID, stage)
	filePath := filepath.Join(debugDir, filename)

	// 解析 body 为 JSON 对象（若解析失败则存为字符串）
	var bodyJSON json.RawMessage
	if parsed := parseJSON(body); parsed != nil {
		bodyJSON = parsed
	} else {
		// 非 JSON body（如 SSE 流式响应的 event 文本）
		bodyJSON, _ = json.Marshal(string(body))
	}

	// 构建 headers JSON，认证与会话凭证一律脱敏（T-02-02）。
	var headersJSON json.RawMessage
	if headers != nil {
		headersMap := make(map[string]string)
		for key, values := range headers {
			val := strings.Join(values, ", ")
			if isSensitiveDebugHeader(key) {
				val = "[REDACTED]"
			}
			headersMap[key] = val
		}
		headersJSON, _ = json.Marshal(headersMap)
	}

	entry := debugEntry{
		Timestamp:    timestamp.Format(time.RFC3339),
		RequestID:    requestID,
		Stage:        stage,
		SessionID:    sessionID,
		Model:        model,
		MessageCount: messageCount,
		Headers:      headersJSON,
		Body:         bodyJSON,
	}

	data, err := json.Marshal(entry)
	if err != nil {
		slog.Warn("无法序列化 debug 条目", "file", filePath, "error", err)
		return
	}

	if err := writeDebugEntryFile(filePath, data, func(name string, flag int, perm os.FileMode) (debugWriteCloser, error) {
		return os.OpenFile(name, flag, perm)
	}); err != nil {
		slog.Warn("无法写入 debug 文件", "file", filePath, "error", err)
	}
}

// writeFullBodyDebug 在显式开启 full_body 时，为每个请求阶段最多写入一份完整正文。
// raw_inbound 与 forwarded 分开落盘，便于直接审计代理在请求管线中的具体改写。
func (s *Server) writeFullBodyDebug(meta *requestMeta, timestamp time.Time, stage debugBodyStage, body []byte, headers http.Header, model string, messageCount int) {
	if !s.Config.Debug.Enabled || !s.Config.Debug.FullBody || meta == nil {
		return
	}
	once := meta.debugBodyOnce(stage)
	if once == nil {
		return
	}
	once.Do(func() {
		s.writeDebugFile(meta.RequestSessionID, meta.ID, timestamp, stage, body, headers, model, messageCount)
	})
}

func writeDebugEntryFile(filePath string, data []byte, openFile debugFileOpener) error {
	file, err := openFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	n, writeErr := file.Write(data)
	if writeErr == nil && n != len(data) {
		writeErr = io.ErrShortWrite
	}
	if writeErr != nil {
		_ = file.Close()
		_ = os.Remove(filePath)
		return writeErr
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(filePath)
		return err
	}
	return nil
}

func safeDebugSessionDir(dataDir, sessionID string) (string, bool) {
	root, err := filepath.Abs(filepath.Join(dataDir, "debug"))
	if err != nil {
		return "", false
	}
	sessionHash := fmt.Sprintf("%x", sha256.Sum256([]byte(sessionID)))
	dir := filepath.Join(root, sessionHash)
	rel, err := filepath.Rel(root, dir)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || filepath.IsAbs(rel) {
		return "", false
	}
	return dir, true
}

func isSensitiveDebugHeader(key string) bool {
	switch strings.ToLower(strings.TrimSpace(key)) {
	case "authorization", "proxy-authorization", "x-api-key", "anthropic-api-key", "cookie", "set-cookie":
		return true
	default:
		return false
	}
}

// parseJSON 尝试解析 body 为 JSON 对象，失败返回 nil。
func parseJSON(body []byte) []byte {
	var obj any
	if err := json.Unmarshal(body, &obj); err != nil {
		return nil
	}
	data, err := json.Marshal(obj)
	if err != nil {
		return nil
	}
	return data
}

type upstreamResponseResult struct {
	err       error
	committed bool
}

type upstreamFailureDecision struct {
	status        int
	timeoutSource string
	silent        bool
}

// forwardRaw 核心转发方法：SSE 流式 / JSON 非流式转发 + usage deflation + debug 落盘。
func (s *Server) forwardRaw(w http.ResponseWriter, r *http.Request, meta *requestMeta) {
	const maxBodySize = 10 * 1024 * 1024 // 10 MB（T-02-04）
	logger := meta.Logger

	limitedReader := io.LimitReader(r.Body, maxBodySize+1)
	body, err := io.ReadAll(limitedReader)
	_ = r.Body.Close()
	if err != nil {
		logger.Error("读取请求体失败", "error", err)
		http.Error(w, "Failed to read request body", http.StatusInternalServerError)
		return
	}
	if len(body) > maxBodySize {
		logger.Warn("请求体超限", "size", len(body))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusRequestEntityTooLarge)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "Request Entity Too Large"})
		return
	}

	model := extractModelFromBody(body)
	messageCount := countMessages(body)
	markForwardedPressureCoordinates(meta, body)
	stream := streamRequest(body)
	meta.logEntry(model, messageCount)
	logger.Info("上游请求发送",
		"forwarded_message_count", messageCount,
		"model", model,
	)

	timestamp := time.Now()
	s.writeRequestDebugFacts(meta, timestamp, debugStageRawInbound, body, r)
	s.writeRequestDebugFacts(meta, timestamp, debugStageForwarded, body, r)
	s.writeFullBodyDebug(meta, timestamp, debugBodyStageRawInbound, body, r.Header, model, messageCount)
	s.writeFullBodyDebug(meta, timestamp, debugBodyStageForwarded, body, r.Header, model, messageCount)

	upstreamStartedAt := time.Now()
	tracker := newUpstreamPhaseTracker()
	hardContext, hardCancel := withProxyHardLimit(r.Context(), s.Config.Transport.HardTimeout)
	defer hardCancel()
	upstreamContext := withStreamMarker(hardContext, stream)
	upstreamContext = httptrace.WithClientTrace(upstreamContext, tracker.trace())

	targetURL := strings.TrimRight(s.Config.Proxy.Target, "/") + r.URL.RequestURI()
	upstreamReq, err := http.NewRequestWithContext(upstreamContext, r.Method, targetURL, bytes.NewReader(body))
	if err != nil {
		logUpstreamFailure(logger, "创建上游请求失败", err, upstreamStartedAt, tracker, "upstream_transport", stream, false)
		writeGatewayJSON(w, http.StatusBadGateway, "failed to create upstream request")
		return
	}
	upstreamReq.ContentLength = int64(len(body))

	for key, values := range r.Header {
		for _, value := range values {
			upstreamReq.Header.Add(key, value)
		}
	}
	if upstreamReq.Header.Get("Content-Type") == "" {
		upstreamReq.Header.Set("Content-Type", "application/json")
	}
	upstreamReq.Header.Del("Connection")
	upstreamReq.Header.Del("Content-Length")
	upstreamReq.Header.Del("Accept-Encoding")

	resp, err := s.HTTPClient.Do(upstreamReq)
	if err != nil {
		decision := classifyUpstreamFailure(r.Context(), upstreamContext, tracker, nil, err, stream)
		logUpstreamFailure(logger, "上游请求失败", err, upstreamStartedAt, tracker, decision.timeoutSource, stream, false)
		if decision.silent {
			return
		}
		writeClassifiedGatewayError(w, decision, err.Error(), true)
		return
	}
	tracker.set(upstreamPhaseReadingBody)
	resp.Body = newIdleTimeoutBody(resp.Body, s.Config.Transport.ResponseIdleTimeout)
	defer resp.Body.Close()

	var result upstreamResponseResult
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		result = s.handleNon2xx(w, resp, meta, timestamp, model, messageCount)
	} else if strings.Contains(resp.Header.Get("Content-Type"), "text/event-stream") {
		result = s.handleSSE(w, resp, meta, timestamp, model, messageCount)
	} else {
		result = s.handleJSON(w, resp, meta, timestamp, model, messageCount)
	}
	if result.err == nil {
		return
	}

	decision := classifyUpstreamFailure(r.Context(), upstreamContext, tracker, resp.Body, result.err, stream)
	logUpstreamFailure(logger, "读取上游响应失败", result.err, upstreamStartedAt, tracker, decision.timeoutSource, stream, result.committed)
	if decision.silent || result.committed {
		return
	}
	writeClassifiedGatewayError(w, decision, "failed to read upstream response", false)
}

// markForwardedPressureCoordinates 证明上游 actual 对应的消息坐标仍与原始 pressure decision 一致。
// 任何压缩、桩化或修复只要改变了消息历史，就不能把压缩后 actual 绑定回原始历史前缀。
func markForwardedPressureCoordinates(meta *requestMeta, body []byte) {
	if meta == nil || !meta.PressureDecision.Available {
		return
	}
	var payload struct {
		Messages []Message `json:"messages"`
	}
	if err := json.Unmarshal(body, &payload); err != nil ||
		len(payload.Messages) != meta.PressureDecision.MessageCount ||
		fingerprintMessagesPrefix(payload.Messages, len(payload.Messages)) != meta.PressureDecision.MessagesPrefixFingerprint {
		meta.PressureDecision.ForwardedCoordinatesChanged = true
	}
}

// applyPressureBaselineUsage 只在 actual 与原始消息坐标一致时建立可复用 baseline。
// 返回值表示 actual baseline 是否被请求代际协议真正接受，供 response_usage facts 如实记录。
// 若本轮改写了历史，则清空旧 baseline，确保下一轮从 local_full 重新校准，但不声称建立了 actual baseline。
func (s *Server) applyPressureBaselineUsage(meta *requestMeta, actual int) bool {
	if meta == nil || actual <= 0 || s.Sawtooth == nil || !meta.tracksSawtoothState() || !meta.PressureDecision.Available {
		return false
	}
	decision := meta.PressureDecision
	if decision.ForwardedCoordinatesChanged {
		s.Sawtooth.UpdatePressureBaselineForRequest(meta.RequestSessionID, meta.BaselineGeneration, 0, 0, "", "", "")
		return false
	}
	return s.Sawtooth.UpdatePressureBaselineForRequest(meta.RequestSessionID, meta.BaselineGeneration, actual, decision.MessageCount, decision.SystemFingerprint, decision.ToolsFingerprint, decision.MessagesPrefixFingerprint)
}

func streamRequest(body []byte) bool {
	var envelope struct {
		Stream bool `json:"stream"`
	}
	return json.Unmarshal(body, &envelope) == nil && envelope.Stream
}

func classifyUpstreamFailure(inboundContext, upstreamContext context.Context, tracker *upstreamPhaseTracker, body io.ReadCloser, err error, stream bool) upstreamFailureDecision {
	if inboundContext.Err() != nil {
		return upstreamFailureDecision{timeoutSource: "downstream_context", silent: true}
	}
	if errors.Is(context.Cause(upstreamContext), errProxyHardLimit) {
		return upstreamFailureDecision{status: http.StatusGatewayTimeout, timeoutSource: "proxy_hard_limit"}
	}
	if errors.Is(err, errProxyResponseIdleTimeout) || idleBodyTimedOut(body) {
		return upstreamFailureDecision{status: http.StatusGatewayTimeout, timeoutSource: "response_idle_timeout"}
	}
	if tracker.current() == upstreamPhaseAwaitingHeaders && isTimeoutError(err) {
		source := "non_stream_header_timeout"
		if stream {
			source = "stream_header_timeout"
		}
		return upstreamFailureDecision{status: http.StatusGatewayTimeout, timeoutSource: source}
	}
	return upstreamFailureDecision{status: http.StatusBadGateway, timeoutSource: "upstream_transport"}
}

func idleBodyTimedOut(body io.ReadCloser) bool {
	idleBody, ok := body.(*idleTimeoutBody)
	return ok && idleBody.timedOut()
}

func isTimeoutError(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

func writeClassifiedGatewayError(w http.ResponseWriter, decision upstreamFailureDecision, fallbackDetail string, transportError bool) {
	if decision.status == http.StatusGatewayTimeout {
		writeGatewayJSON(w, http.StatusGatewayTimeout, "upstream request timed out")
		return
	}
	detail := fallbackDetail
	if !transportError {
		detail = "failed to read upstream response"
	}
	writeGatewayJSON(w, http.StatusBadGateway, detail)
}

func writeGatewayJSON(w http.ResponseWriter, status int, detail string) {
	w.Header().Del("Connection")
	w.Header().Del("Content-Encoding")
	w.Header().Del("Content-Length")
	w.Header().Del("Cache-Control")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"error":  http.StatusText(status),
		"detail": detail,
	})
}

func logUpstreamFailure(logger *slog.Logger, message string, err error, started time.Time, tracker *upstreamPhaseTracker, timeoutSource string, stream, committed bool) {
	fields := []any{
		"error", safeUpstreamError(err),
		"elapsed_ms", time.Since(started).Milliseconds(),
		"phase", tracker.current(),
		"timeout_source", timeoutSource,
		"stream", stream,
	}
	if committed {
		fields = append(fields, "response_committed", true)
	}
	logger.Error(message, fields...)
}

func safeUpstreamError(err error) string {
	if err == nil {
		return ""
	}
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		return safeURLError(urlErr)
	}
	return err.Error()
}

func safeURLError(err *url.Error) string {
	safeURL := err.URL
	if parsed, parseErr := url.Parse(err.URL); parseErr == nil {
		parsed.User = nil
		safeURL = parsed.String()
	}
	return fmt.Sprintf("%s %q: %s", err.Op, safeURL, safeUpstreamError(err.Err))
}

func copyResponseHeaders(dst http.Header, src http.Header) {
	for key, values := range src {
		for _, value := range values {
			dst.Add(key, value)
		}
	}
}

func (s *Server) handleNon2xx(w http.ResponseWriter, resp *http.Response, meta *requestMeta, timestamp time.Time, model string, messageCount int) upstreamResponseResult {
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return upstreamResponseResult{err: err}
	}
	meta.Logger.Warn("上游返回非 2xx", "status", resp.StatusCode)
	copyResponseHeaders(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
	_, _ = w.Write(respBody)
	s.writeFullBodyDebug(meta, timestamp, debugBodyStageResponse, respBody, resp.Header, model, messageCount)
	return upstreamResponseResult{committed: true}
}

// countMessages 从请求体 JSON 中提取 messages 数组长度。
func countMessages(body []byte) int {
	var data struct {
		Messages []any `json:"messages"`
	}
	if err := json.Unmarshal(body, &data); err != nil {
		return 0
	}
	return len(data.Messages)
}

type sseBaselineState struct {
	usage   map[string]any
	actual  int
	started bool
	stopped bool
	invalid bool
}

func (state *sseBaselineState) invalidate() {
	state.usage = nil
	state.actual = 0
	state.invalid = true
}

// observe 只接受 event/data 类型一致的单一 message_start → message_stop 生命周期。
// 任意乱序、重复或类型错配都会永久废弃本流的 baseline 候选。
func (state *sseBaselineState) observe(event []string) {
	if state.invalid {
		return
	}
	var eventName string
	var dataLine string
	eventFields := 0
	dataFields := 0
	for _, line := range event {
		switch {
		case strings.HasPrefix(line, "event:"):
			eventFields++
			eventName = strings.TrimSpace(line[len("event:"):])
		case strings.HasPrefix(line, "data:"):
			dataFields++
			dataLine = strings.TrimSpace(line[len("data:"):])
		}
	}

	data, decoded := decodeJSONObjectUseNumber([]byte(dataLine))
	dataType, _ := data["type"].(string)
	baselineEvent := eventName == "message_start" || eventName == "message_stop" ||
		dataType == "message_start" || dataType == "message_stop"
	if !baselineEvent {
		return
	}
	if eventFields != 1 || dataFields != 1 || !decoded || eventName != dataType {
		state.invalidate()
		return
	}

	switch dataType {
	case "message_start":
		if state.started || state.stopped {
			state.invalidate()
			return
		}
		message, ok := data["message"].(map[string]any)
		if !ok {
			state.invalidate()
			return
		}
		usage, actual, ok := parseAnthropicMessageInputUsage(message)
		if !ok {
			state.invalidate()
			return
		}
		state.usage = make(map[string]any, len(usage))
		for key, value := range usage {
			state.usage[key] = value
		}
		state.actual = actual
		state.started = true
	case "message_stop":
		if !state.started || state.stopped {
			state.invalidate()
			return
		}
		state.stopped = true
	}
}

func (state *sseBaselineState) complete() bool {
	return !state.invalid && state.started && state.stopped && state.usage != nil && state.actual > 0
}

// handleSSE 处理 SSE 流式响应。
// 逐行扫描，对 message_start 和 message_delta 事件执行 usage deflation，
// 其他事件原样转发。每个事件后 flush。
func (s *Server) handleSSE(w http.ResponseWriter, resp *http.Response, meta *requestMeta, timestamp time.Time, model string, messageCount int) upstreamResponseResult {
	flusher, ok := w.(http.Flusher)
	if !ok {
		return upstreamResponseResult{err: errors.New("ResponseWriter 不支持 Flush")}
	}

	// gzip 解压（若上游返回了压缩响应）
	var bodyReader io.Reader = resp.Body
	compressed := false
	if resp.Header.Get("Content-Encoding") == "gzip" {
		gzReader, err := gzip.NewReader(resp.Body)
		if err != nil {
			return upstreamResponseResult{err: fmt.Errorf("SSE gzip 解压失败: %w", err)}
		}
		defer gzReader.Close()
		bodyReader = gzReader
		compressed = true
	}

	scanner := bufio.NewScanner(bodyReader)
	// 1 MB 行缓冲区处理大事件
	scanner.Buffer(make([]byte, 0, 1024*1024), 1024*1024)

	var eventBuf []string
	var fullResponse strings.Builder
	var baselineState sseBaselineState
	var downstreamErr error
	committed := false
	commit := func() {
		if committed {
			return
		}
		copyResponseHeaders(w.Header(), resp.Header)
		if compressed {
			w.Header().Del("Content-Encoding")
			w.Header().Del("Content-Length")
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.WriteHeader(http.StatusOK)
		committed = true
	}
	flushEvent := func(event []string) {
		if downstreamErr != nil {
			return
		}
		baselineState.observe(event)
		commit()
		processed := s.processSSEEvent(event)
		for _, line := range processed {
			if _, err := fmt.Fprintf(w, "%s\n", line); err != nil {
				downstreamErr = err
				return
			}
			fullResponse.WriteString(line)
			fullResponse.WriteString("\n")
		}
		if _, err := fmt.Fprint(w, "\n"); err != nil {
			downstreamErr = err
			return
		}
		fullResponse.WriteString("\n")
		flusher.Flush()
	}

	for scanner.Scan() {
		line := scanner.Text()

		if line == "" {
			// 空行 = 事件边界，flush 整个事件
			if len(eventBuf) > 0 {
				flushEvent(eventBuf)
				eventBuf = eventBuf[:0]
			}
			continue
		}
		eventBuf = append(eventBuf, line)
	}

	// 处理末尾可能遗漏的事件（无尾随空行）
	if len(eventBuf) > 0 {
		flushEvent(eventBuf)
	}

	s.writeFullBodyDebug(meta, timestamp, debugBodyStageResponse, []byte(fullResponse.String()), resp.Header, model, messageCount)
	if err := scanner.Err(); err != nil {
		return upstreamResponseResult{err: err, committed: committed}
	}
	if downstreamErr != nil {
		return upstreamResponseResult{err: downstreamErr, committed: committed}
	}
	if baselineState.complete() {
		baselineUpdated := s.applyPressureBaselineUsage(meta, baselineState.actual)
		s.writeUsageDebugFacts(meta, timestamp, baselineState.usage, baselineUpdated)
	}
	if !committed {
		commit()
	}
	return upstreamResponseResult{committed: true}
}

// processSSEEvent 处理单个 SSE 事件。
// 识别 message_start 和 message_delta 事件中的 usage 并执行 deflation。
// 其他事件类型原样返回。JSON 解析失败时原样转发（T-02-05 graceful degradation）。
func (s *Server) processSSEEvent(event []string) []string {
	factor := s.Config.Proxy.Deflation

	// 找到 data: 行
	var dataLine string
	var dataIdx int
	for i, line := range event {
		if strings.HasPrefix(line, "data:") {
			dataLine = strings.TrimSpace(line[5:])
			dataIdx = i
			break
		}
	}

	if dataLine == "" {
		return event
	}

	// 解析 data JSON
	var data map[string]any
	if err := json.Unmarshal([]byte(dataLine), &data); err != nil {
		// 解析失败，原样转发（T-02-05）
		return event
	}

	eventType, _ := data["type"].(string)

	switch eventType {
	case "message_start":
		if msg, ok := data["message"].(map[string]any); ok {
			if usage, ok := msg["usage"].(map[string]any); ok {
				deflateUsage(usage, factor)
			}
		}
	case "message_delta":
		if usage, ok := data["usage"].(map[string]any); ok {
			deflateUsage(usage, factor)
		}
	}

	// 重新 marshal data 行
	newData, err := json.Marshal(data)
	if err != nil {
		return event
	}

	// 重建事件，替换修改后的 data 行
	result := make([]string, len(event))
	copy(result, event)
	result[dataIdx] = "data: " + string(newData)

	return result
}

// handleJSON 处理 JSON 非流式响应。
// 2xx 响应 deflate usage，非 2xx 不修改。
func (s *Server) handleJSON(w http.ResponseWriter, resp *http.Response, meta *requestMeta, timestamp time.Time, model string, messageCount int) upstreamResponseResult {
	logger := meta.Logger
	// gzip 解压（若上游返回了压缩响应）
	var bodyReader io.Reader = resp.Body
	compressed := false
	if resp.Header.Get("Content-Encoding") == "gzip" {
		gzReader, err := gzip.NewReader(resp.Body)
		if err != nil {
			return upstreamResponseResult{err: fmt.Errorf("JSON gzip 解压失败: %w", err)}
		}
		defer gzReader.Close()
		bodyReader = gzReader
		compressed = true
	}

	respBody, err := io.ReadAll(bodyReader)
	if err != nil {
		return upstreamResponseResult{err: err}
	}

	// 2xx: deflate usage
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		body, decoded := decodeJSONObjectUseNumber(respBody)
		if !decoded {
			// 解析失败，原样转发（T-02-05）
			logger.Warn("无法解析上游 JSON 响应，原样转发")
		} else {
			// 只有严格合法的 Anthropic message usage 才能生成 facts 或校准 baseline。
			if usage, actual, ok := parseAnthropicMessageInputUsage(body); ok {
				baselineUpdated := s.applyPressureBaselineUsage(meta, actual)
				s.writeUsageDebugFacts(meta, timestamp, usage, baselineUpdated)
			}

			if usage, ok := body["usage"].(map[string]any); ok {
				deflateUsage(usage, s.Config.Proxy.Deflation)
			}
			if newBody, err := json.Marshal(body); err == nil {
				respBody = newBody
			}
		}
	}

	copyResponseHeaders(w.Header(), resp.Header)
	if compressed {
		w.Header().Del("Content-Encoding")
		w.Header().Del("Content-Length")
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = w.Write(respBody)

	// 步骤 5: Debug 写 JSON 响应
	s.writeFullBodyDebug(meta, timestamp, debugBodyStageResponse, respBody, resp.Header, model, messageCount)
	return upstreamResponseResult{committed: true}
}

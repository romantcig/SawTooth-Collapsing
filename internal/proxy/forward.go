package proxy

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
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

// deflateUsage 对 Anthropic API usage 的三个输入侧字段执行衰减。
// 每个乘以 factor 后 math.Floor 取整；值为 0 的字段保持 0（D-06）。
// factor >= 1 表示关闭，usage 逐字段原样保留。
// 若 usage 含 total_tokens，重算为三项 deflated 值加未改动的 output_tokens。
func deflateUsage(usage map[string]any, factor float64) {
	if usage == nil || factor >= 1 {
		return
	}

	// 只衰减客户端上下文求和的三项。cache_creation_input_tokens 与
	// cache_read_input_tokens 不是 input_tokens 的子集——Anthropic 的 input_tokens
	// 只计未命中缓存的部分，三者互斥、相加才是总输入（见 totalInputTokens）。
	// 漏掉 cache_* 会让该求和混入 factor 与 1.0 两套标尺，缓存命中率越高越失效。
	//
	// 有意偏离 D-05 的"四字段"：output_tokens 不进客户端的上下文求和，
	// 衰减它对抑制 autoCompact 零贡献，只会让输出计数与成本显示失真。
	// 与 YesMem internal/proxy/usage.go:136 的字段集合一致。
	tokenFields := []string{
		"input_tokens",
		"cache_creation_input_tokens",
		"cache_read_input_tokens",
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

	// total_tokens = 三项 deflated 值 + 未衰减的 output_tokens
	if _, ok := usage["total_tokens"]; ok {
		usage["total_tokens"] = sum + toFloat64(usage["output_tokens"])
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
	Timestamp         string          `json:"timestamp"`
	RequestID         uint64          `json:"request_id"`
	Stage             debugBodyStage  `json:"stage"`
	Model             string          `json:"model"`
	MessageCount      int             `json:"message_count"`
	Headers           json.RawMessage `json:"headers,omitempty"`
	UpstreamHeaders   json.RawMessage `json:"upstream_headers,omitempty"`
	DownstreamHeaders json.RawMessage `json:"downstream_headers,omitempty"`
	Body              json.RawMessage `json:"body"`
}

type debugBodyStage string

const (
	debugBodyStageRawInbound debugBodyStage = "raw_inbound"
	debugBodyStageForwarded  debugBodyStage = "forwarded"
	debugBodyStageResponse   debugBodyStage = "response"
)

func debugBodyArtifactStage(stage debugBodyStage) (debugArtifactStage, bool) {
	switch stage {
	case debugBodyStageRawInbound:
		return debugArtifactRawBody, true
	case debugBodyStageForwarded:
		return debugArtifactForwardedBody, true
	case debugBodyStageResponse:
		return debugArtifactResponseBody, true
	default:
		return "", false
	}
}

type debugWriteCloser interface {
	Write([]byte) (int, error)
	Close() error
}

type debugFileOpener func(string, int, os.FileMode) (debugWriteCloser, error)

// writeDebugFile 将请求/响应写入统一的 run/session/request/stage Debug 树。
// headers 参数仅在请求方向传入（用于大小写不敏感地脱敏认证凭证）。
func (s *Server) writeDebugFile(meta *requestMeta, timestamp time.Time, stage debugBodyStage, body []byte, headers http.Header, model string, messageCount int) error {
	return s.writeDebugFileWithSnapshots(meta, timestamp, stage, body, headers, nil, model, messageCount)
}

func debugHeadersJSON(headers http.Header) json.RawMessage {
	if headers == nil {
		return nil
	}
	headersMap := make(map[string]string, len(headers))
	for key, values := range headers {
		value := strings.Join(values, ", ")
		if isSensitiveDebugHeader(key) {
			value = "[REDACTED]"
		}
		headersMap[key] = value
	}
	encoded, _ := json.Marshal(headersMap)
	return encoded
}

func (s *Server) writeDebugFileWithSnapshots(meta *requestMeta, timestamp time.Time, stage debugBodyStage, body []byte, upstreamHeaders, downstreamHeaders http.Header, model string, messageCount int) error {
	artifactStage, ok := debugBodyArtifactStage(stage)
	if !ok {
		return fmt.Errorf("debug body stage 无效: %q", stage)
	}
	routingMeta, err := s.debugMetaForWrite(meta)
	if err != nil {
		return err
	}

	// 解析 body 为 JSON 对象（若解析失败则存为字符串）
	var bodyJSON json.RawMessage
	if parsed := parseJSON(body); parsed != nil {
		bodyJSON = parsed
	} else {
		// 非 JSON body（如 SSE 流式响应的 event 文本）
		bodyJSON, _ = json.Marshal(string(body))
	}

	// 上游/下游 header 分离保存，且两侧都复用同一脱敏边界。
	upstreamJSON := debugHeadersJSON(upstreamHeaders)
	downstreamJSON := debugHeadersJSON(downstreamHeaders)

	entry := debugEntry{
		Timestamp:         timestamp.Format(time.RFC3339),
		RequestID:         meta.ID,
		Stage:             stage,
		Model:             model,
		MessageCount:      messageCount,
		Headers:           upstreamJSON,
		UpstreamHeaders:   upstreamJSON,
		DownstreamHeaders: downstreamJSON,
		Body:              bodyJSON,
	}

	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("序列化 debug 条目失败: %w", err)
	}
	return s.debugLayout.Write(routingMeta, artifactStage, data)
}

// writeFullBodyDebug 在显式开启 full_body 时，为每个请求阶段最多写入一份完整正文。
// raw_inbound 与 forwarded 分开落盘，便于直接审计代理在请求管线中的具体改写。
func (s *Server) writeFullBodyDebug(meta *requestMeta, timestamp time.Time, stage debugBodyStage, body []byte, headers http.Header, model string, messageCount int) {
	s.writeFullBodyDebugWithSnapshots(meta, timestamp, stage, body, headers, nil, model, messageCount)
}

func (s *Server) writeFullBodyDebugWithSnapshots(meta *requestMeta, timestamp time.Time, stage debugBodyStage, body []byte, upstreamHeaders, downstreamHeaders http.Header, model string, messageCount int) {
	if !s.Config.Debug.Enabled || !s.Config.Debug.FullBody || meta == nil {
		return
	}
	once := meta.debugBodyOnce(stage)
	if once == nil {
		return
	}
	once.Do(func() {
		if err := s.writeDebugFileWithSnapshots(meta, timestamp, stage, body, upstreamHeaders, downstreamHeaders, model, messageCount); err != nil {
			slog.Warn("无法写入 debug 文件", "stage", stage, "request_id", meta.ID, "error", err)
		}
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
	dir := filepath.Join(root, stableSessionHash(sessionID))
	if !pathWithinRoot(root, dir) {
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

const downstreamWriteFailureClass = "downstream_write"

type upstreamResponseResult struct {
	err          error
	committed    bool
	failureClass string
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
		requestOutcome(meta).SetAction(outcomeActionUnavailable)
		requestOutcome(meta).SetIntervention(interventionRequired)
		recordUpstreamOutcome(meta, upstreamStateNotStarted, 0)
		http.Error(w, "Failed to read request body", http.StatusInternalServerError)
		return
	}
	if len(body) > maxBodySize {
		logger.Warn("请求体超限", "size", len(body))
		requestOutcome(meta).SetAction(outcomeActionUnavailable)
		requestOutcome(meta).SetIntervention(interventionRequired)
		recordUpstreamOutcome(meta, upstreamStateNotStarted, 0)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusRequestEntityTooLarge)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "Request Entity Too Large"})
		return
	}

	model := extractModelFromBody(body)
	messageCount := countMessages(body)
	markForwardedPressureCoordinates(meta, body, s.TokenCounter)
	stream := streamRequest(body)
	meta.logEntry(model, messageCount)
	// Debug 级事件键：终端模板渲染 `→ 上游发送`（-v 可见），文件侧全量收录。
	logger.Debug(eventKeyRequestForwarded,
		"forwarded_message_count", messageCount,
		"model", model,
		LogDim,
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
		recordUpstreamOutcome(meta, upstreamStateTransportFailure, 0)
		requestOutcome(meta).SetFailureClass(persistenceFailureUpstream)
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
		recordUpstreamOutcome(meta, upstreamFailureState(decision, false, upstreamStateTransportFailure), 0)
		requestOutcome(meta).SetFailureClass(persistenceFailureUpstream)
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
	if result.failureClass == downstreamWriteFailureClass {
		// WriteHeader 已提交后，不能再走“读取上游失败”分类或追加第二份
		// gateway JSON；下游写入错误是独立、稳定的结果类别。
		logger.Error("下游响应写入失败",
			"error", safeUpstreamError(result.err),
			"failure_class", downstreamWriteFailureClass,
			"response_committed", result.committed,
		)
		return
	}

	decision := classifyUpstreamFailure(r.Context(), upstreamContext, tracker, resp.Body, result.err, stream)
	logUpstreamFailure(logger, "读取上游响应失败", result.err, upstreamStartedAt, tracker, decision.timeoutSource, stream, result.committed)
	// 超时是比 handler 内部记录的 body/committed 失败更精确的事实，因此在这里
	// 覆盖；非超时分类保持 handler 已固定的终态不变。
	if state, ok := upstreamTimeoutState(decision); ok {
		recordUpstreamOutcome(meta, state, resp.StatusCode)
	}
	if decision.silent || result.committed {
		return
	}
	writeClassifiedGatewayError(w, decision, "failed to read upstream response", false)
}

// upstreamTimeoutState 把 classifyUpstreamFailure 的 timeout source 映射为受限终态。
// 只有真正的超时/下游取消才返回 ok；其余分类由调用方保留自己的默认终态。
func upstreamTimeoutState(decision upstreamFailureDecision) (upstreamState, bool) {
	switch decision.timeoutSource {
	case "downstream_context":
		return upstreamStateUnavailable, true
	case "proxy_hard_limit":
		return upstreamStateHardTimeout, true
	case "response_idle_timeout":
		return upstreamStateIdleTimeout, true
	case "stream_header_timeout", "non_stream_header_timeout":
		return upstreamStateHeaderTimeout, true
	default:
		return upstreamStateUnknown, false
	}
}

func upstreamFailureState(decision upstreamFailureDecision, committed bool, fallback upstreamState) upstreamState {
	if state, ok := upstreamTimeoutState(decision); ok {
		return state
	}
	if committed {
		return upstreamStateCommittedFailure
	}
	return fallback
}

// markForwardedPressureCoordinates 将 pressure baseline 的坐标绑定到真正发送给上游的
// system/tools/messages。baseline 契约坐标固定为入口 raw 快照
// （meta.PressureEntryCoordinates），绝不随 wire 改写——否则折叠
// 轮之后 CC 的 raw 回放会与 wire 坐标永远失配，actual_plus_delta 主路径被锁死。
// 同点补算转发 wire 的本地全量估算（messages + system + tools，口径与
// buildPressureDecision 的 FullLocalEstimate 一致），供响应侧与 actual 配对写入
// 校准样本；此处只暂存，绝不单独写样本。
func markForwardedPressureCoordinates(meta *requestMeta, body []byte, tc *TokenCounter) {
	if meta == nil || !meta.PressureDecision.Available {
		return
	}

	var payload map[string]json.RawMessage
	decoder := json.NewDecoder(bytes.NewReader(body))
	if err := decoder.Decode(&payload); err != nil {
		meta.PressureDecision.ForwardedCoordinatesBound = false
		return
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		meta.PressureDecision.ForwardedCoordinatesBound = false
		return
	}

	messagesRaw, ok := payload["messages"]
	if !ok || bytes.Equal(bytes.TrimSpace(messagesRaw), []byte("null")) || len(bytes.TrimSpace(messagesRaw)) == 0 || bytes.TrimSpace(messagesRaw)[0] != '[' {
		meta.PressureDecision.ForwardedCoordinatesBound = false
		return
	}
	var messages []Message
	if err := json.Unmarshal(messagesRaw, &messages); err != nil {
		meta.PressureDecision.ForwardedCoordinatesBound = false
		return
	}

	// decision 的坐标保持入口 raw 口径不变；本函数只证明 wire body 已解析、
	// usage 与 wire 估算可安全配对（Bound）。
	decision := &meta.PressureDecision
	decision.ForwardedCoordinatesBound = true
	if tc != nil {
		decision.ForwardedLocalEstimate = saturatingAdd(
			saturatingAdd(tc.CountMessagesTokens(messages), measureTopLevelTokens(payload["system"], tc)),
			measureTopLevelTokens(payload["tools"], tc),
		)
	}
}

type pressureBaselineUpdateKind string

const (
	pressureBaselineUpdateNone         pressureBaselineUpdateKind = "none"
	pressureBaselineUpdateExact        pressureBaselineUpdateKind = "exact"
	pressureBaselineUpdateConservative pressureBaselineUpdateKind = "conservative_floor"
	pressureBaselineUpdateStale        pressureBaselineUpdateKind = "stale_epoch"
)

// applyPressureBaselineUsage 在 actual 与最终 forwarded 消息坐标绑定后建立精确 baseline。
// 若 forwarded body 无法解析，则拒绝写回；保留真正的 stale epoch 门禁，避免迟到
// 响应污染当前分支。返回值表示 exact actual baseline 是否被接受。
func (s *Server) applyPressureBaselineUsage(meta *requestMeta, actual int) bool {
	if meta == nil || actual <= 0 || s.Sawtooth == nil || !meta.tracksSawtoothState() || !meta.PressureDecision.Available {
		return false
	}
	stateKey := meta.HistoryStateKey
	if stateKey == "" {
		stateKey = meta.RequestSessionID
	}
	if s.HistoryEpoch != nil && meta.HistoryEpoch > 0 &&
		!s.HistoryEpoch.IsCurrent(meta.RequestSessionID, meta.HistoryEpoch) {
		// 合法的旧响应仍可记录 response_usage facts，但不能污染当前 epoch。
		meta.BaselineUpdateKind = pressureBaselineUpdateStale
		return false
	}
	decision := meta.PressureDecision
	if !decision.ForwardedCoordinatesBound {
		// 坐标未经 markForwardedPressureCoordinates 绑定过——可能是 body 不可解析，
		// 也可能是调用方根本没走绑定。两种情况都无法证明 actual 对应哪份消息坐标，
		// 绝不能沿用旧坐标或退回 conservative floor。
		meta.BaselineUpdateKind = pressureBaselineUpdateNone
		return false
	}
	updated := s.Sawtooth.UpdatePressureBaselineForRequest(stateKey, meta.BaselineGeneration, actual,
		meta.PressureEntryCoordinates.MessageCount, decision.SystemFingerprint, decision.ToolsFingerprint,
		meta.PressureEntryCoordinates.MessagesPrefixFingerprint)
	if updated {
		meta.BaselineUpdateKind = pressureBaselineUpdateExact
	}
	// estimate 与 actual 在同一响应点配对采集：只有完整响应（SSE 完整 / JSON
	// 2xx 且 usage 可解析）且坐标已绑定才会到达这里，中断路径天然不写样本。
	// baseline 因代际过期未被接受不影响配对正确性，样本照记。
	s.Sawtooth.RecordCalibrationSample(stateKey, actual, decision.ForwardedLocalEstimate)
	return updated
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

var representationBoundHeaderNames = map[string]struct{}{
	"content-length": {},
	"etag":           {},
	"digest":         {},
	"content-md5":    {},
	"content-digest": {},
	"repr-digest":    {},
	"content-range":  {},
}

// stripRepresentationHeaders 删除只对旧正文成立的 framing/validator 字段。
// codingChanged=true 时额外删除旧 Content-Encoding；其它端到端 header（认证、
// cookie、trace 等）原样保留。
func stripRepresentationHeaders(header http.Header, codingChanged bool) {
	for key := range header {
		lower := strings.ToLower(strings.TrimSpace(key))
		if _, bound := representationBoundHeaderNames[lower]; bound || (codingChanged && lower == "content-encoding") {
			delete(header, key)
		}
	}
}

func cloneResponseHeader(header http.Header) http.Header {
	if header == nil {
		return nil
	}
	clone := make(http.Header, len(header))
	for key, values := range header {
		clone[key] = append([]string(nil), values...)
	}
	return clone
}

func (s *Server) handleNon2xx(w http.ResponseWriter, resp *http.Response, meta *requestMeta, timestamp time.Time, model string, messageCount int) upstreamResponseResult {
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		recordUpstreamOutcome(meta, upstreamStateBodyReadFailure, resp.StatusCode)
		requestOutcome(meta).SetFailureClass(persistenceFailureUpstream)
		return upstreamResponseResult{err: err}
	}
	meta.Logger.Warn(eventKeyUpstreamNon2xx, "status", resp.StatusCode)
	// 非 2xx 是明确的上游失败，绝不能因为下游成功写出而记成 success。
	recordUpstreamOutcome(meta, upstreamStateNon2xx, resp.StatusCode)
	requestOutcome(meta).SetFailureClass(persistenceFailureUpstream)
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
		recordUpstreamOutcome(meta, upstreamStateUnavailable, resp.StatusCode)
		return upstreamResponseResult{err: errors.New("ResponseWriter 不支持 Flush")}
	}

	// gzip 解压（若上游返回了压缩响应）
	var bodyReader io.Reader = resp.Body
	manualGzip := !resp.Uncompressed && strings.EqualFold(strings.TrimSpace(resp.Header.Get("Content-Encoding")), "gzip")
	if manualGzip {
		gzReader, err := gzip.NewReader(resp.Body)
		if err != nil {
			recordUpstreamOutcome(meta, upstreamStateBodyReadFailure, resp.StatusCode)
			requestOutcome(meta).SetFailureClass(persistenceFailureUpstream)
			return upstreamResponseResult{err: fmt.Errorf("SSE gzip 解压失败: %w", err)}
		}
		defer gzReader.Close()
		bodyReader = gzReader
	}

	scanner := bufio.NewScanner(bodyReader)
	// 1 MB 行缓冲区处理大事件
	scanner.Buffer(make([]byte, 0, 1024*1024), 1024*1024)

	var eventBuf []string
	var fullResponse strings.Builder
	var baselineState sseBaselineState
	var downstreamErr error
	committed := false
	var downstreamHeaders http.Header
	codingChanged := manualGzip || resp.Uncompressed
	commit := func() {
		if committed {
			return
		}
		copyResponseHeaders(w.Header(), resp.Header)
		// SSE processor 会重新 framing/可能重写每个 data 行，无法在首个
		// event 前知道最终长度；保守地清理全部正文绑定字段。
		stripRepresentationHeaders(w.Header(), codingChanged)
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		downstreamHeaders = cloneResponseHeader(w.Header())
		w.WriteHeader(http.StatusOK)
		committed = true
	}
	flushEvent := func(event []string) {
		if downstreamErr != nil {
			return
		}
		baselineState.observe(event)
		commit()
		processed, _ := s.processSSEEventWithChange(event)
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

	if downstreamHeaders == nil {
		downstreamHeaders = cloneResponseHeader(w.Header())
	}
	s.writeFullBodyDebugWithSnapshots(meta, timestamp, debugBodyStageResponse, []byte(fullResponse.String()), resp.Header, downstreamHeaders, model, messageCount)
	// scanner 必须完整结束、且下游写入没有失败，才允许标记 success。
	if err := scanner.Err(); err != nil {
		recordUpstreamOutcome(meta, sseInterruptedState(committed), resp.StatusCode)
		requestOutcome(meta).SetFailureClass(persistenceFailureUpstream)
		return upstreamResponseResult{err: err, committed: committed}
	}
	if downstreamErr != nil {
		recordUpstreamOutcome(meta, sseInterruptedState(committed), resp.StatusCode)
		return upstreamResponseResult{err: downstreamErr, committed: committed}
	}
	if baselineState.complete() {
		baselineUpdated := s.applyPressureBaselineUsage(meta, baselineState.actual)
		recordAPIUsageOutcome(meta, baselineState.usage)
		s.writeUsageDebugFacts(meta, timestamp, baselineState.usage, baselineUpdated)
	}
	if !committed {
		commit()
	}
	recordUpstreamOutcome(meta, upstreamStateSuccess, resp.StatusCode)
	return upstreamResponseResult{committed: true}
}

// sseInterruptedState 区分「已经把字节交给客户端后中断」与「一个字节都没提交」。
func sseInterruptedState(committed bool) upstreamState {
	if committed {
		return upstreamStateCommittedFailure
	}
	return upstreamStateBodyReadFailure
}

// processSSEEvent 处理单个 SSE 事件。
// 识别 message_start 和 message_delta 事件中的 usage 并执行 deflation；
// message_delta 的输入侧字段在 deflate 前被剥离，只保留 output_tokens。
// 其他事件类型原样返回。JSON 解析失败时原样转发（T-02-05 graceful degradation）。
func (s *Server) processSSEEvent(event []string) []string {
	processed, _ := s.processSSEEventWithChange(event)
	return processed
}

func (s *Server) processSSEEventWithChange(event []string) ([]string, bool) {
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
		return event, false
	}

	// 解析 data JSON
	var data map[string]any
	if err := json.Unmarshal([]byte(dataLine), &data); err != nil {
		// 解析失败，原样转发（T-02-05）
		return event, false
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
			// message_delta.usage 的输入侧字段是整个请求的累计量，与 message_start
			// 重复。客户端对这三个字段是「非 null 即覆盖」，一旦上游两处报值不一致
			// （部分中转站如此），客户端的上下文计数就会在相邻请求间剧烈跳变。
			// 剥离后客户端只消费 message_start 那份自洽 usage；官方端点两处取值
			// 相同，剥离无损。
			delete(usage, "input_tokens")
			delete(usage, "cache_creation_input_tokens")
			delete(usage, "cache_read_input_tokens")
			deflateUsage(usage, factor)
		}
	}

	// 重新 marshal data 行
	newData, err := json.Marshal(data)
	if err != nil {
		return event, false
	}

	// 重建事件，替换修改后的 data 行
	result := make([]string, len(event))
	copy(result, event)
	result[dataIdx] = "data: " + string(newData)

	changed := strings.Join(result, "\n") != strings.Join(event, "\n")
	return result, changed
}

// handleJSON 处理 JSON 非流式响应。
// 2xx 响应 deflate usage，非 2xx 不修改。
func (s *Server) handleJSON(w http.ResponseWriter, resp *http.Response, meta *requestMeta, timestamp time.Time, model string, messageCount int) upstreamResponseResult {
	logger := meta.Logger
	// 只有 Transport 没有自动解压时才手动处理 gzip。resp.Uncompressed
	// 表示 Body 已是解压后的 representation，不能再次套 gzip.Reader。
	var bodyReader io.Reader = resp.Body
	manualGzip := !resp.Uncompressed && strings.EqualFold(strings.TrimSpace(resp.Header.Get("Content-Encoding")), "gzip")
	if manualGzip {
		gzReader, err := gzip.NewReader(resp.Body)
		if err != nil {
			recordUpstreamOutcome(meta, upstreamStateBodyReadFailure, resp.StatusCode)
			requestOutcome(meta).SetFailureClass(persistenceFailureUpstream)
			return upstreamResponseResult{err: fmt.Errorf("JSON gzip 解压失败: %w", err)}
		}
		defer gzReader.Close()
		bodyReader = gzReader
	}

	respBody, err := io.ReadAll(bodyReader)
	if err != nil {
		recordUpstreamOutcome(meta, upstreamStateBodyReadFailure, resp.StatusCode)
		requestOutcome(meta).SetFailureClass(persistenceFailureUpstream)
		return upstreamResponseResult{err: err}
	}
	codingChanged := manualGzip || resp.Uncompressed
	representationChanged := codingChanged

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
				recordAPIUsageOutcome(meta, usage)
				s.writeUsageDebugFacts(meta, timestamp, usage, baselineUpdated)
			}

			if usage, ok := body["usage"].(map[string]any); ok {
				deflateUsage(usage, s.Config.Proxy.Deflation)
			}
			if newBody, err := json.Marshal(body); err == nil {
				respBody = newBody
				representationChanged = true
			}
		}
	}

	copyResponseHeaders(w.Header(), resp.Header)
	if representationChanged {
		stripRepresentationHeaders(w.Header(), codingChanged)
	}
	downstreamHeaders := cloneResponseHeader(w.Header())
	w.WriteHeader(resp.StatusCode)
	n, writeErr := w.Write(respBody)
	if writeErr == nil && n != len(respBody) {
		writeErr = io.ErrShortWrite
	}

	// 步骤 5: Debug 写 JSON 响应
	s.writeFullBodyDebugWithSnapshots(meta, timestamp, debugBodyStageResponse, respBody, resp.Header, downstreamHeaders, model, messageCount)
	if writeErr != nil {
		// 响应已提交，失败只属于下游写入；绝不能记成 upstream success。
		recordUpstreamOutcome(meta, upstreamStateCommittedFailure, resp.StatusCode)
		return upstreamResponseResult{
			err:          writeErr,
			committed:    true,
			failureClass: downstreamWriteFailureClass,
		}
	}
	recordUpstreamOutcome(meta, upstreamStateSuccess, resp.StatusCode)
	return upstreamResponseResult{committed: true}
}

package proxy

import (
	"bufio"
	"compress/gzip"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
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

// forwardRaw 核心转发方法：SSE 流式 / JSON 非流式转发 + usage deflation + debug 落盘。
func (s *Server) forwardRaw(w http.ResponseWriter, r *http.Request, meta *requestMeta) {
	const maxBodySize = 10 * 1024 * 1024 // 10 MB（T-02-04）
	logger := meta.Logger

	// 步骤 1: 读取请求体
	limitedReader := io.LimitReader(r.Body, maxBodySize+1)
	body, err := io.ReadAll(limitedReader)
	r.Body.Close()
	if err != nil {
		logger.Error("读取请求体失败", "error", err)
		http.Error(w, "Failed to read request body", http.StatusInternalServerError)
		return
	}
	if len(body) > maxBodySize {
		logger.Warn("请求体超限", "size", len(body))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusRequestEntityTooLarge)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"error": "Request Entity Too Large",
		})
		return
	}

	// 提取 model 和 message_count
	model := extractModelFromBody(body)
	messageCount := countMessages(body)
	meta.logEntry(model, messageCount)

	logger.Info("上游请求发送",
		"forwarded_message_count", messageCount,
		"model", model,
	)

	timestamp := time.Now()
	// 未进入结构化压缩管线（未初始化或解析降级）时，当前 body 同时是 raw inbound。
	// 正常管线已在任何变换前写入 raw，requestMeta 的 once 会阻止最终 body 覆盖它。
	s.writeRequestDebugFacts(meta, timestamp, debugStageRawInbound, body, r)
	s.writeRequestDebugFacts(meta, timestamp, debugStageForwarded, body, r)

	// 步骤 2: Debug 写请求体。直通/解析降级路径中当前 body 同时是 raw inbound；
	// 正常结构化管线已提前写入真正的 raw，once 会阻止被最终 body 覆盖。
	s.writeFullBodyDebug(meta, timestamp, debugBodyStageRawInbound, body, r.Header, model, messageCount)
	s.writeFullBodyDebug(meta, timestamp, debugBodyStageForwarded, body, r.Header, model, messageCount)

	// 步骤 3: 创建上游请求
	targetURL := strings.TrimRight(s.Config.Proxy.Target, "/") + r.URL.RequestURI()
	upstreamReq, err := http.NewRequestWithContext(r.Context(), r.Method, targetURL, strings.NewReader(string(body)))
	if err != nil {
		logger.Error("创建上游请求失败", "error", err)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadGateway)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"error":  "Bad Gateway",
			"detail": "failed to create upstream request",
		})
		return
	}

	// 复制 headers（包括 Authorization）到上游请求
	for key, values := range r.Header {
		for _, value := range values {
			upstreamReq.Header.Add(key, value)
		}
	}
	if upstreamReq.Header.Get("Content-Type") == "" {
		upstreamReq.Header.Set("Content-Type", "application/json")
	}

	// 删除 Accept-Encoding，强制上游返回未压缩响应
	upstreamReq.Header.Del("Accept-Encoding")

	// 发送上游请求
	resp, err := s.HTTPClient.Do(upstreamReq)
	if err != nil {
		// 网络层错误 → 502 Bad Gateway（D-09）
		logger.Error("上游请求失败", "error", err)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadGateway)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"error":  "Bad Gateway",
			"detail": err.Error(),
		})
		return
	}
	defer resp.Body.Close()

	// 步骤 4: 处理响应
	contentType := resp.Header.Get("Content-Type")

	// 非 2xx: 透传状态码 + body，记录 warn（D-08）
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		logger.Warn("上游返回非 2xx",
			"status", resp.StatusCode,
		)

		// 复制响应 headers
		for key, values := range resp.Header {
			for _, value := range values {
				w.Header().Add(key, value)
			}
		}
		w.WriteHeader(resp.StatusCode)

		respBody, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			logger.Warn("读取非 2xx 响应体失败", "error", readErr)
		}
		_, _ = w.Write(respBody)

		// Debug 写非 2xx 响应
		s.writeFullBodyDebug(meta, timestamp, debugBodyStageResponse, respBody, resp.Header, model, messageCount)
		return
	}

	// 判定流式/非流式
	if strings.Contains(contentType, "text/event-stream") {
		s.handleSSE(w, resp, meta, timestamp, model, messageCount)
	} else {
		s.handleJSON(w, resp, meta, timestamp, model, messageCount)
	}
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

// handleSSE 处理 SSE 流式响应。
// 逐行扫描，对 message_start 和 message_delta 事件执行 usage deflation，
// 其他事件原样转发。每个事件后 flush。
func (s *Server) handleSSE(w http.ResponseWriter, resp *http.Response, meta *requestMeta, timestamp time.Time, model string, messageCount int) {
	sessionID := meta.RequestSessionID
	logger := meta.Logger
	flusher, ok := w.(http.Flusher)
	if !ok {
		logger.Error("ResponseWriter 不支持 Flush")
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	// gzip 解压（若上游返回了压缩响应）
	var bodyReader io.Reader = resp.Body
	if resp.Header.Get("Content-Encoding") == "gzip" {
		gzReader, err := gzip.NewReader(resp.Body)
		if err != nil {
			logger.Error("gzip 解压失败", "error", err)
			http.Error(w, "Failed to decompress SSE stream", http.StatusBadGateway)
			return
		}
		defer gzReader.Close()
		bodyReader = gzReader
	}

	w.WriteHeader(http.StatusOK)

	scanner := bufio.NewScanner(bodyReader)
	// 1 MB 行缓冲区处理大事件
	scanner.Buffer(make([]byte, 0, 1024*1024), 1024*1024)

	var eventBuf []string
	var fullResponse strings.Builder
	usageRecorded := false

	for scanner.Scan() {
		line := scanner.Text()

		// message_start 的 usage 在 processSSEEvent deflation 前更新 Sawtooth。
		if !usageRecorded &&
			strings.HasPrefix(line, "data:") && strings.Contains(line, "message_start") {
			dataStr := strings.TrimSpace(line[5:])
			var data map[string]any
			if json.Unmarshal([]byte(dataStr), &data) == nil {
				if msg, ok := data["message"].(map[string]any); ok {
					if usage, ok := msg["usage"].(map[string]any); ok {
						s.writeUsageDebugFacts(meta, timestamp, usage)
						if s.Sawtooth != nil {
							s.Sawtooth.UpdateAfterResponse(sessionID, totalInputTokens(usage), messageCount)
						}
						usageRecorded = true
					}
				}
			}
		}

		if line == "" {
			// 空行 = 事件边界，flush 整个事件
			if len(eventBuf) > 0 {
				processed := s.processSSEEvent(eventBuf)
				for _, l := range processed {
					fmt.Fprintf(w, "%s\n", l)
					fullResponse.WriteString(l)
					fullResponse.WriteString("\n")
				}
				fmt.Fprintf(w, "\n")
				fullResponse.WriteString("\n")
				flusher.Flush()
				eventBuf = eventBuf[:0]
			}
			continue
		}
		eventBuf = append(eventBuf, line)
	}

	// 处理末尾可能遗漏的事件（无尾随空行）
	if len(eventBuf) > 0 {
		processed := s.processSSEEvent(eventBuf)
		for _, l := range processed {
			fmt.Fprintf(w, "%s\n", l)
			fullResponse.WriteString(l)
			fullResponse.WriteString("\n")
		}
		fmt.Fprintf(w, "\n")
		fullResponse.WriteString("\n")
		flusher.Flush()
	}

	if err := scanner.Err(); err != nil {
		logger.Warn("SSE 流读取错误", "error", err)
	}

	// 步骤 5: Debug 写 SSE 响应（完整拼接文本）
	s.writeFullBodyDebug(meta, timestamp, debugBodyStageResponse, []byte(fullResponse.String()), resp.Header, model, messageCount)
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
func (s *Server) handleJSON(w http.ResponseWriter, resp *http.Response, meta *requestMeta, timestamp time.Time, model string, messageCount int) {
	sessionID := meta.RequestSessionID
	logger := meta.Logger
	// gzip 解压（若上游返回了压缩响应）
	var bodyReader io.Reader = resp.Body
	if resp.Header.Get("Content-Encoding") == "gzip" {
		gzReader, err := gzip.NewReader(resp.Body)
		if err != nil {
			logger.Error("gzip 解压失败", "error", err)
			http.Error(w, "Failed to decompress response", http.StatusBadGateway)
			return
		}
		defer gzReader.Close()
		bodyReader = gzReader
		// 解压后删除 Content-Encoding 和 Content-Length，防止泄漏给客户端
		resp.Header.Del("Content-Encoding")
		resp.Header.Del("Content-Length")
	}

	respBody, err := io.ReadAll(bodyReader)
	if err != nil {
		logger.Error("读取上游 JSON 响应失败", "error", err)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadGateway)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"error":  "Bad Gateway",
			"detail": "failed to read upstream response",
		})
		return
	}

	// 2xx: deflate usage
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		var body map[string]any
		if err := json.Unmarshal(respBody, &body); err != nil {
			// 解析失败，原样转发（T-02-05）
			logger.Warn("无法解析上游 JSON 响应，原样转发", "error", err)
		} else {
			// 在客户端 usage deflation 之前，以完整输入空间更新 Sawtooth。
			if s.Sawtooth != nil {
				if usage, ok := body["usage"].(map[string]any); ok {
					s.writeUsageDebugFacts(meta, timestamp, usage)
					s.Sawtooth.UpdateAfterResponse(sessionID, totalInputTokens(usage), messageCount)
				}
			} else if usage, ok := body["usage"].(map[string]any); ok {
				s.writeUsageDebugFacts(meta, timestamp, usage)
			}

			if usage, ok := body["usage"].(map[string]any); ok {
				deflateUsage(usage, s.Config.Proxy.Deflation)
			}
			if newBody, err := json.Marshal(body); err == nil {
				respBody = newBody
			}
		}
	}

	// 复制响应 headers
	for key, values := range resp.Header {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = w.Write(respBody)

	// 步骤 5: Debug 写 JSON 响应
	s.writeFullBodyDebug(meta, timestamp, debugBodyStageResponse, respBody, resp.Header, model, messageCount)
}

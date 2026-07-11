package proxy

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type debugStage string

const (
	debugStageRawInbound    debugStage = "raw_inbound"
	debugStageForwarded     debugStage = "forwarded"
	debugStageResponseUsage debugStage = "response_usage"
)

type debugError string

const (
	debugErrorInvalidJSON        debugError = "invalid_json"
	debugErrorInvalidMessages    debugError = "invalid_messages"
	debugErrorInvalidMediaBase64 debugError = "invalid_media_base64"
)

const maxDebugBase64Chars = 8 * 1024 * 1024

// debugFact 是唯一允许写入默认 debug 文件的结构。
// 所有字段均为时间、受限枚举、bool 或数字，不持有 header、正文或 session ID。
type debugFact struct {
	Timestamp                string                    `json:"timestamp"`
	RequestID                uint64                    `json:"request_id"`
	Stage                    debugStage                `json:"stage"`
	ModelFamily              agentModelFamily          `json:"model_family"`
	MessageCount             int                       `json:"message_count"`
	HasClaudeMDContext       bool                      `json:"has_claude_md_context"`
	ImageCount               int                       `json:"image_count"`
	DocumentCount            int                       `json:"document_count"`
	DecodedByteCount         int                       `json:"decoded_byte_count"`
	EstimatedTokens          int                       `json:"estimated_tokens"`
	AgentRole                agentRole                 `json:"agent_role"`
	AgentReason              agentClassificationReason `json:"agent_reason"`
	InputTokens              int                       `json:"input_tokens"`
	CacheCreationInputTokens int                       `json:"cache_creation_input_tokens"`
	CacheReadInputTokens     int                       `json:"cache_read_input_tokens"`
	TotalInputTokens         int                       `json:"total_input_tokens"`
	Error                    debugError                `json:"error"`
}

func (s *Server) writeRequestDebugFacts(meta *requestMeta, timestamp time.Time, stage debugStage, body []byte, request *http.Request) {
	if !s.Config.Debug.Enabled || meta == nil {
		return
	}
	once := meta.debugOnce(stage)
	if once == nil {
		return
	}
	once.Do(func() {
		fact := debugFact{
			Timestamp:   timestamp.UTC().Format(time.RFC3339Nano),
			RequestID:   meta.ID,
			Stage:       stage,
			ModelFamily: agentModelFamilyUnknown,
			AgentRole:   agentRoleUnknown,
		}
		var bodyMap map[string]json.RawMessage
		if err := json.Unmarshal(body, &bodyMap); err != nil {
			fact.Error = debugErrorInvalidJSON
			s.writeDebugFact(meta.RequestSessionID, timestamp, fact)
			return
		}
		fact.ModelFamily = extractAgentModelFamily(bodyMap["model"])
		var messages []Message
		if raw, ok := bodyMap["messages"]; ok {
			if err := json.Unmarshal(raw, &messages); err != nil {
				fact.Error = debugErrorInvalidMessages
			}
		}
		fact.MessageCount = len(messages)
		fact.HasClaudeMDContext = ExtractPersistentUserContext(messages) != nil
		if s.TokenCounter != nil {
			fact.EstimatedTokens = s.TokenCounter.CountMessagesTokens(messages)
		}
		var invalidMediaBase64 bool
		fact.ImageCount, fact.DocumentCount, fact.DecodedByteCount, invalidMediaBase64 = debugMediaFacts(messages)
		if invalidMediaBase64 {
			fact.DecodedByteCount = 0
			if fact.Error == "" {
				fact.Error = debugErrorInvalidMediaBase64
			}
		}
		classification := classifyAgentRequest(request, bodyMap, messages)
		fact.AgentRole = classification.Role
		fact.AgentReason = classification.Reason
		s.writeDebugFact(meta.RequestSessionID, timestamp, fact)
	})
}

func (s *Server) writeUsageDebugFacts(meta *requestMeta, timestamp time.Time, usage map[string]any) {
	if !s.Config.Debug.Enabled || meta == nil {
		return
	}
	meta.usageFactsOnce.Do(func() {
		fact := debugFact{
			Timestamp:                timestamp.UTC().Format(time.RFC3339Nano),
			RequestID:                meta.ID,
			Stage:                    debugStageResponseUsage,
			ModelFamily:              agentModelFamilyUnknown,
			AgentRole:                agentRoleUnknown,
			InputTokens:              nonNegativeUsageToken(usage["input_tokens"]),
			CacheCreationInputTokens: nonNegativeUsageToken(usage["cache_creation_input_tokens"]),
			CacheReadInputTokens:     nonNegativeUsageToken(usage["cache_read_input_tokens"]),
			TotalInputTokens:         totalInputTokens(usage),
		}
		s.writeDebugFact(meta.RequestSessionID, timestamp, fact)
	})
}

func (s *Server) writeDebugFact(sessionID string, timestamp time.Time, fact debugFact) {
	debugDir, ok := safeDebugSessionDir(s.Config.Debug.DataDir, sessionID)
	if !ok {
		slog.Warn("debug facts session 目录校验失败")
		return
	}
	if err := os.MkdirAll(debugDir, 0755); err != nil {
		slog.Warn("无法创建 debug facts 目录", "error", err)
		return
	}
	data, err := json.Marshal(fact)
	if err != nil {
		slog.Warn("无法序列化 debug facts", "error", err)
		return
	}
	filename := fmt.Sprintf("%s-%d-%s-facts.json", timestamp.Format("2006-01-02T150405.000000000"), fact.RequestID, fact.Stage)
	filePath := filepath.Join(debugDir, filename)
	if err := writeDebugEntryFile(filePath, data, func(name string, flag int, perm os.FileMode) (debugWriteCloser, error) {
		return os.OpenFile(name, flag, perm)
	}); err != nil {
		slog.Warn("无法写入 debug facts", "stage", fact.Stage, "error", err)
	}
}

func debugMediaFacts(messages []Message) (images, documents, decodedBytes int, invalidBase64 bool) {
	for _, message := range messages {
		var content any
		if json.Unmarshal(message.Content, &content) == nil {
			i, d, b, invalid := debugMediaValueFacts(content)
			images += i
			documents += d
			decodedBytes = saturatingAdd(decodedBytes, b)
			invalidBase64 = invalidBase64 || invalid
		}
	}
	return images, documents, decodedBytes, invalidBase64
}

func debugMediaValueFacts(value any) (images, documents, decodedBytes int, invalidBase64 bool) {
	switch typed := value.(type) {
	case []any:
		for _, item := range typed {
			i, d, b, invalid := debugMediaValueFacts(item)
			images += i
			documents += d
			decodedBytes = saturatingAdd(decodedBytes, b)
			invalidBase64 = invalidBase64 || invalid
		}
	case map[string]any:
		typeName, _ := typed["type"].(string)
		if typeName == "image" || typeName == "document" {
			_, data, ok := semanticBlockSource(typed)
			if ok {
				if typeName == "image" {
					images++
				} else {
					documents++
				}
				if size, valid := validatedDecodedBase64Size(data); valid {
					decodedBytes = saturatingAdd(decodedBytes, size)
				} else {
					invalidBase64 = true
				}
			}
		}
		if content, ok := typed["content"]; ok {
			i, d, b, invalid := debugMediaValueFacts(content)
			images += i
			documents += d
			decodedBytes = saturatingAdd(decodedBytes, b)
			invalidBase64 = invalidBase64 || invalid
		}
	}
	return images, documents, decodedBytes, invalidBase64
}

func decodedBase64Size(data string) int {
	size, _ := validatedDecodedBase64Size(data)
	return size
}

// validatedDecodedBase64Size 对受限长度的标准 base64 执行流式严格解码。
// 只累计解码字节数，不保留 payload；空白、非法 padding 和超限输入均拒绝。
func validatedDecodedBase64Size(data string) (int, bool) {
	if data == "" || len(data) > maxDebugBase64Chars {
		return 0, false
	}
	if strings.IndexAny(data, " \t\r\n") >= 0 {
		return 0, false
	}
	decoder := base64.NewDecoder(base64.StdEncoding.Strict(), strings.NewReader(data))
	decoded, err := io.CopyBuffer(io.Discard, decoder, make([]byte, 4096))
	if err != nil || decoded < 0 || uint64(decoded) > uint64(^uint(0)>>1) {
		return 0, false
	}
	return int(decoded), true
}

func saturatingAdd(left, right int) int {
	if right > 0 && left > int(^uint(0)>>1)-right {
		return int(^uint(0) >> 1)
	}
	return left + right
}

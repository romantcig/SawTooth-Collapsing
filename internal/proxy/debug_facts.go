package proxy

import (
	"encoding/base64"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

type debugStage string

const (
	debugStageRawInbound       debugStage = "raw_inbound"
	debugStageForwarded        debugStage = "forwarded"
	debugStagePressureDecision debugStage = "pressure_decision"
	debugStageResponseUsage    debugStage = "response_usage"
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
	Timestamp                 string                      `json:"timestamp"`
	RequestID                 uint64                      `json:"request_id"`
	Stage                     debugStage                  `json:"stage"`
	ModelFamily               agentModelFamily            `json:"model_family"`
	MessageCount              int                         `json:"message_count"`
	HasClaudeMDContext        bool                        `json:"has_claude_md_context"`
	ImageCount                int                         `json:"image_count"`
	DocumentCount             int                         `json:"document_count"`
	DecodedByteCount          int                         `json:"decoded_byte_count"`
	EstimatedTokens           int                         `json:"estimated_tokens"`
	AgentRole                 agentRole                   `json:"agent_role"`
	AgentReason               agentClassificationReason   `json:"agent_reason"`
	InputTokens               int                         `json:"input_tokens"`
	CacheCreationInputTokens  int                         `json:"cache_creation_input_tokens"`
	CacheReadInputTokens      int                         `json:"cache_read_input_tokens"`
	TotalInputTokens          int                         `json:"total_input_tokens"`
	HistoryEpoch              *uint64                     `json:"history_epoch,omitempty"`
	HistoryCommonPrefix       *int                        `json:"history_common_prefix,omitempty"`
	HistoryTransitionReason   *HistoryEpochReason         `json:"history_transition_reason,omitempty"`
	HistoryEpochChanged       *bool                       `json:"history_epoch_changed,omitempty"`
	HistoryMismatchFirst      *bool                       `json:"history_mismatch_first,omitempty"`
	HistoryTransitionFailed   *bool                       `json:"history_transition_failed,omitempty"`
	MessagesLocalTokens       *int                        `json:"messages_local_tokens,omitempty"`
	SystemLocalTokens         *int                        `json:"system_local_tokens,omitempty"`
	ToolsLocalTokens          *int                        `json:"tools_local_tokens,omitempty"`
	FullLocalTokens           *int                        `json:"full_local_tokens,omitempty"`
	PreviousActualTokens      *int                        `json:"previous_actual_tokens,omitempty"`
	PreviousMessageCount      *int                        `json:"previous_message_count,omitempty"`
	NewMessageDeltaTokens     *int                        `json:"new_message_delta_tokens,omitempty"`
	SelectedPressureTokens    *int                        `json:"selected_pressure_tokens,omitempty"`
	PressureThresholdTokens   *int                        `json:"pressure_threshold_tokens,omitempty"`
	PressureSource            *pressureSource             `json:"pressure_source,omitempty"`
	TriggerReason             *TriggerReason              `json:"trigger_reason,omitempty"`
	BaselineResetReason       *baselineResetReason        `json:"baseline_reset_reason,omitempty"`
	CompressDecision          *bool                       `json:"compress_decision,omitempty"`
	SystemFingerprintChanged  *bool                       `json:"system_fingerprint_changed,omitempty"`
	ToolsFingerprintChanged   *bool                       `json:"tools_fingerprint_changed,omitempty"`
	BaselineUpdated           *bool                       `json:"baseline_updated,omitempty"`
	BaselineUpdateKind        *pressureBaselineUpdateKind `json:"baseline_update_kind,omitempty"`
	ActualMinusSelectedTokens *int                        `json:"actual_minus_selected_tokens,omitempty"`
	Error                     debugError                  `json:"error"`
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
		populateHistoryDebugFact(meta, &fact)
		var bodyMap map[string]json.RawMessage
		if err := json.Unmarshal(body, &bodyMap); err != nil {
			fact.Error = debugErrorInvalidJSON
			s.writeDebugFact(meta, fact)
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
		s.writeDebugFact(meta, fact)
	})
}

func (s *Server) writePressureDecisionDebugFacts(meta *requestMeta, timestamp time.Time) {
	if !s.Config.Debug.Enabled || meta == nil || !meta.tracksSawtoothState() || !meta.PressureDecision.Available {
		return
	}
	meta.pressureFactsOnce.Do(func() {
		decision := meta.PressureDecision
		fact := debugFact{
			Timestamp:                timestamp.UTC().Format(time.RFC3339Nano),
			RequestID:                meta.ID,
			Stage:                    debugStagePressureDecision,
			ModelFamily:              agentModelFamilyUnknown,
			AgentRole:                meta.AgentRole,
			AgentReason:              meta.AgentReason,
			MessagesLocalTokens:      &decision.MessagesLocalTokens,
			SystemLocalTokens:        &decision.SystemLocalTokens,
			ToolsLocalTokens:         &decision.ToolsLocalTokens,
			FullLocalTokens:          &decision.FullLocalEstimate,
			PreviousActualTokens:     &decision.PreviousActual,
			PreviousMessageCount:     &decision.PreviousMessageCount,
			NewMessageDeltaTokens:    &decision.NewMessageDelta,
			SelectedPressureTokens:   &decision.SelectedPressure,
			PressureThresholdTokens:  &decision.Threshold,
			PressureSource:           &decision.Source,
			TriggerReason:            &decision.TriggerReason,
			BaselineResetReason:      &decision.ResetReason,
			CompressDecision:         &decision.CompressDecision,
			SystemFingerprintChanged: &decision.SystemFingerprintChanged,
			ToolsFingerprintChanged:  &decision.ToolsFingerprintChanged,
		}
		populateHistoryDebugFact(meta, &fact)
		s.writeDebugFact(meta, fact)
	})
}

func (s *Server) writeUsageDebugFacts(meta *requestMeta, timestamp time.Time, usage map[string]any, baselineUpdated bool) {
	if meta == nil {
		return
	}
	actual := totalInputTokens(usage)
	if !s.Config.Debug.Enabled {
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
			TotalInputTokens:         actual,
			BaselineUpdated:          &baselineUpdated,
		}
		baselineUpdateKind := meta.BaselineUpdateKind
		if baselineUpdateKind == "" {
			baselineUpdateKind = pressureBaselineUpdateNone
			if baselineUpdated {
				baselineUpdateKind = pressureBaselineUpdateExact
			}
		}
		fact.BaselineUpdateKind = &baselineUpdateKind
		if meta.tracksSawtoothState() && meta.PressureDecision.Available {
			actualMinusSelected := saturatingSubtract(actual, meta.PressureDecision.SelectedPressure)
			fact.ActualMinusSelectedTokens = &actualMinusSelected
		}
		populateHistoryDebugFact(meta, &fact)
		s.writeDebugFact(meta, fact)
	})
}

// populateHistoryDebugFact 只把 request-scoped 数字、布尔值和受限枚举复制到
// facts。HistoryStateKey、session hash、canonical hash 与 dedup digest 均不在接口中。
func populateHistoryDebugFact(meta *requestMeta, fact *debugFact) {
	if meta == nil || fact == nil || meta.HistoryEpoch == 0 {
		return
	}
	epoch := meta.HistoryEpoch
	fact.HistoryEpoch = &epoch
	if meta.HistoryCommonPrefix >= 0 {
		commonPrefix := meta.HistoryCommonPrefix
		fact.HistoryCommonPrefix = &commonPrefix
	}
	if validHistoryEpochReason(meta.HistoryEpochReason) {
		reason := meta.HistoryEpochReason
		fact.HistoryTransitionReason = &reason
	}
	epochChanged := meta.HistoryEpochChanged
	fact.HistoryEpochChanged = &epochChanged
	if meta.HistoryMismatch {
		first := meta.HistoryMismatchFirst
		fact.HistoryMismatchFirst = &first
	}
	if meta.HistoryTransitionFailed {
		failed := true
		fact.HistoryTransitionFailed = &failed
	}
}

func debugFactArtifactStage(stage debugStage) (debugArtifactStage, bool) {
	switch stage {
	case debugStageRawInbound:
		return debugArtifactRawMetadata, true
	case debugStageForwarded:
		return debugArtifactForwardedMeta, true
	case debugStagePressureDecision:
		return debugArtifactPressure, true
	case debugStageResponseUsage:
		return debugArtifactUsage, true
	default:
		return "", false
	}
}

func (s *Server) writeDebugFact(meta *requestMeta, fact debugFact) {
	stage, ok := debugFactArtifactStage(fact.Stage)
	if !ok {
		slog.Warn("debug facts stage 无效", "stage", fact.Stage)
		return
	}
	routingMeta, err := s.debugMetaForWrite(meta)
	if err != nil {
		slog.Warn("debug facts 元数据校验失败", "stage", fact.Stage, "error", err)
		return
	}
	data, err := json.Marshal(fact)
	if err != nil {
		slog.Warn("无法序列化 debug facts", "error", err)
		return
	}
	if err := s.debugLayout.Write(routingMeta, stage, data); err != nil {
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

func saturatingSubtract(left, right int) int {
	maxInt := int(^uint(0) >> 1)
	minInt := -maxInt - 1
	if right > 0 && left < minInt+right {
		return minInt
	}
	if right < 0 && left > maxInt+right {
		return maxInt
	}
	return left - right
}

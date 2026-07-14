package proxy

import (
	"encoding/json"
	"log/slog"
	"strings"
	"unicode"
	"unicode/utf8"
)

type requestKind string

const (
	requestKindNormal       requestKind = ""
	requestKindSessionTitle requestKind = "session_title"
)

type auxiliaryReason string

const (
	auxiliaryReasonNone                auxiliaryReason = ""
	auxiliaryReasonTitleSchema         auxiliaryReason = "title_schema"
	auxiliaryReasonTitlePromptFallback auxiliaryReason = "title_prompt_fallback"
)

type auxiliaryClassification struct {
	Kind   requestKind
	Reason auxiliaryReason
}

type auxiliaryOutputConfigState uint8

const (
	auxiliaryOutputConfigMissing auxiliaryOutputConfigState = iota
	auxiliaryOutputConfigTitleOnly
	auxiliaryOutputConfigInvalid
)

func classifyAuxiliaryRequest(bodyMap map[string]json.RawMessage, messages []Message) auxiliaryClassification {
	if len(messages) != 1 || messages[0].Role != "user" {
		return auxiliaryClassification{}
	}
	userText, ok := extractAuxiliaryText(messages[0].Content)
	if !ok {
		return auxiliaryClassification{}
	}
	if _, ok := splitSessionTitleEnvelope(userText); !ok {
		return auxiliaryClassification{}
	}
	systemText, ok := extractAuxiliaryText(bodyMap["system"])
	if !ok || !isSessionTitleSystem(systemText) {
		return auxiliaryClassification{}
	}

	switch inspectAuxiliaryOutputConfig(bodyMap) {
	case auxiliaryOutputConfigMissing:
		return auxiliaryClassification{Kind: requestKindSessionTitle, Reason: auxiliaryReasonTitlePromptFallback}
	case auxiliaryOutputConfigTitleOnly:
		return auxiliaryClassification{Kind: requestKindSessionTitle, Reason: auxiliaryReasonTitleSchema}
	default:
		return auxiliaryClassification{}
	}
}

func extractAuxiliaryText(raw json.RawMessage) (string, bool) {
	if len(raw) == 0 {
		return "", false
	}
	var text string
	if json.Unmarshal(raw, &text) == nil {
		return text, true
	}

	var blocks []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if json.Unmarshal(raw, &blocks) != nil || len(blocks) == 0 {
		return "", false
	}
	parts := make([]string, 0, len(blocks))
	for _, block := range blocks {
		if block.Type != "text" {
			return "", false
		}
		parts = append(parts, block.Text)
	}
	return strings.Join(parts, "\n"), true
}

const (
	sessionTitleDefaultLanguageInstruction = "Write the title in the predominant language of the session — a stray word or code token in another language doesn't change it. Ignore the language of the examples above."
	sessionTitleLanguagePrefix             = "Write the title in "
	sessionTitleLanguageSuffix             = ". Keep technical terms and code identifiers in their original form."
)

// splitSessionTitleEnvelope validates the single session envelope and returns
// its optional, exact 2.1.207 language instruction. It deliberately never
// exposes or interprets the envelope body.
func splitSessionTitleEnvelope(text string) (string, bool) {
	const (
		openTag         = "<session>"
		closeTag        = "</session>"
		maxSessionDepth = 32
	)

	trimmed := strings.TrimSpace(text)
	if !strings.HasPrefix(trimmed, openTag) {
		return "", false
	}

	depth := 1
	position := len(openTag)
	for position < len(trimmed) {
		nextOpen := strings.Index(trimmed[position:], openTag)
		nextClose := strings.Index(trimmed[position:], closeTag)
		if nextClose < 0 {
			return "", false
		}

		if nextOpen >= 0 && nextOpen < nextClose {
			depth++
			if depth > maxSessionDepth {
				return "", false
			}
			position += nextOpen + len(openTag)
			continue
		}

		depth--
		if depth < 0 {
			return "", false
		}
		position += nextClose + len(closeTag)
		if depth == 0 {
			suffix := strings.TrimSpace(trimmed[position:])
			if suffix == "" {
				return "", true
			}
			if !isSessionTitleLanguageInstruction(suffix) {
				return "", false
			}
			return suffix, true
		}
	}

	return "", false
}

func isSessionTitleLanguageInstruction(text string) bool {
	if text == sessionTitleDefaultLanguageInstruction {
		return true
	}
	if !strings.HasPrefix(text, sessionTitleLanguagePrefix) || !strings.HasSuffix(text, sessionTitleLanguageSuffix) {
		return false
	}
	language := strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(text, sessionTitleLanguagePrefix), sessionTitleLanguageSuffix))
	if language == "" || utf8.RuneCountInString(language) > 64 {
		return false
	}
	for _, r := range language {
		if r == '<' || r == '>' || unicode.IsControl(r) {
			return false
		}
	}
	return true
}

func isSessionTitleSystem(text string) bool {
	lower := strings.ToLower(text)
	hasSessionIntent := strings.Contains(lower, "coding session") || strings.Contains(lower, "session title")
	hasTitleAction := strings.Contains(lower, "generate") && strings.Contains(lower, "title")
	hasShortConstraint := strings.Contains(lower, "3-7 words") || strings.Contains(lower, "sentence-case") || strings.Contains(lower, "sentence case")
	hasSingleTitleField := strings.Contains(lower, "single \"title\" field") || strings.Contains(lower, "single title field")
	return hasSessionIntent && hasTitleAction && hasShortConstraint && hasSingleTitleField
}

func inspectAuxiliaryOutputConfig(bodyMap map[string]json.RawMessage) auxiliaryOutputConfigState {
	raw, present := bodyMap["output_config"]
	if !present {
		return auxiliaryOutputConfigMissing
	}
	if len(raw) == 0 || strings.TrimSpace(string(raw)) == "null" {
		return auxiliaryOutputConfigInvalid
	}

	var outputConfig struct {
		Format struct {
			Type   string `json:"type"`
			Schema struct {
				Type                 string                     `json:"type"`
				Properties           map[string]json.RawMessage `json:"properties"`
				Required             []string                   `json:"required"`
				AdditionalProperties *bool                      `json:"additionalProperties"`
			} `json:"schema"`
		} `json:"format"`
	}
	if json.Unmarshal(raw, &outputConfig) != nil || outputConfig.Format.Type != "json_schema" {
		return auxiliaryOutputConfigInvalid
	}
	schema := outputConfig.Format.Schema
	if schema.Type != "object" || len(schema.Properties) != 1 || len(schema.Required) != 1 ||
		schema.Required[0] != "title" || schema.AdditionalProperties == nil || *schema.AdditionalProperties {
		return auxiliaryOutputConfigInvalid
	}
	titleRaw, ok := schema.Properties["title"]
	if !ok {
		return auxiliaryOutputConfigInvalid
	}
	var titleProperty struct {
		Type string `json:"type"`
	}
	if json.Unmarshal(titleRaw, &titleProperty) != nil || titleProperty.Type != "string" {
		return auxiliaryOutputConfigInvalid
	}
	return auxiliaryOutputConfigTitleOnly
}

func logAuxiliaryClassification(logger *slog.Logger, classification auxiliaryClassification, messageCount int) {
	if logger == nil {
		logger = slog.Default()
	}
	logger.Info("辅助请求安全直通",
		"request_kind", classification.Kind,
		"request_reason", classification.Reason,
		"message_count", messageCount,
	)
}

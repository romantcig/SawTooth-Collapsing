package proxy

import (
	"encoding/json"
	"strings"
)

// PersistentUserContext 是当前 raw inbound 中最早出现的权威持久上下文。
// Message 复用来源消息的角色和所有未知消息级字段，Content 只包含命中的 reminder。
type PersistentUserContext struct {
	Message Message
}

// ExtractPersistentUserContext 返回最早的完整持久 context，不修改输入。
func ExtractPersistentUserContext(messages []Message) *PersistentUserContext {
	_, context := detachPersistentUserContext(messages)
	return context
}

// DetachPersistentUserContext 从 history 中移除所有明确识别的持久 context。
// 最早候选作为本轮权威 context 返回；畸形或无法分类的 reminder 原样保留。
func DetachPersistentUserContext(messages []Message) ([]Message, *PersistentUserContext) {
	return detachPersistentUserContext(messages)
}

// PrependPersistentUserContext 将权威 context 恰好一次放在 history 首部。
// history 中若仍含明确的旧 context，会先被移除；未知候选保持不变。
func PrependPersistentUserContext(history []Message, context *PersistentUserContext) []Message {
	if context == nil {
		return append([]Message(nil), history...)
	}
	detached, _ := detachPersistentUserContext(history)
	contextMessage := context.Message
	if copied := deepCopyMessages([]Message{context.Message}); len(copied) == 1 {
		contextMessage = copied[0]
	}
	result := make([]Message, 0, len(detached)+1)
	result = append(result, contextMessage)
	result = append(result, detached...)
	return result
}

func detachPersistentUserContext(messages []Message) ([]Message, *PersistentUserContext) {
	history := make([]Message, 0, len(messages))
	var authoritative *PersistentUserContext
	for _, message := range messages {
		cleaned, keep, candidates := detachPersistentFromMessage(message)
		if authoritative == nil && len(candidates) > 0 {
			authoritative = &PersistentUserContext{Message: candidates[0]}
		}
		if keep {
			history = append(history, cleaned)
		}
	}
	return history, authoritative
}

func detachPersistentFromMessage(message Message) (Message, bool, []Message) {
	if len(message.Content) == 0 {
		return message, true, nil
	}

	var text string
	if err := json.Unmarshal(message.Content, &text); err == nil {
		cleaned, matches := splitPersistentReminders(text)
		if len(matches) == 0 {
			return message, true, nil
		}
		candidates := make([]Message, 0, len(matches))
		for _, match := range matches {
			candidate := message
			candidate.Content, _ = json.Marshal(match)
			candidates = append(candidates, candidate)
		}
		if strings.TrimSpace(cleaned) == "" {
			return Message{}, false, candidates
		}
		result := message
		result.Content, _ = json.Marshal(cleaned)
		return result, true, candidates
	}

	var blocks []json.RawMessage
	if err := json.Unmarshal(message.Content, &blocks); err != nil {
		return message, true, nil
	}
	resultBlocks := make([]json.RawMessage, 0, len(blocks))
	var candidates []Message
	found := false
	for _, rawBlock := range blocks {
		blockType, blockText, ok := rawTextBlock(rawBlock)
		if !ok || blockType != "text" {
			resultBlocks = append(resultBlocks, rawBlock)
			continue
		}
		cleaned, matches := splitPersistentReminders(blockText)
		if len(matches) == 0 {
			resultBlocks = append(resultBlocks, rawBlock)
			continue
		}
		found = true
		for _, match := range matches {
			candidateBlock, replaceOK := replaceRawTextBlock(rawBlock, match)
			if !replaceOK {
				continue
			}
			candidate := message
			candidate.Content, _ = json.Marshal([]json.RawMessage{candidateBlock})
			candidates = append(candidates, candidate)
		}
		if strings.TrimSpace(cleaned) != "" {
			cleanedBlock, replaceOK := replaceRawTextBlock(rawBlock, cleaned)
			if !replaceOK {
				return message, true, nil
			}
			resultBlocks = append(resultBlocks, cleanedBlock)
		}
	}
	if !found {
		return message, true, nil
	}
	if len(resultBlocks) == 0 {
		return Message{}, false, candidates
	}
	result := message
	result.Content, _ = json.Marshal(resultBlocks)
	return result, true, candidates
}

func splitPersistentReminders(text string) (string, []string) {
	indices := reminderPattern.FindAllStringSubmatchIndex(text, -1)
	if len(indices) == 0 {
		return text, nil
	}
	var cleaned strings.Builder
	cleaned.Grow(len(text))
	last := 0
	var matches []string
	for _, index := range indices {
		if len(index) < 4 || index[2] < 0 || index[3] < 0 {
			continue
		}
		if !hasPersistentUserContextHeading(text[index[2]:index[3]]) {
			continue
		}
		cleaned.WriteString(text[last:index[0]])
		matches = append(matches, text[index[0]:index[1]])
		last = index[1]
	}
	if len(matches) == 0 {
		return text, nil
	}
	cleaned.WriteString(text[last:])
	return cleaned.String(), matches
}

func hasPersistentUserContextHeading(body string) bool {
	for _, line := range strings.Split(body, "\n") {
		switch strings.TrimSpace(strings.TrimSuffix(line, "\r")) {
		case "# claudeMd", "# currentDate", "# userEmail", "# attachedProject":
			return true
		}
	}
	return false
}

func rawTextBlock(raw json.RawMessage) (string, string, bool) {
	var block struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if err := json.Unmarshal(raw, &block); err != nil {
		return "", "", false
	}
	return block.Type, block.Text, true
}

func replaceRawTextBlock(raw json.RawMessage, text string) (json.RawMessage, bool) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return nil, false
	}
	encodedText, err := json.Marshal(text)
	if err != nil {
		return nil, false
	}
	fields["text"] = encodedText
	rebuilt, err := json.Marshal(fields)
	if err != nil {
		return nil, false
	}
	return rebuilt, true
}

package proxy

import (
	"encoding/json"
	"log/slog"
)

// validateToolPairs scans messages for orphaned tool_result blocks whose
// tool_use_id has no matching tool_use "id" earlier in the conversation.
// Returns the repaired messages slice and the count of removed orphans.
// If no orphans are found, returns the original slice unchanged (zero alloc).
func validateToolPairs(messages []Message) ([]Message, int) {
	if len(messages) == 0 {
		return messages, 0
	}

	// Pass 1: collect all tool_use IDs
	toolUseIDs := make(map[string]bool)
	for _, msg := range messages {
		var blocks []ContentBlock
		if err := json.Unmarshal(msg.Content, &blocks); err != nil {
			continue
		}
		for _, b := range blocks {
			if b.Type == "tool_use" && b.ID != "" {
				toolUseIDs[b.ID] = true
			}
		}
	}

	// Pass 2: find orphaned tool_results
	orphanCount := 0
	result := make([]Message, 0, len(messages))

	for _, msg := range messages {
		var blocks []ContentBlock
		if err := json.Unmarshal(msg.Content, &blocks); err != nil {
			// String content or other — keep as-is
			result = append(result, msg)
			continue
		}

		var cleaned []ContentBlock
		removed := 0
		for _, b := range blocks {
			if b.Type == "tool_result" && b.ToolUseID != "" {
				if !toolUseIDs[b.ToolUseID] {
					slog.Warn("移除 orphan tool_result",
						"tool_use_id", b.ToolUseID,
					)
					removed++
					orphanCount++
					continue
				}
			}
			cleaned = append(cleaned, b)
		}

		if removed == 0 {
			result = append(result, msg)
			continue
		}

		if len(cleaned) == 0 {
			// Entire message was orphan tool_results — drop message
			continue
		}

		// Rebuild message with cleaned content
		newContent, _ := json.Marshal(cleaned)
		msg.Content = newContent
		result = append(result, msg)
	}

	if orphanCount == 0 {
		return messages, 0
	}

	// Pass 3: fix alternation violations from removed messages
	result = fixAlternation(result)

	return result, orphanCount
}

// fixAlternation merges consecutive same-role messages to maintain
// the user/assistant alternation required by the Anthropic API.
func fixAlternation(messages []Message) []Message {
	if len(messages) < 2 {
		return messages
	}

	fixed := []Message{messages[0]}
	for i := 1; i < len(messages); i++ {
		prev := fixed[len(fixed)-1]
		curr := messages[i]
		if prev.Role == curr.Role {
			mergeAdjacentContent(&fixed[len(fixed)-1], curr)
		} else {
			fixed = append(fixed, curr)
		}
	}

	return fixed
}

// mergeAdjacentContent appends src's content blocks to dst.
// Handles both JSON array and string content formats.
func mergeAdjacentContent(dst *Message, src Message) {
	var dstBlocks, srcBlocks []ContentBlock

	// Try to parse dst content (might be string or array)
	if err := json.Unmarshal(dst.Content, &dstBlocks); err != nil {
		var text string
		if err := json.Unmarshal(dst.Content, &text); err == nil {
			dstBlocks = []ContentBlock{{Type: "text", Text: text}}
		}
	}

	// Try to parse src content
	if err := json.Unmarshal(src.Content, &srcBlocks); err != nil {
		var text string
		if err := json.Unmarshal(src.Content, &text); err == nil {
			srcBlocks = []ContentBlock{{Type: "text", Text: text}}
		}
	}

	merged, _ := json.Marshal(append(dstBlocks, srcBlocks...))
	dst.Content = merged
}

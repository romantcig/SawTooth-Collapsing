package proxy

import (
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
)

// toolUseInfoExtended maps tool_use ID to tool metadata.
// Built once by scanning all messages, then used when compressing
// the corresponding tool_result blocks.
type toolUseInfoExtended struct {
	Name     string // tool name (e.g. "Read", "Bash", "Edit")
	Keywords string // search keywords extracted from the tool_use input
}

// CompressResult holds statistics from a CompressContext pass.
type CompressResult struct {
	// ThinkingCompressed is the number of thinking blocks compressed.
	ThinkingCompressed int

	// ToolResultsCompressed is the number of tool_result blocks compressed.
	ToolResultsCompressed int

	// TokensSaved is the estimated token reduction from compression
	// (tokensBefore - tokensAfter, clamped to >= 0).
	TokensSaved int
}

// buildToolUseInfoExtended scans all messages and builds a
// tool_use_id → {name, keywords} index. This is called as a first pass
// so that tool_result blocks can be matched to their originating tool_use
// when compressing.
func buildToolUseInfoExtended(messages []Message) map[string]toolUseInfoExtended {
	result := make(map[string]toolUseInfoExtended)
	for _, msg := range messages {
		blocks, _ := parseContent(msg.Content)
		for _, block := range blocks {
			if block.Type == "tool_use" && block.ID != "" {
				result[block.ID] = toolUseInfoExtended{
					Name:     block.Name,
					Keywords: extractToolKeywords(block),
				}
			}
		}
	}
	return result
}

// CompressContext pre-compresses thinking blocks and large tool_result blocks
// in messages outside the keepRecent tail window. This reduces token waste from
// content that has already been "digested" by the model but would otherwise
// persist across subsequent requests.
//
// Rules:
//   - messages[0] is never touched (system message).
//   - Messages in the last keepRecent positions are never touched.
//   - thinking blocks with > 500 chars of thinking text → replaced with
//     "[context compressed: thinking block]".
//   - tool_result blocks with > 500 tokens → replaced with structured summary
//     preserving tool_use_id and is_error.
//
// The function returns a new Message slice (the input is not mutated).
func CompressContext(messages []Message, keepRecent int, tc *TokenCounter) ([]Message, CompressResult) {
	var result CompressResult

	if len(messages) == 0 {
		return messages, result
	}

	// Pass 1: build tool_use_id → {name, keywords} index.
	toolUseInfo := buildToolUseInfoExtended(messages)

	// Snapshot token count before compression.
	tokensBefore := 0
	if tc != nil {
		tokensBefore = tc.CountMessagesTokens(messages)
	}

	// Determine the scan window: [1, len(messages)-keepRecent).
	// Never touch messages[0] (system message or equivalent).
	scanEnd := len(messages) - keepRecent
	if scanEnd < 1 {
		scanEnd = 1
	}

	// Work on a copy to avoid mutating the input.
	compressed := make([]Message, len(messages))
	copy(compressed, messages)

	for i := 1; i < scanEnd; i++ {
		blocks, isArray := parseContent(compressed[i].Content)
		if len(blocks) == 0 {
			continue
		}

		modified := false
		for j := range blocks {
			switch blocks[j].Type {
			case "thinking":
				if countBlockTokens(blocks[j], tc) > 500 {
					// 保留 thinking 类型 — 对标 YesMem（保持 API 块形状一致）。
					// 将压缩文本写入 Thinking 字段，原 Signature 保留（已在 copy 中传递）。
					newBlock := blocks[j]
					newBlock.Thinking = "[context compressed: thinking block]"
					blocks[j] = newBlock
					result.ThinkingCompressed++
					modified = true
				}

			case "tool_result":
				blockTokens := countBlockTokens(blocks[j], tc)
				if blockTokens > 500 {
					info, ok := toolUseInfo[blocks[j].ToolUseID]
					if !ok {
						info = toolUseInfoExtended{Name: "tool", Keywords: ""}
					}
					summary := buildToolResultSummary(blocks[j], info)

					// 浅拷贝保留所有未知字段（对标 YesMem shallowCopyMap）
					newBlock := blocks[j]
					newBlock.Content = summary
					blocks[j] = newBlock
					result.ToolResultsCompressed++
					modified = true
				}
			}
		}

		if modified {
			compressed[i].Content = rebuildContent(blocks, isArray)
		}
	}

	// Snapshot token count after compression.
	if tc != nil {
		tokensAfter := tc.CountMessagesTokens(compressed)
		result.TokensSaved = tokensBefore - tokensAfter
		if result.TokensSaved < 0 {
			result.TokensSaved = 0
		}
	}

	return compressed, result
}

// countBlockTokens estimates the token count of a single content block
// using the same counting strategy as TokenCounter.CountMessagesTokens.
func countBlockTokens(block ContentBlock, tc *TokenCounter) int {
	switch block.Type {
	case "thinking":
		if tc != nil {
			return tc.CountTokens(block.Thinking)
		}
		// Rough fallback: ~4 chars per token.
		return len(block.Thinking) / 4

	case "tool_result":
		text := extractBlockContent(block.Content)
		if tc != nil {
			return tc.CountTokens(text)
		}
		return len(text) / 4

	default:
		return 0
	}
}

// extractBlockContent extracts a flat string representation of a
// tool_result block's Content field, which can be a string or []interface{}.
// (Sibling of extractToolResultText in eager.go, but for ContentBlock.Content.)
func extractBlockContent(content interface{}) string {
	if content == nil {
		return ""
	}
	switch v := content.(type) {
	case string:
		return v
	default:
		// For array/object content: marshal back to string for counting.
		if b, err := json.Marshal(v); err == nil {
			return string(b)
		}
		return ""
	}
}

// truncateMiddle keeps headChars runes from the start and tailChars runes from
// the end, replacing the middle portion with a character-count placeholder.
// 对标 YesMem compress_context.go:195-204（含 +50 字符余量）。
func truncateMiddle(s string, headChars, tailChars int) string {
	runes := []rune(s)
	if len(runes) <= headChars+tailChars+50 {
		return s
	}
	head := string(runes[:headChars])
	tail := string(runes[len(runes)-tailChars:])
	skipped := len(runes) - headChars - tailChars
	return fmt.Sprintf("%s[...%d chars compressed...]%s", head, skipped, tail)
}

// truncateToolResult keeps the first 15 lines and last 10 lines of text,
// inserting a line-count placeholder between them. Lines ≤ 30 are kept intact.
// 对标 YesMem compress_context.go:207-230。
func truncateToolResult(text string) string {
	lines := strings.Split(text, "\n")
	if len(lines) <= 30 {
		return text
	}
	head := strings.Join(lines[:15], "\n")
	tail := strings.Join(lines[len(lines)-10:], "\n")
	skipped := len(lines) - 25
	return fmt.Sprintf("%s\n[...%d lines compressed...]\n%s", head, skipped, tail)
}

// funcSigRe matches function / type / class / def / export declarations for
// signature extraction. Used to produce a human-readable "key" in compressed
// tool_result summaries.
var funcSigRe = regexp.MustCompile(`(?i)\b(func|type|class|def|export)\s+(\w[\w.]*)`)

// buildToolResultSummary creates a structured summary of a tool result block
// after truncation. Format:
//
//	[context compressed: tool_name, N lines, key: func Foo; type Bar] → deep_search('keywords')
//
// The search hint is omitted when no keywords are available.
func buildToolResultSummary(block ContentBlock, info toolUseInfoExtended) string {
	text := extractBlockContent(block.Content)

	// 使用原始文本的行数（对标 YesMem — 报告"压缩了多少内容"）
	lineCount := strings.Count(text, "\n") + 1


	// 从原始文本中提取最多 5 个函数/类型签名（对标 YesMem compress_context.go:241-254）
	matches := funcSigRe.FindAllStringSubmatch(text, 5)
	sigs := make([]string, 0, len(matches))
	for _, m := range matches {
		if len(m) >= 3 {
			sigs = append(sigs, strings.ToLower(m[1])+" "+m[2])
		}
	}

	// Build the summary.
	var sb strings.Builder
	sb.WriteString("[context compressed: ")
	sb.WriteString(info.Name)
	sb.WriteString(", ")
	sb.WriteString(fmt.Sprintf("%d lines", lineCount))
	if len(sigs) > 0 {
		sb.WriteString(", key: ")
		sb.WriteString(strings.Join(sigs, "; "))
	}
	sb.WriteString("]")

	// Append search hint when keywords are available.
	if info.Keywords != "" {
		sb.WriteString(" → deep_search('")
		sb.WriteString(sanitizeDSKeyword(info.Keywords))
		sb.WriteString("')")
	}

	return sb.String()
}

// extractToolKeywords extracts searchable keywords from a tool_use block.
// This is a basic implementation — Phase D will expand with per-tool-type
// strategies matching YesMem's stubify.go:446-473.
func extractToolKeywords(block ContentBlock) string {
	name := block.Name
	input := block.Input

	switch name {
	case "Read", "Edit", "Write":
		if fp, ok := input["file_path"].(string); ok && fp != "" {
			return name + " " + fp
		}
		return name

	case "Bash":
		if cmd, ok := input["command"].(string); ok && cmd != "" {
			cmdRunes := []rune(cmd)
			if len(cmdRunes) > 40 {
				cmd = string(cmdRunes[:40]) + "..."
			}
			return "Bash " + cmd
		}
		return "Bash"

	case "Grep":
		pattern, _ := input["pattern"].(string)
		path, _ := input["path"].(string)
		if pattern != "" && path != "" {
			return "Grep " + pattern + " " + path
		}
		if pattern != "" {
			return "Grep " + pattern
		}
		return "Grep"

	case "Glob":
		if pat, ok := input["pattern"].(string); ok && pat != "" {
			return "Glob " + pat
		}
		return "Glob"

	case "Agent":
		if desc, ok := input["description"].(string); ok && desc != "" {
			return "Agent " + truncateRunes(desc, 60)
		}
		return "Agent"

	case "WebSearch":
		if q, ok := input["query"].(string); ok && q != "" {
			return truncateRunes(q, 60)
		}
		return "WebSearch"

	case "WebFetch":
		if u, ok := input["url"].(string); ok && u != "" {
			return truncateRunes(u, 60)
		}
		return "WebFetch"

	case "Skill":
		if s, ok := input["skill"].(string); ok && s != "" {
			return "Skill " + truncateRunes(s, 60)
		}
		return "Skill"

	default:
		// 未知工具类型：尝试提取所有字符串字段值作为关键词
		return extractFallbackKeywords(name, input)
	}
}

// extractFallbackKeywords 为未知工具类型提取关键词。
// 扫描 input 中所有长度 3-50 的字符串值，返回 toolName + 前 3 个值。
func extractFallbackKeywords(toolName string, input map[string]any) string {
	// 对 key 排序以保证确定性输出（Go map 迭代顺序非确定）
	var keys []string
	for k := range input {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var values []string
	for _, key := range keys {
		val := input[key]
		s, ok := val.(string)
		if !ok || len(s) < 3 || len(s) > 50 {
			continue
		}
		// 跳过明显不是搜索关键词的 key
		switch key {
		case "thinking", "signature", "cache_control":
			continue
		}
		values = append(values, s)
		if len(values) >= 3 {
			break
		}
	}
	if len(values) > 0 {
		return toolName + " " + strings.Join(values, " ")
	}
	return toolName
}

// sanitizeDSKeyword 转义关键词中可能导致 deep_search 解析截断的字符。
// 将 ')' 替换为空格，防止 extractDSKeywords 中的 strings.Index(..., "')") 提前匹配。
func sanitizeDSKeyword(kw string) string {
	return strings.ReplaceAll(kw, "')", " )")
}

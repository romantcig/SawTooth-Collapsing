package proxy

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"sync"
)

// TokenEstimateFunc 估算文本 token 数量的函数签名。
type TokenEstimateFunc func(string) int

const eagerStubTokenThreshold = 500

// ─────────────────────────────────────────────────────────────
// EagerStubToolResults
// ─────────────────────────────────────────────────────────────

// EagerStubToolResults walks the fresh tail (messages after frozenBoundary)
// and replaces large tool_result content with rule-based summaries.
// Only stubs tool_results that (a) exceed the token threshold and (b) have a
// following assistant turn (meaning Claude already processed them).
// Operates in the uncached zone — zero prompt cache cost.
func EagerStubToolResults(messages []any, frozenBoundary int, estimateTokens TokenEstimateFunc, opts ...EagerStubOption) []any {
	cfg := &eagerStubConfig{}
	for _, opt := range opts {
		opt(cfg)
	}
	result := make([]any, len(messages))
	copy(result, messages)

	for i := frozenBoundary; i < len(result); i++ {
		msg, ok := result[i].(map[string]any)
		if !ok || msg["role"] != "user" {
			continue
		}

		blocks, ok := msg["content"].([]any)
		if !ok {
			continue
		}

		hasFollowingAssistant := false
		for j := i + 1; j < len(result); j++ {
			if m, ok := result[j].(map[string]any); ok && m["role"] == "assistant" {
				hasFollowingAssistant = true
				break
			}
		}

		hasMemoryStub := false
		if cfg.memory != nil && cfg.threadID != "" {
			for _, block := range blocks {
				b, ok := block.(map[string]any)
				if !ok || b["type"] != "tool_result" {
					continue
				}
				id, _ := b["tool_use_id"].(string)
				if id != "" && cfg.memory.WasStubbed(cfg.threadID, id) {
					hasMemoryStub = true
					break
				}
			}
		}

		if !hasFollowingAssistant && !hasMemoryStub {
			continue
		}

		// Find the matching tool_use in the previous assistant message
		var toolName string
		var toolInput map[string]any
		if i > 0 {
			if prev, ok := result[i-1].(map[string]any); ok && prev["role"] == "assistant" {
				toolName, toolInput = eagerExtractToolInfo(prev["content"])
			}
		}

		anyChanged := false
		newBlocks := make([]any, 0, len(blocks))
		for _, block := range blocks {
			b, ok := block.(map[string]any)
			if !ok || b["type"] != "tool_result" {
				newBlocks = append(newBlocks, block)
				continue
			}

			toolUseID, _ := b["tool_use_id"].(string)
			content := extractToolResultText(b)

			memoryHit := cfg.memory != nil && cfg.threadID != "" && toolUseID != "" &&
				cfg.memory.WasStubbed(cfg.threadID, toolUseID)

			if !memoryHit {
				if !hasFollowingAssistant {
					newBlocks = append(newBlocks, block)
					continue
				}
				if estimateTokens(content) <= eagerStubTokenThreshold {
					newBlocks = append(newBlocks, block)
					continue
				}
			}

			stub := buildEagerStub(toolName, toolInput, content)
			newBlock := make(map[string]any)
			for k, v := range b {
				newBlock[k] = v
			}
			newBlock["content"] = stub
			newBlocks = append(newBlocks, newBlock)
			anyChanged = true

			if memoryHit {
				if cfg.stickyHits != nil {
					*cfg.stickyHits++
				}
			} else {
				if cfg.freshStubs != nil {
					*cfg.freshStubs++
				}
				if cfg.memory != nil && cfg.threadID != "" && toolUseID != "" {
					cfg.memory.RecordStubbed(cfg.threadID, toolUseID)
				}
			}
		}

		if anyChanged {
			newMsg := make(map[string]any)
			for k, v := range msg {
				newMsg[k] = v
			}
			newMsg["content"] = newBlocks
			result[i] = newMsg
		}
	}

	return result
}

func eagerExtractToolInfo(content any) (string, map[string]any) {
	blocks, ok := content.([]any)
	if !ok {
		return "", nil
	}
	for _, block := range blocks {
		b, ok := block.(map[string]any)
		if !ok {
			continue
		}
		if b["type"] == "tool_use" {
			name, _ := b["name"].(string)
			input, _ := b["input"].(map[string]any)
			return name, input
		}
	}
	return "", nil
}

func buildEagerStub(toolName string, input map[string]any, content string) string {
	lines := strings.Split(content, "\n")
	lineCount := len(lines)

	switch toolName {
	case "Read":
		path, _ := input["file_path"].(string)
		funcs := eagerExtractFuncSignatures(content)
		funcStr := ""
		if len(funcs) > 0 {
			funcStr = " | " + strings.Join(funcs, ", ")
		}
		return fmt.Sprintf("[Read %s — %d lines%s]", path, lineCount, funcStr)

	case "Grep":
		pattern, _ := input["pattern"].(string)
		path, _ := input["path"].(string)
		return fmt.Sprintf("[Grep '%s' in %s — %d matches]", pattern, path, lineCount)

	case "Bash":
		cmd, _ := input["command"].(string)
		head, tail := eagerHeadTail(lines, 3, 3)
		if tail != "" {
			return fmt.Sprintf("[Bash: %s — %d lines]\n%s\n[...]\n%s", truncateStr(cmd, 80), lineCount, head, tail)
		}
		return fmt.Sprintf("[Bash: %s — %d lines]\n%s", truncateStr(cmd, 80), lineCount, head)

	case "Glob":
		pattern, _ := input["pattern"].(string)
		first := eagerFirstN(lines, 10)
		return fmt.Sprintf("[Glob '%s' — %d results]\n%s", pattern, lineCount, first)

	case "Agent":
		desc, _ := input["description"].(string)
		return fmt.Sprintf("[Agent: %s — %s]", desc, truncateStr(content, 200))

	default:
		return fmt.Sprintf("[%s result — %d lines archived]", toolName, lineCount)
	}
}

var eagerGoFuncRe = regexp.MustCompile(`(?m)^func\s+(\([^)]+\)\s+)?(\w+)\s*\(`)

func eagerExtractFuncSignatures(code string) []string {
	matches := eagerGoFuncRe.FindAllStringSubmatch(code, 20)
	var names []string
	for _, m := range matches {
		if len(m) > 2 {
			names = append(names, m[2]+"()")
		}
	}
	return names
}

func eagerHeadTail(lines []string, h, t int) (string, string) {
	if len(lines) <= h+t {
		return strings.Join(lines, "\n"), ""
	}
	return strings.Join(lines[:h], "\n"), strings.Join(lines[len(lines)-t:], "\n")
}

func eagerFirstN(lines []string, n int) string {
	if len(lines) <= n {
		return strings.Join(lines, "\n")
	}
	return strings.Join(lines[:n], "\n") + fmt.Sprintf("\n[... +%d more]", len(lines)-n)
}

// ─────────────────────────────────────────────────────────────
// EagerStubMemory
// ─────────────────────────────────────────────────────────────

// EagerStubMemory remembers, per session (threadID), which tool_use_ids have
// already been stubbed by EagerStubToolResults. Once a tool_use_id is in this
// memory, the corresponding tool_result is stubbed deterministically on every
// subsequent call — independent of hasFollowingAssistant or frozenBoundary.
// That keeps prefix bytes byte-identical across turns, so the prompt cache hits.
type EagerStubMemory struct {
	mu        sync.RWMutex
	stubbed   map[string]map[string]bool
	persistFn PersistFunc
	loadFn    LoadFunc
	loaded    map[string]bool
}

func NewEagerStubMemory() *EagerStubMemory {
	return &EagerStubMemory{
		stubbed: make(map[string]map[string]bool),
		loaded:  make(map[string]bool),
	}
}

func (m *EagerStubMemory) SetPersistFunc(fn PersistFunc) { m.persistFn = fn }
func (m *EagerStubMemory) SetLoadFunc(fn LoadFunc)       { m.loadFn = fn }

func (m *EagerStubMemory) WasStubbed(threadID, toolUseID string) bool {
	if m == nil || threadID == "" || toolUseID == "" {
		return false
	}
	m.ensureLoaded(threadID)
	m.mu.RLock()
	defer m.mu.RUnlock()
	if ids, ok := m.stubbed[threadID]; ok {
		return ids[toolUseID]
	}
	return false
}

func (m *EagerStubMemory) RecordStubbed(threadID, toolUseID string) {
	if m == nil || threadID == "" || toolUseID == "" {
		return
	}
	m.ensureLoaded(threadID)
	m.mu.Lock()
	if m.stubbed[threadID] == nil {
		m.stubbed[threadID] = make(map[string]bool)
	}
	if m.stubbed[threadID][toolUseID] {
		m.mu.Unlock()
		return
	}
	m.stubbed[threadID][toolUseID] = true
	m.mu.Unlock()
	m.persist(threadID)
}

func (m *EagerStubMemory) ensureLoaded(threadID string) {
	m.mu.RLock()
	already := m.loaded[threadID]
	m.mu.RUnlock()
	if already {
		return
	}

	if m.loadFn == nil {
		m.mu.Lock()
		m.loaded[threadID] = true
		m.mu.Unlock()
		return
	}

	raw, ok := m.loadFn("eagerstub:" + threadID)
	if !ok || raw == "" {
		m.mu.Lock()
		m.loaded[threadID] = true
		m.mu.Unlock()
		return
	}

	var payload struct {
		ToolUseIDs []string `json:"tool_use_ids"`
	}
	if err := json.Unmarshal([]byte(raw), &payload); err == nil {
		m.mu.Lock()
		if m.stubbed[threadID] == nil {
			m.stubbed[threadID] = make(map[string]bool)
		}
		for _, id := range payload.ToolUseIDs {
			m.stubbed[threadID][id] = true
		}
		m.loaded[threadID] = true
		m.mu.Unlock()
		return
	}

	m.mu.Lock()
	m.loaded[threadID] = true
	m.mu.Unlock()
}

func (m *EagerStubMemory) persist(threadID string) {
	if m.persistFn == nil {
		return
	}
	m.mu.RLock()
	ids := make([]string, 0, len(m.stubbed[threadID]))
	for id := range m.stubbed[threadID] {
		ids = append(ids, id)
	}
	m.mu.RUnlock()

	payload := struct {
		ToolUseIDs []string `json:"tool_use_ids"`
	}{ToolUseIDs: ids}

	data, err := json.Marshal(payload)
	if err != nil {
		return
	}
	m.persistFn("eagerstub:"+threadID, string(data))
}

// ─────────────────────────────────────────────────────────────
// EagerStubOption
// ─────────────────────────────────────────────────────────────

type EagerStubOption func(*eagerStubConfig)

type eagerStubConfig struct {
	memory     *EagerStubMemory
	threadID   string
	stickyHits *int
	freshStubs *int
}

func WithStubMemory(memory *EagerStubMemory, threadID string) EagerStubOption {
	return func(c *eagerStubConfig) {
		c.memory = memory
		c.threadID = threadID
	}
}

// WithStubCounters captures, for the duration of one EagerStubToolResults call,
// how many tool_results were stubbed via memory hit (sticky) vs. via a fresh
// in-call decision. Both pointers must be non-nil.
func WithStubCounters(sticky, fresh *int) EagerStubOption {
	return func(c *eagerStubConfig) {
		c.stickyHits = sticky
		c.freshStubs = fresh
	}
}

// ─────────────────────────────────────────────────────────────
// extractToolResultText
// ─────────────────────────────────────────────────────────────

// extractToolResultText extracts text from a tool_result's content field.
// Handles both string content and []any content blocks.
func extractToolResultText(block map[string]any) string {
	content := block["content"]
	switch c := content.(type) {
	case string:
		return c
	case []any:
		var sb strings.Builder
		for _, b := range c {
			if m, ok := b.(map[string]any); ok {
				if text, ok := m["text"].(string); ok {
					sb.WriteString(text)
					sb.WriteByte('\n')
				}
			}
		}
		return sb.String()
	}
	return ""
}

// ─────────────────────────────────────────────────────────────
// Conversion helpers
// ─────────────────────────────────────────────────────────────

// messagesToAny converts []Message to []any for EagerStubToolResults compatibility.
func messagesToAny(msgs []Message) []any {
	result := make([]any, len(msgs))
	for i, m := range msgs {
		data, _ := json.Marshal(m)
		var v map[string]any
		_ = json.Unmarshal(data, &v)
		result[i] = v
	}
	return result
}

// anyToMessages converts []any back to []Message after EagerStubToolResults.
func anyToMessages(items []any) []Message {
	result := make([]Message, 0, len(items))
	for _, item := range items {
		data, _ := json.Marshal(item)
		var m Message
		if json.Unmarshal(data, &m) == nil {
			result = append(result, m)
		}
	}
	return result
}

// ─────────────────────────────────────────────────────────────
// Utilities
// ─────────────────────────────────────────────────────────────

// truncateStr truncates a string to maxLen characters, appending "..." if truncated.
func truncateStr(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}

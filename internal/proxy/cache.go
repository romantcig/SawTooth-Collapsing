package proxy

import (
	"encoding/json"
	"fmt"
	"strings"
)

// ── 公开纯函数 ──

// StripMessagesCacheControl 遍历所有消息，移除数组 content 类型消息中每个 content block 的 cache_control 字段。
// 字符串 content（首字节不是 '['）被跳过（无 cache_control 可 strip）。
// 通过 map[string]any 中间表示安全删除，不高危字符串替换。
func StripMessagesCacheControl(messages []Message) error {
	for i := range messages {
		if err := stripMessageCacheControl(&messages[i]); err != nil {
			return fmt.Errorf("strip cache_control at index %d: %w", i, err)
		}
	}
	return nil
}

// InjectFrozenBoundaryBreakpoint 在 frozen boundary（frozen portion 的最后一条消息的最后一个 content block）
// 注入单个 cache_control: {"type": "ephemeral"} breakpoint。
// frozenCount 是 frozen portion 的消息数。若 frozenCount == 0 或 > len(messages)，不做任何操作。
func InjectFrozenBoundaryBreakpoint(messages []Message, frozenCount int) error {
	if frozenCount == 0 || frozenCount > len(messages) {
		return nil
	}
	boundaryIdx := frozenCount - 1
	return injectCacheControl(&messages[boundaryIdx])
}

// NormalizeCacheTTL 遍历所有消息，将已有 cache_control 的 block 的 ttl 字段统一。
// 自动检测：若任一已有 breakpoint 的 ttl 为 "1h"，则全量提升为 "1h"，
// 防止 Anthropic API 的 increasing-TTL violation（HTTP 400）。
// 否则使用传入的 cacheTTL 参数。
func NormalizeCacheTTL(messages []Message, cacheTTL string) error {
	cacheTTL = strings.TrimSpace(cacheTTL)
	if cacheTTL == "" {
		cacheTTL = "ephemeral"
	}
	if cacheTTL != "ephemeral" && cacheTTL != "1h" {
		return fmt.Errorf("不支持的 cache TTL %q", cacheTTL)
	}
	// 扫描已有 breakpoint，检测 1h TTL
	targetTTL := cacheTTL
	for _, msg := range messages {
		if ttl := findExistingMaxTTL(msg); ttl == "1h" {
			targetTTL = "1h"
			break
		}
	}

	for i := range messages {
		if err := normalizeMessageTTL(&messages[i], targetTTL); err != nil {
			return fmt.Errorf("normalize cache_control TTL at index %d: %w", i, err)
		}
	}
	return nil
}

// findExistingMaxTTL 扫描单条消息中所有 cache_control block，返回最高 TTL 值。
// 若为字符串 content 或无 cache_control，返回空字符串。
func findExistingMaxTTL(msg Message) string {
	if len(msg.Content) == 0 || msg.Content[0] != '[' {
		return ""
	}
	var blocks []map[string]any
	if err := json.Unmarshal(msg.Content, &blocks); err != nil {
		return ""
	}
	for _, block := range blocks {
		cc, ok := block["cache_control"]
		if !ok {
			continue
		}
		ccMap, ok := cc.(map[string]any)
		if !ok {
			continue
		}
		if ttl, _ := ccMap["ttl"].(string); ttl == "1h" {
			return "1h"
		}
	}
	return ""
}

// EnforceCacheBreakpointLimit 确保所有消息中 cache_control breakpoint 的总数不超过 limit。
// 超过时从前往后移除多余的 breakpoint（按时间/位置排序，保留最后的 limit 个）。
func EnforceCacheBreakpointLimit(messages []Message, limit int) error {
	count := countBreakpoints(messages)
	if count <= limit {
		return nil
	}
	return removeExtraBreakpoints(messages, limit)
}

// ── 私有辅助函数 ──

// stripMessageCacheControl 移除单条消息的所有 content block 中的 cache_control 键。
// 字符串 content（首字节不是 '['）直接返回 nil（无 cache_control 可 strip）。
func stripMessageCacheControl(msg *Message) error {
	if len(msg.Content) == 0 || msg.Content[0] != '[' {
		return nil // 字符串 content，无 cache_control
	}
	var blocks []map[string]any
	if err := json.Unmarshal(msg.Content, &blocks); err != nil {
		return err
	}
	for i := range blocks {
		delete(blocks[i], "cache_control")
	}
	data, err := json.Marshal(blocks)
	if err != nil {
		return err
	}
	msg.Content = data
	return nil
}

// injectCacheControl 在消息的最后一个 content block 注入 cache_control: {"type": "ephemeral"}。
// 字符串 content 转为单元素数组后再注入。
func injectCacheControl(msg *Message) error {
	if len(msg.Content) == 0 {
		return nil
	}

	// 字符串 content: 转为单元素数组后注入
	if msg.Content[0] != '[' {
		var text string
		if err := json.Unmarshal(msg.Content, &text); err != nil {
			return err
		}
		blocks := []map[string]any{{
			"type":          "text",
			"text":          text,
			"cache_control": map[string]string{"type": "ephemeral"},
		}}
		data, err := json.Marshal(blocks)
		if err != nil {
			return err
		}
		msg.Content = data
		return nil
	}

	// 数组 content: 在最后一个 block 注入（若已有 cache_control 则跳过）
	var blocks []map[string]any
	if err := json.Unmarshal(msg.Content, &blocks); err != nil {
		return err
	}
	if len(blocks) == 0 {
		return nil
	}
	lastBlock := blocks[len(blocks)-1]
	if _, exists := lastBlock["cache_control"]; exists {
		return nil // 已有 cache_control，不覆盖
	}
	lastBlock["cache_control"] = map[string]string{"type": "ephemeral"}
	data, err := json.Marshal(blocks)
	if err != nil {
		return err
	}
	msg.Content = data
	return nil
}

// normalizeMessageTTL 统一单条消息中所有已有 cache_control 的 block 的 ttl 字段。
// 字符串 content 直接返回 nil（无 content block，无 cache_control）。
func normalizeMessageTTL(msg *Message, cacheTTL string) error {
	if len(msg.Content) == 0 || msg.Content[0] != '[' {
		return nil // 字符串 content，无 cache_control
	}
	var blocks []map[string]any
	if err := json.Unmarshal(msg.Content, &blocks); err != nil {
		return err
	}
	for i := range blocks {
		cc, ok := blocks[i]["cache_control"]
		if !ok {
			continue
		}
		ccMap, ok := cc.(map[string]any)
		if !ok {
			continue
		}
		// Special case: if cacheTTL is "", the caller may have passed an
		// empty string to intentionally remove the ttl field from a
		// previous round — delete the key so the final JSON is clean.
		if cacheTTL == "" || cacheTTL == "ephemeral" {
			// Don't set ttl — Anthropic defaults to 5m for ephemeral.
			// Clean up any stale ttl that may exist from a prior pass.
			delete(ccMap, "ttl")
		} else {
			ccMap["ttl"] = cacheTTL
		}
	}
	data, err := json.Marshal(blocks)
	if err != nil {
		return err
	}
	msg.Content = data
	return nil
}

// countBreakpoints 统计所有消息中所有 content blocks 中的 cache_control 总数。
func countBreakpoints(messages []Message) int {
	count := 0
	for _, msg := range messages {
		if len(msg.Content) == 0 || msg.Content[0] != '[' {
			continue // 字符串 content，无 content blocks
		}
		var blocks []map[string]any
		if err := json.Unmarshal(msg.Content, &blocks); err != nil {
			continue
		}
		for _, block := range blocks {
			if _, ok := block["cache_control"]; ok {
				count++
			}
		}
	}
	return count
}

// breakpointLocation 记录单个 cache_control breakpoint 的位置信息。
type breakpointLocation struct {
	msgIdx   int // 消息索引
	blockIdx int // 该消息中的 block 索引
}

// removeExtraBreakpoints 移除多余的 breakpoint。
// 从前往后遍历，收集所有 breakpoint 位置，从第 0 个开始删除直到剩余 breakpoint 数 == limit。
// 对受影响的 message 重新 marshal（若同一 message 有多个 block 被修改，只做一次 marshal）。
func removeExtraBreakpoints(messages []Message, limit int) error {
	// 收集所有 breakpoint 位置（按消息顺序从前往后）
	var locations []breakpointLocation
	type msgBlocks struct {
		blocks []map[string]any
	}
	msgCache := make(map[int]*msgBlocks)

	for i, msg := range messages {
		if len(msg.Content) == 0 || msg.Content[0] != '[' {
			continue
		}
		var blocks []map[string]any
		if err := json.Unmarshal(msg.Content, &blocks); err != nil {
			continue
		}
		msgCache[i] = &msgBlocks{blocks: blocks}
		for j, block := range blocks {
			if _, ok := block["cache_control"]; ok {
				locations = append(locations, breakpointLocation{msgIdx: i, blockIdx: j})
			}
		}
	}

	if len(locations) <= limit {
		return nil
	}

	// 从第 0 个 breakpoint 开始删除，保留最后的 limit 个
	removeCount := len(locations) - limit

	// 记录哪些消息的哪些 blocks 需要修改
	type blockMod struct {
		blockIndices map[int]bool
	}
	msgMods := make(map[int]*blockMod)

	for i := 0; i < removeCount; i++ {
		loc := locations[i]
		if _, exists := msgMods[loc.msgIdx]; !exists {
			msgMods[loc.msgIdx] = &blockMod{blockIndices: make(map[int]bool)}
		}
		msgMods[loc.msgIdx].blockIndices[loc.blockIdx] = true
	}

	// 对被修改的消息，删除指定 blocks 上的 cache_control 并重新 marshal
	for msgIdx, mod := range msgMods {
		blocks := msgCache[msgIdx].blocks
		for blockIdx := range mod.blockIndices {
			delete(blocks[blockIdx], "cache_control")
		}
		data, err := json.Marshal(blocks)
		if err != nil {
			return fmt.Errorf("remove breakpoint at message %d: %w", msgIdx, err)
		}
		messages[msgIdx].Content = data
	}

	return nil
}

package proxy

import (
	"encoding/json"
	"fmt"

	"github.com/pkoukk/tiktoken-go"
)

// TokenCounter tiktoken-go 封装，统一 token 计数接口。
// 在 Server 启动时初始化一次，避免每次请求重复加载编码器词表。
type TokenCounter struct {
	enc *tiktoken.Tiktoken
}

// NewTokenCounter 初始化 cl100k_base 编码器。
// cl100k_base 是最接近 Claude API 的通用 BPE 编码，与 GPT-4/Claude 共用同一词表。
func NewTokenCounter() (*TokenCounter, error) {
	enc, err := tiktoken.GetEncoding(tiktoken.MODEL_CL100K_BASE)
	if err != nil {
		return nil, fmt.Errorf("初始化 tiktoken 编码器失败: %w", err)
	}
	return &TokenCounter{enc: enc}, nil
}

// CountTokens 计算文本的 token 数量。
// 使用 cl100k_base 编码器，无特殊 token 处理。
// 空字符串返回 0。
func (tc *TokenCounter) CountTokens(text string) int {
	tokens := tc.enc.Encode(text, nil, nil)
	return len(tokens)
}

// perMessageOverhead 是 Anthropic API 每条消息的角色标记和分隔符开销（token 估算）。
// YesMem content_tokens.go 同样使用此值。
const perMessageOverhead = 4

// CountMessagesTokens 估算消息数组的总 token 数。
// 遍历所有消息，对 role 和 content 中的各类型 block 进行 token 计数。
// 返回值是结构性估算，不精确等于 Anthropic 内部计数，但比例足够阈值判断。
func (tc *TokenCounter) CountMessagesTokens(messages []Message) int {
	total := 0
	for _, msg := range messages {
		total += tc.CountMessageTokens(msg)
	}
	return total
}

// CountMessageTokens 估算单条消息的 token 数（含 perMessageOverhead）。
// 用于 80-token 硬底判断等 per-message 决策场景。
func (tc *TokenCounter) CountMessageTokens(msg Message) int {
	total := perMessageOverhead
	total += tc.CountTokens(msg.Role)

	blocks, _ := parseContent(msg.Content)
	for _, block := range blocks {
		switch block.Type {
		case "text":
			total += tc.CountTokens(block.Text)
		case "thinking":
			total += tc.CountTokens(block.Thinking)
		case "tool_use":
			total += tc.CountTokens(block.Name)
			if block.Input != nil {
				if data, err := json.Marshal(block.Input); err == nil {
					total += tc.CountTokens(string(data))
				}
			}
		case "tool_result":
			total += countToolResultTokens(block.Content, tc)
		default:
			if data, err := json.Marshal(block); err == nil {
				total += tc.CountTokens(string(data))
			}
		}
	}
	return total
}

// countToolResultTokens 递归计算 tool_result 内容的 token 数。
// content 可以是字符串（纯文本结果）或 []any（嵌套 content blocks）。
func countToolResultTokens(content any, tc *TokenCounter) int {
	if content == nil {
		return 0
	}
	switch c := content.(type) {
	case string:
		return tc.CountTokens(c)
	case []any:
		// 嵌套 content blocks —— marshal 后计数
		if data, err := json.Marshal(c); err == nil {
			return tc.CountTokens(string(data))
		}
	}
	return 0
}

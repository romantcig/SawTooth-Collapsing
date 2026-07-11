package proxy

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
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

const (
	minSemanticBlockTokens = 32
	maxSemanticBlockTokens = 8192
	maxJPEGHeaderBytes     = 64 * 1024
)

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
	total := perMessageOverhead + tc.CountTokens(msg.Role)
	if len(msg.Content) == 0 {
		return total
	}

	var content any
	if err := json.Unmarshal(msg.Content, &content); err != nil {
		return total
	}
	return total + tc.countSemanticContent(content)
}

// countToolResultTokens 递归计算 tool_result 内容的 token 数。
// content 可以是字符串（纯文本结果）或 []any（嵌套 content blocks）。
func countToolResultTokens(content any, tc *TokenCounter) int {
	return tc.countSemanticContent(content)
}

func (tc *TokenCounter) countSemanticContent(content any) int {
	switch value := content.(type) {
	case nil:
		return 0
	case string:
		return tc.CountTokens(value)
	case []any:
		total := 0
		for _, item := range value {
			total += tc.countSemanticContent(item)
		}
		return total
	case map[string]any:
		return tc.countSemanticBlock(value)
	default:
		return tc.countSanitizedStructure(value)
	}
}

func (tc *TokenCounter) countSemanticBlock(block map[string]any) int {
	typeName, _ := block["type"].(string)
	switch typeName {
	case "text":
		text, _ := block["text"].(string)
		return tc.CountTokens(text)
	case "thinking":
		thinking, _ := block["thinking"].(string)
		return tc.CountTokens(thinking)
	case "tool_use":
		name, _ := block["name"].(string)
		return tc.CountTokens(name) + tc.countSanitizedStructure(block["input"])
	case "tool_result":
		return tc.countSemanticContent(block["content"])
	case "image":
		return countImageBlockTokens(block)
	case "document":
		return countDocumentBlockTokens(block)
	default:
		return tc.countSanitizedStructure(block)
	}
}

func (tc *TokenCounter) countSanitizedStructure(value any) int {
	sanitized := stripSourceData(value, false)
	data, err := json.Marshal(sanitized)
	if err != nil {
		return 0
	}
	return tc.CountTokens(string(data))
}

// stripSourceData 在未知结构进入 BPE 前递归删除 source.data。
func stripSourceData(value any, insideSource bool) any {
	switch typed := value.(type) {
	case map[string]any:
		result := make(map[string]any, len(typed))
		for key, item := range typed {
			if insideSource && key == "data" {
				continue
			}
			result[key] = stripSourceData(item, key == "source")
		}
		return result
	case []any:
		result := make([]any, len(typed))
		for i, item := range typed {
			result[i] = stripSourceData(item, insideSource)
		}
		return result
	default:
		return value
	}
}

func countImageBlockTokens(block map[string]any) int {
	mediaType, data, ok := semanticBlockSource(block)
	if !ok {
		return minSemanticBlockTokens
	}
	width, height, ok := imageDimensionsFromBase64(mediaType, data)
	if !ok {
		return boundedPayloadFallback(data)
	}
	tokens, ok := visualTokenCount(width, height)
	if !ok {
		return boundedPayloadFallback(data)
	}
	return tokens
}

func countDocumentBlockTokens(block map[string]any) int {
	_, data, ok := semanticBlockSource(block)
	if !ok {
		return minSemanticBlockTokens
	}
	return boundedPayloadFallback(data)
}

func semanticBlockSource(block map[string]any) (mediaType, data string, ok bool) {
	source, ok := block["source"].(map[string]any)
	if !ok {
		return "", "", false
	}
	sourceType, _ := source["type"].(string)
	mediaType, _ = source["media_type"].(string)
	data, _ = source["data"].(string)
	return mediaType, data, sourceType == "base64" && data != ""
}

func boundedPayloadFallback(data string) int {
	// base64 解码字节约为字符数的 3/4；每字节按 1/8 token 粗估。
	// 先按会触顶的字符数判断，避免超长输入上的整数溢出。
	const charsAtMaximum = maxSemanticBlockTokens * 32 / 3
	if len(data) >= charsAtMaximum {
		return maxSemanticBlockTokens
	}
	estimate := len(data) * 3 / 32
	if estimate < minSemanticBlockTokens {
		return minSemanticBlockTokens
	}
	return estimate
}

func visualTokenCount(width, height uint32) (int, bool) {
	if width == 0 || height == 0 {
		return 0, false
	}
	tileWidth := (uint64(width) + 27) / 28
	tileHeight := (uint64(height) + 27) / 28
	if tileWidth > maxSemanticBlockTokens || tileHeight > maxSemanticBlockTokens {
		return 0, false
	}
	tokens := tileWidth * tileHeight
	if tokens == 0 || tokens > maxSemanticBlockTokens {
		return 0, false
	}
	return int(tokens), true
}

func imageDimensionsFromBase64(mediaType, data string) (uint32, uint32, bool) {
	maxBytes := imageHeaderLimit(mediaType)
	if maxBytes == 0 {
		return 0, 0, false
	}
	header := decodeBase64Prefix(data, maxBytes)
	switch mediaType {
	case "image/png":
		return pngDimensions(header)
	case "image/jpeg":
		return jpegDimensions(header)
	case "image/gif":
		return gifDimensions(header)
	case "image/webp":
		return webpDimensions(header)
	default:
		return 0, 0, false
	}
}

func imageHeaderLimit(mediaType string) int {
	switch mediaType {
	case "image/png":
		return 24
	case "image/jpeg":
		return maxJPEGHeaderBytes
	case "image/gif":
		return 10
	case "image/webp":
		return 30
	default:
		return 0
	}
}

func decodeBase64Prefix(data string, maxBytes int) []byte {
	encodedLimit := ((maxBytes + 2) / 3) * 4
	if len(data) > encodedLimit {
		data = data[:encodedLimit]
	}
	if remainder := len(data) % 4; remainder != 0 {
		data = data[:len(data)-remainder]
	}
	decoded := make([]byte, base64.StdEncoding.DecodedLen(len(data)))
	n, _ := base64.StdEncoding.Decode(decoded, []byte(data))
	if n > maxBytes {
		n = maxBytes
	}
	return decoded[:n]
}

func pngDimensions(data []byte) (uint32, uint32, bool) {
	if len(data) < 24 || !bytes.Equal(data[:8], []byte("\x89PNG\r\n\x1a\n")) || !bytes.Equal(data[12:16], []byte("IHDR")) {
		return 0, 0, false
	}
	return binary.BigEndian.Uint32(data[16:20]), binary.BigEndian.Uint32(data[20:24]), true
}

func gifDimensions(data []byte) (uint32, uint32, bool) {
	if len(data) < 10 || (!bytes.Equal(data[:6], []byte("GIF87a")) && !bytes.Equal(data[:6], []byte("GIF89a"))) {
		return 0, 0, false
	}
	return uint32(binary.LittleEndian.Uint16(data[6:8])), uint32(binary.LittleEndian.Uint16(data[8:10])), true
}

func jpegDimensions(data []byte) (uint32, uint32, bool) {
	if len(data) < 4 || data[0] != 0xff || data[1] != 0xd8 {
		return 0, 0, false
	}
	for offset := 2; offset+3 < len(data); {
		if data[offset] != 0xff {
			offset++
			continue
		}
		for offset < len(data) && data[offset] == 0xff {
			offset++
		}
		if offset >= len(data) {
			break
		}
		marker := data[offset]
		offset++
		if marker == 0xd8 || marker == 0xd9 || (marker >= 0xd0 && marker <= 0xd7) {
			continue
		}
		if offset+2 > len(data) {
			break
		}
		segmentLength := int(binary.BigEndian.Uint16(data[offset : offset+2]))
		if segmentLength < 2 || offset+segmentLength > len(data) {
			break
		}
		if isJPEGStartOfFrame(marker) && segmentLength >= 7 {
			height := binary.BigEndian.Uint16(data[offset+3 : offset+5])
			width := binary.BigEndian.Uint16(data[offset+5 : offset+7])
			return uint32(width), uint32(height), true
		}
		offset += segmentLength
	}
	return 0, 0, false
}

func isJPEGStartOfFrame(marker byte) bool {
	return marker >= 0xc0 && marker <= 0xcf && marker != 0xc4 && marker != 0xc8 && marker != 0xcc
}

func webpDimensions(data []byte) (uint32, uint32, bool) {
	if len(data) < 20 || !bytes.Equal(data[:4], []byte("RIFF")) || !bytes.Equal(data[8:12], []byte("WEBP")) {
		return 0, 0, false
	}
	switch string(data[12:16]) {
	case "VP8X":
		if len(data) < 30 {
			return 0, 0, false
		}
		return uint24LE(data[24:27]) + 1, uint24LE(data[27:30]) + 1, true
	case "VP8 ":
		if len(data) < 30 || !bytes.Equal(data[23:26], []byte{0x9d, 0x01, 0x2a}) {
			return 0, 0, false
		}
		width := binary.LittleEndian.Uint16(data[26:28]) & 0x3fff
		height := binary.LittleEndian.Uint16(data[28:30]) & 0x3fff
		return uint32(width), uint32(height), true
	case "VP8L":
		if len(data) < 25 || data[20] != 0x2f {
			return 0, 0, false
		}
		bits := binary.LittleEndian.Uint32(data[21:25])
		width := (bits & 0x3fff) + 1
		height := ((bits >> 14) & 0x3fff) + 1
		return width, height, true
	default:
		return 0, 0, false
	}
}

func uint24LE(data []byte) uint32 {
	return uint32(data[0]) | uint32(data[1])<<8 | uint32(data[2])<<16
}

package proxy

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"strings"
	"testing"
)

func TestTokenCounterMultimodalNestedToolResult(t *testing.T) {
	tc := mustTokenCounter(t)
	imageData := base64.StdEncoding.EncodeToString(testImageHeader("image/png", 1920, 897))
	msg := multimodalToolResultMessage(t, []any{
		map[string]any{"type": "text", "text": "nested text sentinel repeated repeated repeated"},
		map[string]any{"type": "image", "source": map[string]any{"type": "base64", "media_type": "image/png", "data": imageData}},
	})

	got := tc.CountMessageTokens(msg)
	imageOnly := tc.CountMessageTokens(multimodalToolResultMessage(t, []any{
		map[string]any{"type": "image", "source": map[string]any{"type": "base64", "media_type": "image/png", "data": imageData}},
	}))
	if got <= imageOnly {
		t.Fatalf("嵌套 text 未进入 tokenizer: with text=%d, image only=%d", got, imageOnly)
	}
	if imageOnly < 2277 || imageOnly > 2277+32 {
		t.Fatalf("1920x897 PNG 估算=%d，期望图片 2277 token 加少量结构开销", imageOnly)
	}
}

func TestTokenCounterImageFormatsAndBoundedPayload(t *testing.T) {
	tc := mustTokenCounter(t)
	for _, mediaType := range []string{"image/png", "image/jpeg", "image/gif", "image/webp"} {
		t.Run(mediaType, func(t *testing.T) {
			header := testImageHeader(mediaType, 1920, 897)
			encoded := base64.StdEncoding.EncodeToString(header)
			short := countImageMessage(t, tc, mediaType, encoded)
			long := countImageMessage(t, tc, mediaType, encoded+strings.Repeat("A", 2_000_000))
			if short < 2277 || short > 2277+32 {
				t.Fatalf("%s 估算=%d，期望图片 2277 token 加少量结构开销", mediaType, short)
			}
			if long != short {
				t.Fatalf("超长 payload 改变头部语义估算: short=%d long=%d", short, long)
			}
		})
	}
}

func TestTokenCounterImageDocumentBase64FallbacksAreBounded(t *testing.T) {
	tc := mustTokenCounter(t)
	tests := []struct {
		name      string
		blockType string
		mediaType string
		data      string
	}{
		{name: "畸形 base64", blockType: "image", mediaType: "image/png", data: "%%%not-base64%%%"},
		{name: "截断 PNG", blockType: "image", mediaType: "image/png", data: base64.StdEncoding.EncodeToString([]byte("\x89PNG"))},
		{name: "未知媒体", blockType: "image", mediaType: "image/x-future", data: strings.Repeat("A", 2_000_000)},
		{name: "PDF document", blockType: "document", mediaType: "application/pdf", data: base64.StdEncoding.EncodeToString([]byte("%PDF-1.7")) + strings.Repeat("A", 2_000_000)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := countSemanticBlockMessage(t, tc, tt.blockType, tt.mediaType, tt.data)
			if got <= 0 || got > maxSemanticBlockTokens+32 {
				t.Fatalf("fallback 估算=%d，期望正数且不超过命名上限", got)
			}
		})
	}
}

func TestTokenCounterImageRejectsInvalidDimensions(t *testing.T) {
	tc := mustTokenCounter(t)
	for _, tcse := range []struct {
		name   string
		width  uint32
		height uint32
	}{
		{name: "零宽", width: 0, height: 897},
		{name: "异常大", width: ^uint32(0), height: ^uint32(0)},
	} {
		t.Run(tcse.name, func(t *testing.T) {
			data := base64.StdEncoding.EncodeToString(testPNGHeader(tcse.width, tcse.height))
			got := countImageMessage(t, tc, "image/png", data)
			if got <= 0 || got > maxSemanticBlockTokens+32 {
				t.Fatalf("异常尺寸估算=%d，期望有界 fallback", got)
			}
		})
	}
}

func TestTokenCounterUnknownBlockExcludesSourceData(t *testing.T) {
	tc := mustTokenCounter(t)
	count := func(data string) int {
		content, err := json.Marshal([]any{map[string]any{
			"type":   "future_media",
			"label":  "same metadata",
			"source": map[string]any{"type": "base64", "media_type": "future/type", "data": data},
		}})
		if err != nil {
			t.Fatal(err)
		}
		return tc.CountMessageTokens(Message{Role: "user", Content: content})
	}
	short := count("AAAA")
	long := count(strings.Repeat("A", 2_000_000))
	if long != short {
		t.Fatalf("未知 block 的 source.data 进入了文本估算: short=%d long=%d", short, long)
	}
}

func countImageMessage(t *testing.T, tc *TokenCounter, mediaType, data string) int {
	t.Helper()
	return countSemanticBlockMessage(t, tc, "image", mediaType, data)
}

func countSemanticBlockMessage(t *testing.T, tc *TokenCounter, blockType, mediaType, data string) int {
	t.Helper()
	return tc.CountMessageTokens(multimodalToolResultMessage(t, []any{
		map[string]any{"type": blockType, "source": map[string]any{"type": "base64", "media_type": mediaType, "data": data}},
	}))
}

func multimodalToolResultMessage(t *testing.T, nested []any) Message {
	t.Helper()
	content, err := json.Marshal([]any{map[string]any{
		"type": "tool_result", "tool_use_id": "toolu_test", "content": nested,
	}})
	if err != nil {
		t.Fatal(err)
	}
	return Message{Role: "user", Content: content}
}

func testImageHeader(mediaType string, width, height uint32) []byte {
	switch mediaType {
	case "image/png":
		return testPNGHeader(width, height)
	case "image/jpeg":
		return testJPEGHeader(width, height)
	case "image/gif":
		buf := []byte("GIF89a\x00\x00\x00\x00")
		binary.LittleEndian.PutUint16(buf[6:8], uint16(width))
		binary.LittleEndian.PutUint16(buf[8:10], uint16(height))
		return buf
	case "image/webp":
		buf := make([]byte, 30)
		copy(buf, "RIFF")
		copy(buf[8:], "WEBPVP8X")
		binary.LittleEndian.PutUint32(buf[16:20], 10)
		putUint24LE(buf[24:27], width-1)
		putUint24LE(buf[27:30], height-1)
		return buf
	default:
		panic("unsupported test media type")
	}
}

func testPNGHeader(width, height uint32) []byte {
	buf := make([]byte, 24)
	copy(buf, "\x89PNG\r\n\x1a\n")
	binary.BigEndian.PutUint32(buf[8:12], 13)
	copy(buf[12:16], "IHDR")
	binary.BigEndian.PutUint32(buf[16:20], width)
	binary.BigEndian.PutUint32(buf[20:24], height)
	return buf
}

func testJPEGHeader(width, height uint32) []byte {
	buf := []byte{0xff, 0xd8, 0xff, 0xe0, 0x00, 0x04, 0x00, 0x00, 0xff, 0xc0, 0x00, 0x11, 0x08, 0x00, 0x00, 0x00, 0x00, 0x03, 0x01, 0x11, 0x00, 0x02, 0x11, 0x00, 0x03, 0x11, 0x00}
	binary.BigEndian.PutUint16(buf[13:15], uint16(height))
	binary.BigEndian.PutUint16(buf[15:17], uint16(width))
	return buf
}

func putUint24LE(dst []byte, value uint32) {
	dst[0] = byte(value)
	dst[1] = byte(value >> 8)
	dst[2] = byte(value >> 16)
}

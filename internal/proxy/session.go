package proxy

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

// extractSessionID 从 HTTP 请求中提取 SessionID。
// 优先读取 X-Claude-Code-Session-Id header，缺失时回退到 UUID v4。
func extractSessionID(r *http.Request) string {
	if sid := r.Header.Get("X-Claude-Code-Session-Id"); sid != "" {
		return sid
	}

	// 回退：生成 UUID v4
	uuid := make([]byte, 16)
	if _, err := rand.Read(uuid); err != nil {
		// crypto/rand.Read 在极端情况下可能失败（如系统熵不足）
		// 返回空字符串让调用方决定如何处理
		return ""
	}

	// 设置 version 4 (0100xxxx)
	uuid[6] = (uuid[6] & 0x0f) | 0x40
	// 设置 variant 10xx (RFC 4122)
	uuid[8] = (uuid[8] & 0x3f) | 0x80

	sid := fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		uuid[0:4],
		uuid[4:6],
		uuid[6:8],
		uuid[8:10],
		uuid[10:16],
	)

	return sid
}

// isSubagent 检测当前请求是否来自 CC subagent。
// Phase 5 实现三重检测（OR 逻辑）：
//   Check A: system 字段包含 "cc_entrypoint=sdk-ts"（sdk-ts entrypoint）
//   Check B: model 字段包含 "haiku"（Haiku 模型）
//   Check C: 缺少 thinking 字段（无扩展思考）
// 参数 bodyMap 是解析后的请求体 JSON，messages 是提取后的消息数组。
func isSubagent(bodyMap map[string]json.RawMessage, messages []Message) bool {
	// 边缘情况：空消息不是 subagent
	if len(messages) == 0 {
		return false
	}

	// Check A — sdk-ts entrypoint（billing header 检测）
	if systemRaw, ok := bodyMap["system"]; ok {
		// 尝试解析为数组格式: [{"type": "text", "text": "cc_entrypoint=sdk-ts"}]
		var systemArray []json.RawMessage
		if err := json.Unmarshal(systemRaw, &systemArray); err == nil {
			for _, elem := range systemArray {
				var elemMap map[string]json.RawMessage
				if err := json.Unmarshal(elem, &elemMap); err == nil {
					if textRaw, ok := elemMap["text"]; ok {
						var text string
						if err := json.Unmarshal(textRaw, &text); err == nil {
							if strings.Contains(text, "cc_entrypoint=sdk-ts") {
								return true
							}
						}
					}
				}
			}
		} else {
			// 字符串格式: "cc_entrypoint=sdk-ts ..."
			var systemStr string
			if err := json.Unmarshal(systemRaw, &systemStr); err == nil {
				if strings.Contains(systemStr, "cc_entrypoint=sdk-ts") {
					return true
				}
			}
		}
	}

	// Check B — Haiku 模型
	if modelRaw, ok := bodyMap["model"]; ok {
		var model string
		if err := json.Unmarshal(modelRaw, &model); err == nil {
			if strings.Contains(model, "haiku") {
				return true
			}
		}
	}

	// Check C — 无 thinking 字段（无扩展思考 = subagent）
	if _, ok := bodyMap["thinking"]; !ok {
		return true
	}

	return false
}

// extractModelFromBody 从请求体 JSON 中提取 model 字段值。
// 若 model 字段缺失、非字符串类型或 body 解析失败，返回 "unknown"。
func extractModelFromBody(body []byte) string {
	if len(body) == 0 {
		return "unknown"
	}

	var data map[string]any
	if err := json.Unmarshal(body, &data); err != nil {
		return "unknown"
	}

	modelVal, ok := data["model"]
	if !ok {
		return "unknown"
	}

	modelStr, ok := modelVal.(string)
	if !ok {
		return "unknown"
	}

	if modelStr == "" {
		return "unknown"
	}

	return modelStr
}

package proxy

import (
	"bytes"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"unicode/utf8"
)

// assertEagerStubUTF8Clean 断言 stub 本身合法 UTF-8，且序列化后不含替换字符。
// encoding/json 对非法字节写出 6 字符转义 U+FFFD——那正是污染上游请求体的形态。
var jsonReplacementEscape = []byte{'\\', 'u', 'f', 'f', 'f', 'd'}

func assertEagerStubUTF8Clean(t *testing.T, label, stub string) {
	t.Helper()
	if !utf8.ValidString(stub) {
		t.Fatalf("%s 产出非法 UTF-8: %q", label, stub)
	}
	if strings.ContainsRune(stub, utf8.RuneError) {
		t.Fatalf("%s 含 U+FFFD 替换字符: %q", label, stub)
	}
	encoded, err := json.Marshal(stub)
	if err != nil {
		t.Fatalf("%s json.Marshal 失败: %v", label, err)
	}
	if bytes.Contains(encoded, jsonReplacementEscape) {
		t.Fatalf("%s 序列化后含替换字符转义: %s", label, encoded)
	}
}

func TestEagerStubToolResultsWithoutFrozenBoundary(t *testing.T) {
	large := strings.Repeat("large tool output ", 80)
	messages := []any{
		map[string]any{
			"role": "assistant",
			"content": []any{map[string]any{
				"type": "tool_use", "id": "tool-1", "name": "Read",
				"input": map[string]any{"file_path": "example.go"},
			}},
		},
		map[string]any{
			"role": "user",
			"content": []any{map[string]any{
				"type": "tool_result", "tool_use_id": "tool-1", "content": large,
			}},
		},
		map[string]any{"role": "assistant", "content": []any{map[string]any{"type": "text", "text": "done"}}},
	}

	out := EagerStubToolResults(messages, 0, func(text string) int { return len(text) })
	result := out[1].(map[string]any)["content"].([]any)[0].(map[string]any)["content"].(string)
	if !strings.HasPrefix(result, "[Read example.go") {
		t.Fatalf("无 Frozen 边界时旧 tool_result 未被 eager stub: %q", result)
	}
}

func TestEagerStubMatchesParallelToolResultsByID(t *testing.T) {
	large := strings.Repeat("tool output line\n", 80)
	messages := []any{
		map[string]any{"role": "assistant", "content": []any{
			map[string]any{"type": "tool_use", "id": "read-1", "name": "Read", "input": map[string]any{"file_path": "a.go"}},
			map[string]any{"type": "tool_use", "id": "bash-1", "name": "Bash", "input": map[string]any{"command": "go test ./..."}},
		}},
		map[string]any{"role": "user", "content": []any{
			map[string]any{"type": "tool_result", "tool_use_id": "bash-1", "content": large},
			map[string]any{"type": "tool_result", "tool_use_id": "read-1", "content": large},
		}},
		map[string]any{"role": "assistant", "content": []any{map[string]any{"type": "text", "text": "done"}}},
	}

	out := EagerStubToolResults(messages, 0, func(text string) int { return len(text) })
	blocks := out[1].(map[string]any)["content"].([]any)
	bashStub := blocks[0].(map[string]any)["content"].(string)
	readStub := blocks[1].(map[string]any)["content"].(string)
	if !strings.HasPrefix(bashStub, "[Bash: go test ./...") {
		t.Fatalf("Bash result 使用了错误工具元数据: %q", bashStub)
	}
	if !strings.HasPrefix(readStub, "[Read a.go") {
		t.Fatalf("Read result 使用了错误工具元数据: %q", readStub)
	}
}

func TestEagerStubMemoryConcurrentPersistenceNeverRegresses(t *testing.T) {
	memory := NewEagerStubMemory()
	var writesMu sync.Mutex
	var writes []string
	memory.SetPersistFunc(func(_, value string) {
		writesMu.Lock()
		writes = append(writes, value)
		writesMu.Unlock()
	})

	start := make(chan struct{})
	var wg sync.WaitGroup
	for _, id := range []string{"tool-a", "tool-b"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			memory.RecordStubbed("session", id)
		}()
	}
	close(start)
	wg.Wait()

	writesMu.Lock()
	defer writesMu.Unlock()
	if len(writes) != 2 {
		t.Fatalf("persist writes=%d, want 2", len(writes))
	}
	var final struct {
		ToolUseIDs []string `json:"tool_use_ids"`
	}
	if err := json.Unmarshal([]byte(writes[len(writes)-1]), &final); err != nil {
		t.Fatal(err)
	}
	if len(final.ToolUseIDs) != 2 || final.ToolUseIDs[0] != "tool-a" || final.ToolUseIDs[1] != "tool-b" {
		t.Fatalf("最终持久化快照倒退: %v", final.ToolUseIDs)
	}
}

func TestEagerStubBashCommandCJKBoundaryValidUTF8(t *testing.T) {
	// 80 的截断点落在第 27 个三字节字符中间——按 byte 切会产出非法 UTF-8。
	cmd := strings.Repeat("中", 100)
	input := map[string]any{"command": cmd}

	for _, tt := range []struct {
		label   string
		content string
	}{
		{label: "Bash 有 tail 分支", content: strings.Repeat("output line\n", 10)},
		{label: "Bash 无 tail 分支", content: "line1\nline2"},
	} {
		stub := buildEagerStub("Bash", input, tt.content)
		assertEagerStubUTF8Clean(t, tt.label, stub)
		if !strings.Contains(stub, strings.Repeat("中", 80)) {
			t.Fatalf("%s 截断早于 80 rune: %q", tt.label, stub)
		}
		if strings.Contains(stub, strings.Repeat("中", 81)) {
			t.Fatalf("%s 未在 80 rune 处截断: %q", tt.label, stub)
		}
	}

	// 未超限命令原样保留，不追加省略号（既有 [Bash: go test ./... 前缀契约）。
	short := buildEagerStub("Bash", map[string]any{"command": "go test ./..."}, "line1")
	if want := "[Bash: go test ./... — 1 lines]\nline1"; short != want {
		t.Fatalf("未超限命令被改写: got=%q want=%q", short, want)
	}
}

func TestEagerStubAgentContentCJKBoundaryValidUTF8(t *testing.T) {
	stub := buildEagerStub("Agent", map[string]any{"description": "分析"}, strings.Repeat("测", 300))
	assertEagerStubUTF8Clean(t, "Agent 分支", stub)
	if !strings.Contains(stub, strings.Repeat("测", 200)) {
		t.Fatalf("Agent 分支截断早于 200 rune: %q", stub)
	}
	if strings.Contains(stub, strings.Repeat("测", 201)) {
		t.Fatalf("Agent 分支未在 200 rune 处截断: %q", stub)
	}

	short := buildEagerStub("Agent", map[string]any{"description": "分析"}, "短结果")
	if want := "[Agent: 分析 — 短结果]"; short != want {
		t.Fatalf("未超限内容被改写: got=%q want=%q", short, want)
	}
}

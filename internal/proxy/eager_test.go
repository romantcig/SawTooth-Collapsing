package proxy

import (
	"encoding/json"
	"strings"
	"sync"
	"testing"
)

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

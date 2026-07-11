package proxy

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func TestPersistentUserContextRecognizesStableKeys(t *testing.T) {
	keys := []string{"claudeMd", "currentDate", "userEmail", "attachedProject"}
	for _, key := range keys {
		t.Run(key, func(t *testing.T) {
			raw := fmt.Sprintf(`{"role":"user","content":[{"type":"text","text":%q}],"isMeta":true,"agent_id":null}`, persistentReminder(key, "fictional-value"))
			var contextMessage Message
			if err := json.Unmarshal([]byte(raw), &contextMessage); err != nil {
				t.Fatalf("unmarshal context message: %v", err)
			}
			messages := []Message{
				{Role: "assistant", Content: json.RawMessage(`"older"`)},
				contextMessage,
				{Role: "assistant", Content: json.RawMessage(`"after context"`)},
				{Role: "user", Content: json.RawMessage(`"latest user"`)},
			}

			context := ExtractPersistentUserContext(messages)
			if context == nil {
				t.Fatalf("# %s context was not recognized", key)
			}
			got := mustMarshalJSON(t, context.Message)
			assertJSONEquivalent(t, got, []byte(raw))
		})
	}

	t.Run("combined", func(t *testing.T) {
		body := "<system-reminder>\n# claudeMd\nfictional-rule\n# currentDate\n2099-01-01\n# userEmail\nnone@example.invalid\n# attachedProject\nfictional-project\n</system-reminder>"
		messages := []Message{{Role: "user", Content: mustMarshalJSON(t, []map[string]any{{"type": "text", "text": body}})}}
		if context := ExtractPersistentUserContext(messages); context == nil {
			t.Fatal("combined stable headings were not recognized")
		}
	})
}

func TestPersistentUserContextDetachMixedBlocksAndPrepend(t *testing.T) {
	contextText := persistentReminder("claudeMd", "Always use the fictional salutation Chief.")
	var mixed Message
	raw := fmt.Sprintf(`{"role":"user","content":[{"type":"text","text":%q,"future_block":null},{"type":"tool_result","tool_use_id":"tool-1","content":"result"}],"isMeta":true,"agent_id":"agent-1"}`, "ordinary-before\n"+contextText+"\nordinary-after")
	if err := json.Unmarshal([]byte(raw), &mixed); err != nil {
		t.Fatalf("unmarshal mixed message: %v", err)
	}
	messages := []Message{
		{Role: "assistant", Content: json.RawMessage(`[ {"type":"tool_use","id":"tool-1","name":"Read","input":{}} ]`)},
		mixed,
		{Role: "user", Content: json.RawMessage(`"latest"`)},
	}

	history, context := DetachPersistentUserContext(messages)
	if context == nil {
		t.Fatal("expected detached persistent context")
	}
	if len(history) != len(messages) {
		t.Fatalf("mixed context message should remain in history: got %d messages, want %d", len(history), len(messages))
	}
	if got := allText(t, history[1]); !strings.Contains(got, "ordinary-before") || !strings.Contains(got, "ordinary-after") || strings.Contains(got, "# claudeMd") {
		t.Fatalf("mixed history text was not split safely: %q", got)
	}
	if countBlockType(t, history[0], "tool_use") != 1 || countBlockType(t, history[1], "tool_result") != 1 {
		t.Fatal("detach changed tool_use/tool_result pairing")
	}
	var historyObject map[string]any
	if err := json.Unmarshal(mustMarshalJSON(t, history[1]), &historyObject); err != nil {
		t.Fatal(err)
	}
	if _, ok := historyObject["agent_id"]; !ok {
		t.Fatal("detach dropped unknown message-level fields")
	}

	result := PrependPersistentUserContext(history, context)
	if len(result) != len(history)+1 {
		t.Fatalf("prepend length = %d, want %d", len(result), len(history)+1)
	}
	if got := persistentContextCount(result); got != 1 {
		t.Fatalf("persistent context count = %d, want exactly 1", got)
	}
	if !strings.Contains(string(result[0].Content), "# claudeMd") {
		t.Fatal("authoritative context was not prepended")
	}
	var firstObject map[string]json.RawMessage
	if err := json.Unmarshal(mustMarshalJSON(t, result[0]), &firstObject); err != nil {
		t.Fatal(err)
	}
	if string(firstObject["agent_id"]) != `"agent-1"` || string(firstObject["isMeta"]) != "true" {
		t.Fatalf("prepended context lost message metadata: %s", mustMarshalJSON(t, result[0]))
	}
}

func TestPersistentUserContextSelectsEarliestAndRemovesDuplicates(t *testing.T) {
	messages := []Message{
		persistentContextMessage("claudeMd", "authoritative-first"),
		{Role: "assistant", Content: json.RawMessage(`"middle"`)},
		persistentContextMessage("currentDate", "stale-second"),
		{Role: "user", Content: json.RawMessage(`"<system-reminder>\n# claudeMd\nmalformed without close"`)},
	}

	history, context := DetachPersistentUserContext(messages)
	if context == nil || !strings.Contains(string(context.Message.Content), "authoritative-first") {
		t.Fatalf("earliest context was not selected: %+v", context)
	}
	if persistentContextCount(history) != 0 {
		t.Fatalf("recognized duplicate context remained in history: %s", mustMarshalJSON(t, history))
	}
	if !strings.Contains(string(mustMarshalJSON(t, history)), "malformed without close") {
		t.Fatal("malformed candidate should remain fail-safe in history")
	}

	result := PrependPersistentUserContext(history, context)
	if persistentContextCount(result) != 1 || !strings.Contains(string(result[0].Content), "authoritative-first") {
		t.Fatalf("prepend did not keep exactly the authoritative context: %s", mustMarshalJSON(t, result))
	}
}

func TestPersistentUserContextNoContextPreservesHistory(t *testing.T) {
	messages := []Message{
		{Role: "user", Content: json.RawMessage(`"ordinary"`)},
		{Role: "assistant", Content: json.RawMessage(`"<system-reminder>unknown complete reminder</system-reminder>"`)},
	}
	history, context := DetachPersistentUserContext(messages)
	if context != nil {
		t.Fatalf("unexpected context: %+v", context)
	}
	assertJSONEquivalent(t, mustMarshalJSON(t, history), mustMarshalJSON(t, messages))
	assertJSONEquivalent(t, mustMarshalJSON(t, PrependPersistentUserContext(history, nil)), mustMarshalJSON(t, messages))
}

func persistentReminder(key, value string) string {
	return "<system-reminder>\nAs you answer, use this context:\n# " + key + "\n" + value + "\n</system-reminder>"
}

func persistentContextMessage(key, value string) Message {
	content, _ := json.Marshal([]map[string]any{{"type": "text", "text": persistentReminder(key, value)}})
	return Message{Role: "user", Content: content}
}

func persistentContextCount(messages []Message) int {
	total := 0
	for _, message := range messages {
		if ExtractPersistentUserContext([]Message{message}) != nil {
			total++
		}
	}
	return total
}

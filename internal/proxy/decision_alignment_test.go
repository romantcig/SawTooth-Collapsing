package proxy

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func TestDecisionDetectionMatchesYesMemStructuralRules(t *testing.T) {
	longAnalysis := strings.Repeat("analysis ", 60)
	messages := []Message{
		decisionAlignmentMessage(t, "assistant", longAnalysis),
		decisionAlignmentMessage(t, "user", "Use option B."),
		decisionAlignmentMessage(t, "user", "Should we use option C?"),
	}

	if !isDecisionMessage(messages, 1) {
		t.Fatal("承接长分析的短 user 答复应识别为 decision")
	}
	if isDecisionMessage(messages, 2) {
		t.Fatal("问句不应识别为 decision")
	}
}

func TestDecisionUsesYesMemIntensityBoostWhenStubbed(t *testing.T) {
	tracker := NewDecayTracker()
	tc, err := NewTokenCounter()
	if err != nil {
		t.Fatal(err)
	}
	messages := []Message{
		decisionAlignmentMessage(t, "assistant", strings.Repeat("analysis ", 80)),
		decisionAlignmentMessage(t, "user", "Use option B. "+strings.Repeat("decision detail ", 50)),
		decisionAlignmentMessage(t, "assistant", strings.Repeat("ordinary explanation ", 100)),
	}

	_, stats := stubifyMessages(messages, tc, "", 0, false, tracker, "session", 0, 0, 1)
	if !stats.IsDecision {
		t.Fatal("stubify 未识别 YesMem 风格的 user decision")
	}
	if got := tracker.GetStage("session", 1, 8, len(messages), 3.0); got != DecayFresh {
		t.Fatalf("decision 在 intensity boost 期间阶段 = %v，期望 %v", got, DecayFresh)
	}
	if got := tracker.GetStage("session", 2, 8, len(messages), 3.0); got != DecayMiddle {
		t.Fatalf("普通消息阶段 = %v，期望 %v", got, DecayMiddle)
	}
}

func TestApplyDecayBatchWritesBackMiddleAndOldText(t *testing.T) {
	tracker := NewDecayTracker()
	messages := []Message{
		decisionAlignmentMessage(t, "user", "start"),
		decisionAlignmentMessage(t, "assistant", strings.Repeat("historical detail ", 80)),
	}
	tracker.MarkStubbed("session", 1, 0, 0)

	middle, _ := tracker.ApplyDecayBatch(messages, "session", 300, 100, nil, "", 6)
	if bytes.Equal(middle[1].Content, messages[1].Content) {
		t.Fatal("DecayMiddle 计算后未写回消息")
	}
	old, _ := tracker.ApplyDecayBatch(messages, "session", 300, 100, nil, "", 20)
	if bytes.Equal(old[1].Content, messages[1].Content) {
		t.Fatal("DecayOld 计算后未写回消息")
	}
}

func decisionAlignmentMessage(t *testing.T, role, text string) Message {
	t.Helper()
	content, err := json.Marshal([]map[string]any{{"type": "text", "text": text}})
	if err != nil {
		t.Fatal(err)
	}
	return Message{Role: role, Content: content}
}

package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestTotalInputTokens(t *testing.T) {
	tests := []struct {
		name  string
		usage map[string]any
		want  int
	}{
		{name: "真实 cache hit 25", usage: map[string]any{"input_tokens": 196, "cache_creation_input_tokens": 0, "cache_read_input_tokens": 93056}, want: 93252},
		{name: "真实 cache hit 26", usage: map[string]any{"input_tokens": json.Number("5559"), "cache_read_input_tokens": float64(15744)}, want: 21303},
		{name: "缺失字段", usage: map[string]any{"input_tokens": int64(7)}, want: 7},
		{name: "负数与非数字忽略", usage: map[string]any{"input_tokens": -1, "cache_creation_input_tokens": "secret", "cache_read_input_tokens": 4}, want: 4},
		{name: "NaN Inf 忽略", usage: map[string]any{"input_tokens": math.NaN(), "cache_creation_input_tokens": math.Inf(1), "cache_read_input_tokens": 3}, want: 3},
		{name: "单字段饱和", usage: map[string]any{"input_tokens": json.Number("9223372036854775807")}, want: math.MaxInt},
		{name: "求和饱和", usage: map[string]any{"input_tokens": math.MaxInt, "cache_creation_input_tokens": math.MaxInt, "cache_read_input_tokens": math.MaxInt}, want: math.MaxInt},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := totalInputTokens(tt.usage); got != tt.want {
				t.Fatalf("totalInputTokens() = %d, want %d", got, tt.want)
			}
		})
	}
}

// 只衰减客户端上下文求和的三项；output_tokens 原样透传。
// cache_* 逃逸衰减会让 input+cache_creation+cache_read 求和混入两套标尺。
func TestDeflateUsageDeflatesCacheFields(t *testing.T) {
	usage := map[string]any{
		"input_tokens":                100,
		"cache_creation_input_tokens": 201,
		"cache_read_input_tokens":     303,
		"output_tokens":               11,
		"total_tokens":                999,
	}
	deflateUsage(usage, 0.7)

	want := map[string]any{
		"input_tokens":                float64(70),
		"cache_creation_input_tokens": float64(140),
		"cache_read_input_tokens":     float64(212),
		"output_tokens":               11,           // 未衰减，连类型都不该变
		"total_tokens":                float64(433), // 70+140+212 + 未衰减的 11
	}
	for field, expected := range want {
		if usage[field] != expected {
			t.Fatalf("%s=%v, want %v; usage=%+v", field, usage[field], expected, usage)
		}
	}
}

// factor >= 1 表示关闭：usage 必须逐字段原样保留，连 total_tokens 也不重算。
func TestDeflateUsageDisabledLeavesUsageUntouched(t *testing.T) {
	for _, factor := range []float64{1.0, 1.5} {
		usage := map[string]any{
			"input_tokens":                100,
			"cache_creation_input_tokens": 201,
			"cache_read_input_tokens":     303,
			"output_tokens":               11,
			"total_tokens":                999, // 故意与各项之和不一致，用来证明没被重算
		}
		deflateUsage(usage, factor)

		want := map[string]any{
			"input_tokens": 100, "cache_creation_input_tokens": 201,
			"cache_read_input_tokens": 303, "output_tokens": 11, "total_tokens": 999,
		}
		for field, expected := range want {
			if usage[field] != expected {
				t.Fatalf("factor=%v 时 %s=%v(%T), want %v(%T)；usage 应逐字段原样保留: %+v",
					factor, field, usage[field], usage[field], expected, expected, usage)
			}
		}
	}
}

// 值为 0 的 cache 字段保持 0（D-06），不因纳入 tokenFields 而改变。
func TestDeflateUsageKeepsZeroCacheFields(t *testing.T) {
	usage := map[string]any{
		"input_tokens":                100,
		"cache_creation_input_tokens": 0,
		"cache_read_input_tokens":     0,
	}
	deflateUsage(usage, 0.7)

	if usage["cache_creation_input_tokens"] != 0 || usage["cache_read_input_tokens"] != 0 {
		t.Fatalf("零值 cache 字段被改写: %+v", usage)
	}
}

// deflation 默认关闭，且 `deflation: 0` 表示关闭而非"把 usage 全报成 0"。
func TestDeflationDefaultsOffAndZeroMeansDisabled(t *testing.T) {
	if got := DefaultConfig().Proxy.Deflation; got != 1.0 {
		t.Fatalf("默认 deflation=%v, want 1.0（关闭）", got)
	}

	for name, body := range map[string]string{
		"显式 0": "proxy:\n  deflation: 0\n",
		"越界负值": "proxy:\n  deflation: -1\n",
		"越界超限": "proxy:\n  deflation: 2\n",
		"未指定":  "proxy:\n  target: https://api.anthropic.com\n",
	} {
		path := filepath.Join(tempDirRetryCleanup(t), "sawtooth.yaml")
		if err := os.WriteFile(path, []byte(body), 0600); err != nil {
			t.Fatalf("%s: 写入配置: %v", name, err)
		}
		loaded, err := LoadConfig(path)
		if err != nil {
			t.Fatalf("%s: LoadConfig: %v", name, err)
		}
		if loaded.Proxy.Deflation != 1.0 {
			t.Fatalf("%s: deflation=%v, want 1.0", name, loaded.Proxy.Deflation)
		}
	}
}

// deflation 关闭时，message_delta 的输入侧剥离仍必须生效——上游中转站
// 补报的矛盾值一旦透传，客户端计数会跳变，且没有衰减再替它兜底。
func TestMessageDeltaStrippedEvenWhenDeflationOff(t *testing.T) {
	s := NewServer(Config{Proxy: ProxyConfig{Deflation: 1.0}})
	event := []string{
		"event: message_delta",
		`data: {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"input_tokens":95944,"cache_read_input_tokens":82432,"output_tokens":246}}`,
	}

	processed := s.processSSEEvent(event)
	var data map[string]any
	if err := json.Unmarshal([]byte(strings.TrimPrefix(processed[1], "data: ")), &data); err != nil {
		t.Fatalf("解析转发后的 data: %v; line=%q", err, processed[1])
	}
	usage := data["usage"].(map[string]any)
	for _, field := range []string{"input_tokens", "cache_creation_input_tokens", "cache_read_input_tokens"} {
		if _, present := usage[field]; present {
			t.Fatalf("deflation 关闭时 %s 未被剥离: %+v", field, usage)
		}
	}
	if usage["output_tokens"] != float64(246) {
		t.Fatalf("output_tokens=%v, want 246（关闭时不衰减）", usage["output_tokens"])
	}
}

// message_delta 的输入侧字段必须被剥离——客户端对这三个字段是「非 null 即覆盖」，
// 上游若在此补报与 message_start 矛盾的值，就会造成上下文计数跳变。
func TestProcessSSEEventStripsMessageDeltaInputUsage(t *testing.T) {
	s := NewServer(Config{Proxy: ProxyConfig{Deflation: 0.5}})
	event := []string{
		"event: message_delta",
		`data: {"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"input_tokens":90000,"cache_creation_input_tokens":1024,"cache_read_input_tokens":22016,"output_tokens":246}}`,
	}

	processed := s.processSSEEvent(event)
	if len(processed) != 2 || processed[0] != "event: message_delta" {
		t.Fatalf("事件结构被破坏: %#v", processed)
	}

	var data map[string]any
	if err := json.Unmarshal([]byte(strings.TrimPrefix(processed[1], "data: ")), &data); err != nil {
		t.Fatalf("解析转发后的 data: %v; line=%q", err, processed[1])
	}
	usage, ok := data["usage"].(map[string]any)
	if !ok {
		t.Fatalf("usage 丢失: %+v", data)
	}
	for _, field := range []string{"input_tokens", "cache_creation_input_tokens", "cache_read_input_tokens"} {
		if _, present := usage[field]; present {
			t.Fatalf("%s 未被剥离: %+v", field, usage)
		}
	}
	if usage["output_tokens"] != float64(246) {
		t.Fatalf("output_tokens=%v, want 246（不参与衰减）", usage["output_tokens"])
	}
	if delta, _ := data["delta"].(map[string]any); delta["stop_reason"] != "tool_use" {
		t.Fatalf("delta 未原样保留: %+v", data["delta"])
	}
}

// message_start 是客户端唯一的输入侧 usage 来源，四个字段都必须打折。
func TestProcessSSEEventMessageStartDeflatesCacheFields(t *testing.T) {
	s := NewServer(Config{Proxy: ProxyConfig{Deflation: 0.5}})
	event := []string{
		"event: message_start",
		`data: {"type":"message_start","message":{"type":"message","usage":{"input_tokens":196,"cache_creation_input_tokens":1024,"cache_read_input_tokens":93056,"output_tokens":20}}}`,
	}

	processed := s.processSSEEvent(event)
	var data map[string]any
	if err := json.Unmarshal([]byte(strings.TrimPrefix(processed[1], "data: ")), &data); err != nil {
		t.Fatalf("解析转发后的 data: %v; line=%q", err, processed[1])
	}
	usage := data["message"].(map[string]any)["usage"].(map[string]any)

	want := map[string]float64{
		"input_tokens":                98,
		"cache_creation_input_tokens": 512,
		"cache_read_input_tokens":     46528,
		"output_tokens":               20, // 不参与衰减
	}
	for field, expected := range want {
		if usage[field] != expected {
			t.Fatalf("%s=%v, want %v; usage=%+v", field, usage[field], expected, usage)
		}
	}
}

// 端到端：ST 的 pressure baseline 取 deflation 之前的真值（observe 早于
// processSSEEvent），而转发给客户端的流已完成四字段衰减与 message_delta 剥离。
func TestHandleSSEStripsDeltaWhileBaselineKeepsTruth(t *testing.T) {
	trigger := NewSawtoothTrigger(time.Hour, 50000, 1000)
	var persisted string
	trigger.SetPersistFunc(func(_ string, value string) { persisted = value })
	s := NewServer(Config{Proxy: ProxyConfig{Deflation: 0.5}})
	s.Sawtooth = trigger
	system := json.RawMessage(`[{"type":"text","text":"delta system"}]`)
	tools := json.RawMessage(`[{"name":"delta_tool","input_schema":{"type":"object"}}]`)
	forwarded := pipelineMessages(29, 6)
	meta := newRequestMeta(3, "sse-delta-strip")
	meta.PressureDecision = pressureDecision{
		Available:         true,
		SystemFingerprint: fingerprintTopLevelJSON(system),
		ToolsFingerprint:  fingerprintTopLevelJSON(tools),
	}
	// 本场景入口 raw == wire（无改写），baseline 契约坐标与 wire 同值，
	// 避免写出 MsgCount=0 的退化 baseline。
	meta.PressureEntryCoordinates = pressureEntryCoordinates{
		MessageCount:              len(forwarded),
		MessagesPrefixFingerprint: fingerprintMessagesPrefix(forwarded, len(forwarded)),
	}
	markForwardedPressureCoordinates(meta, forwardedPressureBody(t, system, tools, forwarded), nil)

	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": {"text/event-stream"}},
		Body: io.NopCloser(strings.NewReader("event: message_start\n" +
			`data: {"type":"message_start","message":{"type":"message","usage":{"input_tokens":196,"cache_creation_input_tokens":1024,"cache_read_input_tokens":93056,"output_tokens":20}}}` + "\n\n" +
			"event: message_delta\n" +
			`data: {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"input_tokens":90000,"cache_read_input_tokens":22016,"output_tokens":246}}` + "\n\n" +
			"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n")),
	}
	recorder := httptest.NewRecorder()
	s.handleSSE(recorder, resp, meta, time.Now(), "model", 4)

	var state persistedState
	if err := json.Unmarshal([]byte(persisted), &state); err != nil {
		t.Fatalf("解析持久状态: %v; raw=%q", err, persisted)
	}
	if state.Tokens != 94276 {
		t.Fatalf("baseline tokens=%d, want 94276（message_start 的未衰减三字段之和）", state.Tokens)
	}

	body := recorder.Body.String()
	for _, forbidden := range []string{`"input_tokens":90000`, `"input_tokens":45000`, `"cache_read_input_tokens":22016`, `"cache_read_input_tokens":11008`} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("message_delta 输入侧 usage 泄漏 %s: %s", forbidden, body)
		}
	}
	for _, required := range []string{`"input_tokens":98`, `"cache_creation_input_tokens":512`, `"cache_read_input_tokens":46528`, `"output_tokens":246`} {
		if !strings.Contains(body, required) {
			t.Fatalf("缺少 %s: %s", required, body)
		}
	}
}

// forwardedPressureBody 组装一份真实的上游 wire body，供测试通过
// markForwardedPressureCoordinates 绑定坐标——而不是手工置位
// ForwardedCoordinatesBound，那等于在测试里给 invariant 留后门。
func forwardedPressureBody(t *testing.T, system, tools json.RawMessage, messages []Message) []byte {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"system":   system,
		"tools":    tools,
		"messages": messages,
	})
	if err != nil {
		t.Fatalf("marshal forwarded body: %v", err)
	}
	return body
}

func TestHandleSSEPressureBaseline(t *testing.T) {
	trigger := NewSawtoothTrigger(time.Hour, 50000, 1000)
	var persisted string
	trigger.SetPersistFunc(func(_ string, value string) { persisted = value })
	s := NewServer(Config{Proxy: ProxyConfig{Deflation: 0.5}})
	s.Sawtooth = trigger
	system := json.RawMessage(`[{"type":"text","text":"sse system"}]`)
	tools := json.RawMessage(`[{"name":"sse_tool","input_schema":{"type":"object"}}]`)
	forwarded := pipelineMessages(37, 6)
	meta := newRequestMeta(1, "sse-cache")
	meta.PressureDecision = pressureDecision{
		Available:         true,
		SystemFingerprint: fingerprintTopLevelJSON(system),
		ToolsFingerprint:  fingerprintTopLevelJSON(tools),
	}
	// 本场景入口 raw == wire（无改写），baseline 契约坐标与 wire 同值。
	meta.PressureEntryCoordinates = pressureEntryCoordinates{
		MessageCount:              len(forwarded),
		MessagesPrefixFingerprint: fingerprintMessagesPrefix(forwarded, len(forwarded)),
	}
	markForwardedPressureCoordinates(meta, forwardedPressureBody(t, system, tools, forwarded), nil)
	if !meta.PressureDecision.ForwardedCoordinatesBound {
		t.Fatalf("forwarded 坐标未绑定: %+v", meta.PressureDecision)
	}
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": {"text/event-stream"}},
		Body: io.NopCloser(strings.NewReader("event: message_start\n" +
			"data: {\"type\":\"message_start\",\"message\":{\"type\":\"message\",\"usage\":{\"input_tokens\":196,\"cache_creation_input_tokens\":0,\"cache_read_input_tokens\":93056,\"output_tokens\":20}}}\n\n" +
			"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n")),
	}
	recorder := httptest.NewRecorder()
	s.handleSSE(recorder, resp, meta, time.Now(), "model", 4)

	var state persistedState
	if err := json.Unmarshal([]byte(persisted), &state); err != nil {
		t.Fatalf("解析持久状态: %v; raw=%q", err, persisted)
	}
	if state.Tokens != 93252 || state.MsgCount != len(forwarded) || state.SystemFingerprint != fingerprintTopLevelJSON(system) || state.ToolsFingerprint != fingerprintTopLevelJSON(tools) || state.MessagesPrefixFingerprint != fingerprintMessagesPrefix(forwarded, len(forwarded)) {
		t.Fatalf("SSE 持久状态=%+v, want actual=93252 forwarded_count=%d", state, len(forwarded))
	}
	if meta.BaselineUpdateKind != pressureBaselineUpdateExact {
		t.Fatalf("SSE baseline update kind=%q, want exact", meta.BaselineUpdateKind)
	}
	if strings.Contains(recorder.Body.String(), `"input_tokens":196`) || !strings.Contains(recorder.Body.String(), `"input_tokens":98`) {
		t.Fatalf("客户端 deflation 行为变化: %s", recorder.Body.String())
	}
}

func TestHandleJSONPressureBaseline(t *testing.T) {
	trigger := NewSawtoothTrigger(time.Hour, 50000, 1000)
	var persisted string
	trigger.SetPersistFunc(func(_ string, value string) { persisted = value })
	s := NewServer(Config{Proxy: ProxyConfig{Deflation: 0.5}})
	s.Sawtooth = trigger
	system := json.RawMessage(`[{"type":"text","text":"json system"}]`)
	tools := json.RawMessage(`[{"name":"json_tool","input_schema":{"type":"object"}}]`)
	forwarded := pipelineMessages(41, 6)
	meta := newRequestMeta(2, "json-cache")
	meta.PressureDecision = pressureDecision{
		Available:         true,
		SystemFingerprint: fingerprintTopLevelJSON(system),
		ToolsFingerprint:  fingerprintTopLevelJSON(tools),
	}
	// 本场景入口 raw == wire（无改写），baseline 契约坐标与 wire 同值。
	meta.PressureEntryCoordinates = pressureEntryCoordinates{
		MessageCount:              len(forwarded),
		MessagesPrefixFingerprint: fingerprintMessagesPrefix(forwarded, len(forwarded)),
	}
	markForwardedPressureCoordinates(meta, forwardedPressureBody(t, system, tools, forwarded), nil)
	if !meta.PressureDecision.ForwardedCoordinatesBound {
		t.Fatalf("forwarded 坐标未绑定: %+v", meta.PressureDecision)
	}
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": {"application/json"}},
		Body:       io.NopCloser(strings.NewReader(`{"type":"message","usage":{"input_tokens":196,"cache_creation_input_tokens":0,"cache_read_input_tokens":93056,"output_tokens":20}}`)),
	}
	recorder := httptest.NewRecorder()
	s.handleJSON(recorder, resp, meta, time.Now(), "model", 3)

	var state persistedState
	if err := json.Unmarshal([]byte(persisted), &state); err != nil {
		t.Fatalf("解析持久状态: %v; raw=%q", err, persisted)
	}
	if state.Tokens != 93252 || state.MsgCount != len(forwarded) || state.SystemFingerprint != fingerprintTopLevelJSON(system) || state.ToolsFingerprint != fingerprintTopLevelJSON(tools) || state.MessagesPrefixFingerprint != fingerprintMessagesPrefix(forwarded, len(forwarded)) {
		t.Fatalf("JSON 持久状态=%+v, want actual=93252 forwarded_count=%d", state, len(forwarded))
	}
	if meta.BaselineUpdateKind != pressureBaselineUpdateExact {
		t.Fatalf("JSON baseline update kind=%q, want exact", meta.BaselineUpdateKind)
	}
	if strings.Contains(recorder.Body.String(), `"input_tokens":196`) || !strings.Contains(recorder.Body.String(), `"input_tokens":98`) {
		t.Fatalf("客户端 deflation 行为变化: %s", recorder.Body.String())
	}

	restored := NewSawtoothTrigger(time.Hour, 50000, 1000)
	restored.SetLoadFunc(func(key string) (string, bool) {
		return persisted, key == "sawtooth:json-cache"
	})
	restoredBaseline := restored.PressureBaseline("json-cache")
	if got := restored.ShouldTrigger("json-cache", restoredBaseline.ActualTokens); got != TriggerEmergency {
		t.Fatalf("冷启动 trigger=%q, want %q", got, TriggerEmergency)
	}
}

func TestForwardRawOutOfOrderResponsesKeepNewestRequestBaseline(t *testing.T) {
	const sessionID = "out-of-order-baseline"
	requestAStarted := make(chan struct{})
	releaseA := make(chan struct{})
	requestBDone := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Model string `json:"model"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode upstream request: %v", err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		switch body.Model {
		case "request-a":
			close(requestAStarted)
			<-releaseA
			_, _ = w.Write([]byte(`{"type":"message","usage":{"input_tokens":111}}`))
		case "request-b":
			_, _ = w.Write([]byte(`{"type":"message","usage":{"input_tokens":222}}`))
			close(requestBDone)
		default:
			t.Errorf("unexpected model %q", body.Model)
		}
	}))
	defer upstream.Close()

	trigger := NewSawtoothTrigger(time.Hour, 50_000, 1000)
	var persistedMu sync.Mutex
	var persisted string
	trigger.SetPersistFunc(func(_ string, value string) {
		persistedMu.Lock()
		persisted = value
		persistedMu.Unlock()
	})
	debugDir := tempDirRetryCleanup(t)
	s := NewServer(Config{
		Proxy: ProxyConfig{Target: upstream.URL, Deflation: 1},
		Debug: DebugConfig{Enabled: true, DataDir: debugDir},
	})
	s.Sawtooth = trigger
	fingerprint := fingerprintTopLevelJSON(nil)

	makeRequest := func(model string, messages []Message, generation uint64) (*http.Request, *requestMeta) {
		body, err := json.Marshal(map[string]any{"model": model, "messages": messages})
		if err != nil {
			t.Fatal(err)
		}
		req := httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body))
		meta := newRequestMeta(generation, sessionID)
		meta.BaselineGeneration = generation
		meta.PressureDecision = pressureDecision{
			Available: true, MessageCount: len(messages),
			SystemFingerprint: fingerprint, ToolsFingerprint: fingerprint,
			MessagesPrefixFingerprint: fingerprintMessagesPrefix(messages, len(messages)),
		}
		meta.PressureEntryCoordinates = pressureEntryCoordinates{
			MessageCount:              len(messages),
			MessagesPrefixFingerprint: fingerprintMessagesPrefix(messages, len(messages)),
		}
		return req, meta
	}

	messagesA := pipelineMessages(1, 2)
	generationA := trigger.BeginPressureRequest(sessionID)
	reqA, metaA := makeRequest("request-a", messagesA, generationA)
	doneA := make(chan struct{})
	go func() {
		s.forwardRaw(httptest.NewRecorder(), reqA, metaA)
		close(doneA)
	}()
	<-requestAStarted

	messagesB := pipelineMessages(2, 3)
	generationB := trigger.BeginPressureRequest(sessionID)
	reqB, metaB := makeRequest("request-b", messagesB, generationB)
	s.forwardRaw(httptest.NewRecorder(), reqB, metaB)
	<-requestBDone
	close(releaseA)
	<-doneA

	baseline := trigger.PressureBaseline(sessionID)
	if !baseline.Available || baseline.ActualTokens != 222 || baseline.MessageCount != len(messagesB) ||
		baseline.MessagesPrefixFingerprint != metaB.PressureDecision.MessagesPrefixFingerprint {
		t.Fatalf("旧响应覆盖了较新内存 baseline: %+v", baseline)
	}
	persistedMu.Lock()
	rawPersisted := persisted
	persistedMu.Unlock()
	var state persistedState
	if err := json.Unmarshal([]byte(rawPersisted), &state); err != nil {
		t.Fatalf("decode persisted baseline: %v raw=%q", err, rawPersisted)
	}
	if state.Tokens != 222 || state.MsgCount != len(messagesB) || state.MessagesPrefixFingerprint != metaB.PressureDecision.MessagesPrefixFingerprint {
		t.Fatalf("旧响应覆盖了较新持久 baseline: %+v", state)
	}
	baselineUpdatedByRequest := make(map[uint64]bool)
	for _, data := range readDebugFactFiles(t, debugDir, sessionID) {
		var fact debugFact
		if err := json.Unmarshal(data, &fact); err != nil {
			t.Fatal(err)
		}
		if fact.Stage != debugStageResponseUsage || fact.BaselineUpdated == nil {
			continue
		}
		baselineUpdatedByRequest[fact.RequestID] = *fact.BaselineUpdated
	}
	if updated, ok := baselineUpdatedByRequest[metaB.ID]; !ok || !updated {
		t.Fatalf("较新响应 baseline_updated=%v present=%v, want true", updated, ok)
	}
	if updated, ok := baselineUpdatedByRequest[metaA.ID]; !ok || updated {
		t.Fatalf("较旧迟到响应 baseline_updated=%v present=%v, want false", updated, ok)
	}
}

func TestHandleJSONAuxiliaryDoesNotUpdate(t *testing.T) {
	testHandleAuxiliaryDoesNotUpdate(t, false)
}

func TestHandleSSEAuxiliaryDoesNotUpdate(t *testing.T) {
	testHandleAuxiliaryDoesNotUpdate(t, true)
}

func testHandleAuxiliaryDoesNotUpdate(t *testing.T, sse bool) {
	t.Helper()
	for _, tc := range []struct {
		name string
		meta *requestMeta
	}{
		{name: "session title", meta: &requestMeta{RequestKind: requestKindSessionTitle}},
		{name: "subagent", meta: &requestMeta{AgentRole: agentRoleSubagent}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const sessionID = "auxiliary-baseline"
			trigger := NewSawtoothTrigger(time.Hour, 50000, 1000)
			oldSystem := fingerprintTopLevelJSON(json.RawMessage(`"old system"`))
			oldTools := fingerprintTopLevelJSON(json.RawMessage(`[]`))
			trigger.UpdatePressureBaseline(sessionID, 777, 9, oldSystem, oldTools, strings.Repeat("c", 64))
			before := trigger.PressureBaseline(sessionID)
			persistCalls := 0
			trigger.SetPersistFunc(func(_ string, _ string) { persistCalls++ })
			s := NewServer(Config{Proxy: ProxyConfig{Deflation: 0.5}})
			s.Sawtooth = trigger
			meta := newRequestMeta(10, sessionID)
			meta.RequestKind = tc.meta.RequestKind
			meta.AgentRole = tc.meta.AgentRole
			meta.PressureDecision = pressureDecision{
				Available:         true,
				MessageCount:      20,
				SystemFingerprint: fingerprintTopLevelJSON(json.RawMessage(`"new system"`)),
				ToolsFingerprint:  fingerprintTopLevelJSON(json.RawMessage(`[{"name":"new"}]`)),
			}
			recorder := httptest.NewRecorder()
			if sse {
				resp := &http.Response{
					StatusCode: http.StatusOK,
					Header:     http.Header{"Content-Type": {"text/event-stream"}},
					Body: io.NopCloser(strings.NewReader("event: message_start\n" +
						"data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":196,\"cache_read_input_tokens\":93056}}}\n\n")),
				}
				s.handleSSE(recorder, resp, meta, time.Now(), "model", 2)
			} else {
				resp := &http.Response{
					StatusCode: http.StatusOK,
					Header:     http.Header{"Content-Type": {"application/json"}},
					Body:       io.NopCloser(strings.NewReader(`{"usage":{"input_tokens":196,"cache_read_input_tokens":93056}}`)),
				}
				s.handleJSON(recorder, resp, meta, time.Now(), "model", 2)
			}
			if persistCalls != 0 {
				t.Fatalf("auxiliary persisted baseline %d times", persistCalls)
			}
			if after := trigger.PressureBaseline(sessionID); after != before {
				t.Fatalf("auxiliary changed baseline\nbefore=%+v\nafter=%+v", before, after)
			}
			if strings.Contains(recorder.Body.String(), `"input_tokens":196`) || !strings.Contains(recorder.Body.String(), `"input_tokens":98`) {
				t.Fatalf("auxiliary deflation changed: %s", recorder.Body.String())
			}
		})
	}
}

func TestHandleJSONFailureDoesNotUpdate(t *testing.T) {
	testHandleFailureDoesNotUpdate(t, false)
	t.Run("upstream transport", func(t *testing.T) {
		const sessionID = "upstream-failure-baseline"
		trigger := NewSawtoothTrigger(time.Hour, 50000, 1000)
		fingerprint := fingerprintTopLevelJSON(nil)
		trigger.UpdatePressureBaseline(sessionID, 777, 9, fingerprint, fingerprint, strings.Repeat("d", 64))
		before := trigger.PressureBaseline(sessionID)
		persistCalls := 0
		trigger.SetPersistFunc(func(_ string, _ string) { persistCalls++ })
		s := NewServer(Config{Proxy: ProxyConfig{Target: "https://upstream.invalid", Deflation: 0.5}})
		s.Sawtooth = trigger
		s.HTTPClient = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return nil, io.ErrUnexpectedEOF
		})}
		meta := newRequestMeta(21, sessionID)
		meta.PressureDecision = pressureDecision{Available: true, MessageCount: 10, SystemFingerprint: fingerprint, ToolsFingerprint: fingerprint}
		req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"model":"test","messages":[]}`))
		recorder := httptest.NewRecorder()
		s.forwardRaw(recorder, req, meta)
		if persistCalls != 0 {
			t.Fatalf("upstream failure persisted baseline %d times", persistCalls)
		}
		if after := trigger.PressureBaseline(sessionID); after != before {
			t.Fatalf("upstream failure changed baseline\nbefore=%+v\nafter=%+v", before, after)
		}
	})
}

func TestHandleSSEFailureDoesNotUpdate(t *testing.T) {
	testHandleFailureDoesNotUpdate(t, true)
}

type readThenErrorCloser struct {
	reader *strings.Reader
	err    error
}

func (r *readThenErrorCloser) Read(p []byte) (int, error) {
	if r.reader.Len() > 0 {
		return r.reader.Read(p)
	}
	if r.err != nil {
		err := r.err
		r.err = nil
		return 0, err
	}
	return 0, io.EOF
}

func (r *readThenErrorCloser) Close() error { return nil }

func TestHandleSSEInterruptedAfterMessageStartDiscardsPendingUsage(t *testing.T) {
	const sessionID = "sse-interrupted-after-start"
	trigger := NewSawtoothTrigger(time.Hour, 50_000, 1000)
	fingerprint := fingerprintTopLevelJSON(nil)
	trigger.UpdatePressureBaseline(sessionID, 777, 9, fingerprint, fingerprint, strings.Repeat("a", 64))
	before := trigger.PressureBaseline(sessionID)
	persistCalls := 0
	trigger.SetPersistFunc(func(_ string, _ string) { persistCalls++ })
	dataDir := tempDirRetryCleanup(t)
	s := NewServer(Config{Proxy: ProxyConfig{Deflation: 0.5}, Debug: DebugConfig{Enabled: true, DataDir: dataDir}})
	s.Sawtooth = trigger
	meta := newRequestMeta(22, sessionID)
	meta.PressureDecision = pressureDecision{
		Available:                 true,
		SelectedPressure:          700,
		MessageCount:              10,
		SystemFingerprint:         fingerprint,
		ToolsFingerprint:          fingerprint,
		MessagesPrefixFingerprint: strings.Repeat("b", 64),
	}
	stamp := time.Now()
	s.writePressureDecisionDebugFacts(meta, stamp)
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": {"text/event-stream"}},
		Body: &readThenErrorCloser{
			reader: strings.NewReader("event: message_start\n" +
				"data: {\"type\":\"message_start\",\"message\":{\"type\":\"message\",\"usage\":{\"input_tokens\":196,\"cache_read_input_tokens\":93056}}}\n\n" +
				"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"partial\"}}\n\n"),
			err: io.ErrUnexpectedEOF,
		},
	}

	result := s.handleSSE(httptest.NewRecorder(), resp, meta, stamp, "model", 2)
	if !errors.Is(result.err, io.ErrUnexpectedEOF) {
		t.Fatalf("stream error=%v, want %v", result.err, io.ErrUnexpectedEOF)
	}
	if persistCalls != 0 {
		t.Fatalf("interrupted stream persisted baseline %d times", persistCalls)
	}
	if after := trigger.PressureBaseline(sessionID); after != before {
		t.Fatalf("interrupted stream changed baseline\nbefore=%+v\nafter=%+v", before, after)
	}
	facts := debugFactsByStage(t, dataDir, sessionID)
	if len(facts) != 1 || facts[debugStagePressureDecision] == nil {
		t.Fatalf("interrupted stream facts=%v, want only pressure decision", facts)
	}
	if _, ok := facts[debugStageResponseUsage]; ok {
		t.Fatalf("interrupted stream wrote response_usage: %v", facts)
	}
}

func TestHandleJSONRejectsUntrustedUsageBaseline(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "non-message", body: `{"type":"error","usage":{"input_tokens":99999}}`},
		{name: "fractional", body: `{"type":"message","usage":{"input_tokens":1.5,"cache_read_input_tokens":2}}`},
		{name: "negative mixed with positive", body: `{"type":"message","usage":{"input_tokens":-1,"cache_read_input_tokens":99999}}`},
		{name: "NaN", body: `{"type":"message","usage":{"input_tokens":NaN}}`},
		{name: "infinite exponent", body: `{"type":"message","usage":{"input_tokens":1e999}}`},
		{name: "integer overflow", body: `{"type":"message","usage":{"input_tokens":9223372036854775808}}`},
	}
	for index, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sessionID := fmt.Sprintf("json-untrusted-usage-%d", index)
			trigger := NewSawtoothTrigger(time.Hour, 50_000, 1000)
			fingerprint := fingerprintTopLevelJSON(nil)
			trigger.UpdatePressureBaseline(sessionID, 777, 9, fingerprint, fingerprint, strings.Repeat("a", 64))
			before := trigger.PressureBaseline(sessionID)
			persistCalls := 0
			trigger.SetPersistFunc(func(_ string, _ string) { persistCalls++ })
			dataDir := tempDirRetryCleanup(t)
			s := NewServer(Config{Proxy: ProxyConfig{Deflation: 0.5}, Debug: DebugConfig{Enabled: true, DataDir: dataDir}})
			s.Sawtooth = trigger
			meta := newRequestMeta(uint64(30+index), sessionID)
			meta.PressureDecision = pressureDecision{
				Available: true, SelectedPressure: 700, MessageCount: 10,
				SystemFingerprint: fingerprint, ToolsFingerprint: fingerprint,
				MessagesPrefixFingerprint: strings.Repeat("b", 64),
			}
			stamp := time.Now()
			s.writePressureDecisionDebugFacts(meta, stamp)
			resp := &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": {"application/json"}},
				Body:       io.NopCloser(strings.NewReader(tc.body)),
			}
			s.handleJSON(httptest.NewRecorder(), resp, meta, stamp, "model", 2)

			if persistCalls != 0 {
				t.Fatalf("untrusted usage persisted baseline %d times", persistCalls)
			}
			if after := trigger.PressureBaseline(sessionID); after != before {
				t.Fatalf("untrusted usage changed baseline\nbefore=%+v\nafter=%+v", before, after)
			}
			facts := debugFactsByStage(t, dataDir, sessionID)
			if len(facts) != 1 || facts[debugStagePressureDecision] == nil {
				t.Fatalf("untrusted usage facts=%v, want only pressure decision", facts)
			}
		})
	}
}

func TestHandleSSERejectsUntrustedUsageBaseline(t *testing.T) {
	const sessionID = "sse-untrusted-usage"
	trigger := NewSawtoothTrigger(time.Hour, 50_000, 1000)
	fingerprint := fingerprintTopLevelJSON(nil)
	trigger.UpdatePressureBaseline(sessionID, 777, 9, fingerprint, fingerprint, strings.Repeat("a", 64))
	before := trigger.PressureBaseline(sessionID)
	persistCalls := 0
	trigger.SetPersistFunc(func(_ string, _ string) { persistCalls++ })
	dataDir := tempDirRetryCleanup(t)
	s := NewServer(Config{Proxy: ProxyConfig{Deflation: 0.5}, Debug: DebugConfig{Enabled: true, DataDir: dataDir}})
	s.Sawtooth = trigger
	meta := newRequestMeta(40, sessionID)
	meta.PressureDecision = pressureDecision{
		Available: true, SelectedPressure: 700, MessageCount: 10,
		SystemFingerprint: fingerprint, ToolsFingerprint: fingerprint,
		MessagesPrefixFingerprint: strings.Repeat("b", 64),
	}
	stamp := time.Now()
	s.writePressureDecisionDebugFacts(meta, stamp)
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": {"text/event-stream"}},
		Body: io.NopCloser(strings.NewReader("event: message_start\n" +
			"data: {\"type\":\"message_start\",\"message\":{\"type\":\"message\",\"usage\":{\"input_tokens\":1.5,\"cache_read_input_tokens\":99999}}}\n\n" +
			"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n")),
	}
	s.handleSSE(httptest.NewRecorder(), resp, meta, stamp, "model", 2)

	if persistCalls != 0 {
		t.Fatalf("untrusted SSE usage persisted baseline %d times", persistCalls)
	}
	if after := trigger.PressureBaseline(sessionID); after != before {
		t.Fatalf("untrusted SSE usage changed baseline\nbefore=%+v\nafter=%+v", before, after)
	}
	facts := debugFactsByStage(t, dataDir, sessionID)
	if len(facts) != 1 || facts[debugStagePressureDecision] == nil {
		t.Fatalf("untrusted SSE usage facts=%v, want only pressure decision", facts)
	}
}

func TestHandleSSERejectsInvalidMessageLifecycleBaseline(t *testing.T) {
	validStart := "event: message_start\n" +
		`data: {"type":"message_start","message":{"type":"message","usage":{"input_tokens":321}}}` + "\n\n"
	validStop := "event: message_stop\n" + `data: {"type":"message_stop"}` + "\n\n"
	tests := []struct {
		name string
		body string
	}{
		{name: "stop before start", body: validStop + validStart},
		{name: "repeated start", body: validStart + validStart + validStop},
		{name: "repeated stop", body: validStart + validStop + validStop},
		{name: "start after stop", body: validStart + validStop + validStart},
		{name: "event data start mismatch", body: "event: message_delta\n" + `data: {"type":"message_start","message":{"type":"message","usage":{"input_tokens":321}}}` + "\n\n" + validStop},
		{name: "event data stop mismatch", body: validStart + "event: message_delta\n" + `data: {"type":"message_stop"}` + "\n\n"},
	}
	for index, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sessionID := fmt.Sprintf("sse-invalid-lifecycle-%d", index)
			trigger := NewSawtoothTrigger(time.Hour, 50_000, 1000)
			fingerprint := fingerprintTopLevelJSON(nil)
			trigger.UpdatePressureBaseline(sessionID, 777, 9, fingerprint, fingerprint, strings.Repeat("a", 64))
			before := trigger.PressureBaseline(sessionID)
			persistCalls := 0
			trigger.SetPersistFunc(func(_ string, _ string) { persistCalls++ })
			s := NewServer(Config{Proxy: ProxyConfig{Deflation: 0.5}})
			s.Sawtooth = trigger
			meta := newRequestMeta(uint64(50+index), sessionID)
			meta.PressureDecision = pressureDecision{
				Available: true, MessageCount: 10,
				SystemFingerprint: fingerprint, ToolsFingerprint: fingerprint,
				MessagesPrefixFingerprint: strings.Repeat("b", 64),
			}
			resp := &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": {"text/event-stream"}},
				Body:       io.NopCloser(strings.NewReader(tc.body)),
			}
			s.handleSSE(httptest.NewRecorder(), resp, meta, time.Now(), "model", 2)

			if persistCalls != 0 {
				t.Fatalf("invalid lifecycle persisted baseline %d times", persistCalls)
			}
			if after := trigger.PressureBaseline(sessionID); after != before {
				t.Fatalf("invalid lifecycle changed baseline\nbefore=%+v\nafter=%+v", before, after)
			}
		})
	}
}

func testHandleFailureDoesNotUpdate(t *testing.T, sse bool) {
	t.Helper()
	cases := []struct {
		name       string
		statusCode int
		body       string
	}{
		{name: "non-2xx", statusCode: http.StatusBadGateway, body: `{"usage":{"input_tokens":99999}}`},
		{name: "parse failure", statusCode: http.StatusOK, body: `{not-json`},
		{name: "no usage", statusCode: http.StatusOK, body: `{"type":"message"}`},
		{name: "empty usage", statusCode: http.StatusOK, body: `{"usage":{}}`},
	}
	if sse {
		cases = []struct {
			name       string
			statusCode int
			body       string
		}{
			{name: "parse failure", statusCode: http.StatusOK, body: "event: message_start\ndata: {not-json\n\n"},
			{name: "no message start", statusCode: http.StatusOK, body: "event: message_delta\ndata: {\"type\":\"message_delta\",\"usage\":{\"input_tokens\":99999}}\n\n"},
			{name: "spoofed message start text", statusCode: http.StatusOK, body: "event: message_delta\ndata: {\"type\":\"message_delta\",\"note\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":99999}}}\n\n"},
			{name: "message start without usage", statusCode: http.StatusOK, body: "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{}}\n\n"},
			{name: "empty usage", statusCode: http.StatusOK, body: "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"usage\":{}}}\n\n"},
		}
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			const sessionID = "failure-baseline"
			trigger := NewSawtoothTrigger(time.Hour, 50000, 1000)
			fingerprint := fingerprintTopLevelJSON(nil)
			trigger.UpdatePressureBaseline(sessionID, 777, 9, fingerprint, fingerprint, strings.Repeat("e", 64))
			before := trigger.PressureBaseline(sessionID)
			persistCalls := 0
			trigger.SetPersistFunc(func(_ string, _ string) { persistCalls++ })
			s := NewServer(Config{Proxy: ProxyConfig{Deflation: 0.5}})
			s.Sawtooth = trigger
			meta := newRequestMeta(20, sessionID)
			meta.PressureDecision = pressureDecision{Available: true, MessageCount: 10, SystemFingerprint: fingerprint, ToolsFingerprint: fingerprint}
			resp := &http.Response{
				StatusCode: tc.statusCode,
				Header:     http.Header{"Content-Type": {"application/json"}},
				Body:       io.NopCloser(strings.NewReader(tc.body)),
			}
			recorder := httptest.NewRecorder()
			if sse {
				resp.Header.Set("Content-Type", "text/event-stream")
				s.handleSSE(recorder, resp, meta, time.Now(), "model", 2)
			} else {
				s.handleJSON(recorder, resp, meta, time.Now(), "model", 2)
			}
			if persistCalls != 0 {
				t.Fatalf("failure path persisted baseline %d times", persistCalls)
			}
			if after := trigger.PressureBaseline(sessionID); after != before {
				t.Fatalf("failure path changed baseline\nbefore=%+v\nafter=%+v", before, after)
			}
		})
	}
}

type failingDebugFile struct {
	file     *os.File
	writeErr error
	closeErr error
}

func (f *failingDebugFile) Write(data []byte) (int, error) {
	if f.writeErr != nil {
		n, _ := f.file.Write(data[:len(data)/2])
		return n, f.writeErr
	}
	return f.file.Write(data)
}

func (f *failingDebugFile) Close() error {
	err := f.file.Close()
	if f.closeErr != nil {
		return f.closeErr
	}
	return err
}

func TestWriteDebugFileRedactsCredentialHeaders(t *testing.T) {
	dataDir := tempDirRetryCleanup(t)
	s := NewServer(Config{Debug: DebugConfig{Enabled: true, FullBody: true, DataDir: dataDir}})
	meta := s.nextRequestMeta("session")
	headers := http.Header{
		"AUTHORIZATION":       {"Bearer auth-secret"},
		"proxy-AUTHORIZATION": {"Basic proxy-secret"},
		"x-API-key":           {"api-secret"},
		"ANTHROPIC-api-KEY":   {"anthropic-secret"},
		"cOoKiE":              {"session=cookie-secret"},
		"SET-cookie":          {"set-cookie-secret"},
		"X-Diagnostic":        {"safe-value"},
	}
	timestamp := time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC)
	body := []byte(`{"authorization":"BODY-SECRET-MUST-STAY","ok":true}`)
	s.writeFullBodyDebug(meta, timestamp, debugBodyStageForwarded, body, headers, "model", 1)

	path, err := s.debugLayout.RequestPath(meta, debugArtifactForwardedBody)
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("读取 debug 文件: %v", err)
	}
	for _, secret := range []string{"auth-secret", "proxy-secret", "api-secret", "anthropic-secret", "cookie-secret", "set-cookie-secret"} {
		if bytes.Contains(data, []byte(secret)) {
			t.Fatalf("debug 文件泄漏凭证 %q: %s", secret, data)
		}
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		t.Fatalf("解析 debug 字段: %v", err)
	}
	if _, ok := fields["session_id"]; ok || bytes.Contains(data, []byte(meta.RequestSessionID)) {
		t.Fatalf("full-body stage 重复保存 session 身份: %s", data)
	}
	var entry debugEntry
	if err := json.Unmarshal(data, &entry); err != nil {
		t.Fatalf("解析 debug 文件: %v", err)
	}
	if entry.RequestID != 1 || entry.Stage != debugBodyStageForwarded {
		t.Fatalf("debug 条目缺少关联字段: %+v", entry)
	}
	if !bytes.Contains(entry.Headers, []byte("safe-value")) {
		t.Fatalf("诊断 header 未保留: %s", entry.Headers)
	}
	if !bytes.Contains(entry.Body, []byte("BODY-SECRET-MUST-STAY")) {
		t.Fatalf("正文中的用户数据被改写: %s", entry.Body)
	}
}

func TestWriteDebugFileSessionPathCannotEscapeDebugRoot(t *testing.T) {
	for _, sessionID := range []string{"../escape", `..\\escape`, `C:\\escape`, `\\\\server\\share\\escape`} {
		t.Run(sessionID, func(t *testing.T) {
			dataDir := tempDirRetryCleanup(t)
			s := NewServer(Config{Debug: DebugConfig{Enabled: true, FullBody: true, DataDir: dataDir}})
			meta := s.nextRequestMeta(sessionID)
			s.writeFullBodyDebug(meta, time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC), debugBodyStageForwarded, []byte(`{}`), nil, "model", 0)

			debugDir, ok := safeDebugSessionDir(dataDir, sessionID)
			if !ok {
				t.Fatal("哈希后的 session 目录应通过根目录校验")
			}
			root, _ := filepath.Abs(filepath.Join(dataDir, "debug"))
			rel, err := filepath.Rel(root, debugDir)
			if err != nil || strings.HasPrefix(rel, "..") || filepath.IsAbs(rel) {
				t.Fatalf("debug 目录逃逸: root=%s dir=%s rel=%s err=%v", root, debugDir, rel, err)
			}
			path, err := s.debugLayout.RequestPath(meta, debugArtifactForwardedBody)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := os.Stat(path); err != nil {
				t.Fatalf("debug 文件未写入哈希目录: %v", err)
			}
		})
	}
}

// TestResponseHandlersRejectUnboundForwardedCoordinates 封闭 W2 的核心 invariant：
// 调用方**完全没有**调用 markForwardedPressureCoordinates 时，两个坐标字段都是零值。
// 收紧前的门禁 `Changed && !Bound` 会因为 Changed==false 而放行，把未绑定的坐标
// 当作 exact baseline 写回；收紧后必须一律拒绝。
func TestResponseHandlersRejectUnboundForwardedCoordinates(t *testing.T) {
	tests := []struct {
		name        string
		contentType string
		body        string
		invoke      func(s *Server, resp *http.Response, meta *requestMeta)
	}{
		{
			name:        "json",
			contentType: "application/json",
			body:        `{"type":"message","usage":{"input_tokens":196,"cache_read_input_tokens":93056,"output_tokens":20}}`,
			invoke: func(s *Server, resp *http.Response, meta *requestMeta) {
				s.handleJSON(httptest.NewRecorder(), resp, meta, time.Now(), "model", 3)
			},
		},
		{
			name:        "sse",
			contentType: "text/event-stream",
			body: "event: message_start\n" +
				"data: {\"type\":\"message_start\",\"message\":{\"type\":\"message\",\"usage\":{\"input_tokens\":196,\"cache_read_input_tokens\":93056,\"output_tokens\":20}}}\n\n" +
				"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
			invoke: func(s *Server, resp *http.Response, meta *requestMeta) {
				s.handleSSE(httptest.NewRecorder(), resp, meta, time.Now(), "model", 4)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			trigger := NewSawtoothTrigger(time.Hour, 50000, 1000)
			var persisted string
			trigger.SetPersistFunc(func(_ string, value string) { persisted = value })
			s := NewServer(Config{Proxy: ProxyConfig{Deflation: 0.5}})
			s.Sawtooth = trigger
			forwarded := pipelineMessages(12, 6)
			meta := newRequestMeta(3, "unbound-"+tt.name)
			// 坐标看起来完全自洽，唯独没有经过绑定。
			meta.PressureDecision = pressureDecision{
				Available:                 true,
				MessageCount:              len(forwarded),
				SystemFingerprint:         fingerprintTopLevelJSON(nil),
				ToolsFingerprint:          fingerprintTopLevelJSON(nil),
				MessagesPrefixFingerprint: fingerprintMessagesPrefix(forwarded, len(forwarded)),
			}
			if meta.PressureDecision.ForwardedCoordinatesBound {
				t.Fatalf("前置条件错误，坐标字段应为零值: %+v", meta.PressureDecision)
			}

			tt.invoke(s, &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": {tt.contentType}},
				Body:       io.NopCloser(strings.NewReader(tt.body)),
			}, meta)

			if persisted != "" {
				t.Fatalf("未绑定 forwarded 坐标却写入了 baseline: %q", persisted)
			}
			if meta.BaselineUpdateKind != pressureBaselineUpdateNone {
				t.Fatalf("baseline update kind=%q, want none", meta.BaselineUpdateKind)
			}
			if baseline := trigger.PressureBaseline("unbound-" + tt.name); baseline.Available {
				t.Fatalf("未绑定坐标污染了内存 baseline: %+v", baseline)
			}
		})
	}
}

// TestUnchangedForwardedBodyBindsExactPressureBaseline 覆盖"body 未被改写但已绑定"：
// 写回仍必须成立——门禁只看 Bound。
func TestUnchangedForwardedBodyBindsExactPressureBaseline(t *testing.T) {
	trigger := NewSawtoothTrigger(time.Hour, 150_000, 75_000)
	var persisted string
	trigger.SetPersistFunc(func(_ string, value string) { persisted = value })
	s := NewServer(Config{})
	s.Sawtooth = trigger
	system := json.RawMessage(`"stable system"`)
	tools := json.RawMessage(`[{"name":"Read"}]`)
	forwarded := pipelineMessages(9, 4)
	meta := newRequestMeta(4, "unchanged-forwarded-baseline")
	meta.BaselineGeneration = trigger.BeginPressureRequest(meta.RequestSessionID)
	// decision 的坐标与最终 wire body 完全一致——本轮没有压缩、桩化或注入，
	// 入口 raw 坐标 == wire 坐标，baseline 契约两者同值。
	meta.PressureDecision = pressureDecision{
		Available:                 true,
		MessageCount:              len(forwarded),
		SelectedPressure:          12_000,
		SystemFingerprint:         fingerprintTopLevelJSON(system),
		ToolsFingerprint:          fingerprintTopLevelJSON(tools),
		MessagesPrefixFingerprint: fingerprintMessagesPrefix(forwarded, len(forwarded)),
	}
	meta.PressureEntryCoordinates = pressureEntryCoordinates{
		MessageCount:              len(forwarded),
		MessagesPrefixFingerprint: fingerprintMessagesPrefix(forwarded, len(forwarded)),
	}

	markForwardedPressureCoordinates(meta, forwardedPressureBody(t, system, tools, forwarded), nil)
	if !meta.PressureDecision.ForwardedCoordinatesBound {
		t.Fatalf("未改写的 body 应为 bound=true: %+v", meta.PressureDecision)
	}
	if updated := s.applyPressureBaselineUsage(meta, 31_500); !updated {
		t.Fatal("未改写但已绑定的 actual 未被接受为 exact baseline")
	}
	if meta.BaselineUpdateKind != pressureBaselineUpdateExact {
		t.Fatalf("baseline update kind=%q, want exact", meta.BaselineUpdateKind)
	}
	var state persistedState
	if err := json.Unmarshal([]byte(persisted), &state); err != nil {
		t.Fatalf("解析 exact baseline: %v raw=%q", err, persisted)
	}
	if state.Tokens != 31_500 || state.Conservative || state.MsgCount != len(forwarded) {
		t.Fatalf("持久化 exact baseline=%+v", state)
	}
}

func TestForwardedRewriteBindsExactPressureBaseline(t *testing.T) {
	trigger := NewSawtoothTrigger(time.Hour, 150_000, 75_000)
	var persisted string
	trigger.SetPersistFunc(func(_ string, value string) { persisted = value })
	s := NewServer(Config{})
	s.Sawtooth = trigger
	original := pipelineMessages(32, 8)
	forwarded := pipelineMessages(4, 2)
	system := json.RawMessage(`"stable system"`)
	tools := json.RawMessage(`[{"name":"Read"}]`)
	meta := newRequestMeta(1, "exact-forwarded-baseline")
	meta.BaselineGeneration = trigger.BeginPressureRequest(meta.RequestSessionID)
	meta.PressureDecision = pressureDecision{
		Available:                 true,
		MessageCount:              len(original),
		SelectedPressure:          109_612,
		SystemFingerprint:         fingerprintTopLevelJSON(system),
		ToolsFingerprint:          fingerprintTopLevelJSON(tools),
		MessagesPrefixFingerprint: fingerprintMessagesPrefix(original, len(original)),
	}
	// baseline 契约坐标 = 入口 raw 快照；折叠改写只影响 wire，绝不改写它。
	meta.PressureEntryCoordinates = pressureEntryCoordinates{
		MessageCount:              len(original),
		MessagesPrefixFingerprint: fingerprintMessagesPrefix(original, len(original)),
	}

	body, err := json.Marshal(map[string]any{
		"system":   system,
		"tools":    tools,
		"messages": forwarded,
	})
	if err != nil {
		t.Fatal(err)
	}
	markForwardedPressureCoordinates(meta, body, nil)
	if !meta.PressureDecision.ForwardedCoordinatesBound {
		t.Fatalf("forwarded 坐标未被绑定: %+v", meta.PressureDecision)
	}
	if meta.PressureDecision.MessageCount != len(original) || meta.PressureDecision.MessagesPrefixFingerprint != fingerprintMessagesPrefix(original, len(original)) {
		t.Fatalf("decision 必须保持入口 raw 坐标，不得随 wire 改写: %+v", meta.PressureDecision)
	}

	if updated := s.applyPressureBaselineUsage(meta, 27_743); !updated {
		t.Fatal("改写后的 actual 未被接受为 exact baseline")
	}
	if meta.BaselineUpdateKind != pressureBaselineUpdateExact {
		t.Fatalf("baseline update kind=%q，want exact", meta.BaselineUpdateKind)
	}
	var state persistedState
	if err := json.Unmarshal([]byte(persisted), &state); err != nil {
		t.Fatalf("解析 exact baseline: %v raw=%q", err, persisted)
	}
	if state.Tokens != 27_743 || state.Conservative || state.MsgCount != len(original) ||
		state.SystemFingerprint != fingerprintTopLevelJSON(system) || state.ToolsFingerprint != fingerprintTopLevelJSON(tools) ||
		state.MessagesPrefixFingerprint != fingerprintMessagesPrefix(original, len(original)) {
		t.Fatalf("持久化 exact baseline=%+v", state)
	}
	baseline := trigger.PressureBaseline(meta.RequestSessionID)
	if !baseline.Available || baseline.Conservative || baseline.ActualTokens != 27_743 || baseline.MessageCount != len(original) || baseline.MessagesPrefixFingerprint != fingerprintMessagesPrefix(original, len(original)) {
		t.Fatalf("内存 exact baseline=%+v", baseline)
	}
}

func TestMalformedForwardedBodyDoesNotWritePressureBaseline(t *testing.T) {
	trigger := NewSawtoothTrigger(time.Hour, 150_000, 75_000)
	var persisted string
	trigger.SetPersistFunc(func(_ string, value string) { persisted = value })
	s := NewServer(Config{})
	s.Sawtooth = trigger
	meta := newRequestMeta(2, "malformed-forwarded-body")
	meta.BaselineGeneration = trigger.BeginPressureRequest(meta.RequestSessionID)
	meta.PressureDecision = pressureDecision{
		Available:                 true,
		MessageCount:              1,
		SystemFingerprint:         fingerprintTopLevelJSON(nil),
		ToolsFingerprint:          fingerprintTopLevelJSON(nil),
		MessagesPrefixFingerprint: strings.Repeat("a", 64),
	}

	markForwardedPressureCoordinates(meta, []byte(`{"messages":`), nil)
	if meta.PressureDecision.ForwardedCoordinatesBound {
		t.Fatalf("malformed body incorrectly bound: %+v", meta.PressureDecision)
	}
	if updated := s.applyPressureBaselineUsage(meta, 27_743); updated {
		t.Fatal("malformed forwarded body unexpectedly updated baseline")
	}
	if meta.BaselineUpdateKind != pressureBaselineUpdateNone {
		t.Fatalf("malformed body baseline kind=%q, want none", meta.BaselineUpdateKind)
	}
	if persisted != "" {
		t.Fatalf("malformed forwarded body wrote persisted state: %q", persisted)
	}
}

func TestSafeDebugSessionDirUsesShortStableHash(t *testing.T) {
	dataDir := tempDirRetryCleanup(t)
	first, ok := safeDebugSessionDir(dataDir, "session-one")
	if !ok {
		t.Fatal("合法 session debug 目录校验失败")
	}
	again, ok := safeDebugSessionDir(dataDir, "session-one")
	if !ok || again != first {
		t.Fatalf("相同 session 的 debug 目录不稳定: first=%q again=%q", first, again)
	}
	other, ok := safeDebugSessionDir(dataDir, "session-two")
	if !ok || other == first {
		t.Fatalf("不同 session 未被区分: first=%q other=%q", first, other)
	}
	name := filepath.Base(first)
	if len(name) != 16 {
		t.Fatalf("debug session hash 长度=%d，want 16: %q", len(name), name)
	}
	for _, char := range name {
		if (char < '0' || char > '9') && (char < 'a' || char > 'f') {
			t.Fatalf("debug session hash 含非小写 hex 字符: %q", name)
		}
	}
}

func TestWriteDebugFileUsesRequestIDToPreventCollisions(t *testing.T) {
	dataDir := tempDirRetryCleanup(t)
	s := NewServer(Config{Debug: DebugConfig{Enabled: true, FullBody: true, DataDir: dataDir}})
	timestamp := time.Date(2026, 7, 11, 12, 0, 0, 123, time.UTC)
	var wg sync.WaitGroup
	for _, requestID := range []uint64{41, 42} {
		meta := newRequestMetaWithRun(requestID, "session", s.debugRunID)
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.writeFullBodyDebug(meta, timestamp, debugBodyStageForwarded, []byte(`{"request":true}`), nil, "model", 1)
		}()
	}
	wg.Wait()
	for _, requestID := range []uint64{41, 42} {
		meta := newRequestMetaWithRun(requestID, "session", s.debugRunID)
		path, err := s.debugLayout.RequestPath(meta, debugArtifactForwardedBody)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("request %d 的稳定 Debug 文件不存在: %v", requestID, err)
		}
	}
}

func TestDebugFullBodyOptInWritesExactlyThreeStableFiles(t *testing.T) {
	stages := []struct {
		bodyStage     debugBodyStage
		artifactStage debugArtifactStage
		body          []byte
	}{
		{bodyStage: debugBodyStageRawInbound, artifactStage: debugArtifactRawBody, body: []byte(`{"raw":true}`)},
		{bodyStage: debugBodyStageForwarded, artifactStage: debugArtifactForwardedBody, body: []byte(`{"forwarded":true}`)},
		{bodyStage: debugBodyStageResponse, artifactStage: debugArtifactResponseBody, body: []byte(`{"response":true}`)},
	}

	for _, tc := range []struct {
		name     string
		enabled  bool
		fullBody bool
		want     bool
	}{
		{name: "debug disabled", enabled: false, fullBody: true},
		{name: "full body disabled", enabled: true, fullBody: false},
		{name: "explicit opt in", enabled: true, fullBody: true, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dataDir := tempDirRetryCleanup(t)
			s := NewServer(Config{Debug: DebugConfig{Enabled: tc.enabled, FullBody: tc.fullBody, DataDir: dataDir}})
			meta := s.nextRequestMeta("full-body-opt-in")
			for _, stage := range stages {
				s.writeFullBodyDebug(meta, time.Date(2026, 7, 24, 1, 2, 3, 4, time.UTC), stage.bodyStage, stage.body, nil, "model", 1)
			}
			for _, stage := range stages {
				path, err := s.debugLayout.RequestPath(meta, stage.artifactStage)
				if err != nil {
					t.Fatal(err)
				}
				_, statErr := os.Stat(path)
				if tc.want && statErr != nil {
					t.Fatalf("显式 full-body 缺少 %s: %v", stage.artifactStage, statErr)
				}
				if !tc.want && !os.IsNotExist(statErr) {
					t.Fatalf("未 opt-in 仍生成 %s: %v", stage.artifactStage, statErr)
				}
			}
		})
	}
}

func TestWriteDebugEntryFileRemovesPartialFileOnFailure(t *testing.T) {
	for _, tt := range []struct {
		name     string
		writeErr error
		closeErr error
	}{
		{name: "write error", writeErr: errors.New("injected write error")},
		{name: "close error", closeErr: errors.New("injected close error")},
	} {
		t.Run(tt.name, func(t *testing.T) {
			filePath := filepath.Join(tempDirRetryCleanup(t), "entry.json")
			err := writeDebugEntryFile(filePath, []byte(`{"complete":true}`), func(name string, flag int, perm os.FileMode) (debugWriteCloser, error) {
				file, err := os.OpenFile(name, flag, perm)
				if err != nil {
					return nil, err
				}
				return &failingDebugFile{file: file, writeErr: tt.writeErr, closeErr: tt.closeErr}, nil
			})
			if err == nil {
				t.Fatal("注入失败后应返回错误")
			}
			if _, statErr := os.Stat(filePath); !os.IsNotExist(statErr) {
				t.Fatalf("失败后残留截断 debug 文件: %v", statErr)
			}
		})
	}
}

func TestForwardRawTargetTrailingSlash(t *testing.T) {
	receivedURI := make(chan string, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedURI <- r.URL.RequestURI()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"type":"message","usage":{"input_tokens":1,"output_tokens":1}}`))
	}))
	defer upstream.Close()

	s := NewServer(Config{
		Proxy: ProxyConfig{
			Target:    upstream.URL + "/",
			Deflation: 1,
		},
	})

	req := httptest.NewRequest(http.MethodPost, "/v1/messages?beta=true", strings.NewReader(`{"model":"test","messages":[]}`))
	recorder := httptest.NewRecorder()
	s.forwardRaw(recorder, req, newRequestMeta(1, "test-session"))

	if got := <-receivedURI; got != "/v1/messages?beta=true" {
		t.Fatalf("上游请求 URI = %q，期望 %q", got, "/v1/messages?beta=true")
	}
	if recorder.Code != http.StatusOK {
		t.Fatalf("代理响应状态码 = %d，期望 %d", recorder.Code, http.StatusOK)
	}
}

func TestForwardRawRequestLogFields(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"type":"message","usage":{"input_tokens":1,"output_tokens":1}}`))
	}))
	defer upstream.Close()

	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(NewLogHandler(&logs, slog.LevelDebug)))
	t.Cleanup(func() { slog.SetDefault(previous) })

	s := NewServer(Config{Proxy: ProxyConfig{Target: upstream.URL, Deflation: 1}})
	meta := newRequestMeta(17, "request-session")
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"model":"test-model","messages":[{"role":"user","content":"one"},{"role":"assistant","content":"two"}]}`))
	recorder := httptest.NewRecorder()
	s.forwardRaw(recorder, req, meta)

	output := logs.String()
	// 高频行已事件键化并降为 Debug：终端模板渲染单行中文，不再追加 kv
	// （request_id 等关联字段只在文件侧保留）。
	for _, want := range []string{
		"▸ 请求进入",
		"→ 上游发送",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("日志缺少 %q:\n%s", want, output)
		}
	}
	if strings.Contains(output, "original_message_count=2") || strings.Contains(output, "forwarded_message_count=2") {
		t.Fatalf("模板化事件行不应渲染英文 kv:\n%s", output)
	}
	// LOG-03：session 身份不进终端渲染，关联改由 request_id 承担。
	if strings.Contains(output, "request-session") || strings.Contains(output, "request_session_id") {
		t.Fatalf("终端日志泄漏 session 身份:\n%s", output)
	}
	if strings.Contains(output, "请求已接收") || strings.Contains(output, "Authorization") {
		t.Fatalf("日志包含旧事件名或敏感 header:\n%s", output)
	}
}

func TestForwardRawClaudeCodeCancelDoesNotWriteGatewayError(t *testing.T) {
	started := make(chan struct{})
	canceled := make(chan struct{})
	s := NewServer(Config{Proxy: ProxyConfig{Target: "https://upstream.invalid"}})
	s.HTTPClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		close(started)
		<-req.Context().Done()
		close(canceled)
		return nil, req.Context().Err()
	})}

	logs := captureForwardLogs(t)
	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"stream":true,"messages":[]}`)).WithContext(ctx)
	w := newCountingResponseWriter()
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.forwardRaw(w, req, newRequestMeta(100, "cancel-session"))
	}()
	<-started
	cancel()

	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("Claude Code 取消未传播到上游")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("取消后 forwardRaw 未结束")
	}
	status, headerCalls, body := w.snapshot()
	if status != 0 || headerCalls != 0 || len(body) != 0 {
		t.Fatalf("下游取消后写入了伪造网关响应: status=%d calls=%d body=%q", status, headerCalls, body)
	}
	assertLogFields(t, logs.String(), "timeout_source=downstream_context", "stream=true", "elapsed_ms=", "phase=")
}

func TestForwardRawHeaderTimeoutClassification(t *testing.T) {
	logs := captureForwardLogs(t)
	for _, tc := range []struct {
		name   string
		stream bool
		source string
	}{
		{name: "stream", stream: true, source: "stream_header_timeout"},
		{name: "non-stream", stream: false, source: "non_stream_header_timeout"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			received := make(chan struct{}, 1)
			release := make(chan struct{})
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				received <- struct{}{}
				<-release
				w.WriteHeader(http.StatusNoContent)
			}))

			cfg := Config{
				Proxy: ProxyConfig{Target: upstream.URL, Deflation: 1},
				Transport: TransportConfig{
					StreamHeaderTimeout:    30 * time.Millisecond,
					NonStreamHeaderTimeout: 30 * time.Millisecond,
				},
			}
			s := NewServer(cfg)
			req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(fmt.Sprintf(`{"stream":%t,"messages":[]}`, tc.stream)))
			w := newCountingResponseWriter()
			done := make(chan struct{})
			go func() {
				defer close(done)
				s.forwardRaw(w, req, newRequestMeta(101, tc.name))
			}()
			<-received
			select {
			case <-done:
			case <-time.After(time.Second):
				close(release)
				upstream.Close()
				t.Fatal("响应头超时未结束请求")
			}
			close(release)
			upstream.Close()

			assertSingleGatewayResponse(t, w, http.StatusGatewayTimeout, "Gateway Timeout")
			assertLogFields(t, logs.String(), "timeout_source="+tc.source, "phase=awaiting_headers")
		})
	}
}

func TestForwardRawHardTimeoutClassification(t *testing.T) {
	logs := captureForwardLogs(t)
	s := NewServer(Config{
		Proxy:     ProxyConfig{Target: "https://upstream.invalid"},
		Transport: TransportConfig{HardTimeout: 35 * time.Millisecond},
	})
	s.HTTPClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		select {
		case <-req.Context().Done():
			return nil, req.Context().Err()
		case <-time.After(200 * time.Millisecond):
			return nil, timeoutError{"fallback timeout"}
		}
	})}

	req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"messages":[]}`))
	w := newCountingResponseWriter()
	s.forwardRaw(w, req, newRequestMeta(102, "hard-timeout"))

	assertSingleGatewayResponse(t, w, http.StatusGatewayTimeout, "Gateway Timeout")
	assertLogFields(t, logs.String(), "timeout_source=proxy_hard_limit", "stream=false")
}

func TestForwardRawResponseIdleTimeoutClassification(t *testing.T) {
	logs := captureForwardLogs(t)
	var body *stagedReadCloser
	s := NewServer(Config{
		Proxy:     ProxyConfig{Target: "https://upstream.invalid", Deflation: 1},
		Transport: TransportConfig{ResponseIdleTimeout: 35 * time.Millisecond},
	})
	s.HTTPClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		body = newStagedReadCloser([]byte(`{"type":"message"`), req.Context(), 200*time.Millisecond)
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": {"application/json"}},
			Body:       body,
		}, nil
	})}

	req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"messages":[]}`))
	w := newCountingResponseWriter()
	s.forwardRaw(w, req, newRequestMeta(103, "idle-timeout"))
	if body != nil {
		_ = body.Close()
	}

	assertSingleGatewayResponse(t, w, http.StatusGatewayTimeout, "Gateway Timeout")
	assertLogFields(t, logs.String(), "timeout_source=response_idle_timeout", "phase=reading_body")
}

func TestForwardRawUnexpectedEOFRemainsBadGateway(t *testing.T) {
	logs := captureForwardLogs(t)
	s := NewServer(Config{Proxy: ProxyConfig{Target: "https://upstream.invalid"}})
	s.HTTPClient = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, io.ErrUnexpectedEOF
	})}

	req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"messages":[]}`))
	w := newCountingResponseWriter()
	s.forwardRaw(w, req, newRequestMeta(104, "unexpected-eof"))

	assertSingleGatewayResponse(t, w, http.StatusBadGateway, "Bad Gateway")
	assertLogFields(t, logs.String(), "timeout_source=upstream_transport")
	if strings.Contains(logs.String(), "proxy_hard_limit") || strings.Contains(logs.String(), "header_timeout") {
		t.Fatalf("unexpected EOF 被误判为代理超时:\n%s", logs.String())
	}
}

func TestForwardRawDoesNotRetryAmbiguousPost(t *testing.T) {
	var calls atomic.Int32
	s := NewServer(Config{Proxy: ProxyConfig{Target: "https://upstream.invalid"}})
	s.HTTPClient = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		return nil, io.ErrUnexpectedEOF
	})}

	req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"messages":[]}`))
	w := newCountingResponseWriter()
	s.forwardRaw(w, req, newRequestMeta(105, "single-post"))

	if got := calls.Load(); got != 1 {
		t.Fatalf("含糊 POST 调用次数=%d, want 1", got)
	}
	assertSingleGatewayResponse(t, w, http.StatusBadGateway, "Bad Gateway")
	if got := bytes.Count(w.bodyBytes(), []byte(`"error"`)); got != 1 {
		t.Fatalf("错误 JSON 数量=%d, body=%s", got, w.bodyBytes())
	}
}

func TestForwardRawConnectOrTLSFailureRemainsBadGateway(t *testing.T) {
	for _, tc := range []struct {
		name  string
		trace func(*httptrace.ClientTrace)
	}{
		{name: "connect", trace: func(trace *httptrace.ClientTrace) { trace.GetConn("upstream.invalid") }},
		{name: "tls", trace: func(trace *httptrace.ClientTrace) { trace.TLSHandshakeStart() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logs := captureForwardLogs(t)
			s := NewServer(Config{Proxy: ProxyConfig{Target: "https://upstream.invalid"}})
			s.HTTPClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				trace := httptrace.ContextClientTrace(req.Context())
				if trace == nil {
					t.Fatal("请求 context 缺少 httptrace")
				}
				tc.trace(trace)
				return nil, timeoutError{tc.name + " timeout"}
			})}

			req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"messages":[]}`))
			w := newCountingResponseWriter()
			s.forwardRaw(w, req, newRequestMeta(106, tc.name))

			assertSingleGatewayResponse(t, w, http.StatusBadGateway, "Bad Gateway")
			assertLogFields(t, logs.String(), "phase="+map[string]string{"connect": "connect", "tls": "tls_handshake"}[tc.name], "timeout_source=upstream_transport")
		})
	}
}

func TestForwardRawFailureLogSensitiveBoundary(t *testing.T) {
	logs := captureForwardLogs(t)
	const (
		userinfoSecret = "userinfo-secret"
		authSecret     = "authorization-secret"
		apiKeySecret   = "api-key-secret"
		bodySecret     = "request-body-secret"
	)
	s := NewServer(Config{Proxy: ProxyConfig{Target: "https://ordinary-target.invalid"}})
	s.HTTPClient = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, fmt.Errorf("transport wrapper: %w", &url.Error{
			Op:  "Post",
			URL: "https://" + userinfoSecret + ":password@diagnostic-host.invalid/diagnostic-path?diagnostic-query=1",
			Err: io.ErrUnexpectedEOF,
		})
	})}

	req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"messages":[],"sentinel":"`+bodySecret+`"}`))
	req.Header.Set("Authorization", "Bearer "+authSecret)
	req.Header.Set("X-Api-Key", apiKeySecret)
	w := newCountingResponseWriter()
	s.forwardRaw(w, req, newRequestMeta(107, "safe-log"))

	output := logs.String()
	assertLogFields(t, output, "diagnostic-host.invalid", "/diagnostic-path", "diagnostic-query=1")
	for _, secret := range []string{userinfoSecret, "password", authSecret, apiKeySecret, bodySecret} {
		if strings.Contains(output, secret) {
			t.Fatalf("失败日志泄漏 %q:\n%s", secret, output)
		}
	}
}

func TestForwardRawStripsConnectionAndSetsContentLength(t *testing.T) {
	const requestBody = `{"model":"test","messages":[{"role":"user","content":"完整正文"}]}`
	var seenConnection string
	var seenHeaderLength string
	var seenContentLength int64
	var seenAuthorization string
	var seenBody []byte

	s := NewServer(Config{Proxy: ProxyConfig{Target: "https://upstream.invalid", Deflation: 1}})
	s.HTTPClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		seenConnection = req.Header.Get("Connection")
		seenHeaderLength = req.Header.Get("Content-Length")
		seenContentLength = req.ContentLength
		seenAuthorization = req.Header.Get("Authorization")
		seenBody, _ = io.ReadAll(req.Body)
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": {"application/json"}},
			Body:       io.NopCloser(strings.NewReader(`{"type":"message","usage":{"input_tokens":1,"output_tokens":1}}`)),
		}, nil
	})}

	req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(requestBody))
	req.Header.Set("Connection", "keep-alive")
	req.Header.Set("Content-Length", "999999")
	req.Header.Set("Authorization", "Bearer preserved")
	w := newCountingResponseWriter()
	s.forwardRaw(w, req, newRequestMeta(108, "wire"))

	if seenConnection != "" || seenHeaderLength != "" {
		t.Fatalf("逐跳/旧长度 header 未清理: Connection=%q Content-Length=%q", seenConnection, seenHeaderLength)
	}
	if seenContentLength != int64(len(requestBody)) || string(seenBody) != requestBody {
		t.Fatalf("上游 body 长度不一致: ContentLength=%d len=%d body=%q", seenContentLength, len(seenBody), seenBody)
	}
	if seenAuthorization != "Bearer preserved" {
		t.Fatalf("必要认证 header 未透传: %q", seenAuthorization)
	}
}

func TestForwardRawNon2xxBodyTimeoutBeforeCommit(t *testing.T) {
	for _, tc := range []struct {
		name       string
		status     int
		transport  TransportConfig
		wantSource string
	}{
		{name: "idle", status: http.StatusTooManyRequests, transport: TransportConfig{ResponseIdleTimeout: 35 * time.Millisecond}, wantSource: "response_idle_timeout"},
		{name: "hard", status: http.StatusServiceUnavailable, transport: TransportConfig{HardTimeout: 35 * time.Millisecond}, wantSource: "proxy_hard_limit"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logs := captureForwardLogs(t)
			var body *stagedReadCloser
			s := NewServer(Config{Proxy: ProxyConfig{Target: "https://upstream.invalid"}, Transport: tc.transport})
			s.HTTPClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				body = newStagedReadCloser([]byte("partial-upstream-body"), req.Context(), 200*time.Millisecond)
				return &http.Response{StatusCode: tc.status, Header: http.Header{"Content-Type": {"application/json"}}, Body: body}, nil
			})}

			req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"messages":[]}`))
			w := newCountingResponseWriter()
			s.forwardRaw(w, req, newRequestMeta(109, tc.name))
			if body != nil {
				_ = body.Close()
			}

			assertSingleGatewayResponse(t, w, http.StatusGatewayTimeout, "Gateway Timeout")
			if bytes.Contains(w.bodyBytes(), []byte("partial-upstream-body")) {
				t.Fatalf("504 混入上游部分正文: %s", w.bodyBytes())
			}
			assertLogFields(t, logs.String(), "timeout_source="+tc.wantSource, "phase=reading_body")
		})
	}
}

func TestForwardRawLongSSEProgressOutlivesLegacyLimit(t *testing.T) {
	const legacyBoundary = 50 * time.Millisecond
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flusher := w.(http.Flusher)
		for index := 0; index < 6; index++ {
			_, _ = fmt.Fprintf(w, "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":%d}\n\n", index)
			flusher.Flush()
			time.Sleep(15 * time.Millisecond)
		}
	}))
	defer upstream.Close()

	s := NewServer(Config{
		Proxy: ProxyConfig{Target: upstream.URL, Deflation: 1},
		Transport: TransportConfig{
			StreamHeaderTimeout: 200 * time.Millisecond,
			ResponseIdleTimeout: 100 * time.Millisecond,
			HardTimeout:         time.Second,
		},
	})
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"stream":true,"messages":[]}`))
	w := newCountingResponseWriter()
	started := time.Now()
	s.forwardRaw(w, req, newRequestMeta(110, "long-sse"))
	elapsed := time.Since(started)

	status, _, body := w.snapshot()
	if status != http.StatusOK || bytes.Contains(body, []byte("Gateway")) {
		t.Fatalf("长 SSE 被错误截断: status=%d body=%s", status, body)
	}
	if elapsed <= legacyBoundary {
		t.Fatalf("测试总时长=%s，未超过概念旧界限=%s", elapsed, legacyBoundary)
	}
	if got := bytes.Count(body, []byte("content_block_delta")); got != 12 {
		t.Fatalf("SSE 事件未完整到达: marker count=%d body=%s", got, body)
	}
}

func TestForwardRawSSETimeoutAfterCommitTerminatesWithoutForgedJSON(t *testing.T) {
	for _, tc := range []struct {
		name       string
		transport  TransportConfig
		wantSource string
	}{
		{name: "idle", transport: TransportConfig{ResponseIdleTimeout: 40 * time.Millisecond}, wantSource: "response_idle_timeout"},
		{name: "hard", transport: TransportConfig{HardTimeout: 40 * time.Millisecond}, wantSource: "proxy_hard_limit"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logs := captureForwardLogs(t)
			var body *stagedReadCloser
			s := NewServer(Config{Proxy: ProxyConfig{Target: "https://upstream.invalid", Deflation: 1}, Transport: tc.transport})
			s.HTTPClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				body = newStagedReadCloser([]byte("event: content_block_delta\ndata: {\"type\":\"content_block_delta\"}\n\n"), req.Context(), 200*time.Millisecond)
				return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: body}, nil
			})}

			req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"stream":true,"messages":[]}`))
			w := newCountingResponseWriter()
			done := make(chan struct{})
			go func() {
				defer close(done)
				s.forwardRaw(w, req, newRequestMeta(111, tc.name))
			}()

			select {
			case <-w.written:
			case <-time.After(time.Second):
				if body != nil {
					_ = body.Close()
				}
				t.Fatal("首个 SSE 事件未提交")
			}
			select {
			case <-done:
			case <-time.After(time.Second):
				if body != nil {
					_ = body.Close()
				}
				<-done
				t.Fatal("SSE timeout 后 handler 未结束")
			}

			status, headerCalls, responseBody := w.snapshot()
			if status != http.StatusOK || headerCalls != 1 {
				t.Fatalf("已提交 SSE 状态被改写: status=%d calls=%d", status, headerCalls)
			}
			if bytes.Contains(responseBody, []byte("Bad Gateway")) || bytes.Contains(responseBody, []byte("Gateway Timeout")) {
				t.Fatalf("已提交 SSE 被追加伪造 JSON: %s", responseBody)
			}
			assertLogFields(t, logs.String(), "timeout_source="+tc.wantSource, "response_committed=true", "stream=true")
		})
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return fn(req)
}

type timeoutError struct{ message string }

func (err timeoutError) Error() string    { return err.message }
func (timeoutError) Timeout() bool        { return true }
func (timeoutError) Temporary() bool      { return true }
func (timeoutError) Unwrap() error        { return nil }
func (timeoutError) Network() net.Error   { return timeoutError{} }
func (timeoutError) Is(target error) bool { return false }

type countingResponseWriter struct {
	mu          sync.Mutex
	header      http.Header
	status      int
	headerCalls int
	body        bytes.Buffer
	written     chan struct{}
	writtenOnce sync.Once
}

func newCountingResponseWriter() *countingResponseWriter {
	return &countingResponseWriter{header: make(http.Header), written: make(chan struct{})}
}

func (w *countingResponseWriter) Header() http.Header {
	return w.header
}

func (w *countingResponseWriter) WriteHeader(status int) {
	w.mu.Lock()
	w.headerCalls++
	if w.status == 0 {
		w.status = status
	}
	w.mu.Unlock()
	w.signalWritten()
}

func (w *countingResponseWriter) Write(data []byte) (int, error) {
	w.mu.Lock()
	if w.status == 0 {
		w.status = http.StatusOK
		w.headerCalls++
	}
	n, err := w.body.Write(data)
	w.mu.Unlock()
	w.signalWritten()
	return n, err
}

func (w *countingResponseWriter) Flush() {}

func (w *countingResponseWriter) signalWritten() {
	w.writtenOnce.Do(func() { close(w.written) })
}

func (w *countingResponseWriter) snapshot() (int, int, []byte) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.status, w.headerCalls, append([]byte(nil), w.body.Bytes()...)
}

func (w *countingResponseWriter) bodyBytes() []byte {
	_, _, body := w.snapshot()
	return body
}

type stagedReadCloser struct {
	mu       sync.Mutex
	first    []byte
	ctx      context.Context
	fallback time.Duration
	closed   chan struct{}
	close    sync.Once
}

func newStagedReadCloser(first []byte, ctx context.Context, fallback time.Duration) *stagedReadCloser {
	return &stagedReadCloser{first: append([]byte(nil), first...), ctx: ctx, fallback: fallback, closed: make(chan struct{})}
}

func (body *stagedReadCloser) Read(buffer []byte) (int, error) {
	body.mu.Lock()
	if len(body.first) > 0 {
		n := copy(buffer, body.first)
		body.first = body.first[n:]
		body.mu.Unlock()
		return n, nil
	}
	body.mu.Unlock()

	timer := time.NewTimer(body.fallback)
	defer timer.Stop()
	select {
	case <-body.closed:
		return 0, io.ErrClosedPipe
	case <-body.ctx.Done():
		return 0, body.ctx.Err()
	case <-timer.C:
		return 0, timeoutError{"staged body fallback timeout"}
	}
}

func (body *stagedReadCloser) Close() error {
	body.close.Do(func() { close(body.closed) })
	return nil
}

func captureForwardLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(NewLogHandler(&logs, slog.LevelDebug)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return &logs
}

func assertSingleGatewayResponse(t *testing.T, w *countingResponseWriter, wantStatus int, wantBody string) {
	t.Helper()
	status, headerCalls, body := w.snapshot()
	if status != wantStatus || headerCalls != 1 || !bytes.Contains(body, []byte(wantBody)) {
		t.Fatalf("网关响应 = status=%d calls=%d body=%q, want status=%d single body containing %q", status, headerCalls, body, wantStatus, wantBody)
	}
}

func assertLogFields(t *testing.T, output string, fields ...string) {
	t.Helper()
	for _, field := range fields {
		if !strings.Contains(output, field) {
			t.Fatalf("日志缺少 %q:\n%s", field, output)
		}
	}
}

// ── Plan 06 Task 1：upstream 生命周期终态矩阵 ──

func newForwardOutcomeServer(t *testing.T, upstreamURL string) *Server {
	t.Helper()
	cfg := DefaultConfig()
	cfg.Proxy.Target = upstreamURL
	cfg.Proxy.Deflation = 1
	cfg.Debug.Enabled = false
	cfg.Transport.HardTimeout = 0
	return NewServer(cfg)
}

func forwardOutcomeRequest(t *testing.T, server *Server, body string) (requestOutcomeSnapshot, *httptest.ResponseRecorder) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	meta := newRequestMeta(1, "forward-outcome-session")
	sink := &recordingOutcomeDispatcher{}
	meta.Outcome.SetDispatcher(sink)
	recorder := httptest.NewRecorder()
	server.forwardRaw(recorder, req, meta)
	if err := meta.Outcome.SealProducers(); err != nil {
		t.Fatalf("SealProducers: %v", err)
	}
	return sink.sole(t), recorder
}

func TestForwardOutcomeJSONAndSSEMatrix(t *testing.T) {
	for _, tc := range []struct {
		name        string
		contentType string
		payload     string
		stream      bool
		wantStatus  int
	}{
		{
			name: "json_2xx", contentType: "application/json",
			payload: `{"type":"message","usage":{"input_tokens":42,"output_tokens":1}}`,
		},
		{
			name: "json_no_usage", contentType: "application/json",
			payload: `{"type":"message"}`,
		},
		{
			name: "json_malformed", contentType: "application/json",
			payload: `not json at all`,
		},
		{
			name: "sse_complete", contentType: "text/event-stream", stream: true,
			payload: "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"type\":\"message\",\"usage\":{\"input_tokens\":10,\"output_tokens\":1}}}\n\n" +
				"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
		},
		{
			name: "sse_no_usage", contentType: "text/event-stream", stream: true,
			payload: "event: ping\ndata: {\"type\":\"ping\"}\n\n",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", tc.contentType)
				_, _ = io.WriteString(w, tc.payload)
			}))
			defer upstream.Close()
			server := newForwardOutcomeServer(t, upstream.URL)
			body := `{"model":"m","messages":[]}`
			if tc.stream {
				body = `{"model":"m","stream":true,"messages":[]}`
			}
			snapshot, recorder := forwardOutcomeRequest(t, server, body)
			if snapshot.UpstreamState != upstreamStateSuccess {
				t.Fatalf("upstream_state=%s, want success", snapshot.UpstreamState)
			}
			if snapshot.UpstreamStatus != http.StatusOK {
				t.Fatalf("upstream_status=%d, want 200", snapshot.UpstreamStatus)
			}
			if recorder.Code != http.StatusOK {
				t.Fatalf("下游 status=%d, want 200", recorder.Code)
			}
		})
	}
}

func TestForwardOutcomeFailureMatrix(t *testing.T) {
	t.Run("non_2xx", func(t *testing.T) {
		upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = io.WriteString(w, `{"error":"rate limited"}`)
		}))
		defer upstream.Close()
		server := newForwardOutcomeServer(t, upstream.URL)
		snapshot, _ := forwardOutcomeRequest(t, server, `{"model":"m","messages":[]}`)
		if snapshot.UpstreamState != upstreamStateNon2xx || snapshot.UpstreamStatus != http.StatusTooManyRequests {
			t.Fatalf("upstream=%s/%d, want non_2xx/429", snapshot.UpstreamState, snapshot.UpstreamStatus)
		}
		if snapshot.FailureClass != persistenceFailureUpstream {
			t.Fatalf("failure_class=%s, want upstream", snapshot.FailureClass)
		}
	})

	t.Run("transport_failure", func(t *testing.T) {
		upstream := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
		url := upstream.URL
		upstream.Close()
		server := newForwardOutcomeServer(t, url)
		snapshot, recorder := forwardOutcomeRequest(t, server, `{"model":"m","messages":[]}`)
		if snapshot.UpstreamState != upstreamStateTransportFailure {
			t.Fatalf("upstream_state=%s, want transport_failure", snapshot.UpstreamState)
		}
		if recorder.Code != http.StatusBadGateway {
			t.Fatalf("下游 status=%d, want 502", recorder.Code)
		}
		if strings.Contains(recorder.Body.String(), "127.0.0.1") && strings.Contains(recorder.Body.String(), "@") {
			t.Fatalf("网关响应泄漏凭证: %s", recorder.Body.String())
		}
	})

	t.Run("header_timeout", func(t *testing.T) {
		release := make(chan struct{})
		upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			<-release
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{}`)
		}))
		defer func() {
			close(release)
			upstream.Close()
		}()
		server := newForwardOutcomeServer(t, upstream.URL)
		server.Config.Transport.NonStreamHeaderTimeout = 40 * time.Millisecond
		server.HTTPClient = newUpstreamHTTPClient(server.Config.Transport)
		snapshot, recorder := forwardOutcomeRequest(t, server, `{"model":"m","messages":[]}`)
		if snapshot.UpstreamState != upstreamStateHeaderTimeout {
			t.Fatalf("upstream_state=%s, want header_timeout", snapshot.UpstreamState)
		}
		if recorder.Code != http.StatusGatewayTimeout {
			t.Fatalf("下游 status=%d, want 504", recorder.Code)
		}
	})

	t.Run("hard_timeout", func(t *testing.T) {
		release := make(chan struct{})
		upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			<-release
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{}`)
		}))
		defer func() {
			close(release)
			upstream.Close()
		}()
		server := newForwardOutcomeServer(t, upstream.URL)
		server.Config.Transport.HardTimeout = 40 * time.Millisecond
		server.HTTPClient = newUpstreamHTTPClient(server.Config.Transport)
		snapshot, _ := forwardOutcomeRequest(t, server, `{"model":"m","messages":[]}`)
		if snapshot.UpstreamState != upstreamStateHardTimeout {
			t.Fatalf("upstream_state=%s, want hard_timeout", snapshot.UpstreamState)
		}
	})

	t.Run("committed_stream_failure", func(t *testing.T) {
		server := NewServer(Config{Proxy: ProxyConfig{Deflation: 1}})
		meta := newRequestMeta(1, "forward-committed-session")
		sink := &recordingOutcomeDispatcher{}
		meta.Outcome.SetDispatcher(sink)
		resp := &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": {"text/event-stream"}},
			Body: &readThenErrorCloser{
				reader: strings.NewReader("event: message_start\n" +
					"data: {\"type\":\"message_start\",\"message\":{\"type\":\"message\",\"usage\":{\"input_tokens\":10,\"output_tokens\":1}}}\n\n"),
				err: errors.New("upstream stream reset"),
			},
		}
		recorder := httptest.NewRecorder()
		result := server.handleSSE(recorder, resp, meta, time.Now(), "model", 1)
		if result.err == nil {
			t.Fatal("被中断的 SSE 应返回错误")
		}
		if err := meta.Outcome.SealProducers(); err != nil {
			t.Fatalf("SealProducers: %v", err)
		}
		snapshot := sink.sole(t)
		if snapshot.UpstreamState != upstreamStateCommittedFailure {
			t.Fatalf("upstream_state=%s, want committed_failure", snapshot.UpstreamState)
		}
	})

	t.Run("json_body_read_failure", func(t *testing.T) {
		server := NewServer(Config{Proxy: ProxyConfig{Deflation: 1}})
		meta := newRequestMeta(1, "forward-body-read-session")
		sink := &recordingOutcomeDispatcher{}
		meta.Outcome.SetDispatcher(sink)
		resp := &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": {"application/json"}, "Content-Encoding": {"gzip"}},
			Body:       io.NopCloser(strings.NewReader("not gzip at all")),
		}
		recorder := httptest.NewRecorder()
		if result := server.handleJSON(recorder, resp, meta, time.Now(), "model", 1); result.err == nil {
			t.Fatal("gzip 解压失败应返回错误")
		}
		if err := meta.Outcome.SealProducers(); err != nil {
			t.Fatalf("SealProducers: %v", err)
		}
		if got := sink.sole(t).UpstreamState; got != upstreamStateBodyReadFailure {
			t.Fatalf("upstream_state=%s, want body_read_failure", got)
		}
	})

	t.Run("downstream_write_failure", func(t *testing.T) {
		server := NewServer(Config{Proxy: ProxyConfig{Deflation: 1}})
		meta := newRequestMeta(1, "forward-downstream-session")
		sink := &recordingOutcomeDispatcher{}
		meta.Outcome.SetDispatcher(sink)
		resp := &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": {"application/json"}},
			Body:       io.NopCloser(strings.NewReader(`{"type":"message"}`)),
		}
		writer := &failingResponseWriter{header: http.Header{}}
		if result := server.handleJSON(writer, resp, meta, time.Now(), "model", 1); result.failureClass != downstreamWriteFailureClass {
			t.Fatalf("failure_class=%q, want %q", result.failureClass, downstreamWriteFailureClass)
		}
		if err := meta.Outcome.SealProducers(); err != nil {
			t.Fatalf("SealProducers: %v", err)
		}
		if got := sink.sole(t).UpstreamState; got != upstreamStateCommittedFailure {
			t.Fatalf("upstream_state=%s, want committed_failure", got)
		}
	})
}

// failingResponseWriter 在 WriteHeader 之后让 Write 失败，用于证明下游写失败
// 不会被伪装成 upstream success。
type failingResponseWriter struct {
	header http.Header
	status int
}

func (w *failingResponseWriter) Header() http.Header { return w.header }

func (w *failingResponseWriter) WriteHeader(status int) { w.status = status }

func (w *failingResponseWriter) Write([]byte) (int, error) {
	return 0, errors.New("downstream connection reset")
}

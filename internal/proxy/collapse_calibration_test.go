package proxy

// COLLAPSE_FIX_PLAN 的配套测试：校准系数（frozen 层滚动样本）、estimate/actual
// 配对（mark 同点补算、响应侧同点写入、中断路径不写）、边界保护重试，以及
// threshold ≥60000 环境下的折叠落点回归——16000 环境下新旧 floor 公式都被
// 10000 下限钉死，对口径修正零区分度。

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// ── 校准系数（SawtoothTrigger 滚动样本队列）──

func TestSawtoothCalibrationRatioColdStartClampAndWindow(t *testing.T) {
	st := NewSawtoothTrigger(time.Minute, 1000, 500)
	if got := st.CalibrationRatio("thread"); got != defaultCalibrationRatio {
		t.Fatalf("冷启动 ratio=%v, want 常数 %v", got, defaultCalibrationRatio)
	}
	// 无效输入不落样本
	st.RecordCalibrationSample("thread", 0, 100)
	st.RecordCalibrationSample("thread", 100, 0)
	if len(st.calibrationSamples) != 0 {
		t.Fatalf("无效输入产生了样本: %v", st.calibrationSamples)
	}

	// 窗口内混合样本取中位数（1.4 与 1.6 各一半 → 1.5，未被 clamp 干扰）
	for i := 0; i < calibrationSampleWindow/2; i++ {
		st.RecordCalibrationSample("thread", 14, 10)
		st.RecordCalibrationSample("thread", 16, 10)
	}
	if got := st.CalibrationRatio("thread"); math.Abs(got-1.5) > 1e-9 {
		t.Fatalf("中位 ratio=%v, want 1.5", got)
	}

	// clamp 下界：持续低样本
	low := NewSawtoothTrigger(time.Minute, 1000, 500)
	for i := 0; i < calibrationSampleWindow; i++ {
		low.RecordCalibrationSample("thread", 50, 100) // 0.5，远低于过渡期下界
	}
	if got := low.CalibrationRatio("thread"); got != calibrationMinRatio {
		t.Fatalf("低样本 ratio=%v, want clamp 下界 %v", got, calibrationMinRatio)
	}

	// clamp 上界：持续高样本
	high := NewSawtoothTrigger(time.Minute, 1000, 500)
	for i := 0; i < calibrationSampleWindow; i++ {
		high.RecordCalibrationSample("thread", 300, 100) // 3.0
	}
	if got := high.CalibrationRatio("thread"); got != calibrationMaxRatio {
		t.Fatalf("高样本 ratio=%v, want clamp 上界 %v", got, calibrationMaxRatio)
	}

	// 窗口滚动：8 个低样本后压入 8 个高样本，旧样本必须全部挤出
	for i := 0; i < calibrationSampleWindow; i++ {
		high.RecordCalibrationSample("thread", 140, 100) // 1.4
	}
	if got := len(high.calibrationSamples["thread"]); got != calibrationSampleWindow {
		t.Fatalf("窗口长度=%d, want %d", got, calibrationSampleWindow)
	}
	if got := high.CalibrationRatio("thread"); got != 1.4 {
		t.Fatalf("滚动后 ratio=%v, want 1.4（旧样本已挤出）", got)
	}
}

// ── estimate/actual 配对 ──

func TestMarkForwardedCoordinatesComputesLocalEstimate(t *testing.T) {
	tc, err := NewTokenCounter()
	if err != nil {
		t.Fatalf("NewTokenCounter: %v", err)
	}
	system := json.RawMessage(`[{"type":"text","text":"estimate system prompt"}]`)
	tools := json.RawMessage(`[{"name":"estimate_tool","input_schema":{"type":"object"}}]`)
	forwarded := pipelineMessages(9, 6)

	meta := newRequestMeta(1, "estimate-pairing")
	meta.PressureDecision = pressureDecision{Available: true}
	markForwardedPressureCoordinates(meta, forwardedPressureBody(t, system, tools, forwarded), tc)
	want := tc.CountMessagesTokens(forwarded) + measureTopLevelTokens(system, tc) + measureTopLevelTokens(tools, tc)
	if got := meta.PressureDecision.ForwardedLocalEstimate; got != want {
		t.Fatalf("ForwardedLocalEstimate=%d, want %d（messages+system+tools 口径）", got, want)
	}

	// body 不可解析：坐标未绑定，estimate 保持 0（响应侧因此不产生样本）
	bad := newRequestMeta(2, "estimate-pairing")
	bad.PressureDecision = pressureDecision{Available: true}
	markForwardedPressureCoordinates(bad, []byte(`{"messages":`), tc)
	if bad.PressureDecision.ForwardedLocalEstimate != 0 || bad.PressureDecision.ForwardedCoordinatesBound {
		t.Fatalf("坏 body 应保持 estimate=0 且坐标未绑定: %+v", bad.PressureDecision)
	}
}

func TestApplyPressureBaselineUsageRecordsPairedSample(t *testing.T) {
	tc, err := NewTokenCounter()
	if err != nil {
		t.Fatalf("NewTokenCounter: %v", err)
	}
	s := NewServer(DefaultConfig())
	s.Sawtooth = NewSawtoothTrigger(time.Hour, 100000, 50000)
	s.TokenCounter = tc

	meta := newRequestMeta(1, "pair-sample-session")
	meta.PressureDecision = pressureDecision{Available: true}
	forwarded := pipelineMessages(5, 4)
	markForwardedPressureCoordinates(meta, forwardedPressureBody(t, nil, nil, forwarded), tc)
	estimate := meta.PressureDecision.ForwardedLocalEstimate
	if estimate <= 0 {
		t.Fatalf("mark 未补算 estimate: %+v", meta.PressureDecision)
	}

	actual := estimate + estimate/2 // 模拟 1.5 倍真实计费
	if !s.applyPressureBaselineUsage(meta, actual) {
		t.Fatalf("exact baseline 未被接受")
	}
	if got := len(s.Sawtooth.calibrationSamples["pair-sample-session"]); got != 1 {
		t.Fatalf("完整响应后样本数=%d, want 1", got)
	}

	// 坐标未经 mark 绑定：actual 拒绝写回，estimate 也不得单独产生样本
	unbound := newRequestMeta(2, "pair-sample-session")
	unbound.PressureDecision = pressureDecision{Available: true, ForwardedLocalEstimate: 12345}
	if s.applyPressureBaselineUsage(unbound, 20000) {
		t.Fatalf("未绑定坐标的 actual 不应写回")
	}
	if got := len(s.Sawtooth.calibrationSamples["pair-sample-session"]); got != 1 {
		t.Fatalf("坐标未绑定却写入了样本: %d", got)
	}
}

// 端到端：SSE 流在 message_start 后中断（无 message_stop）——usage 不完整，
// baseline 与校准样本都不写。
func TestCalibrationSampleSkippedOnInterruptedStream(t *testing.T) {
	const sessionID = "calibration-interrupted-stream"
	serverRef := (*Server)(nil)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if streamRequest(body) {
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = w.Write([]byte("event: message_start\n" +
				`data: {"type":"message_start","message":{"type":"message","usage":{"input_tokens":4800,"output_tokens":1}}}` + "\n\n"))
			return // 连接直接结束：只有 message_start，没有 message_stop
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"type":"message","usage":{"input_tokens":2400,"output_tokens":1}}`))
	}))
	defer upstream.Close()

	server := newPipelineTestServerWithThreshold(t, upstream.URL, 60000)
	serverRef = server
	_ = serverRef

	messages := pipelineMessages(5, 6)
	servePipelineRequest(t, server, sessionID, messages)
	key := soleCalibrationKey(t, server.Sawtooth)
	if got := calibrationSampleCount(server.Sawtooth, key); got != 1 {
		t.Fatalf("第一轮完整 JSON 响应后样本数=%d, want 1", got)
	}

	servePipelineRequestWith(t, server, sessionID, messages, map[string]any{"stream": true}, nil)
	if got := calibrationSampleCount(server.Sawtooth, key); got != 1 {
		t.Fatalf("中断流不应写入校准样本: %d", got)
	}
	baseline := server.Sawtooth.PressureBaseline(key)
	if baseline.ActualTokens != 2400 {
		t.Fatalf("中断流污染了 baseline actual: %+v", baseline)
	}
}

func soleCalibrationKey(t *testing.T, st *SawtoothTrigger) string {
	t.Helper()
	st.mu.RLock()
	defer st.mu.RUnlock()
	if len(st.calibrationSamples) != 1 {
		t.Fatalf("校准样本 key 数=%d, want 1", len(st.calibrationSamples))
	}
	for key := range st.calibrationSamples {
		return key
	}
	return ""
}

func calibrationSampleCount(st *SawtoothTrigger, key string) int {
	st.mu.RLock()
	defer st.mu.RUnlock()
	return len(st.calibrationSamples[key])
}

// ── 边界保护重试 ──

func TestCollapseCutoffWithBoundaryGuard(t *testing.T) {
	tc, err := NewTokenCounter()
	if err != nil {
		t.Fatalf("NewTokenCounter: %v", err)
	}

	t.Run("尾部大消息触发逐级上调", func(t *testing.T) {
		// n=40：idx0..31 每条 ~300；idx32..38 每条 ~100；idx39 单条 ~20000。
		// 初始 floor=19000 在 i=39 立即满足 → keepRecent 钳制到 32（保留 8 <12）
		// → 逐级上调后 cutoff 前移，保留条数恢复 ≥12。
		messages := make([]Message, 0, 40)
		for i := 0; i < 32; i++ {
			messages = append(messages, messageWithAtLeastTokens(t, tc, roleFor(i), 300))
		}
		for i := 32; i < 39; i++ {
			messages = append(messages, messageWithAtLeastTokens(t, tc, roleFor(i), 100))
		}
		messages = append(messages, messageWithAtLeastTokens(t, tc, "user", 20000))

		const n = 40
		original := CalcCollapseCutoff(messages, 19000, tc, 8)
		if original != n-8 || n-original >= 12 {
			t.Fatalf("fixture 前提不成立: 初始应钳制到 n-keepRecent 且保留 <12, got cutoff=%d 保留=%d", original, n-original)
		}
		cutoff := collapseCutoffWithBoundaryGuard(messages, 19000, 60000, 8, tc)
		if cutoff < 0 || cutoff >= original {
			t.Fatalf("重试应使 cutoff 前移: guard=%d, original=%d", cutoff, original)
		}
		if kept := n - cutoff; kept < 12 {
			t.Fatalf("重试后保留条数=%d, want ≥12", kept)
		}
	})

	t.Run("封顶后接受钳制结果", func(t *testing.T) {
		// n=20：idx0..18 每条 ~600，idx19 单条 ~20000。keepRecent 窗口内
		// token 总量（20000+7×600 ≈ 24200）覆盖全部可尝试的 floor
		// （10000→24000），cutoff 永远钳在 n-keepRecent=12：循环走到封顶后
		// 必须终止并接受钳制结果。
		messages := make([]Message, 0, 20)
		for i := 0; i < 19; i++ {
			messages = append(messages, messageWithAtLeastTokens(t, tc, roleFor(i), 600))
		}
		messages = append(messages, messageWithAtLeastTokens(t, tc, "user", 20000))

		cutoff := collapseCutoffWithBoundaryGuard(messages, 10000, 30000, 8, tc)
		if cutoff != 12 {
			t.Fatalf("封顶后应接受钳制 cutoff=%d, want 12（n-keepRecent）", cutoff)
		}
	})

	t.Run("正常场景直通", func(t *testing.T) {
		// floor 选在窗口内时 cutoff 不会被 keepRecent 或 keepSafe 钳制，guard
		// 第一轮即成功，结果与直接调用 CalcCollapseCutoff 完全一致。
		// （floor 过小时尾部窗口条数不足 keepSafe=12，会误入逐级上调路径。）
		messages := make([]Message, 0, 30)
		for i := 0; i < 30; i++ {
			messages = append(messages, messageWithAtLeastTokens(t, tc, roleFor(i), 100))
		}
		const floor = 3000
		want := CalcCollapseCutoff(messages, floor, tc, 8)
		if got := collapseCutoffWithBoundaryGuard(messages, floor, 60000, 8, tc); got != want {
			t.Fatalf("guard=%d, want 直通 %d", got, want)
		}
	})

	t.Run("消息不足时返回负值不悬挂", func(t *testing.T) {
		messages := pipelineMessages(5, 6) // keepRecent=8 > n
		if got := collapseCutoffWithBoundaryGuard(messages, 19000, 60000, 8, tc); got != -1 {
			t.Fatalf("guard=%d, want -1", got)
		}
	})
}

func roleFor(i int) string {
	if i%2 == 1 {
		return "assistant"
	}
	return "user"
}

// ── local_full 判定乘校准系数（P0：断流期漏判链）──

// 冷启动 local_full 判定必须乘上校准系数再比 threshold。实测本地估算系统性
// 低估 1.5~1.6 倍，不乘系数时断流/429 常态下（baseline 无法刷新）会持续漏判
// 到真实 ~1.6×threshold 才触发，中途撞上游 prompt-too-long。
func TestLocalFullPressureAppliesCalibrationRatio(t *testing.T) {
	const (
		sessionID = "local-full-calibration"
		threshold = 150000
	)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"type":"message","usage":{"input_tokens":10,"output_tokens":1}}`))
	}))
	defer upstream.Close()

	server := newPipelineTestServerWithThreshold(t, upstream.URL, threshold)
	server.searchAndExpandFn = func(current []Message, _ *SQLiteStore, _ int, _ *TokenCounter, _ *Budget, meta *requestMeta) RecallOutcome {
		return RecallOutcome{Messages: current}
	}

	// 本地估算约 threshold 的 0.6~0.9 倍区间：乘 1.50 后越过阈值、裸估算则不越。
	// fixture 先量出单条 token 数，再按目标精确铺条数。
	tc := server.TokenCounter
	perMessage := tc.CountMessageTokens(Message{Role: "user", Content: mustMarshal(strings.Repeat("context ", 40))})
	count := int(float64(threshold)*0.7) / perMessage
	if count < 2 {
		t.Fatalf("fixture 条数异常: perMessage=%d count=%d", perMessage, count)
	}
	messages := make([]Message, 0, count+1)
	for i := 0; i < count; i++ {
		messages = append(messages, Message{Role: roleFor(i), Content: mustMarshal(strings.Repeat("context ", 40))})
	}
	rawEstimate := tc.CountMessagesTokens(messages) + measureTopLevelTokens(nil, tc) + measureTopLevelTokens(nil, tc)
	ratio := server.Sawtooth.CalibrationRatio(sessionID) // 冷启动 = defaultCalibrationRatio
	if rawEstimate >= threshold || rawEstimate <= threshold/2 {
		t.Fatalf("fixture 本地全量估算=%d 应落在 (%d, %d] 区间内", rawEstimate, threshold/2, threshold)
	}

	var decision pressureDecision
	capture := server.searchAndExpandFn
	server.searchAndExpandFn = func(current []Message, store *SQLiteStore, limit int, counter *TokenCounter, budget *Budget, meta *requestMeta) RecallOutcome {
		out := capture(current, store, limit, counter, budget, meta)
		decision = meta.PressureDecision
		return out
	}
	servePipelineRequest(t, server, sessionID, messages)

	if decision.Source != pressureSourceLocalFull {
		t.Fatalf("应为 local_full 冷启动判定: source=%s", decision.Source)
	}
	want := int(float64(decision.FullLocalEstimate) * ratio)
	if decision.SelectedPressure != want {
		t.Fatalf("SelectedPressure=%d, want FullLocalEstimate×ratio = %d×%.2f = %d",
			decision.SelectedPressure, decision.FullLocalEstimate, ratio, want)
	}
	if decision.SelectedPressure <= threshold {
		t.Fatalf("乘系数后应越过阈值触发折叠: pressure=%d threshold=%d", decision.SelectedPressure, threshold)
	}
	if got := archiveCount(t, server.Store); got != 1 {
		t.Fatalf("应产生 collapse archive: %d", got)
	}
}

// actual_plus_delta 与 conservative_high_water 是真实口径，绝不能再乘校准系数。
func TestCalibratedSourcesSkipRatio(t *testing.T) {
	const sessionID = "calibrated-sources-skip-ratio"
	st := NewSawtoothTrigger(time.Minute, 1000, 500)
	for i := 0; i < calibrationSampleWindow; i++ {
		st.RecordCalibrationSample(sessionID, 300, 100) // ratio 样本 3.0 → clamp 1.80
	}
	baseline := pressureBaseline{
		ActualTokens:              900,
		MessageCount:              2,
		SystemFingerprint:         fingerprintTopLevelJSON(nil),
		ToolsFingerprint:          fingerprintTopLevelJSON(nil),
		MessagesPrefixFingerprint: fingerprintMessagesPrefix(pipelineMessages(2, 4), 2),
		Available:                 true,
	}
	messages := append(pipelineMessages(2, 4), pipelineMessages(2, 4)...)
	tc, err := NewTokenCounter()
	if err != nil {
		t.Fatalf("NewTokenCounter: %v", err)
	}
	decision := buildPressureDecision(messages, nil, nil, baseline, tc, 1000)
	if decision.Source != pressureSourceActualPlusDelta {
		t.Fatalf("fixture 应落 actual_plus_delta: %s", decision.Source)
	}
	if decision.SelectedPressure != decision.PreviousActual+decision.NewMessageDelta {
		t.Fatalf("actual_plus_delta 不得被校准系数改写: %d ≠ %d+%d",
			decision.SelectedPressure, decision.PreviousActual, decision.NewMessageDelta)
	}
}

func messageWithAtLeastTokens(t *testing.T, tc *TokenCounter, role string, target int) Message {
	t.Helper()
	words := target
	for {
		text := strings.Repeat("context ", words)
		message := Message{Role: role, Content: mustMarshal(text)}
		if tc.CountMessageTokens(message) >= target {
			return message
		}
		words += words/4 + 1
	}
}

// ── 折叠当次落点回归（threshold ≥60000 环境）──

// 第二轮用超过阈值的完整 history 触发折叠，断言转发 wire 的本地全量估算
// 换算到真实计费口径后落在目标（threshold/2）的 1.20 倍上界内。
func TestCollapseFloorProjectsRealBillingLanding(t *testing.T) {
	const (
		threshold          = 60000
		sessionID          = "collapse-real-billing-projection"
		simulatedInflation = 1.5
	)
	serverRef := (*Server)(nil)
	var forwardedBodies [][]byte
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		forwardedBodies = append(forwardedBodies, body)
		var payload struct {
			Messages []Message `json:"messages"`
		}
		if err := json.Unmarshal(body, &payload); err != nil {
			t.Errorf("decode upstream request: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		actual := int(float64(serverRef.TokenCounter.CountMessagesTokens(payload.Messages)) * simulatedInflation)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"type":"message","usage":{"input_tokens":%d,"output_tokens":1}}`, actual)
	}))
	defer upstream.Close()

	server := newPipelineTestServerWithThreshold(t, upstream.URL, threshold)
	serverRef = server

	// 第一轮：小请求建立校准样本，滚动系数收敛到 1.5（不再依赖冷启动常数）。
	servePipelineRequest(t, server, sessionID, pipelineMessages(5, 6))
	key := soleCalibrationKey(t, server.Sawtooth)
	if got := server.Sawtooth.CalibrationRatio(key); math.Abs(got-simulatedInflation) > 0.01 {
		t.Fatalf("第一轮后 calibration=%v, want ≈%v", got, simulatedInflation)
	}

	// 第二轮：超过阈值的完整 history 触发折叠。
	second := messagesWithTotalTokens(t, server.TokenCounter, threshold+5000)
	servePipelineRequest(t, server, sessionID, second)
	if got := archiveCount(t, server.Store); got != 1 {
		t.Fatalf("折叠归档数=%d, want 1", got)
	}

	last := forwardedBodies[len(forwardedBodies)-1]
	var payload struct {
		Messages []Message `json:"messages"`
	}
	if err := json.Unmarshal(last, &payload); err != nil {
		t.Fatalf("decode forwarded body: %v", err)
	}
	estimate := server.TokenCounter.CountMessagesTokens(payload.Messages)
	landing := int(float64(estimate) * simulatedInflation)
	if limit := threshold / 2 * 6 / 5; landing > limit {
		t.Fatalf("折叠当次预测落点=%d（estimate=%d），超过上界 %d", landing, estimate, limit)
	}
	if kept := len(payload.Messages); kept < 2+server.Config.Stubify.KeepRecent+4 {
		t.Fatalf("折叠后保留条数=%d, want ≥%d", kept, 2+server.Config.Stubify.KeepRecent+4)
	}
}

func messagesWithTotalTokens(t *testing.T, tc *TokenCounter, target int) []Message {
	t.Helper()
	messages := make([]Message, 0, 128)
	total := 0
	for i := 0; total < target; i++ {
		message := Message{
			Role:    roleFor(i),
			Content: mustMarshal(fmt.Sprintf("history-%03d %s", i, strings.Repeat("context ", 40))),
		}
		total += tc.CountMessageTokens(message)
		messages = append(messages, message)
	}
	return messages
}

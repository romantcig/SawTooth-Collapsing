# Phase 10: Token 压力与触发统一 - Pattern Map

> generic-agent workaround：本文件由通用代理按 `gsd-pattern-mapper` 契约生成；未使用 typed GSD pattern mapper。模式仅来自当前 Sawtooth 文件、相邻测试和锁定 YesMem 快照。

**Mapped:** 2026-07-15
**Files analyzed:** 12 个计划修改文件
**Analogs found:** 12 / 12

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|---|---|---|---|---|
| `internal/proxy/proxy.go` | controller / orchestration | request-response、transform | 同文件 `HandleMessages` 单管线出口；YesMem `estimateTotalTokens` | exact + locked reference |
| `internal/proxy/frozen.go` | state store / service | event-driven、SQLite persistence | 同文件 `SawtoothTrigger`、`FrozenStubs` | exact |
| `internal/proxy/forward.go` | response adapter | streaming + request-response | 同文件 `handleSSE` / `handleJSON` + `totalInputTokens` | exact |
| `internal/proxy/debug_facts.go` | audit utility | file-I/O、transform | 同文件 `debugFact` / `writeDebugFact` | exact |
| `internal/proxy/request_meta.go` | request-scoped model | request-response、event gating | 同文件 `tracksSawtoothState` / `sync.Once` stage guards | exact |
| `internal/proxy/auxiliary.go` | classifier | transform、request-response | 同文件严格 title 分类器 | exact |
| `internal/proxy/session.go` | classifier / feature extractor | transform、request-response | 同文件 billing + `agentContext` 强特征 | exact |
| `internal/proxy/proxy_pipeline_test.go` | integration test | request-response | title 状态隔离 + previous actual 越线回归 | exact |
| `internal/proxy/phase08_integration_test.go` | requirement integration test | request-response | `TestPhase08AgentIsolationMatrix` | exact |
| `internal/proxy/forward_test.go` | adapter/state test | streaming + request-response | SSE/JSON actual-before-deflation 双路径 | exact |
| `internal/proxy/debug_facts_test.go` | security/schema test | file-I/O | 白名单、secret sentinel、request_id/stage 文件 | exact |
| `internal/proxy/auxiliary_test.go` / `session_test.go` | classifier tests | table-driven transform | 强/弱形态矩阵与 fail-closed 边界 | exact |

## Strongest Existing Analogs

### 1. YesMem 的两路径压力估算（锁定参考）

**Source:** `C:/Users/romantcig/Desktop/yesmem/internal/proxy/proxy_helpers.go`, `measureOverhead`（稳定符号，约 480–499）与 `estimateTotalTokens`（约 509–532）。

可复制的核心结构：

```go
lastActual := s.sawtoothTrigger.GetLastTokens(threadID)
lastMsgCount := s.sawtoothTrigger.GetLastMessageCount(threadID)
if lastActual == 0 || lastMsgCount == 0 || lastMsgCount > len(messages) {
    return s.countMessageTokens(messages) + overhead
}
deltaTokens := s.countMessageTokens(messages[lastMsgCount:])
return lastActual + deltaTokens
```

Phase 10 应吸收其“首次 full、稳定 actual+delta、消息缩短 full fallback”骨架，但不要照搬 5000-token overhead floor。Sawtooth 的锁定决定要求分别计算 messages/system/tools，且 system/tools 指纹变化也必须使 actual 基线失效。actual 路径不得再次叠加 overhead。

**Test analogs:** `proxy_helpers_test.go#TestEstimateTotalTokens_WithPreviousActual`、`#TestEstimateTotalTokens_FallbackOnFirstRequest`、`#TestEstimateTotalTokens_FallbackOnMessageShrink`（约 196–266）。三组测试恰好对应 Phase 10 的稳定、首次、坐标缩短分支；新增指纹变化分支应沿用相同 table/fixture 风格。

### 2. SawtoothTrigger 的锁、冷启动和持久化生命周期

**Source:** `internal/proxy/frozen.go#SawtoothTrigger`（约 531–544）、`#ShouldTrigger`（约 574–623）、`#UpdateAfterResponse`（约 625–638）、`#loadSawtoothFromDB`（约 664 起）。

现有模式：

```go
st.mu.Lock()
st.lastTotalTokens[threadID] = totalInputTokens
st.lastMessageCount[threadID] = messageCount
st.lastRequestTime[threadID] = time.Now()
st.loadedFromDB[threadID] = true
st.mu.Unlock()

if st.persistFn != nil {
    data, _ := json.Marshal(persistedState{Tokens: totalInputTokens, MsgCount: messageCount})
    st.persistFn("sawtooth:"+threadID, string(data))
}
```

为 pressure baseline 增加 actual、消息坐标、system/tools 指纹时，应扩展同一个 `persistedState` 和同一 `sawtooth:<threadID>` 生命周期；读取采用 `RLock` 快路径、必要时释放锁后 lazy load、再重读。不要创建平行状态容器。指纹只持久化固定长度 hash/脱敏元数据，不持久化正文。

`ShouldTrigger` 的受限枚举 `TriggerNone/Tokens/Pause/Emergency`（约 516–523）是 `trigger_reason` 的最近 analog。保留当前 threshold 语义和 `threshold+10_000` emergency 保险线，不增加隐藏比例。

同文件 `FrozenStubs` 的 `boundaryHash`/`prefixHash`（约 23–50）是安全指纹存储 analog：SHA-256、锁内 map、JSON 持久化、只比较 hash。Phase 10 的 system/tools fingerprint 应复用这种“正文不出状态”的形态。

### 3. JSON/SSE 共用 actual 口径、先记录再 deflate

**Source:** `internal/proxy/forward.go#totalInputTokens`（约 83–99）、`#handleSSE`（约 576–677）、`#handleJSON`（约 738–794）。

```go
for _, field := range []string{
    "input_tokens", "cache_creation_input_tokens", "cache_read_input_tokens",
} {
    total += nonNegativeUsageToken(usage[field])
}
```

SSE `message_start` 在 `processSSEEvent` deflation 前调用 `writeUsageDebugFacts` 与 `UpdateAfterResponse`（约 635–648）；JSON 只在 2xx、成功解析且存在 usage 时做同样处理（约 759–780）。Phase 10 必须继续让两条路径调用同一个 total helper 和同一个 `meta.tracksSawtoothState()` 闸门。非 2xx、JSON 解析失败、SSE 无合法 usage 时不建立 baseline，也不伪造 actual。

**Test analogs:** `forward_test.go#TestHandleSSECacheUsagePersistsTotalBeforeDeflation`（约 49–74）与 `#TestHandleJSONCacheUsagePersistsTotalAndColdStartTriggers`（约 76–108）。保持成对测试：同一 usage fixture、同一 93252 total、客户端仍看到 deflated 196→98；再加入辅助身份不写回和 baseline 指纹更新断言。

### 4. 扁平 Debug facts、受限枚举与原子分阶段文件

**Source:** `internal/proxy/debug_facts.go#debugFact`（约 34–54）、`#writeRequestDebugFacts`（约 56–103）、`#writeUsageDebugFacts`（约 105–123）、`#writeDebugFact`（约 125–147）。

既有契约：事实对象只含数字、布尔、时间和受限枚举；每个阶段由 `sync.Once` 保证最多一次；文件名包含 timestamp、request ID、stage；`json.Marshal` 后通过 `writeDebugEntryFile` 原子落盘。新增 `pressure_decision` 应作为新的 `debugStage` 和新的 stage once guard，字段保持扁平，不嵌套 raw system/tools/messages。

**Security test analog:** `debug_facts_test.go#TestDebugFactsSchemaAndSecretSafety`（约 18–94）维护显式 `allowedDebugFactKeys`，注入 session、model suffix、system、parent ID、ClaudeMD、base64、Authorization、billing header sentinel，并扫描序列化结果。新增字段必须同步进入白名单，且只能是整数、bool 或受限枚举；继续拒绝嵌套对象。

**Join/precision analog:** `#TestDebugFactsUsageUsesTotalInputTokens`（约 120–140）直接反序列化 `debugFact` 并断言精确整数。Phase 10 应增加同 request_id 下 `pressure_decision` 与 `response_usage` 的双文件解析，并证明 forwarded estimate 不等于 selected pressure 字段。

### 5. 高置信辅助分类与状态旁路

**Sources:**

- `internal/proxy/auxiliary.go#classifyAuxiliaryRequest`（约 37–58）、`#hasCompleteSessionEnvelope`（约 86–127）、`#inspectAuxiliaryOutputConfig`（约 138–177）。
- `internal/proxy/session.go#classifyAgentFeatures`（约 117–127）、`#hasBillingSubagentMarker`（约 135–142）、`#inspectAgentContext`（约 144–180）。
- `internal/proxy/request_meta.go#tracksSawtoothState`（约 25–29）与 `#auxiliaryLogger`（约 65–75）。

标题分类使用逐项 fail-closed：单条 user、完整平衡 envelope、严格 system intent、output_config 缺失或 title-only schema；null/畸形/额外字段均拒绝。子代理使用 token-boundary billing regex 或结构化 `agentContext`，未命中默认 main。新增 2.1.207 system attribution block 应在 `inspectAgentSystem` 相邻位置做“独立 text block + 固定前缀 + 严格 marker 边界”的窄匹配，不得全文泛搜。

当前 `requestMeta` 只记录 `RequestKind`，导致 subagent 在响应路径仍可能按 normal meta；Phase 10 应沿用 request-scoped 字段 + 单一 predicate 的模式，把入口得到的 agent role/classification 写入 meta，使 title 和 subagent 都由 `tracksSawtoothState()` 判为只读。`auxiliaryLogger` 只继承 request_id，不继承 session ID，是 pressure 摘要/分类日志脱敏的 analog。

## Per-file Pattern Assignments

### `internal/proxy/proxy.go`

- 在 `HandleMessages` 分类完成、persistent context detach 之后，把 `classification` 写入 `requestMeta`；可靠辅助请求仍在 Frozen/Archive/seq/pressure 之前返回。
- 在当前 `rawEstimate/historyEstimate/contextTokens` 和首次 `needCompress` 区域（约 485–607）建立唯一 `pressureDecision`。不要保留“本地 totalTokens 一次 + `ShouldTrigger(sessionID, 0)` 一次 + Recall 后再重新判定”的多口径决策。
- 触发依据使用 raw/authoritative request 的 messages/system/tools；Collapse 后或 forwarded 消息大小只进入 forwarded facts，不能覆盖 decision。
- 日志沿用单条 `meta.Logger.Info` + 受限字段模式；每条主请求最多一条 pressure 摘要，辅助请求不输出主压力摘要。

### `internal/proxy/frozen.go`

- 扩展 `SawtoothTrigger`，保持 per-session map + `sync.RWMutex` + lazy DB load。
- 提供一次性读取 baseline snapshot 的 API，避免 planner/实现者分别调用多个 getter 造成锁间状态不一致。
- `UpdateAfterResponse` 继续作为唯一成功响应写回入口；把当前请求 fingerprint/消息坐标与 actual 原子更新并持久化。
- reset reason 应为受限枚举（如 none/no_actual/message_shrink/system_changed/tools_changed），不要保存正文或把 Frozen prefix length 当 API message coordinate。

### `internal/proxy/forward.go`

- SSE/JSON 继续共享 `totalInputTokens`；响应 usage fact 在 deflation 前写。
- 只在 `meta.tracksSawtoothState()` 为真时更新 baseline；辅助请求仍可写自身 `response_usage`。
- 成功 usage 写回需要携带入口时已保存的 request fingerprint/消息 count；失败路径只留下 pre-send decision。

### `internal/proxy/debug_facts.go`

- 新增 `debugStagePressureDecision`，在 `requestMeta` 中增加对应 `sync.Once`。
- 字段直接放在扁平 `debugFact`：messages/system/tools local tokens、previous actual、previous message count、new-message delta、selected pressure、threshold、source/reason/reset/compress、fingerprint changed bool；无正文、无完整敏感 ID、无嵌套 map。
- `response_usage` 保持精确三项 token 与 total；可增加 baseline-updated bool/误差整数，但不能输出 deflated usage 作为 actual。

### `internal/proxy/request_meta.go`

- 按现有 `RequestKind`、`sync.Once`、logger 的 request-scoped 模式增加 agent role/reason、pressure decision snapshot、pressure facts once。
- `tracksSawtoothState` 继续 nil/零值默认 true，仅高置信 title/subagent false，避免误伤未知主请求。

### `internal/proxy/auxiliary.go`

- 保持 table-driven 严格分类器；只补 2.1.207 已证明的 envelope 闭合后受控语言指令形态。
- 延续 `output_config` present-but-null/malformed fail-closed；不得放宽任意后缀或加入 2.1.199 版本分支。

### `internal/proxy/session.go`

- 在结构化 system block 遍历中识别原生 attribution block；沿用 billing regex 的严格 token boundary 思路。
- 扩展 `agentClassificationReason` 为受限枚举；日志只输出 feature bool/enum。

### Tests

- `proxy_pipeline_test.go`：复用 `TestHandleMessagesPreviousUsageAboveThresholdTriggersCollapse` 保留提交 `8b7db1f` 意图；扩展首次 full、actual+delta、system/tools 变更、消息缩短、低于/高于阈值、forwarded 与 trigger 分离。
- `proxy_pipeline_test.go` 的 title JSON/SSE fixture 与 `TestHandleMessagesSubagentNoSideEffects`：前后比较 actual、message count、request seq、Frozen、Archive、pause/trigger 状态，确保完全只读。
- `phase08_integration_test.go#TestPhase08AgentIsolationMatrix`（约 127 起）：沿用 billing subagent、agentContext subagent、unknown-is-main 三行矩阵，新增 2.1.207 system attribution 行。
- `forward_test.go`：SSE/JSON 成对断言 full actual、写回闸门、冷启动恢复、失败/无 usage 不写回。
- `debug_facts_test.go`：扩展顶层字段白名单与 secret sentinels，解析同 request_id 的 decision/usage，断言阶段各一次且并发文件不覆盖。
- `auxiliary_test.go#TestClassifyAuxiliaryRequest`（约 40–107）和 `session_test.go#TestAgentBillingMarker/#TestAgentContextStrongFeatures`：继续使用表驱动强路径/相似弱路径/畸形输入，明确 fail-closed。

## Shared Patterns

### 错误处理与降级

- 请求 JSON/messages 无法解析：记录受限错误并透明转发（`proxy.go` 405–429）。
- 响应 JSON/SSE usage 无法解析：不更新 baseline，响应尽可能原样转发（`forward.go` 635–650、759–780）。
- 持久化失败不阻断请求；内存状态仍生效，日志不含敏感正文。

### 状态锁与一致性

- map 状态全部由 `sync.RWMutex` 保护；复合 baseline 应整体 snapshot/整体 update，不让 actual、message coordinate、fingerprint 来自不同瞬间。
- DB lazy load 必须避免持锁调用外部 callback；沿用 `ShouldTrigger`/YesMem getters 的 unlock-load-relock 结构。

### JSON/SSE 双路径

- 完整 actual 一律在客户端 deflation 前提取。
- SSE 只在 `message_start` 首次 usage 记录，`usageRecorded` 防重复；JSON 只处理合法 2xx body。
- 两条路径共享 request meta、write-back predicate、total helper 和 response fact schema。

### 脱敏白名单

- 允许：整数、bool、受限 enum、计数、是否变化。
- 禁止：system/tools/messages/title 正文、session/parent ID、Authorization/API key/billing header 原文、base64、完整模型后缀。
- 文件路径可以按 session 分目录，但事实 JSON 本身不得包含 session ID；辅助 logger 仅绑定 request_id。

## No Analog Found

无。Phase 10 不应创建新模块；所有计划修改均有同文件模式或锁定 YesMem analog。若实现需要新增独立 `pressure.go`/`pressure_state.go`，planner 应先证明现有 `proxy.go` + `SawtoothTrigger` 无法承载，否则视为偏离本阶段“扩展现有生命周期”的约束。

## Metadata

**Analog search scope:** `internal/proxy/*.go`、对应测试、`C:/Users/romantcig/Desktop/yesmem/internal/proxy/` 锁定快照
**Primary analogs:** 5 组
**Pattern extraction date:** 2026-07-15
**External docs/network:** 未使用；无新依赖或兼容性参数问题

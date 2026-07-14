# Phase 10 Research: Token 压力与触发统一

> generic-agent workaround：本文件由通用代理按 `gsd-phase-researcher` 契约生成；没有使用 typed GSD researcher。所有实现事实均来自本地仓库、锁定 YesMem 快照或项目权威 Claude Code 2.1.207 源码；未安装新包，也没有不必要的联网查询。

## User Constraints

### Phase Boundary

本阶段统一可折叠主请求的 token 压力事实、触发入口、辅助请求隔离和可观测性：首次请求以覆盖 messages、顶层 system 与 tools 的本地估算判断压力；取得 API actual 后以真实基线和新增消息增量判断下一请求；标题与可靠子代理不得污染主会话压力状态；每个 request_id 必须能解释采用了哪种压力来源以及最终为何压缩或不压缩。

本阶段不负责重新设计 Collapse、Frozen、Archive、Recall 的完整执行顺序或数据契约，那些属于 Phase 11；也不负责实现精确复刻 Anthropic 的 tokenizer、多版本 Claude Code 兼容框架或新的辅助请求类型。

### Implementation Decisions

- **D-01:** 不以复刻 Anthropic 精确 tokenizer 为目标。token 尺子的成功标准是不会造成可感知的过早压缩，也不会产生持续漏触发；正常且不可感知的小误差可以接受。
- **D-02:** 没有历史 API actual 时，当前请求的本地完整估算必须覆盖 messages、顶层 system 和 tools，并在 Debug 中分别展示三者贡献。
- **D-03:** 有有效历史 API actual 时，采用 YesMem 式 `lastActual + new-message delta` 作为当前压力候选，而不是每轮重新依赖整段 messages 本地估算。历史 actual 已包含 system、tools 与 cache token 开销，不得重复叠加同一份 overhead。
- **D-04:** system 或 tools 通过安全指纹判断为发生变化时，当前轮停止沿用旧 actual，改用完整本地估算；当前轮响应返回的 API actual 成为后续请求的新基线。不得记录或持久化完整 system/tools 正文作为指纹证据。
- **D-05:** 历史消息数量缩短或坐标失效时（包括 Collapse、回退、会话结构重建等），旧 actual+delta 基线失效，当前轮重新完整估算并等待新 actual 建立基线。
- **D-06:** 不增加隐藏的提前压缩比例。配置中的 `TokenThreshold` 就是正常压缩线；Emergency 只处理明显超限的保险场景，不改变日常阈值含义。
- **D-07:** 压力验收按最终行为判断：明显低于阈值时不应过早压缩，明显高于阈值时不得漏开压缩入口；只在紧贴阈值的窄区域允许最多一轮左右的轻微提前或延后；API actual 返回后下一轮必须纠正，不能连续沿用已失真的判断。
- **D-08:** 上一主请求的 API `total_input_tokens` 已越过阈值时，下一条可折叠主请求必须进入压缩决策，不能被较低的 messages-only 本地估算挡在入口之外。进入决策不等于无条件 Collapse，后续仍按当前历史和阶段契约决定实际压缩动作。
- **D-09:** API actual 的完整输入口径延续 Phase 08 决定：`input_tokens + cache_creation_input_tokens + cache_read_input_tokens`。
- **D-10:** 已可靠识别的 session title 与 subagent 可以记录自身脱敏 Debug usage，但不得读取、推进、覆盖、重置或持久化主会话的 actual 基线、消息数量基线、请求序号、暂停计时、压力触发状态或 Frozen invalidation 状态。
- **D-11:** 辅助身份必须从请求入口贯穿到响应 usage 写回；不能只在进入主管线时旁路，响应回来后又按普通主请求调用 `UpdateAfterResponse`。
- **D-12:** Claude Code 2.1.207 是本阶段唯一权威分类基线。不得为 2.1.199 新增分支、版本矩阵或历史兼容承诺；现有旧安全形态若不降低严格度且无需额外复杂度，可以自然保留。
- **D-13:** 2.1.207 标题识别继续要求单条 user、完整且平衡的 `<session>...</session>`、严格标题 system 意图，以及 title-only JSON Schema 或源码已证明的 `output_config` 完全缺失回退。
- **D-14:** 标题 envelope 闭合后只允许 2.1.207 源码确认的受控标题语言指令形态；不得放宽为任意后缀。若 `output_config` 明确存在但为 null、畸形或不是 title-only schema，则不分类为标题请求。
- **D-15:** 子代理识别保留 HTTP `x-anthropic-billing-header` 中 `cc_is_subagent=true` 与 body `agentContext` 的兼容入口，并新增 2.1.207 原生 wire 形态：独立 system text attribution block 同时具有 `x-anthropic-billing-header:` 前缀和严格 token 边界上的 `cc_is_subagent=true`。
- **D-16:** 不得在完整 system 文本中泛搜 `subagent` 或 `cc_is_subagent`。只有已由源码或真实 wire 证明的强组合特征才能旁路；未匹配请求沿用现有主请求行为。
- **D-17:** 不实现假设性的标记冲突框架、第三种无状态模式、第三方上游入站降级或未来协议猜测。未来只有出现真实新版本漏识别证据时，才读取对应版本源码并增加新的严格形态。
- **D-18:** 每条主请求在终端最多输出一条易读压力摘要；详细计算进入脱敏结构化 Debug facts，避免长会话日志刷屏。
- **D-19:** 终端 token 数使用易读近似值；Debug facts 保存精确整数，供自动测试、误差比较和边界复核使用。
- **D-20:** 每个 `request_id` 至少关联两条核心事实：发送前的 `pressure_decision` 与响应后的 `response_usage`。上游失败时保留发送前决定，但不得伪造 actual 或建立新基线。
- **D-21:** `pressure_decision` 必须同时记录可用候选事实和最终选择，包括 messages/system/tools 本地贡献、previous API actual、new-message delta、selected pressure、pressure source、threshold、trigger reason、baseline reset reason 与 compress decision。
- **D-22:** 压缩后的低压力或 forwarded messages 估算不得冒充原始触发依据；Debug 必须明确区分“为什么进入压缩”和“压缩后最终转发多大”。
- **D-23:** Debug 与日志只允许数字、布尔值、受限枚举、计数以及“指纹是否变化”等脱敏事实；不得记录完整 system、tools schema、messages 正文、session title 正文、base64、凭证或敏感 ID。
- **D-24:** 标题和子代理可保留自己的 `response_usage` Debug 事实，但不得输出会与主压力判定混淆的主会话摘要，也不得更新 Sawtooth 状态。

### the agent's Discretion

- system/tools 指纹的具体哈希输入规范、存放位置和受限 reset reason 枚举，只要不保留敏感正文、能稳定判断语义结构变化并有测试覆盖。
- `pressure_source`、`trigger_reason`、`baseline_reset_reason` 等枚举的具体名称，以及 Debug JSON 的文件名和字段排序，只要语义固定、可机械解析并能通过同一 request_id 关联。
- 终端摘要的精确文字、颜色和 token 取整展示方式，只要每条主请求最多一条且不会混淆 actual、estimate、trigger 与 forwarded size。
- 现有工作树 pressure 候选补丁可以验证、重构或吸收到统一模型中，但必须保留老大已有改动与测试意图，不得覆盖、丢弃或未经证据直接提交为最终方案。

### Deferred Ideas

- 其他 Claude Code auxiliary 类型（compact、prompt suggestion、memory extraction、rename、teleport 等）继续延期；只有取得真实 wire 或对应版本源码证据后另行讨论。
- 通用未来协议兼容框架、标记冲突状态机和第三种无状态透明模式不进入本阶段。
- 精确 Anthropic tokenizer 或更接近上游的 tokenizer 继续作为 EST-01 延期项；只有混合模型无法满足安全边界时再评估。
- Collapse、Frozen、Archive、Recall 的完整状态机与字节契约由 Phase 11 处理。

## Project Constraints (from AGENTS.md)

- 使用 MSYS2/Git Bash 执行命令，`shell="bash"`、`login=false`；不要使用 `cmd` 或 PowerShell。
- 执行 CGO、`go test -race` 或 Windows 原生构建前，必须 `source /c/Users/romantcig/.bashrc`，并确认 `$CC -dumpmachine` 为 `x86_64-w64-mingw32`。
- 禁止让 Go 使用 `/usr/bin/gcc`（Cygwin 目标）；Phase 10 只沿用现有 Go 标准库和项目组件，不引入包。

## Executive Findings

1. 当前 Sawtooth 已在工作树候选提交中修复“上一响应 actual 超阈值但本轮 messages 估算较低时未进入入口”的路径：`HandleMessages` 先调用 `Sawtooth.ShouldTrigger(sessionID, 0)`，并将结果并入 `needCompress`；集成回归 `TestHandleMessagesPreviousUsageAboveThresholdTriggersCollapse` 覆盖该意图。[VERIFIED: `internal/proxy/proxy.go#HandleMessages` lines 545-608; `internal/proxy/proxy_pipeline_test.go#TestHandleMessagesPreviousUsageAboveThresholdTriggersCollapse`; commit `8b7db1f`]
2. 当前压力值仍主要是 `contextTokens + CountMessagesTokens(messages)`；未统一纳入顶层 `system`、`tools`，也没有 pressure decision facts。`debugFact` 只有 raw/forwarded/response_usage 三类阶段，响应 usage 已正确汇总三种 API 输入字段。[VERIFIED: `internal/proxy/proxy.go#HandleMessages`; `internal/proxy/debug_facts.go#debugFact`; `internal/proxy/forward.go#totalInputTokens`]
3. YesMem 的明确可复用模型是首次请求 `countMessageTokens(messages)+measureOverhead(system+tools)`，有稳定历史时 `lastActual+countMessageTokens(messages[lastMsgCount:])`，消息数量缩短则完整回退；历史 actual 不再重复叠加 overhead。[VERIFIED: `C:/Users/romantcig/Desktop/yesmem/internal/proxy/proxy_helpers.go#measureOverhead`, `#estimateTotalTokens`; `proxy_helpers_test.go#TestEstimateTotalTokens_*`]
4. `SawtoothTrigger` 当前持久化 actual 与 message count，但没有 system/tools 指纹、delta 计算或 baseline reset reason；`ShouldTrigger` 的 emergency、tokens、pause 原因仍是独立枚举。[VERIFIED: `internal/proxy/frozen.go#SawtoothTrigger`, `#ShouldTrigger`, `#UpdateAfterResponse`]
5. session title 已在入口旁路，`requestMeta.tracksSawtoothState` 也阻止 JSON/SSE 响应写回；可靠 subagent 在 Frozen/Archive/压缩前透明转发。因此 Phase 10 重点是确保相同角色贯穿到 pressure facts 和 usage 写回，不要扩大辅助类型。[VERIFIED: `internal/proxy/proxy.go#HandleMessages`; `request_meta.go#tracksSawtoothState`; `proxy_pipeline_test.go#TestHandleMessagesSessionTitleRequestState`, `#TestSessionTitleJSONResponseState`, `#TestSessionTitleSSEResponseState`, `#TestHandleMessagesSubagentNoSideEffects`]

## Standard Stack

| 用途 | 现有组件 | 研究结论 |
|---|---|---|
| 消息 token | `TokenCounter.CountMessagesTokens` / `countContentTokens` | 沿用单一语义计数入口；不要引入第二套 tokenizer。[VERIFIED: `internal/proxy/proxy.go`, `internal/proxy/token_counter.go`]
| API actual | `totalInputTokens` | 统一为 `input_tokens + cache_creation_input_tokens + cache_read_input_tokens`，先记录完整 usage 再做客户端 deflation。[VERIFIED: `internal/proxy/forward.go#totalInputTokens`, `#handleSSE`, `#handleJSON`]
| 状态 | `SawtoothTrigger` | 扩展现有 per-session maps/持久化，不建立平行 pressure store；需要增加 actual、message coordinate、fingerprint 与 reset 元数据。[VERIFIED: `internal/proxy/frozen.go#SawtoothTrigger`]
| 请求身份 | `requestMeta` | 从入口分类一路传到响应处理，`tracksSawtoothState()` 是唯一写回闸门。[VERIFIED: `internal/proxy/request_meta.go`]
| 事实输出 | `debugFact` + `writeDebugFact` | 扩展为受限枚举和整数；保留原子写入、request_id 文件命名和白名单测试。[VERIFIED: `internal/proxy/debug_facts.go`, `debug_facts_test.go`]
| 持久化 | 既有 SQLite `proxy_state` 回调 | 只持久化脱敏状态和指纹，不存 system/tools/messages 正文；无新依赖。[VERIFIED: `internal/proxy/frozen.go#persistedState`; `store.go`]

## Architecture Patterns to Use

### 1. Per-request pressure context, single decision point

在 `HandleMessages` 完成身份分类、persistent context detach、Frozen/Recall 输入确定后，构造一个仅包含计数和指纹的 pressure context：

- `message_tokens`：当前 raw/authoritative messages 的语义估算；
- `system_tokens`、`tools_tokens`：首次完整估算的顶层开销；
- `previous_actual`、`previous_message_count`；
- `new_message_delta`：仅当消息坐标严格延长且指纹未变化时计算；
- `selected_pressure`、`pressure_source`；
- `baseline_reset_reason`、`trigger_reason`、`compress_decision`。

先做一次选择，再让 Frozen invalidation、Collapse/stubify 读取同一结果。不要在 `HandleMessages` 的多个分支分别重新调用不同口径的 `ShouldTrigger`。[VERIFIED: 当前代码有两次 `needCompress` 和两处 `ShouldTrigger`，这是重复口径风险；`internal/proxy/proxy.go#HandleMessages`]

### 2. Two-path estimate with strict invalidation

推荐沿用 YesMem 的两条路径：

```text
if no_valid_actual || message_count_shrank || system/tools_fingerprint_changed:
    selected = messages + system + tools full estimate
    source = local_full
else:
    delta = tokens(messages[previous_message_count:])
    selected = previous_actual + delta
    source = actual_plus_delta
```

历史 actual 已含 system/tools/cache，actual+delta 路径绝不重复叠加 overhead。Collapse、回退或结构重建导致消息坐标缩短时必须 reset；本轮 API 响应成功后再建立新基线。[VERIFIED: YesMem `proxy_helpers.go#estimateTotalTokens`; tests `TestEstimateTotalTokens_FallbackOnMessageShrink`; Phase 10 D-03–D-05]

### 3. Fingerprint only canonical metadata

system/tools 指纹应由规范化 JSON/受限结构生成 SHA-256，并只保存 hash、present/changed bool 和枚举 reset reason。不要将原文写入 `debugFact`、slog、SQLite 或 fixture。系统/工具变更即使消息数量延长，也必须走 `local_full`，避免旧 actual 对新 overhead 产生危险低估。[VERIFIED: Phase 10 D-04/D-23; 现有 debug facts secret-safety 测试]

### 4. Role propagation is a write-back gate

入口分类结果写入 `requestMeta.RequestKind`/agent role；`forward.go` 的 SSE 和 JSON 两条 usage 路径统一使用 `meta.tracksSawtoothState()`。标题/subagent 可以写自己的 `response_usage` facts，但不能调用 `UpdateAfterResponse`、递增 request sequence、触发 Frozen 或 Archive。[VERIFIED: `internal/proxy/request_meta.go#tracksSawtoothState`; `forward.go#handleSSE/#handleJSON`; `proxy_pipeline_test.go` auxiliary tests]

### 5. Two-stage observability contract

发送前写一条 `pressure_decision`；响应成功后写一条 `response_usage`。两者共享 request_id，且 failure path 只留下 decision，不伪造 actual。`forwarded` facts 中的最终消息大小必须与 trigger facts 分开，不能将压缩后的估算当成触发依据。[VERIFIED: 现有 `debugStage` 三阶段和 `writeUsageDebugFacts`; Phase 10 D-18–D-22]

## Don't Hand-Roll

- 不要实现精确 Anthropic tokenizer；项目明确接受不可感知误差，EST-01 延期。[VERIFIED: Phase 10 D-01, REQUIREMENTS.md EST-01]
- 不要新增平行 `PressureState` 生命周期；复用 `SawtoothTrigger` 的 per-session actual/message baseline 和回调。[VERIFIED: `frozen.go#SawtoothTrigger`]
- 不要在 usage 写回处分别实现 SSE/JSON 两套 total 口径；复用 `totalInputTokens`。[VERIFIED: `forward.go`]
- 不要通过日志正文或完整 system/tools 做指纹；只用 hash/布尔/计数。[VERIFIED: Phase 10 D-04/D-23]
- 不要泛搜 `subagent` 或扩展 auxiliary 类型；只接受源码/真实 wire 已证明的强组合特征。[VERIFIED: Phase 10 D-12–D-17]

## Common Pitfalls and Required Verification

| 风险 | 现状/来源 | 计划中的验证 |
|---|---|---|
| messages-only 低估 system/tools | 当前 `totalTokens` 只合并 messages/context；gap `PRESSURE-002` | 构造 messages 低于阈值、system+tools 越阈值；断言三项 facts 与压缩入口。 |
| actual 与 overhead 重复相加 | YesMem 明确禁止；D-03 | 先记录 full estimate，再注入 actual，比较 actual+delta 与 full recount，断言不重复 overhead。 |
| 消息缩短仍使用旧 delta | YesMem 测试已有回退 | Collapse/回退/新会话 fixture 断言 `local_full` 与 reset reason。 |
| system/tools 改变仍沿用旧 actual | D-04 | 同消息坐标下变更 schema 指纹，断言完整估算并在响应后替换 baseline。 |
| previous actual 越线被本地估算挡住 | 已有候选修复 `8b7db1f` | 保留并扩展低估回归；断言进入决策、非无条件 Collapse。 |
| Emergency 改变正常阈值 | `ShouldTrigger` 使用 threshold+10k | 边界测试区分 `tokens`、`emergency`，不得引入隐藏提前比例。 |
| title/subagent usage 污染主状态 | 当前已在 response gate 防护 | JSON/SSE 各自断言 actual、msg count、request seq、pause/Frozen 不变。 |
| 失败响应伪造 actual | forward 当前仅成功 2xx/`message_start` 写回 | upstream 失败/无 usage/非 2xx 测试断言只保留 decision。 |
| debug 泄漏正文/凭证/base64 | 现有白名单和 secret-safety 测试 | 扩展 schema 白名单，扫描 system/tools/messages/title/session/header/base64 等 sentinel。 |
| forwarded size 冒充 trigger source | D-22 | 同 request_id 写入 raw pressure 与 forwarded facts，字段名/枚举显式区分。 |

## Code and Test Evidence Map

### Sawtooth

- `internal/proxy/proxy.go#HandleMessages`：分类、Frozen/Recall、当前 messages/context 估算、`needCompress`、Collapse/stubify 分支；工作树候选已把历史 `ShouldTrigger` 并入入口。[VERIFIED]
- `internal/proxy/frozen.go#SawtoothTrigger`：`lastTotalTokens`、`lastMessageCount`、`lastRequestTime`、持久化、冷启动、`ShouldTrigger`/`UpdateAfterResponse`。[VERIFIED]
- `internal/proxy/forward.go#totalInputTokens/#handleSSE/#handleJSON`：API actual 聚合、成功响应写回、deflation 前 facts。[VERIFIED]
- `internal/proxy/debug_facts.go#debugFact`：当前 raw/forwarded/response usage 脱敏 schema；需增加 pressure decision 阶段和局部字段。[VERIFIED]
- `internal/proxy/auxiliary.go#classifyAuxiliaryRequest`：严格 title 分类，缺失 output_config 才允许 prompt fallback。[VERIFIED]
- `internal/proxy/session.go#classifyAgentRequest`：billing marker、agentContext、system marker 的强特征分类；不得全文泛搜。[VERIFIED]
- `internal/proxy/request_meta.go#tracksSawtoothState`：只有 session title 关闭主状态写回，需让 agent role 也贯穿。[VERIFIED]

### Existing tests to extend

- `proxy_pipeline_test.go#TestHandleMessagesPreviousUsageAboveThresholdTriggersCollapse`：历史 actual 越阈值入口回归；保留其 dirty/已提交意图。[VERIFIED]
- `proxy_pipeline_test.go#TestHandleMessagesSessionTitleRequestState`, `#TestSessionTitleJSONResponseState`, `#TestSessionTitleSSEResponseState`：title 不推进 Sawtooth/Frozen/Archive，JSON/SSE 都覆盖。[VERIFIED]
- `phase08_integration_test.go`：主/子代理分类与 Frozen/Archive 副作用隔离、total input usage 证据。[VERIFIED]
- `forward_test.go#TestHandleSSECacheUsagePersistsTotalBeforeDeflation`, `#TestHandleJSONCacheUsagePersistsTotalAndColdStartTriggers`：三项 API actual 汇总与冷启动 trigger。[VERIFIED]
- `debug_facts_test.go#TestDebugFactsSchemaAndSecretSafety`, `#TestDebugFactsUsageUsesTotalInputTokens`：facts 白名单、精确 total 与敏感信息门禁。[VERIFIED]
- `auxiliary_test.go#TestClassifyAuxiliaryRequest`, `#TestAuxiliaryClassificationLog`：title 强/弱路径、畸形 schema fail-closed、日志脱敏。[VERIFIED]

### YesMem baseline

- `C:/Users/romantcig/Desktop/yesmem/internal/proxy/proxy_helpers.go#measureOverhead`：JSON marshal system/tools，floor 5000；`#estimateTotalTokens`：full fallback 或 `lastActual + delta`。[VERIFIED; SHA-256 `0f35e16a...`]
- `.../proxy_helpers_test.go#TestEstimateTotalTokens_WithPreviousActual`, `#TestEstimateTotalTokens_FallbackOnFirstRequest`, `#TestEstimateTotalTokens_FallbackOnMessageShrink`：三条关键模型回归。[VERIFIED]
- `.../sawtooth.go#UpdateAfterResponse`, `#GetLastTokens`, `#GetLastMessageCount`：actual/message baseline 生命周期。[VERIFIED; SHA-256 `1268e1c5...`]
- `.../usage.go#UsageTracker.TotalInputTokens`：完整 API 输入口径。[VERIFIED; SHA-256 `1c23e1e6...`]

## Requirement-to-Research Mapping

| 需求 | 研究结论与实现入口 | 必须验收 |
|---|---|---|
| PRESSURE-01 | `SawtoothTrigger` actual + message coordinate；入口 pressure decision 优先 actual 越线。[VERIFIED] | 上一 actual 越阈值、下一 messages 估算低：进入压缩决策；不要求无条件 Collapse。 |
| PRESSURE-02 | 首次/重基线完整估算必须拆 messages/system/tools；复用 TokenCounter 和受限 canonical JSON。[VERIFIED] | facts 三分量与 selected pressure 精确整数，system/tools 不写正文。 |
| PRESSURE-03 | YesMem actual+delta 作为候选；指纹变化/消息缩短回退 full；记录误差与 reset reason。[VERIFIED] | 连续 fixture 比较 full 与 delta，锁定误差阈值/拒绝条件，覆盖缩短。 |
| PRESSURE-04 | `requestMeta` role 贯穿入口到 SSE/JSON usage 写回；title/subagent facts 允许但状态只读。[VERIFIED] | 主 baseline、seq、pause、Frozen、Archive 在辅助请求前后完全不变。 |
| PRESSURE-05 | `pressure_decision` + `response_usage` 两阶段 facts，request_id join，forwarded 分离。[VERIFIED] | 自动解析同 request_id 的 source/trigger/reset/compress/actual，秘密扫描通过。 |

## Security and Privacy Boundary

- ASVS L1 适用重点是日志与诊断数据最小化：debugFact 只能存数字、布尔、受限枚举、计数和 hash-change 状态；不能有凭证、完整消息/system/tools/title、base64 或 session identity。[VERIFIED: `debug_facts.go`, `debug_facts_test.go`, Phase 10 D-23]
- system/tools 指纹应使用 SHA-256 canonical bytes；hash 不是正文，但需避免将未经规范化的 map 顺序造成伪变化。只保留指纹值或 changed bool，日志不输出完整 hash 若其可能成为敏感 ID。[VERIFIED: D-04/D-23；具体 hash 输入属 agent discretion]
- 辅助请求不得借由 `requestMeta` 的 logger 继承主 session 属性；沿用 `auxiliaryLogger` 只继承 request_id 的模式。[VERIFIED: `request_meta.go#auxiliaryLogger`, `proxy_pipeline_test.go`]
- 上游失败、解析失败或非 2xx 不建立新 actual；不能以客户端 deflation 后的值作为基线。[VERIFIED: `forward.go#handleJSON/#handleSSE`, `forward_test.go`]
- 研究没有新增依赖或网络源；沿用标准库 `crypto/sha256`、`encoding/json`、现有 SQLite/Go 组件即可。[VERIFIED: `go.mod`, inspected code]

## Planning Guidance

1. 先在 `SawtoothTrigger` 增加可验证的 baseline/fingerprint 读取与 reset API，再在 `HandleMessages` 生成单一 pressure decision；避免先改日志导致口径分裂。
2. 让 `forward.go` 在成功 usage 写回时同时记录 actual 与误差/新 baseline facts；失败路径只记录 response absence/error，不更新状态。
3. 扩展 `debugFact` 为新的 `pressure_decision` stage 或等价受限结构，保留现有 raw/forwarded/response_usage schema 的白名单和文件原子性。
4. 将 title 与 subagent 的 role 统一放入 requestMeta（而非依赖 request kind 单点），并在 JSON/SSE 及请求入口集成测试中证明状态隔离。
5. 测试按首次 full、稳定 actual+delta、system/tools 变更、消息缩短、previous actual 越线、辅助请求、失败响应和 debug secret scan 分组；不把 Collapse/Frozen/Archive 完整状态机带入本阶段。
6. `workflow.nyquist_validation=false`，因此本研究不输出 Validation Architecture；验证以阶段需求对应的自动化回归为主。

## Source Inventory and Confidence

| 来源 | 用途 | 置信度 |
|---|---|---|
| `.planning/phases/10-token/10-CONTEXT.md` | 锁定决定、边界、deferred | HIGH（项目权威输入） |
| `.planning/ROADMAP.md`, `REQUIREMENTS.md`, `STATE.md` | 目标、PRESSURE-01..05、Phase 09 handoff | HIGH（项目权威输入） |
| Sawtooth `internal/proxy/*.go` 与测试 | 当前实现/缺口/工作树候选 | HIGH（本地源码与测试） |
| YesMem `proxy_helpers.go`, `sawtooth.go`, `usage.go` 与测试 | actual+delta、overhead、usage 基线 | HIGH（锁定本地快照；关键文件 SHA 已记录） |
| `C:/Users/romantcig/.tweakcc/native-claudejs-orig-v207.js` 与 session-title 分析 | 2.1.207 分类边界 | HIGH（项目提供的权威源码；本阶段仅沿用 CONTEXT 已锁定结论） |

没有需要使用 Context7 或 web-research 的外部库/API 问题；本阶段研究不安装新包、不改变运行配置、不访问生产 SQLite 或认证上游。

## What Might Have Been Missed

- `SawtoothTrigger` 当前 pause 计时是进程内时间，SQLite 冷启动不恢复 `lastRequestTime`；Phase 10 只需保留既有 pause 语义并在 facts 中说明 source，不扩展跨重启 pause 设计（属于后续状态机/长会话范围）。[VERIFIED: `frozen.go#loadSawtoothFromDB`; Phase 10 boundary]
- Frozen prefix 的 raw cutoff 与 compressed prefix length 是双坐标；pressure baseline 只能以当前请求消息坐标和响应 message count 判断，不能把 Frozen prefix length 当 API message coordinate。[VERIFIED: `frozen.go#FrozenResult`, `proxy.go` comments; Phase 07.1 decisions]
- 现有 `debugFact.ModelFamily` 可能记录模型族枚举而非完整模型，扩展 pressure facts 时应继续保持这一脱敏级别。[VERIFIED: `debug_facts.go`]
- Phase 10 的 actual+delta 误差证据应比较本地 full estimate 与 API actual，但不能把客户端 deflation 后 usage 当 ground truth；真实响应 usage 先在代理侧记录。[VERIFIED: `forward.go`, `forward_test.go`]

## Research Status

- 研究范围：完成（pressure、actual+delta、system/tools overhead/fingerprint、message shrink reset、auxiliary isolation、debug/security）。
- 外部依赖：无；无需 Context7/web-research。
- Validation Architecture：按 `workflow.nyquist_validation=false` 省略。
- 产物：本文件；`commit_docs=true` 时由 orchestrator 负责提交或纳入后续规划提交。

---
status: investigating
trigger: "API Error: 422 Failed to deserialize the JSON body into the target type: messages[9]: missing field `content`; 同时首轮无显式召回关键词却出现 Archive 召回汇总"
created: 2026-07-12
updated: 2026-07-13
---

# Symptoms

- expected_behavior: 对话开始后，Claude Code 的 ToolSearch 结果应能继续发送给上游；Archive 只应在有明确恢复意图或真正相关线索时召回，后台标题请求不应注入大块历史。
- actual_behavior: request 8 在 ToolSearch 结果回传后收到 422，错误称内部 messages[9] 缺少 content；request 5 的后台标题生成请求在首轮即召回并注入 19798 tokens。
- errors: `Failed to deserialize the JSON body into the target type: messages[9]: missing field content at line 1 column 42923`。
- timeline: 2026-07-12；发生在新对话开始后的第一个 ToolSearch 循环。
- reproduction: 首条消息包含精确工作区路径；Claude Code 并发发起标题生成和主请求；随后调用 ToolSearch 并把 tool_reference 结果回传给 Grok/New API 上游。

# Current Focus

- hypothesis: 422 根因尚未确认；当前最强嫌疑是 sawtooth 将首条 user 的持久 context 与正文拆成两条相邻 user 消息，并与 ToolSearch 历史形成组合故障。标题请求误入主管线与精确路径触发 Archive 的根因已确认。
- test: 对完全相同的 request body 执行 raw、普通重序列化但不拆分、仅拆分、重新合并四组同端点 A/B；每次只改变消息边界。未经老大单独授权，不执行带认证的 live 重放。
- expecting: 若 unsplit 成功、split 失败且 re-merged 恢复，则确认消息边界拆分是必要触发因素；否则继续检查真实出站 wire 或上游 ToolSearch 转换。
- next_action: Phase 08.1 只纳入受门禁的调查任务；根因确认前禁止实施 ToolSearch 降级、删除 defer_loading、移除 beta header、422 自动重试或持久 context 行为修复。
- reasoning_checkpoint:
- tdd_checkpoint:

# Evidence

- timestamp: 2026-07-12
  checked: data/debug/副本 request 8 raw_inbound 与 forwarded
  found: raw 有 6 条、forwarded 有 7 条 Anthropic 消息；forwarded 的 7 条消息全部存在 content，代理没有发出外层缺 content 的消息。
  implication: 错误中的 messages[9] 不是代理转发数组的索引，而是上游转换后内部消息数组的索引。
- timestamp: 2026-07-12
  checked: request 7 成功与 request 8 失败的内容块差异
  found: request 8 首次新增 ToolSearch 的 tool_result，其 content 为 29 个 tool_reference；同时 tools 从 12 个增加到 41 个，新增 29 个定义均 defer_loading=true，引用名称全部有匹配定义。
  implication: tool_reference 是最小且直接的失败触发差异。
- timestamp: 2026-07-12
  checked: request 8 headers
  found: Anthropic-Beta 包含 advanced-tool-use-2025-11-20，Anthropic-Version 为 2023-06-01。
  implication: 客户端按高级工具协议显式声明了 beta 能力。
- timestamp: 2026-07-12
  checked: Anthropic 官方 Tool Search 文档与官方 cookbook
  found: 客户端自定义工具搜索应返回标准 tool_result，content 数组中直接包含 tool_reference；API 会依据顶层 defer_loading 工具定义展开引用。
  implication: request 8 的形状符合 Anthropic 官方协议；不兼容点在 Grok/New API 的 Anthropic 适配层，而非 sawtooth 的 JSON 序列化。
- timestamp: 2026-07-12
  checked: request 5/6 raw 与 forwarded、internal/proxy/reexpand.go
  found: 首条消息包含精确路径 C:\\Users\\romantcig\\Desktop\\discourse-main；extractRecallSignals 无条件把最新 user 文本中的精确路径作为 RecallSignalExactPath。request 5 是后台标题生成请求，复用了整段首条内容并把 54134 字符的完整 archive 追加到同一 user content，token_cost=19798。
  implication: 无需“召回”关键词也会触发；后台辅助请求没有被排除，导致首轮重复且昂贵的召回。
- timestamp: 2026-07-13
  checked: 老大提供的 data/debug/B 自然复现 request 9-11
  found: request 11 仅含 2 个 tool_reference、14 个顶层工具，其中 2 个 defer_loading=true，仍返回 422 messages[6] missing content；raw 4 条消息经 sawtooth 拆分为 forwarded 5 条，ToolSearch blocks 与 tools 未被改写。
  implication: 422 可重复且不是 29 个 tool_reference 数量过多导致；本次不是严格控制变量的 A/B，仍不能区分 tool_reference 本身与消息边界拆分的组合影响。
- timestamp: 2026-07-13
  checked: 两个独立只读调试 AI 对 request 7/8、user_context.go 与代理管线的因果复核
  found: 两者均判定现有证据不足以确认上游不支持 ToolSearch；唯一确认的语义改写是 user context 拆分，且应将 422 作为 investigation-only 规划项。
  implication: 既有 resolved 根因属于过度确认，必须恢复 investigating 状态并设置实施门禁。

# Eliminated

- hypothesis: sawtooth 直接转发了缺少 content 的 messages[9]。
  evidence: request 8 forwarded 只有 7 条消息，且每条都有 content。
  timestamp: 2026-07-12
- hypothesis: Archive 注入直接造成 request 8 的 422。
  evidence: request 8 没有 Retrieved archive payload；失败首次发生在 ToolSearch tool_reference 回传。Archive 注入出现在 request 5/6。
  timestamp: 2026-07-12
- hypothesis: request 8 的 tool_reference 缺少对应工具定义或 defer_loading 标记。
  evidence: 29 个引用全部有同名顶层工具定义，且 29 个新增定义全部 defer_loading=true。
  timestamp: 2026-07-12
- hypothesis: 422 由 tool_reference 数量达到 29 个触发。
  evidence: request 11 只有 2 个 tool_reference 仍稳定返回 422 messages[6] missing content。
  timestamp: 2026-07-13
- hypothesis: 已经确认 Grok/New API 本身不支持合法 ToolSearch。
  evidence: 老大报告同一 Grok 正常直连可工作，且尚无完全相同 raw body 与 forwarded body 的严格同端点 A/B；两个独立复核均判定证据不足。
  timestamp: 2026-07-13

# Resolution

- root_cause: 部分确认。Archive/标题问题：自动会话标题辅助请求没有专用分类，被当作 main 进入有状态主管线；其复制的日常项目路径又被 RecallSignalExactPath 当作无需恢复意图的强信号，导致无谓 Archive 搜索/注入。422：根因未确认；确认 sawtooth 会把原始一条 user 的持久 context 与正文拆为两条相邻 user，且合法 ToolSearch blocks/tools 未被改写，但尚未证明该拆分与 ToolSearch 的组合是必要或充分条件。
- fix: not applied。已锁定的规划方向是自动标题请求完全安全直通、精确路径不再独立触发召回；422 仅调查，不预设或实施修复。
- verification: 标题与路径根因由 request 5/6 抓包和生产代码确认；422 已由 request 8 与 data/debug/B request 11 两次自然复现，排除 Archive、缺外层 content、tools/reference 丢失及引用数量过多，但缺少严格 raw/forwarded/re-merged 同输入 A/B。
- files_changed: .planning/debug/tool-reference-422-recall.md

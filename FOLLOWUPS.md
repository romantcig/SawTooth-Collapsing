# 压力判定修复的遗留项（2026-09-07）

本次"反复折叠死循环"修复（baseline 坐标回归入口 raw 口径）已提交。以下三项是审查与实测中发现、但**不阻塞**的遗留工作，按优先级排序。每项都附了根因和建议修法，直接丢给 AI 照着做即可。

---

## 1. frozen boundary 校验对 CC 的尾消息重组不鲁棒（建议下一个修）——✅ 已于同日修复

**修复记录**：实际采用的是比本条建议更小的改动——`normalizeBoundaryContent`（frozen.go）补上"单 text 块数组折叠回纯文本"规则（与 `normalizeHistoryContent` 同源），`stableBoundaryHash` 体系不变。机制已在 CC 2.1.258 源码（chunk-dg4szctq.js 的 `q5o`/`T5o`/`gVn`）钉死：wire 上"块数组+cache_control"是当轮锚点包壳，退位后按内部原样直出纯文本，内容逐字节不变。回归测试 `TestStableBoundaryHashSurvivesCCShellRewrite`；真实录制（data/debug/918fccfb09739f29 第 6→7 请求，含持久 context 先摘除）回放 Store→Get 已命中。全量 `go test -race` 通过。


**现象**：实测日志出现 `frozen prefix 未命中 reason=boundary_changed cutoff=248 raw_prefix_mode=full`，frozen 缓存每轮必然失配一次，导致每轮重新折叠（判定本身正确，只是白折一遍、少一次缓存命中）。

**根因**（已用 debug 录制定位）：CC 每轮会把"上一轮的最后一条用户消息"重新序列化——第一轮 wire 里该消息的 content 是带 `cache_control: ephemeral` 的块数组，第二轮它成为历史后变成纯文本字符串（cache_control 被剥掉）。frozen 存储的 boundary 哈希基于第一轮形态，第二轮验证时对不上。

**为什么现在无害**：压力判定主路径（actual_plus_delta）已不依赖 frozen；失配的代价只是重新折叠一次。

**建议修法**：`stableBoundaryHash`（internal/proxy/frozen.go）改为对 `canonicalHistoryMessage` 规范化后的内容取哈希（与 `reuseSafetyPrefixHash` 同口径），剥离 cache_control 与形态差异；或者验证 3/4 的哈希输入先过 `StripReminders` + canonical 化。修完后用两轮真实录制回放验证 frozen 命中。

**涉及位置**：`internal/proxy/frozen.go` 的 `GetWithLoadResult` 验证 3/4、`stableBoundaryHash`、`reuseSafetyPrefixHash`（internal/proxy/history_epoch.go）。

---

## 2. dispatcher 测试竞态（全量跑偶发假失败）——✅ 已于同日修复

**修复记录**：按本条建议把等待条件改为 `countTerminalText(terminal.lines(), "详细记录已恢复保存") >= 1`（outcome_dispatcher_test.go），并加注释说明两个同步点的窗口。全量 `go test -race` 通过。

**现象**：`TestSessionQueueFullEnteredOnceAndAcceptedDoesNotRecover`（internal/proxy/outcome_dispatcher_test.go:497）在全量 race 测试中偶发失败（"queue recovered 提示=0"），单跑 -count=5 稳定通过。

**根因**：`HealthTracker.ObserveGapCommit` 在锁内翻转 degraded 状态、解锁后才写终端日志行；测试的 `waitForDispatcher` 只轮询状态恢复，不等日志行落盘——状态与日志两个同步点之间有窗口（TOCTOU）。与压力判定修复无因果（该测试不走 frozen 路径）。

**建议修法**：等待条件改为直接轮询 `countTerminalText(terminal.lines(), "详细记录已恢复保存") >= 1`（或状态与行数同时满足），消掉窗口。

---

## 3. ForwardedCoordinatesChanged 诊断字段已无判别力（低优先级）——✅ 已于同日删除

**修复记录**：选择删除路线。字段定义（proxy.go）、`markForwardedPressureCoordinates` 的全部写入与坐标对比（forward.go）、forward_test.go 四处断言一并移除；`TestForwardedCoordinatesUsePositionSensitiveHistoryFingerprint` 改写为直接断言 `fingerprintMessagesPrefix` 对 `tool_use.input.cache_control` 业务变化敏感（指纹函数仍在 baseline 契约中使用，守护保留）。全量 `go test -race` 通过。

**现象**：带持久 context 的请求，`PrependPersistentUserContext`（internal/proxy/user_context.go）会把 context 消息前插回 wire，导致 wire 恒比入口 raw 多一条消息，`ForwardedCoordinatesChanged`（internal/proxy/forward.go）恒为 true。

**影响**：该字段无生产消费方（纯诊断 + 测试），新设计里本就降级为诊断位，不影响任何行为。

**建议修法**：改为与 `finalizeMessages(history)` 的预期形状比较，或干脆删除该字段；动手前先 grep 消费方确认。

---

## 附：本次修复的验证基线（回归时对照用）

- 新增回归测试：`TestPressureBaselineSurvivesCollapseRewrite`（collapse_calibration_test.go）——第一轮折叠 → baseline 记 raw 坐标 → 第二轮回放全量 raw + 追加，断言判定源为 actual_plus_delta、不再折叠。
- 真实环境验证：190K 旧对话接入后，第二轮判定源变为"上次实际输入+新增"（37311/150000），不再出现连续"ST 已触发"。
- 全量 `go test -race ./internal/proxy` 通过。

## 附：测试渠道的已知坑（与代码无关）

- 低价测试渠道（ai.venlacy.com）模型掺水：标称 1M 实际 200K（模型自述为 Kiro/AWS 系）。生产阈值按 1M 配置时在该渠道上是 75% 占用，429 可能含窗口压力成分。测试时阈值应按 200K 校准。
- 该渠道 usage 的缓存字段异常（几乎零 cache_creation、全 cache_read），官方缓存语义下不可能——usage 绝对值在掺水渠道上不可信，actual 锚点会跟着漂（单轮自愈，但别拿它做精度结论）。
- CC 的上下文百分比来自 GSD 本地估算，对工具密集内容高估 4 倍级（43K 真实显示 19%），不能当真实值用。

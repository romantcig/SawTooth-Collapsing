# Roadmap: Sawtooth Proxy

## Overview

v1.1 只完善锯齿折叠闭环，不追求复制 YesMem。路线先以只读审计证明哪些差距真实影响 token pressure、Trigger、Collapse、Archive、Frozen、Recall 或 Claude Code/Anthropic 协议，再依次统一压力决策、闭合折叠状态机、建立可重复长会话证据，最后用 Windows 原生发布门禁收口。任何 YesMem 候选能力若不能证明缺失会破坏折叠、恢复、协议或可验证性，均不进入实现阶段。

## Milestones

- ✅ **v1.0 锯齿折叠核心与 Claude Code 兼容基线** - Phases 1–08.1（shipped 2026-07-14；完整历史见 [v1.0 路线图](milestones/v1.0-ROADMAP.md)）
- 🚧 **v1.1 锯齿折叠对齐与可靠性闭环** - Phases 09–13（in progress）

## Phases

- [x] **Phase 09: YesMem 锯齿闭环差距审计** - 只读建立证据化差距矩阵与移植准入结论，不修改生产逻辑
- [ ] **Phase 10: Token 压力与触发统一** - 让本地估算、API actual、辅助请求隔离和最终压缩判定形成统一可审计入口
- [ ] **Phase 11: Collapse/Frozen/Archive 闭环对齐** - 证明并加固从 Trigger 到 Recall 的顺序、字节契约与协议安全边界
- [ ] **Phase 12: 真实长会话锯齿验证** - 用可重复 fixture 与 Claude Code debug 证据展示完整锯齿曲线和重启恢复
- [ ] **Phase 13: Windows 发布门禁** - 以自动化测试、race、原生构建、SQLite 故障恢复和敏感日志检查完成发布收口

## Phase Details

### Phase 09: YesMem 锯齿闭环差距审计

**Goal**: 维护者能基于源码与测试证据准确判断 Sawtooth 的锯齿核心还缺什么、为什么值得补，以及哪些 YesMem 外围能力永久不移植。
**Depends on**: Phase 08.1
**Requirements**: ALIGN-01, ALIGN-02, ALIGN-03, ALIGN-04
**Success Criteria** (what must be TRUE):

1. 维护者可以逐项查看 YesMem 与 Sawtooth 在 token pressure、Trigger、CompressContext、Collapse、Archive、Frozen、Recall 和协议边界上的源码、行为与测试证据。
2. 差距矩阵中的每个条目都具有且仅具有“已对齐、必要缺口、Sawtooth 特有适配、明确不移植”之一的结论，不存在待猜测条目。
3. 每个必要缺口都能指出若不处理会破坏的折叠/恢复/协议/可验证性结果，并给出自动化测试或 debug 证据验收方式。
4. daemon、learnings、session flavors、pulse、reflection、narrative、briefing、身份注入、prompt ungate、系统提示重写、时间戳、Persona、画像、任务统计、MCP、多 Agent 编排、文档索引与跨项目知识融合均有明确排除记录；无新证据时不会进入后续阶段。
5. 本阶段结束时生产代码与运行配置保持不变，后续实现范围完全由审计确认的必要缺口驱动。

**Plans**: 3/3 plans complete
**Wave 1**

- [x] 09-01-PLAN.md — `09-GAP-MATRIX.csv`

**Wave 2**

- [x] 09-02-PLAN.md — `09-GAP-AUDIT.md`

**Wave 3** *(verification gap closure)*

- [x] 09-03-PLAN.md — 修复 13 行失效证据锚点，并建立覆盖 61 行四证据列的确定性校验

### Phase 10: Token 压力与触发统一

**Goal**: 每条可折叠主请求都能基于一致、可解释且不受辅助请求污染的 token 压力事实作出压缩判定。
**Depends on**: Phase 09
**Requirements**: PRESSURE-01, PRESSURE-02, PRESSURE-03, PRESSURE-04, PRESSURE-05
**Success Criteria** (what must be TRUE):

1. 上一主请求 API `total_input_tokens` 超过阈值后，即使下一主请求的 messages 本地估算较低，自动化测试仍能观察到它进入压缩决策。
2. 没有历史 API usage 时，debug facts 能分别展示 messages、system、tools 的估算贡献和最终请求压力值。
3. 有历史 API usage 时，仓库存在基于误差证据的明确决定与回归测试，证明采用或拒绝 `lastActual + new-message delta` 不会产生触发死路径或危险低估。
4. session title、可靠子代理及其他明确辅助请求的测试证明它们不会读取、推进或覆盖主会话压力状态。
5. 单个 request_id 的 debug/log 记录可区分本地估算、API actual、Trigger 来源与最终压缩判定，维护者无需凭界面百分比猜测触发原因。

**Plans**: TBD

### Phase 11: Collapse/Frozen/Archive 闭环对齐

**Goal**: Trigger → CompressContext → Collapse → Archive → Frozen → Recall 成为单一、可恢复、协议安全且可由测试证明的锯齿状态机。
**Depends on**: Phase 10
**Requirements**: CYCLE-01, CYCLE-02, CYCLE-03, CYCLE-04, CYCLE-05
**Success Criteria** (what must be TRUE):

1. Token、Emergency、Pause 与 Frozen invalidation 的状态转移均有可执行测试覆盖，已记录的有效触发信号不存在无法打开压缩入口的死路径。
2. 管线证据能按顺序还原 CompressContext、cutoff、Collapse、Archive 保存、Frozen Store 与 Recall，且每一步的输入输出边界明确。
3. Collapse 后实际转发 prefix、持久化 Frozen snapshot 与下一请求恢复 prefix 的字节契约测试一致，cache breakpoint 不发生漂移或重复。
4. 活动 tool-use 轮次、tool_use/tool_result 配对、本轮最新持久 user context 与带签名 thinking 在折叠和恢复路径中保持协议有效。
5. 重复或并发 Archive 保存不增加重复记录；Recall 始终带 provenance、单请求最多一次、候选去重且不突破硬预算。

**Plans**: TBD

### Phase 12: 真实长会话锯齿验证

**Goal**: 维护者可以用可重复证据观察完整锯齿曲线，并证明代理重启后折叠状态不会丢失或跨 session 错配。
**Depends on**: Phase 11
**Requirements**: E2E-01, E2E-02, E2E-03
**Success Criteria** (what must be TRUE):

1. 自动化 fixture 可重复产生“上下文增长 → 越阈值 → 折叠下降 → Frozen 复用 → 再次增长 → 再次折叠”的完整序列，并对每个转折点有断言。
2. 一份脱敏的真实 Claude Code debug 录制可按 request_id 关联 raw inbound、forwarded、response usage、Trigger、Collapse、Frozen 与 Recall 事件。
3. 代理在超阈值、已折叠和已归档状态下重启后，SQLite 恢复测试证明 pressure、Frozen 与 Archive 继续属于原 session，且不同 session 之间无状态串扰。
4. 锯齿是否成立由 token/消息/状态事件证据自动判断，不设置肉眼无法可靠判定的重复人工通过门禁。

**Plans**: TBD

### Phase 13: Windows 发布门禁

**Goal**: v1.1 锯齿闭环只有在 Windows 原生环境的自动化可靠性与敏感数据安全证据全部通过后才可发布。
**Depends on**: Phase 12
**Requirements**: E2E-04
**Success Criteria** (what must be TRUE):

1. 全量 Go 测试与锯齿关键路径定向回归在干净运行中通过，失败时能定位到具体 request/session 状态转移。
2. 经确认的 MinGW-w64 工具链可通过关键包 race 测试并成功产出 Windows 原生可执行文件，不误用 Cygwin GCC。
3. SQLite 打开、并发、故障恢复与进程重启场景均通过自动化验证，临时目录清理波动不会被误报成功或掩盖真实故障。
4. debug 与常规日志检查证明不包含认证凭证、完整 system/messages 正文、base64 payload 或可反推 session 身份的敏感数据。
5. 发布证据能逐项回链 v1.1 的 18 条 requirements；任何未满足项都会阻止里程碑完成，而不是转成人工目测放行。

**Plans**: TBD

## Progress

**Execution Order:** Phase 09 → Phase 10 → Phase 11 → Phase 12 → Phase 13

| Phase | Milestone | Plans Complete | Status | Completed |
|---|---|---:|---|---|
| 09. YesMem 锯齿闭环差距审计 | v1.1 | 3/3 | Complete    | 2026-07-14 |
| 10. Token 压力与触发统一 | v1.1 | 0/TBD | Not started | - |
| 11. Collapse/Frozen/Archive 闭环对齐 | v1.1 | 0/TBD | Not started | - |
| 12. 真实长会话锯齿验证 | v1.1 | 0/TBD | Not started | - |
| 13. Windows 发布门禁 | v1.1 | 0/TBD | Not started | - |

## Requirement Coverage

| Requirement | Phase |
|---|---|
| ALIGN-01 | Phase 09 |
| ALIGN-02 | Phase 09 |
| ALIGN-03 | Phase 09 |
| ALIGN-04 | Phase 09 |
| PRESSURE-01 | Phase 10 |
| PRESSURE-02 | Phase 10 |
| PRESSURE-03 | Phase 10 |
| PRESSURE-04 | Phase 10 |
| PRESSURE-05 | Phase 10 |
| CYCLE-01 | Phase 11 |
| CYCLE-02 | Phase 11 |
| CYCLE-03 | Phase 11 |
| CYCLE-04 | Phase 11 |
| CYCLE-05 | Phase 11 |
| E2E-01 | Phase 12 |
| E2E-02 | Phase 12 |
| E2E-03 | Phase 12 |
| E2E-04 | Phase 13 |

**Coverage:** 18/18 v1.1 requirements mapped exactly once; 0 unmapped; 0 duplicated.

---
*Roadmap updated: 2026-07-14 for v1.1 锯齿折叠对齐与可靠性闭环*

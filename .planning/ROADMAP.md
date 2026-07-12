# Roadmap: Sawtooth Proxy

**Created:** 2026-06-27
**Granularity:** Coarse (5 phases)
**Execution:** Sequential

---

## Milestone Completion Registry

以下阶段均已完成。Phase 1–6 的实现早于标准 GSD phase 目录落地，现补齐
完成标记和兼容目录，使健康检查与进度分析器能够和 `STATE.md` 的
“Milestone complete”状态保持一致。

- [x] **Phase 1: Bare Proxy (透传骨架)** — completed 2026-06-27
- [x] **Phase 2: Stubify & Decay (消息桩化)** — completed 2026-06-27
- [x] **Phase 3: Collapse & Archive (折叠存档)** — completed 2026-06-27
- [x] **Phase 4: Frozen Prefix & Cache (缓存优化)** — completed 2026-06-27
- [x] **Phase 5: Production Polish (打磨)** — completed before Phase 6
- [x] **Phase 6: Compression Pipeline Gaps** — completed 2026-06-28
- [x] **Phase 7: Collapse-First 管线重排** — completed 2026-06-29
- [x] **Phase 07.1: Frozen、Archive 召回与多 Agent 隔离修复** — completed 2026-07-09
- [x] **Phase 8: Claude Code 协议兼容性加固** — completed 2026-07-11

---

### Phase 1: Bare Proxy (透传骨架)

**Goal:** CC → proxy → API 链路跑通，能记录请求但不做任何修改

**Success Criteria:**

1. `sawtooth-proxy.exe` 启动后 CC 通过 `ANTHROPIC_BASE_URL=http://localhost:9099` 正常对话
2. Debug 模式下请求体和响应体完整落盘到 `data_dir/debug/`
3. 日志输出 threadID、消息数、模型名
4. SSE 流式和 JSON 非流式响应都正确处理

**Files to create:**

- `cmd/proxy/main.go` — 入口、flag 解析、config 加载
- `internal/proxy/proxy.go` — Server struct、handleMessages 骨架
- `internal/proxy/forward.go` — forwardRaw (SSE + JSON)
- `internal/proxy/session.go` — extractSessionID、isSubagent、extractModelFromBody
- `sawtooth.yaml` — 示例配置文件

**Requirements:** PROXY-01 → PROXY-06

**Plans:** 2/2 plans complete
Plans:
**Wave 1**

- [x] 01-01-PLAN.md — 项目骨架：go.mod、sawtooth.yaml、session.go、proxy.go（Config 类型体系、SessionID 提取）

**Wave 2** *(blocked on Wave 1 completion)*

- [x] 01-02-PLAN.md — 核心透传：forward.go（SSE/JSON 转发 + deflation）+ main.go（入口启动、路由注册）

---

### Phase 2: Stubify & Decay (消息桩化)

**Goal:** 检测 token 数量，超阈值时对旧消息做 stubify + 渐进衰减

**Success Criteria:**

1. Thinking 块被完全移除
2. Tool results 变成 `[tool result archived]`
3. Tool uses 变成 `[→] ToolName args — annotation`
4. 短用户消息和决策消息受到保护
5. Debug 日志显示 stub 前后的 token 数量变化

**Requirements:** STUB-01 → STUB-07, DECAY-01 → DECAY-02

**Plans:** 2/2 plans complete
Plans:
**Wave 1**

- [x] 02-01-PLAN.md — Stubify 引擎：tiktoken-go TokenCounter 封装、消息桩化（thinking 移除、tool stub、截断、decision/pivot 保护）、StubifyConfig 配置组

**Wave 2** *(blocked on Wave 1 completion)*

- [x] 02-02-PLAN.md — Decay 引擎与管线集成：4 阶段渐进衰减（fresh→middle→old→compacted）、决策感知减速、HandleMessages 管线接入、main.go 启动装配

---

### Phase 3: Collapse & Archive (折叠存档)

**Goal:** Token 超阈值时折叠旧消息为 archive block，存档到 SQLite

**Success Criteria:**

1. CalcCollapseCutoff 正确计算 cutoff 点，不折断 tool_use/tool_result 对
2. Archive block 包含 commits、timeline、tool 摘要、文件列表
3. 折叠后消息数显著减少，第一条消息内容被 blanking
4. 原始消息完整存入 SQLite archive_blocks 表
5. 可通过消息范围检索存档

**Files to create/copy:**

- `internal/proxy/collapse.go` — 从 YesMem 复制，去 daemon 调用
- `internal/proxy/store.go` — SQLite schema + CRUD
- `internal/proxy/reexpand.go` — 关键词搜索 + 恢复注入

**Requirements:** COLLAPSE-01 → COLLAPSE-05, ARCHIVE-01 → ARCHIVE-02

**Plans:** 3/3 plans complete
Plans:
**Wave 1**

- [x] 03-01-PLAN.md — SQLite 持久化层（SQLiteStore + 4 张表 schema + CRUD）+ collapse 配置体系（go.mod/sawtooth.yaml/proxy.go Config）
- [x] 03-02-PLAN.md — 折叠引擎（CalcCollapseCutoff/CollapseOldMessages/buildArchiveBlock/blankFirstMessage）+ 重展开引擎（ExtractKeywords/SearchAndExpand）

**Wave 2** *(blocked on Wave 1 completion)*

- [x] 03-03-PLAN.md — 管线集成：DecayManager SQLite 迁移（D-14）+ HandleMessages collapse 步骤（D-01/D-02）+ main.go 依赖注入与 Cleanup 移除

---

### Phase 4: Frozen Prefix & Cache (缓存优化)

**Goal:** 折叠后的前缀冻结复用，管理 cache_control breakpoint 以最大化 prompt cache 命中

**Success Criteria:**

1. FrozenStubs.Store 后 Get 返回相同 frozen prefix
2. SHA-256 校验，prefix 被意外修改时自动 invalidate
3. SawtoothTrigger 正确判断三种触发条件
4. 冷启动从 SQLite 恢复 frozen stubs 和 trigger 状态
5. Frozen portion 的 embedded cache_control 被 strip
6. 只有一个 cache_control breakpoint 在 frozen boundary
7. 所有 breakpoint TTL 统一

**Files to create/copy:**

- `internal/proxy/frozen.go` — 从 YesMem sawtooth.go 复制 FrozenStubs + SawtoothTrigger
- `internal/proxy/cache.go` — StripMessagesCacheControl、InjectFrozenStubCacheBreakpoint、NormalizeCacheTTL、EnforceCacheBreakpointLimit

**Requirements:** FROZEN-01 → FROZEN-05, CACHE-01 → CACHE-04, ARCHIVE-03 → ARCHIVE-04

**Plans:** 2/2 plans complete
Plans:

- [x] 04-01-PLAN.md — FrozenStubs + SawtoothTrigger（frozen.go 移植 + store.go frozen_state 表 + PersistState/LoadState）
- [x] 04-02-PLAN.md — cache_control 处理 + 管线集成（cache.go + proxy.go 管线重构 + main.go 初始化 + sawtooth.yaml）

**Wave 1** *(both plans run in parallel — 04-02 depends on 04-01 for type contracts)*

---

### Phase 5: Production Polish (打磨)

**Goal:** Subagent 透传、eager stubbing、orphan repair、Windows 验证

**Success Criteria:**

1. Haiku 模型和无 thinking 请求被识别为 subagent 并透传
2. 每次请求主动清理旧 tool results
3. Orphan tool_use/tool_result 对在序列化前被修复
4. `GOOS=windows GOARCH=amd64 go build` 成功
5. Windows 上 CC 完整对话，折叠触发正常，无崩溃

**Files to create/copy:**

- `internal/proxy/eager.go` — 从 YesMem 复制 EagerStubToolResults + EagerStubMemory + extractToolResultText
- `internal/proxy/validate_pairs.go` — validateToolPairs + fixAlternation (orphan repair 安全网)

**Files to modify:**

- `internal/proxy/session.go` — 重写 isSubagent 占位函数
- `internal/proxy/proxy.go` — 插入 subagent 检测 + EagerStub 调用 + orphan repair 调用
- `cmd/proxy/main.go` — 初始化 EagerStubMemory

**Requirements:** EAGER-01 → EAGER-02, SUBAGENT-01 → SUBAGENT-02

---

## Summary

| Phase | Name | Requirements | Key Deliverable |
|-------|------|-------------|-----------------|
| 1 | 2/2 | Complete   | 2026-06-27 |
| 2 | 2/2 | Complete    | 2026-06-27 |
| 3 | 3/3 | Complete    | 2026-06-27 |
| 4 | 2/2 | Complete   | 2026-06-27 |
| 5 | Polish | EAGER-01..02, SUBAGENT-01..02 | Subagent 透传 + eager stub |
| 6 | Pipeline Gaps | REMIND-01..05 | StripReminders |
| 7 | Collapse-First | — | 管线重排 collapse-first |

### Phase 6: Compression Pipeline Gaps

**Goal:** 补齐折叠管线相对 YesMem 参考实现缺失的预处理步骤。本 phase 涵盖三项（StripReminders / CompressContext / Intensity），按优先级分轮实现；本轮只做 P1 StripReminders。

**本轮范围（Wave 1 — StripReminders）:** 移除旧消息中过期的 system-reminder / skill-hint 标签，无损 token 回收（当前全项目零覆盖）。

**Success Criteria（StripReminders）:**

1. 旧消息（最后一条 user 消息之前）中的 `<system-reminder>...</system-reminder>` 块被移除或降级
2. `skill-hint` 块被移除
3. 最后一条 user 消息完整保留；SessionStart 类 reminder 保留
4. 移除步骤在管线最早阶段执行（reexpand 之前）
5. 单元测试覆盖移除逻辑，且不破坏 tool_use/tool_result 配对

**后续轮次（暂缓）:** P2 CompressContext、P3 Intensity

**Depends on:** Phase 5

**Requirements:** REMIND-01 → REMIND-05

**参考:** `data/GAP-ANALYSIS.md`；`yesmem/internal/proxy/reminders.go`、`skill_hints.go`

**Plans:** 1/1 plans complete

Plans:
**Wave 1**

- [x] 06-01-PLAN.md — StripReminders：新建 reminders.go（移除旧 system-reminder / skill-hint，保留末条 user 与 SessionStart）+ reminders_test.go 表驱动单测 + proxy.go::HandleMessages 管线最早阶段集成（REMIND-01 → REMIND-05）✅ 2026-06-28

---

### Phase 7: Collapse-First 管线重排

**Goal:** 将压缩管线从 stubify-first 重排为 collapse-first——对标 YesMem 架构。Collapse 从"死代码"变为主路径（266 条 → ~12 条消息），stubify+decay 降级为 fallback。

**Success Criteria:**

1. CompressContext 后立即 CalcCollapseCutoff，cutoff > 0 走 collapse 主路径
2. collapse 后消息数从 250+ 降到 ~12 条（blank + archive + tail）
3. countMessageTokens 计入所有 block 类型（tool_use/tool_result/thinking/text）
4. DecayTracker 使用 session-scoped key 隔离多 session 并发
5. 全部现有测试通过，构建成功

**Plans:** 1/1 plans complete

Plans:
**Wave 1**

- [x] PLAN.md — 管线重排（proxy.go collapse-first）+ countMessageTokens 修复（collapse.go）+ CollapseOldMessages 返回 ArchiveBlock + DecayTracker session-scoped key + 级联签名更新 + sawtooth.yaml 废弃 ✅ 2026-06-29

---
*Roadmap created: 2026-06-27*
*Last updated: 2026-06-29 after Phase 7 execution*

### Phase 07.1: Frozen、Archive 召回与多 Agent 隔离修复 (INSERTED)

**Goal:** 修复 Frozen 生命周期、Archive 持久化与召回、多 Agent 隔离和并发日志审计缺陷，使折叠前缀可稳定复用、每个请求最多执行一次受预算约束的召回、重复存档不再增长，子代理可靠绕过有副作用的主管线。

**Success Criteria:**

1. 300 条原始消息 collapse 后，下一请求追加 fresh tail 时 `Frozen.Get` 命中，Freeze/Restore frozen prefix 字节一致且边界只有一个 cache breakpoint
2. 每个 HTTP 请求最多执行一次 Archive 搜索；请求累计召回成本不超过 `Budget.ReExpansion`，nil Budget 仍不超过 `tokenThreshold/10`
3. 相同 Archive 的顺序或并发重复保存不增加行；旧数据库可兼容打开，历史重复数据不被自动删除
4. 召回只由显式 hint、精确路径或明确恢复意图触发，候选最多 3 个，同 session 高度重叠快照一次只选择一个
5. DeepSeek 主/子代理矩阵通过；可靠子代理和保守 unknown 请求均不执行 Search、Collapse、SaveArchive 或 Decay 副作用
6. 并发日志可按 `request_id` 还原请求链，当前请求 session 与 Archive 来源 session 使用不同字段
7. `go test ./... -count=1`、`go vet ./...`、Windows amd64 构建和生产数据库只读审计全部通过

**Requirements:** CR-01, CR-02, CR-03, CR-04, CR-05, WR-01, WR-02, WR-03, WR-04, WR-05
**Depends on:** Phase 7
**Plans:** 9/9 plans complete

Plans:

**Wave 1**

- [x] 07.1-01-PLAN.md — 建立不泄漏正文/凭证的 Agent 请求特征诊断和脱敏 fixture
- [x] 07.1-02-PLAN.md — 固化 Frozen 双坐标、Freeze/Restore 字节一致性和 Collapse 基线

**Wave 2** *(blocked on Wave 1 completion)*

- [x] 07.1-03-PLAN.md — 修复 collapse Store 坐标、首次 breakpoint 和最终 snapshot

**Wave 3** *(blocked on Wave 2 completion)*

- [x] 07.1-04-PLAN.md — 保证每请求最多一次召回并按候选即时消费剩余预算

**Wave 4** *(blocked on Wave 3 completion)*

- [x] 07.1-05-PLAN.md — 增加兼容旧重复行的 SQLite migration、content hash 与并发幂等保存

**Wave 5** *(blocked on Wave 4 completion)*

- [x] 07.1-06-PLAN.md — 收紧显式召回信号、跨 session 门槛、top-3、稳定排序和区间 dominance

**Wave 6** *(blocked on Waves 1 and 5 completion)*

- [x] 07.1-07-PLAN.md — 基于脱敏证据实现 DeepSeek Agent 三态分类与 parent Frozen 安全行为

**Wave 7** *(blocked on Wave 6 completion)*

- [x] 07.1-08-PLAN.md — request ID 贯穿、Archive 单汇总 Info 和并发日志可审计性

**Wave 8** *(blocked on Wave 7 completion)*

- [x] 07.1-09-PLAN.md — 跨模块回归、生产数据库只读 dry-run 审计和全量 Windows 验收

**Cross-cutting constraints:**

- 不自动删除、合并或隐式迁移 `data/sawtooth.db` 中的历史 Archive；真正清理需要老大另行明确授权
- 不引入 YesMem daemon、embedding、learnings、briefing、narrative、reflection fork、code map、rules/caps
- YesMem `subagent.go` 只作为旧启发式反例；DeepSeek Agent 分类必须基于脱敏证据，未知身份采用无 Archive 副作用的保守行为
- 所有新增日志禁止记录 system prompt、messages 正文、Authorization 或完整请求体

### Phase 8: Claude Code 协议兼容性加固：多模态 token、持久 user context、真实子代理识别、usage 与双阶段 debug

**Goal:** 修复 sawtooth-proxy 与 Claude Code 2.1.x 的请求协议兼容性，使图片不会触发过度折叠、CLAUDE.md 每轮规则不丢失、子代理可靠隔离、usage 与 debug 可准确审计。

**Success Criteria:**

1. 491,776 字符 PNG base64 不再被当作数十万文本 token，折叠保留量不再被图片编码长度主导
2. `# claudeMd` user context 在低于阈值、collapse、Frozen hit/invalid 三类路径均存在且使用本轮最新版本
3. 主/子代理按 CC billing header/agentContext 强特征分类，model、thinking、sdk-ts 变化不造成误判或跨 session Frozen 污染
4. SSE/JSON/SawtoothTrigger 的总输入 token 与 Anthropic usage 三项之和一致
5. raw-inbound 与 forwarded debug 能证明每一变换阶段，同时不落盘敏感正文或 base64
6. `go test ./... -count=1`、`go vet ./...`、Windows amd64 build 与相关 race 测试通过

**Requirements**: MMTOK-01, MMTOK-02, MMTOK-03, CTX-01, CTX-02, CTX-03, CTX-04, AGENT-01, AGENT-02, USAGE-01, USAGE-02, DEBUG-01, DEBUG-02
**Depends on:** Phase 07.1
**Plans:** 4/4 plans complete

Plans:

**Wave 1**

- [x] 08-01-PLAN.md — 修复多模态语义 token 计数并锁定真实大截图 cutoff 回归

**Wave 2** *(blocked on Wave 1 completion)*

- [x] 08-02-PLAN.md — 建立持久 user context 分离与未知消息字段透明重建契约

**Wave 3** *(blocked on Wave 2 completion)*

- [x] 08-03-PLAN.md — 接线本轮 context-first Frozen 管线与 Claude Code 强特征 Agent 分类

**Wave 4** *(blocked on Wave 3 completion)*

- [x] 08-04-PLAN.md — 统一 usage、双阶段安全 debug 并完成 Phase 8 全量验收 (completed 2026-07-11)

---

*Last updated: 2026-07-13 — Phase 8 is the final completed phase; Phase 1–6 completion metadata and compatibility directories were backfilled so GSD health/progress analysis agrees with STATE.md.*

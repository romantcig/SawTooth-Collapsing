---
gsd_state_version: 1.0
milestone: v1.1
milestone_name: 锯齿折叠对齐与可靠性闭环
status: planning
current_phase: "09"
last_updated: "2026-07-13T21:53:21.421Z"
last_activity: 2026-07-13
progress:
  total_phases: 5
  completed_phases: 0
  total_plans: 0
  completed_plans: 0
  percent: 0
---

# State: Sawtooth Proxy

## Project Reference

See: `.planning/PROJECT.md`
See: `.planning/ROADMAP.md`

**Core value:** CC session 想聊多久聊多久——上下文不会爆，折叠过的历史可以恢复

## Phase Status

v1.0 已于 2026-07-14 归档。Phase 08.1 的 9/9 must-haves 已由自动化证据重验证通过；无法可靠肉眼判断、且仅重复要求接受内部证据的人工 UAT 项已明确关闭并记录为 skipped。

v1.1 已建立 Phase 09–13，当前从 Phase 09 的只读 YesMem 锯齿闭环差距审计开始。任何生产修复必须等待审计确认必要缺口。

## Active Plan

无活跃计划；下一步讨论并规划 Phase 09，只做审计，不修改生产逻辑。

### Quick Tasks Completed

| # | Description | Date | Commit | Status | Directory |
|---|-------------|------|--------|--------|-----------|
| 260705-fxk | 删除 stubify.go 中语义反转的 isDebugFollowup（F-10）逻辑，回归 YesMem 基线行为 | 2026-07-05 | 16b9067 |  | [260705-fxk-stubify-go-isdebugfollowup-f-10-yesmem](./quick/260705-fxk-stubify-go-isdebugfollowup-f-10-yesmem/) |
| 260705-gs9 | 删除 StubStats.ArchivedMessages 死代码（只写入无消费者） | 2026-07-05 | b9717ef |  | [260705-gs9-stubstats-archivedmessages](./quick/260705-gs9-stubstats-archivedmessages/) |
| 260705-rce | golangci-lint 22 项告警清零：删除 unused 死代码、修复 ineffassign/S1008/errcheck | 2026-07-05 | 86c4654, 3ccf970, f034026 |  | [260705-rce-golangci-lint-collapse-go-filterstopword](./quick/260705-rce-golangci-lint-collapse-go-filterstopword/) |
| 260709-pjw | 修复 truncateRunes 代码围栏截断污染与 SearchArchives bm25 聚合排序 | 2026-07-09 | 1e2daac, 9591150 |  | [260709-pjw-truncaterunes-searcharchives-bm25](./quick/260709-pjw-truncaterunes-searcharchives-bm25/) |
| 260709-ukb | 收尾清洁：删 corrupt.db 残留、修 reexpand.go 注释缩进瑕疵、补 SQLite 损坏恢复单测 | 2026-07-09 | e305658, d73f7e7 |  | [260709-ukb-corrupt-db-reexpand-go-sqlite](./quick/260709-ukb-corrupt-db-reexpand-go-sqlite/) |
| 260709-vui | SummaryText 分段感知截断保留 Gotchas/Conclusion + 归档注入预算降级减半重试 | 2026-07-09 | df3baac, 438f903 |  | [260709-vui-searchandexpand-summarytext-gotchas-conc](./quick/260709-vui-searchandexpand-summarytext-gotchas-conc/) |
| 260711-12v | 定制 slog.Handler，使终端日志颜色和短时间戳对齐 YesMem，并确保文件日志无 ANSI 编码 | 2026-07-11 | 22eaf65 |  | [260711-12v-slog-handler-yesmem-ansi](./quick/260711-12v-slog-handler-yesmem-ansi/) |
| 260711-1he | 将 AGENTS.md 加入提交黑名单，审查剩余改动并在验证后提交推送 GitHub | 2026-07-11 | 01849d6 |  | [260711-1he-agents-md-github](./quick/260711-1he-agents-md-github/) |
| 260711-32d | 将日志格式改为时间戳加方括号级别，并仅为级别文字着色 | 2026-07-11 | 21717fe |  | [260711-32d-log-level-prefix-color](./quick/260711-32d-log-level-prefix-color/) |
| 260711-4e0 | 修复上游 Base URL 尾斜杠导致请求路径错误，并添加回归测试 | 2026-07-11 | 1fa804d |  | [260711-4e0-base-url](./quick/260711-4e0-base-url/) |
| 260714-019 | 重构 Claude Code 上游生命周期，以分层 timeout 取代固定 120 秒总限制 | 2026-07-14 | e4fc5f8 | Verified | [260714-019-claude-code-120-hard-limit-header](./quick/260714-019-claude-code-120-hard-limit-header/) |

Last activity: 2026-07-13

---
*Last updated: 2026-07-14*

## Accumulated Context

### Roadmap Evolution

- Phase 07.1 inserted after Phase 7: Frozen、Archive 召回与多 Agent 隔离修复 (URGENT)
- Redundant YesMem gap candidate not retained: its four items were already implemented by `ab39219` and later hardened; evidence archived outside active phases
- Phase 08 added: Claude Code 协议兼容性加固——修复多模态 token、持久 user context、真实子代理识别、usage 与双阶段 debug
- Phase 08.1 inserted after Phase 8: Claude Code 辅助请求隔离与高级工具兼容 (URGENT)

## Performance Metrics

| Phase | Plan | Duration | Notes |
|-------|------|----------|-------|
| Phase 07.1-frozen-archive-agent P01 | 1min | 2 tasks | 6 files |
| Phase 07.1-frozen-archive-agent P02 | 9 min | 2 tasks | 5 files |
| Phase 07.1-frozen-archive-agent P03 | 13min | 2 tasks | 2 files |
| Phase 07.1-frozen-archive-agent P04 | 27min | 2 tasks | 6 files |
| Phase 07.1-frozen-archive-agent P05 | 8min | 2 tasks | 4 files |
| Phase 07.1-frozen-archive-agent P06 | 27min | 2 tasks | 6 files |
| Phase 07.1-frozen-archive-agent P07 | 24min | 2 tasks | 5 files |
| Phase 07.1-frozen-archive-agent P08 | 11min | 2 tasks | 8 files |
| Phase 07.1-frozen-archive-agent P09 | 18min | 2 tasks | 3 files |
| Phase 08.1 P01 | 13 min | 3 tasks | 7 files |
| Phase 08.1 P02 | 18 min | 2 tasks | 2 files |
| Phase 08.1 P03 | 7min | 2 tasks | 5 files |

## Decisions

- [Phase 07.1-frozen-archive-agent]: 当前没有可验证的 parent header；parent marker 保持 false、关系保持 unavailable，不猜测父 session。 — 防止跨 session 错配和敏感 ID 进入诊断。
- [Phase 07.1-frozen-archive-agent]: sdk-ts 只作为兼容 marker，thinking 只记录存在性；Plan 01 不改变现有代理身份判断。 — 分类重写由 Plan 07 基于 fixture 证据完成。
- [Phase 07.1-frozen-archive-agent]: LengthFor 只返回 frozen prefix 消息数，FrozenResult.Cutoff 继续保留原始请求坐标。 — 防止 raw cutoff 与压缩后 prefix length 再次混用。
- [Phase 07.1-frozen-archive-agent]: UpdateMessages 仅允许等长覆盖，并保留 cutoff、boundary 与 token 元数据持久化新 bytes/hash。 — 保证冷启动恢复与 Freeze turn 已发送前缀一致。
- [Phase 07.1-frozen-archive-agent]: tool pair 与 keepRecent 冲突时，无法整对折叠则后退 cutoff 保留整对。 — 同时避免孤立 tool_result 和 recent tail 缩短。
- [Phase 07.1-frozen-archive-agent]: FrozenResult.Cutoff 只用于原始 history 的 boundary 验证与 fresh tail 切片；cache、EagerStub 和 snapshot 边界使用 frozen prefix length。 — 防止 raw cutoff 与压缩后 prefix length 再次混用。
- [Phase 07.1-frozen-archive-agent]: collapse 当轮先完成 EagerStub 与 orphan repair，再注入 cache breakpoint 并 Store 最终发送 prefix bytes。 — 保证 freeze 与 restore 的上游 prefix JSON bytes 一致。
- [Phase 07.1-frozen-archive-agent]: RecallOutcome 只统计实际进入返回 Messages 的候选，预算不足候选计入 Discarded。 — 保证日志、TokenCost 与最终 wire body 一致。
- [Phase 07.1-frozen-archive-agent]: 共享 Budget 逐候选使用 RemainingReExpansion 即时门控和扣费，nil Budget 使用 tokenThreshold/10 本地硬上限。 — 阻止单请求累计超支并保持 nil 路径同等安全。
- [Phase 07.1-frozen-archive-agent]: HandleMessages 在 Frozen 最终判定后执行唯一一次 Archive 召回。 — 消除失效路径的二次搜索、重复副作用和虚假注入记录。
- [Phase 07.1-frozen-archive-agent]: 保留随机 UUID 作为 Archive 引用 ID，以 session、range 与 canonical messages SHA-256 作为新写入逻辑身份。 — 避免破坏现有外键与搜索引用，同时停止以随机 ID 作为幂等边界。
- [Phase 07.1-frozen-archive-agent]: 历史行 content_hash 保持 NULL，不回填、不删除；partial unique index 只约束非空新 hash。 — 允许已有重复旧库无损原地打开。
- [Phase 07.1-frozen-archive-agent]: SaveArchive 由 SQLite ON CONFLICT DO NOTHING 与 RowsAffected 决定是否写关键词。 — 以数据库唯一约束消除并发 SELECT-then-INSERT 竞态。
- [Phase 07.1-frozen-archive-agent]: Archive 召回只接受显式信号，并使用同 session 2 词、跨 session 3 词或精确路径完全匹配门槛。 — 阻止普通 tool/公共词触发跨会话信息污染。
- [Phase 07.1-frozen-archive-agent]: 召回候选在预算前按 content hash 与 80% 区间重叠去重，且每请求全局最多一次完整展开。 — 避免同一历史以 full 与重复 summary 多次污染上下文。
- [Phase 07.1-frozen-archive-agent]: sdk-ts marker 优先识别可靠子代理；DeepSeek 模型族作为主代理证据，missing-thinking 不再决定身份；没有受支持 marker 时返回 unknown。 — 避免 missing-thinking 假阳性，同时让每个身份结论具有受限 reason。
- [Phase 07.1-frozen-archive-agent]: parent Frozen 只接受显式 X-Claude-Code-Parent-Session-Id、非空且非自引用关系，并继续通过 Frozen.Get 校验 cutoff、boundary 和 prefix hash。 — 防止 child session 猜测或误用任意会话 Frozen。
- [Phase 07.1-frozen-archive-agent]: subagent/unknown 无 Frozen 替换时直接转发原 body bytes，只有 parent Frozen 实际替换前缀后才重序列化。 — 保持 DeepSeek/OpenAI 请求前缀 bytes 稳定并减少无意义变换。
- [Phase 07.1-frozen-archive-agent]: request_id 使用 Server 内 atomic.Uint64 从 1 单调分配，并通过 request-scoped logger 固定 request_id/request_session_id。 — 在不修改日志格式器或增加 goroutine 缓冲的前提下，使并发请求链可独立审计。
- [Phase 07.1-frozen-archive-agent]: 入口与上游发送分别使用 original_message_count 和 forwarded_message_count，Archive 来源只使用 source_session_id。 — 消除处理前后计数和当前/来源 session 的语义混淆。
- [Phase 07.1-frozen-archive-agent]: 显式 Archive 召回尝试只输出一次基于最终 RecallOutcome 的 Info 汇总，逐块与预算降级明细降为 Debug。 — 保证日志描述最终发送状态并限制 Info 日志量。
- [Phase 07.1-frozen-archive-agent]: 生产 DB/WAL/SHM 只读复制到临时快照，SQLite mode=ro 仅打开副本，并以前后 size/mtime/SHA-256 证明生产文件不变。
- [Phase 08.1]: 标题分类只依赖单条 user、完整 session envelope、受限标题 system 意图及 title-only schema；tools、thinking、model 不参与硬门禁。
- [Phase 08.1]: output_config 只有整段缺失时允许严格 prompt fallback；存在但无效时 fail closed。
- [Phase 08.1]: nil/零值 requestMeta 默认跟踪 Sawtooth，只有 session_title 禁止响应状态写回。
- [Phase 08.1]: 删除 RecallSignalExactPath 独立 kind；ExactPath 只作为 recovery/deep_search provenance 的完全匹配修饰。
- [Phase 08.1]: 明确恢复短语携带路径时生成 Recovery signal，并以原始路径作 Query、Terms 和 ExactPath。
- [Phase 08.1]: 普通路径零查询使用已关闭的临时 SQLite store 作故障探针，不添加生产测试钩子。

## Session

**Last session:** 2026-07-14T02:53:22+08:00
**Stopped at:** Quick task 260714-019 completed and verified; ready for the next task
**Resume file:** None

## Current Position

Phase: Not started (defining requirements)
Plan: —
Status: Ready to plan Phase 09
Last activity: 2026-07-14 — Milestone v1.1 requirements and roadmap created

## Operator Next Steps

- Run `$gsd-discuss-phase 09` to lock the audit evidence format and comparison baseline.

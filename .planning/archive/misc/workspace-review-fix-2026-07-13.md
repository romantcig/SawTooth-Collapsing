---
phase: workspace
fixed_at: 2026-07-12T18:03:07Z
review_path: .planning/archive/misc/workspace-review-2026-07-13.md
iteration: 3
findings_in_scope: 8
fixed: 8
skipped: 0
status: all_fixed
committed: false
---

# Workspace Code Review Fix Report

**Fixed at:** 2026-07-12T18:03:07Z

**Source review:** `.planning/archive/misc/workspace-review-2026-07-13.md`

**Final iteration:** 3

**Commit policy:** 工作区模式，不自动提交老大原有修改

## Summary

- Findings in scope: 8
- Fixed: 8
- Skipped: 0
- Final deep re-review: clean

## Fixed Issues

### CR-01: 活动 tool-use 轮次在 fallback 中丢失 signed thinking

`CompressContext`、`stubifyMessages`、`ApplyDecayBatch` 与 eager 路径现在统一保护活动 assistant/tool_result 原子对，不依赖 `keep_recent`，并增加回归测试验证未知字段和 `cache_control` 不被改写。

### CR-02: ContentBlock 重建丢失协议字段

为 `ContentBlock` 增加透明 JSON round trip，保留 `redacted_thinking.data`、`cache_control` 和未来 block 字段；非 `tool_use` block 不再产生 `input:null`。

### WR-01: Stage 1/2 decay 未写回

文本 decay 现在按 `decayed != original` 标记变更并重建消息，测试覆盖 Middle 与 Old 阶段的实际 content。

### WR-02: 并行工具结果错误复用第一个工具元数据

eager stub 改为构建 `tool_use_id -> tool info` 映射，并按每个 `tool_result.tool_use_id` 生成摘要；测试覆盖逆序 Read/Bash 并行结果。

### WR-03: 非法 cache_ttl 进入 wire JSON

配置加载严格规范化为 `ephemeral` 或 `1h`；底层 `NormalizeCacheTTL` 对未知值返回错误，避免内部策略与 wire TTL 不一致。

### CR-03: Collapse cutoff 越过活动工具对并产生空 ArchiveBlock

`CalcCollapseCutoff` 将活动工具对视为不可跨越尾部，并拒绝退化或越界 cutoff；调用方同时拒绝空 ArchiveBlock，失败时进入 fallback，不保存伪存档、不清理 decay 状态。

### WR-04: cache 管理改写活动工具对

cache strip、breakpoint 注入、数量限制和 TTL normalize 只作用于活动 assistant 之前的稳定历史切片，活动 pair 保持入口 JSON 不变。

### WR-05: EagerStubMemory 旧持久化快照覆盖新快照

同 session 的更新、排序快照和同步持久化现在在同一写锁内串行完成；并发测试验证最终持久化集合不会倒退。

## Verification

- `go test ./... -count=1`：通过。
- MinGW-w64 工具链确认：`windows/amd64`、`CGO_ENABLED=1`、`CC=C:/msys64/mingw64/bin/gcc.exe`。
- `go test -race ./internal/proxy -count=1`：通过。
- `go build -o sawtooth-proxy.exe ./cmd/proxy/`：通过。
- `git diff --check`：通过；仅有仓库现存的 LF/CRLF 转换提示。
- GSD iteration 3 deep re-review：20 个文件，0 findings，`status: clean`。

---

_Fix mode: workspace-safe inline repair; no automatic commit._
_Review dispatch: generic-agent workaround because typed GSD agents were unavailable._

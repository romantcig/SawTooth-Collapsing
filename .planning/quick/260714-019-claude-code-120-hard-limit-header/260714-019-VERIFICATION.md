---
phase: quick-260714-019
verified: 2026-07-13T18:49:48Z
status: passed
score: 8/8 must-haves verified
behavior_unverified: 0
overrides_applied: 0
---

# Quick Task 260714-019 Verification Report

**目标：** 重构 Sawtooth 到上游模型的请求生命周期，使 Claude Code 决定正常等待、取消与重试，Sawtooth 只负责连接健康和极端资源兜底。

**验证时间：** 2026-07-13T18:49:48Z  
**状态：** passed  
**模式：** 首次验证

## 结论

当前代码和测试真实实现了 PLAN 的 objective、8 项 `must_haves.truths` 以及末尾 success criteria。固定 120 秒 `http.Client.Timeout` 已被 `Timeout: 0` 和六项分层保护取代；Claude Code 的下游 context 仍是取消上游的首要权威；stream/non-stream 响应头预算、响应体 idle、hard limit、504/502 分类、响应提交边界、wire header/body 长度、POST 单次调用和日志敏感边界均有针对性行为测试。

未发现必须人工验证的行为，也未发现阻塞目标的实现缺口。

## Goal Achievement

### Observable Truths

| # | Truth | Status | Evidence |
|---|---|---|---|
| 1 | Claude Code 持续等待时不再被固定 120 秒总时限截断；Claude Code 取消/断开仍权威地取消上游 | ✓ VERIFIED | `newUpstreamHTTPClient` 明确设置 `Timeout: 0`；`forwardRaw` 从 `r.Context()` 派生 hard context 后创建上游请求；`TestForwardRawClaudeCodeCancelDoesNotWriteGatewayError` 验证取消传播且下游零写入；`TestForwardRawLongSSEProgressOutlivesLegacyLimit` 验证持续 SSE 超过概念旧总界限仍完整结束。 |
| 2 | 六项保护默认 15s/15s/10m/30m/10m/60m，均可配置，显式 0 禁用且不会回退旧 120s | ✓ VERIFIED | `TransportConfig`、`DefaultConfig`、仅负值回退的 `validateConfig` 与裸 YAML `0` 解码均已实现；`TestTransportConfigDefaults`、`TestTransportConfigYAMLAndZeroDisable`、`TestTransportConfigFilesMatchDefaults` 通过。 |
| 3 | stream/non-stream 依据请求正文选择不同响应头预算；响应头之后不再受 header timeout 限制，持续进展 SSE 可长期读取 | ✓ VERIFIED | `streamRequest` → context marker → `streamAwareTransport` 的链路已接通；两套长期 `http.Transport` 分别设置 `ResponseHeaderTimeout`；`TestStreamAwareTransportSelectsHeaderTimeout` 和长 SSE 测试通过。 |
| 4 | ST-owned header/idle/hard timeout 在可提交状态时返回 504；connect/TLS/EOF 等 transport 失败返回 502；下游取消不伪造网关错误 | ✓ VERIFIED | `classifyUpstreamFailure` 依次判断 downstream context、hard cause、idle flag、awaiting_headers timeout 与 transport fallback；header、hard、idle、unexpected EOF、connect/TLS 和 cancel 分类测试全部通过。 |
| 5 | SSE 已提交后发生 idle/hard timeout 只终止流并记录 `response_committed=true`，不追加伪造 JSON | ✓ VERIFIED | `handleSSE` 延迟到首个完整事件才提交 200；`forwardRaw` 在 `result.committed` 时只记录后返回；`TestForwardRawSSETimeoutAfterCommitTerminatesWithoutForgedJSON` 的 idle/hard 两个子测试均通过。 |
| 6 | 上游请求不携带 `Connection`，`ContentLength` 与重建 body 一致；模型 POST 不自动重试 | ✓ VERIFIED | `forwardRaw` 使用 `bytes.NewReader(body)`、设置 `ContentLength` 并删除复制的 `Connection`/`Content-Length`；routing RoundTripper 只路由一次；wire 测试与 ambiguous POST 严格单次调用测试通过。 |
| 7 | 失败/超时日志包含 elapsed、phase、timeout source、stream；普通 URL 可保留，URL userinfo、Authorization/API key 和正文不进入本地失败日志 | ✓ VERIFIED | `logUpstreamFailure` 固定输出 `elapsed_ms`、`phase`、`timeout_source`、`stream`；`safeURLError` 清除 userinfo 并保留普通 URL；独立哨兵测试确认普通 host/path/query 保留且四类敏感内容均未泄漏。 |
| 8 | 非 2xx 响应完整读取 body 后才提交；读取期间 ST-owned idle/hard timeout 返回单一 504，不混入部分上游正文 | ✓ VERIFIED | `handleNon2xx` 在 `io.ReadAll` 成功后才复制 header/提交 status/body；读取错误回到统一分类器；429 idle 与 503 hard 两个 pre-commit 测试均断言单次 504 且无部分正文。 |

**Score:** 8/8 truths verified（behavior-unverified: 0）

## Required Artifacts

| Artifact | Expected | Status | Details |
|---|---|---|---|
| `internal/proxy/transport.go` | 分层 Transport、stream 路由、phase、hard/idle 原语 | ✓ VERIFIED | 182 行实质实现；由 `NewServer` 和 `forwardRaw` 实际调用；无 TODO/FIXME/XXX。 |
| `internal/proxy/proxy.go` | `TransportConfig`、默认值/校验、无总 timeout client 构造 | ✓ VERIFIED | 六项字段、默认值、自定义 YAML 0 解码、仅负值回退及 client 构造均已接线。 |
| `internal/proxy/forward.go` | context 派生、header 清理、错误分类、响应生命周期 | ✓ VERIFIED | 上游请求、Do/read 分类、日志、non-2xx/JSON/SSE 提交路径均为实际生产调用路径。 |
| `internal/proxy/transport_test.go` | 配置、路由、idle、hard 和 YAML 行为测试 | ✓ VERIFIED | 288 行；所有计划命名行为均存在并通过。 |
| `internal/proxy/forward_test.go` | cancel、504/502、wire、日志、non-2xx、SSE 与单次 POST 测试 | ✓ VERIFIED | 856 行；计划重点行为均由确定性短时测试覆盖。 |
| `sawtooth.yaml.example` / `sawtooth_zh.yaml.example` | 六项默认值、用途及 0 语义 | ✓ VERIFIED | 中英文说明均完整；解析值与 `DefaultConfig` 一致。 |
| `sawtooth.yaml` | 本机六项推荐配置并保持 CRLF | ✓ VERIFIED | 文件存在、六项值与默认值一致，`file` 确认仍为 UTF-8 CRLF；该文件继续由 `.gitignore` 排除。历史非 transport 内容没有 Git 基线可做字节级前后比较，但当前值与 PLAN 记录的本机设置一致。 |

## Key Link Verification

| From | To | Via | Status | Details |
|---|---|---|---|---|
| `r.Context()` | 上游请求 context | `withProxyHardLimit` → stream marker → `httptrace.WithClientTrace` → `http.NewRequestWithContext` | ✓ WIRED | parent cancel 和 hard cause 均有行为测试。 |
| 请求 body `stream` | 两套响应头预算 | `streamRequest` → context value → `streamAwareTransport.RoundTrip` | ✓ WIRED | 不依赖响应 Content-Type 猜测；不同预算选择测试通过。 |
| phase/cause/idle flag | 504/502/silent cancel | `classifyUpstreamFailure` | ✓ WIRED | Do 错误和 response body read 错误都进入同一分类入口。 |
| `HTTPClient.Do` error | 安全本地日志 | `logUpstreamFailure` → `safeUpstreamError` / `safeURLError` | ✓ WIRED | 普通 URL 诊断信息保留，userinfo 被清除。 |
| YAML transport duration | 每请求实际 deadline | `LoadConfig`/`validateConfig` → `NewServer` → client Transport / `forwardRaw` hard+idle | ✓ WIRED | 六项配置均有实际消费者。 |
| 重建 body bytes | 上游 wire 长度 | `bytes.NewReader` + `Request.ContentLength` + header 清理 | ✓ WIRED | 自定义 RoundTripper 观察到正确 body 与长度。 |
| 非 2xx response | 延迟提交或单一网关错误 | `handleNon2xx` pre-commit buffer → unified classifier | ✓ WIRED | idle/hard timeout 不提交原状态、不泄漏部分正文。 |

## Behavioral Spot-Checks

| Behavior | Command | Result | Status |
|---|---|---|---|
| 计划指定的 transport 与 forwarding 生命周期行为 | `go test ./internal/proxy -run 'TestTransportConfig\|TestStreamAwareTransport\|TestIdleTimeoutBody\|TestHardLimitCause\|TestForwardRaw(...)' -count=1 -v` | 所有目标测试及子测试通过，包结果 `ok` | ✓ PASS |
| 普通 workspace 回归 | `go test ./... -count=1` | `cmd/proxy` 无测试；`internal/proxy` 的一个既有测试在 Windows `TempDir RemoveAll cleanup` 报目录非空。该测试来自提交 `78ab9545`，不在本 quick 的改动范围；其业务断言没有失败。 | ⚠️ NON-BLOCKING FLAKE |
| 对上述异常的单测试复核 | `go test ./internal/proxy -run '^TestSearchAndExpandNoRecallSummaryForPlainPath$' -count=1 -v` | PASS | ✓ PASS |
| Windows MinGW-w64 race 全包回归 | `source /c/Users/romantcig/.bashrc && test "$($CC -dumpmachine)" = "x86_64-w64-mingw32" && go test -race ./internal/proxy -count=1` | `CC=C:/msys64/mingw64/bin/gcc.exe`，完整 `internal/proxy` race 套件通过（64.258s） | ✓ PASS |
| 静态检查 | `go vet ./...` | exit 0，无输出 | ✓ PASS |
| Windows 原生构建 | `go build -o sawtooth-proxy.exe ./cmd/proxy/` | exit 0；在已验证的 MinGW-w64 环境中执行 | ✓ PASS |

普通 workspace 命令的一次性失败被归类为非阻塞环境抖动，理由是：失败仅发生于测试框架的临时目录清理阶段；该测试未被本 quick 修改；单独复跑通过；随后覆盖整个 `internal/proxy` 包的 race 测试也全部通过。按 verifier 的“全 workspace 命令最多运行一次”约束，没有再次重复整套普通测试。

## Requirements Coverage

PLAN 的 `requirements` 为空，本 quick 不映射 milestone requirements；末尾 6 项 success criteria 均由上面 8 项 truth 覆盖，没有孤立 requirement。

## Anti-Patterns and Scope Audit

| Check | Result | Severity |
|---|---|---|
| 修改文件中的 `TBD` / `FIXME` / `XXX` | 无匹配 | None |
| `TODO` / `HACK` / `PLACEHOLDER` 或空实现 | 无相关匹配 | None |
| 生产代码中的旧 120 秒 `http.Client.Timeout` | 未发现；唯一 client-level timeout 是 `Timeout: 0` | None |
| 代理 POST retry | 未发现；单次 RoundTrip 行为测试通过 | None |
| `context.Background/TODO` 脱离下游取消 | 生产实现未发现；匹配仅在测试中 | None |
| ROADMAP / milestone / dependency 改动 | 提交范围没有 `.planning/ROADMAP.md`、phase 目录、`go.mod` 或 `go.sum` 改动 | None |
| `git diff --check d730029^..e4fc5f8` | 通过 | None |
| 提交存在性 | `d730029`、`ef62d08`、`3200f1e`、`5c3fdef`、`e4fc5f8` 均可解析 | None |

## Disconfirmation Pass

- **可能的部分满足项：** 普通 `go test ./...` 首次运行出现既有 SQLite 测试的 Windows 临时目录清理抖动。单测和全包 race 复核均通过，因此记录为警告而非本 quick 缺口。
- **可能误导的测试：** header 分类测试为 stream/non-stream 注入了相同 30ms；它独立证明分类来源，但不单独证明预算不同。不同预算的实际路由由 `TestStreamAwareTransportSelectsHeaderTimeout` 使用 40ms/500ms 明确验证，因此组合证据充分。
- **额外错误路径检查：** connect/TLS 的 `net.Error.Timeout()` 由 phase 优先级保持为 502；2xx JSON idle、非 2xx idle/hard、SSE committed idle/hard 和 header-before-commit 均有覆盖，没有发现未经测试的核心提交边界。

## Probe Execution

SKIPPED：PLAN/SUMMARY 未声明 probe，仓库中也没有适用于本 quick 的 `scripts/*/tests/probe-*.sh`。

## Human Verification Required

无。所有 must-have 都是可由 Go 单元/集成测试和静态接线检查验证的网络生命周期行为；没有视觉、外部服务或性能体感项目。

## Gaps Summary

无阻塞缺口。Quick task 目标已实现，可以由主代理继续完成文档/STATE 收尾；verifier 未修改实现源码、STATE、ROADMAP，也未创建提交。

---

_Verified: 2026-07-13T18:49:48Z_  
_Verifier: gsd-verifier_

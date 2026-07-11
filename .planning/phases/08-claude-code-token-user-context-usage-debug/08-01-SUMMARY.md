---
phase: 08-claude-code-token-user-context-usage-debug
plan: 01
subsystem: proxy
tags: [go, multimodal, tokenizer, png, jpeg, gif, webp, collapse]

requires:
  - phase: 07-collapse-first
    provides: Collapse-first cutoff、ArchiveBlock 与统一 TokenCounter 基础
  - phase: 07.1-frozen-archive-agent
    provides: keepRecent、tool pair 与 Frozen boundary 回归契约
provides:
  - 递归多模态消息 token 估算，base64 永不进入文本 BPE
  - PNG、JPEG、GIF、WebP 有界头部尺寸解析与视觉 token 公式
  - image/document/未知媒体失败安全的有界 fallback
  - 真实规模脱敏大截图 fixture 与 75k retained-tail cutoff 回归
affects: [08-02-user-context, 08-04-debug-observability, collapse-budget]

tech-stack:
  added: []
  patterns:
    - "多模态语义计数：text 走 BPE，image/document 走独立估算"
    - "有界二进制解析：固定格式头或 64 KiB JPEG 扫描窗口"
    - "单一计数入口：Collapse 只调用 TokenCounter.CountMessageTokens"

key-files:
  created:
    - internal/proxy/tokenizer_test.go
    - internal/proxy/testdata/multimodal/large-screenshot-tool-result.json
  modified:
    - internal/proxy/tokenizer.go
    - internal/proxy/collapse.go
    - internal/proxy/collapse_test.go

key-decisions:
  - "图片按 ceil(width/28) * ceil(height/28) 估算，视觉结果与 fallback 统一限制在 8192 token。"
  - "JPEG 最多解码并扫描 64 KiB base64 前缀；其他格式只解码各自固定头部。"
  - "未知 block 在结构估算前递归删除任意 source.data，避免未来媒体类型绕回 BPE。"
  - "回归 fixture 使用可解码的纯白 PNG 和合法 ancillary padding，保留 1920x897、491776 base64 字符、368830 raw bytes，不复制抓包像素。"

patterns-established:
  - "TokenCounter.CountMessagesTokens -> CountMessageTokens -> semantic block traversal 是本地预算唯一调用链。"
  - "不可信媒体解析失败只影响估算分支，返回正数且有上限，不影响请求转发。"

requirements-completed: [MMTOK-01, MMTOK-02, MMTOK-03]

coverage:
  - id: D1
    description: "嵌套 tool_result 按 text/image/document 语义递归计数，source.data 不进入 BPE"
    requirement: MMTOK-01
    verification:
      - kind: unit
        ref: "internal/proxy/tokenizer_test.go#TestTokenCounterMultimodalNestedToolResult,TestTokenCounterUnknownBlockExcludesSourceData"
        status: pass
    human_judgment: false
  - id: D2
    description: "PNG、JPEG、GIF、WebP 从有界头部读取尺寸并按 28px tile 公式估算"
    requirement: MMTOK-02
    verification:
      - kind: unit
        ref: "internal/proxy/tokenizer_test.go#TestTokenCounterImageFormatsAndBoundedPayload"
        status: pass
    human_judgment: false
  - id: D3
    description: "异常媒体有界 fallback，真实规模截图不再把 cutoff 钳到 keep_recent 边界"
    requirement: MMTOK-03
    verification:
      - kind: unit
        ref: "internal/proxy/tokenizer_test.go#TestTokenCounterImageDocumentBase64FallbacksAreBounded"
        status: pass
      - kind: integration
        ref: "internal/proxy/collapse_test.go#TestTokenCounterLargeScreenshotFixtureIsSanitizedAndVisualScale,TestCalcCollapseCutoffLargeScreenshotRetainsSemanticTokenFloor"
        status: pass
    human_judgment: false

duration: 1h 3m
completed: 2026-07-12
status: complete
---

# Phase 08 Plan 01: 多模态 Token 与 Collapse 预算修复 Summary

**将 491776 字符 PNG 从 344931 文本 token 纠正为约 2277 视觉 token，并让 Collapse cutoff 重新按 75000 semantic token tail floor 保留历史。**

## Performance

- **Duration:** 1h 3m
- **Started:** 2026-07-11T19:51:57Z
- **Completed:** 2026-07-11T20:55:49Z
- **Tasks:** 2
- **Files modified:** 5

## Accomplishments

- `CountMessageTokens` 直接递归原始 JSON，嵌套 text 继续使用 cl100k BPE，image/document 的 `source.data` 永不进入 BPE。
- PNG、JPEG、GIF、WebP 只解码所需头部；畸形 base64、未知媒体、异常尺寸和 document 使用正数、有上限的 fallback。
- 脱敏 fixture 精确保留真实样本的 1920x897、491776 base64 字符和 368830 raw bytes，标准库可验证为合法 PNG。
- Collapse cutoff、Archive token 与 blank placeholder 全部复用 TokenCounter；大截图回归不再退化到 `n-8` keep_recent 边界。

## Task Commits

每个 TDD 阶段均已原子提交：

1. **Task 1 RED: 多模态 token 失败契约** - `9479cae` (test)
2. **Task 1 GREEN: 递归语义估算与有界图片头解析** - `5de8616` (fix)
3. **Task 2 RED: 大截图 fixture、tail floor 与单一入口契约** - `731243a` (test)
4. **Task 2 GREEN: Collapse 统一使用 TokenCounter** - `276c11b` (fix)

**Plan metadata:** 本 SUMMARY 完成提交。

## Files Created/Modified

- `internal/proxy/tokenizer.go` - 递归 block 分派、图片尺寸解析、视觉公式、有界 fallback 与 source.data 清理。
- `internal/proxy/tokenizer_test.go` - 四格式、嵌套内容、异常输入、超长 payload 和未知 block 测试。
- `internal/proxy/collapse.go` - 删除重复计数器，cutoff 与 placeholder 改用 TokenCounter。
- `internal/proxy/collapse_test.go` - 合法脱敏 PNG、视觉估算、75k tail floor 和单一入口回归。
- `internal/proxy/testdata/multimodal/large-screenshot-tool-result.json` - 不含凭证、system、CLAUDE.md、session 或抓包正文的最小 tool_result fixture。

## Decisions Made

- 视觉 token 与异常 fallback 均使用命名上限 8192；既覆盖 CC 发送前常见的 2000px 级图像，也阻止伪造尺寸制造整数溢出或无限预算。
- JPEG 采用最大 64 KiB SOF marker 扫描窗口；PNG/GIF/WebP 使用固定字段，避免完整解码大 payload。
- document 本阶段仅根据编码规模做有界 fallback，不实现 PDF 分页，符合 Phase 08 deferred scope。
- Anthropic schema 通过 Context7 的官方 TypeScript SDK 类型复核：`tool_result.content` 可嵌套 text/image/document，base64 source 使用 `type`、`media_type`、`data`。

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

- 一次性 fixture 生成器最初把 `color.White.Y` 的 `uint16` 赋给灰度像素 `uint8`，编译失败；改为确定值 255 后生成成功。生成器已删除，未进入提交。
- Context7 CLI 不在子代理 PATH 中；发现 Context7 MCP 可用后按项目首选顺序完成官方 SDK schema 查询，未使用网页搜索。

## User Setup Required

None - no external service configuration required.

## Verification

- `go test ./internal/proxy -run 'TestTokenCounter.*(Multimodal|Image|Document|Base64)' -count=1` - PASS
- `go test ./internal/proxy -run 'Test(CalcCollapseCutoff|CollapseOldMessages).*(Multimodal|LargeScreenshot|TokenFloor)|TestTokenCounter.*LargeScreenshot' -count=1` - PASS
- `go test ./internal/proxy -run 'TestTokenCounter|TestCalcCollapseCutoff|TestCollapseOldMessages' -count=1` - PASS
- `go vet ./internal/proxy` - PASS
- `go test ./internal/proxy -count=1` - PASS

## Next Phase Readiness

- MMTOK-01 至 MMTOK-03 已闭环，Collapse 的 tokenFloor 重新基于语义 token。
- Plan 08-02 可在可信预算基础上修复 `# claudeMd` 持久 user context 的 Strip/Collapse/Frozen 生命周期。
- 无遗留 blocker；执行前已有的 `.planning/STATE.md` 修改保持原样，由主执行器统一处理。

## Self-Check: PASSED

- 已确认五个计划产物存在，fixture 为合法 1920x897 PNG 且 base64 长度为 491776。
- 已确认四个任务提交存在，所有 acceptance criteria 和计划级验证均通过。
- 已确认 `.planning/STATE.md` 未暂存，SUMMARY 提交不包含执行前工作区修改。

---
*Phase: 08-claude-code-token-user-context-usage-debug*
*Completed: 2026-07-12*

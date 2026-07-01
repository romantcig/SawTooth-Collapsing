# SawTooth-Collapsing

[English](README.md)

一个位于 Claude Code 和 Anthropic API 之间的代理。当对话接近阈值时，自动对历史消息进行锯齿折叠（Sawtooth Collapsing）——省钱、提纯上下文、按需召回。

---

## 核心能力

**📦 锯齿折叠**
对话达到阈值时，自动将旧消息压缩为结构化存档块（Archive Block），保留工具调用记录、文件变更、关键决策、时间线等元信息。折叠后的历史仍然可恢复，不丢信息。

**💰 省钱**
通过 Frozen Prefix Cache 锁定已折叠的消息前缀，利用 Anthropic 前缀缓存机制，重复请求不再计费。折叠后的消息体积下降 60-90%，每次 API 调用的 token 消耗大幅降低。

**上下文提纯，提升长对话续航🔋**
分层压缩策略——截断大文本、桩化已完成工具调用、压缩 thinking 块、合并连续低价值消息。对话越久，噪声占比越低，模型注意力集中在当前任务上。

**🗺️ 按需召回**
当用户提问涉及之前讨论过的内容，只需包含 `deep_search('关键词')`，代理自动从 SQLite 全文索引中检索相关存档块，将原文注入当前对话。模型能直接引用之前的细节回答。

---

## 工作流程

```
Claude Code → Sawtooth Proxy → Anthropic API
                  │
                  ├─ Stubify   (截断 + 桩化)
                  ├─ Decay     (四阶段老化追踪)
                  ├─ Compress  (压缩 thinking / tool_result)
                  ├─ Compact   (合并低价值消息)
                  ├─ Collapse  (折叠为 Archive Block)
                  ├─ Frozen    (前缀缓存，省 API 费)
                  └─ Reexpand  (关键词召回已折叠内容)
```

---

## 项目状态⚠️

**Alpha** —— 锯齿折叠核心已完善（对齐原项目）。项目为预览版，仍具有不确定性。


## 快速开始

### 构建

```bash
# 需要 Go 1.23+
go build -o sawtooth-proxy.exe ./cmd/proxy/
```

### 配置

复制并编辑配置文件：

```bash
cp sawtooth.yaml.example sawtooth.yaml
```

关键配置项：

```yaml
server:
  port: 9099

proxy:
  target: https://api.anthropic.com   # 上游 API 地址

stubify:
  token_threshold: 200000             # 触发压缩的 token 阈值
```

### 运行

```bash
./sawtooth-proxy.exe
```

### 配置 Claude Code

设置环境变量：

```bash
set ANTHROPIC_BASE_URL=http://localhost:9099
```

## 约束

- **Go 1.23+**：纯 Go，无 CGo
- **Windows 原生**：不依赖 WSL2
- **单进程**：不依赖 daemon/service manager
- **SQLite**：嵌入式存储，无需外部数据库

## 出处

Sawtooth Proxy 的核心算法源自 [YesMem](https://github.com/papoo/yesmem) 项目（`internal/proxy/`），经提取和适配后作为独立代理运行。

关键适配点：
- 移除 daemon 依赖，改用嵌入式 SQLite
- Unix 信号处理改为 Windows 兼容（`os.Interrupt`）
- 类型化消息结构体替代动态 `map[string]any`

## 协议

Apache License 2.0。原始 YesMem 代码版权归 [Papoo Software & Media GmbH](https://papoo.de) 所有。衍生部分版权归 romantcig 所有。

详见 [LICENSE](./LICENSE)。

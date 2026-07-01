# SawTooth-Collapsing

[中文](README_CN.md)

A proxy between Claude Code and the Anthropic API. When a conversation approaches a threshold, it automatically performs sawtooth collapsing on the message history—saving costs, refining the context, and retrieving information on demand.

---

## Core Competencies

**📦 Sawtooth Collapse**
When a conversation reaches a certain threshold, older messages are automatically compressed into structured archive blocks, retaining metadata such as tool call logs, file changes, key decisions, and timelines. The collapsed history can still be restored without any loss of information.

**💰 Save Money**
By locking collapsed message prefixes via the Frozen Prefix Cache and leveraging Anthropic’s prefix caching mechanism, repeat requests are no longer billed. The size of collapsed messages is reduced by 60–90%, significantly lowering token consumption per API call.

**🔋 Context Purification**
Layered compression strategy to extend long conversation lifespan—truncating long text, stubbing completed tool calls, compressing “thinking” blocks, and merging consecutive low-value messages. The longer the conversation lasts, the lower the proportion of noise becomes, and the model’s attention focuses on the current task.

**🗺️ On-Demand Recall**
When a user’s question relates to content discussed earlier, simply include `deep_search('keyword')`, and the agent will automatically retrieve the relevant archive blocks from the SQLite full-text search index and inject the original text into the current conversation. The model can then directly reference those details in its response.

---

## Workflow

```
Claude Code → Sawtooth Proxy → Anthropic API
                  │
                  ├─ Stubify   (Truncation + Stubification)
                  ├─ Decay     (Four-Stage Aging Tracking)
                  ├─ Compress  (Thinking + Tool Result Compression)
                  ├─ Compact   (Merge low-value messages)
                  ├─ Collapse  (Collapse to Archive Block)
                  ├─ Frozen    (Prefix cache, saves on API fees)
                  └─ Reexpand  (Recall collapsed content using keywords)
```

---

## Project Status ⚠️

**Alpha** — The sawtooth folding core has been refined (aligned with the original project). This is a preview version and is still subject to change.

## Quick Start

### Build

```bash
# Requires Go 1.23 or later
go build -o sawtooth-proxy.exe ./cmd/proxy/
```

### Configuration

Copy and edit the configuration file:

```bash
cp sawtooth.yaml.example sawtooth.yaml
```

Key Configuration Items:

```yaml
server:
  port: 9099

proxy:
  target: https://api.anthropic.com # Upstream API address

stubify:
  token_threshold: 200000 # Threshold for triggering compression
```

### Run

```bash
./sawtooth-proxy.exe
```

### Configuring Claude Code

Set the environment variables:

```bash
set ANTHROPIC_BASE_URL=http://localhost:9099
```

## Constraints

- **Go 1.23+**: Pure Go, no CGo
- **Native Windows**: Does not rely on WSL2
- **Single-process**: Does not rely on a daemon or service manager
- **SQLite**: Embedded storage; no external database required

## Source

The core algorithm of Sawtooth Proxy is derived from the [YesMem](https://github.com/papoo/yesmem) project (`internal/proxy/`); after being extracted and adapted, it runs as a standalone proxy.

Key Fit Points:
- Remove the daemon dependency and switch to embedded SQLite
- Unix signal handling has been made Windows-compatible (`os.Interrupt`)
- Typed message structs as an alternative to dynamic `map[string]any`

## License

Apache License 2.0. The original YesMem code is copyrighted by [Papoo Software & Media GmbH](https://papoo.de). The derivative portions are copyrighted by romantcig.

For more information, see [LICENSE](./LICENSE).

package proxy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"gopkg.in/yaml.v3"
)

// DebugConfig 调试模式配置
type DebugConfig struct {
	// Enabled 为 true 时启用不含正文或凭证的结构化 facts。
	Enabled bool `yaml:"enabled"`
	// FullBody 为 true 时额外启用旧完整请求/响应落盘；默认关闭。
	FullBody bool `yaml:"full_body"`
	// DataDir 调试数据落盘根目录
	DataDir string `yaml:"data_dir"`
}

// ProxyConfig 代理转发配置
type ProxyConfig struct {
	// Target Anthropic API 目标地址
	Target string `yaml:"target"`
	// Deflation usage 衰减系数（0.0-1.0，默认 0.7 表示保留 70%）
	Deflation float64 `yaml:"deflation"`
}

// TransportConfig 控制 Sawtooth 到上游 API 的分阶段网络生命周期。
// 所有 duration 的 0 值都表示显式禁用对应保护。
type TransportConfig struct {
	DialTimeout            time.Duration `yaml:"dial_timeout"`
	TLSHandshakeTimeout    time.Duration `yaml:"tls_handshake_timeout"`
	StreamHeaderTimeout    time.Duration `yaml:"stream_header_timeout"`
	NonStreamHeaderTimeout time.Duration `yaml:"non_stream_header_timeout"`
	ResponseIdleTimeout    time.Duration `yaml:"response_idle_timeout"`
	HardTimeout            time.Duration `yaml:"hard_timeout"`
}

// UnmarshalYAML 兼容 yaml.v3 对 duration 字符串的原生解析，并额外接受裸标量 0。
// yaml.v3 原生支持 "15s" 和字符串 "0"，但裸整数 0 不能直接解码到 time.Duration；
// 配置契约要求 0 表示显式禁用，因此在 transport 分组边界统一补齐该语义。
func (cfg *TransportConfig) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("transport 必须是 YAML mapping")
	}
	for index := 0; index+1 < len(node.Content); index += 2 {
		key := node.Content[index].Value
		value := node.Content[index+1]
		known := key == "dial_timeout" ||
			key == "tls_handshake_timeout" ||
			key == "stream_header_timeout" ||
			key == "non_stream_header_timeout" ||
			key == "response_idle_timeout" ||
			key == "hard_timeout"
		if !known {
			continue
		}
		duration, err := decodeTransportDuration(value)
		if err != nil {
			return fmt.Errorf("解析 transport.%s 失败: %w", key, err)
		}
		switch key {
		case "dial_timeout":
			cfg.DialTimeout = duration
		case "tls_handshake_timeout":
			cfg.TLSHandshakeTimeout = duration
		case "stream_header_timeout":
			cfg.StreamHeaderTimeout = duration
		case "non_stream_header_timeout":
			cfg.NonStreamHeaderTimeout = duration
		case "response_idle_timeout":
			cfg.ResponseIdleTimeout = duration
		case "hard_timeout":
			cfg.HardTimeout = duration
		}
	}
	return nil
}

func decodeTransportDuration(node *yaml.Node) (time.Duration, error) {
	if node.Tag == "!!int" && node.Value == "0" {
		return 0, nil
	}
	var duration time.Duration
	if err := node.Decode(&duration); err != nil {
		return 0, err
	}
	return duration, nil
}

// StubifyConfig stub 化与衰减配置 (Phase 2, D-04)
type StubifyConfig struct {
	// TokenThreshold 触发衰减处理的总 token 上限（默认 100000）
	TokenThreshold int `yaml:"token_threshold"`
	// KeepRecent 尾部不桩化的最近消息数（对标 YesMem cfg.KeepRecent，默认 8）
	KeepRecent int `yaml:"keep_recent"`
	// KeepThinking 是否保留 thinking blocks（默认 false，1M 上下文建议 true）
	KeepThinking bool `yaml:"keep_thinking"`
}

// CollapseConfig 折叠存档配置 (Phase 3, D-12)
type CollapseConfig struct {
	// Enabled 是否启用折叠（默认 true）
	Enabled bool `yaml:"enabled"`
	// ThresholdMultiplier token 超过此倍率 x token_threshold 时触发折叠（默认 3.0）
	ThresholdMultiplier float64 `yaml:"threshold_multiplier"`
	// CompactEnabled 是否启用连续 Stage-3 桩消息合并（Phase C，默认 true）
	CompactEnabled bool `yaml:"compact_enabled"`
}

// FrozenConfig frozen prefix 配置 (Phase 4, D-13)
type FrozenConfig struct {
	// Enabled 是否启用 frozen prefix（默认 true）
	Enabled bool `yaml:"enabled"`
	// TTLMinutes 保留用于旧配置兼容；实际 TTL 由 cache_ttl 推导。
	TTLMinutes int `yaml:"ttl_minutes"`
}

// CacheConfig cache_control 管理配置 (Phase 4, D-13)
type CacheConfig struct {
	// Enabled 是否启用 cache_control 管理（默认 true）
	Enabled bool `yaml:"enabled"`
	// BreakpointLimit 最大 cache_control breakpoint 数（Anthropic 硬限制 4）
	BreakpointLimit int `yaml:"breakpoint_limit"`
	// CacheTTL 缓存 TTL 策略："ephemeral"（默认 5m）或 "1h"
	CacheTTL string `yaml:"cache_ttl"`
}

// ServerConfig HTTP 服务端配置
type ServerConfig struct {
	// Port 监听端口
	Port int `yaml:"port"`
	// Host 监听地址
	Host string `yaml:"host"`
}

// Config 顶层配置，包含 server、proxy、transport、debug、stubify、collapse、frozen、cache 分组
type Config struct {
	Server    ServerConfig    `yaml:"server"`
	Proxy     ProxyConfig     `yaml:"proxy"`
	Transport TransportConfig `yaml:"transport"`
	Debug     DebugConfig     `yaml:"debug"`
	Stubify   StubifyConfig   `yaml:"stubify"`
	Collapse  CollapseConfig  `yaml:"collapse"`
	Frozen    FrozenConfig    `yaml:"frozen"` // Phase 4: frozen prefix 配置 (D-13)
	Cache     CacheConfig     `yaml:"cache"`  // Phase 4: cache_control 管理配置 (D-13)
}

// DefaultConfig 返回所有字段均已设置默认值的 Config。
func DefaultConfig() Config {
	return Config{
		Server: ServerConfig{
			Port: 9099,
			Host: "localhost",
		},
		Proxy: ProxyConfig{
			Target:    "https://api.anthropic.com",
			Deflation: 0.7,
		},
		Transport: TransportConfig{
			DialTimeout:            15 * time.Second,
			TLSHandshakeTimeout:    15 * time.Second,
			StreamHeaderTimeout:    10 * time.Minute,
			NonStreamHeaderTimeout: 30 * time.Minute,
			ResponseIdleTimeout:    10 * time.Minute,
			HardTimeout:            60 * time.Minute,
		},
		Debug: DebugConfig{
			Enabled:  false,
			FullBody: false,
			DataDir:  "./data/",
		},
		Stubify: StubifyConfig{
			TokenThreshold: 100000, // D-04 默认值（1M 模型建议 500000+）
			KeepRecent:     8,      // 对标 YesMem（默认 10），放宽自硬编码 4
			KeepThinking:   false,  // 默认不保留 thinking，保持向后兼容
		},
		Collapse: CollapseConfig{
			Enabled:             true,
			ThresholdMultiplier: 3.0,
			CompactEnabled:      true,
		},
		Frozen: FrozenConfig{
			Enabled:    true,
			TTLMinutes: 30,
		},
		Cache: CacheConfig{
			Enabled:         true,
			BreakpointLimit: 4,
			CacheTTL:        "ephemeral",
		},
	}
}

// Server 代理服务核心结构体，持有配置和 HTTP 客户端。
// Phase 2 加入 TokenCounter 和 DecayMgr（D-01, D-11）。
// Phase 3 添加 SQLite。
// Phase 4 添加 FrozenStubs 和 SawtoothTrigger。
type Server struct {
	Config            Config
	HTTPClient        *http.Client
	TokenCounter      *TokenCounter    // Phase 2: token 计数单例 (D-01)
	DecayTracker      *DecayTracker    // Phase B: per-message decay tracking
	Store             *SQLiteStore     // Phase 3: SQLite 持久化 (D-14)
	Frozen            *FrozenStubs     // Phase 4: frozen prefix 存储 (D-12)
	Sawtooth          *SawtoothTrigger // Phase 4: 桩化周期触发 (D-03)
	EagerStub         *EagerStubMemory // Phase 5: eager stub memory (EAGER-01)
	cacheMu           sync.Mutex       // 保护 cachedTTL 的比较与下游 TTL 更新
	cachedTTL         string           // 当前生效的 cache TTL（"ephemeral" 或 "1h"），用于检测切换
	searchAndExpandFn func([]Message, *SQLiteStore, int, *TokenCounter, *Budget, *requestMeta) RecallOutcome
	requestIdx        atomic.Uint64
}

func (s *Server) nextRequestMeta(requestSessionID string) *requestMeta {
	return newRequestMeta(s.requestIdx.Add(1), requestSessionID)
}

// NewServer 创建代理服务实例。
// 若 Debug.Enabled 且 DataDir 非空，自动创建数据目录。
func NewServer(cfg Config) *Server {
	s := &Server{
		Config:     cfg,
		HTTPClient: newUpstreamHTTPClient(cfg.Transport),
	}

	// 调试模式下创建数据目录
	if cfg.Debug.Enabled && cfg.Debug.DataDir != "" {
		if err := os.MkdirAll(cfg.Debug.DataDir, 0755); err != nil {
			slog.Warn("无法创建 debug 数据目录", "path", cfg.Debug.DataDir, "error", err)
		}
	}

	return s
}

// validateConfig 验证配置值合法性，非法值回退到默认值。
// 对应威胁模型 T-01-01。
func validateConfig(cfg *Config) {
	// 端口范围校验 (1-65535)
	if cfg.Server.Port < 1 || cfg.Server.Port > 65535 {
		slog.Warn("非法端口值，回退到默认值", "port", cfg.Server.Port, "default", 9099)
		cfg.Server.Port = 9099
	}

	// host 非空校验
	if cfg.Server.Host == "" {
		slog.Warn("host 为空，回退到默认值", "default", "localhost")
		cfg.Server.Host = "localhost"
	}

	// deflation 范围校验 (0.0-1.0)
	if cfg.Proxy.Deflation < 0.0 || cfg.Proxy.Deflation > 1.0 {
		slog.Warn("非法 deflation 值，回退到默认值", "deflation", cfg.Proxy.Deflation, "default", 0.7)
		cfg.Proxy.Deflation = 0.7
	}

	// target 非空校验
	if cfg.Proxy.Target == "" {
		slog.Warn("target 为空，回退到默认值", "default", "https://api.anthropic.com")
		cfg.Proxy.Target = "https://api.anthropic.com"
	}

	transportDefaults := DefaultConfig().Transport
	if cfg.Transport.DialTimeout < 0 {
		slog.Warn("非法 dial_timeout，回退到默认值", "value", cfg.Transport.DialTimeout, "default", transportDefaults.DialTimeout)
		cfg.Transport.DialTimeout = transportDefaults.DialTimeout
	}
	if cfg.Transport.TLSHandshakeTimeout < 0 {
		slog.Warn("非法 tls_handshake_timeout，回退到默认值", "value", cfg.Transport.TLSHandshakeTimeout, "default", transportDefaults.TLSHandshakeTimeout)
		cfg.Transport.TLSHandshakeTimeout = transportDefaults.TLSHandshakeTimeout
	}
	if cfg.Transport.StreamHeaderTimeout < 0 {
		slog.Warn("非法 stream_header_timeout，回退到默认值", "value", cfg.Transport.StreamHeaderTimeout, "default", transportDefaults.StreamHeaderTimeout)
		cfg.Transport.StreamHeaderTimeout = transportDefaults.StreamHeaderTimeout
	}
	if cfg.Transport.NonStreamHeaderTimeout < 0 {
		slog.Warn("非法 non_stream_header_timeout，回退到默认值", "value", cfg.Transport.NonStreamHeaderTimeout, "default", transportDefaults.NonStreamHeaderTimeout)
		cfg.Transport.NonStreamHeaderTimeout = transportDefaults.NonStreamHeaderTimeout
	}
	if cfg.Transport.ResponseIdleTimeout < 0 {
		slog.Warn("非法 response_idle_timeout，回退到默认值", "value", cfg.Transport.ResponseIdleTimeout, "default", transportDefaults.ResponseIdleTimeout)
		cfg.Transport.ResponseIdleTimeout = transportDefaults.ResponseIdleTimeout
	}
	if cfg.Transport.HardTimeout < 0 {
		slog.Warn("非法 hard_timeout，回退到默认值", "value", cfg.Transport.HardTimeout, "default", transportDefaults.HardTimeout)
		cfg.Transport.HardTimeout = transportDefaults.HardTimeout
	}

	// stubify 校验 (Phase 2, D-04)
	if cfg.Stubify.TokenThreshold < 1000 {
		slog.Warn("非法 token_threshold 值，回退到默认值", "token_threshold", cfg.Stubify.TokenThreshold, "default", 100000)
		cfg.Stubify.TokenThreshold = 100000
	}

	// Collapse 校验 (Phase 3, D-12)
	if cfg.Collapse.ThresholdMultiplier < 1.0 {
		slog.Warn("非法 collapse threshold_multiplier 值，回退到默认值",
			"threshold_multiplier", cfg.Collapse.ThresholdMultiplier, "default", 3.0)
		cfg.Collapse.ThresholdMultiplier = 3.0
	}

	// frozen 校验 (Phase 4, D-13)
	if cfg.Frozen.TTLMinutes < 1 {
		slog.Warn("非法 frozen ttl_minutes 值，回退到默认值",
			"ttl_minutes", cfg.Frozen.TTLMinutes, "default", 30)
		cfg.Frozen.TTLMinutes = 30
	}

	// cache 校验 (Phase 4, D-13)
	if cfg.Cache.BreakpointLimit < 1 || cfg.Cache.BreakpointLimit > 4 {
		slog.Warn("非法 cache breakpoint_limit 值，回退到默认值",
			"breakpoint_limit", cfg.Cache.BreakpointLimit, "default", 4)
		cfg.Cache.BreakpointLimit = 4
	}
	switch strings.TrimSpace(cfg.Cache.CacheTTL) {
	case "", "ephemeral":
		cfg.Cache.CacheTTL = "ephemeral"
	case "1h":
		cfg.Cache.CacheTTL = "1h"
	default:
		slog.Warn("非法 cache_ttl 值，回退到默认值",
			"cache_ttl", cfg.Cache.CacheTTL, "default", "ephemeral")
		cfg.Cache.CacheTTL = "ephemeral"
	}
}

// LoadConfig 从 YAML 文件加载配置。
// 以 DefaultConfig() 为基础，文件中的值覆盖对应字段。
// 若 path 为空或文件不存在，返回默认配置。
func LoadConfig(path string) (Config, error) {
	cfg := DefaultConfig()

	if path == "" {
		return cfg, nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			slog.Info("配置文件不存在，使用默认配置", "path", path)
			return cfg, nil
		}
		return cfg, fmt.Errorf("读取配置文件 %s 失败: %w", path, err)
	}

	// 将文件内容反序列化到 cfg 上，覆盖非零值
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return cfg, fmt.Errorf("解析配置文件 %s 失败: %w", path, err)
	}

	// T-01-01 威胁缓解：验证配置值
	validateConfig(&cfg)

	return cfg, nil
}

// pressureSource 表示当前请求最终采用的压力尺度。
type pressureSource string

const (
	pressureSourceLocalFull       pressureSource = "local_full"
	pressureSourceActualPlusDelta pressureSource = "actual_plus_delta"
)

// pressureDecision 保存请求进入有状态管线前的一次性压力决定。
// 该值只包含计数、固定长度指纹和受限枚举，不保存请求正文。
type pressureDecision struct {
	Available                bool
	MessagesLocalTokens      int
	SystemLocalTokens        int
	ToolsLocalTokens         int
	FullLocalEstimate        int
	PreviousActual           int
	PreviousMessageCount     int
	NewMessageDelta          int
	SelectedPressure         int
	Source                   pressureSource
	ResetReason              baselineResetReason
	TriggerReason            TriggerReason
	Threshold                int
	MessageCount             int
	SystemFingerprint        string
	ToolsFingerprint         string
	SystemFingerprintChanged bool
	ToolsFingerprintChanged  bool
	CompressDecision         bool
}

type topLevelMeasurement struct {
	tokens      int
	fingerprint string
}

// canonicalizeTopLevelJSON 将合法顶层 JSON 解码为通用值后重新编码，
// 使 map key 顺序不影响计数与指纹。缺失和显式 null 保持不同语义。
func canonicalizeTopLevelJSON(raw json.RawMessage) (canonical []byte, present bool, explicitNull bool) {
	if len(bytes.TrimSpace(raw)) == 0 {
		return nil, false, false
	}
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, false, false
	}
	canonical, err := json.Marshal(value)
	if err != nil {
		return nil, false, false
	}
	return canonical, true, value == nil
}

func inspectTopLevelJSON(raw json.RawMessage, tc *TokenCounter) topLevelMeasurement {
	canonical, present, explicitNull := canonicalizeTopLevelJSON(raw)
	if !present {
		return topLevelMeasurement{fingerprint: sha256hex([]byte("missing"))}
	}
	measurement := topLevelMeasurement{fingerprint: sha256hex(canonical)}
	if !explicitNull && tc != nil {
		measurement.tokens = tc.CountTokens(string(canonical))
	}
	return measurement
}

// measureTopLevelTokens 计算顶层 system/tools 的规范 JSON token；缺失或 null 为 0。
func measureTopLevelTokens(raw json.RawMessage, tc *TokenCounter) int {
	return inspectTopLevelJSON(raw, tc).tokens
}

// fingerprintTopLevelJSON 返回规范 JSON 的 SHA-256 指纹。
// 缺失使用固定 sentinel，显式 null 使用规范字节 "null"，因此两者稳定区分。
func fingerprintTopLevelJSON(raw json.RawMessage) string {
	return inspectTopLevelJSON(raw, nil).fingerprint
}

// buildPressureDecision 在 local_full 与 actual_plus_delta 中只选择一次。
// production 调用对 system/tools 各规范化一次，同时保存 full estimate 作为误差证据。
func buildPressureDecision(messages []Message, systemRaw, toolsRaw json.RawMessage, baseline pressureBaseline, tc *TokenCounter, threshold int) pressureDecision {
	system := inspectTopLevelJSON(systemRaw, tc)
	tools := inspectTopLevelJSON(toolsRaw, tc)
	messagesTokens := 0
	if tc != nil {
		messagesTokens = tc.CountMessagesTokens(messages)
	}
	fullEstimate := saturatingAdd(saturatingAdd(messagesTokens, system.tokens), tools.tokens)
	decision := pressureDecision{
		Available:            tc != nil,
		MessagesLocalTokens:  messagesTokens,
		SystemLocalTokens:    system.tokens,
		ToolsLocalTokens:     tools.tokens,
		FullLocalEstimate:    fullEstimate,
		PreviousActual:       baseline.ActualTokens,
		PreviousMessageCount: baseline.MessageCount,
		SelectedPressure:     fullEstimate,
		Source:               pressureSourceLocalFull,
		ResetReason:          baselineResetNoActual,
		Threshold:            threshold,
		MessageCount:         len(messages),
		SystemFingerprint:    system.fingerprint,
		ToolsFingerprint:     tools.fingerprint,
	}

	baselineValid := baseline.Available && baseline.ActualTokens > 0 && baseline.MessageCount >= 0 &&
		validPressureFingerprint(baseline.SystemFingerprint) && validPressureFingerprint(baseline.ToolsFingerprint)
	if !baselineValid {
		return decision
	}
	decision.SystemFingerprintChanged = baseline.SystemFingerprint != system.fingerprint
	decision.ToolsFingerprintChanged = baseline.ToolsFingerprint != tools.fingerprint
	if baseline.MessageCount > len(messages) {
		decision.ResetReason = baselineResetMessageShrink
		return decision
	}
	if decision.SystemFingerprintChanged {
		decision.ResetReason = baselineResetSystemChanged
		return decision
	}
	if decision.ToolsFingerprintChanged {
		decision.ResetReason = baselineResetToolsChanged
		return decision
	}

	delta := 0
	if tc != nil {
		delta = tc.CountMessagesTokens(messages[baseline.MessageCount:])
	}
	decision.NewMessageDelta = delta
	decision.SelectedPressure = saturatingAdd(baseline.ActualTokens, delta)
	decision.Source = pressureSourceActualPlusDelta
	decision.ResetReason = baselineResetNone
	return decision
}

// HandleMessages 处理 POST /v1/messages 请求。
// 管线顺序: parse -> FrozenStubs.Get -> Reexpand -> CompressContext -> CalcCollapseCutoff
//
//	-> PRIMARY: collapse -> EagerStub -> cache -> orphan -> forwardRaw
//	-> FALLBACK: stubify -> decay -> compact -> SawtoothTrigger+FrozenStubs.Store -> EagerStub -> cache -> orphan -> forwardRaw
//
// (D-01/D-02/D-09/D-10/D-11, Phase F collapse-first)
func (s *Server) HandleMessages(w http.ResponseWriter, r *http.Request) {
	sessionID := extractSessionID(r)
	meta := s.nextRequestMeta(sessionID)

	requestSeq := 0

	// Phase 2+: stubify + decay + collapse + frozen + cache 管线
	// 仅在 TokenCounter 和 DecayTracker 已初始化时执行
	if s.TokenCounter != nil && s.DecayTracker != nil {
		const maxBodySize = 10 * 1024 * 1024 // 10 MB
		limitedReader := io.LimitReader(r.Body, maxBodySize+1)
		body, err := io.ReadAll(limitedReader)
		r.Body.Close()
		if err != nil {
			meta.Logger.Error("读取请求体失败", "error", err)
			http.Error(w, "Failed to read request body", http.StatusInternalServerError)
			return
		}
		if len(body) > maxBodySize {
			meta.Logger.Warn("请求体超限", "size", len(body))
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusRequestEntityTooLarge)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "Request Entity Too Large"})
			return
		}
		rawTimestamp := time.Now()
		rawModel := extractModelFromBody(body)
		rawMessageCount := countMessages(body)
		meta.logEntry(rawModel, rawMessageCount)
		s.writeRequestDebugFacts(meta, rawTimestamp, debugStageRawInbound, body, r)
		s.writeFullBodyDebug(meta, rawTimestamp, debugBodyStageRawInbound, body, r.Header, rawModel, rawMessageCount)

		// 解析 Anthropic messages API 请求体
		// 使用 map[string]json.RawMessage 保留所有原始字段（tools/thinking/tool_choice
		// 等），避免 json.Marshal 重建 body 时静默丢弃未映射的字段。
		var bodyMap map[string]json.RawMessage
		if err := json.Unmarshal(body, &bodyMap); err != nil {
			meta.Logger.Warn("无法解析请求体 JSON，跳过管线处理", "error", err)
			r.Body = io.NopCloser(bytes.NewReader(body))
			s.forwardRaw(w, r, meta)
			return
		}

		// 从 bodyMap 中提取 messages 数组
		msgData, ok := bodyMap["messages"]
		if !ok {
			meta.Logger.Warn("请求体中缺少 messages 字段，原样转发")
			r.Body = io.NopCloser(bytes.NewReader(body))
			s.forwardRaw(w, r, meta)
			return
		}
		var messages []Message
		if err := json.Unmarshal(msgData, &messages); err != nil {
			meta.Logger.Warn("无法解析 messages 数组，原样转发", "error", err)
			r.Body = io.NopCloser(bytes.NewReader(body))
			s.forwardRaw(w, r, meta)
			return
		}

		auxiliary := classifyAuxiliaryRequest(bodyMap, messages)
		if auxiliary.Kind == requestKindSessionTitle {
			meta.RequestKind = auxiliary.Kind
			logAuxiliaryClassification(meta.auxiliaryLogger(), auxiliary, len(messages))
			r.Body = io.NopCloser(bytes.NewReader(body))
			s.forwardRaw(w, r, meta)
			return
		}

		features := extractAgentRequestFeatures(r, bodyMap, messages)
		logAgentRequestFeatures(meta.Logger, features)
		classification := classifyAgentRequest(r, bodyMap, messages)
		meta.AgentRole = classification.Role
		meta.AgentReason = classification.Reason
		rawMessages := messages
		historyMessages, currentContext := DetachPersistentUserContext(rawMessages)
		finalizeMessages := func(history []Message) []Message {
			return PrependPersistentUserContext(history, currentContext)
		}
		// 所有结构化管线出口只通过此 finalizer 重附加本轮权威 context。
		rebuildBody := func(history []Message) ([]byte, error) {
			data, err := json.Marshal(finalizeMessages(history))
			if err != nil {
				return nil, err
			}
			bodyMap["messages"] = data
			return json.Marshal(bodyMap)
		}

		// 可靠子代理必须在 Frozen、Archive、压缩和持久化副作用前透明返回。
		// agentContext.parentSessionId 只参与分类，绝不作为另一个 session 的状态键。
		if classification.Role == agentRoleSubagent {
			meta.Logger.Info("Agent 请求安全直通",
				"model", extractModelFromBody(body),
				"message_count", len(rawMessages),
				"agent_role", classification.Role,
				"agent_reason", classification.Reason,
				LogDim,
			)
			newBody, err := rebuildBody(historyMessages)
			if err != nil {
				meta.Logger.Warn("Agent 请求体透明重建失败，回退原样转发", "error", err)
				newBody = body
			}
			r.Body = io.NopCloser(bytes.NewReader(newBody))
			s.forwardRaw(w, r, meta)
			return
		}

		// Phase B: 只有进入有状态主请求管线时才递增请求序号（DecayTracker 用）。
		if s.Sawtooth != nil {
			requestSeq = s.Sawtooth.IncrementRequestSeq(sessionID)
		}
		threshold := s.Config.Stubify.TokenThreshold
		baseline := pressureBaseline{}
		if s.Sawtooth != nil {
			baseline = s.Sawtooth.PressureBaseline(sessionID)
		}
		decision := buildPressureDecision(rawMessages, bodyMap["system"], bodyMap["tools"], baseline, s.TokenCounter, threshold)
		if s.Sawtooth != nil {
			decision.TriggerReason = s.Sawtooth.ShouldTrigger(sessionID, decision.SelectedPressure)
		} else if decision.SelectedPressure > threshold {
			decision.TriggerReason = TriggerTokens
		}
		decision.CompressDecision = decision.TriggerReason != TriggerNone
		meta.PressureDecision = decision
		messages = historyMessages

		// Phase 4 Step 0: 保存原始 token 估算和消息数（SawtoothTrigger + frozen cutoff）
		// rawCutoff 必须在 reexpand 前保存——reexpand 会注入 archive block 增加消息数
		// 对标 YesMem: cutoff := len(messages) 使用原始（未修改）消息数
		rawEstimate := decision.MessagesLocalTokens
		rawCutoff := len(messages)
		historyEstimate := s.TokenCounter.CountMessagesTokens(messages)
		contextTokens := rawEstimate - historyEstimate
		if contextTokens < 0 {
			contextTokens = 0
		}

		// Phase 6 Step 0: StripReminders (REMIND-04) — 在 Frozen.Get / SearchAndExpand 之前
		// 移除旧消息中过期的 system-reminder / skill-hint，使 frozen prefix 的 boundary hash
		// 与 reexpand 的关键词搜索都基于已清理消息。strip 不增删消息，rawCutoff 不受影响。
		messages = StripReminders(messages)

		// 保存 stripped 原始消息副本——frozen prefix 失效时需从原始消息重新压缩
		// 对标 YesMem: messages 不被覆盖；frozen 有效时走快速路径，失效时从原始消息重新压缩
		originalMessages := make([]Message, len(messages))
		copy(originalMessages, messages)

		// 保存 boundary 消息——用于 frozen prefix 验证（检测用户撤回/编辑）
		// boundary = 原始请求中 frozen prefix 覆盖范围内的最后一条消息
		var rawBoundary Message
		if rawCutoff > 0 && rawCutoff <= len(originalMessages) {
			boundaryCopy := deepCopyMessages(originalMessages[rawCutoff-1 : rawCutoff])
			if len(boundaryCopy) == 1 {
				rawBoundary = boundaryCopy[0]
			}
		}

		// Phase 4 Step 1: Frozen prefix retrieval (D-01)
		var frozenRawCutoff int
		var frozenPrefixLen int
		var frozenTokens int // YesMem shouldInvalidateFrozen: 存储 frozen prefix 的 token 估算
		if s.Frozen != nil {
			result := s.Frozen.GetWithLogger(meta.Logger, sessionID, messages)
			if result != nil {
				if result.Cutoff <= 0 || result.Cutoff > len(messages) {
					meta.Logger.Warn("frozen cutoff 非法，忽略状态", "cutoff", result.Cutoff, "message_count", len(messages))
					s.Frozen.InvalidateWithLogger(meta.Logger, sessionID)
					result = nil
				}
			}
			if result != nil {
				frozenRawCutoff = result.Cutoff
				frozenPrefixLen = len(result.Messages)
				frozenTokens = result.Tokens
				messages = append(result.Messages, messages[result.Cutoff:]...)
				meta.Logger.Info("frozen prefix 命中并拼接",
					"raw_cutoff", result.Cutoff,
					"frozen_prefix_len", frozenPrefixLen,
					"frozen_tokens", result.Tokens,
					LogGreen,
				)
			}
		}

		// 先基于 frozen+tail 判定 frozen 是否仍有效，确定本请求唯一的权威基础消息。
		totalTokens := contextTokens + s.TokenCounter.CountMessagesTokens(messages)
		pressureTrigger := decision.TriggerReason

		// ── YesMem shouldInvalidateFrozen ──
		// frozen prefix 存在时，若 frozen+tail 仍在阈值内则跳过压缩管线。
		// 对标 YesMem proxy.go:1230: shouldInvalidateFrozen(combinedTokens, threshold)
		needCompress := decision.CompressDecision
		if frozenPrefixLen > 0 && needCompress {
			// 用原始消息切片计算 tail tokens——防止 frozen prefix 替换后
			// len(messages) < raw cutoff 导致切片越界
			var tailTokens int
			if frozenRawCutoff <= len(originalMessages) {
				tailTokens = s.TokenCounter.CountMessagesTokens(originalMessages[frozenRawCutoff:])
			}
			combinedTokens := contextTokens + frozenTokens + tailTokens
			if combinedTokens <= threshold && pressureTrigger == TriggerNone {
				needCompress = false
				meta.Logger.Info("frozen prefix 仍在阈值内，跳过压缩",
					"frozen_tokens", frozenTokens,
					"tail_tokens", tailTokens,
					"combined_tokens", combinedTokens,
					"threshold", threshold,
					LogLightGreen,
				)
			} else {
				meta.Logger.Info("frozen prefix 不足，重新压缩",
					"frozen_tokens", frozenTokens,
					"tail_tokens", tailTokens,
					"combined_tokens", combinedTokens,
					"threshold", threshold,
					LogGreen,
				)
				// frozen prefix 失效——清除并从原始消息重新压缩
				// 对标 YesMem: frozen=nil 后 runStubCycle(messages) 使用原始未压缩消息
				s.Frozen.InvalidateWithLogger(meta.Logger, sessionID)
				messages = originalMessages
				frozenRawCutoff = 0
				frozenPrefixLen = 0
				// 重新计算 totalTokens（基于原始未压缩消息）
				totalTokens = contextTokens + s.TokenCounter.CountMessagesTokens(messages)
			}
		}

		// Frozen 最终有效性确定后，每个请求只执行一次 Archive 搜索与注入。
		// RecallOutcome 保存在请求局部变量中，后续日志可直接复用最终结果。
		recallOutcome := RecallOutcome{Messages: messages}
		if s.Store != nil {
			reExpandBudget := NewBudget(threshold)
			recallOutcome = s.searchAndExpand(messages, reExpandBudget, meta)
			messages = recallOutcome.Messages
		}

		// 召回后的消息才是最终压缩输入；Frozen 失效分支不再重跑召回。
		totalTokens = contextTokens + s.TokenCounter.CountMessagesTokens(messages)
		needCompress = decision.CompressDecision
		if needCompress {
			// 提取 pivot text：使用最新一条 user 消息的内容文本
			pivotText := extractLatestUserText(messages)

			// 保存原始消息副本，供 buildArchiveBlock 使用（桩化前内容）
			originalMessages := make([]Message, len(messages))
			copy(originalMessages, messages)

			// Phase A: CompressContext — 预压缩 keepRecent 外的 thinking block 和
			// 超过 500 token 的 tool_result block，回收已被模型"消化"的上下文空间。
			var compressResult CompressResult
			messages, compressResult = CompressContext(messages, s.Config.Stubify.KeepRecent, s.TokenCounter)
			if compressResult.ThinkingCompressed > 0 || compressResult.ToolResultsCompressed > 0 {
				meta.Logger.Debug("compress_context 完成",
					"thinkingCompressed", compressResult.ThinkingCompressed,
					"toolResultsCompressed", compressResult.ToolResultsCompressed,
					"tokensSaved", compressResult.TokensSaved,
				)
				// 更新 totalTokens，使下游衰减/折叠决策反映压缩后的实际压力
				totalTokens -= compressResult.TokensSaved
				if totalTokens < 0 {
					totalTokens = 0
				}
			}

			// ================================================================
			// Phase F: Collapse-First（对标 YesMem proxy_stub.go:56-280）
			// 在 CompressContext 之后、stubify 之前计算 cutoff。
			// cutoff > 0 → collapse 为主路径
			// cutoff <= 0 → stubify+decay 为 fallback
			// ================================================================
			tokenFloor := threshold / 2
			if tokenFloor < 10000 {
				tokenFloor = 10000
			}
			cutoffIdx := CalcCollapseCutoff(messages, tokenFloor, s.TokenCounter, s.Config.Stubify.KeepRecent)
			if cutoffIdx >= len(messages) {
				meta.Logger.Warn("collapse cutoff 越界，回退到 stubify",
					"cutoff", cutoffIdx, "message_count", len(messages))
				cutoffIdx = -1
			}

			if cutoffIdx > 0 {
				// ── PRIMARY PATH: Collapse ──

				// 折叠消息：266 条 → blank[0] + archive block[1] + tail[cutoffIdx:]
				// CollapseOldMessages 内部调用 buildArchiveBlock，返回 (messages, archiveBlock)
				collapsedMessages, archiveBlock := CollapseOldMessages(
					messages,         // CompressContext 后的消息（modified）
					originalMessages, // 桩化前的原始消息（供 archive 提取完整摘要）
					cutoffIdx,
					s.TokenCounter,
					sessionID,
				)
				if archiveBlock.ID == "" {
					meta.Logger.Warn("collapse 未生成有效存档，回退到 stubify",
						"cutoff", cutoffIdx, "message_count", len(messages))
				} else {

					// 持久化到 SQLite（graceful degradation：失败不阻断请求）
					if s.Store != nil {
						if err := s.Store.SaveArchive(archiveBlock); err != nil {
							meta.Logger.Error("保存存档失败", "error", err)
						}
					}

					meta.Logger.Info("collapse 完成",
						"before", len(messages),
						"after", len(collapsedMessages),
						"cutoff", cutoffIdx,
						"archived_tokens", archiveBlock.EstimatedTokens,
						LogGreen,
					)

					// DecayTracker: 清理被折叠的旧消息索引。
					// 折叠后消息数组完全重建（indices 0:blank, 1:archive, 2+:tail），
					// 旧 indices 不再有效。
					if s.DecayTracker != nil {
						s.DecayTracker.ClearSession(sessionID)
					}

					// 重建请求体
					messages = collapsedMessages

					// Phase 5: Eager stubbing（collapse 后 tail 中的大 tool_result 仍需处理）
					if s.EagerStub != nil {
						freshStubs := 0
						stickyHits := 0
						messagesAny := messagesToAny(messages)
						messagesAny = EagerStubToolResults(messagesAny, 0,
							func(text string) int { return s.TokenCounter.CountTokens(text) },
							WithStubMemory(s.EagerStub, sessionID),
							WithStubCounters(&stickyHits, &freshStubs),
						)
						messages = anyToMessages(messagesAny)
						if freshStubs > 0 || stickyHits > 0 {
							meta.Logger.Info("eager stub 完成（collapse 路径）",
								"fresh_stubs", freshStubs,
								"sticky_hits", stickyHits,
								LogGreen,
							)
						}
					}

					// Orphan repair
					repaired, orphans := validateToolPairs(messages)
					if orphans > 0 {
						meta.Logger.Warn("修复 orphan tool_use/tool_result 对",
							"orphans_removed", orphans,
						)
						messages = repaired
					}

					if s.Sawtooth != nil && s.Frozen != nil {
						if pressureTrigger != TriggerNone {
							meta.Logger.Info("SawtoothTrigger 触发",
								"reason", pressureTrigger,
								"raw_estimate", rawEstimate,
								LogGreen,
							)
							frozenPrefixLen = len(messages)
							s.applyCacheControl(messages, frozenPrefixLen, sessionID)
							compressedTokens := s.TokenCounter.CountMessagesTokens(messages)
							s.Frozen.StoreWithLogger(meta.Logger, sessionID, messages, rawCutoff, rawBoundary, compressedTokens, rawEstimate)
						}
					}

					newBody, err := rebuildBody(messages)
					if err != nil {
						meta.Logger.Error("重建折叠后请求体失败", "error", err)
						http.Error(w, "Internal Server Error", http.StatusInternalServerError)
						return
					}

					r.Body = io.NopCloser(bytes.NewReader(newBody))
					s.forwardRaw(w, r, meta)
					return // collapse 路径在此结束
				}
			}

			// ── FALLBACK PATH: stubify+decay（cutoffIdx <= 0，对标 YesMem StubifyWithTotal） ──
			// 以下代码与当前 stubify+decay+compact 流程完全一致，不变。

			// 步骤 1: stubify（保护 messages[0] 和最近 4 条消息）
			intensity := estimateIntensity(messages)
			stubbedMessages, stats := stubifyMessages(messages, s.TokenCounter, pivotText, s.Config.Stubify.KeepRecent, s.Config.Stubify.KeepThinking, s.DecayTracker, sessionID, requestSeq, intensity, threshold)

			// Phase B: 提取 pinnedPaths
			pinnedPaths := extractPinnedPaths(messages)
			s.DecayTracker.SetPinnedPaths(pinnedPaths)

			// 步骤 2: decay
			decayedMessages, phase := s.DecayTracker.ApplyDecayBatch(stubbedMessages, sessionID, totalTokens, threshold, s.TokenCounter, pivotText, requestSeq)

			meta.Logger.Info("stubify+decay 完成",
				"original_tokens", stats.OriginalTokens,
				"stubbed_tokens", stats.StubbedTokens,
				"messages_stubbed", stats.MessagesStubbed,
				"thinking_removed", stats.ThinkingRemoved,
				"tools_processed", stats.ToolsProcessed,
				"decay_phase", phase,
				"pressure", fmt.Sprintf("%.2f", float64(totalTokens)/float64(threshold)),
				LogGreen,
			)

			// Phase C: CompactMessages
			if s.Config.Collapse.CompactEnabled && s.DecayTracker != nil {
				pressure := float64(totalTokens) / float64(threshold)
				beforeCompact := len(decayedMessages)
				var compactedBlocks []CompactedBlock
				decayedMessages, compactedBlocks = CompactMessages(decayedMessages, originalMessages, s.DecayTracker, sessionID, requestSeq, pressure)
				if len(compactedBlocks) > 0 {
					meta.Logger.Info("compact 完成",
						"before", beforeCompact,
						"after", len(decayedMessages),
						"blocks", len(compactedBlocks),
						LogGreen,
					)
				}
			}

			messages = decayedMessages
			if s.EagerStub != nil {
				freshStubs := 0
				stickyHits := 0
				messagesAny := messagesToAny(messages)
				messagesAny = EagerStubToolResults(messagesAny, frozenPrefixLen,
					func(text string) int { return s.TokenCounter.CountTokens(text) },
					WithStubMemory(s.EagerStub, sessionID),
					WithStubCounters(&stickyHits, &freshStubs),
				)
				messages = anyToMessages(messagesAny)
				if freshStubs > 0 || stickyHits > 0 {
					meta.Logger.Info("eager stub 完成（超阈值无折叠）",
						"fresh_stubs", freshStubs,
						"sticky_hits", stickyHits,
					)
				}
			}
			// Orphan repair
			repaired, orphans := validateToolPairs(messages)
			if orphans > 0 {
				meta.Logger.Warn("修复 orphan tool_use/tool_result 对",
					"orphans_removed", orphans,
				)
				messages = repaired
			}

			// fallback 与 collapse 路径保持项目 Phase 07.1 的 snapshot 契约：
			// 持久化的 prefix bytes 与本次实际上游 wire prefix 完全一致。
			if s.Sawtooth != nil && s.Frozen != nil {
				if pressureTrigger != TriggerNone {
					meta.Logger.Info("SawtoothTrigger 触发",
						"reason", pressureTrigger,
						"raw_estimate", rawEstimate,
						LogGreen,
					)
					frozenPrefixLen = len(messages)
					s.applyCacheControl(messages, frozenPrefixLen, sessionID)
					compressedTokens := s.TokenCounter.CountMessagesTokens(messages)
					s.Frozen.StoreWithLogger(meta.Logger, sessionID, messages, rawCutoff, rawBoundary, compressedTokens, rawEstimate)
				} else {
					s.applyCacheControl(messages, frozenPrefixLen, sessionID)
				}
			} else {
				s.applyCacheControl(messages, frozenPrefixLen, sessionID)
			}

			newBody, err := rebuildBody(messages)
			if err != nil {
				meta.Logger.Error("重建请求体失败", "error", err)
				http.Error(w, "Internal Server Error", http.StatusInternalServerError)
				return
			}

			// 替换 r.Body 为处理后的内容——forwardRaw 透明读取
			r.Body = io.NopCloser(bytes.NewReader(newBody))

		} else {
			// Phase 5: 每个主代理请求都主动清理已被后续 assistant 消化的旧 tool_result。
			if s.EagerStub != nil {
				freshStubs := 0
				stickyHits := 0
				messagesAny := messagesToAny(messages)
				messagesAny = EagerStubToolResults(messagesAny, frozenPrefixLen,
					func(text string) int { return s.TokenCounter.CountTokens(text) },
					WithStubMemory(s.EagerStub, sessionID),
					WithStubCounters(&stickyHits, &freshStubs),
				)
				messages = anyToMessages(messagesAny)
				if freshStubs > 0 || stickyHits > 0 {
					meta.Logger.Info("eager stub 完成（低于阈值）",
						"fresh_stubs", freshStubs,
						"sticky_hits", stickyHits,
					)
				}
			}

			// Phase 5: Orphan repair (ORPHAN-02)
			repaired, orphans := validateToolPairs(messages)
			if orphans > 0 {
				meta.Logger.Warn("修复 orphan tool_use/tool_result 对",
					"orphans_removed", orphans,
				)
				messages = repaired
			}

			// Eager 只改 fresh tail；在它和 pair repair 之后统一处理 frozen boundary。
			s.applyCacheControl(messages, frozenPrefixLen, sessionID)
			if newBody, err := rebuildBody(messages); err == nil {
				r.Body = io.NopCloser(bytes.NewReader(newBody))
			} else {
				meta.Logger.Warn("marshal final messages 失败", "error", err)
				r.Body = io.NopCloser(bytes.NewReader(body))
			}
		}
	}

	s.forwardRaw(w, r, meta)
}

// searchAndExpand 是 HandleMessages 的唯一召回调用点；测试可注入计数 spy。
func (s *Server) searchAndExpand(messages []Message, budget *Budget, meta *requestMeta) RecallOutcome {
	if s.searchAndExpandFn != nil {
		return s.searchAndExpandFn(messages, s.Store, s.Config.Stubify.TokenThreshold, s.TokenCounter, budget, meta)
	}
	return searchAndExpandWithMeta(messages, s.Store, s.Config.Stubify.TokenThreshold, s.TokenCounter, budget, meta)
}

// applyCacheControl 执行 cache_control 四步处理（Phase 4, D-09/D-10）。
// 仅在 frozen prefix 命中时（frozenCount > 0）执行 Strip/Inject/Normalize/Enforce。
// 所有操作为 best-effort——失败时 warn 日志但不阻断管线。
func (s *Server) applyCacheControl(messages []Message, frozenCount int, sessionID string) {
	if !s.Config.Cache.Enabled || s.Frozen == nil || frozenCount <= 0 {
		return
	}

	// Guard: after collapse, frozenCount may exceed len(messages).
	// Clamp to avoid slice bounds panic in messages[:frozenCount].
	if frozenCount > len(messages) {
		frozenCount = len(messages)
	}
	// 活动 tool_use/tool_result 必须保持入口 wire JSON 语义不变；cache boundary
	// 只能落在 active assistant 之前的稳定历史前缀。
	if activeAssistant, _ := activeToolPairIndices(messages); activeAssistant >= 0 && frozenCount > activeAssistant {
		frozenCount = activeAssistant
	}
	if frozenCount <= 0 {
		return
	}

	// Step 1: Strip —— 移除 frozen portion 中已有的 cache_control
	if err := StripMessagesCacheControl(messages[:frozenCount]); err != nil {
		slog.Warn("cache_control strip 失败", "session_id", sessionID, "error", err)
	}

	// Step 2: Inject —— 在 frozen boundary 注入 breakpoint（Inject 在 Enforce 之前，
	// 确保注入的 breakpoint 计入总数限制，不会超限）
	if err := InjectFrozenBoundaryBreakpoint(messages, frozenCount); err != nil {
		slog.Warn("cache_control inject 失败", "session_id", sessionID, "error", err)
	}

	// Step 3: EnforceLimit —— 限制已有 breakpoint 总数（包含刚注入的 boundary breakpoint）
	if err := EnforceCacheBreakpointLimit(messages[:frozenCount], s.Config.Cache.BreakpointLimit); err != nil {
		slog.Warn("cache_control enforce 失败", "session_id", sessionID, "error", err)
	}

	// Step 4: NormalizeTTL —— 统一为配置的 ephemeral(5m) 或 1h。
	if err := NormalizeCacheTTL(messages[:frozenCount], s.Config.Cache.CacheTTL); err != nil {
		slog.Warn("cache_control normalize 失败", "session_id", sessionID, "error", err)
	}

	// Cache TTL 自适应：检测请求中的有效 TTL 并动态调整 Frozen TTL 和 PauseThreshold
	// 对标 YesMem cache.go TTL 自适应逻辑（1h 断点 → Frozen TTL 65min, pause 61min）
	if s.Sawtooth != nil && s.Frozen != nil {
		effectiveTTL := s.Config.Cache.CacheTTL
		if effectiveTTL == "" {
			effectiveTTL = "ephemeral"
		}
		// NormalizeCacheTTL 会将 ephemeral 升级为 1h 当检测到已有 1h 断点时
		for _, msg := range messages[:frozenCount] {
			if findExistingMaxTTL(msg) == "1h" {
				effectiveTTL = "1h"
				break
			}
		}
		s.cacheMu.Lock()
		defer s.cacheMu.Unlock()
		if effectiveTTL != s.cachedTTL {
			s.Frozen.UpdateTTL(SawtoothTTLForCacheTTL(effectiveTTL))
			s.Sawtooth.SetPauseThreshold(CacheGapForTTL(effectiveTTL))
			s.cachedTTL = effectiveTTL
			slog.Debug("cache TTL 自适应调整",
				"session_id", sessionID,
				"effective_ttl", effectiveTTL,
			)
		}
	}
}

// extractLatestUserText 从消息数组中提取最新一条 user 消息的文本内容。
// 用于 stubify 的 pivot text 检测（pivot protection per D-08）。
// 从末尾向前遍历，找到第一条 role 为 "user" 的消息，
// 提取其纯文本内容（字符串或 text block）。
func extractLatestUserText(messages []Message) string {
	for i := len(messages) - 1; i >= 0; i-- {
		if messages[i].Role == "user" {
			var text string
			if err := json.Unmarshal(messages[i].Content, &text); err == nil {
				return text
			}
			// Content is array — extract first text block
			var blocks []ContentBlock
			if err := json.Unmarshal(messages[i].Content, &blocks); err == nil {
				for _, b := range blocks {
					if b.Type == "text" {
						return b.Text
					}
				}
			}
			return "" // user message with no text content
		}
	}
	return "" // no user messages found
}

// extractPinnedPaths 从消息数组的 tool_use 块中提取文件路径集合。
// Phase B: 供 DecayTracker.SetPinnedPaths 使用（无 daemon 替代方案）。
func extractPinnedPaths(messages []Message) []string {
	seen := make(map[string]bool)
	for _, msg := range messages {
		blocks, _ := parseContent(msg.Content)
		for _, block := range blocks {
			if block.Type == "tool_use" && block.Input != nil {
				if fp, ok := block.Input["file_path"].(string); ok && fp != "" && !seen[fp] {
					seen[fp] = true
				}
			}
		}
	}
	paths := make([]string, 0, len(seen))
	for p := range seen {
		paths = append(paths, p)
	}
	return paths
}

package proxy

import (
	"log/slog"
	"sync"
)

// requestMeta 保存一次代理请求的审计元数据。
type requestMeta struct {
	ID                   uint64
	RequestSessionID     string
	RequestKind          requestKind
	AgentRole            agentRole
	AgentReason          agentClassificationReason
	PressureDecision     pressureDecision
	BaselineUpdated      bool
	OriginalMessageCount int
	Logger               *slog.Logger
	auxiliaryAuditLogger *slog.Logger
	entryOnce            sync.Once
	rawFactsOnce         sync.Once
	forwardedFactsOnce   sync.Once
	pressureFactsOnce    sync.Once
	pressureSummaryOnce  sync.Once
	usageFactsOnce       sync.Once
	rawBodyOnce          sync.Once
	forwardedBodyOnce    sync.Once
	responseBodyOnce     sync.Once
}

// tracksSawtoothState 使用默认安全策略：nil、零值、main 与 unknown 都保持状态跟踪，
// 只有经过高置信分类的 session_title 或 subagent 请求关闭 Sawtooth 状态读写。
func (m *requestMeta) tracksSawtoothState() bool {
	return m == nil || (m.RequestKind != requestKindSessionTitle && m.AgentRole != agentRoleSubagent)
}

func (m *requestMeta) debugBodyOnce(stage debugBodyStage) *sync.Once {
	switch stage {
	case debugBodyStageRawInbound:
		return &m.rawBodyOnce
	case debugBodyStageForwarded:
		return &m.forwardedBodyOnce
	case debugBodyStageResponse:
		return &m.responseBodyOnce
	default:
		return nil
	}
}

func (m *requestMeta) debugOnce(stage debugStage) *sync.Once {
	switch stage {
	case debugStageRawInbound:
		return &m.rawFactsOnce
	case debugStageForwarded:
		return &m.forwardedFactsOnce
	case debugStagePressureDecision:
		return &m.pressureFactsOnce
	default:
		return nil
	}
}

func newRequestMeta(id uint64, requestSessionID string) *requestMeta {
	baseLogger := slog.New(slog.Default().Handler()).With("request_id", id)
	return &requestMeta{
		ID:                   id,
		RequestSessionID:     requestSessionID,
		Logger:               baseLogger.With("request_session_id", requestSessionID),
		auxiliaryAuditLogger: baseLogger,
	}
}

// auxiliaryLogger 返回辅助分类专用审计 logger，只允许继承 request_id。
// 零值或手工构造的 requestMeta 不得回退到可能预绑定 session 属性的通用 Logger。
func (m *requestMeta) auxiliaryLogger() *slog.Logger {
	if m == nil {
		return slog.Default()
	}
	if m.auxiliaryAuditLogger != nil {
		return m.auxiliaryAuditLogger
	}
	return slog.New(slog.Default().Handler()).With("request_id", m.ID)
}

func (m *requestMeta) logEntry(model string, originalMessageCount int) {
	m.entryOnce.Do(func() {
		m.OriginalMessageCount = originalMessageCount
		m.Logger.Info("请求进入",
			"original_message_count", originalMessageCount,
			"model", model,
		)
	})
}

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
	OriginalMessageCount int
	Logger               *slog.Logger
	entryOnce            sync.Once
	rawFactsOnce         sync.Once
	forwardedFactsOnce   sync.Once
	usageFactsOnce       sync.Once
	rawBodyOnce          sync.Once
	forwardedBodyOnce    sync.Once
	responseBodyOnce     sync.Once
}

// tracksSawtoothState 使用默认安全策略：nil 与零值都保持现有状态跟踪，
// 只有经过高置信分类的 session_title 请求关闭响应后的 Sawtooth 写回。
func (m *requestMeta) tracksSawtoothState() bool {
	return m == nil || m.RequestKind != requestKindSessionTitle
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
	default:
		return nil
	}
}

func newRequestMeta(id uint64, requestSessionID string) *requestMeta {
	return &requestMeta{
		ID:               id,
		RequestSessionID: requestSessionID,
		Logger: slog.Default().With(
			"request_id", id,
			"request_session_id", requestSessionID,
		),
	}
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

package proxy

import (
	"log/slog"
	"sync"
)

// requestMeta 保存一次代理请求的审计元数据。
type requestMeta struct {
	ID                   uint64
	RequestSessionID     string
	OriginalMessageCount int
	Logger               *slog.Logger
	entryOnce            sync.Once
	rawFactsOnce         sync.Once
	forwardedFactsOnce   sync.Once
	usageFactsOnce       sync.Once
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

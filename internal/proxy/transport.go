package proxy

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"sync"
	"sync/atomic"
	"time"
)

var (
	errProxyHardLimit           = errors.New("proxy hard limit exceeded")
	errProxyResponseIdleTimeout = errors.New("proxy response idle timeout")
)

type streamContextKey struct{}

func withStreamMarker(ctx context.Context, stream bool) context.Context {
	return context.WithValue(ctx, streamContextKey{}, stream)
}

func streamFromContext(ctx context.Context) bool {
	stream, _ := ctx.Value(streamContextKey{}).(bool)
	return stream
}

// streamAwareTransport 持有两套长期复用的连接池，只负责按请求形态选择响应头预算。
// RoundTrip 不执行重试，避免含糊的模型 POST 被重复提交。
type streamAwareTransport struct {
	stream    http.RoundTripper
	nonStream http.RoundTripper
}

func (t *streamAwareTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if streamFromContext(req.Context()) {
		return t.stream.RoundTrip(req)
	}
	return t.nonStream.RoundTrip(req)
}

func newUpstreamHTTPClient(cfg TransportConfig) *http.Client {
	return &http.Client{
		Transport: &streamAwareTransport{
			stream:    newUpstreamTransport(cfg, cfg.StreamHeaderTimeout),
			nonStream: newUpstreamTransport(cfg, cfg.NonStreamHeaderTimeout),
		},
		Timeout: 0,
	}
}

func newUpstreamTransport(cfg TransportConfig, headerTimeout time.Duration) *http.Transport {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	dialer := &net.Dialer{
		Timeout:   cfg.DialTimeout,
		KeepAlive: 30 * time.Second,
	}
	transport.DialContext = dialer.DialContext
	transport.TLSHandshakeTimeout = cfg.TLSHandshakeTimeout
	transport.ResponseHeaderTimeout = headerTimeout
	return transport
}

type upstreamPhase string

const (
	upstreamPhaseConnect         upstreamPhase = "connect"
	upstreamPhaseTLSHandshake    upstreamPhase = "tls_handshake"
	upstreamPhaseAwaitingHeaders upstreamPhase = "awaiting_headers"
	upstreamPhaseReadingBody     upstreamPhase = "reading_body"
)

// upstreamPhaseTracker 只记录无敏感信息的生命周期阶段，供日志和错误分类使用。
type upstreamPhaseTracker struct {
	mu    sync.RWMutex
	phase upstreamPhase
}

func newUpstreamPhaseTracker() *upstreamPhaseTracker {
	return &upstreamPhaseTracker{phase: upstreamPhaseConnect}
}

func (t *upstreamPhaseTracker) set(phase upstreamPhase) {
	t.mu.Lock()
	t.phase = phase
	t.mu.Unlock()
}

func (t *upstreamPhaseTracker) current() upstreamPhase {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.phase
}

func (t *upstreamPhaseTracker) trace() *httptrace.ClientTrace {
	return &httptrace.ClientTrace{
		GetConn: func(string) {
			t.set(upstreamPhaseConnect)
		},
		TLSHandshakeStart: func() {
			t.set(upstreamPhaseTLSHandshake)
		},
		WroteRequest: func(httptrace.WroteRequestInfo) {
			t.set(upstreamPhaseAwaitingHeaders)
		},
		GotFirstResponseByte: func() {
			// 收到首字节时响应头尚未完整交付给调用方，仍处于响应头阶段。
			t.set(upstreamPhaseAwaitingHeaders)
		},
	}
}

// withProxyHardLimit 从下游 request context 派生绝对兜底时限。
// timeout=0 时保留父 context，仅返回可统一 defer 的 no-op cancel。
func withProxyHardLimit(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		return parent, func() {}
	}
	return context.WithTimeoutCause(parent, timeout, errProxyHardLimit)
}

// idleTimeoutBody 为每次阻塞 Read 单独计时；读到任意字节后下一次 Read 重新开始窗口。
type idleTimeoutBody struct {
	body      io.ReadCloser
	timeout   time.Duration
	readMu    sync.Mutex
	closeOnce sync.Once
	closeErr  error
	timedOutF atomic.Bool
}

func newIdleTimeoutBody(body io.ReadCloser, timeout time.Duration) io.ReadCloser {
	if timeout <= 0 {
		return body
	}
	return &idleTimeoutBody{body: body, timeout: timeout}
}

func (b *idleTimeoutBody) Read(p []byte) (int, error) {
	b.readMu.Lock()
	defer b.readMu.Unlock()
	if b.timedOut() {
		return 0, errProxyResponseIdleTimeout
	}

	timerDone := make(chan struct{})
	timer := time.AfterFunc(b.timeout, func() {
		b.timedOutF.Store(true)
		_ = b.closeUnderlying()
		close(timerDone)
	})

	n, err := b.body.Read(p)
	if !timer.Stop() {
		<-timerDone
	}
	if b.timedOut() {
		if n > 0 {
			return n, nil
		}
		return 0, errProxyResponseIdleTimeout
	}
	return n, err
}

func (b *idleTimeoutBody) Close() error {
	return b.closeUnderlying()
}

func (b *idleTimeoutBody) closeUnderlying() error {
	b.closeOnce.Do(func() {
		b.closeErr = b.body.Close()
	})
	return b.closeErr
}

func (b *idleTimeoutBody) timedOut() bool {
	return b.timedOutF.Load()
}

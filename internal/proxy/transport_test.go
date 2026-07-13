package proxy

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestTransportConfigDefaults(t *testing.T) {
	cfg := DefaultConfig()
	want := TransportConfig{
		DialTimeout:            15 * time.Second,
		TLSHandshakeTimeout:    15 * time.Second,
		StreamHeaderTimeout:    10 * time.Minute,
		NonStreamHeaderTimeout: 30 * time.Minute,
		ResponseIdleTimeout:    10 * time.Minute,
		HardTimeout:            60 * time.Minute,
	}
	if cfg.Transport != want {
		t.Fatalf("DefaultConfig().Transport = %+v, want %+v", cfg.Transport, want)
	}

	server := NewServer(cfg)
	if server.HTTPClient.Timeout != 0 {
		t.Fatalf("HTTPClient.Timeout = %s, want 0", server.HTTPClient.Timeout)
	}
}

func TestTransportConfigYAMLAndZeroDisable(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "sawtooth.yaml")
	content := []byte(`transport:
  dial_timeout: 1s
  tls_handshake_timeout: 2s
  stream_header_timeout: 3s
  non_stream_header_timeout: 4s
  response_idle_timeout: 5s
  hard_timeout: 6s
`)
	if err := os.WriteFile(configPath, content, 0600); err != nil {
		t.Fatalf("写入测试配置: %v", err)
	}
	loaded, err := LoadConfig(configPath)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	want := TransportConfig{
		DialTimeout:            time.Second,
		TLSHandshakeTimeout:    2 * time.Second,
		StreamHeaderTimeout:    3 * time.Second,
		NonStreamHeaderTimeout: 4 * time.Second,
		ResponseIdleTimeout:    5 * time.Second,
		HardTimeout:            6 * time.Second,
	}
	if loaded.Transport != want {
		t.Fatalf("YAML Transport = %+v, want %+v", loaded.Transport, want)
	}

	zeroPath := filepath.Join(t.TempDir(), "zero.yaml")
	zeroContent := []byte(`transport:
  dial_timeout: 0
  tls_handshake_timeout: 0
  stream_header_timeout: 0
  non_stream_header_timeout: 0
  response_idle_timeout: 0
  hard_timeout: 0
`)
	if err := os.WriteFile(zeroPath, zeroContent, 0600); err != nil {
		t.Fatalf("写入零值配置: %v", err)
	}
	zero, err := LoadConfig(zeroPath)
	if err != nil {
		t.Fatalf("LoadConfig zero: %v", err)
	}
	if zero.Transport != (TransportConfig{}) {
		t.Fatalf("显式 0 被回退: %+v", zero.Transport)
	}

	negative := TransportConfig{
		DialTimeout:            -1,
		TLSHandshakeTimeout:    -1,
		StreamHeaderTimeout:    -1,
		NonStreamHeaderTimeout: -1,
		ResponseIdleTimeout:    -1,
		HardTimeout:            -1,
	}
	cfg := DefaultConfig()
	cfg.Transport = negative
	validateConfig(&cfg)
	if cfg.Transport != DefaultConfig().Transport {
		t.Fatalf("负值未逐项回退默认值: %+v", cfg.Transport)
	}
}

func TestStreamAwareTransportSelectsHeaderTimeout(t *testing.T) {
	received := make(chan struct{}, 2)
	release := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		received <- struct{}{}
		<-release
		w.WriteHeader(http.StatusNoContent)
	}))
	defer upstream.Close()

	client := newUpstreamHTTPClient(TransportConfig{
		StreamHeaderTimeout:    40 * time.Millisecond,
		NonStreamHeaderTimeout: 500 * time.Millisecond,
	})

	streamReq, err := http.NewRequestWithContext(withStreamMarker(context.Background(), true), http.MethodPost, upstream.URL, nil)
	if err != nil {
		t.Fatalf("创建 stream 请求: %v", err)
	}
	streamResult := make(chan error, 1)
	go func() {
		resp, err := client.Do(streamReq)
		if resp != nil {
			_ = resp.Body.Close()
		}
		streamResult <- err
	}()
	<-received
	select {
	case err := <-streamResult:
		if err == nil {
			t.Fatal("stream 请求应触发响应头超时")
		}
	case <-time.After(time.Second):
		t.Fatal("stream 响应头超时未生效")
	}

	nonStreamReq, err := http.NewRequestWithContext(withStreamMarker(context.Background(), false), http.MethodPost, upstream.URL, nil)
	if err != nil {
		t.Fatalf("创建 non-stream 请求: %v", err)
	}
	nonStreamResult := make(chan error, 1)
	go func() {
		resp, err := client.Do(nonStreamReq)
		if resp != nil {
			_ = resp.Body.Close()
		}
		nonStreamResult <- err
	}()
	<-received
	select {
	case err := <-nonStreamResult:
		t.Fatalf("non-stream 错误使用了 stream 预算: %v", err)
	case <-time.After(120 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-nonStreamResult:
		if err != nil {
			t.Fatalf("释放响应头后 non-stream 请求失败: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("non-stream 请求未完成")
	}
}

func TestIdleTimeoutBodyResetsOnProgress(t *testing.T) {
	reader, writer := io.Pipe()
	body := newIdleTimeoutBody(reader, 80*time.Millisecond)
	if body == reader {
		t.Fatal("非零 idle timeout 应包装响应体")
	}

	writerDone := make(chan struct{})
	go func() {
		defer writer.Close()
		for _, chunk := range []byte{'a', 'b', 'c'} {
			time.Sleep(30 * time.Millisecond)
			_, _ = writer.Write([]byte{chunk})
		}
		<-writerDone
	}()

	buffer := make([]byte, 1)
	started := time.Now()
	for _, want := range []byte{'a', 'b', 'c'} {
		n, err := body.Read(buffer)
		if err != nil || n != 1 || buffer[0] != want {
			t.Fatalf("进展读取 = (%d, %v, %q), want (1, nil, %q)", n, err, buffer[:n], want)
		}
	}
	if time.Since(started) <= 80*time.Millisecond {
		t.Fatal("测试未证明总读取时间可超过单个 idle 窗口")
	}

	_, err := body.Read(buffer)
	if err == nil {
		t.Fatal("连续无字节时应触发 idle timeout")
	}
	idleBody, ok := body.(*idleTimeoutBody)
	if !ok || !idleBody.timedOut() {
		t.Fatalf("idle timeout 标记不可识别: type=%T err=%v", body, err)
	}
	if !errors.Is(err, errProxyResponseIdleTimeout) {
		t.Fatalf("idle timeout error = %v, want sentinel", err)
	}
	close(writerDone)
	if err := body.Close(); err != nil {
		t.Fatalf("第一次 Close: %v", err)
	}
	if err := body.Close(); err != nil {
		t.Fatalf("第二次 Close 不幂等: %v", err)
	}

	disabled := io.NopCloser(&emptyReader{})
	if got := newIdleTimeoutBody(disabled, 0); got != disabled {
		t.Fatal("idle timeout=0 不应包装响应体")
	}
}

func TestHardLimitCause(t *testing.T) {
	ctx, cancel := withProxyHardLimit(context.Background(), 20*time.Millisecond)
	defer cancel()
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("hard limit 未触发")
	}
	if !errors.Is(context.Cause(ctx), errProxyHardLimit) {
		t.Fatalf("hard cause = %v, want sentinel", context.Cause(ctx))
	}

	parent, parentCancel := context.WithCancel(context.Background())
	child, childCancel := withProxyHardLimit(parent, time.Second)
	parentCancel()
	defer childCancel()
	<-child.Done()
	if errors.Is(context.Cause(child), errProxyHardLimit) || !errors.Is(context.Cause(child), context.Canceled) {
		t.Fatalf("parent cancel cause 被误判: %v", context.Cause(child))
	}

	plain := context.Background()
	disabled, disabledCancel := withProxyHardLimit(plain, 0)
	disabledCancel()
	if disabled != plain {
		t.Fatal("hard timeout=0 应保留父 context")
	}
}

type emptyReader struct{}

func (*emptyReader) Read([]byte) (int, error) { return 0, io.EOF }

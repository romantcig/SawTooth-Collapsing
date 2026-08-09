package proxy

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestHandleJSONRemarshalClearsRepresentationHeaders(t *testing.T) {
	s := NewServer(Config{Proxy: ProxyConfig{Deflation: 1}})
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header: http.Header{
			"Content-Type":   {"application/json"},
			"Content-Length": {"999"},
			"ETag":           {"\"old\""},
			"Digest":         {"sha-256=old"},
			"Content-MD5":    {"old"},
			"Content-Digest": {"sha-256=old"},
			"Repr-Digest":    {"sha-256=old"},
			"Content-Range":  {"bytes 0-998/999"},
			"X-Keep":         {"yes"},
		},
		Body: io.NopCloser(strings.NewReader(`{"type":"message","usage":{"output_tokens":2,"input_tokens":1}}`)),
	}
	w := httptest.NewRecorder()
	result := s.handleJSON(w, resp, newRequestMeta(1, "json-contract"), nowForContractTest(), "model", 2)
	if result.err != nil {
		t.Fatalf("handleJSON error: %v", result.err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("rewritten body is not JSON: %v; body=%q", err, w.Body.Bytes())
	}
	for _, key := range []string{"Content-Length", "ETag", "Digest", "Content-MD5", "Content-Digest", "Repr-Digest", "Content-Range"} {
		if got := w.Header().Get(key); got != "" && got != "0" {
			t.Fatalf("stale representation header %s=%q", key, got)
		}
	}
	if w.Header().Get("X-Keep") != "yes" {
		t.Fatalf("unrelated header was removed: %v", w.Header())
	}
}

func TestHandleJSONGzipMalformedRepresentation(t *testing.T) {
	var compressed bytes.Buffer
	gz := gzip.NewWriter(&compressed)
	_, _ = gz.Write([]byte(`{"broken":`))
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	s := NewServer(Config{})
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header: http.Header{
			"Content-Type":     {"application/json"},
			"Content-Encoding": {"gzip"},
			"Content-Length":   {"999"},
			"ETag":             {"old"},
		},
		Body: io.NopCloser(bytes.NewReader(compressed.Bytes())),
	}
	w := httptest.NewRecorder()
	result := s.handleJSON(w, resp, newRequestMeta(2, "gzip-contract"), nowForContractTest(), "model", 1)
	if result.err != nil {
		t.Fatalf("gzip malformed should degrade gracefully: %v", result.err)
	}
	if got := w.Body.String(); got != `{"broken":` {
		t.Fatalf("malformed decompressed body=%q", got)
	}
	if w.Header().Get("Content-Encoding") != "" || w.Header().Get("Content-Length") != "" || w.Header().Get("ETag") != "" {
		t.Fatalf("decompression kept stale headers: %v", w.Header())
	}
}

func TestHandleJSONAutoDecompressedResponse(t *testing.T) {
	s := NewServer(Config{})
	resp := &http.Response{
		StatusCode:   http.StatusOK,
		Uncompressed: true,
		Header: http.Header{
			"Content-Type":   {"application/json"},
			"Content-Length": {"1000"},
			"ETag":           {"old"},
		},
		Body: io.NopCloser(strings.NewReader(`{"ok":true}`)),
	}
	w := httptest.NewRecorder()
	result := s.handleJSON(w, resp, newRequestMeta(3, "auto-contract"), nowForContractTest(), "model", 1)
	if result.err != nil {
		t.Fatalf("auto-decompressed response error: %v", result.err)
	}
	if w.Header().Get("Content-Length") != "" || w.Header().Get("ETag") != "" {
		t.Fatalf("auto-decompressed stale headers: %v", w.Header())
	}
	var value map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &value); err != nil || value["ok"] != true {
		t.Fatalf("auto-decompressed body=%q err=%v", w.Body.Bytes(), err)
	}
}

func TestHandleSSERewriteClearsRepresentationHeaders(t *testing.T) {
	s := NewServer(Config{Proxy: ProxyConfig{Deflation: 0.5}})
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header: http.Header{
			"Content-Type":   {"text/event-stream"},
			"Content-Length": {"42"},
			"ETag":           {"old"},
			"Digest":         {"old"},
			"Content-Range":  {"bytes 0-41/42"},
		},
		Body: io.NopCloser(strings.NewReader("event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":2}}}\n\n")),
	}
	w := httptest.NewRecorder()
	result := s.handleSSE(w, resp, newRequestMeta(4, "sse-contract"), nowForContractTest(), "model", 1)
	if result.err != nil {
		t.Fatalf("SSE error: %v", result.err)
	}
	for _, key := range []string{"Content-Length", "ETag", "Digest", "Content-Range"} {
		if got := w.Header().Get(key); got != "" {
			t.Fatalf("SSE stale representation header %s=%q", key, got)
		}
	}
}

func TestHandleJSONShortWriteCommitted(t *testing.T) {
	s := NewServer(Config{})
	resp := contractResponse(`{"ok":true}`)
	w := newContractWriter(contractWriteShortNil)
	result := s.handleJSON(w, resp, newRequestMeta(5, "short-write"), nowForContractTest(), "model", 1)
	if !result.committed || !errors.Is(result.err, io.ErrShortWrite) || result.failureClass != "downstream_write" {
		t.Fatalf("short write result=%+v, want committed downstream_write/io.ErrShortWrite", result)
	}
}

func TestHandleJSONWriteErrorCommitted(t *testing.T) {
	s := NewServer(Config{})
	resp := contractResponse(`{"ok":true}`)
	w := newContractWriter(contractWriteError)
	want := errors.New("downstream failed")
	w.writeErr = want
	result := s.handleJSON(w, resp, newRequestMeta(6, "write-error"), nowForContractTest(), "model", 1)
	if !result.committed || !errors.Is(result.err, want) || result.failureClass != "downstream_write" {
		t.Fatalf("write error result=%+v, want committed downstream_write/%v", result, want)
	}
}

func TestForwardRawCommittedJSONWriteDoesNotAppendGateway(t *testing.T) {
	s := NewServer(Config{Proxy: ProxyConfig{Target: "https://upstream.invalid"}})
	s.HTTPClient = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return contractResponse(`{"ok":true}`), nil
	})}
	w := newContractWriter(contractWriteError)
	w.writeErr = errors.New("downstream failed")
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"messages":[]}`))
	s.forwardRaw(w, req, newRequestMeta(7, "forward-committed"))
	if got := bytes.Count(w.body.Bytes(), []byte(`"error"`)); got != 0 {
		t.Fatalf("forwardRaw appended gateway error after committed write: %s", w.body.Bytes())
	}
}

func TestNon2xxByteExactRepresentation(t *testing.T) {
	s := NewServer(Config{})
	body := []byte(`{"error":"keep","usage":{"input_tokens":3}}`)
	resp := &http.Response{StatusCode: http.StatusBadRequest, Header: http.Header{"Content-Type": {"application/json"}, "ETag": {"old"}}, Body: io.NopCloser(bytes.NewReader(body))}
	w := httptest.NewRecorder()
	result := s.handleNon2xx(w, resp, newRequestMeta(8, "non2xx"), nowForContractTest(), "model", 1)
	if result.err != nil || !bytes.Equal(w.Body.Bytes(), body) {
		t.Fatalf("non-2xx changed body: result=%+v body=%q", result, w.Body.Bytes())
	}
}

func contractResponse(body string) *http.Response {
	return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(body))}
}

func nowForContractTest() (now time.Time) { return time.Unix(1, 0) }

type contractWriteMode int

const (
	contractWriteFull contractWriteMode = iota
	contractWriteShortNil
	contractWriteError
)

type contractWriter struct {
	header   http.Header
	status   int
	body     bytes.Buffer
	mode     contractWriteMode
	writeErr error
}

func newContractWriter(mode contractWriteMode) *contractWriter {
	return &contractWriter{header: make(http.Header), mode: mode}
}

func (w *contractWriter) Header() http.Header { return w.header }

func (w *contractWriter) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
	}
}

func (w *contractWriter) Write(data []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	switch w.mode {
	case contractWriteShortNil:
		n := len(data) / 2
		if n == 0 && len(data) > 0 {
			n = 1
		}
		_, _ = w.body.Write(data[:n])
		return n, nil
	case contractWriteError:
		return 0, w.writeErr
	default:
		return w.body.Write(data)
	}
}

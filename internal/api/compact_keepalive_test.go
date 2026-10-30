package api

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/NevermindZZT/dsh-manager/internal/config"
	"github.com/NevermindZZT/dsh-manager/internal/storage"
	"github.com/coder/websocket"
)

const compactKeepaliveRequestBody = `{"type":"client-request","rpcId":"compact-rpc","method":"commands/execute","payload":{"args":{"agentId":"agent-1","line":"/compact","submittedAttachments":[]}}}`

func TestPrepareCompactKeepaliveScope(t *testing.T) {
	tests := []struct {
		name, method, path, contentType, body string
		eligible                              bool
	}{
		{"manual compact", "POST", "/api/commands/execute", "application/json", compactKeepaliveRequestBody, true},
		{"JSON parameters", "POST", "/api/commands/execute", "application/json; charset=utf-8", compactKeepaliveRequestBody, true},
		{"query", "POST", "/api/commands/execute?trace=1", "application/json", compactKeepaliveRequestBody, true},
		{"GET", "GET", "/api/commands/execute", "application/json", compactKeepaliveRequestBody, false},
		{"other API", "POST", "/api/session.list", "application/json", compactKeepaliveRequestBody, false},
		{"route suffix", "POST", "/api/commands/execute/extra", "application/json", compactKeepaliveRequestBody, false},
		{"missing content type", "POST", "/api/commands/execute", "", compactKeepaliveRequestBody, false},
		{"text body", "POST", "/api/commands/execute", "text/plain", compactKeepaliveRequestBody, false},
		{"other request type", "POST", "/api/commands/execute", "application/json", strings.Replace(compactKeepaliveRequestBody, "client-request", "server-response", 1), false},
		{"other RPC method", "POST", "/api/commands/execute", "application/json", strings.Replace(compactKeepaliveRequestBody, `"method":"commands/execute"`, `"method":"agent/run"`, 1), false},
		{"missing correlation", "POST", "/api/commands/execute", "application/json", strings.Replace(compactKeepaliveRequestBody, `"rpcId":"compact-rpc",`, "", 1), false},
		{"non-string correlation", "POST", "/api/commands/execute", "application/json", strings.Replace(compactKeepaliveRequestBody, `"rpcId":"compact-rpc"`, `"rpcId":42`, 1), false},
		{"missing agent", "POST", "/api/commands/execute", "application/json", strings.Replace(compactKeepaliveRequestBody, `"agentId":"agent-1",`, "", 1), false},
		{"other command", "POST", "/api/commands/execute", "application/json", strings.Replace(compactKeepaliveRequestBody, "/compact", "/help", 1), false},
		{"arguments", "POST", "/api/commands/execute", "application/json", strings.Replace(compactKeepaliveRequestBody, "/compact", "/compact now", 1), false},
		{"native whitespace", "POST", "/api/commands/execute", "application/json", strings.Replace(compactKeepaliveRequestBody, "/compact", " /compact ", 1), true},
		{"attached file", "POST", "/api/commands/execute", "application/json", strings.Replace(compactKeepaliveRequestBody, `"submittedAttachments":[]`, `"submittedAttachments":[{"id":"file-1"}]`, 1), false},
		{"null attachments", "POST", "/api/commands/execute", "application/json", strings.Replace(compactKeepaliveRequestBody, `"submittedAttachments":[]`, `"submittedAttachments":null`, 1), false},
		{"missing attachments", "POST", "/api/commands/execute", "application/json", strings.Replace(compactKeepaliveRequestBody, `,"submittedAttachments":[]`, "", 1), false},
		{"wrong attachment type", "POST", "/api/commands/execute", "application/json", strings.Replace(compactKeepaliveRequestBody, `"submittedAttachments":[]`, `"submittedAttachments":{}`, 1), false},
		{"malformed JSON", "POST", "/api/commands/execute", "application/json", compactKeepaliveRequestBody[:len(compactKeepaliveRequestBody)-1], false},
		{"trailing JSON value", "POST", "/api/commands/execute", "application/json", compactKeepaliveRequestBody + `{}`, false},
		{"not an envelope", "POST", "/api/commands/execute", "application/json", `{"agentId":"agent-1","line":"/compact"}`, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequest(tt.method, tt.path, strings.NewReader(tt.body))
			r.Header.Set("Content-Type", tt.contentType)
			keepalive, err := prepareCompactKeepalive(r)
			if err != nil {
				t.Fatal(err)
			}
			if (keepalive != nil) != tt.eligible {
				t.Fatalf("eligible=%v, want %v", keepalive != nil, tt.eligible)
			}
			if keepalive != nil && (keepalive.rpcID != "compact-rpc" || keepalive.started) {
				t.Fatalf("unexpected prepared state: %+v", keepalive)
			}
			replayed, err := io.ReadAll(r.Body)
			if err != nil || string(replayed) != tt.body {
				t.Fatalf("raw body changed: %q, error=%v", replayed, err)
			}
		})
	}
}

type compactObservedBody struct {
	reader       io.Reader
	read, closed int
}

func (b *compactObservedBody) Read(p []byte) (int, error) {
	n, err := b.reader.Read(p)
	b.read += n
	return n, err
}
func (b *compactObservedBody) Close() error { b.closed++; return nil }

func TestPrepareCompactKeepaliveReplaysBodyWithBoundedInspection(t *testing.T) {
	const limit = 64 << 10
	for _, size := range []int{len(compactKeepaliveRequestBody), limit, limit + 1, limit + 4096} {
		t.Run(fmt.Sprintf("%d bytes", size), func(t *testing.T) {
			body := compactKeepaliveRequestBody + strings.Repeat(" ", size-len(compactKeepaliveRequestBody))
			original := &compactObservedBody{reader: strings.NewReader(body)}
			r := httptest.NewRequest(http.MethodPost, "/api/commands/execute", nil)
			r.Header.Set("Content-Type", "application/json")
			r.Body, r.ContentLength = original, int64(len(body))
			keepalive, err := prepareCompactKeepalive(r)
			if err != nil {
				t.Fatal(err)
			}
			if original.read > limit+1 {
				t.Fatalf("inspected %d bytes, maximum %d", original.read, limit+1)
			}
			if original.closed != 0 {
				t.Fatal("inspection prematurely closed original body")
			}
			if r.ContentLength != int64(len(body)) {
				t.Fatalf("ContentLength changed to %d", r.ContentLength)
			}
			if (keepalive != nil) != (size <= limit) {
				t.Fatalf("eligible=%v for %d-byte body", keepalive != nil, size)
			}
			replayed, err := io.ReadAll(r.Body)
			if err != nil || string(replayed) != body {
				t.Fatalf("body not replayed verbatim: length=%d error=%v", len(replayed), err)
			}
			if err := r.Body.Close(); err != nil {
				t.Fatal(err)
			}
			if original.closed != 1 {
				t.Fatalf("closing replay closed original %d times", original.closed)
			}
		})
	}
}

func TestCompactKeepaliveHeartbeatFlushesAndPreservesNativeJSON(t *testing.T) {
	keepalive := &compactKeepalive{rpcID: "compact-rpc", interval: time.Millisecond}
	w := httptest.NewRecorder()
	w.Header().Set("Content-Length", "999")
	if err := keepalive.writeHeartbeat(w); err != nil {
		t.Fatal(err)
	}
	if !keepalive.started || !w.Flushed || w.Code != http.StatusOK || w.Body.String() != "\n" {
		t.Fatalf("heartbeat not flushed: started=%v flushed=%v status=%d body=%q", keepalive.started, w.Flushed, w.Code, w.Body.String())
	}
	response := w.Result()
	mediaType, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		t.Fatalf("Content-Type=%q error=%v", response.Header.Get("Content-Type"), err)
	}
	cacheControl := strings.ToLower(response.Header.Get("Cache-Control"))
	if !strings.Contains(cacheControl, "no-store") || !strings.Contains(cacheControl, "no-transform") || response.Header.Get("X-Accel-Buffering") != "no" {
		t.Fatalf("unsafe headers: %v", response.Header)
	}
	if response.Header.Get("Content-Length") != "" {
		t.Fatal("heartbeat committed fixed Content-Length")
	}
	if err := keepalive.writeHeartbeat(w); err != nil {
		t.Fatal(err)
	}
	terminal := []byte(`{ "type":"server-response", "rpcId":"compact-rpc", "result":{"ok":true,"value":{"commandId":"cmd-1","result":{"kind":"success","text":"Compacted 1 history item."}}} }`)
	if err := keepalive.finish(w, proxyResponse{Status: http.StatusOK, Headers: map[string]string{"Content-Type": "application/json", "Content-Length": "999"}}, terminal); err != nil {
		t.Fatal(err)
	}
	if got := w.Body.Bytes(); !bytes.Equal(got, append([]byte("\n\n"), terminal...)) {
		t.Fatalf("terminal JSON changed: %q", got)
	}
	var decoded map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("whole HTTP body must remain one JSON value: %v", err)
	}
	if w.Header().Get("Content-Length") != "" {
		t.Fatal("finish added Content-Length after heartbeat")
	}
}

func assertCompactFailureEnvelope(t *testing.T, body []byte) {
	t.Helper()
	var envelope struct {
		Type   string `json:"type"`
		RPCID  string `json:"rpcId"`
		Result struct {
			OK *bool `json:"ok"`
		} `json:"result"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		t.Fatalf("failure not valid native JSON: %q: %v", body, err)
	}
	if envelope.Type != "server-response" || envelope.RPCID != "compact-rpc" || envelope.Result.OK == nil || *envelope.Result.OK {
		t.Fatalf("failure lost correlation or became success: %s", body)
	}
}

func TestCompactKeepaliveLateGatewayFailureIsNotSuccess(t *testing.T) {
	tests := []struct {
		name     string
		response proxyResponse
		body     string
	}{
		{"gateway status", proxyResponse{Status: http.StatusBadGateway}, `upstream unavailable`},
		{"malformed JSON", proxyResponse{Status: http.StatusOK}, `not JSON`},
		{"wrong correlation", proxyResponse{Status: http.StatusOK}, `{"type":"server-response","rpcId":"another-rpc","result":{"ok":true}}`},
		{"missing result", proxyResponse{Status: http.StatusOK}, `{"type":"server-response","rpcId":"compact-rpc"}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			keepalive := &compactKeepalive{rpcID: "compact-rpc"}
			w := httptest.NewRecorder()
			if err := keepalive.writeHeartbeat(w); err != nil {
				t.Fatal(err)
			}
			if err := keepalive.finish(w, tt.response, []byte(tt.body)); err != nil {
				t.Fatal(err)
			}
			if w.Code != http.StatusOK {
				t.Fatalf("committed status changed: %d", w.Code)
			}
			assertCompactFailureEnvelope(t, w.Body.Bytes())
		})
	}
	t.Run("transport failure", func(t *testing.T) {
		keepalive := &compactKeepalive{rpcID: "compact-rpc"}
		w := httptest.NewRecorder()
		if err := keepalive.writeHeartbeat(w); err != nil {
			t.Fatal(err)
		}
		if err := keepalive.fail(w, http.StatusBadGateway, "agent disconnected"); err != nil {
			t.Fatal(err)
		}
		assertCompactFailureEnvelope(t, w.Body.Bytes())
	})
}

func TestCompactKeepaliveWithoutHeartbeatPreservesNativeError(t *testing.T) {
	keepalive := &compactKeepalive{rpcID: "compact-rpc"}
	w := httptest.NewRecorder()
	terminal := []byte(`{"type":"server-response","rpcId":"compact-rpc","result":{"ok":false,"error":{"code":"gateway/internal","message":"no context to compact","details":{"reason":"empty"}}}}`)
	if err := keepalive.finish(w, proxyResponse{Status: http.StatusConflict, Headers: map[string]string{"Content-Type": "application/json"}}, terminal); err != nil {
		t.Fatal(err)
	}
	if w.Code != http.StatusConflict || w.Flushed || keepalive.started || !bytes.Equal(w.Body.Bytes(), terminal) {
		t.Fatalf("native error changed: status=%d flushed=%v started=%v body=%q", w.Code, w.Flushed, keepalive.started, w.Body.Bytes())
	}
}

func TestProxyHTTPCompactKeepalive(t *testing.T) {
	for _, test := range []struct{ streamRequest, cancelBrowser bool }{{false, false}, {false, true}, {true, false}, {true, true}} {
		t.Run(fmt.Sprintf("stream_request_%v/browser_cancel_%v", test.streamRequest, test.cancelBrowser), func(t *testing.T) {
			db, err := storage.Open(t.TempDir() + "/manager.db")
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			srv := NewServer(config.Config{PairingCode: "compact-pair", AdminToken: "compact-admin"}, db, slog.New(slog.NewTextHandler(io.Discard, nil)))
			srv.compactHeartbeatInterval = 10 * time.Millisecond
			ts := httptest.NewServer(srv.Handler())
			defer ts.Close()
			caps := []string{"proxy.http", "proxy.http-stream-v1", "proxy.cancel-v1"}
			if test.streamRequest {
				caps = append(caps, "proxy.http-request-stream-v1")
			}
			agent, _ := enrollAndConnect(t, ts, srv, "compact-pair", "compact-proxy", caps)
			defer agent.CloseNow()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, ts.URL+"/dsh/compact-proxy/api/commands/execute", strings.NewReader(compactKeepaliveRequestBody))
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Content-Type", "application/json; charset=utf-8")
			type browserResult struct {
				response *http.Response
				err      error
			}
			browser := make(chan browserResult, 1)
			go func() { response, err := http.DefaultClient.Do(req); browser <- browserResult{response, err} }()
			_, data, err := agent.Read(ctx)
			if err != nil {
				t.Fatal(err)
			}
			var proxied proxyRequest
			if err := json.Unmarshal(data, &proxied); err != nil {
				t.Fatal(err)
			}
			if proxied.StreamResponse {
				t.Fatal("compact must retain buffered Agent response transport")
			}
			rawBody, err := base64.StdEncoding.DecodeString(proxied.Body)
			if test.streamRequest {
				if proxied.Type != "proxy_request_start" || !proxied.StreamRequest || proxied.Body != "" {
					t.Fatalf("unexpected request stream start: %+v", proxied)
				}
				rawBody = nil
				for {
					messageType, chunk, readErr := agent.Read(ctx)
					if readErr != nil {
						t.Fatal(readErr)
					}
					var message agentMessage
					if messageType == websocket.MessageBinary {
						separator := bytes.IndexByte(chunk, '\n')
						if separator < 1 || json.Unmarshal(chunk[:separator], &message) != nil || message.Type != "proxy_request_chunk_binary" || message.RequestID != proxied.RequestID {
							t.Fatalf("unexpected request chunk: %q", chunk)
						}
						rawBody = append(rawBody, chunk[separator+1:]...)
						continue
					}
					if json.Unmarshal(chunk, &message) != nil || message.Type != "proxy_request_end" || message.RequestID != proxied.RequestID || message.Error != "" {
						t.Fatalf("unexpected request end: %q", chunk)
					}
					break
				}
			}
			if err != nil || string(rawBody) != compactKeepaliveRequestBody {
				t.Fatalf("Agent request body changed: %q %v", rawBody, err)
			}
			var result browserResult
			select {
			case result = <-browser:
			case <-ctx.Done():
				t.Fatal("headers never flushed before Agent response")
			}
			if result.err != nil {
				t.Fatal(result.err)
			}
			defer result.response.Body.Close()
			reader := bufio.NewReader(result.response.Body)
			first, err := reader.ReadByte()
			if err != nil || first != '\n' {
				t.Fatalf("first heartbeat=%q error=%v", first, err)
			}
			if result.response.Header.Get("Content-Length") != "" {
				t.Fatal("streamed heartbeat has Content-Length")
			}
			if test.cancelBrowser {
				cancel()
				_, data, err = agent.Read(contextWithCompactTimeout(t))
				if err != nil {
					t.Fatalf("read proxy_cancel: %v", err)
				}
				var canceled map[string]string
				if err := json.Unmarshal(data, &canceled); err != nil {
					t.Fatal(err)
				}
				if canceled["type"] != "proxy_cancel" || canceled["requestId"] != proxied.RequestID || canceled["reason"] != "client_disconnected" {
					t.Fatalf("unexpected cancel: %v", canceled)
				}
				return
			}
			terminal := []byte(compactNativeSuccess)
			// The browser has already consumed a heartbeat; keep the native reply delayed.
			timer := time.NewTimer(150 * time.Millisecond)
			defer timer.Stop()
			select {
			case <-timer.C:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			response, _ := json.Marshal(agentMessage{Type: "proxy_response", RequestID: proxied.RequestID, Status: http.StatusOK, Headers: map[string]string{"Content-Type": "application/json", "Content-Length": fmt.Sprint(len(terminal))}, Body: base64.StdEncoding.EncodeToString(terminal)})
			if err := agent.Write(ctx, websocket.MessageText, response); err != nil {
				t.Fatal(err)
			}
			rest, err := io.ReadAll(reader)
			if err != nil {
				t.Fatal(err)
			}
			body := append([]byte{first}, rest...)
			if !bytes.Equal(bytes.TrimLeft(body, "\n"), terminal) {
				t.Fatalf("native terminal body changed: %q", body)
			}
			var envelope map[string]any
			if err := json.Unmarshal(body, &envelope); err != nil {
				t.Fatalf("browser JSON parse: %v", err)
			}
		})
	}
}

func contextWithCompactTimeout(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	t.Cleanup(cancel)
	return ctx
}

package api

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/NevermindZZT/dsh-manager/internal/config"
	"github.com/NevermindZZT/dsh-manager/internal/storage"
	"github.com/coder/websocket"
)

const compactNativeSuccess = `{"type":"server-response","rpcId":"compact-rpc","result":{"ok":true,"value":{"commandId":"cmd-1","result":{"kind":"success","text":"Compacted 1 history item."}}}}`

// Exercise a real socket WriteTimeout, not merely a recorder or an unbounded
// httptest server. The terminal result must survive the original socket budget.
func TestCompactWriteDeadlineGrace(t *testing.T) {
	for _, scenario := range []string{"success", "timeout", "timeout_stream_upload", "binary", "late_cookie", "bad_base64", "bad_schema"} {
		t.Run(scenario, func(t *testing.T) {
			db, err := storage.Open(t.TempDir() + "/manager.db")
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			srv := NewServer(config.Config{PairingCode: "deadline-pair"}, db, slog.New(slog.NewTextHandler(io.Discard, nil)))
			srv.compactHeartbeatInterval = 10 * time.Millisecond
			srv.compactProxyTimeout = 250 * time.Millisecond
			srv.compactWriteGrace = time.Second
			ts := httptest.NewUnstartedServer(srv.Handler())
			ts.Config.WriteTimeout = 100 * time.Millisecond
			ts.Start()
			defer ts.Close()
			caps := []string{"proxy.http", "proxy.cancel-v1"}
			if scenario == "timeout_stream_upload" {
				caps = append(caps, "proxy.http-request-stream-v1")
			}
			if scenario == "binary" {
				caps = append(caps, "proxy.binary-response-v1")
			}
			agent, agentID := enrollAndConnect(t, ts, srv, "deadline-pair", "deadline-proxy", caps)
			defer agent.CloseNow()
			jar := newProxyCookieJar()
			srv.webTargetsMu.Lock()
			target := srv.webTargets["deadline-proxy"]
			target.cookies = jar
			srv.webTargets["deadline-proxy"] = target
			srv.webTargetsMu.Unlock()
			srv.sessionsMu.RLock()
			owner := srv.sessions[agentID]
			srv.sessionsMu.RUnlock()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			req, _ := http.NewRequestWithContext(ctx, http.MethodPost, ts.URL+"/dsh/deadline-proxy/api/commands/execute", strings.NewReader(compactKeepaliveRequestBody))
			req.Header.Set("Content-Type", "application/json")
			type answer struct {
				r   *http.Response
				err error
			}
			browser := make(chan answer, 1)
			go func() { r, err := http.DefaultClient.Do(req); browser <- answer{r, err} }()
			_, raw, err := agent.Read(ctx)
			if err != nil {
				t.Fatal(err)
			}
			var proxied proxyRequest
			if err = json.Unmarshal(raw, &proxied); err != nil {
				t.Fatal(err)
			}
			if scenario == "timeout_stream_upload" {
				for {
					kind, chunk, readErr := agent.Read(ctx)
					if readErr != nil {
						t.Fatal(readErr)
					}
					if kind == websocket.MessageBinary {
						continue
					}
					var message agentMessage
					if err := json.Unmarshal(chunk, &message); err != nil {
						t.Fatal(err)
					}
					if message.Type != "proxy_request_end" {
						t.Fatalf("unexpected upload frame: %s", chunk)
					}
					break
				}
			}
			var result answer
			select {
			case result = <-browser:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if result.err != nil {
				t.Fatal(result.err)
			}
			defer result.r.Body.Close()
			reader := bufio.NewReader(result.r.Body)
			if b, err := reader.ReadByte(); err != nil || b != '\n' {
				t.Fatalf("heartbeat=%q error=%v", b, err)
			}
			if strings.HasPrefix(scenario, "timeout") {
				_, raw, err = agent.Read(ctx)
				if err != nil {
					t.Fatal(err)
				}
				var canceled agentMessage
				if err = json.Unmarshal(raw, &canceled); err != nil {
					t.Fatal(err)
				}
				if canceled.Type != "proxy_cancel" || canceled.RequestID != proxied.RequestID {
					t.Fatalf("missing upstream cancel: %s", raw)
				}
				var reason map[string]any
				_ = json.Unmarshal(raw, &reason)
				if reason["reason"] != "proxy_timeout" {
					t.Fatalf("wrong cancel reason: %s", raw)
				}
			} else {
				// Deliberately cross the socket's original 100ms write deadline.
				timer := time.NewTimer(140 * time.Millisecond)
				select {
				case <-timer.C:
				case <-ctx.Done():
					timer.Stop()
					t.Fatal(ctx.Err())
				}
				response := agentMessage{Type: "proxy_response", RequestID: proxied.RequestID, Status: 200, Headers: map[string]string{"Content-Type": "application/json"}, Body: base64.StdEncoding.EncodeToString([]byte(compactNativeSuccess))}
				if scenario == "late_cookie" {
					response.SetCookies = []string{"dsh-auth=rotated; Path=/; HttpOnly"}
				}
				if scenario == "bad_base64" {
					response.Body = "%%%"
				}
				if scenario == "bad_schema" {
					response.Body = base64.StdEncoding.EncodeToString([]byte(`{"type":"server-response","rpcId":"compact-rpc","result":{"ok":true,"summary":"not native"}}`))
				}
				if scenario == "binary" {
					err = agent.Write(ctx, websocket.MessageBinary, proxyBinaryMessage(agentMessage{Type: "proxy_response_binary", RequestID: proxied.RequestID, Status: 200, Headers: response.Headers}, []byte(compactNativeSuccess)))
				} else {
					raw, _ = json.Marshal(response)
					err = agent.Write(ctx, websocket.MessageText, raw)
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			rest, err := io.ReadAll(reader)
			if err != nil {
				t.Fatalf("terminal response was truncated: %v", err)
			}
			body := append([]byte{'\n'}, rest...)
			if scenario == "success" || scenario == "binary" {
				if strings.TrimSpace(string(body)) != compactNativeSuccess {
					t.Fatalf("native reply changed: %s", body)
				}
			} else {
				assertCompactFailureEnvelope(t, body)
			}
			if jar.requestHeader("/", "") != "" {
				t.Fatal("late cookie changed Manager authentication state")
			}
			for _, cookie := range result.r.Header.Values("Set-Cookie") {
				if strings.Contains(cookie, "dsh-auth=") {
					t.Fatal("late DSH cookie reached browser")
				}
			}
			end := time.NewTimer(time.Second)
			defer end.Stop()
			tick := time.NewTicker(time.Millisecond)
			defer tick.Stop()
			for {
				srv.pendingMu.Lock()
				pending := len(srv.pending)
				srv.pendingMu.Unlock()
				if pending == 0 && owner.activeHTTP.Load() == 0 {
					break
				}
				select {
				case <-tick.C:
				case <-end.C:
					t.Fatalf("leaked pending=%d slots=%d", pending, owner.activeHTTP.Load())
				}
			}
		})
	}
}

func TestCompactDeadlineFailsClosedWithoutControllerSupport(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "/api/commands/execute", nil)
	ctx, cancel, _, err := compactRequestDeadline(httptest.NewRecorder(), r, time.Second, time.Second)
	if err == nil || ctx != nil || cancel != nil {
		t.Fatal("unsupported response deadlines must not start an upstream compact")
	}
}

package api

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/NevermindZZT/dsh-manager/internal/config"
	"github.com/NevermindZZT/dsh-manager/internal/storage"
	"github.com/coder/websocket"
)

// Opt-in wall-clock test: same 300s HTTP WriteTimeout, task budget, 15s heartbeat
// and streaming upload negotiation as production. No model is called: the Agent
// deliberately withholds its reply so this boundary is deterministic.
func TestCompactProductionTimeout(t *testing.T) {
	if os.Getenv("DSH_MANAGER_FULL_TIMEOUT_TEST") != "1" {
		t.Skip("set DSH_MANAGER_FULL_TIMEOUT_TEST=1 for the real five-minute socket test")
	}
	db, err := storage.Open(t.TempDir() + "/manager.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	srv := NewServer(config.Config{PairingCode: "long-pair"}, db, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ts := httptest.NewUnstartedServer(srv.Handler())
	ts.Config.WriteTimeout = proxyHTTPTimeout
	ts.Start()
	defer ts.Close()
	agent, id := enrollAndConnect(t, ts, srv, "long-pair", "long-compact", []string{"proxy.http", "proxy.cancel-v1", "proxy.http-request-stream-v1", "proxy.binary-response-v1"})
	defer agent.CloseNow()
	srv.sessionsMu.RLock()
	owner := srv.sessions[id]
	srv.sessionsMu.RUnlock()
	ctx, cancel := context.WithTimeout(context.Background(), proxyHTTPTimeout+30*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, ts.URL+"/dsh/long-compact/api/commands/execute", strings.NewReader(compactKeepaliveRequestBody))
	req.Header.Set("Content-Type", "application/json")
	type result struct {
		response *http.Response
		err      error
	}
	answers := make(chan result, 1)
	start := time.Now()
	go func() { r, err := ts.Client().Do(req); answers <- result{r, err} }()
	_, data, err := agent.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var proxied proxyRequest
	if err = json.Unmarshal(data, &proxied); err != nil {
		t.Fatal(err)
	}
	if proxied.Type != "proxy_request_start" {
		t.Fatalf("unexpected request %s", data)
	}
	for {
		kind, frame, err := agent.Read(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if kind == websocket.MessageBinary {
			continue
		}
		var m agentMessage
		if err = json.Unmarshal(frame, &m); err != nil {
			t.Fatal(err)
		}
		if m.Type != "proxy_request_end" {
			t.Fatalf("unexpected upload frame %s", frame)
		}
		break
	}
	answer := <-answers
	if answer.err != nil {
		t.Fatal(answer.err)
	}
	defer answer.response.Body.Close()
	reader := bufio.NewReader(answer.response.Body)
	first, err := reader.ReadByte()
	if err != nil || first != '\n' {
		t.Fatalf("first heartbeat %q error %v", first, err)
	}
	firstMS := time.Since(start).Milliseconds()
	// The browser continues reading while the Manager ends the Agent task.
	restResult := make(chan struct {
		body []byte
		err  error
	}, 1)
	go func() {
		body, err := io.ReadAll(reader)
		restResult <- struct {
			body []byte
			err  error
		}{body, err}
	}()
	_, data, err = agent.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var canceled map[string]any
	if err = json.Unmarshal(data, &canceled); err != nil {
		t.Fatal(err)
	}
	if canceled["type"] != "proxy_cancel" || canceled["requestId"] != proxied.RequestID || canceled["reason"] != "proxy_timeout" {
		t.Fatalf("missing timeout cancellation: %s", data)
	}
	rest := <-restResult
	if rest.err != nil {
		t.Fatalf("production response truncated: %v", rest.err)
	}
	body := append([]byte{'\n'}, rest.body...)
	assertCompactFailureEnvelope(t, body)
	elapsed := time.Since(start)
	if elapsed < proxyHTTPTimeout || elapsed > proxyHTTPTimeout+compactTerminalWriteGrace {
		t.Fatalf("budget not respected: %s", elapsed)
	}
	pendingLifecycleAssertDrained(t, srv, owner)
	var envelope any
	if err = json.Unmarshal(body, &envelope); err != nil {
		t.Fatal(err)
	}
	report := map[string]any{"firstHeartbeatMs": firstMS, "elapsedMs": elapsed.Milliseconds(), "httpStatus": answer.response.StatusCode, "nativeRPC": envelope, "cancelReason": canceled["reason"], "activeHTTP": owner.activeHTTP.Load(), "completeBody": true}
	encoded, _ := json.MarshalIndent(report, "", "  ")
	t.Log(string(encoded))
	if path := os.Getenv("DSH_MANAGER_LONG_TEST_REPORT"); path != "" {
		if err = os.WriteFile(path, append(encoded, '\n'), 0600); err != nil {
			t.Fatal(err)
		}
	}
}

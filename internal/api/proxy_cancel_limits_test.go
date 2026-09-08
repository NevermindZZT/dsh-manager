package api

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/NevermindZZT/dsh-manager/internal/config"
	"github.com/NevermindZZT/dsh-manager/internal/storage"
)

func TestProxyHTTPClientCancellationSendsNegotiatedCancel(t *testing.T) {
	db, err := storage.Open(t.TempDir() + "/manager.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	srv := NewServer(config.Config{PairingCode: "cancel-pair", AdminToken: "cancel-admin"}, db, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	agent, _ := enrollAndConnect(t, ts, srv, "cancel-pair", "cancel-proxy", []string{"proxy.http", "proxy.cancel-v1"})
	defer agent.CloseNow()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/dsh/cancel-proxy/api/session.list", nil)
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { _, err := http.DefaultClient.Do(req); result <- err }()
	_, data, err := agent.Read(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var proxied proxyRequest
	if err := json.Unmarshal(data, &proxied); err != nil {
		t.Fatal(err)
	}
	cancel()

	readCtx, readCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer readCancel()
	_, data, err = agent.Read(readCtx)
	if err != nil {
		t.Fatalf("read proxy_cancel: %v", err)
	}
	var canceled map[string]string
	if err := json.Unmarshal(data, &canceled); err != nil {
		t.Fatal(err)
	}
	if canceled["type"] != "proxy_cancel" || canceled["requestId"] != proxied.RequestID || canceled["instanceId"] != "local" || canceled["reason"] != "client_disconnected" {
		t.Fatalf("unexpected cancel: %#v", canceled)
	}
	if err := <-result; err == nil {
		t.Fatal("canceled browser request unexpectedly succeeded")
	}
}

func TestProxyHTTPPerAgentLimitReturns429(t *testing.T) {
	session := &agentSession{agentID: "limited", hasCapabilities: true, capabilities: []string{"proxy.http"}}
	for range proxyHTTPMaxInFlight {
		if !session.tryAcquireHTTP() {
			t.Fatal("failed to fill HTTP proxy slots")
		}
	}
	srv := &Server{logger: slog.New(slog.NewTextHandler(io.Discard, nil)), sessions: map[string]*agentSession{"limited": session}}
	req := httptest.NewRequest(http.MethodGet, "/asset", nil)
	res := httptest.NewRecorder()
	srv.proxyHTTPForTarget(res, req, webTarget{AgentID: "limited", InstanceID: "local"}, "", false)
	if res.Code != http.StatusTooManyRequests {
		t.Fatalf("status=%d, want 429", res.Code)
	}
	if got := res.Header().Get("Retry-After"); got != "1" {
		t.Fatalf("Retry-After=%q", got)
	}
	if got := srv.proxyStats.httpRejected.Load(); got != 1 {
		t.Fatalf("rejected=%d", got)
	}
	for range proxyHTTPMaxInFlight {
		session.releaseHTTP()
	}
	if got := session.activeHTTP.Load(); got != 0 {
		t.Fatalf("active HTTP=%d", got)
	}
}

func TestStreamOverflowIncrementsCounter(t *testing.T) {
	stream := newProxyStream()
	for range proxyStreamQueue {
		stream.chunks <- proxyChunk{}
	}
	srv := &Server{logger: slog.New(slog.NewTextHandler(io.Discard, nil)), pending: map[string]*proxyStream{"proxy-overflow": stream}}
	if err := srv.handleAgentBinaryMessage("agent", proxyBinaryMessage(agentMessage{Type: "proxy_response_chunk_binary", RequestID: "proxy-overflow"}, []byte("x"))); err != nil {
		t.Fatal(err)
	}
	if got := srv.proxyStats.streamOverflow.Load(); got != 1 {
		t.Fatalf("stream overflow=%d", got)
	}
}

func TestTunnelDropIncrementsCounter(t *testing.T) {
	ch := make(chan agentMessage, 1)
	ch <- agentMessage{}
	srv := &Server{logger: slog.New(slog.NewTextHandler(io.Discard, nil)), tunnels: map[string]chan agentMessage{"ws-overflow": ch}}
	srv.dispatchTunnelMessage("ws-overflow", agentMessage{Type: "proxy_ws_frame"})
	if got := srv.proxyStats.tunnelDropped.Load(); got != 1 {
		t.Fatalf("tunnel drops=%d", got)
	}
}

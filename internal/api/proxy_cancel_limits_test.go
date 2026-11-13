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

func TestProxyHTTPPerAgentLimitWaitsForSlot(t *testing.T) {
	session := &agentSession{agentID: "limited"}
	for range proxyHTTPMaxInFlight {
		if !session.tryAcquireHTTP() {
			t.Fatal("failed to fill HTTP proxy slots")
		}
	}

	type acquireResult struct {
		acquired bool
		queued   bool
	}
	result := make(chan acquireResult, 1)
	go func() {
		acquired, queued := session.acquireHTTP(context.Background())
		result <- acquireResult{acquired: acquired, queued: queued}
	}()

	deadline := time.Now().Add(time.Second)
	for session.httpWaiters.Load() != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := session.httpWaiters.Load(); got != 1 {
		t.Fatalf("queued HTTP requests=%d, want 1", got)
	}
	session.releaseHTTP()
	select {
	case got := <-result:
		if !got.acquired || !got.queued {
			t.Fatalf("acquire result=%#v, want acquired and queued", got)
		}
	case <-time.After(time.Second):
		t.Fatal("queued HTTP request did not acquire the released slot")
	}
	for range proxyHTTPMaxInFlight {
		session.releaseHTTP()
	}
	if got := session.activeHTTP.Load(); got != 0 {
		t.Fatalf("active HTTP=%d", got)
	}
}

func TestProxyHTTPPerAgentLimitStillBoundsQueue(t *testing.T) {
	session := &agentSession{agentID: "limited", hasCapabilities: true, capabilities: []string{"proxy.http"}}
	for range proxyHTTPMaxInFlight {
		if !session.tryAcquireHTTP() {
			t.Fatal("failed to fill HTTP proxy slots")
		}
	}
	// Simulate a saturated bounded waiting room so the handler can fail fast.
	session.httpWaiters.Store(proxyHTTPMaxQueued)
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
	if got := srv.proxyStats.httpQueueRejected.Load(); got != 1 {
		t.Fatalf("queue rejected=%d", got)
	}
	session.httpWaiters.Store(0)
	for range proxyHTTPMaxInFlight {
		session.releaseHTTP()
	}
}

func TestStreamOverflowIncrementsCounter(t *testing.T) {
	stream := newProxyStream()
	for range proxyStreamQueue {
		stream.chunks <- proxyChunk{}
	}
	srv := &Server{logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	session := &agentSession{agentID: "agent"}
	srv.installAgentSession(session)
	if err := srv.registerPendingStream(session, "proxy-overflow", stream); err != nil {
		t.Fatal(err)
	}
	if err := srv.handleAgentBinaryMessage("agent", proxyBinaryMessage(agentMessage{Type: "proxy_response_chunk_binary", RequestID: "proxy-overflow"}, []byte("x"))); err != nil {
		t.Fatal(err)
	}
	if got := srv.proxyStats.streamOverflow.Load(); got != 1 {
		t.Fatalf("stream overflow=%d", got)
	}
}

func TestTunnelOverflowAbortsForRecoverableReconnect(t *testing.T) {
	cancelled := make(chan struct{})
	tunnel := &browserTunnel{
		messages: make(chan agentMessage, 1),
		overflow: make(chan struct{}),
		cancel:   func() { close(cancelled) },
	}
	tunnel.messages <- agentMessage{Type: "proxy_ws_frame", Body: "queued"}
	srv := &Server{logger: slog.New(slog.NewTextHandler(io.Discard, nil)), tunnels: map[string]*browserTunnel{"ws-overflow": tunnel}}
	srv.dispatchTunnelMessage("ws-overflow", agentMessage{Type: "proxy_ws_frame", Body: "overflow"})
	if !tunnel.failed.Load() {
		t.Fatal("full tunnel queue must abort the logical WebSocket")
	}
	select {
	case <-tunnel.overflow:
	default:
		t.Fatal("overflow signal was not delivered")
	}
	select {
	case <-cancelled:
	default:
		t.Fatal("overflow did not cancel the blocked browser write context")
	}
	if got := srv.proxyStats.tunnelOverflow.Load(); got != 1 {
		t.Fatalf("tunnel overflow=%d, want 1", got)
	}
	if got := srv.proxyStats.tunnelDropped.Load(); got != 0 {
		t.Fatalf("legacy silent-drop counter=%d, want 0", got)
	}
}

func TestTunnelQueuePreservesFramesWithinBudget(t *testing.T) {
	tunnel := newBrowserTunnel(nil)
	message := agentMessage{Type: "proxy_ws_frame", Body: "in-order"}
	if !tunnel.enqueue(message) {
		t.Fatal("expected frame to fit in empty tunnel queue")
	}
	got := <-tunnel.messages
	if got.Body != message.Body {
		t.Fatalf("body=%q, want %q", got.Body, message.Body)
	}
	tunnel.release(got)
	if queued := tunnel.queuedBytes.Load(); queued != 0 {
		t.Fatalf("queued bytes=%d, want 0", queued)
	}
}

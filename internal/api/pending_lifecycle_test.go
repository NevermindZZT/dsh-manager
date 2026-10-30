package api

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/NevermindZZT/dsh-manager/internal/config"
	"github.com/NevermindZZT/dsh-manager/internal/storage"
	"github.com/coder/websocket"
)

func TestPendingLifecycleDisconnectFailsOnlyOwner(t *testing.T) {
	srv := &Server{}
	owner, other := &agentSession{agentID: "one"}, &agentSession{agentID: "two"}
	srv.installAgentSession(owner)
	srv.installAgentSession(other)
	owned, unrelated := newProxyStream(), newProxyStream()
	if err := srv.registerPendingStream(owner, "owned", owned); err != nil {
		t.Fatal(err)
	}
	if err := srv.registerPendingStream(other, "other", unrelated); err != nil {
		t.Fatal(err)
	}
	if !srv.removeAgentSession(owner) {
		t.Fatal("current connection was not removed")
	}
	select {
	case err := <-owned.errors:
		if !errors.Is(err, errAgentSessionDisconnected) {
			t.Fatalf("disconnect error = %v", err)
		}
	default:
		t.Fatal("disconnect did not synchronously fail pending")
	}
	if srv.pendingStream("owned") != nil || srv.pendingStream("other") != unrelated {
		t.Fatal("disconnect did not isolate pending ownership")
	}
	select {
	case err := <-unrelated.errors:
		t.Fatalf("unrelated request failed: %v", err)
	default:
	}
	if err := srv.registerPendingStream(owner, "late", newProxyStream()); !errors.Is(err, errAgentSessionDisconnected) {
		t.Fatalf("registration after disconnect = %v", err)
	}
}

func TestPendingLifecycleReplacementRejectsBothDirectionsOfSpoofing(t *testing.T) {
	srv := &Server{}
	old := &agentSession{agentID: "same"}
	current := &agentSession{agentID: "same"}
	srv.installAgentSession(old)
	oldStream := newProxyStream()
	if err := srv.registerPendingStream(old, "old", oldStream); err != nil {
		t.Fatal(err)
	}
	if previous := srv.installAgentSession(current); previous != old {
		t.Fatal("replacement did not return old connection")
	}
	newStream := newProxyStream()
	if err := srv.registerPendingStream(current, "new", newStream); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		session *agentSession
		id      string
	}{{current, "old"}, {old, "new"}, {old, "old"}, {&agentSession{agentID: "same"}, "new"}} {
		if srv.withSessionPendingStream(tc.session, tc.id, func(*proxyStream) { t.Error("spoofed response delivered") }) {
			t.Fatalf("accepted response from wrong connection for %q", tc.id)
		}
	}
	if srv.removeAgentSession(old) {
		t.Fatal("old close removed replacement")
	}
	if srv.pendingStream("new") != newStream {
		t.Fatal("old close removed replacement's request")
	}
	if !srv.withSessionPendingStream(current, "new", func(p *proxyStream) { p.deliverHeader(proxyResponse{Status: 201}) }) {
		t.Fatal("current owner could not deliver response")
	}
	if response := <-newStream.headers; response.Status != 201 {
		t.Fatalf("status = %d", response.Status)
	}
	select {
	case <-oldStream.errors:
	default:
		t.Fatal("replacement did not fail old request")
	}
	select {
	case err := <-newStream.errors:
		t.Fatalf("replacement request failed: %v", err)
	default:
	}
}

func TestPendingLifecycleCleanupUsesStreamIdentity(t *testing.T) {
	srv := &Server{}
	session := &agentSession{agentID: "one"}
	srv.installAgentSession(session)
	old, current := newProxyStream(), newProxyStream()
	if err := srv.registerPendingStream(session, "reused", old); err != nil {
		t.Fatal(err)
	}
	if err := srv.registerPendingStream(session, "reused", current); err == nil {
		t.Fatal("duplicate ID replaced active request")
	}
	srv.unregisterPendingStream("reused", old)
	if err := srv.registerPendingStream(session, "reused", current); err != nil {
		t.Fatal(err)
	}
	srv.unregisterPendingStream("reused", old)
	if srv.pendingStream("reused") != current {
		t.Fatal("late cleanup deleted another stream")
	}
}

func TestPendingLifecycleRegistrationRacesDisconnect(t *testing.T) {
	for i := range 100 {
		srv := &Server{}
		session := &agentSession{agentID: "racing"}
		srv.installAgentSession(session)
		stream := newProxyStream()
		start := make(chan struct{})
		var wg sync.WaitGroup
		var registration error
		wg.Add(2)
		go func() { defer wg.Done(); <-start; registration = srv.registerPendingStream(session, "request", stream) }()
		go func() { defer wg.Done(); <-start; srv.removeAgentSession(session) }()
		close(start)
		wg.Wait()
		if srv.pendingStream("request") != nil {
			t.Fatalf("iteration %d leaked pending", i)
		}
		if registration == nil {
			select {
			case <-stream.errors:
			default:
				t.Fatalf("iteration %d registered request was not failed", i)
			}
		} else if !errors.Is(registration, errAgentSessionDisconnected) {
			t.Fatal(registration)
		}
	}
}

func TestPendingLifecycleDeliveryRacesReplacement(t *testing.T) {
	srv := &Server{}
	old, current := &agentSession{agentID: "same"}, &agentSession{agentID: "same"}
	srv.installAgentSession(old)
	stream := newProxyStream()
	if err := srv.registerPendingStream(old, "old", stream); err != nil {
		t.Fatal(err)
	}
	started, release, replaced := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		srv.withSessionPendingStream(old, "old", func(p *proxyStream) {
			close(started)
			<-release // Deliberately pause delivery to exercise the lock boundary.
			p.deliverHeader(proxyResponse{Status: 200})
		})
	}()
	<-started
	go func() { srv.installAgentSession(current); close(replaced) }()
	close(release)
	select {
	case <-replaced:
	case <-time.After(time.Second):
		t.Fatal("delivery/replacement lock order deadlocked")
	}
	if srv.withSessionPendingStream(old, "old", func(*proxyStream) { t.Error("retired delivery") }) {
		t.Fatal("accepted delivery after replacement completed")
	}
}

func TestPendingLifecyclePinnedWriterDoesNotRetryReplacement(t *testing.T) {
	closed := &outboundScheduler{}
	closed.stopped.Store(true)
	old := &agentSession{agentID: "same", scheduler: closed}
	// A cross-connection retry would dereference the nil replacement socket.
	current := &agentSession{agentID: "same"}
	srv := &Server{sessions: map[string]*agentSession{"same": current}}
	if err := srv.writeAgentJSONForSession(context.Background(), old, proxyRequest{Type: "proxy_request"}, outboundInteractive); !errors.Is(err, errOutboundClosed) {
		t.Fatalf("pinned write = %v", err)
	}
}

// The integration tests below require server.go to use install/removeAgentSession,
// register/unregisterPendingStream, and actual-reader session-scoped delivery.
// They intentionally exercise the public WebSocket/HTTP handlers rather than
// manually invoking lifecycle helpers to make a missing integration fail.
func pendingLifecycleFixture(t *testing.T) (*Server, *httptest.Server, func() *websocket.Conn) {
	t.Helper()
	db, err := storage.Open(t.TempDir() + "/manager.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.CreateAgent("pending-agent", "pending", "linux", "test", "pending-token"); err != nil {
		t.Fatal(err)
	}
	srv := NewServer(config.Config{}, db, slog.New(slog.NewTextHandler(io.Discard, nil)))
	srv.webTargets["pending-target"] = webTarget{AgentID: "pending-agent", InstanceID: "local", ExpiresAt: time.Now().Add(time.Hour)}
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	connect := func() *websocket.Conn {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(ts.URL, "http")+"/api/v1/agent/connect", &websocket.DialOptions{HTTPHeader: http.Header{"Authorization": {"Bearer pending-token"}, "X-Agent-Id": {"pending-agent"}}})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = conn.CloseNow() })
		if _, _, err := conn.Read(ctx); err != nil {
			t.Fatal(err)
		}
		return conn
	}
	return srv, ts, connect
}

type pendingHTTPResult struct {
	status int
	body   string
	err    error
}

func pendingLifecycleRequest(t *testing.T, ts *httptest.Server) <-chan pendingHTTPResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	t.Cleanup(cancel)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/dsh/pending-target/api/session.list", nil)
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan pendingHTTPResult, 1)
	go func() {
		response, err := ts.Client().Do(req)
		if err != nil {
			result <- pendingHTTPResult{err: err}
			return
		}
		defer response.Body.Close()
		body, err := io.ReadAll(response.Body)
		result <- pendingHTTPResult{status: response.StatusCode, body: string(body), err: err}
	}()
	return result
}

func pendingLifecycleReadRequest(t *testing.T, conn *websocket.Conn) proxyRequest {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var request proxyRequest
	if err := json.Unmarshal(data, &request); err != nil {
		t.Fatal(err)
	}
	if request.Type != "proxy_request" {
		t.Fatalf("unexpected request %q", data)
	}
	return request
}

func pendingLifecycleAssertDrained(t *testing.T, srv *Server, session *agentSession) {
	t.Helper()
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		srv.pendingMu.Lock()
		count := 0
		for _, stream := range srv.pending {
			if stream.session == session {
				count++
			}
		}
		srv.pendingMu.Unlock()
		if count == 0 && session.activeHTTP.Load() == 0 {
			return
		}
		select {
		case <-tick.C:
		case <-deadline.C:
			t.Fatalf("connection leaked pending=%d slots=%d", count, session.activeHTTP.Load())
		}
	}
}

func TestPendingLifecycleHandlersRejectForeignAndStaleReaders(t *testing.T) {
	srv := &Server{logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	owner, foreign := &agentSession{agentID: "owner"}, &agentSession{agentID: "foreign"}
	srv.installAgentSession(owner)
	srv.installAgentSession(foreign)
	stream := newProxyStream()
	if err := srv.registerPendingStream(owner, "owned", stream); err != nil {
		t.Fatal(err)
	}
	for _, reader := range []*agentSession{foreign, {agentID: "owner"}} {
		for _, typ := range []string{"proxy_response", "proxy_response_start", "proxy_response_end"} {
			if err := srv.handleAgentSessionMessage(reader, agentMessage{Type: typ, RequestID: "owned", Status: 599, Error: "spoofed"}); err != nil {
				t.Fatal(err)
			}
		}
		for _, typ := range []string{"proxy_response_binary", "proxy_response_chunk_binary"} {
			if err := srv.handleAgentSessionBinaryMessage(reader, proxyBinaryMessage(agentMessage{Type: typ, RequestID: "owned", Status: 599, Final: true}, []byte("spoofed"))); err != nil {
				t.Fatal(err)
			}
		}
	}
	if len(stream.headers) != 0 || len(stream.chunks) != 0 || len(stream.errors) != 0 {
		t.Fatal("foreign or same-ID stale reader delivered a response")
	}
	if err := srv.handleAgentSessionBinaryMessage(owner, proxyBinaryMessage(agentMessage{Type: "proxy_response_binary", RequestID: "owned", Status: 200}, []byte("owned"))); err != nil {
		t.Fatal(err)
	}
	if response := <-stream.headers; !response.BodyBytesSet || string(response.BodyBytes) != "owned" {
		t.Fatalf("owner response = %+v", response)
	}
}

func TestPendingLifecycleAgentDisconnectUnblocksStream(t *testing.T) {
	srv, ts, connect := pendingLifecycleFixture(t)
	agent := connect()
	srv.sessionsMu.RLock()
	session := srv.sessions["pending-agent"]
	srv.sessionsMu.RUnlock()
	session.metadataMu.Lock()
	session.hasCapabilities = true
	session.capabilities = []string{"proxy.http", "proxy.http-stream-v1"}
	session.metadataMu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/dsh/pending-target/assets/pending-abcdef.bin", nil)
	if err != nil {
		t.Fatal(err)
	}
	headersReady := make(chan struct{})
	result := make(chan pendingHTTPResult, 1)
	go func() {
		response, err := ts.Client().Do(req)
		close(headersReady)
		if err != nil {
			result <- pendingHTTPResult{err: err}
			return
		}
		defer response.Body.Close()
		body, err := io.ReadAll(response.Body)
		result <- pendingHTTPResult{status: response.StatusCode, body: string(body), err: err}
	}()
	request := pendingLifecycleReadRequest(t, agent)
	if !request.StreamResponse {
		t.Fatal("manager did not negotiate a streamed response")
	}
	start, err := json.Marshal(agentMessage{Type: "proxy_response_start", RequestID: request.RequestID, Status: 200, Headers: map[string]string{"Content-Type": "application/octet-stream"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := agent.Write(ctx, websocket.MessageText, start); err != nil {
		t.Fatal(err)
	}
	// Exceed net/http's output buffer so receipt of headers proves the HTTP
	// handler is waiting for more stream chunks rather than the initial header.
	partial := strings.Repeat("partial-", 2048)
	if err := agent.Write(ctx, websocket.MessageBinary, proxyBinaryMessage(agentMessage{Type: "proxy_response_chunk_binary", RequestID: request.RequestID}, []byte(partial))); err != nil {
		t.Fatal(err)
	}
	select {
	case <-headersReady:
	case <-ctx.Done():
		t.Fatal("stream did not start")
	}
	_ = agent.CloseNow()
	select {
	case response := <-result:
		// A post-header disconnect may terminate the HTTP stream with EOF or
		// an abrupt transport error. It must not wait for the proxy timeout.
		if response.status != 200 || response.body != partial {
			t.Fatalf("interrupted stream = %+v", response)
		}
	case <-time.After(time.Second):
		t.Fatal("agent disconnect left streamed HTTP request pending")
	}
	pendingLifecycleAssertDrained(t, srv, session)
}

func TestPendingLifecycleAgentDisconnectUnblocksHTTP(t *testing.T) {
	srv, ts, connect := pendingLifecycleFixture(t)
	agent := connect()
	srv.sessionsMu.RLock()
	session := srv.sessions["pending-agent"]
	srv.sessionsMu.RUnlock()
	result := pendingLifecycleRequest(t, ts)
	_ = pendingLifecycleReadRequest(t, agent)
	_ = agent.CloseNow()
	select {
	case response := <-result:
		if response.err != nil || response.status != http.StatusBadGateway {
			t.Fatalf("disconnect response = %+v", response)
		}
	case <-time.After(time.Second):
		t.Fatal("agent disconnect left HTTP request pending")
	}
	pendingLifecycleAssertDrained(t, srv, session)
}

func TestPendingLifecycleAgentReplacementUnblocksOldHTTPAndKeepsNew(t *testing.T) {
	srv, ts, connect := pendingLifecycleFixture(t)
	old := connect()
	srv.sessionsMu.RLock()
	oldSession := srv.sessions["pending-agent"]
	srv.sessionsMu.RUnlock()
	oldResult := pendingLifecycleRequest(t, ts)
	oldRequest := pendingLifecycleReadRequest(t, old)
	current := connect()
	select {
	case response := <-oldResult:
		if response.err != nil || response.status != http.StatusBadGateway {
			t.Fatalf("replacement response = %+v", response)
		}
	case <-time.After(time.Second):
		t.Fatal("replacement left old HTTP request pending")
	}
	pendingLifecycleAssertDrained(t, srv, oldSession)
	result := pendingLifecycleRequest(t, ts)
	request := pendingLifecycleReadRequest(t, current)
	// Re-run old cleanup after a new request has registered. This must be a no-op.
	if srv.removeAgentSession(oldSession) {
		t.Fatal("late old cleanup removed replacement")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for _, response := range []agentMessage{
		{Type: "proxy_response", RequestID: oldRequest.RequestID, Status: 599, Body: base64.StdEncoding.EncodeToString([]byte("stale"))},
		{Type: "proxy_response", RequestID: request.RequestID, Status: http.StatusOK, Body: base64.StdEncoding.EncodeToString([]byte("replacement response"))},
	} {
		data, err := json.Marshal(response)
		if err != nil {
			t.Fatal(err)
		}
		if err := current.Write(ctx, websocket.MessageText, data); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case response := <-result:
		if response.err != nil || response.status != http.StatusOK || response.body != "replacement response" {
			t.Fatalf("new connection request = %+v", response)
		}
	case <-time.After(time.Second):
		t.Fatal("replacement request did not complete")
	}
	srv.sessionsMu.RLock()
	currentSession := srv.sessions["pending-agent"]
	srv.sessionsMu.RUnlock()
	if currentSession == nil {
		t.Fatal("old disconnect removed new session")
	}
	pendingLifecycleAssertDrained(t, srv, currentSession)
}

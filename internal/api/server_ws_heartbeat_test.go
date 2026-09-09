package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func TestStartBrowserWebSocketHeartbeatResponsivePeer(t *testing.T) {
	serverConn := make(chan *websocket.Conn, 1)
	release := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Errorf("accept websocket: %v", err)
			return
		}
		serverConn <- conn
		<-release
		_ = conn.CloseNow()
	}))
	defer ts.Close()
	defer close(release)

	client, _, err := websocket.Dial(context.Background(), "ws"+strings.TrimPrefix(ts.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseNow()
	// The peer must read control frames for coder/websocket to observe pongs.
	client.CloseRead(context.Background())
	conn := <-serverConn
	defer conn.CloseNow()

	heartbeatErrors := startBrowserWebSocketHeartbeat(context.Background(), conn, 5*time.Millisecond, 100*time.Millisecond)
	select {
	case err := <-heartbeatErrors:
		t.Fatalf("responsive peer heartbeat failed: %v", err)
	case <-time.After(40 * time.Millisecond):
	}
}

func TestStartBrowserWebSocketHeartbeatDetectsUnresponsivePeer(t *testing.T) {
	serverConn := make(chan *websocket.Conn, 1)
	release := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		serverConn <- conn
		<-release
		_ = conn.CloseNow()
	}))
	defer ts.Close()
	defer close(release)

	client, _, err := websocket.Dial(context.Background(), "ws"+strings.TrimPrefix(ts.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseNow()
	conn := <-serverConn
	defer conn.CloseNow()

	heartbeatErrors := startBrowserWebSocketHeartbeat(context.Background(), conn, 5*time.Millisecond, 20*time.Millisecond)
	select {
	case err := <-heartbeatErrors:
		if err == nil {
			t.Fatal("expected heartbeat failure")
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("heartbeat did not detect unresponsive peer")
	}
}

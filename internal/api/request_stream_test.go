package api

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/NevermindZZT/dsh-manager/internal/config"
	"github.com/NevermindZZT/dsh-manager/internal/storage"
	"github.com/coder/websocket"
)

func TestHTTPProxyStreamsRequestBodyWhenNegotiated(t *testing.T) {
	db, err := storage.Open(t.TempDir() + "/manager.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	srv := NewServer(config.Config{PairingCode: "request-stream-pair", AdminToken: "request-stream-admin"}, db, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	agent, _ := enrollAndConnect(t, ts, srv, "request-stream-pair", "request-stream", []string{"proxy.http", "proxy.http-request-stream-v1"})
	agent.SetReadLimit(agentMessageReadLimit)
	defer agent.CloseNow()

	body := bytes.Repeat([]byte("x"), proxyChunkSize+17)
	result := make(chan *http.Response, 1)
	go func() {
		response, err := http.Post(ts.URL+"/dsh/request-stream/api/upload", "application/octet-stream", bytes.NewReader(body))
		if err != nil {
			t.Errorf("proxy request: %v", err)
			return
		}
		result <- response
	}()

	messageType, data, err := agent.Read(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if messageType != websocket.MessageText {
		t.Fatalf("start message type=%v", messageType)
	}
	var start proxyRequest
	if err := json.Unmarshal(data, &start); err != nil {
		t.Fatal(err)
	}
	if start.Type != "proxy_request_start" || !start.StreamRequest || start.Body != "" || start.Path != "/api/upload" {
		t.Fatalf("unexpected start: %+v", start)
	}

	var received []byte
	for {
		messageType, data, err = agent.Read(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if messageType == websocket.MessageBinary {
			separator := bytes.IndexByte(data, '\n')
			var header agentMessage
			if separator < 1 || json.Unmarshal(data[:separator], &header) != nil || header.Type != "proxy_request_chunk_binary" || header.RequestID != start.RequestID {
				t.Fatalf("unexpected request chunk: %q", data)
			}
			if len(data[separator+1:]) > proxyChunkSize {
				t.Fatalf("chunk exceeds bound: %d", len(data[separator+1:]))
			}
			received = append(received, data[separator+1:]...)
			continue
		}
		var end agentMessage
		if json.Unmarshal(data, &end) != nil || end.Type != "proxy_request_end" || end.RequestID != start.RequestID || end.Error != "" {
			t.Fatalf("unexpected request end: %q", data)
		}
		break
	}
	if !bytes.Equal(received, body) {
		t.Fatalf("request body mismatch: got %d bytes, want %d", len(received), len(body))
	}
	response, _ := json.Marshal(agentMessage{Type: "proxy_response", RequestID: start.RequestID, Status: http.StatusOK, Body: base64.StdEncoding.EncodeToString([]byte("ok"))})
	if err := agent.Write(context.Background(), websocket.MessageText, response); err != nil {
		t.Fatal(err)
	}
	select {
	case r := <-result:
		defer r.Body.Close()
		if r.StatusCode != http.StatusOK {
			t.Fatalf("status=%d", r.StatusCode)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("proxy response timed out")
	}
}

func TestHTTPProxyRequestBodyFallsBackToBase64(t *testing.T) {
	db, err := storage.Open(t.TempDir() + "/manager.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	srv := NewServer(config.Config{PairingCode: "legacy-request-pair", AdminToken: "legacy-request-admin"}, db, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	agent, _ := enrollAndConnect(t, ts, srv, "legacy-request-pair", "legacy-request", []string{"proxy.http"})
	defer agent.CloseNow()

	result := make(chan *http.Response, 1)
	go func() {
		response, err := http.Post(ts.URL+"/dsh/legacy-request/api/upload", "text/plain", bytes.NewBufferString("legacy-body"))
		if err != nil {
			t.Errorf("proxy request: %v", err)
			return
		}
		result <- response
	}()
	_, data, err := agent.Read(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var request proxyRequest
	if err := json.Unmarshal(data, &request); err != nil {
		t.Fatal(err)
	}
	if request.Type != "proxy_request" || request.StreamRequest || request.Body != base64.StdEncoding.EncodeToString([]byte("legacy-body")) {
		t.Fatalf("legacy fallback changed: %+v", request)
	}
	response, _ := json.Marshal(agentMessage{Type: "proxy_response", RequestID: request.RequestID, Status: http.StatusOK, Body: base64.StdEncoding.EncodeToString([]byte("ok"))})
	if err := agent.Write(context.Background(), websocket.MessageText, response); err != nil {
		t.Fatal(err)
	}
	select {
	case r := <-result:
		defer r.Body.Close()
		if r.StatusCode != http.StatusOK {
			t.Fatalf("status=%d", r.StatusCode)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("legacy proxy response timed out")
	}
}

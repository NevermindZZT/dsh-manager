package api

import (
	"bytes"
	"compress/gzip"
	"context"
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

func TestHTTPProxyPreservesCompressedAssetsAndCacheHeaders(t *testing.T) {
	db, err := storage.Open(t.TempDir() + "/manager.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	srv := NewServer(config.Config{PairingCode: "gzip-pair", AdminToken: "gzip-admin"}, db, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	response, err := http.Post(ts.URL+"/api/v1/agents/enroll", "application/json", strings.NewReader(`{"pairingCode":"gzip-pair","name":"gzip-agent","platform":"linux"}`))
	if err != nil {
		t.Fatal(err)
	}
	var enrolled enrollResponse
	if err := json.NewDecoder(response.Body).Decode(&enrolled); err != nil {
		response.Body.Close()
		t.Fatal(err)
	}
	response.Body.Close()

	const sessionID = "gzip-proxy"
	srv.webTargets[sessionID] = webTarget{AgentID: enrolled.AgentID, InstanceID: "local", ExpiresAt: time.Now().Add(time.Hour)}
	agent, _, err := websocket.Dial(context.Background(), "ws"+strings.TrimPrefix(ts.URL, "http")+"/api/v1/agent/connect", &websocket.DialOptions{HTTPHeader: http.Header{
		"Authorization": {"Bearer " + enrolled.AgentToken},
		"X-Agent-Id":    {enrolled.AgentID},
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer agent.CloseNow()
	if _, _, err := agent.Read(context.Background()); err != nil {
		t.Fatal(err)
	}
	register, err := json.Marshal(agentMessage{
		Type:         "register",
		AgentType:    "launcher",
		Capabilities: []string{"proxy.http", "proxy.binary-response-v1"},
		Instances:    []storage.Instance{{InstanceID: "local", DisplayName: "Local", State: "running", URLAvailable: true}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := agent.Write(context.Background(), websocket.MessageText, register); err != nil {
		t.Fatal(err)
	}

	plain := []byte("compressed static asset")
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	if _, err := writer.Write(plain); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	resultCh := make(chan *http.Response, 1)
	go func() {
		client := &http.Client{Transport: &http.Transport{DisableCompression: true}}
		result, requestErr := client.Get(ts.URL + "/dsh/" + sessionID + "/assets/index-abc123.css")
		if requestErr != nil {
			t.Errorf("proxy request failed: %v", requestErr)
			return
		}
		resultCh <- result
	}()

	_, requestData, err := agent.Read(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var request proxyRequest
	if err := json.Unmarshal(requestData, &request); err != nil {
		t.Fatal(err)
	}
	if request.Path != "/assets/index-abc123.css" {
		t.Fatalf("unexpected proxy request path: %q", request.Path)
	}
	if !request.BinaryResponse {
		t.Fatal("manager did not request binary response transport")
	}

	proxyHeader, err := json.Marshal(agentMessage{
		Type:      "proxy_response_binary",
		RequestID: request.RequestID,
		Status:    http.StatusOK,
		Headers: map[string]string{
			"Content-Type":     "text/css",
			"Content-Encoding": "gzip",
			"Vary":             "Accept-Encoding",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	binaryResponse := append(append(proxyHeader, '\n'), compressed.Bytes()...)
	if err := agent.Write(context.Background(), websocket.MessageBinary, binaryResponse); err != nil {
		t.Fatal(err)
	}

	proxied := <-resultCh
	if proxied == nil {
		t.Fatal("proxy response missing")
	}
	defer proxied.Body.Close()
	if proxied.StatusCode != http.StatusOK {
		t.Fatalf("proxy status=%d", proxied.StatusCode)
	}
	if proxied.Header.Get("Content-Encoding") != "gzip" {
		t.Fatalf("content encoding=%q, want gzip", proxied.Header.Get("Content-Encoding"))
	}
	if !strings.Contains(proxied.Header.Get("Cache-Control"), "immutable") {
		t.Fatalf("cache control=%q, want immutable", proxied.Header.Get("Cache-Control"))
	}
	if !strings.Contains(proxied.Header.Get("Vary"), "Accept-Encoding") {
		t.Fatalf("vary=%q, want Accept-Encoding", proxied.Header.Get("Vary"))
	}
	received, err := io.ReadAll(proxied.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(received, compressed.Bytes()) {
		t.Fatalf("compressed body length=%d, want %d", len(received), compressed.Len())
	}
	reader, err := gzip.NewReader(bytes.NewReader(received))
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(decoded, plain) {
		t.Fatalf("decoded body=%q, want %q", decoded, plain)
	}
}

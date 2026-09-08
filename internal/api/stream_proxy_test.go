package api

import (
	"bytes"
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

func enrollAndConnect(t *testing.T, ts *httptest.Server, srv *Server, pairingCode, sessionID string, caps []string) (*websocket.Conn, string) {
	t.Helper()
	response, err := http.Post(ts.URL+"/api/v1/agents/enroll", "application/json", strings.NewReader("{\"pairingCode\":\""+pairingCode+"\",\"name\":\"agent\",\"platform\":\"linux\"}"))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var enrolled enrollResponse
	if err := json.NewDecoder(response.Body).Decode(&enrolled); err != nil {
		t.Fatal(err)
	}
	srv.webTargets[sessionID] = webTarget{AgentID: enrolled.AgentID, InstanceID: "local", ExpiresAt: time.Now().Add(time.Hour)}
	agent, _, err := websocket.Dial(context.Background(), "ws"+strings.TrimPrefix(ts.URL, "http")+"/api/v1/agent/connect", &websocket.DialOptions{HTTPHeader: http.Header{"Authorization": {"Bearer " + enrolled.AgentToken}, "X-Agent-Id": {enrolled.AgentID}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := agent.Read(context.Background()); err != nil {
		t.Fatal(err)
	}
	register, _ := json.Marshal(agentMessage{Type: "register", Capabilities: caps})
	if err := agent.Write(context.Background(), websocket.MessageText, register); err != nil {
		t.Fatal(err)
	}
	return agent, enrolled.AgentID
}

func TestHTTPProxyStreamsBinaryResponseChunks(t *testing.T) {
	db, err := storage.Open(t.TempDir() + "/manager.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	srv := NewServer(config.Config{PairingCode: "stream-pair", AdminToken: "stream-admin"}, db, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	agent, _ := enrollAndConnect(t, ts, srv, "stream-pair", "stream-proxy", []string{"proxy.http", "proxy.http-stream-v1"})
	defer agent.CloseNow()
	result := make(chan []byte, 1)
	go func() {
		r, e := http.Get(ts.URL + "/dsh/stream-proxy/assets/archive-abc123.bin")
		if e != nil {
			t.Errorf("proxy request: %v", e)
			return
		}
		defer r.Body.Close()
		if r.StatusCode != http.StatusOK {
			t.Errorf("status=%d", r.StatusCode)
			return
		}
		body, e := io.ReadAll(r.Body)
		if e != nil {
			t.Errorf("read response: %v", e)
			return
		}
		result <- body
	}()
	_, requestData, err := agent.Read(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var request proxyRequest
	if err := json.Unmarshal(requestData, &request); err != nil {
		t.Fatal(err)
	}
	if !request.StreamResponse {
		t.Fatal("manager did not request streamed response")
	}
	start, _ := json.Marshal(agentMessage{Type: "proxy_response_start", RequestID: request.RequestID, Status: http.StatusOK, Headers: map[string]string{"Content-Type": "application/octet-stream"}})
	if err := agent.Write(context.Background(), websocket.MessageText, start); err != nil {
		t.Fatal(err)
	}
	for _, chunk := range [][]byte{[]byte("first-"), []byte("second")} {
		if err := agent.Write(context.Background(), websocket.MessageBinary, proxyBinaryMessage(agentMessage{Type: "proxy_response_chunk_binary", RequestID: request.RequestID}, chunk)); err != nil {
			t.Fatal(err)
		}
	}
	end, _ := json.Marshal(agentMessage{Type: "proxy_response_end", RequestID: request.RequestID})
	if err := agent.Write(context.Background(), websocket.MessageText, end); err != nil {
		t.Fatal(err)
	}
	select {
	case body := <-result:
		if !bytes.Equal(body, []byte("first-second")) {
			t.Fatalf("body=%q", body)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("streamed response timed out")
	}
}

func TestBinaryWebSocketFramesAvoidBase64(t *testing.T) {
	db, err := storage.Open(t.TempDir() + "/manager.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	srv := NewServer(config.Config{PairingCode: "binary-pair", AdminToken: "binary-admin"}, db, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	agent, _ := enrollAndConnect(t, ts, srv, "binary-pair", "binary-proxy", []string{"proxy.websocket", "proxy.binary-websocket-frame-v1"})
	defer agent.CloseNow()
	browser, _, err := websocket.Dial(context.Background(), "ws"+strings.TrimPrefix(ts.URL, "http")+"/dsh/binary-proxy/", &websocket.DialOptions{HTTPHeader: http.Header{"Cookie": {"dsh-target=binary-proxy"}}})
	if err != nil {
		t.Fatal(err)
	}
	defer browser.CloseNow()
	_, openData, err := agent.Read(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var open map[string]any
	if err := json.Unmarshal(openData, &open); err != nil {
		t.Fatal(err)
	}
	if open["binaryFrames"] != true {
		t.Fatal("manager did not negotiate binary frames")
	}
	requestID := open["requestId"].(string)
	ok := true
	ack, _ := json.Marshal(agentMessage{Type: "proxy_ws_open_result", RequestID: requestID, OK: &ok})
	if err := agent.Write(context.Background(), websocket.MessageText, ack); err != nil {
		t.Fatal(err)
	}
	if err := browser.Write(context.Background(), websocket.MessageBinary, []byte{0, 1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	messageType, frame, err := agent.Read(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	separator := bytes.IndexByte(frame, '\n')
	var header agentMessage
	if messageType != websocket.MessageBinary || separator < 1 || json.Unmarshal(frame[:separator], &header) != nil || header.Type != "proxy_ws_frame_binary" || header.FrameType != "binary" || !bytes.Equal(frame[separator+1:], []byte{0, 1, 2, 3}) {
		t.Fatalf("unexpected binary frame: %q", frame)
	}
	if err := agent.Write(context.Background(), websocket.MessageBinary, proxyBinaryMessage(agentMessage{Type: "proxy_ws_frame_binary", RequestID: requestID, FrameType: "binary"}, []byte{4, 5})); err != nil {
		t.Fatal(err)
	}
	mt, payload, err := browser.Read(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if mt != websocket.MessageBinary || !bytes.Equal(payload, []byte{4, 5}) {
		t.Fatalf("browser frame=%v %v", mt, payload)
	}
}

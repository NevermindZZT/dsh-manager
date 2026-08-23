package api

import (
	"context"
	"crypto/tls"
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

func TestBrowserWebSocketTunnelRelaysFrames(t *testing.T) {
	db, err := storage.Open(t.TempDir() + "/manager.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	srv := NewServer(config.Config{PairingCode: "pair-browser", AdminToken: "admin-browser"}, db, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ts := httptest.NewTLSServer(srv.Handler())
	defer ts.Close()
	client := ts.Client()
	client.Transport.(*http.Transport).TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	enrollBody := `{"pairingCode":"pair-browser","name":"browser-pc","platform":"windows","launcherVersion":"0.2.0"}`
	resp, err := client.Post(ts.URL+"/api/v1/agents/enroll", "application/json", strings.NewReader(enrollBody))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var enrolled enrollResponse
	if err := json.NewDecoder(resp.Body).Decode(&enrolled); err != nil {
		t.Fatal(err)
	}
	sessionID := "dsh-test-session"
	srv.webTargets[sessionID] = webTarget{AgentID: enrolled.AgentID, InstanceID: "local", ExpiresAt: time.Now().Add(time.Hour)}
	headers := http.Header{"Authorization": []string{"Bearer " + enrolled.AgentToken}, "X-Agent-Id": []string{enrolled.AgentID}}
	agent, _, err := websocket.Dial(context.Background(), "wss"+strings.TrimPrefix(ts.URL, "https")+"/api/v1/agent/connect", &websocket.DialOptions{HTTPClient: client, HTTPHeader: headers})
	if err != nil {
		t.Fatal(err)
	}
	defer agent.CloseNow()
	_, _, _ = agent.Read(context.Background())
	browserHeaders := http.Header{"Cookie": []string{"dsh-target=" + sessionID}}
	browser, _, err := websocket.Dial(context.Background(), "wss"+strings.TrimPrefix(ts.URL, "https")+"/dsh/"+sessionID+"/", &websocket.DialOptions{HTTPClient: client, HTTPHeader: browserHeaders})
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
	requestID := open["requestId"].(string)
	ok := true
	ack, _ := json.Marshal(agentMessage{Type: "proxy_ws_open_result", RequestID: requestID, OK: &ok})
	if err := agent.Write(context.Background(), websocket.MessageText, ack); err != nil {
		t.Fatal(err)
	}
	if err := browser.Write(context.Background(), websocket.MessageText, []byte("ping")); err != nil {
		t.Fatal(err)
	}
	_, frameData, err := agent.Read(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var frame map[string]any
	if err := json.Unmarshal(frameData, &frame); err != nil {
		t.Fatal(err)
	}
	pong, _ := json.Marshal(agentMessage{Type: "proxy_ws_frame", RequestID: requestID, FrameType: "text", Body: base64.StdEncoding.EncodeToString([]byte("pong"))})
	if err := agent.Write(context.Background(), websocket.MessageText, pong); err != nil {
		t.Fatal(err)
	}
	typ, data, err := browser.Read(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if typ != websocket.MessageText || string(data) != "pong" {
		t.Fatalf("unexpected browser frame: %v %q", typ, data)
	}
}

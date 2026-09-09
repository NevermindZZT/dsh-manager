package api

import (
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

func TestBrowserWebSocketTunnelRelaysFrames(t *testing.T) {
	db, err := storage.Open(t.TempDir() + "/manager.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	srv := NewServer(config.Config{PairingCode: "pair-browser", AdminToken: "admin-browser"}, db, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	client := ts.Client()
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
	localTarget, _ := srv.targetForSession(sessionID)
	localTarget.cookies.apply([]string{"dsh-auth-test=ok; Path=/"}, "/")
	otherSession := "dsh-other-session"
	srv.webTargets[otherSession] = webTarget{AgentID: enrolled.AgentID, InstanceID: "other", ExpiresAt: time.Now().Add(time.Hour), sessionID: otherSession, cookies: newProxyCookieJar()}
	headers := http.Header{"Authorization": []string{"Bearer " + enrolled.AgentToken}, "X-Agent-Id": []string{enrolled.AgentID}}
	agent, _, err := websocket.Dial(context.Background(), "ws"+strings.TrimPrefix(ts.URL, "http")+"/api/v1/agent/connect", &websocket.DialOptions{HTTPClient: client, HTTPHeader: headers})
	if err != nil {
		t.Fatal(err)
	}
	defer agent.CloseNow()
	_, _, _ = agent.Read(context.Background())
	browserHeaders := http.Header{"Cookie": []string{"dsh-target=" + sessionID + "; dsh-session=manager; dsh-auth-test=ok"}}
	browser, _, err := websocket.Dial(context.Background(), "ws"+strings.TrimPrefix(ts.URL, "http")+"/dsh/"+sessionID+"/", &websocket.DialOptions{HTTPClient: client, HTTPHeader: browserHeaders})
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
	openHeaders, headersOK := open["headers"].(map[string]any)
	if !headersOK || openHeaders["Cookie"] != "dsh-auth-test=ok" {
		t.Fatalf("browser Cookie was not forwarded: %#v", open["headers"])
	}
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
	rootHeaders := http.Header{"Cookie": []string{"dsh-target=" + otherSession + "; dsh-session=manager"}, "Referer": []string{ts.URL + "/dsh/" + sessionID + "/"}}
	rootBrowser, _, err := websocket.Dial(context.Background(), "ws"+strings.TrimPrefix(ts.URL, "http")+"/api/session.list", &websocket.DialOptions{HTTPClient: client, HTTPHeader: rootHeaders})
	if err != nil {
		t.Fatal(err)
	}
	defer rootBrowser.CloseNow()
	_, rootOpenData, err := agent.Read(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var rootOpen map[string]any
	if err := json.Unmarshal(rootOpenData, &rootOpen); err != nil {
		t.Fatal(err)
	}
	rootHeadersMap, ok := rootOpen["headers"].(map[string]any)
	if !ok || rootOpen["instanceId"] != "local" || rootOpen["path"] != "/api/session.list" || rootHeadersMap["Cookie"] != "dsh-auth-test=ok" {
		t.Fatalf("root WebSocket target/cookie mismatch: %#v", rootOpen)
	}
	rootRequestID := rootOpen["requestId"].(string)
	rootAck, _ := json.Marshal(agentMessage{Type: "proxy_ws_open_result", RequestID: rootRequestID, OK: &ok})
	if err := agent.Write(context.Background(), websocket.MessageText, rootAck); err != nil {
		t.Fatal(err)
	}
	explicitHeaders := http.Header{"Cookie": []string{"dsh-target=" + otherSession + "; dsh-session=manager; dsh-auth-test=ok"}, "Referer": []string{ts.URL + "/dsh/" + otherSession + "/"}}
	explicitBrowser, _, err := websocket.Dial(context.Background(), "ws"+strings.TrimPrefix(ts.URL, "http")+"/dsh/"+sessionID+"/ws", &websocket.DialOptions{HTTPClient: client, HTTPHeader: explicitHeaders})
	if err != nil {
		t.Fatal(err)
	}
	defer explicitBrowser.CloseNow()
	_, explicitOpenData, err := agent.Read(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var explicitOpen map[string]any
	if err := json.Unmarshal(explicitOpenData, &explicitOpen); err != nil {
		t.Fatal(err)
	}
	if explicitOpen["instanceId"] != "local" || explicitOpen["path"] != "/ws" {
		t.Fatalf("explicit WebSocket target was overridden: %#v", explicitOpen)
	}
	explicitRequestID := explicitOpen["requestId"].(string)
	explicitAck, _ := json.Marshal(agentMessage{Type: "proxy_ws_open_result", RequestID: explicitRequestID, OK: &ok})
	if err := agent.Write(context.Background(), websocket.MessageText, explicitAck); err != nil {
		t.Fatal(err)
	}
}

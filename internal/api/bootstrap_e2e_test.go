package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/NevermindZZT/dsh-manager/internal/config"
	"github.com/NevermindZZT/dsh-manager/internal/storage"
	"github.com/coder/websocket"
)

func TestBootstrapRedirectAndCookieProxy(t *testing.T) {
	db, err := storage.Open(t.TempDir() + "/manager.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	srv := NewServer(config.Config{PairingCode: "pair", AdminToken: "admin"}, db, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	enrollBody := []byte(`{"pairingCode":"pair","name":"agent","platform":"windows"}`)
	resp, err := http.Post(ts.URL+"/api/v1/agents/enroll", "application/json", bytes.NewReader(enrollBody))
	if err != nil {
		t.Fatal(err)
	}
	var enrolled enrollResponse
	if err = json.NewDecoder(resp.Body).Decode(&enrolled); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	headers := http.Header{"Authorization": {"Bearer " + enrolled.AgentToken}, "X-Agent-Id": {enrolled.AgentID}}
	agent, _, err := websocket.Dial(context.Background(), "ws"+ts.URL[4:]+"/api/v1/agent/connect", &websocket.DialOptions{HTTPHeader: headers})
	if err != nil {
		t.Fatal(err)
	}
	defer agent.CloseNow()
	_, _, _ = agent.Read(context.Background())
	register, _ := json.Marshal(agentMessage{Type: "register", Capabilities: []string{"proxy.http", "dsh.web.bootstrap-v1"}, Instances: []storage.Instance{{InstanceID: "local", DisplayName: "Local", Type: "plugin", State: "running", URLAvailable: true, StartupURL: "http://127.0.0.1:1/?token=secret"}}})
	if err = agent.Write(context.Background(), websocket.MessageText, register); err != nil {
		t.Fatal(err)
	}
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/api/v1/instances/"+enrolled.AgentID+"/local/open", nil)
	req.Header.Set("Authorization", "Bearer admin")
	opened, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	var open struct {
		URL string `json:"url"`
	}
	if err = json.NewDecoder(opened.Body).Decode(&open); err != nil {
		t.Fatal(err)
	}
	targetCookie := opened.Cookies()[0]
	opened.Body.Close()
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	firstReq, _ := http.NewRequest(http.MethodGet, ts.URL+open.URL, nil)
	firstReq.AddCookie(targetCookie)
	firstDone := make(chan *http.Response, 1)
	go func() { r, _ := client.Do(firstReq); firstDone <- r }()
	_, raw, err := agent.Read(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var proxied proxyRequest
	if err = json.Unmarshal(raw, &proxied); err != nil {
		t.Fatal(err)
	}
	if !proxied.Bootstrap || proxied.Path != "/" {
		t.Fatalf("expected bootstrap root request: %#v", proxied)
	}
	response, _ := json.Marshal(agentMessage{Type: "proxy_response", RequestID: proxied.RequestID, Status: http.StatusSeeOther, Headers: map[string]string{"Location": "/"}, SetCookies: []string{"dsh-auth-test=ok; Path=/"}})
	if err = agent.Write(context.Background(), websocket.MessageText, response); err != nil {
		t.Fatal(err)
	}
	first := <-firstDone
	if first == nil {
		t.Fatal("first proxy response missing")
	}
	defer first.Body.Close()
	if first.StatusCode != http.StatusSeeOther || first.Header.Get("Location") != open.URL {
		t.Fatalf("redirect=%d %q want %q", first.StatusCode, first.Header.Get("Location"), open.URL)
	}
	if state, ok := srv.targetForSession(targetCookie.Value); !ok || state.BootstrapPending {
		t.Fatalf("bootstrap state was not consumed: %#v", state)
	}
	secondReq, _ := http.NewRequest(http.MethodGet, ts.URL+open.URL, nil)
	secondReq.AddCookie(targetCookie)
	secondReq.AddCookie(&http.Cookie{Name: "dsh-auth-test", Value: "ok"})
	secondDone := make(chan *http.Response, 1)
	go func() { r, _ := client.Do(secondReq); secondDone <- r }()
	_, raw, err = agent.Read(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	proxied = proxyRequest{}
	if err = json.Unmarshal(raw, &proxied); err != nil {
		t.Fatal(err)
	}
	if proxied.Bootstrap || proxied.Headers["Cookie"] == "" {
		t.Fatalf("expected clean request with cookie: %#v", proxied)
	}
	response, _ = json.Marshal(agentMessage{Type: "proxy_response", RequestID: proxied.RequestID, Status: http.StatusOK, Headers: map[string]string{"Content-Type": "text/plain"}, Body: "b2s="})
	_ = agent.Write(context.Background(), websocket.MessageText, response)
	second := <-secondDone
	if second == nil || second.StatusCode != http.StatusOK {
		t.Fatal("clean proxy request failed")
	}
	second.Body.Close()
}

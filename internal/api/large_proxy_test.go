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
	"strings"
	"testing"
	"time"

	"github.com/NevermindZZT/dsh-manager/internal/config"
	"github.com/NevermindZZT/dsh-manager/internal/storage"
	"github.com/coder/websocket"
)

type proxyHTTPResult struct {
	response *http.Response
	err      error
}

func TestHTTPProxyRelaysLargeAgentResponse(t *testing.T) {
	db, err := storage.Open(t.TempDir() + "/manager.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	server := NewServer(config.Config{PairingCode: "large-pair", AdminToken: "admin-token"}, db, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ts := httptest.NewServer(server.Handler())
	defer ts.Close()

	enrollBody := `{"pairingCode":"large-pair","name":"large-agent","platform":"linux"}`
	response, err := http.Post(ts.URL+"/api/v1/agents/enroll", "application/json", strings.NewReader(enrollBody))
	if err != nil {
		t.Fatal(err)
	}
	var enrolled enrollResponse
	if err := json.NewDecoder(response.Body).Decode(&enrolled); err != nil {
		response.Body.Close()
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("enroll status=%d", response.StatusCode)
	}

	sessionID := "large-proxy"
	server.webTargets[sessionID] = webTarget{AgentID: enrolled.AgentID, InstanceID: "local", ExpiresAt: time.Now().Add(time.Hour)}
	agentURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/api/v1/agent/connect"
	headers := http.Header{
		"Authorization": []string{"Bearer " + enrolled.AgentToken},
		"X-Agent-Id":    []string{enrolled.AgentID},
	}
	agent, _, err := websocket.Dial(context.Background(), agentURL, &websocket.DialOptions{HTTPHeader: headers})
	if err != nil {
		t.Fatal(err)
	}
	defer agent.CloseNow()
	if _, _, err := agent.Read(context.Background()); err != nil {
		t.Fatal(err)
	}

	resultCh := make(chan proxyHTTPResult, 1)
	go func() {
		request, requestErr := http.NewRequest(http.MethodPost, ts.URL+"/dsh/"+sessionID+"/api/session.list", bytes.NewBufferString(`{"type":"client-request","rpcId":"large-test","method":"session.list","payload":{}}`))
		if requestErr != nil {
			resultCh <- proxyHTTPResult{err: requestErr}
			return
		}
		request.Header.Set("Content-Type", "application/json")
		result, requestErr := http.DefaultClient.Do(request)
		resultCh <- proxyHTTPResult{response: result, err: requestErr}
	}()

	_, requestData, err := agent.Read(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var request proxyRequest
	if err := json.Unmarshal(requestData, &request); err != nil {
		t.Fatal(err)
	}
	if request.Type != "proxy_request" || request.Path != "/api/session.list" {
		t.Fatalf("unexpected proxy request: %+v", request)
	}

	body := bytes.Repeat([]byte("x"), 24<<20)
	proxyResponse, err := json.Marshal(agentMessage{
		Type:      "proxy_response",
		RequestID: request.RequestID,
		Status:    http.StatusOK,
		Headers:   map[string]string{"Content-Type": "application/json"},
		Body:      base64.StdEncoding.EncodeToString(body),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := agent.Write(context.Background(), websocket.MessageText, proxyResponse); err != nil {
		t.Fatal(err)
	}

	result := <-resultCh
	if result.err != nil {
		t.Fatal(result.err)
	}
	defer result.response.Body.Close()
	if result.response.StatusCode != http.StatusOK {
		t.Fatalf("proxy status=%d", result.response.StatusCode)
	}
	received, err := io.ReadAll(result.response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(received, body) {
		t.Fatalf("proxy body length=%d, want %d", len(received), len(body))
	}
	if result.response.ContentLength != int64(len(body)) {
		t.Fatalf("proxy Content-Length=%d, want %d", result.response.ContentLength, len(body))
	}
}

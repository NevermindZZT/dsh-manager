package api

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/NevermindZZT/dsh-manager/internal/config"
	"github.com/NevermindZZT/dsh-manager/internal/storage"
	"github.com/coder/websocket"
)

func TestAgentWebSocketCommandDispatch(t *testing.T) {
	db, err := storage.Open(t.TempDir() + "/manager.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	srv := NewServer(config.Config{PairingCode: "pair-ws", AdminToken: "admin-ws"}, db, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ts := httptest.NewTLSServer(srv.Handler())
	defer ts.Close()
	client := ts.Client()
	client.Transport.(*http.Transport).TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	enrollBody := `{"pairingCode":"pair-ws","name":"ws-pc","platform":"windows","launcherVersion":"0.2.0"}`
	resp, err := client.Post(ts.URL+"/api/v1/agents/enroll", "application/json", bytes.NewBufferString(enrollBody))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var enrolled enrollResponse
	if err := json.NewDecoder(resp.Body).Decode(&enrolled); err != nil {
		t.Fatal(err)
	}
	wsURL := "wss" + strings.TrimPrefix(ts.URL, "https")
	headers := http.Header{"Authorization": []string{"Bearer " + enrolled.AgentToken}, "X-Agent-Id": []string{enrolled.AgentID}}
	conn, _, err := websocket.Dial(context.Background(), wsURL+"/api/v1/agent/connect", &websocket.DialOptions{HTTPClient: client, HTTPHeader: headers})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseNow()
	_, _, err = conn.Read(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	register, _ := json.Marshal(agentMessage{Type: "register", Instances: []storage.Instance{{InstanceID: "local", DisplayName: "Local", Type: "local", State: "running"}}})
	if err := conn.Write(context.Background(), websocket.MessageText, register); err != nil {
		t.Fatal(err)
	}
	reqBody := `{"action":"restart"}`
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/api/v1/instances/"+enrolled.AgentID+"/local/commands", bytes.NewBufferString(reqBody))
	req.Header.Set("Authorization", "Bearer admin-ws")
	req.Header.Set("Content-Type", "application/json")
	resp, err = client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("command status=%d", resp.StatusCode)
	}
	_, data, err := conn.Read(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var command map[string]any
	if err := json.Unmarshal(data, &command); err != nil {
		t.Fatal(err)
	}
	if command["action"] != "restart" {
		t.Fatalf("unexpected command: %s", data)
	}
}

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

	"github.com/NevermindZZT/dsh-manager/internal/config"
	"github.com/NevermindZZT/dsh-manager/internal/storage"
	"github.com/coder/websocket"
)

func TestPairingRefreshKeepsExistingAgentCredentials(t *testing.T) {
	db, err := storage.Open(t.TempDir() + "/manager.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	server := NewServer(config.Config{PairingCode: "initial-pair", AdminToken: "admin-token"}, db, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ts := httptest.NewServer(server.Handler())
	defer ts.Close()

	enrollBody := `{"pairingCode":"initial-pair","name":"test-agent","platform":"windows"}`
	response, err := http.Post(ts.URL+"/api/v1/agents/enroll", "application/json", bytes.NewBufferString(enrollBody))
	if err != nil {
		t.Fatal(err)
	}
	var enrolled enrollResponse
	if err := json.NewDecoder(response.Body).Decode(&enrolled); err != nil {
		response.Body.Close()
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusCreated || enrolled.AgentID == "" || enrolled.AgentToken == "" {
		t.Fatalf("enroll status=%d response=%+v", response.StatusCode, enrolled)
	}

	oldPairingCode := server.pairingCode
	refreshRequest, err := http.NewRequest(http.MethodPost, ts.URL+"/api/v1/admin/pairing/refresh", nil)
	if err != nil {
		t.Fatal(err)
	}
	refreshRequest.Header.Set("Authorization", "Bearer admin-token")
	response, err = http.DefaultClient.Do(refreshRequest)
	if err != nil {
		t.Fatal(err)
	}
	var refreshed struct {
		PairingCode string `json:"pairingCode"`
	}
	if err := json.NewDecoder(response.Body).Decode(&refreshed); err != nil {
		response.Body.Close()
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK || refreshed.PairingCode == "" || refreshed.PairingCode == oldPairingCode {
		t.Fatalf("refresh status=%d response=%+v old=%q", response.StatusCode, refreshed, oldPairingCode)
	}

	heartbeatRequest, err := http.NewRequest(http.MethodPost, ts.URL+"/api/v1/agent/heartbeat", bytes.NewBufferString(`{"instances":[]}`))
	if err != nil {
		t.Fatal(err)
	}
	heartbeatRequest.Header.Set("Authorization", "Bearer "+enrolled.AgentToken)
	heartbeatRequest.Header.Set("X-Agent-Id", enrolled.AgentID)
	response, err = http.DefaultClient.Do(heartbeatRequest)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("existing Agent heartbeat status after refresh=%d", response.StatusCode)
	}

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/api/v1/agent/connect"
	wsHeaders := http.Header{
		"Authorization": []string{"Bearer " + enrolled.AgentToken},
		"X-Agent-Id":    []string{enrolled.AgentID},
	}
	connection, _, err := websocket.Dial(context.Background(), wsURL, &websocket.DialOptions{HTTPHeader: wsHeaders})
	if err != nil {
		t.Fatalf("existing Agent websocket after refresh: %v", err)
	}
	defer connection.CloseNow()
	if _, _, err := connection.Read(context.Background()); err != nil {
		t.Fatalf("read Agent hello after refresh: %v", err)
	}

	oldCodeRequest, err := http.NewRequest(http.MethodPost, ts.URL+"/api/v1/agents/enroll", bytes.NewBufferString(`{"pairingCode":"`+oldPairingCode+`","name":"old-code","platform":"windows"}`))
	if err != nil {
		t.Fatal(err)
	}
	response, err = http.DefaultClient.Do(oldCodeRequest)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("old pairing code status after refresh=%d", response.StatusCode)
	}

	newCodeRequest, err := http.NewRequest(http.MethodPost, ts.URL+"/api/v1/agents/enroll", bytes.NewBufferString(`{"pairingCode":"`+refreshed.PairingCode+`","name":"new-agent","platform":"windows"}`))
	if err != nil {
		t.Fatal(err)
	}
	response, err = http.DefaultClient.Do(newCodeRequest)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("new pairing code status=%d", response.StatusCode)
	}
}

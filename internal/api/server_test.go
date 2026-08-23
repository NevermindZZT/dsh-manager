package api

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/NevermindZZT/dsh-manager/internal/config"
	"github.com/NevermindZZT/dsh-manager/internal/storage"
)

func TestEnrollHeartbeatAndAdminList(t *testing.T) {
	db, err := storage.Open(t.TempDir() + "/manager.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv := NewServer(config.Config{PairingCode: "pair-123", AdminToken: "admin-123"}, db, logger)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	enrollBody := `{"pairingCode":"pair-123","name":"test-pc","platform":"windows","launcherVersion":"0.1.0"}`
	enrollReq, _ := http.NewRequest(http.MethodPost, ts.URL+"/api/v1/agents/enroll", bytes.NewBufferString(enrollBody))
	enrollReq.Header.Set("Content-Type", "application/json")
	enrollReq.Header.Set("X-Forwarded-Proto", "https")
	response, err := http.DefaultClient.Do(enrollReq)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("enroll status = %d", response.StatusCode)
	}
	var enrolled enrollResponse
	if err := json.NewDecoder(response.Body).Decode(&enrolled); err != nil {
		t.Fatal(err)
	}
	if enrolled.AgentID == "" || enrolled.AgentToken == "" {
		t.Fatal("missing enrollment credentials")
	}

	heartbeat := `{"instances":[{"instanceId":"local","displayName":"Local","type":"local","state":"running","urlAvailable":true,"generation":1,"eventSeq":7}]}`
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/api/v1/agent/heartbeat", bytes.NewBufferString(heartbeat))
	req.Header.Set("Authorization", "Bearer "+enrolled.AgentToken)
	req.Header.Set("X-Agent-Id", enrolled.AgentID)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Forwarded-Proto", "https")
	response, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("heartbeat status = %d", response.StatusCode)
	}

	req, _ = http.NewRequest(http.MethodGet, ts.URL+"/api/v1/instances", nil)
	req.Header.Set("Authorization", "Bearer admin-123")
	response, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("instances status = %d", response.StatusCode)
	}
	var listed struct {
		Instances []storage.Instance `json:"instances"`
	}
	if err := json.NewDecoder(response.Body).Decode(&listed); err != nil {
		t.Fatal(err)
	}
	if len(listed.Instances) != 1 || listed.Instances[0].InstanceID != "local" {
		t.Fatalf("unexpected instances: %+v", listed)
	}

	enrollReq, _ = http.NewRequest(http.MethodPost, ts.URL+"/api/v1/agents/enroll", bytes.NewBufferString(enrollBody))
	enrollReq.Header.Set("Content-Type", "application/json")
	enrollReq.Header.Set("X-Forwarded-Proto", "https")
	response, err = http.DefaultClient.Do(enrollReq)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("pairing code reused with status %d", response.StatusCode)
	}
}

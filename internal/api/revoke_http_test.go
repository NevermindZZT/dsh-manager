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

func TestRevokeAgentPostEndpoint(t *testing.T) {
	db, err := storage.Open(t.TempDir() + "/manager.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	srv := NewServer(config.Config{PairingCode: "pair", AdminToken: "admin"}, db, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	enroll, err := http.NewRequest(http.MethodPost, ts.URL+"/api/v1/agents/enroll", bytes.NewBufferString("{\"pairingCode\":\"pair\",\"name\":\"pc\",\"platform\":\"windows\"}"))
	if err != nil {
		t.Fatal(err)
	}
	enroll.Header.Set("X-Forwarded-Proto", "https")
	response, err := http.DefaultClient.Do(enroll)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var result enrollResponse
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	if result.AgentID == "" {
		t.Fatal("missing agent id")
	}
	request, err := http.NewRequest(http.MethodPost, ts.URL+"/api/v1/admin/agents/"+result.AgentID+"/revoke", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer admin")
	request.AddCookie(&http.Cookie{Name: "dsh-target", Value: "stale-target"})
	response, err = http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("revoke status = %d", response.StatusCode)
	}
	agents, err := db.ListAgents()
	if err != nil {
		t.Fatal(err)
	}
	if len(agents) != 0 {
		t.Fatalf("agent remains after revoke: %+v", agents)
	}
}

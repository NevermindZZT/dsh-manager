package api

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/NevermindZZT/dsh-manager/internal/config"
	"github.com/NevermindZZT/dsh-manager/internal/storage"
)

func TestPluginEnrollmentMetadataIsOptionalAndVisible(t *testing.T) {
	db, err := storage.Open(t.TempDir() + "/manager.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	server := NewServer(config.Config{PairingCode: "plugin-pair", AdminToken: "admin-token"}, db, slog.Default())
	ts := httptest.NewServer(server.Handler())
	defer ts.Close()
	payload := []byte("{\"pairingCode\":\"plugin-pair\",\"name\":\"linux-dsh\",\"platform\":\"linux\",\"agentType\":\"dsh-plugin\",\"agentVersion\":\"v22\",\"pluginVersion\":\"0.1.0\",\"capabilities\":[\"proxy.http\",\"proxy.websocket\"]}")
	response, err := http.Post(ts.URL+"/api/v1/agents/enroll", "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("enroll status = %d", response.StatusCode)
	}
	agents, err := http.NewRequest(http.MethodGet, ts.URL+"/api/v1/agents", nil)
	if err != nil {
		t.Fatal(err)
	}
	agents.Header.Set("Authorization", "Bearer admin-token")
	listResponse, err := http.DefaultClient.Do(agents)
	if err != nil {
		t.Fatal(err)
	}
	defer listResponse.Body.Close()
	var body map[string]json.RawMessage
	if err := json.NewDecoder(listResponse.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	var list []storage.Agent
	if err := json.Unmarshal(body["agents"], &list); err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 || list[0].AgentType != "dsh-plugin" {
		t.Fatalf("unexpected agent metadata: %#v", list)
	}
}

package api

import (
	"testing"

	"github.com/NevermindZZT/dsh-manager/internal/storage"
)

func TestDeleteAgentRemovesInstancesBeforeAgent(t *testing.T) {
	db, err := storage.Open(t.TempDir() + "/manager.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.CreateAgent("agent-delete", "delete-pc", "windows", "test", "token"); err != nil {
		t.Fatal(err)
	}
	if err := db.UpsertHeartbeat("agent-delete", []storage.Instance{{InstanceID: "local", DisplayName: "Local", Type: "local", State: "running"}}); err != nil {
		t.Fatal(err)
	}
	if err := db.DeleteAgent("agent-delete"); err != nil {
		t.Fatalf("delete agent with instances: %v", err)
	}
	agents, err := db.ListAgents()
	if err != nil {
		t.Fatal(err)
	}
	if len(agents) != 0 {
		t.Fatalf("deleted agent remains: %+v", agents)
	}
	instances, err := db.ListInstances()
	if err != nil {
		t.Fatal(err)
	}
	if len(instances) != 0 {
		t.Fatalf("deleted instances remain: %+v", instances)
	}
}

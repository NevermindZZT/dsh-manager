package storage

import "testing"

func TestUpsertHeartbeatReconcilesRemovedInstances(t *testing.T) {
	db, err := Open(t.TempDir() + "/manager.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.CreateAgent("agent-1", "test", "windows", "0.3.2", "token"); err != nil {
		t.Fatal(err)
	}
	instances := []Instance{
		{InstanceID: "local", DisplayName: "Local", Type: "local", State: "running", URLAvailable: true},
		{InstanceID: "ssh:remote", DisplayName: "Remote", Type: "ssh", State: "running", URLAvailable: true},
	}
	if err := db.UpsertHeartbeat("agent-1", instances); err != nil {
		t.Fatal(err)
	}
	if err := db.UpsertHeartbeat("agent-1", instances[:1]); err != nil {
		t.Fatal(err)
	}
	got, err := db.ListInstances()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].InstanceID != "local" {
		t.Fatalf("instances after removal=%#v", got)
	}
	if err := db.UpsertHeartbeat("agent-1", nil); err != nil {
		t.Fatal(err)
	}
	got, err = db.ListInstances()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("instances after empty heartbeat=%#v", got)
	}
}

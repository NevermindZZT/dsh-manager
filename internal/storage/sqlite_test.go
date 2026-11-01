package storage

import (
	"testing"
	"time"
)

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

func TestWebTargetPersistence(t *testing.T) {
	db, err := Open(t.TempDir() + "/manager.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// 1. Initially non-existent
	rec, err := db.GetWebTarget("non-existent")
	if err != nil {
		t.Fatal(err)
	}
	if rec != nil {
		t.Fatalf("expected nil record, got %#v", rec)
	}

	// 2. Save
	if err := db.SaveWebTarget("dsh-1234", "agent-1", "inst-1"); err != nil {
		t.Fatal(err)
	}

	// 3. Get
	rec, err = db.GetWebTarget("dsh-1234")
	if err != nil {
		t.Fatal(err)
	}
	if rec == nil || rec.AgentID != "agent-1" || rec.InstanceID != "inst-1" {
		t.Fatalf("unexpected record: %#v", rec)
	}

	// 4. Find recent
	found, err := db.FindRecentWebTarget("agent-1", "inst-1")
	if err != nil {
		t.Fatal(err)
	}
	if found == nil || found.SessionID != "dsh-1234" {
		t.Fatalf("unexpected found target: %#v", found)
	}

	// 5. Touch
	if err := db.TouchWebTarget("dsh-1234"); err != nil {
		t.Fatal(err)
	}

	// 6. Delete
	if err := db.DeleteWebTarget("dsh-1234"); err != nil {
		t.Fatal(err)
	}
	rec, err = db.GetWebTarget("dsh-1234")
	if err != nil {
		t.Fatal(err)
	}
	if rec != nil {
		t.Fatalf("expected nil after delete, got %#v", rec)
	}
}

func TestAuthSessionPersistence(t *testing.T) {
	db, err := Open(t.TempDir() + "/manager.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// 1. Initially non-existent
	rec, err := db.GetAuthSession("non-existent")
	if err != nil {
		t.Fatal(err)
	}
	if rec != nil {
		t.Fatalf("expected nil, got %#v", rec)
	}

	// 2. Save
	exp := time.Now().Add(30 * 24 * time.Hour)
	if err := db.SaveAuthSession("sess-1", "admin", exp); err != nil {
		t.Fatal(err)
	}

	// 3. Get
	rec, err = db.GetAuthSession("sess-1")
	if err != nil {
		t.Fatal(err)
	}
	if rec == nil || rec.Username != "admin" || rec.ID != "sess-1" {
		t.Fatalf("unexpected auth session: %#v", rec)
	}

	// 4. Touch (slide expiration)
	newExp := time.Now().Add(60 * 24 * time.Hour)
	if err := db.TouchAuthSession("sess-1", newExp); err != nil {
		t.Fatal(err)
	}
	rec, err = db.GetAuthSession("sess-1")
	if err != nil {
		t.Fatal(err)
	}
	if rec.ExpiresAt.Before(exp) {
		t.Fatalf("expected extended expiration, got %v", rec.ExpiresAt)
	}

	// 5. Clean expired
	pastExp := time.Now().Add(-time.Hour)
	if err := db.SaveAuthSession("sess-expired", "admin", pastExp); err != nil {
		t.Fatal(err)
	}
	if err := db.CleanExpiredAuthSessions(); err != nil {
		t.Fatal(err)
	}
	rec, err = db.GetAuthSession("sess-expired")
	if err != nil {
		t.Fatal(err)
	}
	if rec != nil {
		t.Fatal("expected expired session to be cleaned up")
	}

	// 6. Delete
	if err := db.DeleteAuthSession("sess-1"); err != nil {
		t.Fatal(err)
	}
	rec, err = db.GetAuthSession("sess-1")
	if err != nil {
		t.Fatal(err)
	}
	if rec != nil {
		t.Fatal("expected session to be deleted")
	}
}

package api

import (
	"context"
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

func TestDirectInstanceURL_UnauthenticatedRedirect(t *testing.T) {
	db, err := storage.Open(t.TempDir() + "/manager.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	srv := NewServer(config.Config{AdminUsername: "admin", AdminPassword: "password"}, db, slog.New(slog.NewTextHandler(io.Discard, nil)))

	req := httptest.NewRequest("GET", "/instances/agent-1/local", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusFound {
		t.Fatalf("status=%d, want %d", rec.Code, http.StatusFound)
	}
	loc := rec.Header().Get("Location")
	if !strings.HasPrefix(loc, "/manager?redirect=") || !strings.Contains(loc, "agent-1") {
		t.Fatalf("unexpected redirect location: %s", loc)
	}
}

func TestDirectInstanceURL_AuthenticatedSuccess(t *testing.T) {
	db, err := storage.Open(t.TempDir() + "/manager.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	srv := NewServer(config.Config{PairingCode: "pair-direct", AdminToken: "admin-direct"}, db, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	client := ts.Client()

	// 1. Enroll agent
	enrollBody := `{"pairingCode":"pair-direct","name":"pc","platform":"linux"}`
	resp, err := client.Post(ts.URL+"/api/v1/agents/enroll", "application/json", strings.NewReader(enrollBody))
	if err != nil {
		t.Fatal(err)
	}
	var enrolled enrollResponse
	_ = json.NewDecoder(resp.Body).Decode(&enrolled)
	resp.Body.Close()

	// 2. Connect agent websocket and register instance
	headers := http.Header{"Authorization": []string{"Bearer " + enrolled.AgentToken}, "X-Agent-Id": []string{enrolled.AgentID}}
	agent, _, err := websocket.Dial(context.Background(), "ws"+strings.TrimPrefix(ts.URL, "http")+"/api/v1/agent/connect", &websocket.DialOptions{HTTPClient: client, HTTPHeader: headers})
	if err != nil {
		t.Fatal(err)
	}
	defer agent.CloseNow()
	_, _, _ = agent.Read(context.Background())

	registerMsg, _ := json.Marshal(agentMessage{
		Type: "register",
		Instances: []storage.Instance{
			{InstanceID: "work", DisplayName: "Work Instance", Type: "plugin", State: "running", URLAvailable: true},
		},
	})
	_ = agent.Write(context.Background(), websocket.MessageText, registerMsg)

	// Wait for heartbeat registration
	for attempt := 0; attempt < 100; attempt++ {
		instances, _ := srv.db.ListInstances()
		if len(instances) > 0 && instances[0].State == "running" {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}

	// 3. User logs in to get session cookie
	srv.sessionsAuthMu.Lock()
	userSessionID := "test-user-session"
	srv.authSessions[userSessionID] = time.Now().Add(time.Hour)
	srv.sessionsAuthMu.Unlock()

	// 4. Request direct instance URL: GET /instances/{agentId}/{instanceId}
	req, _ := http.NewRequest("GET", ts.URL+"/instances/"+enrolled.AgentID+"/work", nil)
	req.AddCookie(&http.Cookie{Name: "dsh-session", Value: userSessionID})
	noRedirectClient := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	directResp, err := noRedirectClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer directResp.Body.Close()

	if directResp.StatusCode != http.StatusFound {
		t.Fatalf("expected 302 redirect to dsh proxy, got status %d", directResp.StatusCode)
	}
	dshURL := directResp.Header.Get("Location")
	if !strings.HasPrefix(dshURL, "/dsh/dsh-") || !strings.HasSuffix(dshURL, "/") {
		t.Fatalf("unexpected dsh target URL: %s", dshURL)
	}

	// Verify target cookie is set
	var targetCookie *http.Cookie
	for _, c := range directResp.Cookies() {
		if c.Name == "dsh-target" {
			targetCookie = c
			break
		}
	}
	if targetCookie == nil || targetCookie.Value == "" {
		t.Fatal("dsh-target cookie not set on direct instance redirect")
	}

	// 5. Verify repeated access reuses the active session
	req2, _ := http.NewRequest("GET", ts.URL+"/instances/"+enrolled.AgentID+"/work", nil)
	req2.AddCookie(&http.Cookie{Name: "dsh-session", Value: userSessionID})
	directResp2, err := noRedirectClient.Do(req2)
	if err != nil {
		t.Fatal(err)
	}
	defer directResp2.Body.Close()
	if directResp2.Header.Get("Location") != dshURL {
		t.Fatalf("expected session reuse, got %s want %s", directResp2.Header.Get("Location"), dshURL)
	}

	// 6. Test single-instance agent direct URL: GET /instances/{agentId}
	req3, _ := http.NewRequest("GET", ts.URL+"/instances/"+enrolled.AgentID, nil)
	req3.AddCookie(&http.Cookie{Name: "dsh-session", Value: userSessionID})
	directResp3, err := noRedirectClient.Do(req3)
	if err != nil {
		t.Fatal(err)
	}
	defer directResp3.Body.Close()
	if directResp3.Header.Get("Location") != dshURL {
		t.Fatalf("expected single-instance agent redirect to %s, got %s", dshURL, directResp3.Header.Get("Location"))
	}

	// 7. Test /dsh/{agentId}/{instanceId} alias
	req4, _ := http.NewRequest("GET", ts.URL+"/dsh/"+enrolled.AgentID+"/work", nil)
	req4.AddCookie(&http.Cookie{Name: "dsh-session", Value: userSessionID})
	directResp4, err := noRedirectClient.Do(req4)
	if err != nil {
		t.Fatal(err)
	}
	defer directResp4.Body.Close()
	if directResp4.StatusCode != http.StatusFound || directResp4.Header.Get("Location") != dshURL {
		t.Fatalf("expected /dsh alias redirect to %s, got status=%d loc=%s", dshURL, directResp4.StatusCode, directResp4.Header.Get("Location"))
	}
}

func TestDirectInstanceURL_AgentOfflineMessage(t *testing.T) {
	db, err := storage.Open(t.TempDir() + "/manager.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	srv := NewServer(config.Config{AdminUsername: "admin", AdminPassword: "password"}, db, slog.New(slog.NewTextHandler(io.Discard, nil)))

	srv.sessionsAuthMu.Lock()
	userSessionID := "test-user-session"
	srv.authSessions[userSessionID] = time.Now().Add(time.Hour)
	srv.sessionsAuthMu.Unlock()

	req := httptest.NewRequest("GET", "/instances/offline-agent/local", nil)
	req.AddCookie(&http.Cookie{Name: "dsh-session", Value: userSessionID})
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusConflict {
		t.Fatalf("status=%d, want %d", rec.Code, http.StatusConflict)
	}
	if !strings.Contains(rec.Body.String(), "Agent 离线") {
		t.Fatalf("expected offline message in body: %s", rec.Body.String())
	}
}

func TestProxySession_TrailingSlashRedirect(t *testing.T) {
	db, err := storage.Open(t.TempDir() + "/manager.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	srv := NewServer(config.Config{}, db, slog.New(slog.NewTextHandler(io.Discard, nil)))

	// Request /dsh/dsh-12345 without trailing slash
	req := httptest.NewRequest("GET", "/dsh/dsh-12345", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusTemporaryRedirect {
		t.Fatalf("status=%d, want %d", rec.Code, http.StatusTemporaryRedirect)
	}
	if rec.Header().Get("Location") != "/dsh/dsh-12345/" {
		t.Fatalf("location=%s, want /dsh/dsh-12345/", rec.Header().Get("Location"))
	}
}

func TestTargetSession_RevivedFromPersistentDB(t *testing.T) {
	db, err := storage.Open(t.TempDir() + "/manager.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	srv := NewServer(config.Config{}, db, slog.New(slog.NewTextHandler(io.Discard, nil)))

	// Set up agent in DB and mock session
	if err := db.CreateAgent("agent-1", "pc", "linux", "1.0", "token"); err != nil {
		t.Fatal(err)
	}
	instances := []storage.Instance{
		{AgentID: "agent-1", InstanceID: "main", DisplayName: "Main", Type: "plugin", State: "running", URLAvailable: true},
	}
	if err := db.UpsertHeartbeat("agent-1", instances); err != nil {
		t.Fatal(err)
	}
	srv.sessions["agent-1"] = &agentSession{}

	// Save web target to DB (simulating session created before a manager restart)
	sessionID := "dsh-saved-session"
	if err := db.SaveWebTarget(sessionID, "agent-1", "main"); err != nil {
		t.Fatal(err)
	}

	// Notice: srv.webTargets is completely EMPTY (simulating restart)
	if len(srv.webTargets) != 0 {
		t.Fatal("expected empty in-memory webTargets")
	}

	// targetForSession should find and revive it from DB!
	target, ok := srv.targetForSession(sessionID)
	if !ok {
		t.Fatal("expected target to be revived from DB")
	}
	if target.AgentID != "agent-1" || target.InstanceID != "main" || target.sessionID != sessionID {
		t.Fatalf("unexpected revived target: %#v", target)
	}

	// Verify it is now in memory
	if _, inMemory := srv.webTargets[sessionID]; !inMemory {
		t.Fatal("expected revived target to be cached in memory")
	}
}

func TestDashboardRedirectWhenAuthenticated(t *testing.T) {
	db, err := storage.Open(t.TempDir() + "/manager.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	srv := NewServer(config.Config{AdminUsername: "admin", AdminPassword: "password"}, db, slog.New(slog.NewTextHandler(io.Discard, nil)))

	srv.sessionsAuthMu.Lock()
	userSessionID := "test-user-session"
	srv.authSessions[userSessionID] = time.Now().Add(time.Hour)
	srv.sessionsAuthMu.Unlock()

	// Authenticated request to /manager with ?redirect=/instances/my-pc/work
	req := httptest.NewRequest("GET", "/manager?redirect=%2Finstances%2Fmy-pc%2Fwork", nil)
	req.AddCookie(&http.Cookie{Name: "dsh-session", Value: userSessionID})
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusFound {
		t.Fatalf("status=%d, want 302", rec.Code)
	}
	if rec.Header().Get("Location") != "/instances/my-pc/work" {
		t.Fatalf("unexpected location: %s", rec.Header().Get("Location"))
	}
}

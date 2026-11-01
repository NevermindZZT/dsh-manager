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
	"golang.org/x/crypto/bcrypt"
)

func TestDashboardLoginSession(t *testing.T) {
	db, err := storage.Open(t.TempDir() + "/manager.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	hash, _ := bcrypt.GenerateFromPassword([]byte("secret"), bcrypt.DefaultCost)
	cfg := config.Config{PairingCode: "p", AdminToken: "legacy", AdminUsername: "admin", AdminPasswordHash: string(hash)}
	srv := NewServer(cfg, db, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	client := ts.Client()
	body, _ := json.Marshal(map[string]string{"username": "admin", "password": "secret"})
	resp, err := client.Post(ts.URL+"/api/v1/auth/login", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("login status=%d", resp.StatusCode)
	}
	cookies := resp.Cookies()
	if len(cookies) == 0 {
		t.Fatal("missing session cookie")
	}
	if cookies[0].MaxAge != adminSessionCookieMaxAge {
		t.Fatalf("session cookie MaxAge=%d want %d (30 days)", cookies[0].MaxAge, adminSessionCookieMaxAge)
	}
	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/api/v1/instances", nil)
	req.AddCookie(cookies[0])
	resp, err = client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("session API status=%d", resp.StatusCode)
	}

	// 2. Verify session is persisted in DB
	sessRec, err := db.GetAuthSession(cookies[0].Value)
	if err != nil {
		t.Fatal(err)
	}
	if sessRec == nil || sessRec.Username != "admin" {
		t.Fatalf("expected session in DB, got %#v", sessRec)
	}

	// 3. Verify server restart (fresh Server instance with same DB) keeps user authenticated!
	srv2 := NewServer(cfg, db, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if len(srv2.authSessions) != 0 {
		t.Fatal("expected fresh in-memory authSessions")
	}
	authReq := httptest.NewRequest("GET", "/api/v1/auth/me", nil)
	authReq.AddCookie(cookies[0])
	authRec := httptest.NewRecorder()
	srv2.Handler().ServeHTTP(authRec, authReq)
	if authRec.Code != http.StatusOK {
		t.Fatalf("expected revived session to succeed, got %d", authRec.Code)
	}

	// 4. Verify logout removes from DB
	logoutReq, _ := http.NewRequest("POST", ts.URL+"/api/v1/auth/logout", nil)
	logoutReq.AddCookie(cookies[0])
	logoutResp, err := client.Do(logoutReq)
	if err != nil {
		t.Fatal(err)
	}
	logoutResp.Body.Close()
	sessRec, err = db.GetAuthSession(cookies[0].Value)
	if err != nil {
		t.Fatal(err)
	}
	if sessRec != nil {
		t.Fatal("expected session deleted from DB after logout")
	}
}

func TestLoginSecureCookieWithReverseProxy(t *testing.T) {
	db, err := storage.Open(t.TempDir() + "/manager.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	hash, _ := bcrypt.GenerateFromPassword([]byte("secret"), bcrypt.DefaultCost)
	cfg := config.Config{AdminUsername: "admin", AdminPasswordHash: string(hash)}
	srv := NewServer(cfg, db, slog.New(slog.NewTextHandler(io.Discard, nil)))

	req := httptest.NewRequest("POST", "/api/v1/auth/login", bytes.NewReader([]byte(`{"username":"admin","password":"secret"}`)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Forwarded-Proto", "https")
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d", rec.Code)
	}
	cookies := rec.Result().Cookies()
	if len(cookies) == 0 {
		t.Fatal("missing cookie")
	}
	if !cookies[0].Secure {
		t.Fatal("expected Secure cookie when X-Forwarded-Proto is https")
	}
}

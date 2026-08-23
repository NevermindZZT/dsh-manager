package api

import (
	"bytes"
	"crypto/tls"
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
	ts := httptest.NewTLSServer(srv.Handler())
	defer ts.Close()
	client := ts.Client()
	client.Transport.(*http.Transport).TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
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
}

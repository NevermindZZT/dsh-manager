package api

import (
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/NevermindZZT/dsh-manager/internal/config"
	"github.com/NevermindZZT/dsh-manager/internal/storage"
)

func TestDashboardServesIndex(t *testing.T) {
	db, err := storage.Open(t.TempDir() + "/manager.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	srv := NewServer(config.Config{PairingCode: "p", AdminToken: "a"}, db, slog.New(slog.NewTextHandler(io.Discard, nil)))
	req := httptest.NewRequest("GET", "/", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatalf("status=%d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "dsh-manager") {
		t.Fatalf("dashboard index not served: %s", rec.Body.String())
	}
	jsReq := httptest.NewRequest("GET", "/app.js", nil)
	jsRec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(jsRec, jsReq)
	if jsRec.Code != 200 || !strings.Contains(jsRec.Body.String(), "loginForm") {
		t.Fatalf("dashboard app.js not served: status=%d", jsRec.Code)
	}
}

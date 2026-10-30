package api

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

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
	svgReq := httptest.NewRequest("GET", "/favicon.svg", nil)
	svgRec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(svgRec, svgReq)
	if svgRec.Code != 200 || !strings.Contains(svgRec.Body.String(), "<svg") || !strings.Contains(svgRec.Header().Get("Content-Type"), "image/svg+xml") {
		t.Fatalf("favicon.svg not served: status=%d content-type=%s", svgRec.Code, svgRec.Header().Get("Content-Type"))
	}
	icoReq := httptest.NewRequest("GET", "/favicon.ico", nil)
	icoRec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(icoRec, icoReq)
	if icoRec.Code != 200 || !strings.Contains(icoRec.Body.String(), "<svg") || !strings.Contains(icoRec.Header().Get("Content-Type"), "image/svg+xml") {
		t.Fatalf("favicon.ico not served: status=%d content-type=%s", icoRec.Code, icoRec.Header().Get("Content-Type"))
	}
}

func TestDashboardServesAuthenticatedStateWithoutLoginFlash(t *testing.T) {
	db, err := storage.Open(t.TempDir() + "/manager.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	srv := NewServer(config.Config{AdminUsername: "admin", AdminPassword: "password"}, db, slog.New(slog.NewTextHandler(io.Discard, nil)))

	// 1. Unauthenticated request renders loginView without hidden class, appView with hidden class
	req := httptest.NewRequest("GET", "/manager", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatalf("status=%d", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, `id="loginView" class="login-wrap"`) || !strings.Contains(body, `id="appView" class="shell hidden"`) {
		t.Fatalf("unauthenticated dashboard should render login view initially: %s", body)
	}

	// 2. Authenticated request directly renders appView without hidden class, loginView with hidden class
	srv.sessionsAuthMu.Lock()
	sessionID := "test-session-auth-flash"
	srv.authSessions[sessionID] = time.Now().Add(time.Hour)
	srv.sessionsAuthMu.Unlock()

	authReq := httptest.NewRequest("GET", "/manager", nil)
	authReq.AddCookie(&http.Cookie{Name: "dsh-session", Value: sessionID})
	authRec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(authRec, authReq)
	if authRec.Code != 200 {
		t.Fatalf("status=%d", authRec.Code)
	}
	authBody := authRec.Body.String()
	if !strings.Contains(authBody, `id="loginView" class="login-wrap hidden"`) {
		t.Fatalf("authenticated dashboard should hide login view on initial HTML: %s", authBody)
	}
	if !strings.Contains(authBody, `id="appView" class="shell"`) || strings.Contains(authBody, `id="appView" class="shell hidden"`) {
		t.Fatalf("authenticated dashboard should show app view on initial HTML: %s", authBody)
	}
	if !strings.Contains(authBody, `已登录：admin`) {
		t.Fatalf("authenticated dashboard should pre-render username: %s", authBody)
	}
}

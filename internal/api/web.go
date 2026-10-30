package api

import (
	"embed"
	"html"
	"io/fs"
	"net/http"
	"strings"

	"github.com/NevermindZZT/dsh-manager/internal/version"
)

//go:embed web/index.html web/app.js web/favicon.svg
var dashboard embed.FS

func (s *Server) dashboard(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	root, err := fs.Sub(dashboard, "web")
	if err != nil {
		http.Error(w, "dashboard unavailable", http.StatusInternalServerError)
		return
	}
	data, err := fs.ReadFile(root, "index.html")
	if err != nil {
		http.Error(w, "dashboard unavailable", http.StatusInternalServerError)
		return
	}
	content := string(data)
	content = strings.ReplaceAll(content, "__DSH_MANAGER_VERSION__", "v"+version.Version)
	if s.isAuthenticated(r) {
		// When already authenticated, render the dashboard shell immediately
		// so page refreshes do not flash the login form while waiting for auth/me.
		content = strings.Replace(content, `id="loginView" class="login-wrap"`, `id="loginView" class="login-wrap hidden"`, 1)
		content = strings.Replace(content, `id="appView" class="shell hidden"`, `id="appView" class="shell"`, 1)
		username := s.cfg.AdminUsername
		if username == "" {
			username = "admin"
		}
		content = strings.Replace(content, `<small id="welcome">实例控制中心</small>`, `<small id="welcome">已登录：`+html.EscapeString(username)+`</small>`, 1)
		content = strings.Replace(content, `<span id="version" class="version"></span>`, `<span id="version" class="version">v`+version.Version+`</span>`, 1)
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(content))
}

func (s *Server) dashboardAsset(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	root, err := fs.Sub(dashboard, "web")
	if err != nil {
		http.Error(w, "dashboard unavailable", http.StatusInternalServerError)
		return
	}
	http.FileServer(http.FS(root)).ServeHTTP(w, r)
}

func (s *Server) dashboardFavicon(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "image/svg+xml")
	root, err := fs.Sub(dashboard, "web")
	if err != nil {
		http.Error(w, "dashboard unavailable", http.StatusInternalServerError)
		return
	}
	data, err := fs.ReadFile(root, "favicon.svg")
	if err != nil {
		http.NotFound(w, r)
		return
	}
	_, _ = w.Write(data)
}

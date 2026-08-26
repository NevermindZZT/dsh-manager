package api

import (
	"embed"
	"io/fs"
	"net/http"
	"strings"

	"github.com/NevermindZZT/dsh-manager/internal/version"
)

//go:embed web/index.html web/app.js
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
	data = []byte(strings.ReplaceAll(string(data), "__DSH_MANAGER_VERSION__", "v"+version.Version))
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(data)
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

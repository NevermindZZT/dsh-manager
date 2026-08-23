package api

import (
	"embed"
	"io/fs"
	"net/http"
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
	copyReq := r.Clone(r.Context())
	copyReq.URL.Path = "/"
	http.FileServer(http.FS(root)).ServeHTTP(w, copyReq)
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

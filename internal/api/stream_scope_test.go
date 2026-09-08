package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestSafeStreamAssetRequestExcludesDSHControlTraffic(t *testing.T) {
	tests := []struct {
		name   string
		method string
		path   string
		want   bool
	}{
		{"hashed CSS asset", http.MethodGet, "/assets/styles-abc123.css", true},
		{"hashed font asset", http.MethodHead, "/assets/font-abc123.woff2", true},
		{"JavaScript compatibility asset", http.MethodGet, "/assets/client-abc123.js", false},
		{"DSH RPC", http.MethodPost, "/api/session.list", false},
		{"DSH control GET", http.MethodGet, "/api/session.list", false},
		{"unhashed asset", http.MethodGet, "/assets/logo.svg", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequest(tt.method, tt.path, nil)
			if got := isSafeStreamAssetRequest(r); got != tt.want {
				t.Fatalf("isSafeStreamAssetRequest(%s %s) = %t, want %t", tt.method, tt.path, got, tt.want)
			}
		})
	}
}

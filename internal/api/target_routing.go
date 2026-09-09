package api

import (
	"context"
	"net/http"
	"net/url"
	"strings"
)

func withTargetCookie(header, sessionID string) string {
	parts := make([]string, 0)
	for _, part := range strings.Split(header, ";") {
		name, _, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok || strings.EqualFold(strings.TrimSpace(name), "dsh-target") {
			continue
		}
		parts = append(parts, strings.TrimSpace(part))
	}
	parts = append(parts, "dsh-target="+sessionID)
	return strings.Join(parts, "; ")
}

type targetSessionContextKey struct{}

func withTargetSession(r *http.Request, sessionID string) *http.Request {
	return r.WithContext(context.WithValue(r.Context(), targetSessionContextKey{}, sessionID))
}

func explicitTargetSession(r *http.Request) string {
	value, _ := r.Context().Value(targetSessionContextKey{}).(string)
	return value
}

func sessionIDFromPath(path string) string {
	if !strings.HasPrefix(path, "/dsh/") {
		return ""
	}
	rest := strings.TrimPrefix(path, "/dsh/")
	if index := strings.IndexByte(rest, '/'); index >= 0 {
		rest = rest[:index]
	}
	return strings.TrimSpace(rest)
}

func sessionIDFromReferer(r *http.Request) string {
	rawReferer := r.Header.Get("Referer")
	if rawReferer == "" {
		return ""
	}
	referer, err := url.Parse(rawReferer)
	if err != nil || !strings.EqualFold(referer.Host, r.Host) {
		return ""
	}
	return sessionIDFromPath(referer.Path)
}

func sessionIDForRequest(r *http.Request) string {
	if sessionID := explicitTargetSession(r); sessionID != "" {
		return sessionID
	}
	if sessionID := sessionIDFromPath(r.URL.Path); sessionID != "" {
		return sessionID
	}
	if sessionID := sessionIDFromReferer(r); sessionID != "" {
		return sessionID
	}
	if cookie, err := r.Cookie("dsh-target"); err == nil {
		return cookie.Value
	}
	return ""
}

package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestDecodeRouteValue(t *testing.T) {
	if got := decodeRouteValue("ssh%3ANas%20Ubuntu"); got != "ssh:Nas Ubuntu" {
		t.Fatalf("decoded route value=%q", got)
	}
}

func TestSessionIDForRequestPrecedence(t *testing.T) {
	tests := []struct {
		name    string
		path    string
		referer string
		cookie  string
		want    string
	}{
		{name: "explicit path wins", path: "/dsh/path-a/api", referer: "https://manager.test/dsh/path-b/", cookie: "path-c", want: "path-a"},
		{name: "same host referer wins root tab", path: "/api/session.list", referer: "https://manager.test/dsh/path-a/", cookie: "path-b", want: "path-a"},
		{name: "foreign referer ignored", path: "/api/session.list", referer: "https://other.test/dsh/path-a/", cookie: "path-b", want: "path-b"},
		{name: "cookie fallback", path: "/api/session.list", cookie: "path-b", want: "path-b"},
		{name: "no target", path: "/api/session.list", want: ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "https://manager.test"+test.path, nil)
			if test.referer != "" {
				req.Header.Set("Referer", test.referer)
			}
			if test.cookie != "" {
				req.AddCookie(&http.Cookie{Name: "dsh-target", Value: test.cookie})
			}
			if got := sessionIDForRequest(req); got != test.want {
				t.Fatalf("session=%q want %q", got, test.want)
			}
		})
	}
}

func TestExplicitTargetContextOverridesReferer(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "https://manager.test/api/session.list", nil)
	req.Header.Set("Referer", "https://manager.test/dsh/path-b/")
	req.AddCookie(&http.Cookie{Name: "dsh-target", Value: "path-c"})
	req = withTargetSession(req, "path-a")
	if got := sessionIDForRequest(req); got != "path-a" {
		t.Fatalf("session=%q want path-a", got)
	}
}

func TestDirectRootDoesNotUseOnlyGlobalTargetCookie(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "https://manager.test/", nil)
	req.AddCookie(&http.Cookie{Name: "dsh-target", Value: "path-a"})
	if got := sessionIDFromReferer(req); got != "" {
		t.Fatalf("referer session=%q want empty", got)
	}
	if got := sessionIDForRequest(req); got != "path-a" {
		t.Fatalf("request session=%q want path-a fallback", got)
	}
}

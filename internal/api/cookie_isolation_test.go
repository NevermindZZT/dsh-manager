package api

import "testing"

func TestProxyCookieJarIsolatesDshSessions(t *testing.T) {
	local := newProxyCookieJar()
	remote := newProxyCookieJar()
	local.apply([]string{"dsh-auth=local; Path=/; HttpOnly"}, "/")
	remote.apply([]string{"dsh-auth=remote; Path=/; HttpOnly"}, "/")

	if got := local.requestHeader("/", "dsh-target=local; dsh-session=manager; dsh-auth=stale"); got != "dsh-auth=local" {
		t.Fatalf("local cookie header=%q", got)
	}
	if got := remote.requestHeader("/", "dsh-target=remote; dsh-session=manager; dsh-auth=stale"); got != "dsh-auth=remote" {
		t.Fatalf("remote cookie header=%q", got)
	}
}

func TestProxyCookieJarMatchesPathAndExpiry(t *testing.T) {
	jar := newProxyCookieJar()
	jar.apply([]string{"sid=root; Path=/"}, "/")
	jar.apply([]string{"sid=api; Path=/api; Max-Age=60"}, "/api/login")

	if got := jar.requestHeader("/api/session.list", ""); got != "sid=api" {
		t.Fatalf("longest matching cookie=%q want sid=api", got)
	}
	if got := jar.requestHeader("/ws", ""); got != "sid=root" {
		t.Fatalf("unmatched path cookie=%q want sid=root", got)
	}

	jar.apply([]string{"sid=deleted; Path=/api; Max-Age=0"}, "/api/logout")
	if got := jar.requestHeader("/api/session.list", ""); got != "sid=root" {
		t.Fatalf("path-specific deletion=%q want sid=root", got)
	}
	jar.apply([]string{"expired=value; Path=/; Expires=Wed, 21 Oct 2015 07:28:00 GMT"}, "/")
	if got := jar.requestHeader("/", ""); got != "sid=root" {
		t.Fatalf("expired cookie replay=%q", got)
	}
}

func TestScopeProxySetCookie(t *testing.T) {
	if got, want := scopeProxySetCookie("dsh-auth=ok; Path=/; HttpOnly; SameSite=Lax", "dsh-local", "/"), "dsh-auth=ok; Path=/dsh/dsh-local/; HttpOnly; SameSite=Lax"; got != want {
		t.Fatalf("scoped cookie=%q want %q", got, want)
	}
	if got, want := scopeProxySetCookie("dsh-api=ok; Path=/api", "dsh-remote", "/api/session"), "dsh-api=ok; Path=/dsh/dsh-remote/api"; got != want {
		t.Fatalf("scoped path cookie=%q want %q", got, want)
	}
	if got, want := scopeProxySetCookie("dsh-api=ok", "dsh-remote", "/api/session"), "dsh-api=ok; Path=/dsh/dsh-remote/api/"; got != want {
		t.Fatalf("default path cookie=%q want %q", got, want)
	}
	if got := scopeProxySetCookie("dsh-target=upstream; Path=/", "dsh-remote", "/"); got != "" {
		t.Fatalf("reserved cookie was forwarded: %q", got)
	}
}

func TestManagerCookiesAreNotForwarded(t *testing.T) {
	jar := newProxyCookieJar()
	if got := jar.requestHeader("/", "dsh-session=manager; dsh-target=dsh-local"); got != "" {
		t.Fatalf("manager cookies leaked upstream: %q", got)
	}
}

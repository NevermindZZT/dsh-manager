package api

import (
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

type proxyCookie struct {
	value     string
	path      string
	expiresAt time.Time
}

type proxyCookieJar struct {
	mu     sync.Mutex
	values map[string]map[string]proxyCookie
}

func newProxyCookieJar() *proxyCookieJar {
	return &proxyCookieJar{values: make(map[string]map[string]proxyCookie)}
}

func isManagerCookieName(name string) bool {
	return strings.EqualFold(name, "dsh-session") || strings.EqualFold(name, "dsh-target")
}

func normalizeProxyCookiePath(path string) string {
	path = strings.TrimSpace(path)
	if path == "" || !strings.HasPrefix(path, "/") {
		return "/"
	}
	return path
}

func defaultProxyCookiePath(requestPath string) string {
	requestPath = normalizeProxyCookiePath(requestPath)
	if requestPath == "/" {
		return "/"
	}
	index := strings.LastIndex(requestPath, "/")
	if index <= 0 {
		return "/"
	}
	return requestPath[:index+1]
}

func proxyCookiePathMatches(path, requestPath string) bool {
	path = normalizeProxyCookiePath(path)
	requestPath = normalizeProxyCookiePath(requestPath)
	if path == "/" || requestPath == path {
		return true
	}
	return strings.HasPrefix(requestPath, path) && (strings.HasSuffix(path, "/") || (len(requestPath) > len(path) && requestPath[len(path)] == '/'))
}

func (j *proxyCookieJar) requestHeader(requestPath, browserHeader string) string {
	values := make(map[string]string)
	order := make([]string, 0)
	add := func(name, value string) {
		name = strings.TrimSpace(name)
		if name == "" || isManagerCookieName(name) {
			return
		}
		if _, ok := values[name]; !ok {
			order = append(order, name)
		}
		values[name] = strings.TrimSpace(value)
	}
	for _, part := range strings.Split(browserHeader, ";") {
		name, value, ok := strings.Cut(part, "=")
		if ok {
			add(name, value)
		}
	}
	now := time.Now()
	j.mu.Lock()
	names := make([]string, 0, len(j.values))
	for name := range j.values {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		var selected proxyCookie
		found := false
		for path, cookie := range j.values[name] {
			if !cookie.expiresAt.IsZero() && !now.Before(cookie.expiresAt) {
				continue
			}
			if !proxyCookiePathMatches(path, requestPath) || (found && len(path) <= len(selected.path)) {
				continue
			}
			selected, found = cookie, true
		}
		if found {
			add(name, selected.value)
		}
	}
	j.mu.Unlock()
	parts := make([]string, 0, len(order))
	for _, name := range order {
		parts = append(parts, name+"="+values[name])
	}
	return strings.Join(parts, "; ")
}

func proxySetCookieName(raw string) string {
	first, _, _ := strings.Cut(raw, ";")
	name, _, ok := strings.Cut(strings.TrimSpace(first), "=")
	if !ok {
		return ""
	}
	return strings.TrimSpace(name)
}

func parseProxySetCookie(raw string) *http.Cookie {
	header := http.Header{}
	header.Add("Set-Cookie", raw)
	response := http.Response{Header: header}
	cookies := response.Cookies()
	if len(cookies) != 1 || cookies[0].Name == "" || isManagerCookieName(cookies[0].Name) {
		return nil
	}
	return cookies[0]
}

func (j *proxyCookieJar) apply(setCookies []string, requestPath string) {
	now := time.Now()
	j.mu.Lock()
	defer j.mu.Unlock()
	for _, raw := range setCookies {
		cookie := parseProxySetCookie(raw)
		if cookie == nil {
			continue
		}
		path := normalizeProxyCookiePath(cookie.Path)
		if cookie.Path == "" {
			path = defaultProxyCookiePath(requestPath)
		}
		byPath := j.values[cookie.Name]
		if byPath == nil {
			byPath = make(map[string]proxyCookie)
			j.values[cookie.Name] = byPath
		}
		if cookie.MaxAge < 0 || (!cookie.Expires.IsZero() && cookie.Expires.Before(now)) {
			delete(byPath, path)
			if len(byPath) == 0 {
				delete(j.values, cookie.Name)
			}
			continue
		}
		expiresAt := time.Time{}
		if cookie.MaxAge > 0 {
			expiresAt = now.Add(time.Duration(cookie.MaxAge) * time.Second)
		} else if !cookie.Expires.IsZero() {
			expiresAt = cookie.Expires
		}
		byPath[path] = proxyCookie{value: cookie.Value, path: path, expiresAt: expiresAt}
	}
}

func scopeProxyCookiePath(sessionID, path string) string {
	prefix := "/dsh/" + sessionID
	path = strings.TrimSpace(path)
	if path == "" || path == "/" {
		return prefix + "/"
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	if path == prefix || strings.HasPrefix(path, prefix+"/") {
		return path
	}
	return prefix + path
}

func scopeProxySetCookie(raw, sessionID, requestPath string) string {
	if isManagerCookieName(proxySetCookieName(raw)) {
		return ""
	}
	if sessionID == "" {
		return raw
	}
	parts := strings.Split(raw, ";")
	foundPath := false
	for i := 1; i < len(parts); i++ {
		attribute, value, hasValue := strings.Cut(strings.TrimSpace(parts[i]), "=")
		if !strings.EqualFold(strings.TrimSpace(attribute), "path") {
			continue
		}
		if !hasValue {
			value = "/"
		}
		parts[i] = " Path=" + scopeProxyCookiePath(sessionID, value)
		foundPath = true
	}
	if !foundPath {
		parts = append(parts, " Path="+scopeProxyCookiePath(sessionID, defaultProxyCookiePath(requestPath)))
	}
	return strings.Join(parts, ";")
}

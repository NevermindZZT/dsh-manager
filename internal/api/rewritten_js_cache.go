package api

import (
	"bytes"
	"compress/gzip"
	"container/list"
	"net/http"
	"strings"
	"sync"
	"strconv"
)

const (
	rewrittenJSCacheBytes = 32 << 20
	rewriteCacheVersion = "browser-compat-v1"
)

type rewrittenJSCacheEntry struct {
	key string
	headers map[string]string
	identity []byte
	gzip []byte
	bytes int
	element *list.Element
}

type rewrittenJSCache struct {
	mu sync.Mutex
	entries map[string]*rewrittenJSCacheEntry
	lru *list.List
	maxBytes int
	usedBytes int
	hits uint64
	misses uint64
	evictions uint64
}

func newRewrittenJSCache(maxBytes int) *rewrittenJSCache {
	if maxBytes <= 0 { maxBytes = rewrittenJSCacheBytes }
	return &rewrittenJSCache{entries: make(map[string]*rewrittenJSCacheEntry), lru: list.New(), maxBytes: maxBytes}
}

func rewrittenJSCacheKey(target webTarget, r *http.Request) string {
	return rewriteCacheVersion + "\x00" + target.AgentID + "\x00" + target.InstanceID + "\x00" + r.URL.Path + "\x00" + r.URL.RawQuery
}

func acceptsGzip(r *http.Request) bool {
	for _, value := range strings.Split(r.Header.Get("Accept-Encoding"), ",") {
		parts := strings.Split(strings.TrimSpace(value), ";")
		if len(parts) > 0 && strings.EqualFold(strings.TrimSpace(parts[0]), "gzip") { return true }
	}
	return false
}

func (c *rewrittenJSCache) serve(w http.ResponseWriter, r *http.Request, key string) bool {
	if c == nil || (r.Method != http.MethodGet && r.Method != http.MethodHead) { return false }
	c.mu.Lock()
	entry := c.entries[key]
	if entry == nil { c.misses++; c.mu.Unlock(); return false }
	c.hits++
	c.lru.MoveToFront(entry.element)
	headers := cloneStringHeaders(entry.headers)
	identity := append([]byte(nil), entry.identity...)
	gzipped := append([]byte(nil), entry.gzip...)
	c.mu.Unlock()
	for name, value := range headers { w.Header().Set(name, value) }
	for _, name := range []string{"Content-Length", "Content-Encoding", "ETag", "Content-MD5", "Digest"} { w.Header().Del(name) }
	if acceptsGzip(r) && len(gzipped) > 0 {
		w.Header().Set("Content-Encoding", "gzip")
		w.Header().Set("Content-Length", strconvItoa(len(gzipped)))
	} else {
		w.Header().Del("Content-Encoding")
		w.Header().Set("Content-Length", strconvItoa(len(identity)))
	}
	w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
	addVaryHeader(w, "Accept-Encoding")
	w.WriteHeader(http.StatusOK)
	if r.Method != http.MethodHead {
		if acceptsGzip(r) && len(gzipped) > 0 { _, _ = w.Write(gzipped) } else { _, _ = w.Write(identity) }
	}
	return true
}

func (c *rewrittenJSCache) add(key string, headers map[string]string, identity []byte) {
	if c == nil || len(identity) == 0 { return }
	gzipped, ok := gzipBytes(identity)
	if !ok { return }
	safeHeaders := safeCachedHeaders(headers)
	size := len(identity) + len(gzipped)
	if size > c.maxBytes { return }
	c.mu.Lock()
	defer c.mu.Unlock()
	if previous := c.entries[key]; previous != nil { c.remove(previous) }
	for c.usedBytes+size > c.maxBytes && c.lru.Len() > 0 { c.remove(c.lru.Back().Value.(*rewrittenJSCacheEntry)); c.evictions++ }
	entry := &rewrittenJSCacheEntry{key: key, headers: safeHeaders, identity: append([]byte(nil), identity...), gzip: gzipped, bytes: size}
	entry.element = c.lru.PushFront(entry)
	c.entries[key] = entry
	c.usedBytes += size
}

func (c *rewrittenJSCache) remove(entry *rewrittenJSCacheEntry) {
	delete(c.entries, entry.key)
	c.lru.Remove(entry.element)
	c.usedBytes -= entry.bytes
}

func gzipBytes(data []byte) ([]byte, bool) {
	var out bytes.Buffer
	writer := gzip.NewWriter(&out)
	if _, err := writer.Write(data); err != nil { return nil, false }
	if err := writer.Close(); err != nil { return nil, false }
	return out.Bytes(), true
}

func safeCachedHeaders(headers map[string]string) map[string]string {
	result := make(map[string]string, len(headers))
	for name, value := range headers {
		switch strings.ToLower(name) {
		case "content-length", "content-encoding", "etag", "content-md5", "digest", "set-cookie":
			continue
		}
		if !isHopHeader(name) { result[name] = value }
	}
	return result
}

func cacheableRewrittenJS(r *http.Request, status int, headers map[string]string, hasCookies, changed bool) bool {
	if !changed || status != http.StatusOK || hasCookies || (r.Method != http.MethodGet && r.Method != http.MethodHead) || !isImmutableAssetPath(r.URL.Path) || !requiresBrowserCompatibility(r.URL.Path) { return false }
	for name, value := range headers {
		if strings.EqualFold(name, "Content-Encoding") && strings.TrimSpace(value) != "" { return false }
		if strings.EqualFold(name, "Cache-Control") { lower := strings.ToLower(value); if strings.Contains(lower, "no-store") || strings.Contains(lower, "private") { return false } }
	}
	return true
}

func cloneStringHeaders(value map[string]string) map[string]string {
	result := make(map[string]string, len(value)); for key, item := range value { result[key] = item }; return result
}

type rewrittenJSCacheSnapshot struct { Entries int; Bytes int; MaxBytes int; Hits uint64; Misses uint64; Evictions uint64 }
func (c *rewrittenJSCache) snapshot() rewrittenJSCacheSnapshot { if c == nil { return rewrittenJSCacheSnapshot{} }; c.mu.Lock(); defer c.mu.Unlock(); return rewrittenJSCacheSnapshot{Entries: len(c.entries), Bytes: c.usedBytes, MaxBytes: c.maxBytes, Hits: c.hits, Misses: c.misses, Evictions: c.evictions} }

// Kept local so the cache stays independent of server.go's strconv import.
func strconvItoa(value int) string { return strconv.Itoa(value) }

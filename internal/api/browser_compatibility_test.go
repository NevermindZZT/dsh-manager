package api

import (
	"strings"
	"testing"
)

func TestInjectBrowserCompatibilityEnablesPrivilegedRemoteSurface(t *testing.T) {
	script := []byte(`const persistence = ctx.remote.$host.isLoopback ? "host" : "memory"; const handle = { isLoopback: transport?.ownsHost === true || pageLocation === void 0 || isLoopbackHostname(pageLocation.hostname) };`)
	got := string(injectBrowserCompatibility(map[string]string{"Content-Type": "application/javascript"}, script))
	if strings.Contains(got, "ctx.remote.$host.isLoopback") || strings.Contains(got, "isLoopbackHostname(pageLocation.hostname)") {
		t.Fatalf("compatibility rewrite was not applied: %s", got)
	}
	if !strings.Contains(got, `const persistence = "host"`) || !strings.Contains(got, "isLoopback: true") {
		t.Fatalf("unexpected compatibility rewrite: %s", got)
	}
}

func TestInjectBrowserCompatibilityDoesNotModifyCompressedScripts(t *testing.T) {
	script := []byte(`const persistence = ctx.remote.$host.isLoopback ? "host" : "memory";`)
	got := injectBrowserCompatibility(map[string]string{"Content-Type": "application/javascript", "Content-Encoding": "gzip"}, script)
	if string(got) != string(script) {
		t.Fatal("compressed script must not be rewritten")
	}
}

func TestRequiresBrowserCompatibility(t *testing.T) {
	if !requiresBrowserCompatibility("/assets/client-abc.js") {
		t.Fatal("JavaScript asset must request identity encoding")
	}
	if requiresBrowserCompatibility("/assets/styles-abc.css") {
		t.Fatal("non-JavaScript asset must preserve compression")
	}
}

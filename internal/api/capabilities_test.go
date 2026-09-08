package api

import "testing"

func TestLegacyAndPluginCapabilities(t *testing.T) {
	legacy := &agentSession{agentType: "launcher"}
	if !supportsCapability(legacy, "proxy.http") || !supportsCapability(legacy, "proxy.websocket") {
		t.Fatal("legacy launcher must retain proxy capabilities")
	}
	if supportsCapability(legacy, "proxy.binary-response-v1") || supportsCapability(legacy, "proxy.http-stream-v1") || supportsCapability(legacy, "proxy.binary-websocket-frame-v1") || supportsCapability(legacy, "proxy.cancel-v1") {
		t.Fatal("legacy launcher must fall back from optional binary transports")
	}
	plugin := &agentSession{agentType: "dsh-plugin", hasCapabilities: true, capabilities: []string{"proxy.http"}}
	if !supportsCapability(plugin, "proxy.http") {
		t.Fatal("plugin HTTP capability missing")
	}
	if supportsCapability(plugin, "proxy.websocket") {
		t.Fatal("plugin must not gain undeclared capability")
	}
	if got := normalizeAgentType(""); got != "launcher" {
		t.Fatalf("default agent type = %q", got)
	}
	got := normalizeCapabilities([]string{" Proxy.HTTP ", "proxy.http", "proxy.websocket"})
	if len(got) != 2 || got[0] != "proxy.http" || got[1] != "proxy.websocket" {
		t.Fatalf("normalized capabilities = %#v", got)
	}
}

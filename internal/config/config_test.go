package config

import (
	"path/filepath"
	"testing"
)

func TestLoadGeneratesFreshPairingCodeOnEveryStart(t *testing.T) {
	t.Setenv("DSH_MANAGER_CONFIG", filepath.Join(t.TempDir(), "config.yaml"))
	t.Setenv("DSH_MANAGER_PAIRING_CODE", "legacy-static-code")
	t.Setenv("DSH_MANAGER_ADMIN_TOKEN", "admin-token")
	t.Setenv("DSH_MANAGER_ADMIN_PASSWORD", "admin-password")

	first := Load()
	second := Load()
	if first.PairingCode == "" || second.PairingCode == "" {
		t.Fatal("Load returned an empty pairing code")
	}
	if first.PairingCode == second.PairingCode {
		t.Fatalf("pairing code was reused across starts: %q", first.PairingCode)
	}
	if first.PairingCode == "legacy-static-code" || second.PairingCode == "legacy-static-code" {
		t.Fatal("legacy configured pairing code was reused")
	}
	if !first.PairingCodeGenerated || !second.PairingCodeGenerated {
		t.Fatal("fresh pairing codes must be marked as generated")
	}
}

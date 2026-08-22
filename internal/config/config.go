package config

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type Config struct {
	HTTPAddr             string
	DataDir              string
	DatabasePath         string
	PairingCode          string
	AdminToken           string
	PairingCodeGenerated bool
	AdminTokenGenerated  bool
}

func Load() Config {
	dataDir := getenv("DSH_MANAGER_DATA_DIR", "./data")
	pairing := strings.TrimSpace(os.Getenv("DSH_MANAGER_PAIRING_CODE"))
	pairingGenerated := pairing == ""
	if pairingGenerated {
		pairing = randomHex(8)
	}
	admin := strings.TrimSpace(os.Getenv("DSH_MANAGER_ADMIN_TOKEN"))
	adminGenerated := admin == ""
	if adminGenerated {
		admin = "adm_" + randomHex(24)
	}
	return Config{HTTPAddr: getenv("DSH_MANAGER_HTTP_ADDR", ":8080"), DataDir: dataDir, DatabasePath: filepath.Join(dataDir, "dsh-manager.db"), PairingCode: pairing, AdminToken: admin, PairingCodeGenerated: pairingGenerated, AdminTokenGenerated: adminGenerated}
}

func getenv(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func randomHex(bytes int) string {
	raw := make([]byte, bytes)
	if _, err := rand.Read(raw); err != nil {
		panic(fmt.Errorf("generate random value: %w", err))
	}
	return hex.EncodeToString(raw)
}

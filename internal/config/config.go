package config

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/crypto/bcrypt"
	"gopkg.in/yaml.v3"
)

type Config struct {
	ConfigFile             string
	HTTPAddr               string
	AgentHTTPSAddr         string
	DataDir                string
	TLSCertFile            string
	TLSKeyFile             string
	TLSFingerprint         string
	DatabasePath           string
	PairingCode            string
	AdminToken             string
	PairingCodeGenerated   bool
	AdminTokenGenerated    bool
	AdminUsername          string
	AdminPasswordHash      string
	AdminPassword          string
	AdminPasswordGenerated bool
}

type fileConfig struct {
	HTTPAddr          string `yaml:"http_addr"`
	AgentHTTPSAddr    string `yaml:"agent_https_addr"`
	DataDir           string `yaml:"data_dir"`
	TLSCertFile       string `yaml:"tls_cert_file"`
	TLSKeyFile        string `yaml:"tls_key_file"`
	DatabasePath      string `yaml:"database_path"`
	PairingCode       string `yaml:"pairing_code"`
	AdminToken        string `yaml:"admin_token"`
	AdminUsername     string `yaml:"admin_username"`
	AdminPassword     string `yaml:"admin_password"`
	AdminPasswordHash string `yaml:"admin_password_hash"`
}

func Load() Config {
	configFile := getenv("DSH_MANAGER_CONFIG", "./config.yaml")
	var file fileConfig
	if data, err := os.ReadFile(configFile); err == nil {
		if err := yaml.Unmarshal(data, &file); err != nil {
			panic(fmt.Errorf("parse config file %s: %w", configFile, err))
		}
	}
	dataDir := envOr("DSH_MANAGER_DATA_DIR", file.DataDir, "./data")
	// Pairing codes are ephemeral enrollment secrets. Generate a fresh code
	// for every manager process instead of reusing a value from YAML/env. Existing
	// Agent tokens are stored in the database and remain valid across this change.
	pairing := randomHex(8)
	pairingGenerated := true
	admin := envOr("DSH_MANAGER_ADMIN_TOKEN", file.AdminToken, "")
	adminGenerated := admin == ""
	if adminGenerated {
		admin = "adm_" + randomHex(24)
	}
	username := envOr("DSH_MANAGER_ADMIN_USERNAME", file.AdminUsername, "admin")
	passwordHash := envOr("DSH_MANAGER_ADMIN_PASSWORD_HASH", file.AdminPasswordHash, "")
	password := envOr("DSH_MANAGER_ADMIN_PASSWORD", file.AdminPassword, "")
	passwordGenerated := passwordHash == "" && password == ""
	if passwordHash == "" {
		if password == "" {
			password = "mgr_" + randomHex(18)
		}
		hashed, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
		if err != nil {
			panic(fmt.Errorf("hash admin password: %w", err))
		}
		passwordHash = string(hashed)
	}
	return Config{
		ConfigFile:     configFile,
		HTTPAddr:       envOr("DSH_MANAGER_HTTP_ADDR", file.HTTPAddr, ":8080"),
		AgentHTTPSAddr: envOr("DSH_MANAGER_AGENT_HTTPS_ADDR", file.AgentHTTPSAddr, ":8443"),
		DataDir:        dataDir,
		TLSCertFile:    envOr("DSH_MANAGER_TLS_CERT", file.TLSCertFile, filepath.Join(dataDir, "server.crt")),
		TLSKeyFile:     envOr("DSH_MANAGER_TLS_KEY", file.TLSKeyFile, filepath.Join(dataDir, "server.key")),
		DatabasePath:   envOr("DSH_MANAGER_DATABASE", file.DatabasePath, filepath.Join(dataDir, "dsh-manager.db")),
		PairingCode:    pairing, AdminToken: admin,
		PairingCodeGenerated: pairingGenerated, AdminTokenGenerated: adminGenerated,
		AdminUsername: username, AdminPasswordHash: passwordHash, AdminPassword: password,
		AdminPasswordGenerated: passwordGenerated,
	}
}

func getenv(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}
func envOr(key, fileValue, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	if value := strings.TrimSpace(fileValue); value != "" {
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

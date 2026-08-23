package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/NevermindZZT/dsh-manager/internal/api"
	"github.com/NevermindZZT/dsh-manager/internal/config"
	"github.com/NevermindZZT/dsh-manager/internal/security"
	"github.com/NevermindZZT/dsh-manager/internal/storage"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)
	cfg := config.Load()
	db, err := storage.Open(cfg.DatabasePath)
	if err != nil {
		logger.Error("open database", "error", err)
		os.Exit(1)
	}
	defer db.Close()
	fingerprint, err := security.EnsureCertificate(cfg.TLSCertFile, cfg.TLSKeyFile)
	if err != nil {
		logger.Error("prepare TLS certificate", "error", err)
		os.Exit(1)
	}
	cfg.TLSFingerprint = fingerprint
	server := api.NewServer(cfg, db, logger)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	logger.Info("dsh-manager starting", "httpAddr", cfg.HTTPAddr, "agentHTTPSAddr", cfg.AgentHTTPSAddr, "data", cfg.DataDir, "configFile", cfg.ConfigFile)
	logger.Info("agent TLS certificate", "fingerprintSha256", fingerprint, "certFile", cfg.TLSCertFile)
	logger.Info("pairing code ready", "code", cfg.PairingCode, "generated", cfg.PairingCodeGenerated)
	if cfg.AdminTokenGenerated {
		logger.Info("generated admin API token; store it securely", "token", cfg.AdminToken)
	} else {
		logger.Info("admin API token loaded from environment")
	}
	if cfg.AdminPasswordGenerated {
		logger.Info("generated dashboard login", "username", cfg.AdminUsername, "password", cfg.AdminPassword)
	} else {
		logger.Info("dashboard login loaded from environment", "username", cfg.AdminUsername)
	}
	if err := server.Run(ctx); err != nil {
		logger.Error("server stopped", "error", err)
		os.Exit(1)
	}
}

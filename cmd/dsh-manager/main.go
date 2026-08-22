package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/NevermindZZT/dsh-manager/internal/api"
	"github.com/NevermindZZT/dsh-manager/internal/config"
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
	server := api.NewServer(cfg, db, logger)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	logger.Info("dsh-manager starting", "addr", cfg.HTTPAddr, "data", cfg.DataDir)
	logger.Info("pairing code ready", "code", cfg.PairingCode, "generated", cfg.PairingCodeGenerated)
	if cfg.AdminTokenGenerated {
		logger.Info("generated admin token; store it securely", "token", cfg.AdminToken)
	} else {
		logger.Info("admin token loaded from environment")
	}
	if err := server.Run(ctx); err != nil {
		logger.Error("server stopped", "error", err)
		os.Exit(1)
	}
}

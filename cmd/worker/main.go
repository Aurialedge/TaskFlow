package main

import (
	"context"
	"log/slog"
	"time"

	"github.com/Aurialedge/TaskFlow/internal/config"
	"github.com/Aurialedge/TaskFlow/internal/observability"
	"github.com/Aurialedge/TaskFlow/internal/repository/postgres"
	"github.com/Aurialedge/TaskFlow/internal/shutdown"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		slog.Error("failed to load configuration", "error", err)
		return
	}

	logger := observability.NewLogger(cfg.Env, cfg.LogLevel, nil)
	logger.Info("starting TaskFlow Worker service",
		"env", cfg.Env,
		"concurrency", cfg.Worker.Concurrency,
		"poll_interval", cfg.Worker.PollInterval,
		"lease_duration", cfg.Worker.LeaseDuration,
	)

	ctx, cancel := shutdown.GracefulContext()
	defer cancel()

	// Connect to PostgreSQL
	dbCtx, dbCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer dbCancel()

	dbClient, err := postgres.NewClient(dbCtx, cfg.Database)
	if err != nil {
		logger.Warn("could not connect to PostgreSQL immediately", "error", err)
	} else {
		defer dbClient.Close()
		logger.Info("worker connected to PostgreSQL successfully")
	}

	logger.Info("worker process initialized and awaiting jobs (Phase 1 skeleton)")

	<-ctx.Done()
	logger.Info("worker received shutdown signal; stopping worker loop")

	if err := shutdown.Drain(cfg.ShutdownTimeout, func(drainCtx context.Context) error {
		// In Phase 3, this drains the bounded worker pool and releases leases.
		return nil
	}); err != nil {
		logger.Error("error during worker graceful drain", "error", err)
	} else {
		logger.Info("worker gracefully stopped")
	}
}

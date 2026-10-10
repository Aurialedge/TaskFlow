package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/Aurialedge/TaskFlow/internal/config"
	"github.com/Aurialedge/TaskFlow/internal/observability"
	"github.com/Aurialedge/TaskFlow/internal/repository/postgres"
	"github.com/Aurialedge/TaskFlow/internal/shutdown"
	"github.com/Aurialedge/TaskFlow/internal/worker"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		slog.Error("failed to load configuration", "error", err)
		return
	}

	logger := observability.NewLogger(cfg.Env, cfg.LogLevel, nil)

	hostname, _ := os.Hostname()
	workerID := fmt.Sprintf("%s-%d", hostname, os.Getpid())

	logger.Info("starting TaskFlow Worker service",
		"env", cfg.Env,
		"worker_id", workerID,
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
		logger.Error("worker could not connect to PostgreSQL", "error", err)
		return
	}
	defer dbClient.Close()
	logger.Info("worker connected to PostgreSQL successfully")

	// Initialize worker components
	jobRepo := postgres.NewJobRepository(dbClient.Pool)
	registry := worker.NewDefaultRegistry()
	logger.Info("handlers registered", "types", registry.RegisteredTypes())

	pool := worker.NewPool(cfg.Worker.Concurrency, workerID, jobRepo, registry)
	pool.Start()

	dispatcher := worker.NewDispatcher(
		jobRepo,
		pool,
		"", // all queues
		cfg.Worker.PollInterval,
		cfg.Worker.LeaseDuration,
		workerID,
	)

	// Run dispatcher loop in background
	dispatcherDone := make(chan struct{})
	go func() {
		defer close(dispatcherDone)
		dispatcher.Start(ctx)
	}()

	logger.Info("worker engine active and polling for jobs")

	// Wait for termination signal
	<-ctx.Done()
	logger.Info("worker received shutdown signal; stopping dispatcher and draining pool")

	// Stop dispatcher first so no new jobs are claimed
	dispatcher.Stop()
	<-dispatcherDone

	// Gracefully drain pool
	if err := shutdown.Drain(cfg.ShutdownTimeout, func(drainCtx context.Context) error {
		pool.Stop()
		return nil
	}); err != nil {
		logger.Error("error during worker graceful drain", "error", err)
	} else {
		logger.Info("worker pool gracefully drained and stopped")
	}
}

package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/Aurialedge/TaskFlow/internal/api"
	"github.com/Aurialedge/TaskFlow/internal/config"
	"github.com/Aurialedge/TaskFlow/internal/observability"
	"github.com/Aurialedge/TaskFlow/internal/repository/postgres"
	"github.com/Aurialedge/TaskFlow/internal/service"
	"github.com/Aurialedge/TaskFlow/internal/shutdown"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		slog.Error("failed to load configuration", "error", err)
		return
	}

	logger := observability.NewLogger(cfg.Env, cfg.LogLevel, nil)
	logger.Info("starting TaskFlow API service", "env", cfg.Env, "port", cfg.HTTP.Port)

	ctx, cancel := shutdown.GracefulContext()
	defer cancel()

	// Connect to PostgreSQL
	dbCtx, dbCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer dbCancel()

	var dbClient *postgres.Client
	var jobService *service.JobService

	dbClient, err = postgres.NewClient(dbCtx, cfg.Database)
	if err != nil {
		logger.Warn("could not establish initial database connection; ready check will report 503", "error", err)
	} else {
		defer dbClient.Close()
		logger.Info("connected to PostgreSQL successfully")

		// Run pending migrations
		migrator := postgres.NewMigrator(dbClient.Pool)
		if err := migrator.MigrateUp(context.Background()); err != nil {
			logger.Error("failed to apply database migrations", "error", err)
			return
		}

		jobRepo := postgres.NewJobRepository(dbClient.Pool)
		jobService = service.NewJobService(jobRepo)
	}

	router := api.NewRouter(api.RouterConfig{
		Database:   dbClient,
		JobService: jobService,
		APIKey:     cfg.HTTP.APIKey,
	})

	server := &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.HTTP.Port),
		Handler:      router,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	serverErrCh := make(chan error, 1)
	go func() {
		logger.Info("HTTP server listening", "addr", server.Addr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErrCh <- err
		}
	}()

	select {
	case <-ctx.Done():
		logger.Info("shutdown signal received; draining in-flight requests")
	case err := <-serverErrCh:
		logger.Error("HTTP server failed", "error", err)
		return
	}

	if err := shutdown.Drain(cfg.ShutdownTimeout, func(drainCtx context.Context) error {
		return server.Shutdown(drainCtx)
	}); err != nil {
		logger.Error("error during graceful shutdown", "error", err)
	} else {
		logger.Info("API server gracefully stopped")
	}
}

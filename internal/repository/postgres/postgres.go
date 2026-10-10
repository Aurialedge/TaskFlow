package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Aurialedge/TaskFlow/internal/config"
)

// Client encapsulates the PostgreSQL connection pool.
type Client struct {
	Pool *pgxpool.Pool
}

// NewClient initializes a pgxpool.Pool from the provided database configuration.
func NewClient(ctx context.Context, cfg config.DatabaseConfig) (*Client, error) {
	poolCfg, err := pgxpool.ParseConfig(cfg.ConnectionString())
	if err != nil {
		return nil, fmt.Errorf("failed to parse database config: %w", err)
	}

	poolCfg.MaxConns = cfg.MaxConns
	poolCfg.MinConns = cfg.MinConns
	poolCfg.MaxConnIdleTime = cfg.MaxConnIdleTime
	poolCfg.MaxConnLifetime = cfg.MaxConnLifetime

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create pgx pool: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("database ping failed: %w", err)
	}

	return &Client{Pool: pool}, nil
}

// Ping checks whether the PostgreSQL database connection pool is healthy.
func (c *Client) Ping(ctx context.Context) error {
	if c == nil || c.Pool == nil {
		return fmt.Errorf("database pool is not initialized")
	}
	return c.Pool.Ping(ctx)
}

// Close gracefully releases the pgx connection pool.
func (c *Client) Close() {
	if c != nil && c.Pool != nil {
		c.Pool.Close()
	}
}

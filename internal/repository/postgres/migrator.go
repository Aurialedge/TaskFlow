package postgres

import (
	"context"
	"fmt"
	"io/fs"
	"log/slog"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Aurialedge/TaskFlow/migrations"
)

// Migration represents a parsed SQL migration file.
type Migration struct {
	Version   int
	Name      string
	Direction string // "up" or "down"
	Filename  string
}

// Migrator manages and applies database schema migrations.
type Migrator struct {
	pool *pgxpool.Pool
	fs   fs.FS
}

// NewMigrator returns a new Migrator instance.
func NewMigrator(pool *pgxpool.Pool, customFS ...fs.FS) *Migrator {
	migrationFS := fs.FS(migrations.FS)
	if len(customFS) > 0 && customFS[0] != nil {
		migrationFS = customFS[0]
	}

	return &Migrator{
		pool: pool,
		fs:   migrationFS,
	}
}

// EnsureSchemaMigrationsTable initializes the migration tracking table.
func (m *Migrator) EnsureSchemaMigrationsTable(ctx context.Context) error {
	query := `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version INT PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
		);
	`
	_, err := m.pool.Exec(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to create schema_migrations table: %w", err)
	}
	return nil
}

// GetAppliedVersions returns a set of all currently applied migration versions.
func (m *Migrator) GetAppliedVersions(ctx context.Context) (map[int]bool, error) {
	if err := m.EnsureSchemaMigrationsTable(ctx); err != nil {
		return nil, err
	}

	rows, err := m.pool.Query(ctx, `SELECT version FROM schema_migrations ORDER BY version ASC`)
	if err != nil {
		return nil, fmt.Errorf("failed to query schema_migrations: %w", err)
	}
	defer rows.Close()

	applied := make(map[int]bool)
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			return nil, fmt.Errorf("failed to scan migration version: %w", err)
		}
		applied[v] = true
	}
	return applied, rows.Err()
}

// MigrateUp applies all pending up migrations in ascending order.
func (m *Migrator) MigrateUp(ctx context.Context) error {
	applied, err := m.GetAppliedVersions(ctx)
	if err != nil {
		return err
	}

	entries, err := fs.ReadDir(m.fs, ".")
	if err != nil {
		return fmt.Errorf("failed to read migrations directory: %w", err)
	}

	var upMigrations []Migration
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".up.sql") {
			continue
		}

		migration, err := parseMigrationFilename(entry.Name())
		if err != nil {
			return err
		}
		if migration.Direction == "up" {
			upMigrations = append(upMigrations, migration)
		}
	}

	sort.Slice(upMigrations, func(i, j int) bool {
		return upMigrations[i].Version < upMigrations[j].Version
	})

	for _, mig := range upMigrations {
		if applied[mig.Version] {
			continue
		}

		slog.InfoContext(ctx, "applying database migration", "version", mig.Version, "name", mig.Name)
		content, err := fs.ReadFile(m.fs, mig.Filename)
		if err != nil {
			return fmt.Errorf("failed to read migration file %s: %w", mig.Filename, err)
		}

		tx, err := m.pool.Begin(ctx)
		if err != nil {
			return fmt.Errorf("failed to begin transaction for migration %d: %w", mig.Version, err)
		}

		if _, err := tx.Exec(ctx, string(content)); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("migration %s failed: %w", mig.Filename, err)
		}

		recordQuery := `INSERT INTO schema_migrations (version, name) VALUES ($1, $2)`
		if _, err := tx.Exec(ctx, recordQuery, mig.Version, mig.Name); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("failed to record migration %d: %w", mig.Version, err)
		}

		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("failed to commit migration %d: %w", mig.Version, err)
		}
		slog.InfoContext(ctx, "migration applied successfully", "version", mig.Version, "name", mig.Name)
	}

	return nil
}

func parseMigrationFilename(filename string) (Migration, error) {
	base := filepath.Base(filename)
	parts := strings.SplitN(base, "_", 2)
	if len(parts) != 2 {
		return Migration{}, fmt.Errorf("invalid migration filename format: %s", filename)
	}

	version, err := strconv.Atoi(parts[0])
	if err != nil {
		return Migration{}, fmt.Errorf("invalid migration version in filename %s: %w", filename, err)
	}

	rest := parts[1]
	var direction, name string
	if strings.HasSuffix(rest, ".up.sql") {
		direction = "up"
		name = strings.TrimSuffix(rest, ".up.sql")
	} else if strings.HasSuffix(rest, ".down.sql") {
		direction = "down"
		name = strings.TrimSuffix(rest, ".down.sql")
	} else {
		return Migration{}, fmt.Errorf("unsupported migration direction extension: %s", filename)
	}

	return Migration{
		Version:   version,
		Name:      name,
		Direction: direction,
		Filename:  base,
	}, nil
}

package config

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config aggregates all application configuration parameters.
type Config struct {
	Env             string
	LogLevel        string
	ShutdownTimeout time.Duration

	HTTP     HTTPConfig
	Database DatabaseConfig
	Worker   WorkerConfig
}

// HTTPConfig defines settings for the HTTP API server.
type HTTPConfig struct {
	Port   int
	APIKey string
}

// DatabaseConfig holds PostgreSQL connection and pool parameters.
type DatabaseConfig struct {
	URL             string
	Host            string
	Port            int
	User            string
	Password        string
	Database        string
	SSLMode         string
	MaxConns        int32
	MinConns        int32
	MaxConnIdleTime time.Duration
	MaxConnLifetime time.Duration
}

// WorkerConfig defines queue worker execution properties.
type WorkerConfig struct {
	Concurrency       int
	PollInterval      time.Duration
	LeaseDuration     time.Duration
	HeartbeatInterval time.Duration
}

// ConnectionString returns the PostgreSQL DSN formatted connection string.
func (c *DatabaseConfig) ConnectionString() string {
	if strings.TrimSpace(c.URL) != "" {
		return strings.TrimSpace(c.URL)
	}
	return fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s?sslmode=%s",
		c.User, c.Password, c.Host, c.Port, c.Database, c.SSLMode,
	)
}

// Load reads configuration from environment variables and an optional .env file.
func Load() (*Config, error) {
	_ = loadDotEnv(".env")

	cfg := &Config{
		Env:             getEnvString("ENV", "development"),
		LogLevel:        getEnvString("LOG_LEVEL", "info"),
		ShutdownTimeout: time.Duration(getEnvInt("SHUTDOWN_TIMEOUT_SECONDS", 15)) * time.Second,
		HTTP: HTTPConfig{
			Port:   getEnvInt("PORT", 8080),
			APIKey: getEnvString("API_KEY", ""),
		},
		Database: DatabaseConfig{
			URL:             getEnvString("DATABASE_URL", ""),
			Host:            getEnvString("DB_HOST", "localhost"),
			Port:            getEnvInt("DB_PORT", 5432),
			User:            getEnvString("DB_USER", "taskflow"),
			Password:        getEnvString("DB_PASSWORD", "taskflow_secret"),
			Database:        getEnvString("DB_NAME", "taskflow"),
			SSLMode:         getEnvString("DB_SSLMODE", "disable"),
			MaxConns:        int32(getEnvInt("DB_MAX_CONNS", 25)),
			MinConns:        int32(getEnvInt("DB_MIN_CONNS", 5)),
			MaxConnIdleTime: time.Duration(getEnvInt("DB_MAX_CONN_IDLE_TIME_MINUTES", 15)) * time.Minute,
			MaxConnLifetime: time.Duration(getEnvInt("DB_MAX_CONN_LIFETIME_HOURS", 1)) * time.Hour,
		},
		Worker: WorkerConfig{
			Concurrency:       getEnvInt("WORKER_CONCURRENCY", 10),
			PollInterval:      time.Duration(getEnvInt("WORKER_POLL_INTERVAL_MS", 500)) * time.Millisecond,
			LeaseDuration:     time.Duration(getEnvInt("WORKER_LEASE_DURATION_SECONDS", 30)) * time.Second,
			HeartbeatInterval: time.Duration(getEnvInt("WORKER_HEARTBEAT_INTERVAL_SECONDS", 10)) * time.Second,
		},
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return cfg, nil
}

// Validate checks for logical and semantic correctness of configurations.
func (c *Config) Validate() error {
	if c.HTTP.Port <= 0 || c.HTTP.Port > 65535 {
		return fmt.Errorf("http port must be between 1 and 65535, got %d", c.HTTP.Port)
	}

	if strings.TrimSpace(c.Database.URL) == "" {
		if strings.TrimSpace(c.Database.Host) == "" {
			return fmt.Errorf("database host cannot be empty")
		}

		if c.Database.Port <= 0 || c.Database.Port > 65535 {
			return fmt.Errorf("database port must be between 1 and 65535, got %d", c.Database.Port)
		}

		if strings.TrimSpace(c.Database.User) == "" {
			return fmt.Errorf("database user cannot be empty")
		}

		if strings.TrimSpace(c.Database.Database) == "" {
			return fmt.Errorf("database name cannot be empty")
		}
	}

	if c.Database.MaxConns < c.Database.MinConns {
		return fmt.Errorf("database max conns (%d) cannot be less than min conns (%d)", c.Database.MaxConns, c.Database.MinConns)
	}

	if c.Worker.Concurrency <= 0 {
		return fmt.Errorf("worker concurrency must be greater than 0, got %d", c.Worker.Concurrency)
	}

	if c.Worker.LeaseDuration <= c.Worker.HeartbeatInterval {
		return fmt.Errorf("worker lease duration (%v) must be greater than heartbeat interval (%v)", c.Worker.LeaseDuration, c.Worker.HeartbeatInterval)
	}

	return nil
}

func getEnvString(key, defaultValue string) string {
	if val, ok := os.LookupEnv(key); ok && strings.TrimSpace(val) != "" {
		return strings.TrimSpace(val)
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if val, ok := os.LookupEnv(key); ok {
		if parsed, err := strconv.Atoi(strings.TrimSpace(val)); err == nil {
			return parsed
		}
	}
	return defaultValue
}

func loadDotEnv(filepath string) error {
	file, err := os.Open(filepath)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) == 2 {
			k := strings.TrimSpace(parts[0])
			v := strings.TrimSpace(parts[1])
			v = strings.Trim(v, `"'`)
			if _, exists := os.LookupEnv(k); !exists {
				_ = os.Setenv(k, v)
			}
		}
	}
	return scanner.Err()
}

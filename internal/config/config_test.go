package config

import (
	"os"
	"testing"
	"time"
)

func TestConfig_Defaults(t *testing.T) {
	os.Clearenv()

	cfg, err := Load()
	if err != nil {
		t.Fatalf("expected Load to succeed with defaults, got error: %v", err)
	}

	if cfg.HTTP.Port != 8080 {
		t.Errorf("expected default port 8080, got %d", cfg.HTTP.Port)
	}
	if cfg.Database.Host != "localhost" {
		t.Errorf("expected default DB host localhost, got %s", cfg.Database.Host)
	}
	if cfg.Database.Port != 5432 {
		t.Errorf("expected default DB port 5432, got %d", cfg.Database.Port)
	}
	if cfg.Worker.Concurrency != 10 {
		t.Errorf("expected default worker concurrency 10, got %d", cfg.Worker.Concurrency)
	}
	if cfg.Worker.LeaseDuration != 30*time.Second {
		t.Errorf("expected default lease duration 30s, got %v", cfg.Worker.LeaseDuration)
	}
}

func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name      string
		modify    func(cfg *Config)
		wantError bool
	}{
		{
			name:      "valid config",
			modify:    func(cfg *Config) {},
			wantError: false,
		},
		{
			name: "invalid port",
			modify: func(cfg *Config) {
				cfg.HTTP.Port = 0
			},
			wantError: true,
		},
		{
			name: "empty db host",
			modify: func(cfg *Config) {
				cfg.Database.Host = ""
			},
			wantError: true,
		},
		{
			name: "invalid worker concurrency",
			modify: func(cfg *Config) {
				cfg.Worker.Concurrency = 0
			},
			wantError: true,
		},
		{
			name: "lease duration less than heartbeat",
			modify: func(cfg *Config) {
				cfg.Worker.LeaseDuration = 5 * time.Second
				cfg.Worker.HeartbeatInterval = 10 * time.Second
			},
			wantError: true,
		},
		{
			name: "max conns less than min conns",
			modify: func(cfg *Config) {
				cfg.Database.MaxConns = 2
				cfg.Database.MinConns = 10
			},
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &Config{
				Env:             "test",
				LogLevel:        "debug",
				ShutdownTimeout: 5 * time.Second,
				HTTP:            HTTPConfig{Port: 8080},
				Database: DatabaseConfig{
					Host:     "localhost",
					Port:     5432,
					User:     "postgres",
					Password: "password",
					Database: "taskflow",
					MaxConns: 10,
					MinConns: 2,
				},
				Worker: WorkerConfig{
					Concurrency:       5,
					PollInterval:      100 * time.Millisecond,
					LeaseDuration:     30 * time.Second,
					HeartbeatInterval: 10 * time.Second,
				},
			}
			tt.modify(cfg)
			err := cfg.Validate()
			if (err != nil) != tt.wantError {
				t.Errorf("Validate() error = %v, wantError = %v", err, tt.wantError)
			}
		})
	}
}

func TestDatabaseConfig_ConnectionString(t *testing.T) {
	dbCfg := DatabaseConfig{
		Host:     "dbhost",
		Port:     5432,
		User:     "user",
		Password: "secret",
		Database: "mydb",
		SSLMode:  "disable",
	}

	expected := "postgres://user:secret@dbhost:5432/mydb?sslmode=disable"
	actual := dbCfg.ConnectionString()
	if actual != expected {
		t.Errorf("expected %s, got %s", expected, actual)
	}
}

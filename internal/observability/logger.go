package observability

import (
	"io"
	"log/slog"
	"os"
	"strings"
)

// NewLogger creates and configures a structured slog.Logger instance based on environment and level.
func NewLogger(env, levelStr string, out io.Writer) *slog.Logger {
	if out == nil {
		out = os.Stdout
	}

	var level slog.Level
	switch strings.ToLower(strings.TrimSpace(levelStr)) {
	case "debug":
		level = slog.LevelDebug
	case "warn", "warning":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{
		Level: level,
	}

	var handler slog.Handler
	if strings.ToLower(env) == "production" || strings.ToLower(env) == "staging" {
		handler = slog.NewJSONHandler(out, opts)
	} else {
		handler = slog.NewTextHandler(out, opts)
	}

	logger := slog.New(handler)
	slog.SetDefault(logger)
	return logger
}

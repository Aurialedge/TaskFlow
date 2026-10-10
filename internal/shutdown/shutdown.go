package shutdown

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// GracefulContext returns a context that cancels on os.Interrupt, SIGTERM, or SIGINT.
func GracefulContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
}

// Drain executes a cleanup operation with a timeout limit.
func Drain(timeout time.Duration, cleanup func(ctx context.Context) error) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return cleanup(ctx)
}

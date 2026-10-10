package shutdown

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestDrain_Success(t *testing.T) {
	executed := false
	err := Drain(1*time.Second, func(ctx context.Context) error {
		executed = true
		return nil
	})

	if err != nil {
		t.Fatalf("expected nil error, got: %v", err)
	}
	if !executed {
		t.Fatal("expected cleanup function to execute")
	}
}

func TestDrain_Error(t *testing.T) {
	customErr := errors.New("cleanup failed")
	err := Drain(1*time.Second, func(ctx context.Context) error {
		return customErr
	})

	if !errors.Is(err, customErr) {
		t.Fatalf("expected custom error, got: %v", err)
	}
}

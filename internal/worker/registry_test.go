package worker

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func TestRegistry(t *testing.T) {
	r := NewDefaultRegistry()

	t.Run("default handlers are registered", func(t *testing.T) {
		for _, name := range []string{"echo", "sleep", "report.generate", "test.fail"} {
			if _, ok := r.Get(name); !ok {
				t.Errorf("expected '%s' to be registered", name)
			}
		}
	})

	t.Run("duplicate registration fails", func(t *testing.T) {
		err := r.Register("echo", EchoHandler)
		if err == nil {
			t.Fatal("expected error on duplicate registration")
		}
	})

	t.Run("empty type fails", func(t *testing.T) {
		err := r.Register("", EchoHandler)
		if err == nil {
			t.Fatal("expected error on empty type registration")
		}
	})

	t.Run("nil handler fails", func(t *testing.T) {
		err := r.Register("custom.nil", nil)
		if err == nil {
			t.Fatal("expected error on nil handler registration")
		}
	})
}

func TestBuiltinHandlers(t *testing.T) {
	ctx := context.Background()

	t.Run("echo handler", func(t *testing.T) {
		payload := json.RawMessage(`{"hello":"world"}`)
		res, err := EchoHandler(ctx, payload)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if string(res) != string(payload) {
			t.Errorf("expected %s, got %s", payload, res)
		}
	})

	t.Run("sleep handler completes", func(t *testing.T) {
		payload := json.RawMessage(`{"duration_ms":10}`)
		res, err := SleepHandler(ctx, payload)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !strings.Contains(string(res), "completed") {
			t.Errorf("expected completed status, got %s", res)
		}
	})

	t.Run("sleep handler respects cancellation", func(t *testing.T) {
		cancelCtx, cancel := context.WithCancel(context.Background())
		cancel() // cancel immediately

		payload := json.RawMessage(`{"duration_ms":500}`)
		_, err := SleepHandler(cancelCtx, payload)
		if err == nil {
			t.Fatal("expected context cancellation error")
		}
	})

	t.Run("report generate handler", func(t *testing.T) {
		payload := json.RawMessage(`{"report_type":"monthly","user_id":"u-123"}`)
		res, err := ReportGenerateHandler(ctx, payload)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !strings.Contains(string(res), "records_processed") {
			t.Errorf("expected records_processed in report, got %s", res)
		}
	})

	t.Run("fail handler returns error", func(t *testing.T) {
		payload := json.RawMessage(`{"message":"boom"}`)
		_, err := FailHandler(ctx, payload)
		if err == nil {
			t.Fatal("expected failure error from FailHandler")
		}
		if !strings.Contains(err.Error(), "boom") {
			t.Errorf("expected error to contain 'boom', got %v", err)
		}
	})
}

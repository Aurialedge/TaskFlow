package observability

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func TestNewLogger_JSON(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := NewLogger("production", "info", buf)

	logger.InfoContext(context.Background(), "test message", "key", "value")

	output := strings.TrimSpace(buf.String())
	if output == "" {
		t.Fatal("expected non-empty log output")
	}

	var parsed map[string]interface{}
	if err := json.Unmarshal([]byte(output), &parsed); err != nil {
		t.Fatalf("expected valid JSON, got: %v (raw: %s)", err, output)
	}

	if parsed["msg"] != "test message" {
		t.Errorf("expected msg 'test message', got '%v'", parsed["msg"])
	}
	if parsed["key"] != "value" {
		t.Errorf("expected key 'value', got '%v'", parsed["key"])
	}
}

func TestNewLogger_Text(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := NewLogger("development", "debug", buf)

	logger.DebugContext(context.Background(), "debug event", "attempt", 1)

	output := buf.String()
	if !strings.Contains(output, "debug event") || !strings.Contains(output, "attempt=1") {
		t.Errorf("expected debug log text output, got: %s", output)
	}
}

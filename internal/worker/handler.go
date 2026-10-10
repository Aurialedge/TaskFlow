package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

var (
	ErrUnknownJobType  = errors.New("unknown job handler type")
	ErrHandlerTimeout  = errors.New("handler execution timed out")
	ErrHandlerPanicked = errors.New("handler execution panicked")
)

// HandlerFunc defines the standard signature for a background job executor.
type HandlerFunc func(ctx context.Context, payload json.RawMessage) (json.RawMessage, error)

// EchoHandler simply echoes the payload back as the result.
func EchoHandler(ctx context.Context, payload json.RawMessage) (json.RawMessage, error) {
	if len(payload) == 0 {
		return json.RawMessage("{}"), nil
	}
	return payload, nil
}

// SleepPayload defines the parameter for the SleepHandler.
type SleepPayload struct {
	DurationMs int `json:"duration_ms"`
}

// SleepHandler pauses execution for the duration requested, respecting context cancellation.
func SleepHandler(ctx context.Context, payload json.RawMessage) (json.RawMessage, error) {
	var p SleepPayload
	if len(payload) > 0 {
		_ = json.Unmarshal(payload, &p)
	}
	if p.DurationMs <= 0 {
		p.DurationMs = 50
	}

	timer := time.NewTimer(time.Duration(p.DurationMs) * time.Millisecond)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
		res, _ := json.Marshal(map[string]interface{}{
			"status":      "completed",
			"slept_ms":    p.DurationMs,
			"finished_at": time.Now().UTC(),
		})
		return res, nil
	}
}

// ReportGeneratePayload defines parameters for report generation.
type ReportGeneratePayload struct {
	ReportType string `json:"report_type"`
	UserID     string `json:"user_id"`
}

// ReportGenerateHandler simulates generating an analytics report.
func ReportGenerateHandler(ctx context.Context, payload json.RawMessage) (json.RawMessage, error) {
	var p ReportGeneratePayload
	if len(payload) > 0 {
		_ = json.Unmarshal(payload, &p)
	}
	if p.ReportType == "" {
		p.ReportType = "standard_summary"
	}

	res, _ := json.Marshal(map[string]interface{}{
		"status":            "generated",
		"report_type":       p.ReportType,
		"user_id":           p.UserID,
		"records_processed": 142,
		"generated_at":      time.Now().UTC(),
	})
	return res, nil
}

// FailPayload allows configuring error details.
type FailPayload struct {
	Message string `json:"message"`
}

// FailHandler intentionally produces an error for testing failure workflows.
func FailHandler(ctx context.Context, payload json.RawMessage) (json.RawMessage, error) {
	var p FailPayload
	if len(payload) > 0 {
		_ = json.Unmarshal(payload, &p)
	}
	msg := "deliberate execution failure"
	if p.Message != "" {
		msg = p.Message
	}
	return nil, fmt.Errorf("task failed intentionally: %s", msg)
}

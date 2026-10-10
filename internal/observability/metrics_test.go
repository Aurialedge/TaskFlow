package observability

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func TestMetrics_Recording(t *testing.T) {
	registry := prometheus.NewRegistry()
	m := NewMetrics(registry)

	m.IncJobsSubmitted("emails", "email.send")
	m.RecordJobCompleted("emails", "email.send", "succeeded", 25*time.Millisecond)
	m.SetQueueDepth("emails", "pending", 5)

	metricFamilies, err := registry.Gather()
	if err != nil {
		t.Fatalf("failed to gather prometheus metrics: %v", err)
	}

	foundSubmitted := false
	foundCompleted := false
	foundDepth := false

	for _, mf := range metricFamilies {
		switch *mf.Name {
		case "taskflow_jobs_submitted_total":
			foundSubmitted = true
		case "taskflow_jobs_completed_total":
			foundCompleted = true
		case "taskflow_queue_depth":
			foundDepth = true
		}
	}

	if !foundSubmitted {
		t.Error("expected taskflow_jobs_submitted_total metric")
	}
	if !foundCompleted {
		t.Error("expected taskflow_jobs_completed_total metric")
	}
	if !foundDepth {
		t.Error("expected taskflow_queue_depth metric")
	}
}

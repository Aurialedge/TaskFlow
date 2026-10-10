package observability

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics holds the registered Prometheus metrics for TaskFlow.
type Metrics struct {
	JobsSubmitted *prometheus.CounterVec
	JobsCompleted *prometheus.CounterVec
	JobDuration   *prometheus.HistogramVec
	ActiveWorkers prometheus.Gauge
	QueueDepth    *prometheus.GaugeVec
}

var globalMetrics *Metrics

func init() {
	globalMetrics = NewMetrics(prometheus.DefaultRegisterer)
}

// GetMetrics returns the singleton Prometheus metrics instance.
func GetMetrics() *Metrics {
	return globalMetrics
}

// NewMetrics initializes and registers TaskFlow Prometheus metrics.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	factory := promauto.With(reg)

	return &Metrics{
		JobsSubmitted: factory.NewCounterVec(
			prometheus.CounterOpts{
				Name: "taskflow_jobs_submitted_total",
				Help: "Total count of jobs submitted to TaskFlow",
			},
			[]string{"queue", "type"},
		),
		JobsCompleted: factory.NewCounterVec(
			prometheus.CounterOpts{
				Name: "taskflow_jobs_completed_total",
				Help: "Total count of jobs processed by workers and their final status",
			},
			[]string{"queue", "type", "status"},
		),
		JobDuration: factory.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "taskflow_job_duration_seconds",
				Help:    "Execution duration of background jobs in seconds",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"queue", "type"},
		),
		ActiveWorkers: factory.NewGauge(
			prometheus.GaugeOpts{
				Name: "taskflow_active_workers",
				Help: "Current count of active worker threads processing jobs",
			},
		),
		QueueDepth: factory.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "taskflow_queue_depth",
				Help: "Current count of jobs in each queue partitioned by state",
			},
			[]string{"queue", "state"},
		),
	}
}

// IncJobsSubmitted records a job submission metric event.
func (m *Metrics) IncJobsSubmitted(queue, jobType string) {
	if m != nil && m.JobsSubmitted != nil {
		m.JobsSubmitted.WithLabelValues(queue, jobType).Inc()
	}
}

// RecordJobCompleted records a completed job event and execution duration.
func (m *Metrics) RecordJobCompleted(queue, jobType, status string, duration time.Duration) {
	if m != nil {
		if m.JobsCompleted != nil {
			m.JobsCompleted.WithLabelValues(queue, jobType, status).Inc()
		}
		if m.JobDuration != nil {
			m.JobDuration.WithLabelValues(queue, jobType).Observe(duration.Seconds())
		}
	}
}

// SetQueueDepth updates the gauge metric for a given queue and state.
func (m *Metrics) SetQueueDepth(queue, state string, count float64) {
	if m != nil && m.QueueDepth != nil {
		m.QueueDepth.WithLabelValues(queue, state).Set(count)
	}
}

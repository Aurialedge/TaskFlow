package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/Aurialedge/TaskFlow/internal/repository"
)

// Dispatcher polls for eligible jobs and dispatches them into the worker pool.
type Dispatcher struct {
	repo          repository.JobRepository
	pool          *Pool
	queue         string
	pollInterval  time.Duration
	leaseDuration time.Duration
	workerID      string
	stopCh        chan struct{}
}

// NewDispatcher creates a new job Dispatcher.
func NewDispatcher(
	repo repository.JobRepository,
	pool *Pool,
	queue string,
	pollInterval time.Duration,
	leaseDuration time.Duration,
	workerID string,
) *Dispatcher {
	if pollInterval <= 0 {
		pollInterval = 500 * time.Millisecond
	}
	if leaseDuration <= 0 {
		leaseDuration = 30 * time.Second
	}

	return &Dispatcher{
		repo:          repo,
		pool:          pool,
		queue:         queue,
		pollInterval:  pollInterval,
		leaseDuration: leaseDuration,
		workerID:      workerID,
		stopCh:        make(chan struct{}),
	}
}

// Start begins the dispatching loop in the calling goroutine until ctx is cancelled or Stop is called.
func (d *Dispatcher) Start(ctx context.Context) {
	slog.Info("dispatcher started",
		"queue", d.queue,
		"poll_interval", d.pollInterval,
		"lease_duration", d.leaseDuration,
		"worker_id", d.workerID,
	)

	ticker := time.NewTicker(d.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("dispatcher stopping due to context cancellation")
			return
		case <-d.stopCh:
			slog.Info("dispatcher stopping due to Stop request")
			return
		case <-ticker.C:
			d.dispatchBatch(ctx)
		}
	}
}

// Stop signals the dispatcher to cease polling.
func (d *Dispatcher) Stop() {
	select {
	case <-d.stopCh:
		// already stopped
	default:
		close(d.stopCh)
	}
}

func (d *Dispatcher) dispatchBatch(ctx context.Context) {
	capacity := d.pool.AvailableCapacity()
	if capacity <= 0 {
		return
	}

	claimCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	claimedJobs, err := d.repo.ClaimJobs(claimCtx, d.queue, capacity, d.workerID, d.leaseDuration)
	if err != nil {
		slog.Error("error claiming jobs from database", "error", err)
		return
	}

	if len(claimedJobs) == 0 {
		return
	}

	slog.Info("claimed jobs for execution", "count", len(claimedJobs))

	for _, job := range claimedJobs {
		if !d.pool.Submit(job) {
			slog.Warn("worker pool could not accept job; will be picked up on next lease cycle", "job_id", job.ID.String())
		}
	}
}

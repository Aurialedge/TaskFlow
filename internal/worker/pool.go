package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/Aurialedge/TaskFlow/internal/domain"
	"github.com/Aurialedge/TaskFlow/internal/observability"
	"github.com/Aurialedge/TaskFlow/internal/repository"
	"github.com/Aurialedge/TaskFlow/internal/retry"
)

// Pool executes claimed jobs using a bounded pool of goroutine workers with lease heartbeats and retries.
type Pool struct {
	concurrency       int
	workerID          string
	repo              repository.JobRepository
	registry          *Registry
	backoff           *retry.BackoffManager
	leaseDuration     time.Duration
	heartbeatInterval time.Duration
	jobQueue          chan *domain.Job
	wg                sync.WaitGroup
	ctx               context.Context
	cancel            context.CancelFunc
	closed            bool
	mu                sync.Mutex
}

// PoolConfig specifies parameters for worker pool construction.
type PoolConfig struct {
	Concurrency       int
	WorkerID          string
	LeaseDuration     time.Duration
	HeartbeatInterval time.Duration
}

// NewPool initializes a new bounded worker pool.
func NewPool(concurrency int, workerID string, repo repository.JobRepository, registry *Registry) *Pool {
	return NewPoolWithConfig(repo, registry, PoolConfig{
		Concurrency:       concurrency,
		WorkerID:          workerID,
		LeaseDuration:     30 * time.Second,
		HeartbeatInterval: 10 * time.Second,
	})
}

// NewPoolWithConfig initializes a new bounded worker pool with full configuration.
func NewPoolWithConfig(repo repository.JobRepository, registry *Registry, cfg PoolConfig) *Pool {
	concurrency := cfg.Concurrency
	if concurrency <= 0 {
		concurrency = 5
	}
	workerID := cfg.WorkerID
	if workerID == "" {
		workerID = "worker-" + fmt.Sprintf("%d", time.Now().UnixNano())
	}
	leaseDuration := cfg.LeaseDuration
	if leaseDuration <= 0 {
		leaseDuration = 30 * time.Second
	}
	heartbeatInterval := cfg.HeartbeatInterval
	if heartbeatInterval <= 0 || heartbeatInterval >= leaseDuration {
		heartbeatInterval = leaseDuration / 3
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &Pool{
		concurrency:       concurrency,
		workerID:          workerID,
		repo:              repo,
		registry:          registry,
		backoff:           retry.NewBackoffManager(1*time.Second, 60*time.Second),
		leaseDuration:     leaseDuration,
		heartbeatInterval: heartbeatInterval,
		jobQueue:          make(chan *domain.Job, concurrency*2),
		ctx:               ctx,
		cancel:            cancel,
	}
}

// Start launches the worker goroutines.
func (p *Pool) Start() {
	for i := 0; i < p.concurrency; i++ {
		p.wg.Add(1)
		go p.workerRoutine(i)
	}
	slog.Info("worker pool started", "concurrency", p.concurrency, "worker_id", p.workerID)
}

// Submit enqueues a job to be processed by an available worker.
func (p *Pool) Submit(job *domain.Job) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return false
	}

	select {
	case p.jobQueue <- job:
		return true
	default:
		return false
	}
}

// AvailableCapacity returns how many more jobs can currently be queued without blocking.
func (p *Pool) AvailableCapacity() int {
	return cap(p.jobQueue) - len(p.jobQueue)
}

// Stop gracefully shuts down the worker pool, waiting for in-flight jobs to complete.
func (p *Pool) Stop() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	close(p.jobQueue)
	p.mu.Unlock()

	p.wg.Wait()
	p.cancel()
	slog.Info("worker pool cleanly stopped", "worker_id", p.workerID)
}

func (p *Pool) workerRoutine(workerIndex int) {
	defer p.wg.Done()

	for job := range p.jobQueue {
		p.processJob(job, workerIndex)
	}
}

func (p *Pool) processJob(job *domain.Job, workerIndex int) {
	startedAt := time.Now().UTC()
	slog.Info("worker executing job",
		"worker_index", workerIndex,
		"worker_id", p.workerID,
		"job_id", job.ID.String(),
		"job_type", job.Type,
		"attempt", job.AttemptCount,
	)

	timeout := time.Duration(job.TimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 60 * time.Second
	}

	execCtx, cancel := context.WithTimeout(p.ctx, timeout)
	defer cancel()

	// Launch background heartbeat renewal during job execution
	heartbeatDone := make(chan struct{})
	if job.LeaseToken != nil {
		go p.runHeartbeat(execCtx, job.ID, *job.LeaseToken, job.LeaseGeneration, heartbeatDone)
	} else {
		close(heartbeatDone)
	}

	result, execErr := p.executeHandlerSafe(execCtx, job.Type, job.Payload)
	close(heartbeatDone)

	finishedAt := time.Now().UTC()
	duration := finishedAt.Sub(startedAt)
	durationMs := duration.Milliseconds()

	// Determine backoff and error classification
	var backoff time.Duration
	isPermanent := false
	statusStr := string(domain.StateSucceeded)
	if execErr != nil {
		isPermanent = retry.IsPermanent(execErr)
		backoff = p.backoff.Calculate(job.AttemptCount)
		if isPermanent || job.AttemptCount >= job.MaxAttempts {
			statusStr = string(domain.StateDead)
		} else {
			statusStr = string(domain.StateFailed)
		}
		slog.Error("job execution failed",
			"job_id", job.ID.String(),
			"job_type", job.Type,
			"attempt", job.AttemptCount,
			"error", execErr,
			"is_permanent", isPermanent,
			"retry_backoff", backoff,
			"duration_ms", durationMs,
		)
	} else {
		slog.Info("job execution succeeded",
			"job_id", job.ID.String(),
			"job_type", job.Type,
			"attempt", job.AttemptCount,
			"duration_ms", durationMs,
		)
	}

	observability.GetMetrics().RecordJobCompleted(job.Queue, job.Type, statusStr, duration)

	// Persist outcome atomically with retry / dead-letter transitions
	if job.LeaseToken != nil {
		persistedState, outcomeErr := p.repo.RecordOutcome(
			p.ctx,
			job.ID,
			*job.LeaseToken,
			job.LeaseGeneration,
			result,
			execErr,
			backoff,
			isPermanent,
		)
		if outcomeErr != nil {
			slog.Error("failed to record job outcome", "job_id", job.ID.String(), "error", outcomeErr)
		} else {
			slog.Info("job outcome recorded", "job_id", job.ID.String(), "resulting_state", persistedState)
		}
	}

	var errMsg *string
	attemptState := domain.StateSucceeded
	if execErr != nil {
		msg := execErr.Error()
		errMsg = &msg
		attemptState = domain.StateFailed
	}

	_ = p.repo.RecordAttempt(p.ctx, &domain.JobAttempt{
		JobID:         job.ID,
		AttemptNumber: job.AttemptCount,
		WorkerID:      p.workerID,
		State:         attemptState,
		ErrorMessage:  errMsg,
		StartedAt:     startedAt,
		FinishedAt:    &finishedAt,
		DurationMs:    &durationMs,
	})
}

func (p *Pool) runHeartbeat(ctx context.Context, jobID, leaseToken uuid.UUID, leaseGen int64, done chan struct{}) {
	ticker := time.NewTicker(p.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := p.repo.RenewLease(ctx, jobID, leaseToken, leaseGen, p.leaseDuration); err != nil {
				slog.Warn("heartbeat lease renewal failed", "job_id", jobID.String(), "error", err)
				return
			}
			slog.Debug("heartbeat renewed lease", "job_id", jobID.String())
		}
	}
}

func (p *Pool) executeHandlerSafe(ctx context.Context, jobType string, payload json.RawMessage) (result json.RawMessage, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%w: %v", ErrHandlerPanicked, r)
		}
	}()

	fn, exists := p.registry.Get(jobType)
	if !exists {
		return nil, fmt.Errorf("%w: %s", ErrUnknownJobType, jobType)
	}

	return fn(ctx, payload)
}

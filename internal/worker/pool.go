package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/Aurialedge/TaskFlow/internal/domain"
	"github.com/Aurialedge/TaskFlow/internal/repository"
)

// Pool executes claimed jobs using a bounded pool of goroutine workers.
type Pool struct {
	concurrency int
	workerID    string
	repo        repository.JobRepository
	registry    *Registry
	jobQueue    chan *domain.Job
	wg          sync.WaitGroup
	ctx         context.Context
	cancel      context.CancelFunc
	closed      bool
	mu          sync.Mutex
}

// NewPool initializes a new bounded worker pool.
func NewPool(concurrency int, workerID string, repo repository.JobRepository, registry *Registry) *Pool {
	if concurrency <= 0 {
		concurrency = 5
	}
	if workerID == "" {
		workerID = "worker-" + fmt.Sprintf("%d", time.Now().UnixNano())
	}

	ctx, cancel := context.WithCancel(context.Background())

	p := &Pool{
		concurrency: concurrency,
		workerID:    workerID,
		repo:        repo,
		registry:    registry,
		jobQueue:    make(chan *domain.Job, concurrency*2),
		ctx:         ctx,
		cancel:      cancel,
	}

	return p
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

	result, execErr := p.executeHandlerSafe(execCtx, job.Type, job.Payload)
	finishedAt := time.Now().UTC()
	durationMs := finishedAt.Sub(startedAt).Milliseconds()

	if execErr != nil {
		slog.Error("job execution failed",
			"job_id", job.ID.String(),
			"job_type", job.Type,
			"attempt", job.AttemptCount,
			"error", execErr,
			"duration_ms", durationMs,
		)

		errMsg := execErr.Error()
		if job.LeaseToken != nil {
			_ = p.repo.RecordFailure(p.ctx, job.ID, *job.LeaseToken, errMsg)
		}

		_ = p.repo.RecordAttempt(p.ctx, &domain.JobAttempt{
			JobID:         job.ID,
			AttemptNumber: job.AttemptCount,
			WorkerID:      p.workerID,
			State:         domain.StateFailed,
			ErrorMessage:  &errMsg,
			StartedAt:     startedAt,
			FinishedAt:    &finishedAt,
			DurationMs:    &durationMs,
		})
		return
	}

	slog.Info("job execution succeeded",
		"job_id", job.ID.String(),
		"job_type", job.Type,
		"attempt", job.AttemptCount,
		"duration_ms", durationMs,
	)

	if job.LeaseToken != nil {
		_ = p.repo.RecordSuccess(p.ctx, job.ID, *job.LeaseToken, result)
	}

	_ = p.repo.RecordAttempt(p.ctx, &domain.JobAttempt{
		JobID:         job.ID,
		AttemptNumber: job.AttemptCount,
		WorkerID:      p.workerID,
		State:         domain.StateSucceeded,
		StartedAt:     startedAt,
		FinishedAt:    &finishedAt,
		DurationMs:    &durationMs,
	})
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

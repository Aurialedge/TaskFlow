package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/Aurialedge/TaskFlow/internal/domain"
	"github.com/Aurialedge/TaskFlow/internal/observability"
	"github.com/Aurialedge/TaskFlow/internal/repository"
)

const (
	DefaultQueue          = "default"
	DefaultMaxAttempts    = 3
	DefaultTimeoutSeconds = 60
	MaxPayloadBytes       = 256 * 1024 // 256 KB
)

// JobService provides business logic for job submission, validation, and retrieval.
type JobService struct {
	repo repository.JobRepository
}

// NewJobService creates a new JobService instance.
func NewJobService(repo repository.JobRepository) *JobService {
	return &JobService{repo: repo}
}

// SubmitJob validates input and creates or retrieves an idempotent job.
func (s *JobService) SubmitJob(ctx context.Context, input domain.CreateJobInput) (*domain.Job, bool, error) {
	if strings.TrimSpace(input.Type) == "" {
		return nil, false, fmt.Errorf("%w: handler type cannot be empty", domain.ErrInvalidJobInput)
	}

	queue := strings.TrimSpace(input.Queue)
	if queue == "" {
		queue = DefaultQueue
	}

	maxAttempts := input.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = DefaultMaxAttempts
	}
	if maxAttempts > 100 {
		return nil, false, fmt.Errorf("%w: max_attempts cannot exceed 100", domain.ErrInvalidJobInput)
	}

	timeoutSecs := input.TimeoutSeconds
	if timeoutSecs <= 0 {
		timeoutSecs = DefaultTimeoutSeconds
	}
	if timeoutSecs > 86400 {
		return nil, false, fmt.Errorf("%w: timeout_seconds cannot exceed 86400 (24 hours)", domain.ErrInvalidJobInput)
	}

	payload := input.Payload
	if len(payload) == 0 {
		payload = json.RawMessage("{}")
	} else {
		if len(payload) > MaxPayloadBytes {
			return nil, false, domain.ErrPayloadTooLarge
		}
		if !json.Valid(payload) {
			return nil, false, fmt.Errorf("%w: payload must be valid JSON", domain.ErrInvalidJobInput)
		}
	}

	now := time.Now().UTC()
	runAt := now
	state := domain.StatePending

	if input.RunAt != nil && !input.RunAt.IsZero() {
		scheduledTime := input.RunAt.UTC()
		if scheduledTime.After(now.Add(1 * time.Second)) {
			runAt = scheduledTime
			state = domain.StateScheduled
		}
	}

	scope := strings.TrimSpace(input.IdempotencyScope)
	if scope == "" {
		scope = "default"
	}

	var idempotencyKey *string
	if input.IdempotencyKey != nil {
		k := strings.TrimSpace(*input.IdempotencyKey)
		if k != "" {
			idempotencyKey = &k
		}
	}

	job := &domain.Job{
		ID:               uuid.New(),
		Queue:            queue,
		Type:             strings.TrimSpace(input.Type),
		Payload:          payload,
		State:            state,
		Priority:         input.Priority,
		MaxAttempts:      maxAttempts,
		AttemptCount:     0,
		RunAt:            runAt,
		TimeoutSeconds:   timeoutSecs,
		IdempotencyKey:   idempotencyKey,
		IdempotencyScope: scope,
		CreatedAt:        now,
		UpdatedAt:        now,
	}

	createdJob, created, err := s.repo.CreateJob(ctx, job)
	if err == nil && created {
		observability.GetMetrics().IncJobsSubmitted(createdJob.Queue, createdJob.Type)
	}
	return createdJob, created, err
}

// GetJob retrieves a job by UUID and optionally includes execution attempts.
func (s *JobService) GetJob(ctx context.Context, id uuid.UUID) (*domain.Job, []*domain.JobAttempt, error) {
	job, err := s.repo.GetJobByID(ctx, id)
	if err != nil {
		return nil, nil, err
	}

	attempts, err := s.repo.GetAttempts(ctx, id)
	if err != nil && !errors.Is(err, domain.ErrJobNotFound) {
		return nil, nil, fmt.Errorf("failed to load attempts: %w", err)
	}

	return job, attempts, nil
}

// ListJobs retrieves a filtered list of jobs and total count.
func (s *JobService) ListJobs(ctx context.Context, filter domain.ListJobsFilter) ([]*domain.Job, int64, error) {
	if filter.State != "" && !domain.IsValidState(filter.State) {
		return nil, 0, fmt.Errorf("%w: unrecognized state '%s'", domain.ErrInvalidState, filter.State)
	}
	return s.repo.ListJobs(ctx, filter)
}

// CancelJob cancels a job if it has not already reached a terminal state.
func (s *JobService) CancelJob(ctx context.Context, id uuid.UUID) (*domain.Job, error) {
	return s.repo.CancelJob(ctx, id)
}

// ManualRetryJob re-queues an exhausted or failed job for re-execution.
func (s *JobService) ManualRetryJob(ctx context.Context, id uuid.UUID) (*domain.Job, error) {
	return s.repo.ManualRetryJob(ctx, id)
}

// GetQueueStats returns operational depths and counts for the requested queue.
func (s *JobService) GetQueueStats(ctx context.Context, queue string) (*domain.QueueStats, error) {
	q := strings.TrimSpace(queue)
	if q == "" {
		q = DefaultQueue
	}
	return s.repo.QueueStats(ctx, q)
}

package repository

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/Aurialedge/TaskFlow/internal/domain"
)

// JobRepository defines persistence operations for jobs and attempts.
type JobRepository interface {
	// CreateJob inserts a new job or returns an existing one if an idempotency collision occurs.
	// Returns (job, created, error) where created is true if a new record was inserted.
	CreateJob(ctx context.Context, job *domain.Job) (*domain.Job, bool, error)

	// GetJobByID retrieves a job by its unique identifier.
	GetJobByID(ctx context.Context, id uuid.UUID) (*domain.Job, error)

	// GetJobByIdempotency retrieves a job matching a given scope and idempotency key.
	GetJobByIdempotency(ctx context.Context, scope, key string) (*domain.Job, error)

	// ListJobs returns jobs matching filters along with the total count.
	ListJobs(ctx context.Context, filter domain.ListJobsFilter) ([]*domain.Job, int64, error)

	// GetAttempts retrieves all execution attempts recorded for a job.
	GetAttempts(ctx context.Context, jobID uuid.UUID) ([]*domain.JobAttempt, error)

	// ClaimJobs atomically finds eligible jobs and leases them to workerID using SELECT FOR UPDATE SKIP LOCKED.
	ClaimJobs(ctx context.Context, queue string, limit int, workerID string, leaseDuration time.Duration) ([]*domain.Job, error)

	// RecordSuccess marks a job as succeeded and records its output result.
	RecordSuccess(ctx context.Context, jobID uuid.UUID, leaseToken uuid.UUID, result []byte) error

	// RecordFailure marks a job as failed and records its error message.
	RecordFailure(ctx context.Context, jobID uuid.UUID, leaseToken uuid.UUID, errMsg string) error

	// RecordAttempt records an execution attempt in the job_attempts table.
	RecordAttempt(ctx context.Context, attempt *domain.JobAttempt) error

	// RecordOutcome durably records success or failure with retry backoff and dead-lettering.
	// Returns the resulting domain.State of the job.
	RecordOutcome(ctx context.Context, jobID uuid.UUID, leaseToken uuid.UUID, result []byte, execErr error, backoff time.Duration, isPermanent bool) (domain.State, error)

	// RenewLease extends the expiration time of a running job's lease.
	RenewLease(ctx context.Context, jobID uuid.UUID, leaseToken uuid.UUID, duration time.Duration) error

	// RecoverAbandonedJobs finds jobs stuck in running with expired leases and either re-queues them or dead-letters them.
	RecoverAbandonedJobs(ctx context.Context, defaultLeaseDuration time.Duration) (int64, error)

	// CancelJob cancels a pending or scheduled job, or marks a running job as cancelled.
	CancelJob(ctx context.Context, jobID uuid.UUID) (*domain.Job, error)

	// ManualRetryJob re-arms a terminal failed or dead job back into pending state.
	ManualRetryJob(ctx context.Context, jobID uuid.UUID) (*domain.Job, error)

	// QueueStats calculates depth and status counts for a queue.
	QueueStats(ctx context.Context, queue string) (*domain.QueueStats, error)
}

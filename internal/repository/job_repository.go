package repository

import (
	"context"

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
}

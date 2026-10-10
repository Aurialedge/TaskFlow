package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Aurialedge/TaskFlow/internal/domain"
)

// JobRepository implements repository.JobRepository for PostgreSQL.
type JobRepository struct {
	pool *pgxpool.Pool
}

// NewJobRepository constructs a new PostgreSQL JobRepository.
func NewJobRepository(pool *pgxpool.Pool) *JobRepository {
	return &JobRepository{pool: pool}
}

// CreateJob persists a job record or retrieves the existing job on idempotency collision.
func (r *JobRepository) CreateJob(ctx context.Context, job *domain.Job) (*domain.Job, bool, error) {
	if job.ID == uuid.Nil {
		job.ID = uuid.New()
	}

	if job.IdempotencyKey != nil && *job.IdempotencyKey != "" {
		query := `
			INSERT INTO jobs (
				id, queue, type, payload, state, priority, max_attempts, attempt_count,
				run_at, timeout_seconds, idempotency_key, idempotency_scope, created_at, updated_at
			) VALUES (
				$1, $2, $3, $4, $5, $6, $7, $8,
				$9, $10, $11, $12, $13, $14
			)
			ON CONFLICT (idempotency_scope, idempotency_key) WHERE idempotency_key IS NOT NULL
			DO NOTHING
			RETURNING id, queue, type, payload, state, priority, max_attempts, attempt_count,
			          run_at, timeout_seconds, lease_token, lease_expires_at, lease_generation,
			          idempotency_key, idempotency_scope, result, last_error, created_at, updated_at;
		`

		row := r.pool.QueryRow(ctx, query,
			job.ID, job.Queue, job.Type, job.Payload, job.State, job.Priority, job.MaxAttempts, job.AttemptCount,
			job.RunAt, job.TimeoutSeconds, job.IdempotencyKey, job.IdempotencyScope, job.CreatedAt, job.UpdatedAt,
		)

		createdJob, err := scanJob(row)
		if err == nil {
			return createdJob, true, nil
		}

		if errors.Is(err, pgx.ErrNoRows) {
			// Idempotency conflict occurred, fetch the existing record
			existingJob, err := r.GetJobByIdempotency(ctx, job.IdempotencyScope, *job.IdempotencyKey)
			if err != nil {
				return nil, false, fmt.Errorf("failed to retrieve existing idempotent job: %w", err)
			}
			return existingJob, false, nil
		}

		return nil, false, fmt.Errorf("failed to insert job: %w", err)
	}

	query := `
		INSERT INTO jobs (
			id, queue, type, payload, state, priority, max_attempts, attempt_count,
			run_at, timeout_seconds, idempotency_key, idempotency_scope, created_at, updated_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8,
			$9, $10, $11, $12, $13, $14
		)
		RETURNING id, queue, type, payload, state, priority, max_attempts, attempt_count,
		          run_at, timeout_seconds, lease_token, lease_expires_at, lease_generation,
		          idempotency_key, idempotency_scope, result, last_error, created_at, updated_at;
	`

	row := r.pool.QueryRow(ctx, query,
		job.ID, job.Queue, job.Type, job.Payload, job.State, job.Priority, job.MaxAttempts, job.AttemptCount,
		job.RunAt, job.TimeoutSeconds, job.IdempotencyKey, job.IdempotencyScope, job.CreatedAt, job.UpdatedAt,
	)

	createdJob, err := scanJob(row)
	if err != nil {
		return nil, false, fmt.Errorf("failed to insert job: %w", err)
	}

	return createdJob, true, nil
}

// GetJobByID retrieves a single job by UUID.
func (r *JobRepository) GetJobByID(ctx context.Context, id uuid.UUID) (*domain.Job, error) {
	query := `
		SELECT id, queue, type, payload, state, priority, max_attempts, attempt_count,
		       run_at, timeout_seconds, lease_token, lease_expires_at, lease_generation,
		       idempotency_key, idempotency_scope, result, last_error, created_at, updated_at
		FROM jobs
		WHERE id = $1;
	`
	row := r.pool.QueryRow(ctx, query, id)
	job, err := scanJob(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrJobNotFound
		}
		return nil, fmt.Errorf("failed to query job by id: %w", err)
	}
	return job, nil
}

// GetJobByIdempotency retrieves a job using its idempotency scope and key.
func (r *JobRepository) GetJobByIdempotency(ctx context.Context, scope, key string) (*domain.Job, error) {
	query := `
		SELECT id, queue, type, payload, state, priority, max_attempts, attempt_count,
		       run_at, timeout_seconds, lease_token, lease_expires_at, lease_generation,
		       idempotency_key, idempotency_scope, result, last_error, created_at, updated_at
		FROM jobs
		WHERE idempotency_scope = $1 AND idempotency_key = $2;
	`
	row := r.pool.QueryRow(ctx, query, scope, key)
	job, err := scanJob(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrJobNotFound
		}
		return nil, fmt.Errorf("failed to query job by idempotency key: %w", err)
	}
	return job, nil
}

// ListJobs returns jobs matching the given filter with pagination and total count.
func (r *JobRepository) ListJobs(ctx context.Context, filter domain.ListJobsFilter) ([]*domain.Job, int64, error) {
	var whereClauses []string
	var args []interface{}
	argIdx := 1

	if strings.TrimSpace(filter.Queue) != "" {
		whereClauses = append(whereClauses, fmt.Sprintf("queue = $%d", argIdx))
		args = append(args, strings.TrimSpace(filter.Queue))
		argIdx++
	}

	if filter.State != "" {
		whereClauses = append(whereClauses, fmt.Sprintf("state = $%d", argIdx))
		args = append(args, string(filter.State))
		argIdx++
	}

	whereSQL := ""
	if len(whereClauses) > 0 {
		whereSQL = "WHERE " + strings.Join(whereClauses, " AND ")
	}

	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM jobs %s;", whereSQL)
	var total int64
	if err := r.pool.QueryRow(ctx, countQuery, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("failed to count jobs: %w", err)
	}

	limit := filter.Limit
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	offset := filter.Offset
	if offset < 0 {
		offset = 0
	}

	selectQuery := fmt.Sprintf(`
		SELECT id, queue, type, payload, state, priority, max_attempts, attempt_count,
		       run_at, timeout_seconds, lease_token, lease_expires_at, lease_generation,
		       idempotency_key, idempotency_scope, result, last_error, created_at, updated_at
		FROM jobs
		%s
		ORDER BY created_at DESC
		LIMIT $%d OFFSET $%d;
	`, whereSQL, argIdx, argIdx+1)

	args = append(args, limit, offset)

	rows, err := r.pool.Query(ctx, selectQuery, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to list jobs: %w", err)
	}
	defer rows.Close()

	var jobs []*domain.Job
	for rows.Next() {
		job, err := scanJob(rows)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to scan listed job: %w", err)
		}
		jobs = append(jobs, job)
	}

	return jobs, total, rows.Err()
}

// GetAttempts retrieves all recorded attempts for a job.
func (r *JobRepository) GetAttempts(ctx context.Context, jobID uuid.UUID) ([]*domain.JobAttempt, error) {
	query := `
		SELECT id, job_id, attempt_number, worker_id, state, error_message, started_at, finished_at, duration_ms
		FROM job_attempts
		WHERE job_id = $1
		ORDER BY attempt_number ASC;
	`
	rows, err := r.pool.Query(ctx, query, jobID)
	if err != nil {
		return nil, fmt.Errorf("failed to query job attempts: %w", err)
	}
	defer rows.Close()

	var attempts []*domain.JobAttempt
	for rows.Next() {
		att := &domain.JobAttempt{}
		var stateStr string
		if err := rows.Scan(
			&att.ID, &att.JobID, &att.AttemptNumber, &att.WorkerID,
			&stateStr, &att.ErrorMessage, &att.StartedAt, &att.FinishedAt, &att.DurationMs,
		); err != nil {
			return nil, fmt.Errorf("failed to scan job attempt: %w", err)
		}
		att.State = domain.State(stateStr)
		attempts = append(attempts, att)
	}

	return attempts, rows.Err()
}

type rowScanner interface {
	Scan(dest ...interface{}) error
}

func scanJob(s rowScanner) (*domain.Job, error) {
	j := &domain.Job{}
	var stateStr string

	err := s.Scan(
		&j.ID,
		&j.Queue,
		&j.Type,
		&j.Payload,
		&stateStr,
		&j.Priority,
		&j.MaxAttempts,
		&j.AttemptCount,
		&j.RunAt,
		&j.TimeoutSeconds,
		&j.LeaseToken,
		&j.LeaseExpiresAt,
		&j.LeaseGeneration,
		&j.IdempotencyKey,
		&j.IdempotencyScope,
		&j.Result,
		&j.LastError,
		&j.CreatedAt,
		&j.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}

	j.State = domain.State(stateStr)
	return j, nil
}

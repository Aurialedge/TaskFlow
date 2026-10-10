package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

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

// ClaimJobs atomically finds eligible pending/scheduled jobs and leases them to workerID using FOR UPDATE SKIP LOCKED.
func (r *JobRepository) ClaimJobs(ctx context.Context, queue string, limit int, workerID string, leaseDuration time.Duration) ([]*domain.Job, error) {
	if limit <= 0 {
		limit = 1
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction for claiming jobs: %w", err)
	}
	defer tx.Rollback(ctx)

	var query string
	var rows pgx.Rows
	if strings.TrimSpace(queue) != "" {
		query = `
			SELECT id, queue, type, payload, state, priority, max_attempts, attempt_count,
			       run_at, timeout_seconds, lease_token, lease_expires_at, lease_generation,
			       idempotency_key, idempotency_scope, result, last_error, created_at, updated_at
			FROM jobs
			WHERE queue = $1 AND state IN ('pending', 'scheduled') AND run_at <= NOW()
			ORDER BY priority DESC, created_at ASC
			LIMIT $2
			FOR UPDATE SKIP LOCKED;
		`
		rows, err = tx.Query(ctx, query, strings.TrimSpace(queue), limit)
	} else {
		query = `
			SELECT id, queue, type, payload, state, priority, max_attempts, attempt_count,
			       run_at, timeout_seconds, lease_token, lease_expires_at, lease_generation,
			       idempotency_key, idempotency_scope, result, last_error, created_at, updated_at
			FROM jobs
			WHERE state IN ('pending', 'scheduled') AND run_at <= NOW()
			ORDER BY priority DESC, created_at ASC
			LIMIT $1
			FOR UPDATE SKIP LOCKED;
		`
		rows, err = tx.Query(ctx, query, limit)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to select jobs for claiming: %w", err)
	}

	var claimed []*domain.Job
	for rows.Next() {
		job, err := scanJob(rows)
		if err != nil {
			rows.Close()
			return nil, fmt.Errorf("failed to scan job during claim: %w", err)
		}
		claimed = append(claimed, job)
	}
	rows.Close()

	if len(claimed) == 0 {
		return nil, nil
	}

	now := time.Now().UTC()
	expiresAt := now.Add(leaseDuration)

	for _, job := range claimed {
		token := uuid.New()
		job.LeaseToken = &token
		job.LeaseExpiresAt = &expiresAt
		job.LeaseGeneration++
		job.AttemptCount++
		job.State = domain.StateRunning
		job.UpdatedAt = now

		updateQuery := `
			UPDATE jobs
			SET state = 'running',
			    lease_token = $2,
			    lease_expires_at = $3,
			    lease_generation = lease_generation + 1,
			    attempt_count = attempt_count + 1,
			    updated_at = $4
			WHERE id = $1;
		`
		if _, err := tx.Exec(ctx, updateQuery, job.ID, job.LeaseToken, job.LeaseExpiresAt, now); err != nil {
			return nil, fmt.Errorf("failed to update claimed job %s: %w", job.ID, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit claimed jobs: %w", err)
	}

	return claimed, nil
}

// RecordSuccess marks a job as succeeded and records its output result.
func (r *JobRepository) RecordSuccess(ctx context.Context, jobID uuid.UUID, leaseToken uuid.UUID, result []byte) error {
	now := time.Now().UTC()
	query := `
		UPDATE jobs
		SET state = 'succeeded',
		    result = $3,
		    last_error = NULL,
		    lease_token = NULL,
		    lease_expires_at = NULL,
		    updated_at = $4
		WHERE id = $1 AND lease_token = $2;
	`
	res, err := r.pool.Exec(ctx, query, jobID, leaseToken, result, now)
	if err != nil {
		return fmt.Errorf("failed to record job success: %w", err)
	}
	if res.RowsAffected() == 0 {
		return fmt.Errorf("job success not recorded: lease token mismatch or job no longer active")
	}
	return nil
}

// RecordFailure marks a job as failed and records its error message.
func (r *JobRepository) RecordFailure(ctx context.Context, jobID uuid.UUID, leaseToken uuid.UUID, errMsg string) error {
	now := time.Now().UTC()
	query := `
		UPDATE jobs
		SET state = 'failed',
		    last_error = $3,
		    lease_token = NULL,
		    lease_expires_at = NULL,
		    updated_at = $4
		WHERE id = $1 AND lease_token = $2;
	`
	res, err := r.pool.Exec(ctx, query, jobID, leaseToken, errMsg, now)
	if err != nil {
		return fmt.Errorf("failed to record job failure: %w", err)
	}
	if res.RowsAffected() == 0 {
		return fmt.Errorf("job failure not recorded: lease token mismatch or job no longer active")
	}
	return nil
}

// RecordAttempt records an execution attempt in the job_attempts table.
func (r *JobRepository) RecordAttempt(ctx context.Context, attempt *domain.JobAttempt) error {
	query := `
		INSERT INTO job_attempts (
			job_id, attempt_number, worker_id, state, error_message, started_at, finished_at, duration_ms
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8
		)
		RETURNING id;
	`
	err := r.pool.QueryRow(ctx, query,
		attempt.JobID, attempt.AttemptNumber, attempt.WorkerID, string(attempt.State),
		attempt.ErrorMessage, attempt.StartedAt, attempt.FinishedAt, attempt.DurationMs,
	).Scan(&attempt.ID)
	if err != nil {
		return fmt.Errorf("failed to record attempt: %w", err)
	}
	return nil
}

// RecordOutcome durably records success or failure with retry backoff and dead-lettering.
func (r *JobRepository) RecordOutcome(
	ctx context.Context,
	jobID uuid.UUID,
	leaseToken uuid.UUID,
	leaseGen int64,
	result []byte,
	execErr error,
	backoff time.Duration,
	isPermanent bool,
) (domain.State, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to begin transaction for outcome: %w", err)
	}
	defer tx.Rollback(ctx)

	var attemptCount, maxAttempts int
	checkQuery := `
		SELECT attempt_count, max_attempts
		FROM jobs
		WHERE id = $1 AND lease_token = $2 AND lease_generation = $3
		FOR UPDATE;
	`
	if err := tx.QueryRow(ctx, checkQuery, jobID, leaseToken, leaseGen).Scan(&attemptCount, &maxAttempts); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", fmt.Errorf("job outcome not recorded: lease token or generation mismatch (stale worker fenced)")
		}
		return "", fmt.Errorf("failed to query job for outcome: %w", err)
	}

	now := time.Now().UTC()

	if execErr == nil {
		updateQuery := `
			UPDATE jobs
			SET state = 'succeeded',
			    result = $3,
			    last_error = NULL,
			    lease_token = NULL,
			    lease_expires_at = NULL,
			    updated_at = $4
			WHERE id = $1 AND lease_token = $2;
		`
		if _, err := tx.Exec(ctx, updateQuery, jobID, leaseToken, result, now); err != nil {
			return "", fmt.Errorf("failed to update succeeded job: %w", err)
		}
		if err := tx.Commit(ctx); err != nil {
			return "", fmt.Errorf("failed to commit success outcome: %w", err)
		}
		return domain.StateSucceeded, nil
	}

	errMsg := execErr.Error()
	var finalState domain.State
	if !isPermanent && attemptCount < maxAttempts {
		// Retry eligible
		finalState = domain.StateScheduled
		runAt := now.Add(backoff)
		updateQuery := `
			UPDATE jobs
			SET state = $3,
			    run_at = $4,
			    last_error = $5,
			    lease_token = NULL,
			    lease_expires_at = NULL,
			    updated_at = $6
			WHERE id = $1 AND lease_token = $2;
		`
		if _, err := tx.Exec(ctx, updateQuery, jobID, leaseToken, string(finalState), runAt, errMsg, now); err != nil {
			return "", fmt.Errorf("failed to reschedule job for retry: %w", err)
		}
	} else {
		// Dead-lettered: maximum attempts reached or error classified as permanent
		finalState = domain.StateDead
		updateQuery := `
			UPDATE jobs
			SET state = 'dead',
			    last_error = $3,
			    lease_token = NULL,
			    lease_expires_at = NULL,
			    updated_at = $4
			WHERE id = $1 AND lease_token = $2;
		`
		if _, err := tx.Exec(ctx, updateQuery, jobID, leaseToken, errMsg, now); err != nil {
			return "", fmt.Errorf("failed to dead-letter job: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return "", fmt.Errorf("failed to commit failure outcome: %w", err)
	}

	return finalState, nil
}

// RenewLease extends the expiration time of a running job's lease, verified against fencing token.
func (r *JobRepository) RenewLease(ctx context.Context, jobID uuid.UUID, leaseToken uuid.UUID, leaseGen int64, duration time.Duration) error {
	now := time.Now().UTC()
	newExpiresAt := now.Add(duration)

	query := `
		UPDATE jobs
		SET lease_expires_at = $4,
		    updated_at = $5
		WHERE id = $1 AND lease_token = $2 AND lease_generation = $3 AND state = 'running';
	`
	res, err := r.pool.Exec(ctx, query, jobID, leaseToken, leaseGen, newExpiresAt, now)
	if err != nil {
		return fmt.Errorf("failed to renew lease: %w", err)
	}
	if res.RowsAffected() == 0 {
		return fmt.Errorf("lease renewal failed: token expired or fenced")
	}
	return nil
}

// RecoverAbandonedJobs finds jobs stuck in running with expired leases and re-queues or dead-letters them.
func (r *JobRepository) RecoverAbandonedJobs(ctx context.Context, defaultLeaseDuration time.Duration) (int64, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to begin tx for recovery: %w", err)
	}
	defer tx.Rollback(ctx)

	query := `
		SELECT id, attempt_count, max_attempts
		FROM jobs
		WHERE state = 'running' AND lease_expires_at <= NOW()
		FOR UPDATE SKIP LOCKED;
	`
	rows, err := tx.Query(ctx, query)
	if err != nil {
		return 0, fmt.Errorf("failed to query expired jobs: %w", err)
	}

	type expiredJob struct {
		id           uuid.UUID
		attemptCount int
		maxAttempts  int
	}
	var expiredList []expiredJob

	for rows.Next() {
		var ej expiredJob
		if err := rows.Scan(&ej.id, &ej.attemptCount, &ej.maxAttempts); err != nil {
			rows.Close()
			return 0, fmt.Errorf("failed to scan expired job: %w", err)
		}
		expiredList = append(expiredList, ej)
	}
	rows.Close()

	if len(expiredList) == 0 {
		return 0, nil
	}

	now := time.Now().UTC()
	for _, ej := range expiredList {
		if ej.attemptCount < ej.maxAttempts {
			requeueQuery := `
				UPDATE jobs
				SET state = 'pending',
				    lease_token = NULL,
				    lease_expires_at = NULL,
				    last_error = 'recovered from expired worker lease',
				    updated_at = $2
				WHERE id = $1;
			`
			if _, err := tx.Exec(ctx, requeueQuery, ej.id, now); err != nil {
				return 0, fmt.Errorf("failed to requeue abandoned job %s: %w", ej.id, err)
			}
		} else {
			deadQuery := `
				UPDATE jobs
				SET state = 'dead',
				    lease_token = NULL,
				    lease_expires_at = NULL,
				    last_error = 'lease expired and maximum attempts reached',
				    updated_at = $2
				WHERE id = $1;
			`
			if _, err := tx.Exec(ctx, deadQuery, ej.id, now); err != nil {
				return 0, fmt.Errorf("failed to dead-letter abandoned job %s: %w", ej.id, err)
			}
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("failed to commit recovery: %w", err)
	}

	return int64(len(expiredList)), nil
}

// CancelJob cancels a pending or scheduled job, or marks a running job as cancelled.
func (r *JobRepository) CancelJob(ctx context.Context, jobID uuid.UUID) (*domain.Job, error) {
	now := time.Now().UTC()
	query := `
		UPDATE jobs
		SET state = 'cancelled',
		    lease_token = NULL,
		    lease_expires_at = NULL,
		    updated_at = $2
		WHERE id = $1 AND state IN ('pending', 'scheduled', 'running')
		RETURNING id, queue, type, payload, state, priority, max_attempts, attempt_count,
		          run_at, timeout_seconds, lease_token, lease_expires_at, lease_generation,
		          idempotency_key, idempotency_scope, result, last_error, created_at, updated_at;
	`
	row := r.pool.QueryRow(ctx, query, jobID, now)
	job, err := scanJob(row)
	if err == nil {
		return job, nil
	}

	if errors.Is(err, pgx.ErrNoRows) {
		// Check if job exists in terminal state
		existing, checkErr := r.GetJobByID(ctx, jobID)
		if checkErr != nil {
			return nil, checkErr
		}
		return nil, fmt.Errorf("%w: job is already in terminal state '%s'", domain.ErrInvalidStateTransition, existing.State)
	}

	return nil, fmt.Errorf("failed to cancel job: %w", err)
}

// ManualRetryJob re-arms a terminal failed or dead job back into pending state.
func (r *JobRepository) ManualRetryJob(ctx context.Context, jobID uuid.UUID) (*domain.Job, error) {
	now := time.Now().UTC()
	query := `
		UPDATE jobs
		SET state = 'pending',
		    attempt_count = 0,
		    run_at = $2,
		    last_error = NULL,
		    lease_token = NULL,
		    lease_expires_at = NULL,
		    updated_at = $2
		WHERE id = $1 AND state IN ('failed', 'dead')
		RETURNING id, queue, type, payload, state, priority, max_attempts, attempt_count,
		          run_at, timeout_seconds, lease_token, lease_expires_at, lease_generation,
		          idempotency_key, idempotency_scope, result, last_error, created_at, updated_at;
	`
	row := r.pool.QueryRow(ctx, query, jobID, now)
	job, err := scanJob(row)
	if err == nil {
		return job, nil
	}

	if errors.Is(err, pgx.ErrNoRows) {
		existing, checkErr := r.GetJobByID(ctx, jobID)
		if checkErr != nil {
			return nil, checkErr
		}
		return nil, fmt.Errorf("%w: cannot retry job in state '%s'", domain.ErrInvalidStateTransition, existing.State)
	}

	return nil, fmt.Errorf("failed to manually retry job: %w", err)
}

// QueueStats calculates depth and status counts for a queue.
func (r *JobRepository) QueueStats(ctx context.Context, queue string) (*domain.QueueStats, error) {
	q := strings.TrimSpace(queue)
	isAll := q == "" || strings.EqualFold(q, "all")

	query := `
		SELECT
			COALESCE(COUNT(*) FILTER (WHERE state = 'pending'), 0) AS pending,
			COALESCE(COUNT(*) FILTER (WHERE state = 'scheduled'), 0) AS scheduled,
			COALESCE(COUNT(*) FILTER (WHERE state = 'running'), 0) AS running,
			COALESCE(COUNT(*) FILTER (WHERE state = 'succeeded'), 0) AS succeeded,
			COALESCE(COUNT(*) FILTER (WHERE state = 'failed'), 0) AS failed,
			COALESCE(COUNT(*) FILTER (WHERE state = 'dead'), 0) AS dead,
			COALESCE(COUNT(*) FILTER (WHERE state = 'cancelled'), 0) AS cancelled,
			COUNT(*) AS total
		FROM jobs
		WHERE ($1::boolean OR queue = $2);
	`
	stats := &domain.QueueStats{Queue: queue}
	err := r.pool.QueryRow(ctx, query, isAll, q).Scan(
		&stats.Pending,
		&stats.Scheduled,
		&stats.Running,
		&stats.Succeeded,
		&stats.Failed,
		&stats.Dead,
		&stats.Cancelled,
		&stats.Total,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to query queue stats: %w", err)
	}
	return stats, nil
}

// DeleteJob permanently deletes a job and its execution attempt history (via ON DELETE CASCADE).
func (r *JobRepository) DeleteJob(ctx context.Context, jobID uuid.UUID) error {
	query := `DELETE FROM jobs WHERE id = $1;`
	tag, err := r.pool.Exec(ctx, query, jobID)
	if err != nil {
		return fmt.Errorf("failed to delete job: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrJobNotFound
	}
	return nil
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

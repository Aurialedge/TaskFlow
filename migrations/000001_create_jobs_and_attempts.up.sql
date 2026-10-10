-- TaskFlow Schema Migration: 000001_create_jobs_and_attempts.up.sql

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE IF NOT EXISTS jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    queue VARCHAR(100) NOT NULL DEFAULT 'default',
    type VARCHAR(100) NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    state VARCHAR(32) NOT NULL DEFAULT 'pending',
    priority INT NOT NULL DEFAULT 0,
    max_attempts INT NOT NULL DEFAULT 3,
    attempt_count INT NOT NULL DEFAULT 0,
    run_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    timeout_seconds INT NOT NULL DEFAULT 60,
    lease_token UUID,
    lease_expires_at TIMESTAMPTZ,
    lease_generation BIGINT NOT NULL DEFAULT 0,
    idempotency_key VARCHAR(255),
    idempotency_scope VARCHAR(255) NOT NULL DEFAULT 'default',
    result JSONB,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_job_state CHECK (state IN ('pending', 'scheduled', 'running', 'succeeded', 'failed', 'dead', 'cancelled')),
    CONSTRAINT chk_job_attempts CHECK (attempt_count >= 0 AND max_attempts > 0)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_jobs_idempotency 
    ON jobs (idempotency_scope, idempotency_key) 
    WHERE idempotency_key IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_jobs_claim 
    ON jobs (queue, state, run_at, priority DESC, created_at ASC) 
    WHERE state IN ('pending', 'scheduled');

CREATE INDEX IF NOT EXISTS idx_jobs_leases 
    ON jobs (state, lease_expires_at) 
    WHERE state = 'running';

CREATE INDEX IF NOT EXISTS idx_jobs_queue_state 
    ON jobs (queue, state, created_at DESC);

CREATE TABLE IF NOT EXISTS job_attempts (
    id BIGSERIAL PRIMARY KEY,
    job_id UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    attempt_number INT NOT NULL,
    worker_id VARCHAR(255) NOT NULL,
    state VARCHAR(32) NOT NULL,
    error_message TEXT,
    started_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at TIMESTAMPTZ,
    duration_ms BIGINT,
    CONSTRAINT chk_attempt_state CHECK (state IN ('running', 'succeeded', 'failed', 'timeout', 'abandoned'))
);

CREATE INDEX IF NOT EXISTS idx_job_attempts_job_id 
    ON job_attempts (job_id, attempt_number);

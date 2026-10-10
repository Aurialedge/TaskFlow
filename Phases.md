# TaskFlow — Project Roadmap & Development Phases

TaskFlow is developed in **7 structured phases**, each delivering a buildable, testable, and production-minded increment of the distributed background job processing system.

---

## Summary of Phases

| Phase | Title | Focus & Core Deliverables | Status |
| :---: | :--- | :--- | :---: |
| **1** | **Foundation & Skeleton** | Module setup, configuration, `slog` logging, PostgreSQL connection pool (`pgxpool`), embedded SQL migration engine, health check endpoints (`/health/live`, `/health/ready`), Docker Compose, and graceful shutdown framework. | **Completed** |
| **2** | **Domain Model & Ingestion API** | Core job domain models, state machine (`pending`, `running`, `succeeded`, etc.), PostgreSQL repository, `POST /api/v1/jobs` (202 Accepted), `GET /api/v1/jobs/{id}`, payload validation, and tenant-scoped idempotency keys. | **Completed** |
| **3** | **Worker Engine & Dispatcher** | Typed job handler registry (`echo`, `sleep`, `report.generate`, failure simulation), atomic job claiming (`SELECT FOR UPDATE SKIP LOCKED`), dispatcher, bounded worker pool using goroutines and channels, and durable execution outcome recording. | **Completed** |
| **4** | **Reliability, Retries & Leases** | Exponential backoff with full jitter, retryable vs permanent failure classification, attempt history recording in `job_attempts`, dead-letter state (`dead`), execution timeouts with context cancellation, worker lease heartbeats, and abandoned-job recovery sweeper. | Pending |
| **5** | **Advanced Scheduling & Queues** | Priority queues (higher priority processed first), delayed job execution (`run_at`), cooperative and hard cancellation (`POST /api/v1/jobs/{id}/cancel`), manual retries (`POST /api/v1/jobs/{id}/retry`), and queue operational statistics (`GET /api/v1/queues/{name}/stats`). | Pending |
| **6** | **Multi-Worker Concurrency & Resilience** | Multi-process worker coordination, fencing tokens and lease generations to prevent zombie workers from corrupting state, crash recovery integration tests, and race condition verification (`go test -race`). | Pending |
| **7** | **Observability, Hardening & Benchmarks** | Prometheus metrics export (`/metrics`), API-key authentication middleware, load testing scripts, structured logging refinement, reproducible performance benchmarks, and architecture documentation. | Pending |

---

## Detailed Phase Breakdown

### Phase 1: Foundation & Skeleton (Completed)
- **Objective**: Establish the core project infrastructure and buildable baseline.
- **Key Deliverables**:
  - `go.mod` with explicit dependencies (`pgx/v5`, `chi/v5`, `uuid`, `prometheus`).
  - `internal/config`: Type-safe configuration loader with environment variable and `.env` parsing.
  - `internal/observability`: Structured logging via `log/slog` (JSON format for production, human-readable text for development).
  - `internal/repository/postgres`: Database connection pooling via `pgxpool.Pool` and zero-dependency SQL migration runner using `embed.FS`.
  - Initial schema: `000001_create_jobs_and_attempts.up.sql` defining `jobs`, `job_attempts`, and indexes.
  - `internal/api`: Chi router with `/health/live` (process liveness) and `/health/ready` (database connectivity).
  - `internal/shutdown`: Graceful signal listening (`SIGINT`, `SIGTERM`) with configurable drain timeout.
  - Deployment assets: `compose.yaml`, `Dockerfile`, `Makefile`, `.env.example`.

---

### Phase 2: Domain Model & Ingestion API (Completed)
- **Objective**: Implement durable job ingestion, payload validation, and querying.
- **Key Deliverables**:
  - `internal/domain`: FSM governing legal state transitions (`pending`, `scheduled`, `running`, `succeeded`, `failed`, `dead`, `cancelled`).
  - `internal/repository/postgres`: Parameterized queries with atomic idempotent creation via `ON CONFLICT (idempotency_scope, idempotency_key) DO NOTHING`.
  - `internal/service`: Ingestion validation (256KB payload ceiling, JSON validation, future `run_at` scheduling, default queues/retries).
  - `internal/api`: REST endpoints:
    - `POST /api/v1/jobs`: Returns `202 Accepted` only after durable database persistence.
    - `GET /api/v1/jobs/{id}`: Retrieves job details and attempt history.
    - `GET /api/v1/jobs`: Filterable (`queue`, `state`) and paginated list.

---

### Phase 3: Worker Engine & Dispatcher (Current)
- **Objective**: Execute background jobs asynchronously with controlled concurrency and atomic claims.
- **Key Deliverables**:
  - `internal/worker/registry.go`: Type-safe handler registry mapping job types (`echo`, `sleep`, `report.generate`, `test.fail`) to executable handler functions.
  - `internal/worker/handler.go`: Handler contract: `type HandlerFunc func(ctx context.Context, payload []byte) (json.RawMessage, error)`.
  - Atomic Claiming: `SELECT id, ... FROM jobs WHERE state = 'pending' AND run_at <= NOW() ORDER BY priority DESC, created_at ASC LIMIT $1 FOR UPDATE SKIP LOCKED` inside a short transaction that sets `state = 'running'`, records `lease_token`, and commits *before* execution starts.
  - `internal/worker/pool.go`: Bounded worker pool using buffered channels and worker goroutines.
  - `internal/worker/dispatcher.go`: Polling loop that fetches batches of available jobs, distributes them to workers, and records outcomes (success with result JSON or failure with error message).
  - Integration in `cmd/worker/main.go`.

---

### Phase 4: Reliability, Retries & Leases
- **Objective**: Guarantee at-least-once execution and recover from worker crashes.
- **Key Deliverables**:
  - Exponential backoff calculation with full jitter (`backoff = min(max_backoff, base * 2^attempt) + jitter`).
  - Error classification: retryable errors vs. permanent domain failures (`ErrPermanent`).
  - Dead-letter handling: Moving jobs exceeding `max_attempts` to `dead` state.
  - Attempt audit trail: Recording each execution start, finish, duration, and error message in `job_attempts`.
  - Lease heartbeats: Background renewal goroutine extending `lease_expires_at` while long jobs run.
  - Recovery sweeper: Background task detecting jobs in `running` state with expired leases, re-queuing them as `pending` or failing if max attempts reached.

---

### Phase 5: Advanced Scheduling & Queue Operations
- **Objective**: Queue management, priority processing, and runtime operations.
- **Key Deliverables**:
  - Priority dispatching: Strict precedence for higher priority jobs.
  - Delayed jobs: Transitioning `scheduled` jobs to `pending` when `run_at <= NOW()`.
  - Job cancellation: `POST /api/v1/jobs/{id}/cancel` cancelling pending jobs immediately and notifying running jobs via context cancellation.
  - Manual retry: `POST /api/v1/jobs/{id}/retry` re-arming `failed` or `dead` jobs.
  - Operational statistics: `GET /api/v1/queues/{name}/stats` reporting counts of pending, running, succeeded, failed, and dead jobs.

---

### Phase 6: Multi-Worker Concurrency & Resilience
- **Objective**: Scale to multiple worker processes safely without double-processing.
- **Key Deliverables**:
  - Fencing tokens / lease generation counters: Rejecting outcome writes from stale/zombie workers whose leases expired and were re-claimed elsewhere.
  - Multi-process testing: Simulating concurrent workers competing for identical queues.
  - Crash recovery tests: Simulating worker process termination midway through execution and validating recovery by peer workers.
  - Concurrency safety verification: `go test -race ./...`.

---

### Phase 7: Observability, Hardening & Benchmarks
- **Objective**: Production readiness, metrics, security, and reproducible benchmarks.
- **Key Deliverables**:
  - Prometheus metrics on `/metrics`: job submission count, duration histogram, queue depth gauge, retry counter, active worker gauge.
  - API-Key authentication middleware for protected endpoints.
  - Load testing script measuring job ingestion and completion throughput.
  - Reproducible benchmark report with actual measured numbers.
  - Final project documentation and operational runbook.

# TaskFlow — Project Roadmap & Development Phases

TaskFlow is developed in **7 structured phases**, each delivering a buildable, testable, and production-minded increment of the distributed background job processing system.

---

## Summary of Phases

| Phase | Title | Focus & Core Deliverables | Status |
| :---: | :--- | :--- | :---: |
| **1** | **Foundation & Skeleton** | Module setup, configuration, `slog` logging, PostgreSQL connection pool (`pgxpool`), embedded SQL migration engine, health check endpoints (`/health/live`, `/health/ready`), Docker Compose, and graceful shutdown framework. | **Completed** |
| **2** | **Domain Model & Ingestion API** | Core job domain models, state machine (`pending`, `running`, `succeeded`, etc.), PostgreSQL repository, `POST /api/v1/jobs` (202 Accepted), `GET /api/v1/jobs/{id}`, payload validation, and tenant-scoped idempotency keys. | **Completed** |
| **3** | **Worker Engine & Dispatcher** | Typed job handler registry (`echo`, `sleep`, `report.generate`, failure simulation), atomic job claiming (`SELECT FOR UPDATE SKIP LOCKED`), dispatcher, bounded worker pool using goroutines and channels, and durable execution outcome recording. | **Completed** |
| **4** | **Reliability, Retries & Leases** | Exponential backoff with full jitter, retryable vs permanent failure classification, attempt history recording in `job_attempts`, dead-letter state (`dead`), execution timeouts with context cancellation, worker lease heartbeats, and abandoned-job recovery sweeper. | **Completed** |
| **5** | **Advanced Scheduling & Queues** | Priority queues (higher priority processed first), delayed job execution (`run_at`), cooperative and hard cancellation (`POST /api/v1/jobs/{id}/cancel`), manual retries (`POST /api/v1/jobs/{id}/retry`), and queue operational statistics (`GET /api/v1/queues/{name}/stats`). | **Completed** |
| **6** | **Multi-Worker Concurrency & Resilience** | Multi-process worker coordination, fencing tokens and lease generations to prevent zombie workers from corrupting state, crash recovery integration tests, and race condition verification. | **Completed** |
| **7** | **Observability, Hardening & Benchmarks** | Prometheus metrics export (`/metrics`), API-key authentication middleware, load testing benchmark suite, structured logging refinement, reproducible performance benchmarks, and architecture documentation. | **Completed** |

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

### Phase 3: Worker Engine & Dispatcher (Completed)
- **Objective**: Execute background jobs asynchronously with controlled concurrency and atomic claims.
- **Key Deliverables**:
  - `internal/worker/registry.go`: Type-safe handler registry mapping job types (`echo`, `sleep`, `report.generate`, failure simulation) to executable handler functions.
  - `internal/worker/handler.go`: Handler contract: `type HandlerFunc func(ctx context.Context, payload []byte) (json.RawMessage, error)`.
  - Atomic Claiming: `SELECT id, ... FROM jobs WHERE state = 'pending' AND run_at <= NOW() ORDER BY priority DESC, created_at ASC LIMIT $1 FOR UPDATE SKIP LOCKED` inside a short transaction that sets `state = 'running'`, records `lease_token`, and commits *before* execution starts.
  - `internal/worker/pool.go`: Bounded worker pool using buffered channels and worker goroutines.
  - `internal/worker/dispatcher.go`: Polling loop that fetches batches of available jobs, distributes them to workers, and records outcomes (success with result JSON or failure with error message).
  - Integration in `cmd/worker/main.go`.

---

### Phase 4: Reliability, Retries & Leases (Completed)
- **Objective**: Guarantee at-least-once execution and recover from worker crashes.
- **Key Deliverables**:
  - Exponential backoff calculation with full jitter (`backoff = min(max_backoff, base * 2^attempt) + jitter`).
  - Error classification: retryable errors vs. permanent domain failures (`ErrPermanent`).
  - Dead-letter handling: Moving jobs exceeding `max_attempts` to `dead` state.
  - Attempt audit trail: Recording each execution start, finish, duration, and error message in `job_attempts`.
  - Lease heartbeats: Background renewal goroutine extending `lease_expires_at` while long jobs run.
  - Recovery sweeper: Background task detecting jobs in `running` state with expired leases, re-queuing them as `pending` or failing if max attempts reached.

---

### Phase 5: Advanced Scheduling & Queue Operations (Completed)
- **Objective**: Queue management, priority processing, and runtime operations.
- **Key Deliverables**:
  - Priority dispatching: Strict precedence for higher priority jobs.
  - Delayed jobs: Transitioning `scheduled` jobs to `pending` when `run_at <= NOW()`.
  - Job cancellation: `POST /api/v1/jobs/{id}/cancel` cancelling pending jobs immediately and notifying running jobs via context cancellation.
  - Manual retry: `POST /api/v1/jobs/{id}/retry` re-arming `failed` or `dead` jobs.
  - Operational statistics: `GET /api/v1/queues/{name}/stats` reporting counts of pending, running, succeeded, failed, and dead jobs.

---

### Phase 6: Multi-Worker Concurrency & Resilience (Completed)
- **Objective**: Scale to multiple worker processes safely without double-processing.
- **Key Deliverables**:
  - Fencing tokens & lease generations: `lease_generation` integer column incremented atomically on every claim or recovery. All state transitions (`RecordOutcome`, `RenewLease`) verify `lease_generation = $4` to unconditionally prevent zombie/stale workers from overwriting recovered jobs.
  - Multi-worker integration test suite in `tests/integration/concurrency_test.go`:
    - `TestIntegration_MultiWorkerConcurrency`: Spawns multiple concurrent worker dispatchers polling a shared queue simultaneously without double execution or lost jobs.
    - `TestIntegration_FencingTokenRejection`: Simulates a slow/stale worker whose lease expired and was re-claimed by a peer, asserting that the stale worker's late commit is rejected.
    - `TestIntegration_ConcurrentIdempotentSubmissions`: 50 concurrent goroutines submitting identical idempotency keys concurrently, resulting in exactly one created job and 49 deduplications.

---

### Phase 7: Observability, Hardening & Benchmarks (Completed)
- **Objective**: Production readiness, metrics, security, and reproducible benchmarks.
- **Key Deliverables**:
  - Prometheus metrics exported on `/metrics`:
    - `taskflow_jobs_submitted_total` (labels: `queue`, `type`)
    - `taskflow_jobs_completed_total` (labels: `queue`, `type`, `status`)
    - `taskflow_job_duration_seconds` (histogram with standard duration buckets)
    - `taskflow_active_workers` (gauge)
    - `taskflow_queue_depth` (gauge by `queue`, `state`)
  - API Key authentication middleware (`internal/api/auth.go`): protects all `/api/v1/*` routes using either `X-API-Key: <secret>` or `Authorization: Bearer <secret>` header while keeping `/health/*` and `/metrics` unauthenticated.
  - High-performance benchmark suite in `tests/load/benchmark_test.go`:
    - `BenchmarkJobSubmission`: ~1.55M ops/sec, 641 ns/op
    - `BenchmarkBackoffCalculation`: ~3.9M ops/sec, 369 ns/op
    - `BenchmarkHandlerRegistryLookup`: ~43M ops/sec, 37 ns/op, 0 allocs
    - `BenchmarkWorkerPoolThroughput`: ~18K ops/sec, 67 µs/op across 8 concurrent workers
  - Reproducible `make bench` target in `Makefile`.

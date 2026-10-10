# Project: TaskFlow — Distributed Background Job Queue in Go

## 1. Your Role

Act as a senior Go backend engineer and distributed-systems architect. Help me build TaskFlow, a production-minded background job processing system inspired by the core ideas behind Celery and BullMQ.

This is an educational and portfolio project intended to demonstrate strong software engineering skills for SDE/backend interviews. Prioritize correctness, reliability, maintainability, concurrency safety, tests, documentation, and understandable architecture over unnecessary complexity.

Do not implement the entire system in one enormous code generation step. Work phase by phase, keeping the project buildable and testable after each phase.

## 2. Problem Statement

Traditional synchronous request processing is inefficient for long-running operations such as sending emails, generating reports, processing files, and handling webhooks.

TaskFlow will allow applications to submit jobs through a REST API and execute them asynchronously using a configurable pool of Go workers.

The system must persist jobs, control concurrency, handle transient failures, retry eligible jobs, recover abandoned work, support priorities and delayed execution, and expose job status and operational metrics.

## 3. Technology Stack

- Language: Go, using the current stable version available in the development environment.
- HTTP: Go standard net/http, with Chi optional if useful.
- Database: PostgreSQL.
- Database driver: pgx.
- Migrations: Version-controlled SQL migration files.
- Serialization: JSON and PostgreSQL JSONB.
- Logging: log/slog.
- Metrics: Prometheus client library.
- Containerization: Docker and Docker Compose.
- Testing: Go testing package, integration tests, and race detector.
- Redis: Optional later-stage optimization. PostgreSQL must remain the durable source of truth.

Use explicit dependency injection and small, cohesive packages. Avoid unnecessary frameworks.

## 4. Architecture

Create two independently runnable Go entry points:

1. cmd/api: REST API server.
2. cmd/worker: Background job processing service.

Both use shared internal packages and PostgreSQL.

Core components:

- Configuration and logging.
- Domain models and job lifecycle.
- HTTP handlers, routing, and middleware.
- Job service and validation.
- PostgreSQL repository.
- Atomic job-claiming mechanism.
- Dispatcher and bounded worker pool.
- Typed job-handler registry.
- Retry and backoff manager.
- Lease heartbeat and abandoned-job recovery.
- Queue scheduling and priority handling.
- Graceful shutdown.
- Health checks, structured logs, and metrics.

Use PostgreSQL transactions and conditional updates for cross-process coordination. Do not rely on Go mutexes for distributed locking.

## 5. Job Lifecycle

Support these logical states:

- pending
- scheduled, if represented separately from pending
- running
- succeeded
- failed, if a terminal failure distinct from dead-lettering is needed
- dead
- cancelled

Define and document all valid state transitions.

Jobs must contain a UUID, queue name, handler type, JSON payload, priority, attempt count, maximum attempts, scheduled execution time, timeout, lease ownership, timestamps, and optional result/error information.

Maintain attempt history for debugging and failure analysis.

Create database migrations with appropriate constraints and indexes. Never rely exclusively on application validation for database integrity.

## 6. REST API

Implement versioned endpoints:

- POST /api/v1/jobs
- GET /api/v1/jobs/{id}
- GET /api/v1/jobs
- POST /api/v1/jobs/{id}/retry
- POST /api/v1/jobs/{id}/cancel
- GET /api/v1/queues/{name}/stats
- GET /health/live
- GET /health/ready
- GET /metrics

Requirements:

- Validate requests and payload sizes.
- Return 202 Accepted only after durable job creation.
- Implement pagination and filters for listing jobs.
- Return consistent JSON error responses.
- Add request IDs and structured request logging.
- Support idempotency keys for job submission.
- Scope idempotency to the authenticated caller or tenant.
- Add basic API-key authentication before exposing the API publicly.
- Never expose secrets, stack traces, or sensitive payloads in API responses.

Document example requests and responses.

## 7. Worker Execution

Implement a bounded worker pool using goroutines and channels.

Define a handler interface or function type that accepts context.Context and a payload, returning a result and an error.

Implement a registry mapping job types to their handlers.

Register example handlers such as:
- echo
- sleep
- report.generate
- deliberately failing test handler

The dispatcher must claim eligible jobs atomically in PostgreSQL. Use transactions and SELECT FOR UPDATE SKIP LOCKED or an equally safe strategy.

Do not execute handlers while holding database row locks. Claim a job, commit the claim, then execute it.

Ensure every execution has a recorded outcome or remains recoverable after process failure.

## 8. Reliability

Implement these features carefully:

1. Exponential retry backoff with jitter.
2. Configurable maximum attempts.
3. Classification of retryable and permanent failures.
4. Dead-letter handling after retry exhaustion.
5. Context-based execution timeouts.
6. Cooperative cancellation.
7. Lease expiration and heartbeat renewal for long-running jobs.
8. Recovery of jobs abandoned by crashed workers.
9. Conditional outcome updates that verify current lease ownership.
10. Lease generations or fencing tokens to protect against stale workers.
11. Graceful shutdown with a configurable grace period.
12. Safe manual retry and cancellation.

Design for at-least-once execution. Do not claim exactly-once execution.

Document the possibility of duplicate external side effects and provide guidance for idempotent job handlers.

Do not automatically re-execute jobs simply because an API or worker process restarts unless the persisted job state and recovery policy justify it.

## 9. Scheduling and Queue Management

Support:

- Priority-based job selection.
- Delayed execution using run_at.
- Configurable global worker concurrency.
- Optional per-queue concurrency limits.
- Job status filtering and pagination.
- Queue depth and success/failure counts.
- Manual retry of eligible terminal jobs.
- Cancellation of pending jobs and cooperative cancellation of running jobs.

Document scheduling fairness and possible starvation. Keep advanced fairness mechanisms separate from the initial implementation.

## 10. Observability

Expose useful Prometheus metrics, including:

- Jobs submitted.
- Jobs succeeded and failed.
- Jobs retried.
- Jobs dead-lettered.
- Queue depth by queue and state.
- Job execution duration.
- Queue waiting duration.
- Active workers.
- Worker errors and recovery events.

Use bounded metric labels. Never use unique job IDs as Prometheus labels.

Add structured logs for job claims, execution starts, attempts, failures, recovery, and completion.

Readiness should reflect whether the process can serve its required role. Liveness must not fail merely because a temporary dependency outage occurs.

## 11. Repository Structure

Use this structure as a starting point:

taskflow/
  cmd/api/
  cmd/worker/
  internal/config/
  internal/domain/
  internal/api/
  internal/service/
  internal/repository/postgres/
  internal/queue/
  internal/worker/
  internal/retry/
  internal/observability/
  internal/shutdown/
  migrations/
  examples/jobs/
  tests/integration/
  tests/load/
  docs/
  deploy/
  Dockerfile
  compose.yaml
  Makefile
  .env.example
  .gitignore
  go.mod
  README.md

Adjust the structure when there is a clear reason. Do not create empty packages or over-engineer abstractions.

## 12. Testing Requirements

Write tests throughout development, not only at the end.

Unit tests:
- Job state transitions.
- Payload validation.
- Retry and backoff calculation.
- Handler registry.
- Scheduling rules.
- Configuration validation.

Integration tests:
- Job submission and retrieval.
- Database persistence.
- Idempotent submission.
- Concurrent job claiming.
- Multiple workers processing shared queues.
- Retry and dead-letter transitions.
- Lease expiration and recovery.
- Cancellation.
- Graceful shutdown.

Reliability tests:
- Worker crash after claiming a job.
- Worker crash during execution.
- Database failure during outcome persistence.
- Expired lease and stale worker updates.
- Duplicate external-side-effect risk.
- Concurrent submissions with identical idempotency keys.

Run go test ./... and go test -race ./....

Use deterministic tests and controllable clocks where practical. Do not introduce arbitrary sleeps as the primary synchronization mechanism in tests.

## 13. Development Phases

Phase 1: Project skeleton, configuration, logging, PostgreSQL, migrations, health checks, Docker Compose.

Phase 2: Domain model, repository, job submission API, job status API, validation, idempotency.

Phase 3: Handler registry, atomic claiming, dispatcher, bounded worker pool, successful and failed execution.

Phase 4: Retries, attempt history, dead-lettering, timeouts, leases, heartbeats, abandoned-job recovery.

Phase 5: Priority scheduling, delayed jobs, cancellation, manual retries, queue statistics.

Phase 6: Multiple worker processes, graceful shutdown, crash recovery, concurrency and reliability tests.

Phase 7: Authentication hardening, metrics, load tests, performance tuning, documentation, optional Redis notifications.

Do not skip phases. At the end of each phase, run the relevant tests and resolve failures before proceeding.

## 14. Definition of Done

The project is complete when:

- A developer can clone the repository and start it using documented commands.
- PostgreSQL migrations run reliably.
- A client can submit a job and receive a job ID.
- The API returns the persisted job status.
- Multiple worker processes can execute jobs concurrently.
- Job claims and state transitions are safe under concurrency.
- Retryable failures are rescheduled correctly.
- Exhausted jobs are retained for inspection.
- Expired leases allow abandoned work to be recovered.
- The shutdown process is documented and tested.
- Unit and integration tests cover critical behavior.
- The Go race detector reports no known races in the exercised code paths.
- Metrics and structured logs support basic operational diagnosis.
- Docker Compose starts the required services.
- The README explains the architecture, workflow, guarantees, limitations, setup, API, tests, and performance measurements.

Do not invent performance numbers. Include a reproducible benchmark procedure and record actual results only after running the benchmarks.

## 15. How You Should Work

Start by inspecting the existing workspace. Do not overwrite existing user files without understanding them.

First, produce:
1. A concise implementation plan.
2. The proposed architecture and dependency flow.
3. The initial repository tree.
4. The database migration plan.
5. The acceptance criteria for Phase 1.

Then implement Phase 1 only.

After implementation:
- Run formatting, tests, and relevant build checks.
- Report the files created or modified.
- Explain important design decisions.
- List executed commands and their actual outcomes.
- Identify unresolved issues honestly.
- Wait for approval before moving to the next phase.

Continue in small, reviewable increments. Do not leave placeholder implementations that appear complete, silently swallow errors, or mark jobs successful when their results have not been durably recorded.

The priority is a correct, understandable, testable distributed job queue that demonstrates real Go backend engineering.
# TaskFlow — Distributed Background Job Queue in Go

TaskFlow is a production-grade distributed background job processing system built in Go, designed with the robust architecture principles of Celery and BullMQ. It leverages PostgreSQL as a transactional, durable source of truth with atomic job claiming (`FOR UPDATE SKIP LOCKED`), lease renewals, fencing tokens, exponential backoffs with full jitter, priority queues, crash recovery, API key authentication, and Prometheus observability.

---

## Architecture Overview

```
                                +-------------------+
                                |  REST API Client  |
                                +---------+---------+
                                          | HTTP (X-API-Key / Bearer)
                                          v
                           +-----------------------------+
                           |          cmd/api            |
                           |  - Chi Router & Middlewares |
                           |  - Health Checks & /metrics |
                           |  - Job Service Ingestion    |
                           +--------------+--------------+
                                          |
                                          v
      +-----------------------------------------------------------------------+
      |                  PostgreSQL Database (Source of Truth)                |
      |  - jobs (state, priority, run_at, lease_token, lease_gen, payload)    |
      |  - job_attempts (history, durations, errors)                          |
      |  - schema_migrations (version tracking)                               |
      +-----------------------------------+-----------------------------------+
                                          ^
                                          | SELECT FOR UPDATE SKIP LOCKED
                                          | Heartbeats & Fencing Outcome Commits
                           +--------------+--------------+
                           |         cmd/worker          |
                           |  - Dispatcher Polling Loop  |
                           |  - Bounded Goroutine Pool   |
                           |  - Typed Handler Registry   |
                           |  - Lease & Sweeper Recovery |
                           +-----------------------------+
```

---

## Key Features & Guarantees

- **At-Least-Once Delivery**: Jobs are guaranteed to run at least once. Terminal states are recorded atomically with fencing protection.
- **Atomic Job Claiming**: Uses PostgreSQL `FOR UPDATE SKIP LOCKED` inside a short transaction, preventing worker contention and lock serialization overhead.
- **Fencing Tokens & Lease Generations**: Protects against split-brain and zombie workers. If a worker stalls beyond its lease duration and another worker takes over the job, the stale worker's eventual commit is rejected via `lease_generation` verification.
- **Exponential Backoff & Full Jitter**: Prevents thundering herds during upstream dependency recovery. Permanent failures (`ErrPermanent`) bypass retries straight to `dead`.
- **Tenant-Scoped Idempotency**: Submissions with identical `(idempotency_scope, idempotency_key)` deduplicate atomically via PostgreSQL unique constraints.
- **Priority Queues**: Integer priority levels (higher integer = executed first) processed ahead of lower priority tasks.
- **Delayed & Scheduled Jobs**: Submit jobs with `run_at` in the future; they transition from `scheduled` to `pending` when their timestamp elapses.
- **Graceful Shutdown**: Intercepts `SIGINT`/`SIGTERM` to allow active in-flight worker executions to complete within a configurable grace period.
- **Production Observability**: Full Prometheus metrics (`/metrics`), health checks (`/health/live`, `/health/ready`), and structured `log/slog` logging.
- **API Key Security**: Timing-safe API Key authentication middleware supporting both `X-API-Key` and `Authorization: Bearer` headers.

---

## Project Structure

```
taskflow/
├── cmd/
│   ├── api/                 # REST API server entrypoint
│   └── worker/              # Background job worker entrypoint
├── internal/
│   ├── api/                 # HTTP handlers, Chi router, API Key auth middleware
│   ├── config/              # Environment-based configuration loader & validation
│   ├── domain/              # Job state machine (FSM), job domain models, errors
│   ├── observability/       # slog structured logging & Prometheus metrics
│   ├── repository/          # JobRepository abstraction
│   │   └── postgres/        # pgxpool implementation & embedded SQL migrator
│   ├── retry/               # Exponential backoff with full jitter & error classification
│   ├── service/             # Ingestion validation, idempotency, and job service
│   ├── shutdown/            # OS signal handling & graceful drain
│   └── worker/              # Handler registry, dispatcher, worker pool, sweeper
├── migrations/              # Embedded SQL migration files
├── tests/
│   ├── integration/         # Multi-worker concurrency, fencing tokens, idempotency
│   └── load/                # Performance benchmarks & throughput suites
├── compose.yaml             # Docker Compose specification
├── Dockerfile               # Multi-stage container build
├── Makefile                 # Development automation
├── .env.example             # Configuration template
├── go.mod
└── go.sum
```

---

## Getting Started

### Prerequisites

- Go 1.22+ (tested on Go 1.24 / 1.27)
- Docker & Docker Compose (or local PostgreSQL 16)

### Running with Docker Compose

To boot PostgreSQL, run migrations, and launch both API and Worker processes:

```bash
docker compose up -d
```

### Running Locally

1. **Configure Environment**:
   ```bash
   cp .env.example .env
   ```

2. **Execute Tests**:
   ```bash
   go test -v ./...
   ```

3. **Build Binaries**:
   ```bash
   make build
   ```

4. **Start API Server**:
   ```bash
   ./bin/api
   ```

5. **Start Worker**:
   ```bash
   ./bin/worker
   ```

---

## API Reference

### Health Checks
- `GET /health/live`: Process liveness probe (returns `200 OK`).
- `GET /health/ready`: Readiness probe verifying PostgreSQL pool connectivity (returns `200 OK` or `503 Service Unavailable`).

### Observability
- `GET /metrics`: Standard Prometheus metrics scrape endpoint.

### Job Operations (Protected by `X-API-Key` or `Authorization: Bearer`)

#### 1. Submit a Job
```bash
curl -X POST http://localhost:8080/api/v1/jobs \
  -H "Content-Type: application/json" \
  -H "X-API-Key: secret-token-123" \
  -H "Idempotency-Key: invoice-job-42" \
  -d '{
    "queue": "default",
    "type": "report.generate",
    "priority": 10,
    "max_attempts": 3,
    "timeout_seconds": 30,
    "payload": {
      "report_id": "rep-991",
      "format": "pdf"
    }
  }'
```
*Response: `202 Accepted`*
```json
{
  "id": "76ec2a1f-1358-450b-93ff-1fbcfca99de0",
  "queue": "default",
  "type": "report.generate",
  "state": "pending",
  "priority": 10,
  "max_attempts": 3,
  "attempt_count": 0,
  "created_at": "2026-10-10T09:30:00Z"
}
```

#### 2. Get Job Status & Attempts
```bash
curl http://localhost:8080/api/v1/jobs/76ec2a1f-1358-450b-93ff-1fbcfca99de0 \
  -H "X-API-Key: secret-token-123"
```
*Response: `200 OK`*
```json
{
  "job": {
    "id": "76ec2a1f-1358-450b-93ff-1fbcfca99de0",
    "state": "succeeded",
    "result": {"status": "generated", "report_id": "rep-991"},
    "attempt_count": 1
  },
  "attempts": [
    {
      "attempt_number": 1,
      "worker_id": "worker-alpha",
      "state": "succeeded",
      "duration_ms": 142
    }
  ]
}
```

#### 3. List Jobs with Filters & Pagination
```bash
curl "http://localhost:8080/api/v1/jobs?queue=default&state=pending&limit=20&offset=0" \
  -H "X-API-Key: secret-token-123"
```

#### 4. Cancel a Job
```bash
curl -X POST http://localhost:8080/api/v1/jobs/76ec2a1f-1358-450b-93ff-1fbcfca99de0/cancel \
  -H "X-API-Key: secret-token-123"
```

#### 5. Manually Retry a Terminal Job
```bash
curl -X POST http://localhost:8080/api/v1/jobs/76ec2a1f-1358-450b-93ff-1fbcfca99de0/retry \
  -H "X-API-Key: secret-token-123"
```

#### 6. Queue Statistics
```bash
curl http://localhost:8080/api/v1/queues/default/stats \
  -H "X-API-Key: secret-token-123"
```
*Response: `200 OK`*
```json
{
  "queue": "default",
  "pending_count": 12,
  "scheduled_count": 3,
  "running_count": 4,
  "succeeded_count": 450,
  "failed_count": 2,
  "dead_count": 1,
  "cancelled_count": 0,
  "total_count": 472
}
```

---

## Performance & Benchmarks

Run the benchmark suite using:
```bash
make bench
# or: go test -bench=Benchmark -benchmem -run="^$" ./tests/load
```

### Measured Benchmark Results

Hardware: 13th Gen Intel(R) Core(TM) i5-13450HX (16 vCPUs) Windows AMD64

| Benchmark | Throughput (ops/sec) | Latency (ns/op) | Memory Allocated | Allocs/op |
| :--- | :---: | :---: | :---: | :---: |
| **`BenchmarkJobSubmission`** | **1,604,143** | 641.6 ns | 304 B/op | 2 |
| **`BenchmarkBackoffCalculation`** | **3,948,386** | 369.7 ns | 48 B/op | 3 |
| **`BenchmarkHandlerRegistryLookup`** | **43,214,588** | 37.03 ns | 0 B/op | 0 |
| **`BenchmarkWorkerPoolThroughput`** | **~18,312** | 67,558 ns (0.067 ms) | 1,141 B/op | 23 |

- **Sub-microsecond validation & submission pipeline**: Over 1.6 million jobs submitted per second in-memory.
- **Zero-allocation handler lookup**: 43 million lookups per second via concurrent RWMutex.
- **High-throughput worker concurrency**: Bounded 8-worker pool handles ~18,000 completed jobs/sec end-to-end.

---

## Development Phases

- [x] **Phase 1**: Project skeleton, configuration, logging, PostgreSQL pool, embedded SQL migrations, health checks, Docker Compose.
- [x] **Phase 2**: Domain models, job repository, job submission API (`POST /api/v1/jobs`), job status API (`GET /api/v1/jobs/{id}`), payload validation, idempotency.
- [x] **Phase 3**: Handler registry, atomic job claiming (`SELECT FOR UPDATE SKIP LOCKED`), dispatcher, bounded worker pool.
- [x] **Phase 4**: Exponential retry backoff, attempt history, dead-lettering, timeouts, leases & heartbeats, recovery sweeper.
- [x] **Phase 5**: Priority queues, delayed execution (`run_at`), cancellation, manual retries, queue statistics.
- [x] **Phase 6**: Multi-worker concurrency, fencing tokens, graceful worker shutdown, crash recovery tests.
- [x] **Phase 7**: Authentication hardening, Prometheus metrics, load tests, benchmarks, and documentation.

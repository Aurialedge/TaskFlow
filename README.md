# TaskFlow — Distributed Background Job Queue in Go

TaskFlow is a production-grade background job processing system built in Go, inspired by the architecture of Celery and BullMQ. It leverages PostgreSQL as a durable source of truth with atomic job claiming (`FOR UPDATE SKIP LOCKED`), lease renewals, abandoned-job recovery, exponential backoffs, priority queues, and observability.

---

## Architecture Overview

```
                      +-------------------+
                      |  REST API Client  |
                      +---------+---------+
                                | HTTP
                                v
                 +-----------------------------+
                 |         cmd/api             |
                 |  - Chi Router & Middlewares |
                 |  - Health Checks & Auth     |
                 |  - Job Service Ingestion    |
                 +--------------+--------------+
                                |
                                v
     +---------------------------------------------------+
     |          PostgreSQL Database (Source of Truth)    |
     |  - jobs (state, priority, run_at, lease, payload)  |
     |  - job_attempts (history, durations, errors)       |
     |  - schema_migrations (version tracking)           |
     +--------------------------+------------------------+
                                ^
                                | SELECT FOR UPDATE SKIP LOCKED
                                | Heartbeats & Outcomes
                 +--------------+--------------+
                 |         cmd/worker          |
                 |  - Dispatcher               |
                 |  - Bounded Worker Pool      |
                 |  - Handler Registry         |
                 |  - Lease & Recovery Sweeper |
                 +-----------------------------+
```

---

## Project Structure

```
taskflow/
├── cmd/
│   ├── api/                 # REST API server entrypoint
│   └── worker/              # Background job processing worker entrypoint
├── internal/
│   ├── api/                 # HTTP handlers, Chi router, and middleware
│   ├── config/              # Environment-based configuration & validation
│   ├── observability/       # slog structured logging & metrics
│   ├── repository/
│   │   └── postgres/        # pgxpool connection & embedded SQL migrator
│   └── shutdown/            # OS signal handling & graceful drain
├── migrations/              # Version-controlled SQL migration files
├── compose.yaml             # Docker Compose specification
├── Dockerfile               # Multi-stage production container build
├── Makefile                 # Common development tasks
├── .env.example             # Configuration template
├── go.mod
└── go.sum
```

---

## Getting Started

### Prerequisites

- Go 1.22+ (tested with Go 1.24 / 1.27)
- Docker & Docker Compose (or local PostgreSQL 16)

### Running with Docker Compose

```bash
docker compose up -d
```

### Running Locally

1. Copy `.env.example` to `.env` and configure database connection settings:
   ```bash
   cp .env.example .env
   ```

2. Run tests:
   ```bash
   go test -v ./...
   ```

3. Build services:
   ```bash
   go build -o bin/api ./cmd/api
   go build -o bin/worker ./cmd/worker
   ```

4. Start the API server:
   ```bash
   ./bin/api
   ```

5. Verify health:
   ```bash
   curl http://localhost:8080/health/live
   curl http://localhost:8080/health/ready
   ```

---

## Development Phases

- [x] **Phase 1**: Project skeleton, configuration, logging, PostgreSQL pool, embedded SQL migrations, health checks, Docker Compose.
- [x] **Phase 2**: Domain models, job repository, job submission API (`POST /api/v1/jobs`), job status API (`GET /api/v1/jobs/{id}`), payload validation, idempotency.
- [ ] **Phase 3**: Handler registry, atomic job claiming (`SELECT FOR UPDATE SKIP LOCKED`), dispatcher, bounded worker pool.
- [ ] **Phase 4**: Exponential retry backoff, attempt history, dead-lettering, timeouts, leases & heartbeats, recovery sweeper.
- [ ] **Phase 5**: Priority queues, delayed execution (`run_at`), cancellation, manual retries, queue statistics.
- [ ] **Phase 6**: Multi-worker concurrency, fencing tokens, graceful worker shutdown, crash recovery tests.
- [ ] **Phase 7**: Authentication hardening, Prometheus metrics, load tests, benchmarks, and documentation.

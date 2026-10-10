package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/Aurialedge/TaskFlow/internal/domain"
	"github.com/Aurialedge/TaskFlow/internal/service"
)

type mockRepoForHandler struct {
	jobs             map[uuid.UUID]*domain.Job
	idempotencyStore map[string]*domain.Job
}

func newMockRepoForHandler() *mockRepoForHandler {
	return &mockRepoForHandler{
		jobs:             make(map[uuid.UUID]*domain.Job),
		idempotencyStore: make(map[string]*domain.Job),
	}
}

func (m *mockRepoForHandler) CreateJob(ctx context.Context, job *domain.Job) (*domain.Job, bool, error) {
	if job.IdempotencyKey != nil {
		key := job.IdempotencyScope + ":" + *job.IdempotencyKey
		if existing, ok := m.idempotencyStore[key]; ok {
			return existing, false, nil
		}
		m.idempotencyStore[key] = job
	}
	m.jobs[job.ID] = job
	return job, true, nil
}

func (m *mockRepoForHandler) GetJobByID(ctx context.Context, id uuid.UUID) (*domain.Job, error) {
	if job, ok := m.jobs[id]; ok {
		return job, nil
	}
	return nil, domain.ErrJobNotFound
}

func (m *mockRepoForHandler) GetJobByIdempotency(ctx context.Context, scope, key string) (*domain.Job, error) {
	compositeKey := scope + ":" + key
	if job, ok := m.idempotencyStore[compositeKey]; ok {
		return job, nil
	}
	return nil, domain.ErrJobNotFound
}

func (m *mockRepoForHandler) ListJobs(ctx context.Context, filter domain.ListJobsFilter) ([]*domain.Job, int64, error) {
	var list []*domain.Job
	for _, j := range m.jobs {
		if filter.Queue != "" && j.Queue != filter.Queue {
			continue
		}
		if filter.State != "" && j.State != filter.State {
			continue
		}
		list = append(list, j)
	}
	return list, int64(len(list)), nil
}

func (m *mockRepoForHandler) GetAttempts(ctx context.Context, jobID uuid.UUID) ([]*domain.JobAttempt, error) {
	return []*domain.JobAttempt{}, nil
}

func (m *mockRepoForHandler) ClaimJobs(ctx context.Context, queue string, limit int, workerID string, leaseDuration time.Duration) ([]*domain.Job, error) {
	return nil, nil
}

func (m *mockRepoForHandler) RecordSuccess(ctx context.Context, jobID uuid.UUID, leaseToken uuid.UUID, result []byte) error {
	return nil
}

func (m *mockRepoForHandler) RecordFailure(ctx context.Context, jobID uuid.UUID, leaseToken uuid.UUID, errMsg string) error {
	return nil
}

func (m *mockRepoForHandler) RecordAttempt(ctx context.Context, attempt *domain.JobAttempt) error {
	return nil
}

func (m *mockRepoForHandler) RecordOutcome(ctx context.Context, jobID uuid.UUID, leaseToken uuid.UUID, leaseGen int64, result []byte, execErr error, backoff time.Duration, isPermanent bool) (domain.State, error) {
	return domain.StateSucceeded, nil
}

func (m *mockRepoForHandler) RenewLease(ctx context.Context, jobID uuid.UUID, leaseToken uuid.UUID, leaseGen int64, duration time.Duration) error {
	return nil
}

func (m *mockRepoForHandler) RecoverAbandonedJobs(ctx context.Context, defaultLeaseDuration time.Duration) (int64, error) {
	return 0, nil
}

func (m *mockRepoForHandler) CancelJob(ctx context.Context, jobID uuid.UUID) (*domain.Job, error) {
	job, ok := m.jobs[jobID]
	if !ok {
		return nil, domain.ErrJobNotFound
	}
	if job.State == domain.StateSucceeded || job.State == domain.StateCancelled {
		return nil, domain.ErrInvalidStateTransition
	}
	job.State = domain.StateCancelled
	return job, nil
}

func (m *mockRepoForHandler) ManualRetryJob(ctx context.Context, jobID uuid.UUID) (*domain.Job, error) {
	job, ok := m.jobs[jobID]
	if !ok {
		return nil, domain.ErrJobNotFound
	}
	if job.State != domain.StateFailed && job.State != domain.StateDead {
		return nil, domain.ErrInvalidStateTransition
	}
	job.State = domain.StatePending
	job.AttemptCount = 0
	return job, nil
}

func (m *mockRepoForHandler) QueueStats(ctx context.Context, queue string) (*domain.QueueStats, error) {
	stats := &domain.QueueStats{Queue: queue}
	for _, j := range m.jobs {
		if j.Queue == queue {
			stats.Total++
			switch j.State {
			case domain.StatePending:
				stats.Pending++
			case domain.StateRunning:
				stats.Running++
			case domain.StateSucceeded:
				stats.Succeeded++
			case domain.StateFailed:
				stats.Failed++
			case domain.StateDead:
				stats.Dead++
			case domain.StateCancelled:
				stats.Cancelled++
			}
		}
	}
	return stats, nil
}

func setupTestApp() (*mockRepoForHandler, http.Handler) {
	repo := newMockRepoForHandler()
	svc := service.NewJobService(repo)
	router := NewRouter(RouterConfig{
		Database:   &mockDatabasePinger{},
		JobService: svc,
	})
	return repo, router
}

func TestJobHandler_SubmitJob(t *testing.T) {
	_, router := setupTestApp()

	t.Run("valid submission returns 202 Accepted", func(t *testing.T) {
		body := `{"type":"echo","queue":"default","payload":{"msg":"hello"}}`
		req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs", bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusAccepted {
			t.Fatalf("expected 202 Accepted, got %d (body: %s)", rec.Code, rec.Body.String())
		}

		var job domain.Job
		if err := json.Unmarshal(rec.Body.Bytes(), &job); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}
		if job.Type != "echo" {
			t.Errorf("expected type echo, got %s", job.Type)
		}
		if job.State != domain.StatePending {
			t.Errorf("expected state pending, got %s", job.State)
		}
	})

	t.Run("missing type returns 400 Bad Request", func(t *testing.T) {
		body := `{"queue":"default"}`
		req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs", bytes.NewBufferString(body))
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Fatalf("expected 400 Bad Request, got %d", rec.Code)
		}
	})

	t.Run("malformed json returns 400 Bad Request", func(t *testing.T) {
		body := `{"type":`
		req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs", bytes.NewBufferString(body))
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Fatalf("expected 400 Bad Request, got %d", rec.Code)
		}
	})

	t.Run("oversized payload returns 413 Payload Too Large", func(t *testing.T) {
		largeData := strings.Repeat("x", 300*1024)
		body := `{"type":"big","payload":{"data":"` + largeData + `"}}`
		req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs", bytes.NewBufferString(body))
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusRequestEntityTooLarge {
			t.Fatalf("expected 413 Payload Too Large, got %d", rec.Code)
		}
	})

	t.Run("idempotent submission with header deduplicates", func(t *testing.T) {
		body := `{"type":"payment","payload":{"amount":100}}`

		// Request 1
		req1 := httptest.NewRequest(http.MethodPost, "/api/v1/jobs", bytes.NewBufferString(body))
		req1.Header.Set("Idempotency-Key", "tx-999")
		req1.Header.Set("X-Tenant-ID", "tenant-1")
		rec1 := httptest.NewRecorder()
		router.ServeHTTP(rec1, req1)

		if rec1.Code != http.StatusAccepted {
			t.Fatalf("expected 202 on first request, got %d", rec1.Code)
		}
		var job1 domain.Job
		_ = json.Unmarshal(rec1.Body.Bytes(), &job1)

		// Request 2 with same idempotency key and tenant
		req2 := httptest.NewRequest(http.MethodPost, "/api/v1/jobs", bytes.NewBufferString(body))
		req2.Header.Set("Idempotency-Key", "tx-999")
		req2.Header.Set("X-Tenant-ID", "tenant-1")
		rec2 := httptest.NewRecorder()
		router.ServeHTTP(rec2, req2)

		if rec2.Code != http.StatusAccepted {
			t.Fatalf("expected 202 on second request, got %d", rec2.Code)
		}
		var job2 domain.Job
		_ = json.Unmarshal(rec2.Body.Bytes(), &job2)

		if job1.ID != job2.ID {
			t.Fatalf("expected identical job ID %s, got %s", job1.ID, job2.ID)
		}

		// Request 3 with same key but different tenant creates new job
		req3 := httptest.NewRequest(http.MethodPost, "/api/v1/jobs", bytes.NewBufferString(body))
		req3.Header.Set("Idempotency-Key", "tx-999")
		req3.Header.Set("X-Tenant-ID", "tenant-2")
		rec3 := httptest.NewRecorder()
		router.ServeHTTP(rec3, req3)

		var job3 domain.Job
		_ = json.Unmarshal(rec3.Body.Bytes(), &job3)

		if job3.ID == job1.ID {
			t.Fatalf("expected different job for different tenant, got same ID")
		}
	})
}

func TestJobHandler_GetJob(t *testing.T) {
	_, router := setupTestApp()

	// Create job first
	body := `{"type":"check"}`
	createReq := httptest.NewRequest(http.MethodPost, "/api/v1/jobs", bytes.NewBufferString(body))
	createRec := httptest.NewRecorder()
	router.ServeHTTP(createRec, createReq)

	var created domain.Job
	_ = json.Unmarshal(createRec.Body.Bytes(), &created)

	t.Run("existing job returns 200 OK", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/"+created.ID.String(), nil)
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d", rec.Code)
		}

		var res JobResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}
		if res.ID != created.ID {
			t.Errorf("expected ID %s, got %s", created.ID, res.ID)
		}
	})

	t.Run("non-existent job returns 404 Not Found", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/"+uuid.New().String(), nil)
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusNotFound {
			t.Fatalf("expected 404 Not Found, got %d", rec.Code)
		}
	})

	t.Run("invalid UUID format returns 400 Bad Request", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/invalid-uuid", nil)
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Fatalf("expected 400 Bad Request, got %d", rec.Code)
		}
	})
}

func TestJobHandler_ListJobs(t *testing.T) {
	_, router := setupTestApp()

	// Seed 2 jobs
	req1 := httptest.NewRequest(http.MethodPost, "/api/v1/jobs", bytes.NewBufferString(`{"type":"job1","queue":"queue-a"}`))
	router.ServeHTTP(httptest.NewRecorder(), req1)

	req2 := httptest.NewRequest(http.MethodPost, "/api/v1/jobs", bytes.NewBufferString(`{"type":"job2","queue":"queue-b"}`))
	router.ServeHTTP(httptest.NewRecorder(), req2)

	t.Run("list all jobs", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs?limit=10&offset=0", nil)
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d", rec.Code)
		}

		var res ListJobsResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil {
			t.Fatalf("failed to decode list response: %v", err)
		}
		if res.Pagination.Total != 2 {
			t.Errorf("expected total 2, got %d", res.Pagination.Total)
		}
	})

	t.Run("filter by queue", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs?queue=queue-a", nil)
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		var res ListJobsResponse
		_ = json.Unmarshal(rec.Body.Bytes(), &res)
		if len(res.Data) != 1 || res.Data[0].Queue != "queue-a" {
			t.Fatalf("expected 1 job with queue-a, got %+v", res.Data)
		}
	})

	t.Run("invalid state filter returns 400 Bad Request", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs?state=non_existent_state", nil)
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Fatalf("expected 400 Bad Request, got %d", rec.Code)
		}
	})
}

func TestJobHandler_CancelAndRetry(t *testing.T) {
	repo, router := setupTestApp()

	// Create a job
	createReq := httptest.NewRequest(http.MethodPost, "/api/v1/jobs", bytes.NewBufferString(`{"type":"echo","queue":"default"}`))
	createRec := httptest.NewRecorder()
	router.ServeHTTP(createRec, createReq)

	var created domain.Job
	_ = json.Unmarshal(createRec.Body.Bytes(), &created)

	t.Run("cancel pending job returns 200 OK", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/"+created.ID.String()+"/cancel", nil)
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d", rec.Code)
		}

		var cancelled domain.Job
		_ = json.Unmarshal(rec.Body.Bytes(), &cancelled)
		if cancelled.State != domain.StateCancelled {
			t.Errorf("expected cancelled state, got %s", cancelled.State)
		}
	})

	t.Run("cancel already cancelled job returns 409 Conflict", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/"+created.ID.String()+"/cancel", nil)
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusConflict {
			t.Fatalf("expected 409 Conflict, got %d", rec.Code)
		}
	})

	t.Run("manual retry failed/dead job returns 200 OK", func(t *testing.T) {
		// Set job state to dead
		repo.jobs[created.ID].State = domain.StateDead

		req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/"+created.ID.String()+"/retry", nil)
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d", rec.Code)
		}

		var retried domain.Job
		_ = json.Unmarshal(rec.Body.Bytes(), &retried)
		if retried.State != domain.StatePending {
			t.Errorf("expected state pending, got %s", retried.State)
		}
	})
}

func TestJobHandler_GetQueueStats(t *testing.T) {
	_, router := setupTestApp()

	// Seed jobs into 'reports' queue
	router.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/api/v1/jobs", bytes.NewBufferString(`{"type":"r1","queue":"reports"}`)))
	router.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/api/v1/jobs", bytes.NewBufferString(`{"type":"r2","queue":"reports"}`)))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/queues/reports/stats", nil)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rec.Code)
	}

	var stats domain.QueueStats
	if err := json.Unmarshal(rec.Body.Bytes(), &stats); err != nil {
		t.Fatalf("failed to decode stats: %v", err)
	}

	if stats.Queue != "reports" {
		t.Errorf("expected queue reports, got %s", stats.Queue)
	}
	if stats.Total != 2 {
		t.Errorf("expected total 2, got %d", stats.Total)
	}
}

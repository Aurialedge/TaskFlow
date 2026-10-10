package service

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/Aurialedge/TaskFlow/internal/domain"
)

type mockJobRepo struct {
	createdJobs      map[uuid.UUID]*domain.Job
	idempotencyStore map[string]*domain.Job
	attempts         map[uuid.UUID][]*domain.JobAttempt
}

func newMockJobRepo() *mockJobRepo {
	return &mockJobRepo{
		createdJobs:      make(map[uuid.UUID]*domain.Job),
		idempotencyStore: make(map[string]*domain.Job),
		attempts:         make(map[uuid.UUID][]*domain.JobAttempt),
	}
}

func (m *mockJobRepo) CreateJob(ctx context.Context, job *domain.Job) (*domain.Job, bool, error) {
	if job.IdempotencyKey != nil {
		key := job.IdempotencyScope + ":" + *job.IdempotencyKey
		if existing, ok := m.idempotencyStore[key]; ok {
			return existing, false, nil
		}
		m.idempotencyStore[key] = job
	}
	m.createdJobs[job.ID] = job
	return job, true, nil
}

func (m *mockJobRepo) GetJobByID(ctx context.Context, id uuid.UUID) (*domain.Job, error) {
	if job, ok := m.createdJobs[id]; ok {
		return job, nil
	}
	return nil, domain.ErrJobNotFound
}

func (m *mockJobRepo) GetJobByIdempotency(ctx context.Context, scope, key string) (*domain.Job, error) {
	compositeKey := scope + ":" + key
	if job, ok := m.idempotencyStore[compositeKey]; ok {
		return job, nil
	}
	return nil, domain.ErrJobNotFound
}

func (m *mockJobRepo) ListJobs(ctx context.Context, filter domain.ListJobsFilter) ([]*domain.Job, int64, error) {
	var list []*domain.Job
	for _, j := range m.createdJobs {
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

func (m *mockJobRepo) GetAttempts(ctx context.Context, jobID uuid.UUID) ([]*domain.JobAttempt, error) {
	return m.attempts[jobID], nil
}

func (m *mockJobRepo) ClaimJobs(ctx context.Context, queue string, limit int, workerID string, leaseDuration time.Duration) ([]*domain.Job, error) {
	return nil, nil
}

func (m *mockJobRepo) RecordSuccess(ctx context.Context, jobID uuid.UUID, leaseToken uuid.UUID, result []byte) error {
	return nil
}

func (m *mockJobRepo) RecordFailure(ctx context.Context, jobID uuid.UUID, leaseToken uuid.UUID, errMsg string) error {
	return nil
}

func (m *mockJobRepo) RecordAttempt(ctx context.Context, attempt *domain.JobAttempt) error {
	return nil
}

func TestJobService_SubmitJob(t *testing.T) {
	ctx := context.Background()

	t.Run("successful submission with defaults", func(t *testing.T) {
		repo := newMockJobRepo()
		svc := NewJobService(repo)

		job, created, err := svc.SubmitJob(ctx, domain.CreateJobInput{
			Type:    "email.send",
			Payload: json.RawMessage(`{"to":"test@example.com"}`),
		})

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !created {
			t.Fatal("expected job to be newly created")
		}
		if job.Queue != "default" {
			t.Errorf("expected default queue 'default', got '%s'", job.Queue)
		}
		if job.MaxAttempts != 3 {
			t.Errorf("expected max attempts 3, got %d", job.MaxAttempts)
		}
		if job.TimeoutSeconds != 60 {
			t.Errorf("expected timeout 60s, got %d", job.TimeoutSeconds)
		}
		if job.State != domain.StatePending {
			t.Errorf("expected pending state, got %s", job.State)
		}
	})

	t.Run("scheduled future execution", func(t *testing.T) {
		repo := newMockJobRepo()
		svc := NewJobService(repo)

		futureTime := time.Now().UTC().Add(10 * time.Minute)
		job, _, err := svc.SubmitJob(ctx, domain.CreateJobInput{
			Type:  "report.generate",
			RunAt: &futureTime,
		})

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if job.State != domain.StateScheduled {
			t.Errorf("expected state scheduled for future job, got %s", job.State)
		}
	})

	t.Run("missing type is rejected", func(t *testing.T) {
		repo := newMockJobRepo()
		svc := NewJobService(repo)

		_, _, err := svc.SubmitJob(ctx, domain.CreateJobInput{
			Type: "",
		})

		if !errors.Is(err, domain.ErrInvalidJobInput) {
			t.Fatalf("expected ErrInvalidJobInput, got %v", err)
		}
	})

	t.Run("invalid json payload is rejected", func(t *testing.T) {
		repo := newMockJobRepo()
		svc := NewJobService(repo)

		_, _, err := svc.SubmitJob(ctx, domain.CreateJobInput{
			Type:    "test.type",
			Payload: json.RawMessage(`{not-valid-json`),
		})

		if !errors.Is(err, domain.ErrInvalidJobInput) {
			t.Fatalf("expected ErrInvalidJobInput, got %v", err)
		}
	})

	t.Run("payload exceeding size limit is rejected", func(t *testing.T) {
		repo := newMockJobRepo()
		svc := NewJobService(repo)

		largePayload := `{"data":"` + strings.Repeat("A", 300*1024) + `"}`
		_, _, err := svc.SubmitJob(ctx, domain.CreateJobInput{
			Type:    "test.type",
			Payload: json.RawMessage(largePayload),
		})

		if !errors.Is(err, domain.ErrPayloadTooLarge) {
			t.Fatalf("expected ErrPayloadTooLarge, got %v", err)
		}
	})

	t.Run("idempotent submission returns existing job", func(t *testing.T) {
		repo := newMockJobRepo()
		svc := NewJobService(repo)

		key := "order-12345"
		input := domain.CreateJobInput{
			Type:           "order.process",
			IdempotencyKey: &key,
		}

		job1, created1, err := svc.SubmitJob(ctx, input)
		if err != nil || !created1 {
			t.Fatalf("job 1 submission failed: err=%v, created=%v", err, created1)
		}

		job2, created2, err := svc.SubmitJob(ctx, input)
		if err != nil {
			t.Fatalf("job 2 submission failed: %v", err)
		}
		if created2 {
			t.Fatal("expected second submission with same idempotency key to NOT be created")
		}
		if job1.ID != job2.ID {
			t.Errorf("expected same job ID %s, got %s", job1.ID, job2.ID)
		}
	})
}

func TestJobService_GetJob(t *testing.T) {
	repo := newMockJobRepo()
	svc := NewJobService(repo)
	ctx := context.Background()

	// Not found
	_, _, err := svc.GetJob(ctx, uuid.New())
	if !errors.Is(err, domain.ErrJobNotFound) {
		t.Fatalf("expected ErrJobNotFound, got %v", err)
	}

	// Found
	job, _, _ := svc.SubmitJob(ctx, domain.CreateJobInput{Type: "echo"})
	foundJob, _, err := svc.GetJob(ctx, job.ID)
	if err != nil {
		t.Fatalf("expected job to be found, got %v", err)
	}
	if foundJob.ID != job.ID {
		t.Errorf("expected ID %s, got %s", job.ID, foundJob.ID)
	}
}

func TestJobService_ListJobs(t *testing.T) {
	repo := newMockJobRepo()
	svc := NewJobService(repo)
	ctx := context.Background()

	_, _, _ = svc.SubmitJob(ctx, domain.CreateJobInput{Type: "echo", Queue: "emails"})
	_, _, _ = svc.SubmitJob(ctx, domain.CreateJobInput{Type: "reports", Queue: "reports"})

	jobs, count, err := svc.ListJobs(ctx, domain.ListJobsFilter{Queue: "emails"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count != 1 || len(jobs) != 1 {
		t.Fatalf("expected 1 job, got %d", count)
	}
	if jobs[0].Queue != "emails" {
		t.Errorf("expected queue emails, got %s", jobs[0].Queue)
	}
}

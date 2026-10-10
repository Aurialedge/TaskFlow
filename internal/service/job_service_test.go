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

func (m *mockJobRepo) RecordOutcome(ctx context.Context, jobID uuid.UUID, leaseToken uuid.UUID, leaseGen int64, result []byte, execErr error, backoff time.Duration, isPermanent bool) (domain.State, error) {
	return domain.StateSucceeded, nil
}

func (m *mockJobRepo) RenewLease(ctx context.Context, jobID uuid.UUID, leaseToken uuid.UUID, leaseGen int64, duration time.Duration) error {
	return nil
}

func (m *mockJobRepo) RecoverAbandonedJobs(ctx context.Context, defaultLeaseDuration time.Duration) (int64, error) {
	return 0, nil
}

func (m *mockJobRepo) CancelJob(ctx context.Context, jobID uuid.UUID) (*domain.Job, error) {
	job, ok := m.createdJobs[jobID]
	if !ok {
		return nil, domain.ErrJobNotFound
	}
	if job.State == domain.StateSucceeded || job.State == domain.StateCancelled {
		return nil, domain.ErrInvalidStateTransition
	}
	job.State = domain.StateCancelled
	return job, nil
}

func (m *mockJobRepo) ManualRetryJob(ctx context.Context, jobID uuid.UUID) (*domain.Job, error) {
	job, ok := m.createdJobs[jobID]
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

func (m *mockJobRepo) QueueStats(ctx context.Context, queue string) (*domain.QueueStats, error) {
	stats := &domain.QueueStats{Queue: queue}
	isAll := queue == "" || queue == "all"
	for _, j := range m.createdJobs {
		if isAll || j.Queue == queue {
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

func (m *mockJobRepo) DeleteJob(ctx context.Context, id uuid.UUID) error {
	if _, ok := m.createdJobs[id]; !ok {
		return domain.ErrJobNotFound
	}
	delete(m.createdJobs, id)
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

func TestJobService_CancelJob(t *testing.T) {
	repo := newMockJobRepo()
	svc := NewJobService(repo)
	ctx := context.Background()

	job, _, _ := svc.SubmitJob(ctx, domain.CreateJobInput{Type: "echo"})

	cancelled, err := svc.CancelJob(ctx, job.ID)
	if err != nil {
		t.Fatalf("unexpected cancel error: %v", err)
	}
	if cancelled.State != domain.StateCancelled {
		t.Errorf("expected cancelled state, got %s", cancelled.State)
	}

	// Double cancel returns error
	_, err = svc.CancelJob(ctx, job.ID)
	if !errors.Is(err, domain.ErrInvalidStateTransition) {
		t.Errorf("expected ErrInvalidStateTransition on already cancelled job, got %v", err)
	}
}

func TestJobService_ManualRetryJob(t *testing.T) {
	repo := newMockJobRepo()
	svc := NewJobService(repo)
	ctx := context.Background()

	job, _, _ := svc.SubmitJob(ctx, domain.CreateJobInput{Type: "echo"})
	repo.createdJobs[job.ID].State = domain.StateDead

	retried, err := svc.ManualRetryJob(ctx, job.ID)
	if err != nil {
		t.Fatalf("unexpected retry error: %v", err)
	}
	if retried.State != domain.StatePending {
		t.Errorf("expected pending state after retry, got %s", retried.State)
	}
}

func TestJobService_GetQueueStats(t *testing.T) {
	repo := newMockJobRepo()
	svc := NewJobService(repo)
	ctx := context.Background()

	_, _, _ = svc.SubmitJob(ctx, domain.CreateJobInput{Type: "echo", Queue: "notifications"})
	_, _, _ = svc.SubmitJob(ctx, domain.CreateJobInput{Type: "echo", Queue: "notifications"})

	stats, err := svc.GetQueueStats(ctx, "notifications")
	if err != nil {
		t.Fatalf("unexpected stats error: %v", err)
	}
	if stats.Total != 2 {
		t.Errorf("expected total 2, got %d", stats.Total)
	}
	if stats.Pending != 2 {
		t.Errorf("expected pending 2, got %d", stats.Pending)
	}

	// Test all queues stats
	allStats, err := svc.GetQueueStats(ctx, "all")
	if err != nil {
		t.Fatalf("unexpected all stats error: %v", err)
	}
	if allStats.Total != 2 {
		t.Errorf("expected all stats total 2, got %d", allStats.Total)
	}
}

func TestJobService_DeleteJob(t *testing.T) {
	repo := newMockJobRepo()
	svc := NewJobService(repo)
	ctx := context.Background()

	job, _, err := svc.SubmitJob(ctx, domain.CreateJobInput{Type: "echo", Queue: "default"})
	if err != nil {
		t.Fatalf("failed to submit job: %v", err)
	}

	if err := svc.DeleteJob(ctx, job.ID); err != nil {
		t.Fatalf("failed to delete job: %v", err)
	}

	// Attempting to delete again should return not found
	if err := svc.DeleteJob(ctx, job.ID); !errors.Is(err, domain.ErrJobNotFound) {
		t.Fatalf("expected ErrJobNotFound on re-delete, got %v", err)
	}
}

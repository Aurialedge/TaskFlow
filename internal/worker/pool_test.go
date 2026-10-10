package worker

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/Aurialedge/TaskFlow/internal/domain"
)

type mockRepoForWorker struct {
	mu           sync.Mutex
	successCalls map[uuid.UUID][]byte
	failureCalls map[uuid.UUID]string
	attempts     []*domain.JobAttempt
	claimedJobs  []*domain.Job
}

func newMockRepoForWorker() *mockRepoForWorker {
	return &mockRepoForWorker{
		successCalls: make(map[uuid.UUID][]byte),
		failureCalls: make(map[uuid.UUID]string),
	}
}

func (m *mockRepoForWorker) CreateJob(ctx context.Context, job *domain.Job) (*domain.Job, bool, error) {
	return job, true, nil
}
func (m *mockRepoForWorker) GetJobByID(ctx context.Context, id uuid.UUID) (*domain.Job, error) {
	return nil, nil
}
func (m *mockRepoForWorker) GetJobByIdempotency(ctx context.Context, scope, key string) (*domain.Job, error) {
	return nil, nil
}
func (m *mockRepoForWorker) ListJobs(ctx context.Context, filter domain.ListJobsFilter) ([]*domain.Job, int64, error) {
	return nil, 0, nil
}
func (m *mockRepoForWorker) GetAttempts(ctx context.Context, jobID uuid.UUID) ([]*domain.JobAttempt, error) {
	return nil, nil
}

func (m *mockRepoForWorker) ClaimJobs(ctx context.Context, queue string, limit int, workerID string, leaseDuration time.Duration) ([]*domain.Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.claimedJobs) == 0 {
		return nil, nil
	}

	count := limit
	if count > len(m.claimedJobs) {
		count = len(m.claimedJobs)
	}

	batch := m.claimedJobs[:count]
	m.claimedJobs = m.claimedJobs[count:]
	return batch, nil
}

func (m *mockRepoForWorker) RecordSuccess(ctx context.Context, jobID uuid.UUID, leaseToken uuid.UUID, result []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.successCalls[jobID] = result
	return nil
}

func (m *mockRepoForWorker) RecordFailure(ctx context.Context, jobID uuid.UUID, leaseToken uuid.UUID, errMsg string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failureCalls[jobID] = errMsg
	return nil
}

func (m *mockRepoForWorker) RecordAttempt(ctx context.Context, attempt *domain.JobAttempt) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.attempts = append(m.attempts, attempt)
	return nil
}

func (m *mockRepoForWorker) RecordOutcome(ctx context.Context, jobID uuid.UUID, leaseToken uuid.UUID, leaseGen int64, result []byte, execErr error, backoff time.Duration, isPermanent bool) (domain.State, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if execErr == nil {
		m.successCalls[jobID] = result
		return domain.StateSucceeded, nil
	}
	m.failureCalls[jobID] = execErr.Error()
	if isPermanent {
		return domain.StateDead, nil
	}
	return domain.StateScheduled, nil
}

func (m *mockRepoForWorker) RenewLease(ctx context.Context, jobID uuid.UUID, leaseToken uuid.UUID, leaseGen int64, duration time.Duration) error {
	return nil
}

func (m *mockRepoForWorker) RecoverAbandonedJobs(ctx context.Context, defaultLeaseDuration time.Duration) (int64, error) {
	return 0, nil
}

func (m *mockRepoForWorker) CancelJob(ctx context.Context, jobID uuid.UUID) (*domain.Job, error) {
	return nil, nil
}

func (m *mockRepoForWorker) ManualRetryJob(ctx context.Context, jobID uuid.UUID) (*domain.Job, error) {
	return nil, nil
}

func (m *mockRepoForWorker) QueueStats(ctx context.Context, queue string) (*domain.QueueStats, error) {
	return nil, nil
}

func (m *mockRepoForWorker) DeleteJob(ctx context.Context, id uuid.UUID) error {
	return nil
}

func TestPool_SuccessfulExecution(t *testing.T) {
	repo := newMockRepoForWorker()
	registry := NewDefaultRegistry()
	pool := NewPool(2, "test-worker-1", repo, registry)
	pool.Start()
	defer pool.Stop()

	leaseToken := uuid.New()
	job := &domain.Job{
		ID:             uuid.New(),
		Type:           "echo",
		Payload:        json.RawMessage(`{"greeting":"hello world"}`),
		State:          domain.StateRunning,
		AttemptCount:   1,
		TimeoutSeconds: 5,
		LeaseToken:     &leaseToken,
	}

	submitted := pool.Submit(job)
	if !submitted {
		t.Fatal("expected job to be submitted to pool")
	}

	// Wait for processing
	time.Sleep(100 * time.Millisecond)

	repo.mu.Lock()
	defer repo.mu.Unlock()

	res, ok := repo.successCalls[job.ID]
	if !ok {
		t.Fatalf("expected job %s to have recorded success", job.ID)
	}
	if string(res) != string(job.Payload) {
		t.Errorf("expected result %s, got %s", job.Payload, res)
	}
	if len(repo.attempts) != 1 {
		t.Fatalf("expected 1 recorded attempt, got %d", len(repo.attempts))
	}
	if repo.attempts[0].State != domain.StateSucceeded {
		t.Errorf("expected attempt state succeeded, got %s", repo.attempts[0].State)
	}
}

func TestPool_FailedExecution(t *testing.T) {
	repo := newMockRepoForWorker()
	registry := NewDefaultRegistry()
	pool := NewPool(2, "test-worker-2", repo, registry)
	pool.Start()
	defer pool.Stop()

	leaseToken := uuid.New()
	job := &domain.Job{
		ID:             uuid.New(),
		Type:           "test.fail",
		Payload:        json.RawMessage(`{"message":"database offline"}`),
		State:          domain.StateRunning,
		AttemptCount:   1,
		TimeoutSeconds: 5,
		LeaseToken:     &leaseToken,
	}

	pool.Submit(job)
	time.Sleep(100 * time.Millisecond)

	repo.mu.Lock()
	defer repo.mu.Unlock()

	errMsg, ok := repo.failureCalls[job.ID]
	if !ok {
		t.Fatalf("expected failure to be recorded for job %s", job.ID)
	}
	if errMsg == "" {
		t.Error("expected non-empty error message")
	}
	if len(repo.attempts) != 1 || repo.attempts[0].State != domain.StateFailed {
		t.Errorf("expected recorded failed attempt, got %+v", repo.attempts)
	}
}

func TestPool_PanicRecovery(t *testing.T) {
	repo := newMockRepoForWorker()
	registry := NewRegistry()
	_ = registry.Register("panic.task", func(ctx context.Context, payload json.RawMessage) (json.RawMessage, error) {
		panic("catastrophic failure inside handler")
	})

	pool := NewPool(2, "test-worker-panic", repo, registry)
	pool.Start()
	defer pool.Stop()

	leaseToken := uuid.New()
	job := &domain.Job{
		ID:             uuid.New(),
		Type:           "panic.task",
		State:          domain.StateRunning,
		AttemptCount:   1,
		TimeoutSeconds: 5,
		LeaseToken:     &leaseToken,
	}

	pool.Submit(job)
	time.Sleep(100 * time.Millisecond)

	repo.mu.Lock()
	defer repo.mu.Unlock()

	errMsg, ok := repo.failureCalls[job.ID]
	if !ok {
		t.Fatalf("expected panic to be safely caught and recorded as failure")
	}
	if errMsg == "" {
		t.Error("expected error message for panicked job")
	}
}

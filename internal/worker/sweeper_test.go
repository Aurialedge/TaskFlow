package worker

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/Aurialedge/TaskFlow/internal/domain"
)

type mockRepoForSweeper struct {
	mu            sync.Mutex
	recoverCalls  int
	recoveredJobs int64
}

func (m *mockRepoForSweeper) CreateJob(ctx context.Context, job *domain.Job) (*domain.Job, bool, error) {
	return job, true, nil
}
func (m *mockRepoForSweeper) GetJobByID(ctx context.Context, id uuid.UUID) (*domain.Job, error) {
	return nil, nil
}
func (m *mockRepoForSweeper) GetJobByIdempotency(ctx context.Context, scope, key string) (*domain.Job, error) {
	return nil, nil
}
func (m *mockRepoForSweeper) ListJobs(ctx context.Context, filter domain.ListJobsFilter) ([]*domain.Job, int64, error) {
	return nil, 0, nil
}
func (m *mockRepoForSweeper) GetAttempts(ctx context.Context, jobID uuid.UUID) ([]*domain.JobAttempt, error) {
	return nil, nil
}
func (m *mockRepoForSweeper) ClaimJobs(ctx context.Context, queue string, limit int, workerID string, leaseDuration time.Duration) ([]*domain.Job, error) {
	return nil, nil
}
func (m *mockRepoForSweeper) RecordSuccess(ctx context.Context, jobID uuid.UUID, leaseToken uuid.UUID, result []byte) error {
	return nil
}
func (m *mockRepoForSweeper) RecordFailure(ctx context.Context, jobID uuid.UUID, leaseToken uuid.UUID, errMsg string) error {
	return nil
}
func (m *mockRepoForSweeper) RecordAttempt(ctx context.Context, attempt *domain.JobAttempt) error {
	return nil
}
func (m *mockRepoForSweeper) RecordOutcome(ctx context.Context, jobID uuid.UUID, leaseToken uuid.UUID, leaseGen int64, result []byte, execErr error, backoff time.Duration, isPermanent bool) (domain.State, error) {
	return domain.StateSucceeded, nil
}
func (m *mockRepoForSweeper) RenewLease(ctx context.Context, jobID uuid.UUID, leaseToken uuid.UUID, leaseGen int64, duration time.Duration) error {
	return nil
}
func (m *mockRepoForSweeper) CancelJob(ctx context.Context, jobID uuid.UUID) (*domain.Job, error) {
	return nil, nil
}
func (m *mockRepoForSweeper) ManualRetryJob(ctx context.Context, jobID uuid.UUID) (*domain.Job, error) {
	return nil, nil
}
func (m *mockRepoForSweeper) QueueStats(ctx context.Context, queue string) (*domain.QueueStats, error) {
	return nil, nil
}

func (m *mockRepoForSweeper) RecoverAbandonedJobs(ctx context.Context, defaultLeaseDuration time.Duration) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.recoverCalls++
	return m.recoveredJobs, nil
}

func TestSweeper_Execution(t *testing.T) {
	repo := &mockRepoForSweeper{recoveredJobs: 3}
	sweeper := NewSweeper(repo, 20*time.Millisecond, 10*time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	go sweeper.Start(ctx)

	time.Sleep(60 * time.Millisecond)
	cancel()
	sweeper.Stop()

	repo.mu.Lock()
	defer repo.mu.Unlock()

	if repo.recoverCalls < 1 {
		t.Fatalf("expected sweeper to invoke RecoverAbandonedJobs, called %d times", repo.recoverCalls)
	}
}

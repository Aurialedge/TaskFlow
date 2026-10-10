package load

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/Aurialedge/TaskFlow/internal/domain"
	"github.com/Aurialedge/TaskFlow/internal/retry"
	"github.com/Aurialedge/TaskFlow/internal/service"
	"github.com/Aurialedge/TaskFlow/internal/worker"
)

func init() {
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
}

type benchMockRepo struct{}

func (b *benchMockRepo) CreateJob(ctx context.Context, job *domain.Job) (*domain.Job, bool, error) {
	return job, true, nil
}
func (b *benchMockRepo) GetJobByID(ctx context.Context, id uuid.UUID) (*domain.Job, error) {
	return nil, nil
}
func (b *benchMockRepo) GetJobByIdempotency(ctx context.Context, scope, key string) (*domain.Job, error) {
	return nil, nil
}
func (b *benchMockRepo) ListJobs(ctx context.Context, filter domain.ListJobsFilter) ([]*domain.Job, int64, error) {
	return nil, 0, nil
}
func (b *benchMockRepo) GetAttempts(ctx context.Context, jobID uuid.UUID) ([]*domain.JobAttempt, error) {
	return nil, nil
}
func (b *benchMockRepo) ClaimJobs(ctx context.Context, queue string, limit int, workerID string, leaseDuration time.Duration) ([]*domain.Job, error) {
	return nil, nil
}
func (b *benchMockRepo) RecordSuccess(ctx context.Context, jobID uuid.UUID, leaseToken uuid.UUID, result []byte) error {
	return nil
}
func (b *benchMockRepo) RecordFailure(ctx context.Context, jobID uuid.UUID, leaseToken uuid.UUID, errMsg string) error {
	return nil
}
func (b *benchMockRepo) RecordAttempt(ctx context.Context, attempt *domain.JobAttempt) error {
	return nil
}
func (b *benchMockRepo) RecordOutcome(ctx context.Context, jobID uuid.UUID, leaseToken uuid.UUID, leaseGen int64, result []byte, execErr error, backoff time.Duration, isPermanent bool) (domain.State, error) {
	return domain.StateSucceeded, nil
}
func (b *benchMockRepo) RenewLease(ctx context.Context, jobID uuid.UUID, leaseToken uuid.UUID, leaseGen int64, duration time.Duration) error {
	return nil
}
func (b *benchMockRepo) RecoverAbandonedJobs(ctx context.Context, defaultLeaseDuration time.Duration) (int64, error) {
	return 0, nil
}
func (b *benchMockRepo) CancelJob(ctx context.Context, jobID uuid.UUID) (*domain.Job, error) {
	return nil, nil
}
func (b *benchMockRepo) ManualRetryJob(ctx context.Context, jobID uuid.UUID) (*domain.Job, error) {
	return nil, nil
}
func (b *benchMockRepo) QueueStats(ctx context.Context, queue string) (*domain.QueueStats, error) {
	return &domain.QueueStats{Queue: queue}, nil
}

func BenchmarkJobSubmission(b *testing.B) {
	repo := &benchMockRepo{}
	svc := service.NewJobService(repo)
	ctx := context.Background()
	payload := json.RawMessage(`{"user_id":"u-99","action":"test"}`)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := svc.SubmitJob(ctx, domain.CreateJobInput{
			Queue:   "default",
			Type:    "bench.job",
			Payload: payload,
		})
		if err != nil {
			b.Fatalf("submit job failed: %v", err)
		}
	}
}

func BenchmarkBackoffCalculation(b *testing.B) {
	bm := retry.NewBackoffManager(1*time.Second, 60*time.Second)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = bm.Calculate((i % 10) + 1)
	}
}

func BenchmarkHandlerRegistryLookup(b *testing.B) {
	reg := worker.NewDefaultRegistry()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = reg.Get("echo")
	}
}

func BenchmarkWorkerPoolThroughput(b *testing.B) {
	repo := &benchMockRepo{}
	reg := worker.NewDefaultRegistry()
	pool := worker.NewPool(8, "bench-worker", repo, reg)
	pool.Start()
	defer pool.Stop()

	leaseToken := uuid.New()
	job := &domain.Job{
		ID:             uuid.New(),
		Type:           "echo",
		Payload:        json.RawMessage(`{}`),
		State:          domain.StateRunning,
		TimeoutSeconds: 5,
		LeaseToken:     &leaseToken,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for !pool.Submit(job) {
			time.Sleep(1 * time.Millisecond)
		}
	}
}

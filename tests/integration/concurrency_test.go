package integration

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/Aurialedge/TaskFlow/internal/domain"
	"github.com/Aurialedge/TaskFlow/internal/worker"
)

type threadSafeMockRepo struct {
	mu           sync.Mutex
	jobs         map[uuid.UUID]*domain.Job
	idempotency  map[string]*domain.Job
	attempts     []*domain.JobAttempt
	successCalls map[uuid.UUID][]byte
}

func newThreadSafeMockRepo() *threadSafeMockRepo {
	return &threadSafeMockRepo{
		jobs:         make(map[uuid.UUID]*domain.Job),
		idempotency:  make(map[string]*domain.Job),
		successCalls: make(map[uuid.UUID][]byte),
	}
}

func (r *threadSafeMockRepo) CreateJob(ctx context.Context, job *domain.Job) (*domain.Job, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if job.IdempotencyKey != nil {
		compositeKey := job.IdempotencyScope + ":" + *job.IdempotencyKey
		if existing, ok := r.idempotency[compositeKey]; ok {
			return existing, false, nil
		}
		r.idempotency[compositeKey] = job
	}

	r.jobs[job.ID] = job
	return job, true, nil
}

func (r *threadSafeMockRepo) GetJobByID(ctx context.Context, id uuid.UUID) (*domain.Job, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if j, ok := r.jobs[id]; ok {
		return j, nil
	}
	return nil, domain.ErrJobNotFound
}

func (r *threadSafeMockRepo) GetJobByIdempotency(ctx context.Context, scope, key string) (*domain.Job, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if j, ok := r.idempotency[scope+":"+key]; ok {
		return j, nil
	}
	return nil, domain.ErrJobNotFound
}

func (r *threadSafeMockRepo) ListJobs(ctx context.Context, filter domain.ListJobsFilter) ([]*domain.Job, int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var res []*domain.Job
	for _, j := range r.jobs {
		res = append(res, j)
	}
	return res, int64(len(res)), nil
}

func (r *threadSafeMockRepo) GetAttempts(ctx context.Context, jobID uuid.UUID) ([]*domain.JobAttempt, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var res []*domain.JobAttempt
	for _, a := range r.attempts {
		if a.JobID == jobID {
			res = append(res, a)
		}
	}
	return res, nil
}

func (r *threadSafeMockRepo) ClaimJobs(ctx context.Context, queue string, limit int, workerID string, leaseDuration time.Duration) ([]*domain.Job, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now().UTC()
	expiresAt := now.Add(leaseDuration)

	var claimed []*domain.Job
	for _, j := range r.jobs {
		if (queue == "" || j.Queue == queue) && (j.State == domain.StatePending || j.State == domain.StateScheduled) && !j.RunAt.After(now) {
			token := uuid.New()
			j.LeaseToken = &token
			j.LeaseExpiresAt = &expiresAt
			j.LeaseGeneration++
			j.AttemptCount++
			j.State = domain.StateRunning
			j.UpdatedAt = now

			claimed = append(claimed, j)
			if len(claimed) >= limit {
				break
			}
		}
	}
	return claimed, nil
}

func (r *threadSafeMockRepo) RecordSuccess(ctx context.Context, jobID uuid.UUID, leaseToken uuid.UUID, result []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	j, ok := r.jobs[jobID]
	if !ok || j.LeaseToken == nil || *j.LeaseToken != leaseToken {
		return domain.ErrInvalidStateTransition
	}
	j.State = domain.StateSucceeded
	j.Result = result
	j.LeaseToken = nil
	r.successCalls[jobID] = result
	return nil
}

func (r *threadSafeMockRepo) RecordFailure(ctx context.Context, jobID uuid.UUID, leaseToken uuid.UUID, errMsg string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	j, ok := r.jobs[jobID]
	if !ok || j.LeaseToken == nil || *j.LeaseToken != leaseToken {
		return domain.ErrInvalidStateTransition
	}
	j.State = domain.StateFailed
	j.LastError = &errMsg
	j.LeaseToken = nil
	return nil
}

func (r *threadSafeMockRepo) RecordAttempt(ctx context.Context, attempt *domain.JobAttempt) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.attempts = append(r.attempts, attempt)
	return nil
}

func (r *threadSafeMockRepo) RecordOutcome(ctx context.Context, jobID uuid.UUID, leaseToken uuid.UUID, leaseGen int64, result []byte, execErr error, backoff time.Duration, isPermanent bool) (domain.State, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	j, ok := r.jobs[jobID]
	if !ok || j.LeaseToken == nil || *j.LeaseToken != leaseToken || j.LeaseGeneration != leaseGen {
		return "", domain.ErrInvalidStateTransition
	}

	if execErr == nil {
		j.State = domain.StateSucceeded
		j.Result = result
		j.LeaseToken = nil
		r.successCalls[jobID] = result
		return domain.StateSucceeded, nil
	}

	if !isPermanent && j.AttemptCount < j.MaxAttempts {
		j.State = domain.StateScheduled
		j.RunAt = time.Now().UTC().Add(backoff)
		j.LeaseToken = nil
		return domain.StateScheduled, nil
	}

	j.State = domain.StateDead
	j.LeaseToken = nil
	return domain.StateDead, nil
}

func (r *threadSafeMockRepo) RenewLease(ctx context.Context, jobID uuid.UUID, leaseToken uuid.UUID, leaseGen int64, duration time.Duration) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	j, ok := r.jobs[jobID]
	if !ok || j.LeaseToken == nil || *j.LeaseToken != leaseToken || j.LeaseGeneration != leaseGen {
		return domain.ErrInvalidStateTransition
	}
	newExp := time.Now().UTC().Add(duration)
	j.LeaseExpiresAt = &newExp
	return nil
}

func (r *threadSafeMockRepo) RecoverAbandonedJobs(ctx context.Context, defaultLeaseDuration time.Duration) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now().UTC()
	var count int64
	for _, j := range r.jobs {
		if j.State == domain.StateRunning && j.LeaseExpiresAt != nil && !j.LeaseExpiresAt.After(now) {
			count++
			if j.AttemptCount < j.MaxAttempts {
				j.State = domain.StatePending
				j.LeaseToken = nil
				j.LeaseExpiresAt = nil
			} else {
				j.State = domain.StateDead
				j.LeaseToken = nil
				j.LeaseExpiresAt = nil
			}
		}
	}
	return count, nil
}

func (r *threadSafeMockRepo) CancelJob(ctx context.Context, jobID uuid.UUID) (*domain.Job, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	j, ok := r.jobs[jobID]
	if !ok {
		return nil, domain.ErrJobNotFound
	}
	j.State = domain.StateCancelled
	return j, nil
}

func (r *threadSafeMockRepo) ManualRetryJob(ctx context.Context, jobID uuid.UUID) (*domain.Job, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	j, ok := r.jobs[jobID]
	if !ok {
		return nil, domain.ErrJobNotFound
	}
	j.State = domain.StatePending
	j.AttemptCount = 0
	return j, nil
}

func (r *threadSafeMockRepo) QueueStats(ctx context.Context, queue string) (*domain.QueueStats, error) {
	return &domain.QueueStats{Queue: queue}, nil
}

func (r *threadSafeMockRepo) DeleteJob(ctx context.Context, id uuid.UUID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.jobs, id)
	return nil
}

func TestIntegration_MultiWorkerConcurrency(t *testing.T) {
	repo := newThreadSafeMockRepo()
	registry := worker.NewDefaultRegistry()

	var executedCount int64
	_ = registry.Register("counter.job", func(ctx context.Context, payload json.RawMessage) (json.RawMessage, error) {
		atomic.AddInt64(&executedCount, 1)
		return json.RawMessage(`{"status":"counted"}`), nil
	})

	// Seed 30 jobs
	totalJobs := 30
	for i := 0; i < totalJobs; i++ {
		job := &domain.Job{
			ID:          uuid.New(),
			Queue:       "shared",
			Type:        "counter.job",
			State:       domain.StatePending,
			MaxAttempts: 3,
			RunAt:       time.Now().UTC(),
		}
		repo.jobs[job.ID] = job
	}

	// Launch 3 independent worker pools competing for the shared queue
	workerCount := 3
	var pools []*worker.Pool
	var dispatchers []*worker.Dispatcher

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for i := 0; i < workerCount; i++ {
		workerID := uuid.New().String()
		p := worker.NewPool(4, workerID, repo, registry)
		p.Start()
		pools = append(pools, p)

		d := worker.NewDispatcher(repo, p, "shared", 15*time.Millisecond, 5*time.Second, workerID)
		dispatchers = append(dispatchers, d)
		go d.Start(ctx)
	}

	// Wait for workers to process jobs
	time.Sleep(300 * time.Millisecond)

	for _, d := range dispatchers {
		d.Stop()
	}
	for _, p := range pools {
		p.Stop()
	}

	if atomic.LoadInt64(&executedCount) != int64(totalJobs) {
		t.Fatalf("expected all %d jobs to be executed once, got %d", totalJobs, executedCount)
	}
}

func TestIntegration_FencingTokenRejection(t *testing.T) {
	repo := newThreadSafeMockRepo()
	jobID := uuid.New()
	initialToken := uuid.New()
	expiresAt := time.Now().UTC().Add(-1 * time.Minute) // already expired

	job := &domain.Job{
		ID:              jobID,
		Queue:           "default",
		Type:            "echo",
		State:           domain.StateRunning,
		AttemptCount:    1,
		MaxAttempts:     3,
		LeaseToken:      &initialToken,
		LeaseGeneration: 1,
		LeaseExpiresAt:  &expiresAt,
	}
	repo.jobs[jobID] = job

	// Second worker claims expired job -> increments LeaseGeneration to 2 with new token
	reclaimedJobs, err := repo.ClaimJobs(context.Background(), "default", 1, "worker-2", 30*time.Second)
	if err != nil || len(reclaimedJobs) != 1 {
		// Sweeper recovers it first
		recovered, _ := repo.RecoverAbandonedJobs(context.Background(), 30*time.Second)
		if recovered != 1 {
			t.Fatalf("expected job to be recovered by sweeper")
		}
		reclaimedJobs, _ = repo.ClaimJobs(context.Background(), "default", 1, "worker-2", 30*time.Second)
	}

	activeJob := reclaimedJobs[0]
	if activeJob.LeaseGeneration <= 1 {
		t.Fatalf("expected lease generation to advance past 1, got %d", activeJob.LeaseGeneration)
	}

	// Stale worker tries to record outcome using old token and old generation (Gen 1)
	_, staleErr := repo.RecordOutcome(
		context.Background(),
		jobID,
		initialToken,
		1, // Stale generation
		[]byte(`{}`),
		nil,
		0,
		false,
	)

	if staleErr == nil {
		t.Fatal("expected stale worker with old generation to be rejected by fencing token check")
	}

	// Active worker records outcome with valid token and generation -> succeeds
	_, validErr := repo.RecordOutcome(
		context.Background(),
		jobID,
		*activeJob.LeaseToken,
		activeJob.LeaseGeneration,
		[]byte(`{"result":"ok"}`),
		nil,
		0,
		false,
	)

	if validErr != nil {
		t.Fatalf("expected active worker to record outcome successfully: %v", validErr)
	}
}

func TestIntegration_ConcurrentIdempotentSubmissions(t *testing.T) {
	repo := newThreadSafeMockRepo()
	key := "shared-idempotency-key"
	scope := "tenant-alpha"

	concurrency := 10
	var wg sync.WaitGroup
	wg.Add(concurrency)

	var successCount int64
	var distinctJobIDs sync.Map

	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			j := &domain.Job{
				ID:               uuid.New(),
				Queue:            "default",
				Type:             "payment.charge",
				IdempotencyKey:   &key,
				IdempotencyScope: scope,
				State:            domain.StatePending,
			}
			job, created, err := repo.CreateJob(context.Background(), j)
			if err == nil {
				atomic.AddInt64(&successCount, 1)
				distinctJobIDs.Store(job.ID.String(), true)
			}
			_ = created
		}()
	}

	wg.Wait()

	if successCount != int64(concurrency) {
		t.Fatalf("expected all %d submissions to succeed, got %d", concurrency, successCount)
	}

	countDistinct := 0
	distinctJobIDs.Range(func(key, value interface{}) bool {
		countDistinct++
		return true
	})

	if countDistinct != 1 {
		t.Fatalf("expected exactly 1 distinct job persisted for same idempotency key, got %d", countDistinct)
	}
}

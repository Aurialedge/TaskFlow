package worker

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/Aurialedge/TaskFlow/internal/domain"
)

func TestDispatcher_PollingAndExecution(t *testing.T) {
	repo := newMockRepoForWorker()
	registry := NewDefaultRegistry()
	pool := NewPool(2, "disp-test-worker", repo, registry)
	pool.Start()
	defer pool.Stop()

	leaseToken := uuid.New()
	testJob := &domain.Job{
		ID:             uuid.New(),
		Queue:          "default",
		Type:           "echo",
		Payload:        json.RawMessage(`{"dispatched":true}`),
		State:          domain.StateRunning,
		AttemptCount:   1,
		TimeoutSeconds: 5,
		LeaseToken:     &leaseToken,
	}

	repo.mu.Lock()
	repo.claimedJobs = []*domain.Job{testJob}
	repo.mu.Unlock()

	dispatcher := NewDispatcher(repo, pool, "default", 20*time.Millisecond, 10*time.Second, "disp-test-worker")

	ctx, cancel := context.WithCancel(context.Background())
	go dispatcher.Start(ctx)

	// Wait for dispatcher to poll, claim, and pool to process
	time.Sleep(100 * time.Millisecond)
	cancel()
	dispatcher.Stop()

	repo.mu.Lock()
	defer repo.mu.Unlock()

	if _, ok := repo.successCalls[testJob.ID]; !ok {
		t.Fatalf("expected dispatched job %s to succeed", testJob.ID)
	}
}

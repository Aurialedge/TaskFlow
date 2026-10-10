package domain

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestStateTransitions(t *testing.T) {
	tests := []struct {
		name      string
		from      State
		to        State
		allowable bool
	}{
		{"pending to running", StatePending, StateRunning, true},
		{"pending to cancelled", StatePending, StateCancelled, true},
		{"pending to succeeded", StatePending, StateSucceeded, false},
		{"scheduled to pending", StateScheduled, StatePending, true},
		{"scheduled to running", StateScheduled, StateRunning, true},
		{"scheduled to cancelled", StateScheduled, StateCancelled, true},
		{"running to succeeded", StateRunning, StateSucceeded, true},
		{"running to pending (retry)", StateRunning, StatePending, true},
		{"running to dead (exhausted)", StateRunning, StateDead, true},
		{"running to failed", StateRunning, StateFailed, true},
		{"running to cancelled", StateRunning, StateCancelled, true},
		{"succeeded to running", StateSucceeded, StateRunning, false},
		{"cancelled to running", StateCancelled, StateRunning, false},
		{"failed to pending (manual retry)", StateFailed, StatePending, true},
		{"dead to pending (manual retry)", StateDead, StatePending, true},
		{"same state is no-op", StatePending, StatePending, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			allowed := CanTransition(tt.from, tt.to)
			if allowed != tt.allowable {
				t.Errorf("CanTransition(%s, %s) = %v, expected %v", tt.from, tt.to, allowed, tt.allowable)
			}
		})
	}
}

func TestJob_TransitionTo(t *testing.T) {
	job := &Job{
		ID:        uuid.New(),
		State:     StatePending,
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}

	// Valid transition
	if err := job.TransitionTo(StateRunning); err != nil {
		t.Fatalf("expected valid transition, got error: %v", err)
	}
	if job.State != StateRunning {
		t.Errorf("expected state running, got %s", job.State)
	}

	// Invalid transition: running cannot transition directly to invalid state
	err := job.TransitionTo("unknown_state")
	if !errors.Is(err, ErrInvalidState) {
		t.Errorf("expected ErrInvalidState, got %v", err)
	}

	// Invalid transition: running directly to scheduled
	err = job.TransitionTo(StateScheduled)
	if !errors.Is(err, ErrInvalidStateTransition) {
		t.Errorf("expected ErrInvalidStateTransition, got %v", err)
	}
}

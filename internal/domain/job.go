package domain

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// State represents the lifecycle status of a Job.
type State string

const (
	StatePending   State = "pending"
	StateScheduled State = "scheduled"
	StateRunning   State = "running"
	StateSucceeded State = "succeeded"
	StateFailed    State = "failed"
	StateDead      State = "dead"
	StateCancelled State = "cancelled"
)

var (
	ErrJobNotFound            = errors.New("job not found")
	ErrInvalidState           = errors.New("invalid job state")
	ErrInvalidStateTransition = errors.New("invalid state transition")
	ErrInvalidJobInput        = errors.New("invalid job input")
	ErrPayloadTooLarge        = errors.New("job payload exceeds maximum allowed size")
	ErrDuplicateIdempotency   = errors.New("duplicate idempotency key with conflicting payload")
)

// ValidStates contains all legal job states.
var ValidStates = map[State]bool{
	StatePending:   true,
	StateScheduled: true,
	StateRunning:   true,
	StateSucceeded: true,
	StateFailed:    true,
	StateDead:      true,
	StateCancelled: true,
}

// IsValidState verifies if a state string matches known domain states.
func IsValidState(s State) bool {
	return ValidStates[s]
}

// CanTransition validates whether transitioning from current to next state is permitted.
func CanTransition(from, to State) bool {
	if from == to {
		return true
	}

	switch from {
	case StatePending:
		return to == StateRunning || to == StateCancelled
	case StateScheduled:
		return to == StatePending || to == StateRunning || to == StateCancelled
	case StateRunning:
		return to == StateSucceeded || to == StatePending || to == StateFailed || to == StateDead || to == StateCancelled
	case StateFailed, StateDead:
		// Manual retry can move failed/dead job back to pending
		return to == StatePending
	case StateSucceeded, StateCancelled:
		// Terminal states cannot transition
		return false
	default:
		return false
	}
}

// Job encapsulates the persistent unit of work.
type Job struct {
	ID               uuid.UUID       `json:"id"`
	Queue            string          `json:"queue"`
	Type             string          `json:"type"`
	Payload          json.RawMessage `json:"payload"`
	State            State           `json:"state"`
	Priority         int             `json:"priority"`
	MaxAttempts      int             `json:"max_attempts"`
	AttemptCount     int             `json:"attempt_count"`
	RunAt            time.Time       `json:"run_at"`
	TimeoutSeconds   int             `json:"timeout_seconds"`
	LeaseToken       *uuid.UUID      `json:"lease_token,omitempty"`
	LeaseExpiresAt   *time.Time      `json:"lease_expires_at,omitempty"`
	LeaseGeneration  int64           `json:"lease_generation"`
	IdempotencyKey   *string         `json:"idempotency_key,omitempty"`
	IdempotencyScope string          `json:"idempotency_scope"`
	Result           json.RawMessage `json:"result,omitempty"`
	LastError        *string         `json:"last_error,omitempty"`
	CreatedAt        time.Time       `json:"created_at"`
	UpdatedAt        time.Time       `json:"updated_at"`
}

// JobAttempt represents a single execution attempt of a job.
type JobAttempt struct {
	ID            int64      `json:"id"`
	JobID         uuid.UUID  `json:"job_id"`
	AttemptNumber int        `json:"attempt_number"`
	WorkerID      string     `json:"worker_id"`
	State         State      `json:"state"`
	ErrorMessage  *string    `json:"error_message,omitempty"`
	StartedAt     time.Time  `json:"started_at"`
	FinishedAt    *time.Time `json:"finished_at,omitempty"`
	DurationMs    *int64     `json:"duration_ms,omitempty"`
}

// CreateJobInput encapsulates data required to submit a new job.
type CreateJobInput struct {
	Queue            string          `json:"queue"`
	Type             string          `json:"type"`
	Payload          json.RawMessage `json:"payload"`
	Priority         int             `json:"priority"`
	MaxAttempts      int             `json:"max_attempts"`
	RunAt            *time.Time      `json:"run_at,omitempty"`
	TimeoutSeconds   int             `json:"timeout_seconds"`
	IdempotencyKey   *string         `json:"idempotency_key,omitempty"`
	IdempotencyScope string          `json:"idempotency_scope,omitempty"`
}

// ListJobsFilter defines filtering and pagination parameters for jobs querying.
type ListJobsFilter struct {
	Queue  string `json:"queue"`
	State  State  `json:"state"`
	Limit  int    `json:"limit"`
	Offset int    `json:"offset"`
}

// TransitionTo attempts to transition the job into a new state.
func (j *Job) TransitionTo(newState State) error {
	if !IsValidState(newState) {
		return fmt.Errorf("%w: %s", ErrInvalidState, newState)
	}

	if !CanTransition(j.State, newState) {
		return fmt.Errorf("%w: cannot transition from %s to %s", ErrInvalidStateTransition, j.State, newState)
	}

	j.State = newState
	j.UpdatedAt = time.Now().UTC()
	return nil
}

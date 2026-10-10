package retry

import (
	"crypto/rand"
	"errors"
	"math"
	"math/big"
	"time"
)

// ErrPermanent indicates that an error is terminal and the job should not be retried.
var ErrPermanent = errors.New("permanent non-retryable failure")

// Permanent marks an error as permanent.
type permanentError struct {
	err error
}

func (e *permanentError) Error() string {
	return e.err.Error()
}

func (e *permanentError) Unwrap() error {
	return e.err
}

// MarkPermanent wraps an error so that IsPermanent returns true.
func MarkPermanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err: err}
}

// IsPermanent returns true if the error is explicitly classified as permanent.
func IsPermanent(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrPermanent) {
		return true
	}
	var perm *permanentError
	return errors.As(err, &perm)
}

// BackoffManager computes exponential backoff with full jitter.
type BackoffManager struct {
	BaseInterval time.Duration
	MaxInterval  time.Duration
}

// NewBackoffManager creates a BackoffManager with standard defaults.
func NewBackoffManager(base, max time.Duration) *BackoffManager {
	if base <= 0 {
		base = 1 * time.Second
	}
	if max <= 0 {
		max = 60 * time.Second
	}
	return &BackoffManager{
		BaseInterval: base,
		MaxInterval:  max,
	}
}

// Calculate computes exponential backoff with full jitter for attempt n (1-indexed).
// Formula: backoff = random_between(0, min(MaxInterval, BaseInterval * 2^(attempt-1)))
func (m *BackoffManager) Calculate(attempt int) time.Duration {
	if attempt <= 0 {
		attempt = 1
	}

	// Exponential factor: 2^(attempt-1)
	expFactor := math.Pow(2, float64(attempt-1))
	tempNanos := float64(m.BaseInterval.Nanoseconds()) * expFactor

	maxNanos := float64(m.MaxInterval.Nanoseconds())
	if tempNanos > maxNanos || math.IsInf(tempNanos, 0) {
		tempNanos = maxNanos
	}

	ceiling := int64(tempNanos)
	if ceiling <= 0 {
		return m.BaseInterval
	}

	// Full jitter: uniformly random duration between [0, ceiling]
	randBig, err := rand.Int(rand.Reader, big.NewInt(ceiling+1))
	if err != nil {
		// Fallback to half the ceiling if crypto/rand fails
		return time.Duration(ceiling / 2)
	}

	return time.Duration(randBig.Int64())
}

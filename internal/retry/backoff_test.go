package retry

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestPermanentErrorClassification(t *testing.T) {
	regularErr := errors.New("transient network timeout")
	if IsPermanent(regularErr) {
		t.Error("expected regular error to NOT be classified as permanent")
	}

	permSentinel := ErrPermanent
	if !IsPermanent(permSentinel) {
		t.Error("expected ErrPermanent to be classified as permanent")
	}

	wrappedPerm := MarkPermanent(errors.New("invalid payload structure"))
	if !IsPermanent(wrappedPerm) {
		t.Error("expected wrapped permanent error to be classified as permanent")
	}

	doubleWrapped := fmt.Errorf("outer error: %w", wrappedPerm)
	if !IsPermanent(doubleWrapped) {
		t.Error("expected double wrapped error to be classified as permanent")
	}
}

func TestBackoffManager_Calculate(t *testing.T) {
	base := 100 * time.Millisecond
	max := 1 * time.Second
	bm := NewBackoffManager(base, max)

	for attempt := 1; attempt <= 10; attempt++ {
		backoff := bm.Calculate(attempt)
		if backoff < 0 {
			t.Errorf("attempt %d produced negative backoff: %v", attempt, backoff)
		}
		if backoff > max {
			t.Errorf("attempt %d produced backoff %v exceeding max %v", attempt, backoff, max)
		}
	}
}

package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/Aurialedge/TaskFlow/internal/repository"
)

// Sweeper periodically inspects the database for abandoned jobs with expired leases.
type Sweeper struct {
	repo          repository.JobRepository
	interval      time.Duration
	leaseDuration time.Duration
	stopCh        chan struct{}
}

// NewSweeper constructs a new Sweeper.
func NewSweeper(repo repository.JobRepository, interval, leaseDuration time.Duration) *Sweeper {
	if interval <= 0 {
		interval = 10 * time.Second
	}
	if leaseDuration <= 0 {
		leaseDuration = 30 * time.Second
	}

	return &Sweeper{
		repo:          repo,
		interval:      interval,
		leaseDuration: leaseDuration,
		stopCh:        make(chan struct{}),
	}
}

// Start runs the periodic sweep loop until ctx is cancelled or Stop is called.
func (s *Sweeper) Start(ctx context.Context) {
	slog.Info("sweeper started", "interval", s.interval, "lease_duration", s.leaseDuration)

	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("sweeper stopping due to context cancellation")
			return
		case <-s.stopCh:
			slog.Info("sweeper stopping due to Stop request")
			return
		case <-ticker.C:
			s.sweep(ctx)
		}
	}
}

// Stop signals the sweeper to cease polling.
func (s *Sweeper) Stop() {
	select {
	case <-s.stopCh:
	default:
		close(s.stopCh)
	}
}

func (s *Sweeper) sweep(ctx context.Context) {
	sweepCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	recovered, err := s.repo.RecoverAbandonedJobs(sweepCtx, s.leaseDuration)
	if err != nil {
		slog.Error("error recovering abandoned jobs", "error", err)
		return
	}

	if recovered > 0 {
		slog.Warn("sweeper recovered abandoned jobs", "recovered_count", recovered)
	}
}

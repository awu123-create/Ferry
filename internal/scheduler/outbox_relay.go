package scheduler

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/awu123-create/ferry/internal/domain"
)

// OutboxRelayConfig configures batching and timing for OutboxRelay.
type OutboxRelayConfig struct {
	BatchSize    int
	PollInterval time.Duration
	BatchTimeout time.Duration
}

// OutboxRelay drains immediately due outbox rows into Kafka via LISTEN/NOTIFY and fallback polling.
type OutboxRelay struct {
	log       *zap.Logger
	outbox    OutboxStore
	publisher ReadyPublisher
	cfg       OutboxRelayConfig
}

// NewOutboxRelay validates dependencies and constructs an OutboxRelay.
func NewOutboxRelay(
	log *zap.Logger,
	outbox OutboxStore,
	publisher ReadyPublisher,
	cfg OutboxRelayConfig,
) (*OutboxRelay, error) {
	if outbox == nil {
		return nil, fmt.Errorf("outbox relay requires an OutboxStore")
	}
	if publisher == nil {
		return nil, fmt.Errorf("outbox relay requires a ReadyPublisher")
	}
	if log == nil {
		log = zap.NewNop()
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 100
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 500 * time.Millisecond
	}
	if cfg.BatchTimeout <= 0 {
		cfg.BatchTimeout = 5 * time.Second
	}
	return &OutboxRelay{
		log:       log,
		outbox:    outbox,
		publisher: publisher,
		cfg:       cfg,
	}, nil
}

// Run executes the immediate-channel Outbox Relay loop until ctx is cancelled.
//
// Contract (Milestone 1 - core logic to be implemented, ADR-0002 & ADR-0008):
//  1. Drain pending immediate rows via r.RelayOnce(ctx) in a loop while claimed >= r.cfg.BatchSize.
//  2. Wait for the next wakeup using either r.outbox.WaitForOutboxNotification(waitCtx)
//     (woken by PostgreSQL trigger notify_ferry_outbox) or r.cfg.PollInterval (500ms fallback).
//  3. Handle transient errors by logging/backing off briefly without tight looping, and return nil on ctx.Done().
func (r *OutboxRelay) Run(ctx context.Context) error {
	if !r.hasAllDependencies() {
		return fmt.Errorf("outbox relay is missing required dependencies")
	}

	r.log.Info(
		"outbox relay core is not yet implemented; waiting for shutdown",
		zap.Int("batch_size", r.cfg.BatchSize),
		zap.Duration("poll_interval", r.cfg.PollInterval),
		zap.Duration("batch_timeout", r.cfg.BatchTimeout),
	)
	<-ctx.Done()
	return nil
}

// RelayOnce claims and dispatches one bounded batch of immediate outbox items.
//
// Contract (Milestone 1 - core logic to be implemented):
//  1. Bound the batch transaction with batchCtx, cancel := context.WithTimeout(ctx, r.cfg.BatchTimeout)
//     (5s hard cap to protect PG Autovacuum).
//  2. Call r.outbox.ClaimAndDispatchImmediateOutbox(batchCtx, r.cfg.BatchSize, dispatch),
//     where dispatch extracts each item.TaskID and calls r.publisher.PublishTaskIDs(txCtx, taskIDs).
func (r *OutboxRelay) RelayOnce(ctx context.Context) (int, error) {
	_ = ctx
	return 0, domain.ErrNotImplemented
}

func (r *OutboxRelay) hasAllDependencies() bool {
	return r.outbox != nil && r.publisher != nil
}

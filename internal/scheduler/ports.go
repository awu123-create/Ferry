package scheduler

import (
	"context"

	"github.com/google/uuid"

	"github.com/awu123-create/ferry/internal/domain"
)

// OutboxStore defines the PostgreSQL Outbox operations consumed by OutboxRelay.
type OutboxStore interface {
	ClaimAndDispatchImmediateOutbox(
		ctx context.Context,
		limit int,
		dispatch func(ctx context.Context, items []domain.OutboxItem) error,
	) (int, error)
	WaitForOutboxNotification(ctx context.Context) error
}

// ReadyPublisher publishes due Task IDs to Kafka.
type ReadyPublisher interface {
	PublishTaskIDs(ctx context.Context, taskIDs []uuid.UUID) error
}

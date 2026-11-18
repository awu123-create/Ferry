package worker

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/awu123-create/ferry/internal/domain"
)

// ReadyStream consumes ready Task tickets from Kafka and commits contiguous completed watermarks.
type ReadyStream interface {
	PollReady(ctx context.Context, maxRecords int) ([]domain.ReadyTicket, error)
	Ack(ticket domain.ReadyTicket)
	CommitCompleted(ctx context.Context) error
}

// TaskStore defines PostgreSQL Task persistence needed by the Worker execution pipeline.
type TaskStore interface {
	GetTask(ctx context.Context, id uuid.UUID) (domain.Task, error)
	SavePreDeliveryDeadLetter(
		ctx context.Context,
		taskID uuid.UUID,
		expectedReplayCount, expectedAttempt int,
		reason string,
		now time.Time,
	) (bool, error)
	SaveDeliveryOutcome(
		ctx context.Context,
		expectedReplayCount, expectedAttempt int,
		task domain.Task,
		attempt domain.DeliveryAttempt,
	) (bool, error)
}

// EndpointReader fetches the current Endpoint configuration for outbound delivery.
type EndpointReader interface {
	GetEndpoint(ctx context.Context, id uuid.UUID) (domain.Endpoint, error)
}

// ExecutionLocker provides per-Task mutual exclusion between Worker execution and API cancellation.
type ExecutionLocker interface {
	AcquireExecutionLock(ctx context.Context, taskID uuid.UUID, ttl time.Duration) (token string, acquired bool, err error)
	ReleaseExecutionLock(ctx context.Context, taskID uuid.UUID, token string) error
}

// HTTPSender dispatches the outbound HTTP POST request and summarizes the attempt outcome.
type HTTPSender interface {
	Send(ctx context.Context, req domain.DeliveryRequest) domain.DeliveryResult
}

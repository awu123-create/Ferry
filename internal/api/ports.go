package api

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/awu123-create/ferry/internal/domain"
)

// EndpointStore defines the persistence operations required by the HTTP control plane for Endpoints.
type EndpointStore interface {
	CreateEndpoint(ctx context.Context, ep domain.Endpoint) error
	GetEndpoint(ctx context.Context, id uuid.UUID) (domain.Endpoint, error)
	ListEndpoints(ctx context.Context) ([]domain.Endpoint, error)
	UpdateEndpoint(ctx context.Context, ep domain.Endpoint) error
}

// TaskStore defines the persistence operations required by the HTTP control plane for Tasks.
type TaskStore interface {
	CreateTaskIdempotent(ctx context.Context, task domain.Task, outbox domain.OutboxItem) (domain.CreateTaskResult, error)
	GetTask(ctx context.Context, id uuid.UUID) (domain.Task, error)
	ListDeliveryAttempts(ctx context.Context, taskID uuid.UUID) ([]domain.DeliveryAttempt, error)
	CancelTask(ctx context.Context, taskID uuid.UUID, now time.Time) (domain.Task, error)
}

// ExecutionLocker provides mutual exclusion between in-flight Worker delivery and API cancellation.
type ExecutionLocker interface {
	AcquireExecutionLock(ctx context.Context, taskID uuid.UUID, ttl time.Duration) (token string, acquired bool, err error)
	ReleaseExecutionLock(ctx context.Context, taskID uuid.UUID, token string) error
}

// ReadinessProbe verifies downstream infrastructure availability for /readyz.
type ReadinessProbe interface {
	Ping(ctx context.Context) error
}

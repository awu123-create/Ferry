package api

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/awu123-create/ferry/internal/domain"
)

// EndpointStore 定义 HTTP 控制面对 Endpoint 所需的持久化操作。
type EndpointStore interface {
	CreateEndpoint(ctx context.Context, ep domain.Endpoint) error
	GetEndpoint(ctx context.Context, id uuid.UUID) (domain.Endpoint, error)
	ListEndpoints(ctx context.Context) ([]domain.Endpoint, error)
	UpdateEndpoint(ctx context.Context, ep domain.Endpoint, expectedUpdatedAt time.Time) error
}

// TaskStore 定义 HTTP 控制面对 Task 所需的持久化操作。
type TaskStore interface {
	CreateTaskIdempotent(ctx context.Context, task domain.Task) (domain.CreateTaskResult, error)
	GetTask(ctx context.Context, id uuid.UUID) (domain.Task, error)
	ListDeliveryAttempts(ctx context.Context, taskID uuid.UUID) ([]domain.DeliveryAttempt, error)
	CancelTask(ctx context.Context, taskID uuid.UUID, now time.Time) (domain.Task, error)
}

// ExecutionLocker 提供 Worker 在途投递与 API 取消操作之间的执行互斥保护。
type ExecutionLocker interface {
	AcquireExecutionLock(ctx context.Context, taskID uuid.UUID, ttl time.Duration) (token string, acquired bool, err error)
	ReleaseExecutionLock(ctx context.Context, taskID uuid.UUID, token string) error
}

// ReadinessProbe 用于 /readyz 检查下游基础设施组件的就绪状态。
type ReadinessProbe interface {
	Ping(ctx context.Context) error
}

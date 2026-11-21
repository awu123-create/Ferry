package worker

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/awu123-create/ferry/internal/domain"
)

// ReadyStream 定义从就绪流拉取待投递任务句柄并按连续完成水位提交 Offset 的接口。
type ReadyStream interface {
	PollReady(ctx context.Context, maxRecords int) ([]domain.ReadyDelivery, error)
	CommitCompleted(ctx context.Context) error
}

// TaskStore 定义 Worker 执行投递流水线所需的 PostgreSQL Task 持久化接口。
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

// EndpointReader 读取目标 Endpoint 的最新投递配置。
type EndpointReader interface {
	GetEndpoint(ctx context.Context, id uuid.UUID) (domain.Endpoint, error)
}

// ExecutionLocker 提供单任务维度的 Worker 执行互斥锁，与 API 取消抢锁互斥。
type ExecutionLocker interface {
	AcquireExecutionLock(ctx context.Context, taskID uuid.UUID, ttl time.Duration) (token string, acquired bool, err error)
	ReleaseExecutionLock(ctx context.Context, taskID uuid.UUID, token string) error
}

// HTTPSender 发起出站 HTTP POST 投递并汇总本次尝试结果。
type HTTPSender interface {
	Send(ctx context.Context, req domain.DeliveryRequest) domain.DeliveryResult
}

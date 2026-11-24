package scheduler

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/awu123-create/ferry/internal/domain"
)

// OutboxStore 定义 OutboxRelay 消费的 PostgreSQL 事务发件箱操作接口。
type OutboxStore interface {
	ClaimAndDispatchImmediateOutbox(
		ctx context.Context,
		now time.Time,
		limit int,
		dispatch func(ctx context.Context, taskIDs []uuid.UUID) error,
	) (int, error)
	ClaimAndDispatchDelayedOutbox(
		ctx context.Context,
		now time.Time,
		limit int,
		dispatch func(ctx context.Context, items []domain.OutboxItem) error,
	) (int, error)
	WaitForOutboxNotification(ctx context.Context) error
}

// ReadyPublisher 定义向 Kafka 发布就绪 Task ID 的出站接口。
type ReadyPublisher interface {
	PublishTaskIDs(ctx context.Context, taskIDs []uuid.UUID) error
}

// DelayedEnqueuer 定义 Outbox 延迟通道批量写入延迟池的出站接口。
type DelayedEnqueuer interface {
	ScheduleDelayedBatch(ctx context.Context, items []domain.OutboxItem) error
}

// DelayedQueue 定义 DelayedForwarder 领取到期租约批次并条件确认删除的延迟池接口。
type DelayedQueue interface {
	ClaimDelayed(ctx context.Context, limit int, leaseTTL time.Duration) (taskIDs []uuid.UUID, leaseUntilMs int64, err error)
	AckDelayed(ctx context.Context, taskIDs []uuid.UUID, leaseUntilMs int64) error
}

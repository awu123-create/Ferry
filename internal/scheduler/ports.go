package scheduler

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// OutboxStore 定义 OutboxRelay 消费的 PostgreSQL 事务发件箱操作接口。
type OutboxStore interface {
	ClaimAndDispatchImmediateOutbox(
		ctx context.Context,
		now time.Time,
		limit int,
		dispatch func(ctx context.Context, taskIDs []uuid.UUID) error,
	) (int, error)
	WaitForOutboxNotification(ctx context.Context) error
}

// ReadyPublisher 定义向 Kafka 发布就绪 Task ID 的出站接口。
type ReadyPublisher interface {
	PublishTaskIDs(ctx context.Context, taskIDs []uuid.UUID) error
}

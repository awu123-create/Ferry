package scheduler

import (
	"context"

	"github.com/google/uuid"

	"github.com/awu123-create/ferry/internal/domain"
)

// OutboxStore 定义 OutboxRelay 消费的 PostgreSQL 事务发件箱操作接口。
type OutboxStore interface {
	ClaimAndDispatchImmediateOutbox(
		ctx context.Context,
		limit int,
		dispatch func(ctx context.Context, items []domain.OutboxItem) error,
	) (int, error)
	WaitForOutboxNotification(ctx context.Context) error
}

// ReadyPublisher 定义向 Kafka 发布就绪 Task ID 的出站接口。
type ReadyPublisher interface {
	PublishTaskIDs(ctx context.Context, taskIDs []uuid.UUID) error
}

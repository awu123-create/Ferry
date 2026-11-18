package scheduler

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/awu123-create/ferry/internal/domain"
)

// OutboxRelayConfig 配置 OutboxRelay 的批量大小、兜底轮询周期与单批硬超时。
type OutboxRelayConfig struct {
	BatchSize    int
	PollInterval time.Duration
	BatchTimeout time.Duration
}

// OutboxRelay 通过 LISTEN/NOTIFY 与兜底定时轮询，将立即到期的 outbox 行搬运至 Kafka。
type OutboxRelay struct {
	log       *zap.Logger
	outbox    OutboxStore
	publisher ReadyPublisher
	cfg       OutboxRelayConfig
}

// NewOutboxRelay 校验依赖并构造 OutboxRelay 实例。
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

// Run 驱动立即通道 OutboxRelay 循环，直到 ctx 被取消。
//
// 契约要求（Milestone 1 核心逻辑，留待实现，见 ADR-0002 & ADR-0008）：
//  1. 当 claimed >= r.cfg.BatchSize 时持续调用 r.RelayOnce(ctx) 排空积压的立即通道记录。
//  2. 排空后阻塞等待下一次唤醒：优先监听 r.outbox.WaitForOutboxNotification(waitCtx)
//     （由 PostgreSQL 触发器 notify_ferry_outbox 瞬时唤醒），并以 r.cfg.PollInterval（500ms）作为兜底定时。
//  3. 遇到瞬时错误时记录日志并短暂退避，避免空转打爆数据库，收到 ctx.Done() 后返回 nil。
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

// RelayOnce 抢占并派发单批有界的立即到期 Outbox 记录。
//
// 契约要求（Milestone 1 核心逻辑，留待实现）：
//  1. 使用 batchCtx, cancel := context.WithTimeout(ctx, r.cfg.BatchTimeout) 绑定单批事务 5s 硬超时，
//     防止慢发布长期持锁阻塞 PostgreSQL Autovacuum。
//  2. 调用 r.outbox.ClaimAndDispatchImmediateOutbox(batchCtx, r.cfg.BatchSize, dispatch)，
//     在 dispatch 回调中提取每条 item.TaskID 并调用 r.publisher.PublishTaskIDs(txCtx, taskIDs)。
func (r *OutboxRelay) RelayOnce(ctx context.Context) (int, error) {
	_ = ctx
	return 0, domain.ErrNotImplemented
}

func (r *OutboxRelay) hasAllDependencies() bool {
	return r.outbox != nil && r.publisher != nil
}

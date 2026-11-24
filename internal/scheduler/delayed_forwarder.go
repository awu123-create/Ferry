package scheduler

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"
)

const (
	defaultDelayedBatchSize    = 100
	defaultDelayedPollInterval = 100 * time.Millisecond
	defaultDelayedLeaseTTL     = 30 * time.Second
	defaultDelayedStepTimeout  = 5 * time.Second
	minDelayedStepTimeout      = 50 * time.Millisecond
	delayedAckShutdownTimeout  = 5 * time.Second
)

// DelayedForwarderConfig 配置延迟池转发器的扫描间隔、单批上限、Schedule Lease 时长与退避封顶。
type DelayedForwarderConfig struct {
	BatchSize       int
	PollInterval    time.Duration
	LeaseTTL        time.Duration
	ErrorBackoffCap time.Duration
}

// DelayedForwarder 周期性从 Redis 延迟池原子领取到期任务并施加 Schedule Lease，
// 批量发布至就绪流确认后再条件删除分数未变的成员
// （详见 docs/adr/0003-redis-zset-lease-and-pg-reconciler.md 与 docs/adr/0008-concurrency-fencing-and-flow-control-hardening.md）。
type DelayedForwarder struct {
	log       *zap.Logger
	queue     DelayedQueue
	publisher ReadyPublisher
	cfg       DelayedForwarderConfig
}

// NewDelayedForwarder 校验必需依赖并构造 DelayedForwarder 实例。
func NewDelayedForwarder(
	log *zap.Logger,
	queue DelayedQueue,
	publisher ReadyPublisher,
	cfg DelayedForwarderConfig,
) (*DelayedForwarder, error) {
	if queue == nil {
		return nil, fmt.Errorf("delayed forwarder requires a DelayedQueue")
	}
	if publisher == nil {
		return nil, fmt.Errorf("delayed forwarder requires a ReadyPublisher")
	}
	if log == nil {
		log = zap.NewNop()
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = defaultDelayedBatchSize
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = defaultDelayedPollInterval
	}
	if cfg.LeaseTTL <= 0 {
		cfg.LeaseTTL = defaultDelayedLeaseTTL
	}
	if cfg.ErrorBackoffCap <= 0 {
		cfg.ErrorBackoffCap = defaultSchedulerErrorBackoff
	}
	return &DelayedForwarder{
		log:       log,
		queue:     queue,
		publisher: publisher,
		cfg:       cfg,
	}, nil
}

// Run 驱动延迟池租约转发循环直到 ctx 取消。
func (f *DelayedForwarder) Run(ctx context.Context) error {
	runPollLoop(ctx, f.log, "delayed forwarder", nil, f.cfg.PollInterval, f.cfg.ErrorBackoffCap, f.cfg.BatchSize, f.forwardOnce)
	return nil
}

func (f *DelayedForwarder) forwardOnce(ctx context.Context) (int, error) {
	stepTimeout := f.outboundStepTimeout()

	claimCtx, cancelClaim := context.WithTimeout(ctx, stepTimeout)
	taskIDs, leaseUntilMs, err := f.queue.ClaimDelayed(claimCtx, f.cfg.BatchSize, f.cfg.LeaseTTL)
	cancelClaim()
	if err != nil {
		return 0, fmt.Errorf("claim delayed tasks: %w", err)
	}
	if len(taskIDs) == 0 {
		return 0, nil
	}

	pubCtx, cancelPub := context.WithTimeout(ctx, stepTimeout)
	pubErr := f.publisher.PublishTaskIDs(pubCtx, taskIDs)
	cancelPub()
	if pubErr != nil {
		return 0, fmt.Errorf("publish %d claimed delayed tasks: %w", len(taskIDs), pubErr)
	}

	// 就绪流已确认写入：即便此刻收到停机信号，也使用独立短超时尽量完成条件删除，避免 30s 后重复转发。
	ackCtx, cancelAck := context.WithTimeout(context.WithoutCancel(ctx), delayedAckShutdownTimeout)
	ackErr := f.queue.AckDelayed(ackCtx, taskIDs, leaseUntilMs)
	cancelAck()
	if ackErr != nil {
		select {
		case <-ctx.Done():
			f.log.Warn("ack delayed tasks during shutdown failed", zap.Int("count", len(taskIDs)), zap.Error(ackErr))
			return 0, nil
		default:
		}
		return 0, fmt.Errorf("ack %d delayed tasks: %w", len(taskIDs), ackErr)
	}

	return len(taskIDs), nil
}

// outboundStepTimeout 约束单次领取与发布的硬超时，严格小于 Schedule Lease 时长，
// 防止下游卡死穿透租约窗口导致其他实例提前接管重复投递。
func (f *DelayedForwarder) outboundStepTimeout() time.Duration {
	halfLease := max(f.cfg.LeaseTTL/2, minDelayedStepTimeout)
	return min(defaultDelayedStepTimeout, halfLease)
}

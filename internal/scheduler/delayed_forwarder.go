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
)

// DelayedForwarderConfig 配置延迟池转发器的扫描间隔、单批数量、Schedule Lease 时长与失败退避上限。
type DelayedForwarderConfig struct {
	BatchSize       int
	PollInterval    time.Duration
	LeaseTTL        time.Duration
	ErrorBackoffCap time.Duration
}

// DelayedForwarder 定期从 Redis 延迟池取出到期任务并续上 Schedule Lease，
// 等批量发往 Kafka 确认成功后再把租约未变的成员从延迟池删掉
// （见 docs/adr/0003-redis-zset-lease-and-pg-reconciler.md 与 docs/adr/0008-concurrency-fencing-and-flow-control-hardening.md）。
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
	runPollLoop(ctx, pollLoopConfig{
		log:          f.log,
		component:    "delayed_forwarder",
		pollInterval: f.cfg.PollInterval,
		backoffCap:   f.cfg.ErrorBackoffCap,
		batchSize:    f.cfg.BatchSize,
		step:         f.forwardOnce,
	})
	return nil
}

func (f *DelayedForwarder) forwardOnce(ctx context.Context) (int, error) {
	roundTimeout := f.roundTimeout()
	roundCtx, cancelRound := context.WithTimeout(ctx, roundTimeout)
	defer cancelRound()

	batch, err := f.queue.ClaimDelayed(roundCtx, f.cfg.BatchSize, f.cfg.LeaseTTL)
	if err != nil {
		return 0, fmt.Errorf("claim delayed tasks: %w", err)
	}
	if len(batch.TaskIDs) == 0 {
		return 0, nil
	}

	if pubErr := f.publisher.PublishTaskIDs(roundCtx, batch.TaskIDs); pubErr != nil {
		return 0, fmt.Errorf("publish %d claimed delayed tasks: %w", len(batch.TaskIDs), pubErr)
	}

	// Kafka 已经确认收到这批任务了：
	// 正常情况下沿用本轮共同的超时时间（包一层 WithoutCancel，防止刚发完 Kafka 撞上停机把删除中途打断）；
	// 如果这时父 ctx 已经取消（进程正在停机），就改用独立的短超时（仍受 roundTimeout 限制，默认 5s 且不超过租约一半）把删除收尾做完。
	var (
		ackCtx    context.Context
		cancelAck context.CancelFunc
	)
	if ctx.Err() != nil {
		ackCtx, cancelAck = context.WithTimeout(context.WithoutCancel(ctx), roundTimeout)
	} else {
		deadline, _ := roundCtx.Deadline()
		ackCtx, cancelAck = context.WithDeadline(context.WithoutCancel(ctx), deadline)
	}
	defer cancelAck()

	if ackErr := f.queue.AckDelayed(ackCtx, batch); ackErr != nil {
		select {
		case <-ctx.Done():
			f.log.Warn("ack delayed tasks during shutdown failed", zap.Int("count", len(batch.TaskIDs)), zap.Error(ackErr))
			return 0, nil
		default:
		}
		return 0, fmt.Errorf("ack %d delayed tasks: %w", len(batch.TaskIDs), ackErr)
	}

	return len(batch.TaskIDs), nil
}

// roundTimeout 计算单轮“领取 + 发 Kafka + 删除租约”总共最多花多久（不超过 Schedule Lease 的一半），
// 避免这一轮卡太久拖到租约过期，让别的实例重复领走同一批任务。
func (f *DelayedForwarder) roundTimeout() time.Duration {
	halfLease := max(f.cfg.LeaseTTL/2, time.Nanosecond)
	return min(defaultDelayedStepTimeout, halfLease)
}

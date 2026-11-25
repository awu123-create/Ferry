package scheduler

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
)

const (
	defaultOutboxBatchSize    = 100
	defaultOutboxPollInterval = 500 * time.Millisecond
	defaultOutboxBatchTimeout = 5 * time.Second
)

// OutboxRelayConfig 配置双通道 OutboxRelay 的批量大小、定时轮询间隔、单批超时上限、失败退避上限和时钟来源。
type OutboxRelayConfig struct {
	BatchSize       int
	PollInterval    time.Duration
	BatchTimeout    time.Duration
	ErrorBackoffCap time.Duration
	Clock           func() time.Time
}

// OutboxRelay 共用一条 PG LISTEN/NOTIFY 连接接收唤醒信号，再分两条互不阻塞的协程搬运立即任务和延迟任务。
type OutboxRelay struct {
	log       *zap.Logger
	outbox    OutboxStore
	publisher ReadyPublisher
	delayed   DelayedEnqueuer
	cfg       OutboxRelayConfig
}

// NewOutboxRelay 校验依赖并构造双通道 OutboxRelay 实例。
func NewOutboxRelay(
	log *zap.Logger,
	outbox OutboxStore,
	publisher ReadyPublisher,
	delayed DelayedEnqueuer,
	cfg OutboxRelayConfig,
) (*OutboxRelay, error) {
	if outbox == nil {
		return nil, fmt.Errorf("outbox relay requires an OutboxStore")
	}
	if publisher == nil {
		return nil, fmt.Errorf("outbox relay requires a ReadyPublisher")
	}
	if delayed == nil {
		return nil, fmt.Errorf("outbox relay requires a DelayedEnqueuer")
	}
	if log == nil {
		log = zap.NewNop()
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = defaultOutboxBatchSize
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = defaultOutboxPollInterval
	}
	if cfg.BatchTimeout <= 0 {
		cfg.BatchTimeout = defaultOutboxBatchTimeout
	}
	if cfg.ErrorBackoffCap <= 0 {
		cfg.ErrorBackoffCap = defaultSchedulerErrorBackoff
	}
	if cfg.Clock == nil {
		cfg.Clock = time.Now
	}
	return &OutboxRelay{
		log:       log,
		outbox:    outbox,
		publisher: publisher,
		delayed:   delayed,
		cfg:       cfg,
	}, nil
}

// Run 启动单连接 LISTEN 扇出协程以及互不阻塞的立即/延迟双通道循环，直到 ctx 取消并等待全部协程退出
// （详见 docs/adr/0002-transactional-outbox-batch-relay.md 与 docs/adr/0008-concurrency-fencing-and-flow-control-hardening.md）。
func (r *OutboxRelay) Run(ctx context.Context) error {
	immediateWake := make(chan struct{}, 1)
	delayedWake := make(chan struct{}, 1)

	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		r.runListener(ctx, immediateWake, delayedWake)
	}()
	go func() {
		defer wg.Done()
		runPollLoop(ctx, pollLoopConfig{
			log:          r.log,
			component:    "immediate_outbox_relay",
			wakeCh:       immediateWake,
			pollInterval: r.cfg.PollInterval,
			backoffCap:   r.cfg.ErrorBackoffCap,
			batchSize:    r.cfg.BatchSize,
			step: func(stepCtx context.Context) (int, error) {
				batchCtx, cancel := context.WithTimeout(stepCtx, r.cfg.BatchTimeout)
				defer cancel()
				return r.outbox.ClaimAndDispatchImmediateOutbox(batchCtx, r.now(), r.cfg.BatchSize, r.publisher.PublishTaskIDs)
			},
		})
	}()
	go func() {
		defer wg.Done()
		runPollLoop(ctx, pollLoopConfig{
			log:          r.log,
			component:    "delayed_outbox_relay",
			wakeCh:       delayedWake,
			pollInterval: r.cfg.PollInterval,
			backoffCap:   r.cfg.ErrorBackoffCap,
			batchSize:    r.cfg.BatchSize,
			step: func(stepCtx context.Context) (int, error) {
				batchCtx, cancel := context.WithTimeout(stepCtx, r.cfg.BatchTimeout)
				defer cancel()
				return r.outbox.ClaimAndDispatchDelayedOutbox(batchCtx, r.now(), r.cfg.BatchSize, r.delayed.ScheduleDelayedBatch)
			},
		})
	}()

	wg.Wait()
	return nil
}

func (r *OutboxRelay) runListener(ctx context.Context, immediateWake, delayedWake chan<- struct{}) {
	for {
		if ctx.Err() != nil {
			return
		}

		if err := r.outbox.WaitForOutboxNotification(ctx); err != nil {
			if ctx.Err() != nil {
				return
			}
			r.log.Warn("outbox notification wait failed; falling back to poll interval", zap.Error(err))
			if !waitWakeOrInterval(ctx, nil, r.cfg.PollInterval) {
				return
			}
			continue
		}

		signalWake(immediateWake)
		signalWake(delayedWake)
	}
}

func (r *OutboxRelay) now() time.Time {
	return r.cfg.Clock().UTC()
}

func signalWake(ch chan<- struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

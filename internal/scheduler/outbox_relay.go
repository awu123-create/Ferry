package scheduler

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	"go.uber.org/zap"
)

const (
	defaultOutboxBatchSize       = 100
	defaultOutboxPollInterval    = 500 * time.Millisecond
	defaultOutboxBatchTimeout    = 5 * time.Second
	defaultSchedulerErrorBackoff = 5 * time.Second
)

// OutboxRelayConfig 配置双通道 OutboxRelay 的批量大小、兜底轮询周期、单批硬超时、退避封顶与时钟来源。
type OutboxRelayConfig struct {
	BatchSize       int
	PollInterval    time.Duration
	BatchTimeout    time.Duration
	ErrorBackoffCap time.Duration
	Clock           func() time.Time
}

// OutboxRelay 共享单连接 LISTEN/NOTIFY 唤醒信号，分别驱动互不阻塞的立即通道与延迟通道搬运 outbox 记录。
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
		runPollLoop(ctx, r.log, "immediate outbox relay", immediateWake, r.cfg.PollInterval, r.cfg.ErrorBackoffCap, r.cfg.BatchSize, func(stepCtx context.Context) (int, error) {
			batchCtx, cancel := context.WithTimeout(stepCtx, r.cfg.BatchTimeout)
			defer cancel()
			return r.outbox.ClaimAndDispatchImmediateOutbox(batchCtx, r.now(), r.cfg.BatchSize, r.publisher.PublishTaskIDs)
		})
	}()
	go func() {
		defer wg.Done()
		runPollLoop(ctx, r.log, "delayed outbox relay", delayedWake, r.cfg.PollInterval, r.cfg.ErrorBackoffCap, r.cfg.BatchSize, func(stepCtx context.Context) (int, error) {
			batchCtx, cancel := context.WithTimeout(stepCtx, r.cfg.BatchTimeout)
			defer cancel()
			return r.outbox.ClaimAndDispatchDelayedOutbox(batchCtx, r.now(), r.cfg.BatchSize, r.delayed.ScheduleDelayedBatch)
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
			if !sleepOrCancel(ctx, r.cfg.PollInterval) {
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

// runPollLoop 统一驱动周期轮询、可选唤醒信号、满批连抽与带抖动的封顶指数退避。
// wakeCh 为 nil 时退化为纯定时轮询；退避窗口内不读取 wakeCh，防止外部通知反复惊醒故障通道；
// 仅在真实成功处理批次（processed > 0）后重置退避，避免空轮询过早清零下游写故障退避。
func runPollLoop(
	ctx context.Context,
	log *zap.Logger,
	name string,
	wakeCh <-chan struct{},
	pollInterval, backoffCap time.Duration,
	batchSize int,
	step func(ctx context.Context) (processed int, err error),
) {
	var backoff time.Duration
	for {
		if ctx.Err() != nil {
			return
		}

		drainWake(wakeCh)
		processed, err := step(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			backoff = nextErrorBackoff(backoff, pollInterval, backoffCap)
			wait := applyBackoffJitter(backoff, backoffCap)
			log.Warn(
				name+" batch failed; backing off",
				zap.Duration("backoff", wait),
				zap.Error(err),
			)
			if !sleepOrCancel(ctx, wait) {
				return
			}
			continue
		}

		if processed > 0 {
			backoff = 0
		}
		if processed >= batchSize {
			continue
		}
		if !waitWakeOrInterval(ctx, wakeCh, pollInterval) {
			return
		}
	}
}

func signalWake(ch chan<- struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

func drainWake(ch <-chan struct{}) {
	if ch == nil {
		return
	}
	select {
	case <-ch:
	default:
	}
}

func waitWakeOrInterval(ctx context.Context, wakeCh <-chan struct{}, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-wakeCh:
		return true
	case <-timer.C:
		return true
	}
}

func sleepOrCancel(ctx context.Context, d time.Duration) bool {
	return waitWakeOrInterval(ctx, nil, d)
}

func nextErrorBackoff(current, base, maxCap time.Duration) time.Duration {
	if current <= 0 {
		return min(base, maxCap)
	}
	if current >= maxCap || current > maxCap/2 {
		return maxCap
	}
	return current * 2
}

// applyBackoffJitter 为指数退避叠加小幅随机抖动（未封顶时向上 0%~20% 且不超过 maxCap，
// 封顶时向下 0%~10%），打散多实例故障重试风暴且严格遵守 maxCap 上限。
func applyBackoffJitter(d, maxCap time.Duration) time.Duration {
	if d >= maxCap {
		spread := maxCap / 10
		if spread <= 0 {
			return maxCap
		}
		return maxCap - time.Duration(rand.Int64N(int64(spread)+1))
	}
	spread := d / 5
	if spread <= 0 {
		return d
	}
	return min(d+time.Duration(rand.Int64N(int64(spread)+1)), maxCap)
}

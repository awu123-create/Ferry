package scheduler

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"
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

// Run 驱动立即通道 OutboxRelay 循环，直到 ctx 被取消（ADR-0002 & ADR-0008）。
func (r *OutboxRelay) Run(ctx context.Context) error {
	if !r.hasAllDependencies() {
		return fmt.Errorf("outbox relay is missing required dependencies")
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		claimed, err := r.RelayOnce(ctx)
		if err != nil {
			select {
			case <-ctx.Done():
				return nil
			default:
				r.log.Error("outbox relay batch failed", zap.Error(err))
			}
			if !sleepOrCancel(ctx, r.cfg.PollInterval) {
				return nil
			}
			continue
		}

		// 若本批拉满，立即继续搬运直至排空积压。
		if claimed >= r.cfg.BatchSize {
			continue
		}

		waitCtx, cancelWait := context.WithTimeout(ctx, r.cfg.PollInterval)
		waitErr := r.outbox.WaitForOutboxNotification(waitCtx)
		cancelWait()

		if waitErr != nil && !errors.Is(waitErr, context.DeadlineExceeded) && !errors.Is(waitErr, context.Canceled) {
			r.log.Warn("outbox notification wait failed; falling back to poll interval", zap.Error(waitErr))
			if !sleepOrCancel(ctx, r.cfg.PollInterval) {
				return nil
			}
		}
	}
}

// RelayOnce 抢占并派发单批有界的立即到期 Outbox 记录（绑定单批 5s 硬超时）。
func (r *OutboxRelay) RelayOnce(ctx context.Context) (int, error) {
	batchCtx, cancel := context.WithTimeout(ctx, r.cfg.BatchTimeout)
	defer cancel()

	return r.outbox.ClaimAndDispatchImmediateOutbox(batchCtx, r.cfg.BatchSize, r.publisher.PublishTaskIDs)
}

func (r *OutboxRelay) hasAllDependencies() bool {
	return r.outbox != nil && r.publisher != nil
}

func sleepOrCancel(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

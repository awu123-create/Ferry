package scheduler

import (
	"context"
	"math/rand/v2"
	"time"

	"go.uber.org/zap"
)

const defaultSchedulerErrorBackoff = 5 * time.Second

type pollLoopConfig struct {
	log          *zap.Logger
	component    string
	wakeCh       <-chan struct{}
	pollInterval time.Duration
	backoffCap   time.Duration
	batchSize    int
	step         func(ctx context.Context) (processed int, err error)
}

// runPollLoop 统一跑定时轮询：支持外部唤醒、一批拉满时直接继续拉下一批，以及出错后带随机抖动的指数退避。
// wakeCh 为 nil 时退化成纯定时轮询；出错退避等待期间不响应 wakeCh，免得新任务触发的 NOTIFY 把故障通道反复叫醒；
// 只要有一轮 step 没报错（包括这轮没数据），退避时间立刻清零。
func runPollLoop(ctx context.Context, cfg pollLoopConfig) {
	var backoff time.Duration
	for {
		if ctx.Err() != nil {
			return
		}

		drainWake(cfg.wakeCh)
		processed, err := cfg.step(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			backoff = nextErrorBackoff(backoff, cfg.pollInterval, cfg.backoffCap)
			wait := applyBackoffJitter(backoff, cfg.backoffCap)
			cfg.log.Warn(
				"scheduler batch failed; backing off",
				zap.String("component", cfg.component),
				zap.Duration("backoff", wait),
				zap.Error(err),
			)
			if !waitWakeOrInterval(ctx, nil, wait) {
				return
			}
			continue
		}

		backoff = 0
		if processed >= cfg.batchSize {
			continue
		}
		if !waitWakeOrInterval(ctx, cfg.wakeCh, cfg.pollInterval) {
			return
		}
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

func nextErrorBackoff(current, base, maxCap time.Duration) time.Duration {
	if current <= 0 {
		return min(base, maxCap)
	}
	if current >= maxCap || current > maxCap/2 {
		return maxCap
	}
	return current * 2
}

// applyBackoffJitter 在退避时间上加一点随机浮动（未到上限时加 0%~20% 且不超过 maxCap，
// 到上限时减 0%~10%），避免多个实例同时出错后在同一时刻齐刷刷重试。
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

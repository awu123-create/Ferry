package worker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/awu123-create/ferry/internal/domain"
	"github.com/awu123-create/ferry/internal/logger"
)

const (
	// initialExecutionLockTTL 为 Step A 首次抢锁的短租约，仅覆盖读库与前置守卫；
	// 读到 Endpoint 后再核对锁持有人 token 并按 timeout_ms + LockGrace 续期，避免短超时任务白占 75s 锁。
	initialExecutionLockTTL  = 15 * time.Second
	defaultLockFallbackDelay = 5 * time.Second
	lockReleaseTimeout       = 5 * time.Second
	pollErrorBackoff         = 200 * time.Millisecond
	periodicCommitPeriod     = 500 * time.Millisecond
	finalCommitTimeout       = 5 * time.Second
)

// Config 配置 Worker 并发度、执行锁保护余量、抢锁争用兜底时长、优雅停机排空超时与时钟来源。
type Config struct {
	Concurrency          int
	LockGrace            time.Duration
	LockFallbackDelay    time.Duration
	ShutdownDrainTimeout time.Duration
	Clock                func() time.Time
}

// Worker 消费就绪 Task 票据，通过 Execution Lock 与复合乐观锁守卫执行安全，
// 发起带签名的出站 HTTP 投递，并在收到 SIGTERM 时执行优雅排空停机。
type Worker struct {
	log       *zap.Logger
	stream    ReadyStream
	tasks     TaskStore
	endpoints EndpointReader
	locker    ExecutionLocker
	delayed   DelayedScheduler
	sender    HTTPSender
	cfg       Config
	ready     atomic.Bool
}

// NewWorker 校验必需依赖并构造就绪可用的 Worker 实例。
func NewWorker(
	log *zap.Logger,
	stream ReadyStream,
	tasks TaskStore,
	endpoints EndpointReader,
	locker ExecutionLocker,
	delayed DelayedScheduler,
	sender HTTPSender,
	cfg Config,
) (*Worker, error) {
	if stream == nil {
		return nil, fmt.Errorf("worker requires a ReadyStream")
	}
	if tasks == nil {
		return nil, fmt.Errorf("worker requires a TaskStore")
	}
	if endpoints == nil {
		return nil, fmt.Errorf("worker requires an EndpointReader")
	}
	if locker == nil {
		return nil, fmt.Errorf("worker requires an ExecutionLocker")
	}
	if delayed == nil {
		return nil, fmt.Errorf("worker requires a DelayedScheduler")
	}
	if sender == nil {
		return nil, fmt.Errorf("worker requires an HTTPSender")
	}
	if log == nil {
		log = zap.NewNop()
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 32
	}
	if cfg.LockGrace <= 0 {
		cfg.LockGrace = 15 * time.Second
	}
	if cfg.LockFallbackDelay <= 0 {
		cfg.LockFallbackDelay = defaultLockFallbackDelay
	}
	if cfg.ShutdownDrainTimeout <= 0 {
		cfg.ShutdownDrainTimeout = 25 * time.Second
	}
	if cfg.Clock == nil {
		cfg.Clock = time.Now
	}

	return &Worker{
		log:       log,
		stream:    stream,
		tasks:     tasks,
		endpoints: endpoints,
		locker:    locker,
		delayed:   delayed,
		sender:    sender,
		cfg:       cfg,
	}, nil
}

// Ready 返回 Worker 当前是否正在接收并处理 Kafka 投递任务（供 /readyz 探针读取）。
func (w *Worker) Ready() bool {
	return w.ready.Load()
}

// Run 持续拉取就绪票据、受控并发投递，并在 ctx 取消时等待在途请求安全排空
// （详见 docs/adr/0006-tech-stack-and-engineering-tooling.md 与 docs/adr/0008-concurrency-fencing-and-flow-control-hardening.md）。
func (w *Worker) Run(ctx context.Context) error {
	// 使用与停止信号分离的 drainCtx，确保收到 SIGTERM 后在途 HTTP 投递仍能完整落库并放锁。
	drainCtx, cancelDrain := context.WithCancel(context.Background())
	defer cancelDrain()

	sem := make(chan struct{}, w.cfg.Concurrency)
	var wg sync.WaitGroup

	stopCommit := make(chan struct{})
	commitStopped := make(chan struct{})
	go w.runPeriodicCommit(drainCtx, stopCommit, commitStopped)

	w.ready.Store(true)
	defer w.ready.Store(false)

	w.log.Info(
		"worker started",
		zap.Int("concurrency", w.cfg.Concurrency),
		zap.Duration("drain_timeout", w.cfg.ShutdownDrainTimeout),
	)

pollLoop:
	for {
		// 等待至少存在 1 个空闲并发槽位，形成天然反压；若已触发停机则立即停止拉取。
		select {
		case sem <- struct{}{}:
			<-sem
		case <-ctx.Done():
			break pollLoop
		}

		available := cap(sem) - len(sem)
		if available <= 0 {
			available = 1
		}

		deliveries, err := w.stream.PollReady(ctx, available)
		if err != nil {
			select {
			case <-ctx.Done():
				break pollLoop
			default:
				w.log.Warn("poll ready stream failed", zap.Error(err))
			}
			if !waitOrCancel(ctx, pollErrorBackoff) {
				break pollLoop
			}
			continue
		}

		for _, delivery := range deliveries {
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				// 停机已触发，尚未分发的任务不 Ack，退出后交由底层重平衡重投。
				break pollLoop
			}

			wg.Add(1)
			go func(d domain.ReadyDelivery) {
				defer wg.Done()
				defer func() { <-sem }()
				if procErr := w.ProcessDelivery(drainCtx, d); procErr != nil {
					w.log.Error(
						"process ready delivery failed",
						zap.String("task_id", d.TaskID.String()),
						zap.Error(procErr),
					)
				}
			}(delivery)
		}
	}

	// 1. 立即摘除就绪探针，阻止外部继续视为健康消费实例。
	w.ready.Store(false)
	w.log.Info("worker shutting down; draining in-flight deliveries")

	// 2. 等待全部在途协程在 ShutdownDrainTimeout（默认 25s）内排空完成。
	drained := make(chan struct{})
	go func() {
		wg.Wait()
		close(drained)
	}()

	drainTimer := time.NewTimer(w.cfg.ShutdownDrainTimeout)
	select {
	case <-drained:
		drainTimer.Stop()
	case <-drainTimer.C:
		w.log.Warn(
			"worker graceful drain timed out; cancelling remaining in-flight deliveries",
			zap.Duration("drain_timeout", w.cfg.ShutdownDrainTimeout),
		)
		cancelDrain()
		<-drained
	}

	// 3. 停止后台周期提交并执行最后一次同步连续水位 Offset 提交。
	close(stopCommit)
	<-commitStopped

	finalCommitCtx, cancelCommit := context.WithTimeout(context.Background(), finalCommitTimeout)
	defer cancelCommit()
	if err := w.stream.CommitCompleted(finalCommitCtx); err != nil {
		w.log.Warn("final kafka offset commit failed", zap.Error(err))
	}

	w.log.Info("worker stopped")
	return nil
}

// ProcessDelivery 对单条就绪任务句柄执行 Step A~E 全流程处理
// （详见 docs/adr/0007-tactical-ddd-and-deep-module-seams.md 与 docs/adr/0008-concurrency-fencing-and-flow-control-hardening.md）。
func (w *Worker) ProcessDelivery(ctx context.Context, delivery domain.ReadyDelivery) error {
	ctx = logger.WithTaskID(ctx, delivery.TaskID)

	// Step A：先以短租约抢占 Redis 执行互斥锁，确保后续读取到最新任务快照；
	// 读取到 Endpoint 实际 timeout_ms 后再按 timeout_ms + LockGrace 续期。
	token, acquired, err := w.locker.AcquireExecutionLock(ctx, delivery.TaskID, initialExecutionLockTTL)
	if err != nil {
		return fmt.Errorf("acquire execution lock: %w", err)
	}
	if !acquired {
		// 别的 Worker 或取消接口正占着这把锁：先查一次 PG，如果任务不存在或已经结束就直接确认消费、零次写 Redis；
		// 还在进行中才写回延迟池（Lua 脚本里会再检查取消标记，防止取消后的迟写污染延迟池）
		// （见 docs/adr/0008-concurrency-fencing-and-flow-control-hardening.md）。
		task, getErr := w.tasks.GetTask(ctx, delivery.TaskID)
		if getErr != nil && !errors.Is(getErr, domain.ErrTaskNotFound) {
			return fmt.Errorf("load contended task: %w", getErr)
		}
		if errors.Is(getErr, domain.ErrTaskNotFound) || task.Status.IsTerminal() {
			ackDelivery(delivery)
			return nil
		}
		if deferErr := w.delayed.DeferContendedTask(ctx, delivery.TaskID, w.cfg.LockFallbackDelay); deferErr != nil {
			return fmt.Errorf("defer contended task: %w", deferErr)
		}
		w.log.Debug("execution lock held by another owner; deferred wake-up and acknowledged delivery", zap.String("task_id", delivery.TaskID.String()))
		ackDelivery(delivery)
		return nil
	}

	if err := w.runLockedDelivery(ctx, delivery.TaskID, token); err != nil {
		return err
	}
	ackDelivery(delivery)
	return nil
}

// runLockedDelivery 在已持有 Execution Lock 的前提下执行 Step B ~ Step E 全流程：
// 利用 Go 返回前必先执行 defer 的特性，确保正常结束时先释放锁再回到外层确认 Kafka 消费；
// 若业务已完成但收尾放锁偶发报错，只记 Warn 交由短锁 TTL 自然过期，不阻断外层 Ack，避免卡死 Kafka 分区提交水位
// （见 docs/adr/0008-concurrency-fencing-and-flow-control-hardening.md）。
func (w *Worker) runLockedDelivery(ctx context.Context, taskID uuid.UUID, token string) error {
	defer func() {
		releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), lockReleaseTimeout)
		defer cancel()
		if relErr := w.locker.ReleaseExecutionLock(releaseCtx, taskID, token); relErr != nil {
			w.log.Warn(
				"release execution lock failed; lock will expire by TTL",
				zap.String("task_id", taskID.String()),
				zap.Error(relErr),
			)
		}
	}()

	// Step B：加载 Task 与 Endpoint 并执行前置守卫检查。
	task, err := w.tasks.GetTask(ctx, taskID)
	if errors.Is(err, domain.ErrTaskNotFound) {
		w.log.Warn("task not found in postgres; acknowledging delivery", zap.String("task_id", taskID.String()))
		return nil
	}
	if err != nil {
		return fmt.Errorf("load task: %w", err)
	}

	loadedReplay := task.ReplayCount
	loadedAttempt := task.Attempt

	ep, err := w.endpoints.GetEndpoint(ctx, task.EndpointID)
	if errors.Is(err, domain.ErrEndpointNotFound) {
		w.log.Warn(
			"endpoint not found for task; acknowledging delivery",
			zap.String("task_id", task.ID.String()),
			zap.String("endpoint_id", task.EndpointID.String()),
		)
		return nil
	}
	if err != nil {
		return fmt.Errorf("load endpoint: %w", err)
	}

	now := w.now()
	decision, err := task.EvaluatePreDelivery(ep, now)
	if err != nil {
		return fmt.Errorf("evaluate pre-delivery guard: %w", err)
	}
	if decision.AlreadyFinal {
		return nil
	}
	if decision.EarlyArrival {
		// 任务还没到执行时间：先覆盖写回延迟池并清掉旧租约，返回后由外层依次释放 Execution Lock 并确认 Kafka 消费。
		// 同时把剩余等待时长传给 Redis，让脚本按 Redis 自己的时钟至少延后这么久，避免两台机器时间不一致时任务刚放回去又立刻被捞出来。
		if schedErr := w.delayed.ScheduleDelayed(ctx, task.ID, task.NextRunAt, task.NextRunAt.Sub(now)); schedErr != nil {
			return fmt.Errorf("requeue early-arrival task: %w", schedErr)
		}
		w.log.Debug(
			"task arrived before next_run_at; requeued to delayed pool",
			zap.String("task_id", task.ID.String()),
			zap.Time("next_run_at", task.NextRunAt),
		)
		return nil
	}
	if decision.DeadLetterCode != "" {
		if _, err := w.tasks.SavePreDeliveryDeadLetter(ctx, task.ID, loadedReplay, loadedAttempt, decision.DeadLetterCode, now); err != nil {
			return fmt.Errorf("save pre-delivery dead letter: %w", err)
		}
		return nil
	}

	// Step C（锁续期）：在发起出站 HTTP 请求前，按 Endpoint 实际超时与保护余量核对归属后续期执行锁。
	deliveryLockTTL := time.Duration(ep.TimeoutMs)*time.Millisecond + w.cfg.LockGrace
	extended, err := w.locker.ExtendExecutionLock(ctx, taskID, token, deliveryLockTTL)
	if err != nil {
		return fmt.Errorf("extend execution lock: %w", err)
	}
	if !extended {
		return fmt.Errorf("execution lock lost before delivery for task %s", taskID)
	}

	// Step D：原样发送 BYTEA 载荷并自动附带 X-Ferry-* 与 HMAC-SHA256 签名头。
	req := domain.DeliveryRequest{
		TaskID:        task.ID,
		EndpointURL:   ep.TargetURL,
		SigningSecret: ep.SigningSecret,
		AttemptNo:     loadedAttempt + 1,
		Timeout:       time.Duration(ep.TimeoutMs) * time.Millisecond,
		Headers:       task.Headers,
		RawPayload:    task.Payload,
		SentAt:        w.now(),
	}
	result := w.sender.Send(ctx, req)

	// Step E：领域分级演进状态并在单事务内追加 task_attempts + 复合乐观锁 CAS 更新 tasks。
	transition, err := task.RecordOutcome(result, w.now())
	if err != nil {
		return fmt.Errorf("record delivery outcome: %w", err)
	}
	applied, err := w.tasks.SaveDeliveryOutcome(ctx, loadedReplay, loadedAttempt, task, transition.Attempt)
	if err != nil {
		return fmt.Errorf("save delivery outcome: %w", err)
	}
	if !applied {
		w.log.Warn(
			"task status CAS missed due to concurrent transition; delivery attempt audit preserved",
			zap.String("task_id", task.ID.String()),
			zap.Int("replay_count", loadedReplay),
			zap.Int("attempt", loadedAttempt),
		)
	}

	return nil
}

func ackDelivery(delivery domain.ReadyDelivery) {
	if delivery.Ack != nil {
		delivery.Ack()
	}
}

func (w *Worker) runPeriodicCommit(ctx context.Context, stop <-chan struct{}, stopped chan<- struct{}) {
	defer close(stopped)

	ticker := time.NewTicker(periodicCommitPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			commitCtx, cancel := context.WithTimeout(ctx, finalCommitTimeout)
			err := w.stream.CommitCompleted(commitCtx)
			cancel()
			if err != nil {
				w.log.Warn("periodic kafka offset commit failed", zap.Error(err))
			}
		}
	}
}

func (w *Worker) now() time.Time {
	return w.cfg.Clock().UTC()
}

func waitOrCancel(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

package worker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/awu123-create/ferry/internal/domain"
	"github.com/awu123-create/ferry/internal/logger"
)

const (
	// maxEndpointTimeout 对应 Endpoint.TimeoutMs 允许的最大值（60s），用于 Step A 抢锁时覆盖最坏执行时长。
	maxEndpointTimeout   = 60 * time.Second
	lockReleaseTimeout   = 5 * time.Second
	pollErrorBackoff     = 200 * time.Millisecond
	periodicCommitPeriod = 500 * time.Millisecond
	finalCommitTimeout   = 5 * time.Second
)

// Config 配置 Worker 并发度、执行锁保护余量与优雅停机排空超时。
type Config struct {
	Concurrency          int
	LockGrace            time.Duration
	ShutdownDrainTimeout time.Duration
}

// Worker 消费就绪 Task 票据，通过 Execution Lock 与复合乐观锁守卫执行安全，
// 发起带签名的出站 HTTP 投递，并在收到 SIGTERM 时执行优雅排空停机。
type Worker struct {
	log       *zap.Logger
	stream    ReadyStream
	tasks     TaskStore
	endpoints EndpointReader
	locker    ExecutionLocker
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
	if cfg.ShutdownDrainTimeout <= 0 {
		cfg.ShutdownDrainTimeout = 25 * time.Second
	}

	return &Worker{
		log:       log,
		stream:    stream,
		tasks:     tasks,
		endpoints: endpoints,
		locker:    locker,
		sender:    sender,
		cfg:       cfg,
	}, nil
}

// Ready 返回 Worker 当前是否正在接收并处理 Kafka 投递任务（供 /readyz 探针读取）。
func (w *Worker) Ready() bool {
	return w.ready.Load()
}

// Run 持续拉取就绪票据、受控并发投递，并在 ctx 取消时等待在途请求安全排空（ADR-0006 & ADR-0008）。
func (w *Worker) Run(ctx context.Context) error {
	if !w.hasAllDependencies() {
		return fmt.Errorf("worker is missing required dependencies")
	}

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

		tickets, err := w.stream.PollReady(ctx, available)
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

		for _, ticket := range tickets {
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				// 停机已触发，尚未分发的票据不 Ack，退出后交由 Kafka 重平衡重投。
				break pollLoop
			}

			wg.Add(1)
			go func(t domain.ReadyTicket) {
				defer wg.Done()
				defer func() { <-sem }()
				if procErr := w.ProcessTicket(drainCtx, t); procErr != nil {
					w.log.Error(
						"process task ticket failed",
						zap.String("task_id", t.TaskID.String()),
						zap.Error(procErr),
					)
				}
			}(ticket)
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

// ProcessTicket 对单条 Kafka ReadyTicket 执行 Step A~E 全流程处理（ADR-0007 & ADR-0008）。
func (w *Worker) ProcessTicket(ctx context.Context, ticket domain.ReadyTicket) error {
	ctx = logger.WithTaskID(ctx, ticket.TaskID)

	// Step A：先抢占 Redis 执行互斥锁（TTL 覆盖最长合法超时 + 保护余量），确保后续读取到最新任务快照。
	lockTTL := maxEndpointTimeout + w.cfg.LockGrace
	token, acquired, err := w.locker.AcquireExecutionLock(ctx, ticket.TaskID, lockTTL)
	if err != nil {
		return fmt.Errorf("acquire execution lock: %w", err)
	}
	if !acquired {
		// 另一个 Worker 或 Cancel API 正持有该任务锁；Milestone 2 会在此处补充 ZADD NX 保底写入 ferry:delayed 后再 Ack。
		w.log.Debug("execution lock held by another owner; skipping duplicate ticket", zap.String("task_id", ticket.TaskID.String()))
		w.stream.Ack(ticket)
		return nil
	}
	defer func() {
		// 必须在 PG 结算事务提交之后才通过 Lua 验主释放执行锁（ADR-0008 §3.1）。
		releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), lockReleaseTimeout)
		defer cancel()
		if relErr := w.locker.ReleaseExecutionLock(releaseCtx, ticket.TaskID, token); relErr != nil {
			w.log.Warn(
				"release execution lock failed",
				zap.String("task_id", ticket.TaskID.String()),
				zap.Error(relErr),
			)
		}
	}()

	// Step B：加载 Task 与 Endpoint 并执行前置守卫检查。
	task, err := w.tasks.GetTask(ctx, ticket.TaskID)
	if errors.Is(err, domain.ErrTaskNotFound) {
		w.log.Warn("task not found in postgres; acknowledging ticket", zap.String("task_id", ticket.TaskID.String()))
		w.stream.Ack(ticket)
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
			"endpoint not found for task; acknowledging ticket",
			zap.String("task_id", task.ID.String()),
			zap.String("endpoint_id", task.EndpointID.String()),
		)
		w.stream.Ack(ticket)
		return nil
	}
	if err != nil {
		return fmt.Errorf("load endpoint: %w", err)
	}

	now := time.Now().UTC()
	decision, err := task.EvaluatePreDelivery(ep, now)
	if err != nil {
		return fmt.Errorf("evaluate pre-delivery guard: %w", err)
	}
	if decision.AlreadyFinal || decision.EarlyArrival {
		// Milestone 2 在 EarlyArrival 时会先保底 ZADD 回延迟池再 Ack。
		w.stream.Ack(ticket)
		return nil
	}
	if decision.DeadLetterCode != "" {
		if _, err := w.tasks.SavePreDeliveryDeadLetter(ctx, task.ID, loadedReplay, loadedAttempt, decision.DeadLetterCode, now); err != nil {
			return fmt.Errorf("save pre-delivery dead letter: %w", err)
		}
		w.stream.Ack(ticket)
		return nil
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
		SentAt:        time.Now().UTC(),
	}
	result := w.sender.Send(ctx, req)

	// Step E：领域分级演进状态并在单事务内追加 task_attempts + 复合乐观锁 CAS 更新 tasks。
	transition, err := task.RecordOutcome(result, time.Now().UTC())
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

	w.stream.Ack(ticket)
	return nil
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

func (w *Worker) hasAllDependencies() bool {
	return w.stream != nil &&
		w.tasks != nil &&
		w.endpoints != nil &&
		w.locker != nil &&
		w.sender != nil
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

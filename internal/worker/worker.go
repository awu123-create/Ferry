package worker

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/awu123-create/ferry/internal/domain"
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

// Run 持续拉取就绪票据、受控并发投递，并在 ctx 取消时等待在途请求安全排空。
//
// 契约要求（Milestone 1 核心逻辑，留待实现，见 ADR-0006 & ADR-0008）：
//  1. 启动后立即设置 w.ready.Store(true)。
//  2. 主循环直到 ctx.Done()：
//     - 使用容量为 w.cfg.Concurrency 的信号量控制并发，通过 w.stream.PollReady(ctx, availableSlots) 拉取批次；
//     - 为每条票据启动受 sync.WaitGroup 追踪的协程调用 w.ProcessTicket(drainCtx, ticket)；
//     - 周期性调用 w.stream.CommitCompleted(commitCtx) 提交分区连续完成水位。
//  3. 收到 ctx 取消（SIGTERM / SIGINT）后执行优雅停机：
//     - 立即将 w.ready.Store(false) 使 /readyz 返回 503，同时停止向 Kafka 拉取新消息；
//     - 在最长 w.cfg.ShutdownDrainTimeout（25s）内等待全部在途协程完成，期间使用分离后的 drainCtx，
//     确保正在进行的 HTTP 调用仍能完整执行 SaveDeliveryOutcome 与 ReleaseExecutionLock；
//     - 退出前执行最后一次 w.stream.CommitCompleted(commitCtx) 提交已处理完的连续 Offset。
func (w *Worker) Run(ctx context.Context) error {
	if !w.hasAllDependencies() {
		return fmt.Errorf("worker is missing required dependencies")
	}

	w.ready.Store(false)
	w.log.Info(
		"worker loop is not yet implemented; waiting for shutdown",
		zap.Int("concurrency", w.cfg.Concurrency),
		zap.Duration("drain_timeout", w.cfg.ShutdownDrainTimeout),
	)
	<-ctx.Done()
	w.ready.Store(false)
	return nil
}

// ProcessTicket 对单条 Kafka ReadyTicket 执行 Step A~E 全流程处理。
//
// 契约要求（Milestone 1 核心逻辑，留待实现，见 ADR-0007 & ADR-0008）：
//   - Step A（执行互斥锁 Execution Lock）：通过 w.locker.AcquireExecutionLock(ctx, ticket.TaskID, lockTTL)
//     抢占执行权，其中 lockTTL = timeout + w.cfg.LockGrace。
//     若未抢到锁则安全跳过（Milestone 2 会补充 ZADD NX 保底顺延）；
//     务必在 defer 中使用 context.WithoutCancel(ctx) 调用 ReleaseExecutionLock，保证在 PG 事务提交后才放锁。
//   - Step B（投递前守卫）：通过 w.tasks.GetTask 与 w.endpoints.GetEndpoint 加载 Task 与 Endpoint，
//     调用 decision, err := task.EvaluatePreDelivery(ep, now)：
//   - 若 decision.AlreadyFinal 或 decision.EarlyArrival：调用 w.stream.Ack(ticket) 后直接返回 nil
//     （Milestone 2 在 EarlyArrival 时会先 ZADD 回延迟池再 Ack）。
//   - 若 decision.DeadLetterCode != ""：调用 w.tasks.SavePreDeliveryDeadLetter(...) 落入死信并 Ack 票据。
//   - Step D（出站 HTTP 投递）：构造 domain.DeliveryRequest（AttemptNo = task.Attempt + 1，
//     Timeout = time.Duration(ep.TimeoutMs) * time.Millisecond，RawPayload = task.Payload）并调用 w.sender.Send。
//   - Step E（状态结算与审计落盘）：调用 transition, err := task.RecordOutcome(result, time.Now().UTC())，
//     通过 w.tasks.SaveDeliveryOutcome(ctx, loadedReplay, loadedAttempt, task, transition.Attempt) 单事务持久化，
//     完成后调用 w.stream.Ack(ticket) 推进提交水位。
func (w *Worker) ProcessTicket(ctx context.Context, ticket domain.ReadyTicket) error {
	_ = ctx
	_ = ticket
	return domain.ErrNotImplemented
}

func (w *Worker) hasAllDependencies() bool {
	return w.stream != nil &&
		w.tasks != nil &&
		w.endpoints != nil &&
		w.locker != nil &&
		w.sender != nil
}

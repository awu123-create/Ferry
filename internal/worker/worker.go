package worker

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/awu123-create/ferry/internal/domain"
)

// Config configures Worker concurrency, lock TTL grace, and graceful shutdown draining.
type Config struct {
	Concurrency          int
	LockGrace            time.Duration
	ShutdownDrainTimeout time.Duration
}

// Worker consumes ready Task tickets, guards execution with Execution Lock + optimistic CAS,
// dispatches signed HTTP requests, and drains cleanly on SIGTERM.
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

// NewWorker validates dependencies and constructs a Worker ready to start.
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

// Ready reports whether the Worker is actively accepting Kafka deliveries (used by /readyz).
func (w *Worker) Ready() bool {
	return w.ready.Load()
}

// Run polls ready tickets, processes them concurrently, and drains in-flight deliveries on ctx cancel.
//
// Contract (Milestone 1 - core logic to be implemented, ADR-0006 & ADR-0008):
//  1. Mark w.ready.Store(true) on startup.
//  2. Loop until ctx.Done():
//     - Poll batches via w.stream.PollReady(ctx, availableSlots) bounded by a semaphore of w.cfg.Concurrency.
//     - Dispatch each ticket to a goroutine tracked by sync.WaitGroup calling w.ProcessTicket(drainCtx, ticket).
//     - Periodically call w.stream.CommitCompleted(commitCtx) for contiguous completed watermarks.
//  3. When ctx is cancelled (SIGTERM / SIGINT):
//     - Immediately set w.ready.Store(false) so /readyz turns 503 and stop polling new tickets.
//     - Wait for all in-flight goroutines up to w.cfg.ShutdownDrainTimeout (25s) using a detached drain context
//     so active HTTP calls still finish SaveDeliveryOutcome and ReleaseExecutionLock cleanly.
//     - Issue a final w.stream.CommitCompleted(commitCtx) before returning.
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

// ProcessTicket executes Step A~E for one Kafka ReadyTicket.
//
// Contract (Milestone 1 - core logic to be implemented, ADR-0007 & ADR-0008):
//   - Step A (Execution Lock): Load task/endpoint or acquire Execution Lock via
//     w.locker.AcquireExecutionLock(ctx, ticket.TaskID, lockTTL) where lockTTL = timeout + w.cfg.LockGrace.
//     If not acquired, skip safely (Milestone 2 will add ZADD NX fallback); always ReleaseExecutionLock
//     in defer with context.WithoutCancel(ctx) AFTER DB outcome persistence.
//   - Step B (Pre-Delivery Guard): Load task via w.tasks.GetTask and endpoint via w.endpoints.GetEndpoint.
//     Call decision, err := task.EvaluatePreDelivery(ep, now):
//   - If decision.AlreadyFinal or decision.EarlyArrival: call w.stream.Ack(ticket) and return nil
//     (Milestone 2 adds ZADD back to ferry:delayed before ack on EarlyArrival).
//   - If decision.DeadLetterCode != "": call w.tasks.SavePreDeliveryDeadLetter(...) and w.stream.Ack(ticket).
//   - Step D (Outbound HTTP): Build domain.DeliveryRequest (with AttemptNo = task.Attempt+1,
//     Timeout = time.Duration(ep.TimeoutMs)*time.Millisecond, RawPayload = task.Payload) and call w.sender.Send.
//   - Step E (Outcome Persistence): Call transition, err := task.RecordOutcome(result, time.Now().UTC()),
//     persist via w.tasks.SaveDeliveryOutcome(ctx, loadedReplay, loadedAttempt, task, transition.Attempt),
//     and ack the ticket via w.stream.Ack(ticket) on success.
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

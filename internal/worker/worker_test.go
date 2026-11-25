package worker_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/awu123-create/ferry/internal/domain"
	"github.com/awu123-create/ferry/internal/worker"
)

type fakeReadyStream struct {
	mu          sync.Mutex
	pollCh      chan []domain.ReadyDelivery
	acked       []uuid.UUID
	onAck       func(taskID uuid.UUID)
	commitCount atomic.Int32
}

func newFakeReadyStream() *fakeReadyStream {
	return &fakeReadyStream{
		pollCh: make(chan []domain.ReadyDelivery, 8),
	}
}

func (s *fakeReadyStream) newDelivery(taskID uuid.UUID) domain.ReadyDelivery {
	return domain.ReadyDelivery{
		TaskID: taskID,
		Ack: func() {
			s.mu.Lock()
			s.acked = append(s.acked, taskID)
			cb := s.onAck
			s.mu.Unlock()
			if cb != nil {
				cb(taskID)
			}
		},
	}
}

func (s *fakeReadyStream) PollReady(ctx context.Context, _ int) ([]domain.ReadyDelivery, error) {
	select {
	case deliveries := <-s.pollCh:
		return deliveries, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *fakeReadyStream) CommitCompleted(_ context.Context) error {
	s.commitCount.Add(1)
	return nil
}

type fakeTaskStore struct {
	mu              sync.Mutex
	tasks           map[uuid.UUID]domain.Task
	attempts        []domain.DeliveryAttempt
	preDLQReasons   map[uuid.UUID]string
	getErr          error
	onSaveOutcome   func()
	lockHeldAtSave  bool
	checkLockHeldFn func(uuid.UUID) bool
}

func newFakeTaskStore() *fakeTaskStore {
	return &fakeTaskStore{
		tasks:         make(map[uuid.UUID]domain.Task),
		preDLQReasons: make(map[uuid.UUID]string),
	}
}

func (s *fakeTaskStore) GetTask(_ context.Context, id uuid.UUID) (domain.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.getErr != nil {
		return domain.Task{}, s.getErr
	}
	task, ok := s.tasks[id]
	if !ok {
		return domain.Task{}, domain.ErrTaskNotFound
	}
	return task, nil
}

func (s *fakeTaskStore) SavePreDeliveryDeadLetter(
	_ context.Context,
	taskID uuid.UUID,
	expectedReplayCount, expectedAttempt int,
	reason string,
	now time.Time,
) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	task, ok := s.tasks[taskID]
	if !ok || task.ReplayCount != expectedReplayCount || task.Attempt != expectedAttempt || !task.Status.IsActive() {
		return false, nil
	}
	ts := now.UTC()
	task.Status = domain.TaskStatusDeadLetter
	task.LastError = reason
	task.CompletedAt = &ts
	task.UpdatedAt = ts
	s.tasks[taskID] = task
	s.preDLQReasons[taskID] = reason
	return true, nil
}

func (s *fakeTaskStore) SaveDeliveryOutcome(
	_ context.Context,
	expectedReplayCount, expectedAttempt int,
	updated domain.Task,
	attempt domain.DeliveryAttempt,
) (bool, error) {
	s.mu.Lock()
	if s.checkLockHeldFn != nil {
		s.lockHeldAtSave = s.checkLockHeldFn(updated.ID)
	}
	s.attempts = append(s.attempts, attempt)
	current, ok := s.tasks[updated.ID]
	applied := ok && current.ReplayCount == expectedReplayCount && current.Attempt == expectedAttempt && current.Status.IsActive()
	if applied {
		s.tasks[updated.ID] = updated
	}
	cb := s.onSaveOutcome
	s.mu.Unlock()
	if cb != nil {
		cb()
	}
	return applied, nil
}

type fakeEndpointReader struct {
	mu        sync.Mutex
	endpoints map[uuid.UUID]domain.Endpoint
}

func (r *fakeEndpointReader) GetEndpoint(_ context.Context, id uuid.UUID) (domain.Endpoint, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	ep, ok := r.endpoints[id]
	if !ok {
		return domain.Endpoint{}, domain.ErrEndpointNotFound
	}
	return ep, nil
}

type fakeLocker struct {
	mu              sync.Mutex
	held            map[uuid.UUID]string
	acquireTTL      time.Duration
	extendTTL       time.Duration
	extendCount     int
	releaseCount    int
	releaseAttempts int
	releaseErr      error
}

func newFakeLocker() *fakeLocker {
	return &fakeLocker{held: make(map[uuid.UUID]string)}
}

func (l *fakeLocker) AcquireExecutionLock(_ context.Context, taskID uuid.UUID, ttl time.Duration) (string, bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, exists := l.held[taskID]; exists {
		return "", false, nil
	}
	token := uuid.NewString()
	l.held[taskID] = token
	l.acquireTTL = ttl
	return token, true, nil
}

func (l *fakeLocker) ExtendExecutionLock(_ context.Context, taskID uuid.UUID, token string, ttl time.Duration) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	current, ok := l.held[taskID]
	if !ok || current != token {
		return false, nil
	}
	l.extendTTL = ttl
	l.extendCount++
	return true, nil
}

func (l *fakeLocker) ReleaseExecutionLock(_ context.Context, taskID uuid.UUID, token string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.releaseAttempts++
	if l.releaseErr != nil {
		return l.releaseErr
	}
	if current, ok := l.held[taskID]; ok && current == token {
		delete(l.held, taskID)
		l.releaseCount++
	}
	return nil
}

func (l *fakeLocker) isHeld(taskID uuid.UUID) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	_, ok := l.held[taskID]
	return ok
}

type scheduledEntry struct {
	taskID   uuid.UUID
	runAt    time.Time
	minDelay time.Duration
}

type contendedEntry struct {
	taskID        uuid.UUID
	fallbackDelay time.Duration
}

type fakeDelayedScheduler struct {
	mu                 sync.Mutex
	scheduled          []scheduledEntry
	contended          []contendedEntry
	scheduleErr        error
	deferErr           error
	lockHeldOnSchedule bool
	checkLockHeldFn    func(uuid.UUID) bool
}

func (d *fakeDelayedScheduler) ScheduleDelayed(_ context.Context, taskID uuid.UUID, runAt time.Time, minDelay time.Duration) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.checkLockHeldFn != nil {
		d.lockHeldOnSchedule = d.checkLockHeldFn(taskID)
	}
	if d.scheduleErr != nil {
		return d.scheduleErr
	}
	d.scheduled = append(d.scheduled, scheduledEntry{taskID: taskID, runAt: runAt, minDelay: minDelay})
	return nil
}

func (d *fakeDelayedScheduler) DeferContendedTask(_ context.Context, taskID uuid.UUID, fallbackDelay time.Duration) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.deferErr != nil {
		return d.deferErr
	}
	d.contended = append(d.contended, contendedEntry{taskID: taskID, fallbackDelay: fallbackDelay})
	return nil
}

type fakeSender struct {
	sendFn func(ctx context.Context, req domain.DeliveryRequest) domain.DeliveryResult
	calls  atomic.Int32
}

func (s *fakeSender) Send(ctx context.Context, req domain.DeliveryRequest) domain.DeliveryResult {
	s.calls.Add(1)
	if s.sendFn != nil {
		return s.sendFn(ctx, req)
	}
	return domain.DeliveryResult{HTTPStatus: 200, DurationMs: 5, ErrorKind: domain.AttemptErrorNone}
}

func mustNewTaskFixture(t *testing.T, enabled bool) (domain.Endpoint, domain.Task) {
	t.Helper()
	now := time.Now().UTC()
	ep, err := domain.NewEndpoint(domain.NewEndpointParams{
		Name:          "orders",
		TargetURL:     "http://localhost:8081/webhook",
		SigningSecret: "whsec_test",
	}, now)
	if err != nil {
		t.Fatalf("NewEndpoint failed: %v", err)
	}
	task, err := domain.NewTask(domain.NewTaskParams{
		Endpoint:       ep,
		IdempotencyKey: uuid.NewString(),
		RawPayload:     []byte(`{"z": 1,  "a": "ok"}`),
		MaxAttempts:    1,
		DefaultWindow:  time.Hour,
		Now:            now,
	})
	if err != nil {
		t.Fatalf("NewTask failed: %v", err)
	}
	ep.Enabled = enabled
	return ep, task
}

func TestNewWorker_ConstructorValidation(t *testing.T) {
	if _, err := worker.NewWorker(zap.NewNop(), nil, newFakeTaskStore(), &fakeEndpointReader{}, newFakeLocker(), &fakeDelayedScheduler{}, &fakeSender{}, worker.Config{}); err == nil {
		t.Fatal("expected error when ReadyStream is nil")
	}
	if _, err := worker.NewWorker(zap.NewNop(), newFakeReadyStream(), newFakeTaskStore(), &fakeEndpointReader{}, newFakeLocker(), nil, &fakeSender{}, worker.Config{}); err == nil {
		t.Fatal("expected error when DelayedScheduler is nil")
	}
}

func TestWorker_ProcessDeliverySuccessLockOrderingAndAudit(t *testing.T) {
	ep, task := mustNewTaskFixture(t, true)
	stream := newFakeReadyStream()
	tasks := newFakeTaskStore()
	tasks.tasks[task.ID] = task
	locker := newFakeLocker()
	tasks.checkLockHeldFn = locker.isHeld
	endpoints := &fakeEndpointReader{endpoints: map[uuid.UUID]domain.Endpoint{ep.ID: ep}}

	var gotReq domain.DeliveryRequest
	sender := &fakeSender{
		sendFn: func(_ context.Context, req domain.DeliveryRequest) domain.DeliveryResult {
			gotReq = req
			return domain.DeliveryResult{HTTPStatus: 200, DurationMs: 12, ErrorKind: domain.AttemptErrorNone}
		},
	}

	const lockGrace = 12 * time.Second
	w, err := worker.NewWorker(zap.NewNop(), stream, tasks, endpoints, locker, &fakeDelayedScheduler{}, sender, worker.Config{
		LockGrace: lockGrace,
	})
	if err != nil {
		t.Fatalf("NewWorker failed: %v", err)
	}

	lockHeldAtAck := true
	stream.onAck = func(id uuid.UUID) {
		lockHeldAtAck = locker.isHeld(id)
	}
	delivery := stream.newDelivery(task.ID)
	if err := w.ProcessDelivery(context.Background(), delivery); err != nil {
		t.Fatalf("ProcessDelivery failed: %v", err)
	}

	// 验证写数据库时锁还拿着，但在调用 Ack 之前已经释放，并且按 Endpoint 实际超时续过期。
	if !tasks.lockHeldAtSave {
		t.Fatal("expected execution lock to remain held while SaveDeliveryOutcome executes")
	}
	if lockHeldAtAck {
		t.Fatal("expected execution lock released before delivery.Ack is called")
	}
	wantExtendTTL := time.Duration(ep.TimeoutMs)*time.Millisecond + lockGrace
	if locker.acquireTTL != 15*time.Second || locker.extendCount != 1 || locker.extendTTL != wantExtendTTL {
		t.Fatalf("unexpected lock TTLs: acquire=%v extend=%v (want %v) count=%d", locker.acquireTTL, locker.extendTTL, wantExtendTTL, locker.extendCount)
	}
	if locker.isHeld(task.ID) || locker.releaseCount != 1 {
		t.Fatalf("expected execution lock released after commit, held=%v releases=%d", locker.isHeld(task.ID), locker.releaseCount)
	}
	if gotReq.AttemptNo != 1 || string(gotReq.RawPayload) != string(task.Payload) {
		t.Fatalf("unexpected delivery request: %+v", gotReq)
	}
	saved := tasks.tasks[task.ID]
	if saved.Status != domain.TaskStatusSucceeded || saved.Attempt != 1 || len(tasks.attempts) != 1 {
		t.Fatalf("unexpected persisted task=%+v attempts=%d", saved, len(tasks.attempts))
	}
	if len(stream.acked) != 1 || stream.acked[0] != task.ID {
		t.Fatalf("expected delivery acknowledged, got %+v", stream.acked)
	}
}

func TestWorker_ProcessDeliveryNonRetryablePreDisabledAndLockContention(t *testing.T) {
	t.Run("non_retryable_400", func(t *testing.T) {
		ep, task := mustNewTaskFixture(t, true)
		stream := newFakeReadyStream()
		tasks := newFakeTaskStore()
		tasks.tasks[task.ID] = task
		locker := newFakeLocker()
		endpoints := &fakeEndpointReader{endpoints: map[uuid.UUID]domain.Endpoint{ep.ID: ep}}
		sender := &fakeSender{
			sendFn: func(_ context.Context, _ domain.DeliveryRequest) domain.DeliveryResult {
				return domain.DeliveryResult{HTTPStatus: 400, DurationMs: 4, ErrorKind: domain.AttemptErrorHTTP4xx, ErrorMessage: "HTTP 400"}
			},
		}

		w, err := worker.NewWorker(zap.NewNop(), stream, tasks, endpoints, locker, &fakeDelayedScheduler{}, sender, worker.Config{})
		if err != nil {
			t.Fatalf("NewWorker failed: %v", err)
		}
		if err := w.ProcessDelivery(context.Background(), stream.newDelivery(task.ID)); err != nil {
			t.Fatalf("ProcessDelivery failed: %v", err)
		}
		saved := tasks.tasks[task.ID]
		if saved.Status != domain.TaskStatusDeadLetter || saved.Attempt != 1 || saved.LastError != domain.DeadLetterReasonNonRetryable4xx {
			t.Fatalf("unexpected task state after 400: %+v", saved)
		}
	})

	t.Run("disabled_endpoint", func(t *testing.T) {
		ep, task := mustNewTaskFixture(t, false)
		stream := newFakeReadyStream()
		tasks := newFakeTaskStore()
		tasks.tasks[task.ID] = task
		locker := newFakeLocker()
		endpoints := &fakeEndpointReader{endpoints: map[uuid.UUID]domain.Endpoint{ep.ID: ep}}
		sender := &fakeSender{}

		w, err := worker.NewWorker(zap.NewNop(), stream, tasks, endpoints, locker, &fakeDelayedScheduler{}, sender, worker.Config{})
		if err != nil {
			t.Fatalf("NewWorker failed: %v", err)
		}
		if err := w.ProcessDelivery(context.Background(), stream.newDelivery(task.ID)); err != nil {
			t.Fatalf("ProcessDelivery failed: %v", err)
		}
		if sender.calls.Load() != 0 {
			t.Fatalf("expected 0 HTTP calls for disabled endpoint, got %d", sender.calls.Load())
		}
		if tasks.tasks[task.ID].Status != domain.TaskStatusDeadLetter || tasks.preDLQReasons[task.ID] != domain.DeadLetterReasonEndpointDisabled {
			t.Fatalf("expected pre-delivery DEAD_LETTER(ENDPOINT_DISABLED), got %+v", tasks.tasks[task.ID])
		}
	})

	t.Run("lock_contention_defers_and_fails_without_ack", func(t *testing.T) {
		ep, task := mustNewTaskFixture(t, true)
		stream := newFakeReadyStream()
		tasks := newFakeTaskStore()
		tasks.tasks[task.ID] = task
		locker := newFakeLocker()
		locker.held[task.ID] = "other-worker"
		delayed := &fakeDelayedScheduler{}
		endpoints := &fakeEndpointReader{endpoints: map[uuid.UUID]domain.Endpoint{ep.ID: ep}}
		sender := &fakeSender{}

		const fallbackDelay = 3 * time.Second
		w, err := worker.NewWorker(zap.NewNop(), stream, tasks, endpoints, locker, delayed, sender, worker.Config{
			LockFallbackDelay: fallbackDelay,
		})
		if err != nil {
			t.Fatalf("NewWorker failed: %v", err)
		}
		if err := w.ProcessDelivery(context.Background(), stream.newDelivery(task.ID)); err != nil {
			t.Fatalf("ProcessDelivery failed: %v", err)
		}
		if sender.calls.Load() != 0 || len(stream.acked) != 1 {
			t.Fatalf("expected defer + ack on lock contention, calls=%d acked=%d", sender.calls.Load(), len(stream.acked))
		}
		if len(delayed.contended) != 1 || delayed.contended[0].taskID != task.ID || delayed.contended[0].fallbackDelay != fallbackDelay {
			t.Fatalf("expected DeferContendedTask called with fallback=%v, got %+v", fallbackDelay, delayed.contended)
		}

		// 写 Redis 失败时必须返回错误且不确认消费位移。
		delayed.deferErr = errors.New("redis down")
		if err := w.ProcessDelivery(context.Background(), stream.newDelivery(task.ID)); err == nil {
			t.Fatal("expected error when DeferContendedTask fails")
		}
		if len(stream.acked) != 1 {
			t.Fatalf("expected no ack when DeferContendedTask fails, acked=%d", len(stream.acked))
		}
	})

	t.Run("lock_contention_terminal_or_missing_task_skips_defer_and_acks", func(t *testing.T) {
		ep, task := mustNewTaskFixture(t, true)
		task.Status = domain.TaskStatusCancelled
		missingID := uuid.New()

		stream := newFakeReadyStream()
		tasks := newFakeTaskStore()
		tasks.tasks[task.ID] = task
		locker := newFakeLocker()
		locker.held[task.ID] = "cancel-api"
		locker.held[missingID] = "cancel-api"
		delayed := &fakeDelayedScheduler{}
		endpoints := &fakeEndpointReader{endpoints: map[uuid.UUID]domain.Endpoint{ep.ID: ep}}
		sender := &fakeSender{}

		w, err := worker.NewWorker(zap.NewNop(), stream, tasks, endpoints, locker, delayed, sender, worker.Config{
			LockFallbackDelay: 3 * time.Second,
		})
		if err != nil {
			t.Fatalf("NewWorker failed: %v", err)
		}

		// 撞锁时先查 PG 读到 CANCELLED 或记录不存在：直接 Ack，0 次写延迟池。
		if err := w.ProcessDelivery(context.Background(), stream.newDelivery(task.ID)); err != nil {
			t.Fatalf("ProcessDelivery(cancelled contention) failed: %v", err)
		}
		if err := w.ProcessDelivery(context.Background(), stream.newDelivery(missingID)); err != nil {
			t.Fatalf("ProcessDelivery(missing contention) failed: %v", err)
		}
		if len(delayed.contended) != 0 || len(stream.acked) != 2 {
			t.Fatalf("expected 0 DeferContendedTask calls and 2 acks, contended=%+v acked=%d", delayed.contended, len(stream.acked))
		}
	})

	t.Run("early_arrival_requeues_releases_lock_before_ack_and_fails_without_ack", func(t *testing.T) {
		now := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)
		futureRunAt := now.Add(30 * time.Second)
		ep, task := mustNewTaskFixture(t, true)
		task.CreatedAt = now
		task.NextRunAt = futureRunAt
		task.DeadlineAt = futureRunAt.Add(time.Hour)

		stream := newFakeReadyStream()
		tasks := newFakeTaskStore()
		tasks.tasks[task.ID] = task
		locker := newFakeLocker()
		lockHeldAtAck := true
		stream.onAck = func(id uuid.UUID) {
			lockHeldAtAck = locker.isHeld(id)
		}
		delayed := &fakeDelayedScheduler{checkLockHeldFn: locker.isHeld}
		endpoints := &fakeEndpointReader{endpoints: map[uuid.UUID]domain.Endpoint{ep.ID: ep}}
		sender := &fakeSender{}

		w, err := worker.NewWorker(zap.NewNop(), stream, tasks, endpoints, locker, delayed, sender, worker.Config{
			Clock: func() time.Time { return now },
		})
		if err != nil {
			t.Fatalf("NewWorker failed: %v", err)
		}
		if err := w.ProcessDelivery(context.Background(), stream.newDelivery(task.ID)); err != nil {
			t.Fatalf("ProcessDelivery(early) failed: %v", err)
		}
		if sender.calls.Load() != 0 || len(stream.acked) != 1 {
			t.Fatalf("expected requeue + ack on early arrival, calls=%d acked=%d", sender.calls.Load(), len(stream.acked))
		}
		if len(delayed.scheduled) != 1 || delayed.scheduled[0].taskID != task.ID ||
			!delayed.scheduled[0].runAt.Equal(futureRunAt) || delayed.scheduled[0].minDelay != futureRunAt.Sub(now) {
			t.Fatalf("expected ScheduleDelayed(%s, %v, %v), got %+v", task.ID, futureRunAt, futureRunAt.Sub(now), delayed.scheduled)
		}
		if !delayed.lockHeldOnSchedule || lockHeldAtAck || locker.isHeld(task.ID) {
			t.Fatalf("expected lock held during ScheduleDelayed and released before Ack, heldDuring=%v heldAtAck=%v heldAfter=%v", delayed.lockHeldOnSchedule, lockHeldAtAck, locker.isHeld(task.ID))
		}

		// 早到写回延迟池失败时必须返回错误且不 Ack。
		delayed.scheduleErr = errors.New("redis down")
		if err := w.ProcessDelivery(context.Background(), stream.newDelivery(task.ID)); err == nil {
			t.Fatal("expected error when ScheduleDelayed fails on early arrival")
		}
		if len(stream.acked) != 1 {
			t.Fatalf("expected no ack when ScheduleDelayed fails, acked=%d", len(stream.acked))
		}
	})
}

func TestWorker_RunGracefulDrainOnCancel(t *testing.T) {
	ep, task := mustNewTaskFixture(t, true)
	stream := newFakeReadyStream()
	tasks := newFakeTaskStore()
	tasks.tasks[task.ID] = task
	locker := newFakeLocker()
	endpoints := &fakeEndpointReader{endpoints: map[uuid.UUID]domain.Endpoint{ep.ID: ep}}

	inFlightStarted := make(chan struct{})
	releaseHTTP := make(chan struct{})
	sender := &fakeSender{
		sendFn: func(ctx context.Context, _ domain.DeliveryRequest) domain.DeliveryResult {
			close(inFlightStarted)
			select {
			case <-releaseHTTP:
				return domain.DeliveryResult{HTTPStatus: 200, DurationMs: 50, ErrorKind: domain.AttemptErrorNone}
			case <-ctx.Done():
				return domain.DeliveryResult{ErrorKind: domain.AttemptErrorTimeout, ErrorMessage: ctx.Err().Error()}
			}
		},
	}

	w, err := worker.NewWorker(zap.NewNop(), stream, tasks, endpoints, locker, &fakeDelayedScheduler{}, sender, worker.Config{
		Concurrency:          4,
		LockGrace:            5 * time.Second,
		ShutdownDrainTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewWorker failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	runErrCh := make(chan error, 1)
	go func() {
		runErrCh <- w.Run(ctx)
	}()

	stream.pollCh <- []domain.ReadyDelivery{stream.newDelivery(task.ID)}

	// 等在途 HTTP 真正进入发送阶段后触发 SIGTERM（cancel）。
	select {
	case <-inFlightStarted:
	case <-time.After(time.Second):
		cancel()
		t.Fatal("timed out waiting for in-flight delivery to start")
	}

	if !w.Ready() {
		t.Fatal("expected Ready() == true while running")
	}

	cancel()

	// 取消后 /readyz 应立即变为 false，同时在途请求不被腰斩。
	deadline := time.Now().Add(time.Second)
	for w.Ready() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if w.Ready() {
		t.Fatal("expected Ready() == false immediately on shutdown")
	}

	// 放行在途 HTTP 响应，确认排空完成、状态落盘为 SUCCEEDED 且执行最终 CommitCompleted。
	close(releaseHTTP)

	select {
	case err := <-runErrCh:
		if err != nil {
			t.Fatalf("Worker.Run returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for Worker.Run graceful exit")
	}

	tasks.mu.Lock()
	finalStatus := tasks.tasks[task.ID].Status
	tasks.mu.Unlock()
	if finalStatus != domain.TaskStatusSucceeded {
		t.Fatalf("expected in-flight task to finish SUCCEEDED during drain, got %s", finalStatus)
	}
	if stream.commitCount.Load() == 0 {
		t.Fatal("expected CommitCompleted called during graceful shutdown")
	}
	if locker.isHeld(task.ID) {
		t.Fatal("expected execution lock released before worker exit")
	}
}

func TestWorker_ProcessDeliveryReleaseLockOrderingAndFailureHandling(t *testing.T) {
	now := time.Date(2026, 11, 24, 10, 0, 0, 0, time.UTC)
	unlockErr := errors.New("redis unlock timeout")

	// 1. 覆盖全部 6 处正常结束出口：
	//    - 正常放锁时，必须严格先释放 Execution Lock 再调 Ack；
	//    - 业务已完成后放锁偶发失败时，同样先尝试放锁 1 次并记 Warn 交由锁 TTL 自然过期，随后照常返回 nil 并 Ack，避免卡死 Kafka 分区提交水位。
	branches := []struct {
		name  string
		setup func(tasks *fakeTaskStore, endpoints *fakeEndpointReader) uuid.UUID
	}{
		{
			name: "task_not_found",
			setup: func(_ *fakeTaskStore, _ *fakeEndpointReader) uuid.UUID {
				return uuid.New()
			},
		},
		{
			name: "endpoint_not_found",
			setup: func(tasks *fakeTaskStore, _ *fakeEndpointReader) uuid.UUID {
				_, task := mustNewTaskFixture(t, true)
				tasks.tasks[task.ID] = task
				return task.ID
			},
		},
		{
			name: "already_final",
			setup: func(tasks *fakeTaskStore, endpoints *fakeEndpointReader) uuid.UUID {
				ep, task := mustNewTaskFixture(t, true)
				task.Status = domain.TaskStatusSucceeded
				tasks.tasks[task.ID] = task
				endpoints.endpoints[ep.ID] = ep
				return task.ID
			},
		},
		{
			name: "early_arrival",
			setup: func(tasks *fakeTaskStore, endpoints *fakeEndpointReader) uuid.UUID {
				ep, task := mustNewTaskFixture(t, true)
				task.CreatedAt = now
				task.NextRunAt = now.Add(30 * time.Second)
				task.DeadlineAt = now.Add(time.Hour)
				tasks.tasks[task.ID] = task
				endpoints.endpoints[ep.ID] = ep
				return task.ID
			},
		},
		{
			name: "pre_delivery_dead_letter",
			setup: func(tasks *fakeTaskStore, endpoints *fakeEndpointReader) uuid.UUID {
				ep, task := mustNewTaskFixture(t, false)
				task.CreatedAt = now
				task.NextRunAt = now
				task.DeadlineAt = now.Add(time.Hour)
				tasks.tasks[task.ID] = task
				endpoints.endpoints[ep.ID] = ep
				return task.ID
			},
		},
		{
			name: "delivery_outcome_saved",
			setup: func(tasks *fakeTaskStore, endpoints *fakeEndpointReader) uuid.UUID {
				ep, task := mustNewTaskFixture(t, true)
				task.CreatedAt = now
				task.NextRunAt = now
				task.DeadlineAt = now.Add(time.Hour)
				tasks.tasks[task.ID] = task
				endpoints.endpoints[ep.ID] = ep
				return task.ID
			},
		},
	}

	for _, tc := range branches {
		t.Run("release_before_ack_"+tc.name, func(t *testing.T) {
			stream := newFakeReadyStream()
			tasks := newFakeTaskStore()
			endpoints := &fakeEndpointReader{endpoints: make(map[uuid.UUID]domain.Endpoint)}
			locker := newFakeLocker()
			delayed := &fakeDelayedScheduler{}
			sender := &fakeSender{}

			lockHeldAtAck := true
			releaseAttemptsAtAck := 0
			stream.onAck = func(id uuid.UUID) {
				lockHeldAtAck = locker.isHeld(id)
				locker.mu.Lock()
				releaseAttemptsAtAck = locker.releaseAttempts
				locker.mu.Unlock()
			}

			taskID := tc.setup(tasks, endpoints)
			w, err := worker.NewWorker(zap.NewNop(), stream, tasks, endpoints, locker, delayed, sender, worker.Config{
				Clock: func() time.Time { return now },
			})
			if err != nil {
				t.Fatalf("NewWorker failed: %v", err)
			}

			if err := w.ProcessDelivery(context.Background(), stream.newDelivery(taskID)); err != nil {
				t.Fatalf("ProcessDelivery failed: %v", err)
			}
			if lockHeldAtAck || releaseAttemptsAtAck != 1 {
				t.Fatalf("expected lock released strictly before Ack, heldAtAck=%v attemptsAtAck=%d", lockHeldAtAck, releaseAttemptsAtAck)
			}
			if len(stream.acked) != 1 || locker.releaseAttempts != 1 {
				t.Fatalf("expected 1 ack and 1 unlock attempt, acked=%d attempts=%d", len(stream.acked), locker.releaseAttempts)
			}
		})

		t.Run("release_failure_warns_and_acks_"+tc.name, func(t *testing.T) {
			core, logs := observer.New(zapcore.WarnLevel)
			stream := newFakeReadyStream()
			tasks := newFakeTaskStore()
			endpoints := &fakeEndpointReader{endpoints: make(map[uuid.UUID]domain.Endpoint)}
			locker := newFakeLocker()
			locker.releaseErr = unlockErr
			delayed := &fakeDelayedScheduler{}
			sender := &fakeSender{}

			releaseAttemptsAtAck := 0
			stream.onAck = func(_ uuid.UUID) {
				locker.mu.Lock()
				releaseAttemptsAtAck = locker.releaseAttempts
				locker.mu.Unlock()
			}

			taskID := tc.setup(tasks, endpoints)
			w, err := worker.NewWorker(zap.New(core), stream, tasks, endpoints, locker, delayed, sender, worker.Config{
				Clock: func() time.Time { return now },
			})
			if err != nil {
				t.Fatalf("NewWorker failed: %v", err)
			}

			if err := w.ProcessDelivery(context.Background(), stream.newDelivery(taskID)); err != nil {
				t.Fatalf("expected nil error when only cleanup unlock fails, got %v", err)
			}
			if len(stream.acked) != 1 || releaseAttemptsAtAck != 1 || locker.releaseAttempts != 1 {
				t.Fatalf("expected 1 unlock attempt before 1 ack, acked=%d attemptsAtAck=%d totalAttempts=%d", len(stream.acked), releaseAttemptsAtAck, locker.releaseAttempts)
			}
			if logs.FilterMessage("release execution lock failed; lock will expire by TTL").Len() != 1 {
				t.Fatalf("expected 1 Warn log for unlock failure, got %d entries", logs.Len())
			}
		})
	}

	// 2. 真正的业务步骤（如 GetTask）先报错，同时 defer 放锁也失败时：保留原始业务错误且绝不 Ack。
	t.Run("primary_error_preserved_and_not_acked_when_defer_unlock_also_fails", func(t *testing.T) {
		core, logs := observer.New(zapcore.WarnLevel)
		primaryErr := errors.New("pg connection reset")
		stream := newFakeReadyStream()
		tasks := newFakeTaskStore()
		tasks.getErr = primaryErr
		locker := newFakeLocker()
		locker.releaseErr = unlockErr

		w, err := worker.NewWorker(zap.New(core), stream, tasks, &fakeEndpointReader{}, locker, &fakeDelayedScheduler{}, &fakeSender{}, worker.Config{
			Clock: func() time.Time { return now },
		})
		if err != nil {
			t.Fatalf("NewWorker failed: %v", err)
		}

		err = w.ProcessDelivery(context.Background(), stream.newDelivery(uuid.New()))
		if !errors.Is(err, primaryErr) {
			t.Fatalf("expected primary GetTask error preserved, got %v", err)
		}
		if len(stream.acked) != 0 || locker.releaseAttempts != 1 {
			t.Fatalf("expected 0 acks and 1 defer unlock attempt, acked=%d attempts=%d", len(stream.acked), locker.releaseAttempts)
		}
		if logs.FilterMessage("release execution lock failed; lock will expire by TTL").Len() != 1 {
			t.Fatalf("expected 1 Warn log for defer unlock failure, got %d entries", logs.Len())
		}
	})

	// 3. 内层处理发生 panic 时：单 defer 仍会先释放执行锁，且绝不推进外层 Ack。
	t.Run("panic_releases_lock_without_ack", func(t *testing.T) {
		ep, task := mustNewTaskFixture(t, true)
		task.CreatedAt = now
		task.NextRunAt = now
		task.DeadlineAt = now.Add(time.Hour)

		stream := newFakeReadyStream()
		tasks := newFakeTaskStore()
		tasks.tasks[task.ID] = task
		endpoints := &fakeEndpointReader{endpoints: map[uuid.UUID]domain.Endpoint{ep.ID: ep}}
		locker := newFakeLocker()
		sender := &fakeSender{
			sendFn: func(_ context.Context, _ domain.DeliveryRequest) domain.DeliveryResult {
				panic("boom during outbound send")
			},
		}

		w, err := worker.NewWorker(zap.NewNop(), stream, tasks, endpoints, locker, &fakeDelayedScheduler{}, sender, worker.Config{
			Clock: func() time.Time { return now },
		})
		if err != nil {
			t.Fatalf("NewWorker failed: %v", err)
		}

		panicked := false
		func() {
			defer func() {
				if r := recover(); r != nil {
					panicked = true
				}
			}()
			_ = w.ProcessDelivery(context.Background(), stream.newDelivery(task.ID))
		}()

		if !panicked {
			t.Fatal("expected panic to propagate")
		}
		if locker.isHeld(task.ID) || locker.releaseCount != 1 {
			t.Fatalf("expected defer to release lock on panic, held=%v releases=%d", locker.isHeld(task.ID), locker.releaseCount)
		}
		if len(stream.acked) != 0 {
			t.Fatalf("expected 0 acks when panic occurs, got %d", len(stream.acked))
		}
	})
}

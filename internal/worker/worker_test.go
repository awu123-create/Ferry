package worker_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/awu123-create/ferry/internal/domain"
	"github.com/awu123-create/ferry/internal/worker"
)

type fakeReadyStream struct {
	mu          sync.Mutex
	pollCh      chan []domain.ReadyDelivery
	acked       []uuid.UUID
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
			s.mu.Unlock()
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
	mu           sync.Mutex
	held         map[uuid.UUID]string
	acquireTTL   time.Duration
	extendTTL    time.Duration
	extendCount  int
	releaseCount int
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
	if _, err := worker.NewWorker(zap.NewNop(), nil, newFakeTaskStore(), &fakeEndpointReader{}, newFakeLocker(), &fakeSender{}, worker.Config{}); err == nil {
		t.Fatal("expected error when ReadyStream is nil")
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
	w, err := worker.NewWorker(zap.NewNop(), stream, tasks, endpoints, locker, sender, worker.Config{
		LockGrace: lockGrace,
	})
	if err != nil {
		t.Fatalf("NewWorker failed: %v", err)
	}

	delivery := stream.newDelivery(task.ID)
	if err := w.ProcessDelivery(context.Background(), delivery); err != nil {
		t.Fatalf("ProcessDelivery failed: %v", err)
	}

	// 验证锁在 PG 写库时尚未释放、ProcessDelivery 返回后才释放，且按 Endpoint 实际超时续期。
	if !tasks.lockHeldAtSave {
		t.Fatal("expected execution lock to remain held while SaveDeliveryOutcome executes")
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

		w, err := worker.NewWorker(zap.NewNop(), stream, tasks, endpoints, locker, sender, worker.Config{})
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

		w, err := worker.NewWorker(zap.NewNop(), stream, tasks, endpoints, locker, sender, worker.Config{})
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

	t.Run("lock_contention", func(t *testing.T) {
		ep, task := mustNewTaskFixture(t, true)
		stream := newFakeReadyStream()
		tasks := newFakeTaskStore()
		tasks.tasks[task.ID] = task
		locker := newFakeLocker()
		locker.held[task.ID] = "other-worker"
		endpoints := &fakeEndpointReader{endpoints: map[uuid.UUID]domain.Endpoint{ep.ID: ep}}
		sender := &fakeSender{}

		w, err := worker.NewWorker(zap.NewNop(), stream, tasks, endpoints, locker, sender, worker.Config{})
		if err != nil {
			t.Fatalf("NewWorker failed: %v", err)
		}
		if err := w.ProcessDelivery(context.Background(), stream.newDelivery(task.ID)); err != nil {
			t.Fatalf("ProcessDelivery failed: %v", err)
		}
		if sender.calls.Load() != 0 || len(stream.acked) != 1 {
			t.Fatalf("expected skip + ack on lock contention, calls=%d acked=%d", sender.calls.Load(), len(stream.acked))
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

	w, err := worker.NewWorker(zap.NewNop(), stream, tasks, endpoints, locker, sender, worker.Config{
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

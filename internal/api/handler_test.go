package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/awu123-create/ferry/internal/domain"
)

type fakeEndpointStore struct {
	items map[uuid.UUID]domain.Endpoint
}

func (s *fakeEndpointStore) CreateEndpoint(_ context.Context, ep domain.Endpoint) error {
	s.items[ep.ID] = ep
	return nil
}

func (s *fakeEndpointStore) GetEndpoint(_ context.Context, id uuid.UUID) (domain.Endpoint, error) {
	ep, ok := s.items[id]
	if !ok {
		return domain.Endpoint{}, domain.ErrEndpointNotFound
	}
	return ep, nil
}

func (s *fakeEndpointStore) ListEndpoints(_ context.Context) ([]domain.Endpoint, error) {
	out := make([]domain.Endpoint, 0, len(s.items))
	for _, ep := range s.items {
		out = append(out, ep)
	}
	return out, nil
}

func (s *fakeEndpointStore) UpdateEndpoint(_ context.Context, ep domain.Endpoint) error {
	if _, ok := s.items[ep.ID]; !ok {
		return domain.ErrEndpointNotFound
	}
	s.items[ep.ID] = ep
	return nil
}

type taskKey struct {
	endpointID uuid.UUID
	idemKey    string
}

type fakeTaskStore struct {
	byID     map[uuid.UUID]domain.Task
	byKey    map[taskKey]uuid.UUID
	outbox   []domain.OutboxItem
	attempts map[uuid.UUID][]domain.DeliveryAttempt
}

func newFakeTaskStore() *fakeTaskStore {
	return &fakeTaskStore{
		byID:     make(map[uuid.UUID]domain.Task),
		byKey:    make(map[taskKey]uuid.UUID),
		attempts: make(map[uuid.UUID][]domain.DeliveryAttempt),
	}
}

func (s *fakeTaskStore) CreateTaskIdempotent(_ context.Context, task domain.Task, outbox domain.OutboxItem) (domain.CreateTaskResult, error) {
	key := taskKey{endpointID: task.EndpointID, idemKey: task.IdempotencyKey}
	if existingID, exists := s.byKey[key]; exists {
		existing := s.byID[existingID]
		if err := existing.VerifyIdempotentReplay(task.EndpointID, task.IdempotencyKey, task.PayloadSHA256); err != nil {
			return domain.CreateTaskResult{}, err
		}
		return domain.CreateTaskResult{Task: existing, Created: false}, nil
	}
	s.byID[task.ID] = task
	s.byKey[key] = task.ID
	s.outbox = append(s.outbox, outbox)
	return domain.CreateTaskResult{Task: task, Created: true}, nil
}

func (s *fakeTaskStore) GetTask(_ context.Context, id uuid.UUID) (domain.Task, error) {
	task, ok := s.byID[id]
	if !ok {
		return domain.Task{}, domain.ErrTaskNotFound
	}
	return task, nil
}

func (s *fakeTaskStore) ListDeliveryAttempts(_ context.Context, taskID uuid.UUID) ([]domain.DeliveryAttempt, error) {
	if _, ok := s.byID[taskID]; !ok {
		return nil, domain.ErrTaskNotFound
	}
	return s.attempts[taskID], nil
}

func (s *fakeTaskStore) CancelTask(_ context.Context, taskID uuid.UUID, now time.Time) (domain.Task, error) {
	task, ok := s.byID[taskID]
	if !ok {
		return domain.Task{}, domain.ErrTaskNotFound
	}
	if err := task.Cancel(now); err != nil {
		return domain.Task{}, err
	}
	s.byID[taskID] = task
	return task, nil
}

type fakeLocker struct {
	held         map[uuid.UUID]string
	releaseCount int
}

func newFakeLocker() *fakeLocker {
	return &fakeLocker{held: make(map[uuid.UUID]string)}
}

func (l *fakeLocker) AcquireExecutionLock(_ context.Context, taskID uuid.UUID, _ time.Duration) (string, bool, error) {
	if _, exists := l.held[taskID]; exists {
		return "", false, nil
	}
	token := uuid.NewString()
	l.held[taskID] = token
	return token, true, nil
}

func (l *fakeLocker) ReleaseExecutionLock(_ context.Context, taskID uuid.UUID, token string) error {
	if current, ok := l.held[taskID]; ok && current == token {
		delete(l.held, taskID)
		l.releaseCount++
	}
	return nil
}

type fakeProbe struct {
	err error
}

func (p fakeProbe) Ping(_ context.Context) error {
	return p.err
}

func TestEndpointsLifecycleAndSecretRedaction(t *testing.T) {
	ctx := t.Context()
	store := &fakeEndpointStore{items: make(map[uuid.UUID]domain.Endpoint)}
	h, err := NewHandler(zap.NewNop(), store, newFakeTaskStore(), newFakeLocker(), []ReadinessProbe{fakeProbe{}}, HandlerConfig{})
	if err != nil {
		t.Fatalf("NewHandler failed: %v", err)
	}

	createBody := []byte(`{"name":"payments","target_url":"http://localhost:8081/webhook","signing_secret":"shhh"}`)
	req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/v1/endpoints", bytes.NewReader(createBody))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201 Created, got %d: %s", rec.Code, rec.Body.String())
	}
	if bytes.Contains(rec.Body.Bytes(), []byte("shhh")) {
		t.Fatalf("expected signing_secret redacted in response, got %s", rec.Body.String())
	}

	var created endpointResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !created.HasSigningSecret || created.Name != "payments" {
		t.Fatalf("unexpected endpoint response: %+v", created)
	}

	patchReq := httptest.NewRequestWithContext(ctx, http.MethodPatch, "/v1/endpoints/"+created.ID.String(), bytes.NewReader([]byte(`{"enabled":false}`)))
	patchRec := httptest.NewRecorder()
	h.ServeHTTP(patchRec, patchReq)
	if patchRec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK on patch, got %d: %s", patchRec.Code, patchRec.Body.String())
	}

	getReq := httptest.NewRequestWithContext(ctx, http.MethodGet, "/v1/endpoints/"+created.ID.String(), nil)
	getRec := httptest.NewRecorder()
	h.ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK on get, got %d", getRec.Code)
	}
}

func TestReadinessAndTaskCreateIdempotency(t *testing.T) {
	ctx := t.Context()
	epStore := &fakeEndpointStore{items: make(map[uuid.UUID]domain.Endpoint)}
	taskStore := newFakeTaskStore()
	locker := newFakeLocker()

	ep, err := domain.NewEndpoint(domain.NewEndpointParams{
		Name:      "orders",
		TargetURL: "http://localhost:8081/webhook",
	}, time.Now().UTC())
	if err != nil {
		t.Fatalf("NewEndpoint failed: %v", err)
	}
	epStore.items[ep.ID] = ep

	h, err := NewHandler(zap.NewNop(), epStore, taskStore, locker, []ReadinessProbe{fakeProbe{err: errors.New("db down")}}, HandlerConfig{})
	if err != nil {
		t.Fatalf("NewHandler failed: %v", err)
	}

	readyReq := httptest.NewRequestWithContext(ctx, http.MethodGet, "/readyz", nil)
	readyRec := httptest.NewRecorder()
	h.ServeHTTP(readyRec, readyReq)
	if readyRec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 on failing probe, got %d", readyRec.Code)
	}

	// 首次创建返回 202 Accepted，且保留原始载荷字节。
	rawPayload := `{"z": 1,   "a": "paid"}`
	taskBody := []byte(`{"endpoint_id":"` + ep.ID.String() + `","payload":` + rawPayload + `}`)
	taskReq := httptest.NewRequestWithContext(ctx, http.MethodPost, "/v1/tasks", bytes.NewReader(taskBody))
	taskReq.Header.Set("Idempotency-Key", "k-1")
	taskRec := httptest.NewRecorder()
	h.ServeHTTP(taskRec, taskReq)
	if taskRec.Code != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted on create, got %d: %s", taskRec.Code, taskRec.Body.String())
	}

	var first taskResponse
	if err := json.Unmarshal(taskRec.Body.Bytes(), &first); err != nil {
		t.Fatalf("decode task response: %v", err)
	}
	if first.PayloadSHA256 != domain.ComputePayloadSHA256([]byte(rawPayload)) {
		t.Fatalf("expected raw payload sha256 match, got %s", first.PayloadSHA256)
	}
	if len(taskStore.outbox) != 1 {
		t.Fatalf("expected 1 outbox row, got %d", len(taskStore.outbox))
	}

	// 同键同载荷重放返回 200 OK 且不重复写 outbox。
	replayReq := httptest.NewRequestWithContext(ctx, http.MethodPost, "/v1/tasks", bytes.NewReader(taskBody))
	replayReq.Header.Set("Idempotency-Key", "k-1")
	replayRec := httptest.NewRecorder()
	h.ServeHTTP(replayRec, replayReq)
	if replayRec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK on idempotent replay, got %d: %s", replayRec.Code, replayRec.Body.String())
	}
	var replayed taskResponse
	if err := json.Unmarshal(replayRec.Body.Bytes(), &replayed); err != nil {
		t.Fatalf("decode replay response: %v", err)
	}
	if replayed.ID != first.ID || len(taskStore.outbox) != 1 {
		t.Fatalf("unexpected replay task=%s outbox=%d", replayed.ID, len(taskStore.outbox))
	}

	// 同键异载荷冲突返回 409 Conflict。
	conflictBody := []byte(`{"endpoint_id":"` + ep.ID.String() + `","payload":{"different":true}}`)
	conflictReq := httptest.NewRequestWithContext(ctx, http.MethodPost, "/v1/tasks", bytes.NewReader(conflictBody))
	conflictReq.Header.Set("Idempotency-Key", "k-1")
	conflictRec := httptest.NewRecorder()
	h.ServeHTTP(conflictRec, conflictReq)
	if conflictRec.Code != http.StatusConflict {
		t.Fatalf("expected 409 Conflict on payload mismatch, got %d: %s", conflictRec.Code, conflictRec.Body.String())
	}

	// 未来 deliver_at 在 Milestone 1 应返回 400 Bad Request。
	future := time.Now().UTC().Add(time.Hour).Format(time.RFC3339)
	futureBody := []byte(`{"endpoint_id":"` + ep.ID.String() + `","payload":{"ok":1},"deliver_at":"` + future + `"}`)
	futureReq := httptest.NewRequestWithContext(ctx, http.MethodPost, "/v1/tasks", bytes.NewReader(futureBody))
	futureReq.Header.Set("Idempotency-Key", "k-future")
	futureRec := httptest.NewRecorder()
	h.ServeHTTP(futureRec, futureReq)
	if futureRec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for future deliver_at, got %d: %s", futureRec.Code, futureRec.Body.String())
	}
}

func TestCancelTaskLockMutexAndStateGuard(t *testing.T) {
	ctx := t.Context()
	epStore := &fakeEndpointStore{items: make(map[uuid.UUID]domain.Endpoint)}
	taskStore := newFakeTaskStore()
	locker := newFakeLocker()

	ep, err := domain.NewEndpoint(domain.NewEndpointParams{
		Name:      "orders",
		TargetURL: "http://localhost:8081/webhook",
	}, time.Now().UTC())
	if err != nil {
		t.Fatalf("NewEndpoint failed: %v", err)
	}
	epStore.items[ep.ID] = ep

	task, outbox, err := domain.NewTask(domain.NewTaskParams{
		Endpoint:       ep,
		IdempotencyKey: "cancel-1",
		RawPayload:     []byte(`{"k":1}`),
		MaxAttempts:    1,
		DefaultWindow:  time.Hour,
		Now:            time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("NewTask failed: %v", err)
	}
	if _, err := taskStore.CreateTaskIdempotent(ctx, task, outbox); err != nil {
		t.Fatalf("CreateTaskIdempotent failed: %v", err)
	}

	h, err := NewHandler(zap.NewNop(), epStore, taskStore, locker, nil, HandlerConfig{})
	if err != nil {
		t.Fatalf("NewHandler failed: %v", err)
	}

	// 模拟 Worker 正在持锁执行：Cancel 必须返回 409 Conflict 且不改任务状态。
	locker.held[task.ID] = "worker-token"
	inFlightReq := httptest.NewRequestWithContext(ctx, http.MethodPost, "/v1/tasks/"+task.ID.String()+"/cancel", nil)
	inFlightRec := httptest.NewRecorder()
	h.ServeHTTP(inFlightRec, inFlightReq)
	if inFlightRec.Code != http.StatusConflict {
		t.Fatalf("expected 409 when execution lock is held, got %d: %s", inFlightRec.Code, inFlightRec.Body.String())
	}
	if taskStore.byID[task.ID].Status != domain.TaskStatusPending {
		t.Fatalf("expected task status still PENDING, got %s", taskStore.byID[task.ID].Status)
	}

	// 释放锁后可成功取消，并在完成后释放执行锁。
	delete(locker.held, task.ID)
	cancelReq := httptest.NewRequestWithContext(ctx, http.MethodPost, "/v1/tasks/"+task.ID.String()+"/cancel", nil)
	cancelRec := httptest.NewRecorder()
	h.ServeHTTP(cancelRec, cancelReq)
	if cancelRec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK on cancel, got %d: %s", cancelRec.Code, cancelRec.Body.String())
	}
	if taskStore.byID[task.ID].Status != domain.TaskStatusCancelled {
		t.Fatalf("expected task status CANCELLED, got %s", taskStore.byID[task.ID].Status)
	}
	if locker.releaseCount != 1 || len(locker.held) != 0 {
		t.Fatalf("expected lock released once, releaseCount=%d held=%d", locker.releaseCount, len(locker.held))
	}

	// 重复取消已终态任务返回 409 Conflict。
	againReq := httptest.NewRequestWithContext(ctx, http.MethodPost, "/v1/tasks/"+task.ID.String()+"/cancel", nil)
	againRec := httptest.NewRecorder()
	h.ServeHTTP(againRec, againReq)
	if againRec.Code != http.StatusConflict {
		t.Fatalf("expected 409 cancelling terminal task, got %d: %s", againRec.Code, againRec.Body.String())
	}
}

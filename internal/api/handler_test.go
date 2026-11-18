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

type fakeTaskStore struct{}

func (fakeTaskStore) CreateTaskIdempotent(_ context.Context, _ domain.Task, _ domain.OutboxItem) (domain.CreateTaskResult, error) {
	return domain.CreateTaskResult{}, domain.ErrNotImplemented
}

func (fakeTaskStore) GetTask(_ context.Context, _ uuid.UUID) (domain.Task, error) {
	return domain.Task{}, domain.ErrTaskNotFound
}

func (fakeTaskStore) ListDeliveryAttempts(_ context.Context, _ uuid.UUID) ([]domain.DeliveryAttempt, error) {
	return nil, domain.ErrTaskNotFound
}

func (fakeTaskStore) CancelTask(_ context.Context, _ uuid.UUID, _ time.Time) (domain.Task, error) {
	return domain.Task{}, domain.ErrNotImplemented
}

type fakeLocker struct{}

func (fakeLocker) AcquireExecutionLock(_ context.Context, _ uuid.UUID, _ time.Duration) (string, bool, error) {
	return "", false, domain.ErrNotImplemented
}

func (fakeLocker) ReleaseExecutionLock(_ context.Context, _ uuid.UUID, _ string) error {
	return domain.ErrNotImplemented
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
	h, err := NewHandler(zap.NewNop(), store, fakeTaskStore{}, fakeLocker{}, []ReadinessProbe{fakeProbe{}}, HandlerConfig{})
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

func TestReadinessAndStubbedTaskRoutes(t *testing.T) {
	ctx := t.Context()
	store := &fakeEndpointStore{items: make(map[uuid.UUID]domain.Endpoint)}
	h, err := NewHandler(zap.NewNop(), store, fakeTaskStore{}, fakeLocker{}, []ReadinessProbe{fakeProbe{err: errors.New("db down")}}, HandlerConfig{})
	if err != nil {
		t.Fatalf("NewHandler failed: %v", err)
	}

	readyReq := httptest.NewRequestWithContext(ctx, http.MethodGet, "/readyz", nil)
	readyRec := httptest.NewRecorder()
	h.ServeHTTP(readyRec, readyReq)
	if readyRec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 on failing probe, got %d", readyRec.Code)
	}

	taskBody := []byte(`{"endpoint_id":"` + uuid.NewString() + `","payload":{"action":"paid"}}`)
	taskReq := httptest.NewRequestWithContext(ctx, http.MethodPost, "/v1/tasks", bytes.NewReader(taskBody))
	taskReq.Header.Set("Idempotency-Key", "k-1")
	taskRec := httptest.NewRecorder()
	h.ServeHTTP(taskRec, taskReq)
	if taskRec.Code != http.StatusNotImplemented {
		t.Fatalf("expected 501 for stubbed createTask, got %d: %s", taskRec.Code, taskRec.Body.String())
	}

	cancelReq := httptest.NewRequestWithContext(ctx, http.MethodPost, "/v1/tasks/"+uuid.NewString()+"/cancel", nil)
	cancelRec := httptest.NewRecorder()
	h.ServeHTTP(cancelRec, cancelReq)
	if cancelRec.Code != http.StatusNotImplemented {
		t.Fatalf("expected 501 for stubbed cancelTask, got %d: %s", cancelRec.Code, cancelRec.Body.String())
	}
}

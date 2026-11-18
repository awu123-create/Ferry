package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/awu123-create/ferry/internal/domain"
)

type fakeReadyStream struct{}

func (fakeReadyStream) PollReady(_ context.Context, _ int) ([]domain.ReadyTicket, error) {
	return nil, nil
}
func (fakeReadyStream) Ack(_ domain.ReadyTicket)                {}
func (fakeReadyStream) CommitCompleted(_ context.Context) error { return nil }

type fakeTaskStore struct{}

func (fakeTaskStore) GetTask(_ context.Context, _ uuid.UUID) (domain.Task, error) {
	return domain.Task{}, domain.ErrTaskNotFound
}

func (fakeTaskStore) SavePreDeliveryDeadLetter(_ context.Context, _ uuid.UUID, _, _ int, _ string, _ time.Time) (bool, error) {
	return false, domain.ErrNotImplemented
}

func (fakeTaskStore) SaveDeliveryOutcome(_ context.Context, _, _ int, _ domain.Task, _ domain.DeliveryAttempt) (bool, error) {
	return false, domain.ErrNotImplemented
}

type fakeEndpointReader struct{}

func (fakeEndpointReader) GetEndpoint(_ context.Context, _ uuid.UUID) (domain.Endpoint, error) {
	return domain.Endpoint{}, domain.ErrEndpointNotFound
}

type fakeLocker struct{}

func (fakeLocker) AcquireExecutionLock(_ context.Context, _ uuid.UUID, _ time.Duration) (string, bool, error) {
	return "", false, domain.ErrNotImplemented
}

func (fakeLocker) ReleaseExecutionLock(_ context.Context, _ uuid.UUID, _ string) error {
	return domain.ErrNotImplemented
}

type fakeSender struct{}

func (fakeSender) Send(_ context.Context, _ domain.DeliveryRequest) domain.DeliveryResult {
	return domain.DeliveryResult{}
}

func TestWorkerConstructorAndStubbedRun(t *testing.T) {
	if _, err := NewWorker(zap.NewNop(), nil, fakeTaskStore{}, fakeEndpointReader{}, fakeLocker{}, fakeSender{}, Config{}); err == nil {
		t.Fatal("expected error when ReadyStream is nil")
	}

	w, err := NewWorker(zap.NewNop(), fakeReadyStream{}, fakeTaskStore{}, fakeEndpointReader{}, fakeLocker{}, fakeSender{}, Config{})
	if err != nil {
		t.Fatalf("NewWorker failed: %v", err)
	}
	if w.Ready() {
		t.Fatal("expected Ready() == false before Run starts or while unimplemented")
	}

	if err := w.ProcessTicket(context.Background(), domain.ReadyTicket{TaskID: uuid.New()}); !errors.Is(err, domain.ErrNotImplemented) {
		t.Fatalf("expected ErrNotImplemented from stubbed ProcessTicket, got %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Millisecond)
	defer cancel()
	if err := w.Run(ctx); err != nil {
		t.Fatalf("expected clean exit on cancel, got %v", err)
	}
	if w.Ready() {
		t.Fatal("expected Ready() == false after Run exits")
	}
}

package scheduler

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/awu123-create/ferry/internal/domain"
)

type fakeOutboxStore struct{}

func (fakeOutboxStore) ClaimAndDispatchImmediateOutbox(
	_ context.Context,
	_ int,
	_ func(ctx context.Context, items []domain.OutboxItem) error,
) (int, error) {
	return 0, domain.ErrNotImplemented
}

func (fakeOutboxStore) WaitForOutboxNotification(_ context.Context) error {
	return domain.ErrNotImplemented
}

type fakeReadyPublisher struct{}

func (fakeReadyPublisher) PublishTaskIDs(_ context.Context, _ []uuid.UUID) error {
	return nil
}

func TestOutboxRelayConstructorAndStubbedMethods(t *testing.T) {
	if _, err := NewOutboxRelay(zap.NewNop(), nil, fakeReadyPublisher{}, OutboxRelayConfig{}); err == nil {
		t.Fatal("expected error when OutboxStore is nil")
	}
	if _, err := NewOutboxRelay(zap.NewNop(), fakeOutboxStore{}, nil, OutboxRelayConfig{}); err == nil {
		t.Fatal("expected error when ReadyPublisher is nil")
	}

	relay, err := NewOutboxRelay(zap.NewNop(), fakeOutboxStore{}, fakeReadyPublisher{}, OutboxRelayConfig{})
	if err != nil {
		t.Fatalf("NewOutboxRelay failed: %v", err)
	}

	if _, err := relay.RelayOnce(context.Background()); !errors.Is(err, domain.ErrNotImplemented) {
		t.Fatalf("expected ErrNotImplemented from stubbed RelayOnce, got %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Millisecond)
	defer cancel()
	if err := relay.Run(ctx); err != nil {
		t.Fatalf("expected clean exit on cancel, got %v", err)
	}
}

package scheduler_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/awu123-create/ferry/internal/scheduler"
)

type fakeOutboxStore struct {
	mu      sync.Mutex
	batches [][]uuid.UUID
	notify  chan struct{}
	errOnce error
	lastNow time.Time
}

func (f *fakeOutboxStore) ClaimAndDispatchImmediateOutbox(
	ctx context.Context,
	now time.Time,
	limit int,
	dispatch func(ctx context.Context, taskIDs []uuid.UUID) error,
) (int, error) {
	f.mu.Lock()
	f.lastNow = now
	if f.errOnce != nil {
		err := f.errOnce
		f.errOnce = nil
		f.mu.Unlock()
		return 0, err
	}
	if len(f.batches) == 0 {
		f.mu.Unlock()
		return 0, nil
	}
	taskIDs := f.batches[0]
	if len(taskIDs) > limit {
		taskIDs = taskIDs[:limit]
	}
	f.batches = f.batches[1:]
	f.mu.Unlock()

	if err := dispatch(ctx, taskIDs); err != nil {
		return 0, err
	}
	return len(taskIDs), nil
}

func (f *fakeOutboxStore) WaitForOutboxNotification(ctx context.Context) error {
	if f.notify == nil {
		<-ctx.Done()
		return ctx.Err()
	}
	select {
	case <-f.notify:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type fakeReadyPublisher struct {
	mu        sync.Mutex
	published []uuid.UUID
	onPublish func(count int)
}

func (f *fakeReadyPublisher) PublishTaskIDs(_ context.Context, taskIDs []uuid.UUID) error {
	f.mu.Lock()
	f.published = append(f.published, taskIDs...)
	count := len(f.published)
	cb := f.onPublish
	f.mu.Unlock()
	if cb != nil {
		cb(count)
	}
	return nil
}

func TestOutboxRelay_ConstructorAndRelayOnce(t *testing.T) {
	if _, err := scheduler.NewOutboxRelay(zap.NewNop(), nil, &fakeReadyPublisher{}, scheduler.OutboxRelayConfig{}); err == nil {
		t.Fatal("expected error when OutboxStore is nil")
	}
	if _, err := scheduler.NewOutboxRelay(zap.NewNop(), &fakeOutboxStore{}, nil, scheduler.OutboxRelayConfig{}); err == nil {
		t.Fatal("expected error when ReadyPublisher is nil")
	}

	fixedNow := time.Date(2026, 11, 22, 8, 0, 0, 0, time.UTC)
	id1, id2 := uuid.New(), uuid.New()
	store := &fakeOutboxStore{
		batches: [][]uuid.UUID{
			{id1, id2},
		},
	}
	pub := &fakeReadyPublisher{}

	relay, err := scheduler.NewOutboxRelay(zap.NewNop(), store, pub, scheduler.OutboxRelayConfig{
		BatchSize:    10,
		PollInterval: 20 * time.Millisecond,
		BatchTimeout: time.Second,
		Clock:        func() time.Time { return fixedNow },
	})
	if err != nil {
		t.Fatalf("NewOutboxRelay failed: %v", err)
	}

	claimed, err := relay.RelayOnce(context.Background())
	if err != nil {
		t.Fatalf("RelayOnce failed: %v", err)
	}
	if claimed != 2 {
		t.Fatalf("expected 2 claimed items, got %d", claimed)
	}
	if !store.lastNow.Equal(fixedNow) {
		t.Fatalf("expected injected clock %v passed to store, got %v", fixedNow, store.lastNow)
	}
	if len(pub.published) != 2 || pub.published[0] != id1 || pub.published[1] != id2 {
		t.Fatalf("unexpected published IDs: %+v", pub.published)
	}
}

func TestOutboxRelay_DrainsBacklogWakesOnNotifyAndExitsCleanly(t *testing.T) {
	id1, id2, id3 := uuid.New(), uuid.New(), uuid.New()
	notifyCh := make(chan struct{}, 1)
	doneCh := make(chan struct{})
	var closeOnce sync.Once

	store := &fakeOutboxStore{
		// 第一批拉满 BatchSize=2 应立即排空第二批；同时验证瞬时错误后会恢复继续搬运。
		errOnce: errors.New("transient pg hiccup"),
		batches: [][]uuid.UUID{
			{id1, id2},
		},
		notify: notifyCh,
	}
	pub := &fakeReadyPublisher{
		onPublish: func(count int) {
			if count == 2 {
				// 模拟收到新入箱通知后迅速唤醒搬运第三条记录。
				store.mu.Lock()
				store.batches = append(store.batches, []uuid.UUID{id3})
				store.mu.Unlock()
				notifyCh <- struct{}{}
			}
			if count == 3 {
				closeOnce.Do(func() { close(doneCh) })
			}
		},
	}

	relay, err := scheduler.NewOutboxRelay(zap.NewNop(), store, pub, scheduler.OutboxRelayConfig{
		BatchSize:    2,
		PollInterval: 15 * time.Millisecond,
		BatchTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("NewOutboxRelay failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- relay.Run(ctx)
	}()

	select {
	case <-doneCh:
	case <-time.After(time.Second):
		cancel()
		t.Fatal("timed out waiting for OutboxRelay to publish all 3 tasks")
	}

	cancel()
	if err := <-errCh; err != nil {
		t.Fatalf("expected clean nil exit, got %v", err)
	}

	pub.mu.Lock()
	defer pub.mu.Unlock()
	if len(pub.published) != 3 || pub.published[0] != id1 || pub.published[1] != id2 || pub.published[2] != id3 {
		t.Fatalf("unexpected published sequence: %+v", pub.published)
	}
}

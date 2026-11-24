package scheduler_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/awu123-create/ferry/internal/domain"
	"github.com/awu123-create/ferry/internal/scheduler"
)

type fakeOutboxStore struct {
	mu             sync.Mutex
	immBatches     [][]uuid.UUID
	delBatches     [][]domain.OutboxItem
	immErrOnce     error
	notify         chan struct{}
	waitActive     atomic.Int32
	maxWaitActive  atomic.Int32
	lastImmNow     time.Time
	lastDelayedNow time.Time
}

func (f *fakeOutboxStore) ClaimAndDispatchImmediateOutbox(
	ctx context.Context,
	now time.Time,
	limit int,
	dispatch func(ctx context.Context, taskIDs []uuid.UUID) error,
) (int, error) {
	f.mu.Lock()
	f.lastImmNow = now
	if f.immErrOnce != nil {
		err := f.immErrOnce
		f.immErrOnce = nil
		f.mu.Unlock()
		return 0, err
	}
	if len(f.immBatches) == 0 {
		f.mu.Unlock()
		return 0, nil
	}
	batch := f.immBatches[0]
	if len(batch) > limit {
		batch = batch[:limit]
	}
	f.immBatches = f.immBatches[1:]
	f.mu.Unlock()

	if err := dispatch(ctx, batch); err != nil {
		f.mu.Lock()
		f.immBatches = append([][]uuid.UUID{batch}, f.immBatches...)
		f.mu.Unlock()
		return 0, err
	}
	return len(batch), nil
}

func (f *fakeOutboxStore) ClaimAndDispatchDelayedOutbox(
	ctx context.Context,
	now time.Time,
	limit int,
	dispatch func(ctx context.Context, items []domain.OutboxItem) error,
) (int, error) {
	f.mu.Lock()
	f.lastDelayedNow = now
	if len(f.delBatches) == 0 {
		f.mu.Unlock()
		return 0, nil
	}
	batch := f.delBatches[0]
	if len(batch) > limit {
		batch = batch[:limit]
	}
	f.delBatches = f.delBatches[1:]
	f.mu.Unlock()

	if err := dispatch(ctx, batch); err != nil {
		f.mu.Lock()
		f.delBatches = append([][]domain.OutboxItem{batch}, f.delBatches...)
		f.mu.Unlock()
		return 0, err
	}
	return len(batch), nil
}

func (f *fakeOutboxStore) WaitForOutboxNotification(ctx context.Context) error {
	cur := f.waitActive.Add(1)
	defer f.waitActive.Add(-1)
	for {
		prev := f.maxWaitActive.Load()
		if cur <= prev || f.maxWaitActive.CompareAndSwap(prev, cur) {
			break
		}
	}

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

type fakeDelayedEnqueuer struct {
	mu        sync.Mutex
	scheduled []domain.OutboxItem
	fail      bool
	failTimes []time.Time
	onAttempt func(err error, totalScheduled int)
}

func (f *fakeDelayedEnqueuer) ScheduleDelayedBatch(_ context.Context, items []domain.OutboxItem) error {
	f.mu.Lock()
	if f.fail {
		f.failTimes = append(f.failTimes, time.Now())
		cb := f.onAttempt
		total := len(f.scheduled)
		f.mu.Unlock()
		err := errors.New("redis unavailable")
		if cb != nil {
			cb(err, total)
		}
		return err
	}
	f.scheduled = append(f.scheduled, items...)
	total := len(f.scheduled)
	cb := f.onAttempt
	f.mu.Unlock()
	if cb != nil {
		cb(nil, total)
	}
	return nil
}

func TestOutboxRelay_ConstructorValidation(t *testing.T) {
	if _, err := scheduler.NewOutboxRelay(zap.NewNop(), nil, &fakeReadyPublisher{}, &fakeDelayedEnqueuer{}, scheduler.OutboxRelayConfig{}); err == nil {
		t.Fatal("expected error when OutboxStore is nil")
	}
	if _, err := scheduler.NewOutboxRelay(zap.NewNop(), &fakeOutboxStore{}, nil, &fakeDelayedEnqueuer{}, scheduler.OutboxRelayConfig{}); err == nil {
		t.Fatal("expected error when ReadyPublisher is nil")
	}
	if _, err := scheduler.NewOutboxRelay(zap.NewNop(), &fakeOutboxStore{}, &fakeReadyPublisher{}, nil, scheduler.OutboxRelayConfig{}); err == nil {
		t.Fatal("expected error when DelayedEnqueuer is nil")
	}
}

func TestOutboxRelay_DualChannelDrainsWakesFromSingleListenAndExitsCleanly(t *testing.T) {
	fixedNow := time.Date(2026, 11, 23, 8, 0, 0, 0, time.UTC)
	imm1, imm2, imm3 := uuid.New(), uuid.New(), uuid.New()
	del1, del2 := uuid.New(), uuid.New()
	delAt1 := fixedNow.Add(10 * time.Second)
	delAt2 := fixedNow.Add(20 * time.Second)

	notifyCh := make(chan struct{}, 4)
	immDone := make(chan struct{})
	delDone := make(chan struct{})
	var immOnce, delOnce sync.Once

	store := &fakeOutboxStore{
		immErrOnce: errors.New("transient pg hiccup"),
		immBatches: [][]uuid.UUID{{imm1, imm2}},
		delBatches: [][]domain.OutboxItem{{{TaskID: del1, DeliverAt: delAt1}}},
		notify:     notifyCh,
	}

	pub := &fakeReadyPublisher{
		onPublish: func(_ []uuid.UUID, total int) error {
			if total == 2 {
				store.mu.Lock()
				store.immBatches = append(store.immBatches, []uuid.UUID{imm3})
				store.delBatches = append(store.delBatches, []domain.OutboxItem{{TaskID: del2, DeliverAt: delAt2}})
				store.mu.Unlock()
				notifyCh <- struct{}{}
			}
			if total == 3 {
				immOnce.Do(func() { close(immDone) })
			}
			return nil
		},
	}

	delayed := &fakeDelayedEnqueuer{
		onAttempt: func(err error, total int) {
			if err == nil && total == 2 {
				delOnce.Do(func() { close(delDone) })
			}
		},
	}

	relay, err := scheduler.NewOutboxRelay(zap.NewNop(), store, pub, delayed, scheduler.OutboxRelayConfig{
		BatchSize:       2,
		PollInterval:    20 * time.Millisecond,
		BatchTimeout:    time.Second,
		ErrorBackoffCap: 100 * time.Millisecond,
		Clock:           func() time.Time { return fixedNow },
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
	case <-immDone:
	case <-time.After(2 * time.Second):
		cancel()
		t.Fatal("timed out waiting for immediate channel to publish 3 tasks")
	}
	select {
	case <-delDone:
	case <-time.After(2 * time.Second):
		cancel()
		t.Fatal("timed out waiting for delayed channel to enqueue 2 items")
	}

	cancel()
	if err := <-errCh; err != nil {
		t.Fatalf("expected clean nil exit, got %v", err)
	}

	if got := store.maxWaitActive.Load(); got > 1 {
		t.Fatalf("expected single shared LISTEN connection, observed %d concurrent waiters", got)
	}
	store.mu.Lock()
	gotImmNow, gotDelNow := store.lastImmNow, store.lastDelayedNow
	store.mu.Unlock()
	if !gotImmNow.Equal(fixedNow) || !gotDelNow.Equal(fixedNow) {
		t.Fatalf("expected injected clock passed to both channels, got imm=%v del=%v", gotImmNow, gotDelNow)
	}

	published := pub.snapshot()
	if len(published) != 3 || published[0] != imm1 || published[1] != imm2 || published[2] != imm3 {
		t.Fatalf("unexpected immediate sequence: %+v", published)
	}
	delayed.mu.Lock()
	defer delayed.mu.Unlock()
	if len(delayed.scheduled) != 2 || delayed.scheduled[0].TaskID != del1 || delayed.scheduled[1].TaskID != del2 {
		t.Fatalf("unexpected delayed items: %+v", delayed.scheduled)
	}
}

func TestOutboxRelay_DelayedFailureBacksOffIgnoringNotifyWithoutBlockingImmediate(t *testing.T) {
	notifyCh := make(chan struct{}, 16)
	delID := uuid.New()
	imm1, imm2, imm3 := uuid.New(), uuid.New(), uuid.New()

	store := &fakeOutboxStore{
		delBatches: [][]domain.OutboxItem{{{TaskID: delID, DeliverAt: time.Now().UTC().Add(time.Minute)}}},
		notify:     notifyCh,
	}

	threeFailures := make(chan struct{})
	recovered := make(chan struct{})
	var failOnce, recOnce sync.Once
	var delayed *fakeDelayedEnqueuer
	delayed = &fakeDelayedEnqueuer{
		fail: true,
		onAttempt: func(err error, total int) {
			if err != nil {
				delayed.mu.Lock()
				n := len(delayed.failTimes)
				delayed.mu.Unlock()
				if n >= 3 {
					failOnce.Do(func() { close(threeFailures) })
				}
				return
			}
			if total >= 1 {
				recOnce.Do(func() { close(recovered) })
			}
		},
	}

	immCountCh := make(chan int, 8)
	pub := &fakeReadyPublisher{
		onPublish: func(_ []uuid.UUID, total int) error {
			immCountCh <- total
			return nil
		},
	}

	relay, err := scheduler.NewOutboxRelay(zap.NewNop(), store, pub, delayed, scheduler.OutboxRelayConfig{
		BatchSize:       10,
		PollInterval:    25 * time.Millisecond,
		BatchTimeout:    time.Second,
		ErrorBackoffCap: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewOutboxRelay failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- relay.Run(ctx)
	}()

	// Redis 故障期间持续注入立即任务与 NOTIFY：立即通道必须照常迅速推完，且 NOTIFY 不得把退避中的延迟通道频繁惊醒。
	for i, id := range []uuid.UUID{imm1, imm2, imm3} {
		store.mu.Lock()
		store.immBatches = append(store.immBatches, []uuid.UUID{id})
		store.mu.Unlock()
		notifyCh <- struct{}{}
		select {
		case gotTotal := <-immCountCh:
			if gotTotal != i+1 {
				cancel()
				t.Fatalf("expected immediate total=%d, got %d", i+1, gotTotal)
			}
		case <-time.After(500 * time.Millisecond):
			cancel()
			t.Fatalf("immediate channel blocked while delayed channel failing on item %d", i+1)
		}
	}

	// 连续发送 5 次 NOTIFY，验证延迟通道在指数退避窗口内不会立刻被惊醒。
	for range 5 {
		notifyCh <- struct{}{}
	}

	select {
	case <-threeFailures:
	case <-time.After(2 * time.Second):
		cancel()
		t.Fatal("timed out waiting for delayed channel backoff retries")
	}

	delayed.mu.Lock()
	t0, t1, t2 := delayed.failTimes[0], delayed.failTimes[1], delayed.failTimes[2]
	delayed.fail = false
	delayed.mu.Unlock()

	gap1 := t1.Sub(t0)
	gap2 := t2.Sub(t1)
	// PollInterval=25ms -> 第 1 次失败休眠 ~25ms，第 2 次失败翻倍休眠 ~50ms（即使期间狂发 NOTIFY 也必须守住退避间隔）。
	if gap1 < 15*time.Millisecond || gap2 < 35*time.Millisecond {
		cancel()
		t.Fatalf("expected exponential backoff ignoring NOTIFY (~25ms, ~50ms), got gap1=%v gap2=%v", gap1, gap2)
	}

	select {
	case <-recovered:
	case <-time.After(2 * time.Second):
		cancel()
		t.Fatal("timed out waiting for delayed channel recovery")
	}

	cancel()
	if err := <-errCh; err != nil {
		t.Fatalf("expected clean nil exit, got %v", err)
	}
}

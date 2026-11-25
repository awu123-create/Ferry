package scheduler_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/awu123-create/ferry/internal/domain"
	"github.com/awu123-create/ferry/internal/scheduler"
)

type fakeDelayedQueue struct {
	mu               sync.Mutex
	batches          []domain.DelayedLeaseBatch
	claimTimes       []time.Time
	claimHasDeadline bool
	claimDeadline    time.Time
	claimDelay       time.Duration
	ackedBatches     []domain.DelayedLeaseBatch
	ackCtxErr        error
	ackDeadline      time.Time
	onAck            func(count int)
}

func (f *fakeDelayedQueue) ClaimDelayed(ctx context.Context, limit int, _ time.Duration) (domain.DelayedLeaseBatch, error) {
	deadline, hasDeadline := ctx.Deadline()
	f.mu.Lock()
	f.claimHasDeadline = hasDeadline
	f.claimDeadline = deadline
	f.claimTimes = append(f.claimTimes, time.Now())
	delay := f.claimDelay
	if len(f.batches) == 0 {
		f.mu.Unlock()
		return domain.DelayedLeaseBatch{}, nil
	}
	batch := f.batches[0]
	if len(batch.TaskIDs) > limit {
		batch.TaskIDs = batch.TaskIDs[:limit]
	}
	f.batches = f.batches[1:]
	f.mu.Unlock()
	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return domain.DelayedLeaseBatch{}, ctx.Err()
		}
	}
	return batch, nil
}

func (f *fakeDelayedQueue) AckDelayed(ctx context.Context, batch domain.DelayedLeaseBatch) error {
	deadline, _ := ctx.Deadline()
	f.mu.Lock()
	f.ackCtxErr = ctx.Err()
	f.ackDeadline = deadline
	f.ackedBatches = append(f.ackedBatches, domain.DelayedLeaseBatch{
		TaskIDs:      append([]uuid.UUID(nil), batch.TaskIDs...),
		LeaseUntilMs: batch.LeaseUntilMs,
	})
	count := len(f.ackedBatches)
	cb := f.onAck
	f.mu.Unlock()
	if cb != nil {
		cb(count)
	}
	return nil
}

func TestDelayedForwarder_ConstructorValidation(t *testing.T) {
	if _, err := scheduler.NewDelayedForwarder(zap.NewNop(), nil, &fakeReadyPublisher{}, scheduler.DelayedForwarderConfig{}); err == nil {
		t.Fatal("expected error when DelayedQueue is nil")
	}
	if _, err := scheduler.NewDelayedForwarder(zap.NewNop(), &fakeDelayedQueue{}, nil, scheduler.DelayedForwarderConfig{}); err == nil {
		t.Fatal("expected error when ReadyPublisher is nil")
	}
}

func TestDelayedForwarder_ClaimsPublishesAcksAndDrainsFullBatchesImmediately(t *testing.T) {
	id1, id2, id3 := uuid.New(), uuid.New(), uuid.New()
	done := make(chan struct{})
	var closeOnce sync.Once

	queue := &fakeDelayedQueue{
		batches: []domain.DelayedLeaseBatch{
			{TaskIDs: []uuid.UUID{id1, id2}, LeaseUntilMs: 1700000030000},
			{TaskIDs: []uuid.UUID{id3}, LeaseUntilMs: 1700000031000},
		},
		onAck: func(count int) {
			if count == 2 {
				closeOnce.Do(func() { close(done) })
			}
		},
	}
	pub := &fakeReadyPublisher{}

	// PollInterval 设为 5s：若满批（len==2）未立即紧接下一轮排空，用例会在 1s 内超时失败。
	fwd, err := scheduler.NewDelayedForwarder(zap.NewNop(), queue, pub, scheduler.DelayedForwarderConfig{
		BatchSize:       2,
		PollInterval:    5 * time.Second,
		LeaseTTL:        30 * time.Second,
		ErrorBackoffCap: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewDelayedForwarder failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- fwd.Run(ctx)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		cancel()
		t.Fatal("timed out waiting for immediate backlog drain across full batch")
	}

	cancel()
	if err := <-errCh; err != nil {
		t.Fatalf("expected clean nil exit, got %v", err)
	}

	published := pub.snapshot()
	if len(published) != 3 || published[0] != id1 || published[1] != id2 || published[2] != id3 {
		t.Fatalf("unexpected published order: %+v", published)
	}

	_, pubHasDeadline := pub.lastDeadlineInfo()
	queue.mu.Lock()
	defer queue.mu.Unlock()
	if !queue.claimHasDeadline || !pubHasDeadline {
		t.Fatalf("expected outbound deadline on ClaimDelayed (%v) and PublishTaskIDs (%v)", queue.claimHasDeadline, pubHasDeadline)
	}
	if len(queue.ackedBatches) != 2 ||
		queue.ackedBatches[0].LeaseUntilMs != 1700000030000 ||
		queue.ackedBatches[1].LeaseUntilMs != 1700000031000 {
		t.Fatalf("unexpected acked batches: %+v", queue.ackedBatches)
	}
}

func TestDelayedForwarder_PublishFailureBacksOffAndEmptyPollResetsBackoff(t *testing.T) {
	id := uuid.New()
	done := make(chan struct{})
	var closeOnce sync.Once

	// 连续 3 次发布失败按指数退避（~25ms -> ~50ms -> ~100ms），接着一轮没数据但没报错的空轮询把退避清零；
	// 随后第 4 次失败应该重新从基础间隔（~25ms）开始等，不能接着按旧退避翻倍到 200ms。
	queue := &fakeDelayedQueue{
		batches: []domain.DelayedLeaseBatch{
			{TaskIDs: []uuid.UUID{id}, LeaseUntilMs: 1001}, // #0 失败 -> 退避 ~25ms
			{TaskIDs: []uuid.UUID{id}, LeaseUntilMs: 1002}, // #1 失败 -> 退避 ~50ms
			{TaskIDs: []uuid.UUID{id}, LeaseUntilMs: 1003}, // #2 失败 -> 退避 ~100ms
			{}, // #3 空轮询 err==nil -> 重置 backoff=0，等 PollInterval(25ms)
			{TaskIDs: []uuid.UUID{id}, LeaseUntilMs: 1004}, // #4 失败 -> 从基础退避 ~25ms 起步（绝非 ~200ms）
			{TaskIDs: []uuid.UUID{id}, LeaseUntilMs: 1005}, // #5 成功 -> Ack
		},
		onAck: func(count int) {
			if count == 1 {
				closeOnce.Do(func() { close(done) })
			}
		},
	}
	pub := &fakeReadyPublisher{
		failErrs: []error{
			errors.New("kafka down 1"),
			errors.New("kafka down 2"),
			errors.New("kafka down 3"),
			errors.New("kafka down 4"),
		},
	}

	fwd, err := scheduler.NewDelayedForwarder(zap.NewNop(), queue, pub, scheduler.DelayedForwarderConfig{
		BatchSize:       10,
		PollInterval:    25 * time.Millisecond,
		LeaseTTL:        30 * time.Second,
		ErrorBackoffCap: 200 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewDelayedForwarder failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- fwd.Run(ctx)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		cancel()
		t.Fatal("timed out waiting for forwarder recovery after publish failures")
	}

	cancel()
	if err := <-errCh; err != nil {
		t.Fatalf("expected clean nil exit, got %v", err)
	}

	queue.mu.Lock()
	defer queue.mu.Unlock()
	if len(queue.ackedBatches) != 1 || queue.ackedBatches[0].LeaseUntilMs != 1005 {
		t.Fatalf("expected AckDelayed only after successful publish (lease=1005), got %+v", queue.ackedBatches)
	}
	if len(queue.claimTimes) < 6 {
		t.Fatalf("expected at least 6 claims, got %d", len(queue.claimTimes))
	}
	gapAfterFail3 := queue.claimTimes[3].Sub(queue.claimTimes[2])
	gapAfterFail4 := queue.claimTimes[5].Sub(queue.claimTimes[4])
	if gapAfterFail3 < 85*time.Millisecond {
		t.Fatalf("expected third consecutive failure to back off ~100ms, got %v", gapAfterFail3)
	}
	if gapAfterFail4 < 18*time.Millisecond || gapAfterFail4 > 90*time.Millisecond {
		t.Fatalf("expected empty poll to reset backoff to base ~25ms (not 200ms), got gapAfterFail4=%v", gapAfterFail4)
	}
}

func TestDelayedForwarder_ShortLeaseSharesRoundDeadlineAndCancelsSlowPublish(t *testing.T) {
	t.Run("short_lease_cancels_slow_publish_before_expiry", func(t *testing.T) {
		id := uuid.New()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		queue := &fakeDelayedQueue{
			batches: []domain.DelayedLeaseBatch{
				{TaskIDs: []uuid.UUID{id}, LeaseUntilMs: 2001},
			},
		}

		var pubErr error
		var pubElapsed time.Duration
		pubDone := make(chan struct{})
		var doneOnce sync.Once
		pub := &fakeReadyPublisher{
			onPublish: func(pubCtx context.Context, _ []uuid.UUID, _ int) error {
				start := time.Now()
				<-pubCtx.Done()
				pubElapsed = time.Since(start)
				pubErr = pubCtx.Err()
				doneOnce.Do(func() { close(pubDone) })
				cancel()
				return pubErr
			},
		}

		// LeaseTTL=30ms -> roundTimeout=15ms：慢发布必须在 30ms 租约到期前被主动取消，不得放宽到 50ms。
		fwd, err := scheduler.NewDelayedForwarder(zap.NewNop(), queue, pub, scheduler.DelayedForwarderConfig{
			BatchSize:       10,
			PollInterval:    20 * time.Millisecond,
			LeaseTTL:        30 * time.Millisecond,
			ErrorBackoffCap: 100 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("NewDelayedForwarder failed: %v", err)
		}

		errCh := make(chan error, 1)
		go func() {
			errCh <- fwd.Run(ctx)
		}()

		select {
		case <-pubDone:
		case <-time.After(time.Second):
			cancel()
			t.Fatal("timed out waiting for slow publish cancellation")
		}
		if err := <-errCh; err != nil {
			t.Fatalf("expected clean nil exit, got %v", err)
		}

		if !errors.Is(pubErr, context.DeadlineExceeded) {
			t.Fatalf("expected slow publish cancelled by context.DeadlineExceeded, got %v", pubErr)
		}
		if pubElapsed >= 30*time.Millisecond {
			t.Fatalf("expected slow publish cancelled strictly before 30ms lease expiry, took %v", pubElapsed)
		}
		queue.mu.Lock()
		defer queue.mu.Unlock()
		if len(queue.ackedBatches) != 0 {
			t.Fatalf("expected no AckDelayed after timed-out publish, got %+v", queue.ackedBatches)
		}
	})

	t.Run("claim_publish_ack_share_single_round_deadline", func(t *testing.T) {
		id := uuid.New()
		done := make(chan struct{})
		var closeOnce sync.Once

		queue := &fakeDelayedQueue{
			batches: []domain.DelayedLeaseBatch{
				{TaskIDs: []uuid.UUID{id}, LeaseUntilMs: 2002},
			},
			claimDelay: 15 * time.Millisecond,
			onAck: func(count int) {
				if count == 1 {
					closeOnce.Do(func() { close(done) })
				}
			},
		}
		pub := &fakeReadyPublisher{}

		const leaseTTL = 100 * time.Millisecond
		fwd, err := scheduler.NewDelayedForwarder(zap.NewNop(), queue, pub, scheduler.DelayedForwarderConfig{
			BatchSize:       10,
			PollInterval:    20 * time.Millisecond,
			LeaseTTL:        leaseTTL,
			ErrorBackoffCap: 100 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("NewDelayedForwarder failed: %v", err)
		}

		roundStart := time.Now()
		ctx, cancel := context.WithCancel(context.Background())
		errCh := make(chan error, 1)
		go func() {
			errCh <- fwd.Run(ctx)
		}()

		select {
		case <-done:
		case <-time.After(time.Second):
			cancel()
			t.Fatal("timed out waiting for round completion")
		}
		cancel()
		if err := <-errCh; err != nil {
			t.Fatalf("expected clean nil exit, got %v", err)
		}

		pubDeadline, pubHasDeadline := pub.lastDeadlineInfo()
		queue.mu.Lock()
		defer queue.mu.Unlock()
		if !queue.claimHasDeadline || !pubHasDeadline {
			t.Fatalf("expected deadlines on claim (%v) and publish (%v)", queue.claimHasDeadline, pubHasDeadline)
		}
		if !queue.claimDeadline.Equal(pubDeadline) || !pubDeadline.Equal(queue.ackDeadline) {
			t.Fatalf("expected Claim/Publish/Ack share exact round deadline, got claim=%v pub=%v ack=%v", queue.claimDeadline, pubDeadline, queue.ackDeadline)
		}
		if budget := queue.claimDeadline.Sub(roundStart); budget >= leaseTTL {
			t.Fatalf("expected shared round deadline strictly < LeaseTTL(%v), got %v", leaseTTL, budget)
		}
	})
}

func TestDelayedForwarder_ShutdownAfterPublishStillAcksLease(t *testing.T) {
	cases := []struct {
		name      string
		leaseTTL  time.Duration
		minBudget time.Duration
		maxBudget time.Duration
	}{
		{
			name:      "default_30s_lease_gets_5s_shutdown_budget",
			leaseTTL:  30 * time.Second,
			minBudget: 4 * time.Second,
			maxBudget: 6 * time.Second,
		},
		{
			name:      "short_40ms_lease_caps_shutdown_budget_to_half_lease",
			leaseTTL:  40 * time.Millisecond,
			minBudget: 5 * time.Millisecond,
			maxBudget: 30 * time.Millisecond,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			id := uuid.New()
			ctx, cancel := context.WithCancel(context.Background())

			queue := &fakeDelayedQueue{
				batches: []domain.DelayedLeaseBatch{
					{TaskIDs: []uuid.UUID{id}, LeaseUntilMs: 9999},
				},
			}
			var cancelledAt time.Time
			pub := &fakeReadyPublisher{
				onPublish: func(_ context.Context, _ []uuid.UUID, _ int) error {
					// 模拟推入 Kafka 确认的瞬间收到 SIGTERM：Forwarder 仍需用受 roundTimeout 约束的独立短超时完成条件 ZREM。
					cancelledAt = time.Now()
					cancel()
					return nil
				},
			}

			fwd, err := scheduler.NewDelayedForwarder(zap.NewNop(), queue, pub, scheduler.DelayedForwarderConfig{
				BatchSize:       10,
				PollInterval:    20 * time.Millisecond,
				LeaseTTL:        tc.leaseTTL,
				ErrorBackoffCap: 100 * time.Millisecond,
			})
			if err != nil {
				t.Fatalf("NewDelayedForwarder failed: %v", err)
			}

			if err := fwd.Run(ctx); err != nil {
				t.Fatalf("expected clean nil exit on shutdown, got %v", err)
			}

			queue.mu.Lock()
			defer queue.mu.Unlock()
			if len(queue.ackedBatches) != 1 || queue.ackedBatches[0].LeaseUntilMs != 9999 {
				t.Fatalf("expected conditional ack executed during shutdown, got %+v", queue.ackedBatches)
			}
			if queue.ackCtxErr != nil {
				t.Fatalf("expected AckDelayed called with non-cancelled ctx, got %v", queue.ackCtxErr)
			}
			ackBudget := queue.ackDeadline.Sub(cancelledAt)
			if ackBudget < tc.minBudget || ackBudget > tc.maxBudget {
				t.Fatalf("expected shutdown ack budget in [%v, %v], got %v", tc.minBudget, tc.maxBudget, ackBudget)
			}
		})
	}
}

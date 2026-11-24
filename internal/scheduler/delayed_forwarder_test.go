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

type claimedBatch struct {
	taskIDs      []uuid.UUID
	leaseUntilMs int64
}

type fakeDelayedQueue struct {
	mu               sync.Mutex
	batches          []claimedBatch
	claimTimes       []time.Time
	claimHasDeadline bool
	ackedBatches     []claimedBatch
	ackCtxErr        error
	onAck            func(count int)
}

func (f *fakeDelayedQueue) ClaimDelayed(ctx context.Context, limit int, _ time.Duration) ([]uuid.UUID, int64, error) {
	f.mu.Lock()
	_, hasDeadline := ctx.Deadline()
	f.claimHasDeadline = hasDeadline
	f.claimTimes = append(f.claimTimes, time.Now())
	if len(f.batches) == 0 {
		f.mu.Unlock()
		return nil, 0, nil
	}
	batch := f.batches[0]
	if len(batch.taskIDs) > limit {
		batch.taskIDs = batch.taskIDs[:limit]
	}
	f.batches = f.batches[1:]
	f.mu.Unlock()
	return batch.taskIDs, batch.leaseUntilMs, nil
}

func (f *fakeDelayedQueue) AckDelayed(ctx context.Context, taskIDs []uuid.UUID, leaseUntilMs int64) error {
	f.mu.Lock()
	f.ackCtxErr = ctx.Err()
	f.ackedBatches = append(f.ackedBatches, claimedBatch{
		taskIDs:      append([]uuid.UUID(nil), taskIDs...),
		leaseUntilMs: leaseUntilMs,
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
		batches: []claimedBatch{
			{taskIDs: []uuid.UUID{id1, id2}, leaseUntilMs: 1700000030000},
			{taskIDs: []uuid.UUID{id3}, leaseUntilMs: 1700000031000},
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

	queue.mu.Lock()
	defer queue.mu.Unlock()
	if !queue.claimHasDeadline || !pub.lastHasDeadline() {
		t.Fatalf("expected outbound deadline on ClaimDelayed (%v) and PublishTaskIDs (%v)", queue.claimHasDeadline, pub.lastHasDeadline())
	}
	if len(queue.ackedBatches) != 2 ||
		queue.ackedBatches[0].leaseUntilMs != 1700000030000 ||
		queue.ackedBatches[1].leaseUntilMs != 1700000031000 {
		t.Fatalf("unexpected acked batches: %+v", queue.ackedBatches)
	}
}

func TestDelayedForwarder_PublishFailureSkipsAckAndEmptyPollPreservesBackoff(t *testing.T) {
	id := uuid.New()
	done := make(chan struct{})
	var closeOnce sync.Once

	// 在两次发布失败之间插入一轮空返回（模拟上批仍在 30s 租约期内）：空轮询不得重置发布退避。
	queue := &fakeDelayedQueue{
		batches: []claimedBatch{
			{taskIDs: []uuid.UUID{id}, leaseUntilMs: 1001}, // 发布失败 #1 -> 退避 ~25ms
			{taskIDs: nil, leaseUntilMs: 0},                // 空轮询 -> 等 PollInterval(25ms)，不重置 backoff
			{taskIDs: []uuid.UUID{id}, leaseUntilMs: 1002}, // 发布失败 #2 -> 翻倍退避 ~50ms
			{taskIDs: []uuid.UUID{id}, leaseUntilMs: 1003}, // 发布成功 -> Ack
		},
		onAck: func(count int) {
			if count == 1 {
				closeOnce.Do(func() { close(done) })
			}
		},
	}
	pub := &fakeReadyPublisher{
		failErrs: []error{errors.New("kafka down 1"), errors.New("kafka down 2")},
	}

	fwd, err := scheduler.NewDelayedForwarder(zap.NewNop(), queue, pub, scheduler.DelayedForwarderConfig{
		BatchSize:       10,
		PollInterval:    25 * time.Millisecond,
		LeaseTTL:        30 * time.Second,
		ErrorBackoffCap: 100 * time.Millisecond,
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
	case <-time.After(2 * time.Second):
		cancel()
		t.Fatal("timed out waiting for forwarder recovery after publish failures")
	}

	cancel()
	if err := <-errCh; err != nil {
		t.Fatalf("expected clean nil exit, got %v", err)
	}

	queue.mu.Lock()
	defer queue.mu.Unlock()
	if len(queue.ackedBatches) != 1 || queue.ackedBatches[0].leaseUntilMs != 1003 {
		t.Fatalf("expected AckDelayed only after successful publish (lease=1003), got %+v", queue.ackedBatches)
	}
	if len(queue.claimTimes) < 4 {
		t.Fatalf("expected at least 4 claims, got %d", len(queue.claimTimes))
	}
	gapAfterFail1 := queue.claimTimes[1].Sub(queue.claimTimes[0])
	gapAfterFail2 := queue.claimTimes[3].Sub(queue.claimTimes[2])
	if gapAfterFail1 < 18*time.Millisecond || gapAfterFail2 < 40*time.Millisecond {
		t.Fatalf("expected exponential backoff preserved across empty poll (~25ms, ~50ms), got gap1=%v gap2=%v", gapAfterFail1, gapAfterFail2)
	}
}

func TestDelayedForwarder_ShutdownAfterPublishStillAcksLease(t *testing.T) {
	id := uuid.New()
	ctx, cancel := context.WithCancel(context.Background())

	queue := &fakeDelayedQueue{
		batches: []claimedBatch{
			{taskIDs: []uuid.UUID{id}, leaseUntilMs: 9999},
		},
	}
	pub := &fakeReadyPublisher{
		onPublish: func(_ []uuid.UUID, _ int) error {
			// 模拟推入 Kafka 确认的瞬间收到 SIGTERM：Forwarder 仍需用独立短超时完成条件 ZREM。
			cancel()
			return nil
		},
	}

	fwd, err := scheduler.NewDelayedForwarder(zap.NewNop(), queue, pub, scheduler.DelayedForwarderConfig{
		BatchSize:       10,
		PollInterval:    20 * time.Millisecond,
		LeaseTTL:        30 * time.Second,
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
	if len(queue.ackedBatches) != 1 || queue.ackedBatches[0].leaseUntilMs != 9999 {
		t.Fatalf("expected conditional ack executed during shutdown, got %+v", queue.ackedBatches)
	}
	if queue.ackCtxErr != nil {
		t.Fatalf("expected AckDelayed called with non-cancelled ctx, got %v", queue.ackCtxErr)
	}
}

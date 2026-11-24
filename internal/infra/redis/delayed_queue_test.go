package redis_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/awu123-create/ferry/internal/domain"
	"github.com/awu123-create/ferry/internal/infra/redis"
)

func openCleanDelayedRedis(t *testing.T) *redis.Client {
	t.Helper()
	client := openTestRedis(t)

	flush := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := client.Raw().FlushDB(ctx).Err(); err != nil {
			t.Fatalf("flush redis unit db failed: %v", err)
		}
	}
	flush()
	t.Cleanup(flush)
	return client
}

func redisNow(t *testing.T, client *redis.Client) time.Time {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ts, err := client.Raw().Time(ctx).Result()
	if err != nil {
		t.Fatalf("read redis TIME failed: %v", err)
	}
	return ts.UTC()
}

func TestDelayedQueue_ClaimLeaseConditionalAckAndExpiry(t *testing.T) {
	client := openCleanDelayedRedis(t)
	ctx := context.Background()
	now := redisNow(t, client)

	dueAcked := uuid.New()
	dueRescheduled := uuid.New()
	dueUnacked := uuid.New()
	futureTask := uuid.New()

	if err := client.ScheduleDelayedBatch(ctx, []domain.OutboxItem{
		{TaskID: dueAcked, DeliverAt: now.Add(-3 * time.Second)},
		{TaskID: dueRescheduled, DeliverAt: now.Add(-2 * time.Second)},
		{TaskID: futureTask, DeliverAt: now.Add(30 * time.Second)},
	}); err != nil {
		t.Fatalf("ScheduleDelayedBatch failed: %v", err)
	}

	// 1. 仅领取已到期的 2 条任务并施加 5s 租约，未来任务不受影响。
	claimed, leaseUntilMs, err := client.ClaimDelayed(ctx, 10, 5*time.Second)
	if err != nil {
		t.Fatalf("ClaimDelayed failed: %v", err)
	}
	if len(claimed) != 2 || leaseUntilMs <= now.UnixMilli() {
		t.Fatalf("expected 2 claimed due tasks with future lease, got ids=%v lease=%d", claimed, leaseUntilMs)
	}

	// 租约期内再次领取必须返回空。
	claimedAgain, _, err := client.ClaimDelayed(ctx, 10, 5*time.Second)
	if err != nil || len(claimedAgain) != 0 {
		t.Fatalf("expected 0 tasks while leased, got ids=%v err=%v", claimedAgain, err)
	}

	// 2. 并发期间 Worker 覆写了 dueRescheduled 的新排期，条件 Ack 只删除分数未变的 dueAcked。
	newRunAt := now.Add(20 * time.Second)
	if err := client.ScheduleDelayed(ctx, dueRescheduled, newRunAt); err != nil {
		t.Fatalf("ScheduleDelayed overwrite failed: %v", err)
	}
	if err := client.AckDelayed(ctx, claimed, leaseUntilMs); err != nil {
		t.Fatalf("AckDelayed failed: %v", err)
	}

	if _, err := client.Raw().ZScore(ctx, "ferry:delayed", dueAcked.String()).Result(); err == nil {
		t.Fatalf("expected dueAcked removed after conditional ack")
	}
	reschedScore, err := client.Raw().ZScore(ctx, "ferry:delayed", dueRescheduled.String()).Result()
	if err != nil || int64(reschedScore) != newRunAt.UnixMilli() {
		t.Fatalf("expected dueRescheduled preserved with score=%d, got score=%v err=%v", newRunAt.UnixMilli(), reschedScore, err)
	}

	// 3. 显式 RemoveDelayed 可移除排队中的未来任务。
	if err := client.RemoveDelayed(ctx, futureTask); err != nil {
		t.Fatalf("RemoveDelayed failed: %v", err)
	}
	if _, err := client.Raw().ZScore(ctx, "ferry:delayed", futureTask.String()).Result(); err == nil {
		t.Fatalf("expected futureTask removed by RemoveDelayed")
	}

	// 4. 模拟 Forwarder 领取后未 Ack 崩溃：短租约到期后任务自动恢复可领取。
	if err := client.ScheduleDelayed(ctx, dueUnacked, redisNow(t, client).Add(-time.Second)); err != nil {
		t.Fatalf("ScheduleDelayed dueUnacked failed: %v", err)
	}
	firstClaim, _, err := client.ClaimDelayed(ctx, 10, 150*time.Millisecond)
	if err != nil || len(firstClaim) != 1 || firstClaim[0] != dueUnacked {
		t.Fatalf("expected first claim [%s], got %v err=%v", dueUnacked, firstClaim, err)
	}

	deadline := time.Now().Add(2 * time.Second)
	var reclaimed []uuid.UUID
	for time.Now().Before(deadline) {
		ids, leaseMs, claimErr := client.ClaimDelayed(ctx, 10, time.Second)
		if claimErr != nil {
			t.Fatalf("re-claim failed: %v", claimErr)
		}
		if len(ids) > 0 {
			reclaimed = ids
			if ackErr := client.AckDelayed(ctx, ids, leaseMs); ackErr != nil {
				t.Fatalf("ack reclaimed failed: %v", ackErr)
			}
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if len(reclaimed) != 1 || reclaimed[0] != dueUnacked {
		t.Fatalf("expected dueUnacked reclaimed after lease expiry, got %v", reclaimed)
	}
}

func TestDelayedQueue_DeferContendedTaskUsesLockPTTLAndFallbackNX(t *testing.T) {
	client := openCleanDelayedRedis(t)
	ctx := context.Background()

	// 场景 1：锁仍在有效期内 -> 按锁剩余 PTTL 写入唤醒点，且 ZADD NX 不覆盖已有更晚排期。
	lockedTask := uuid.New()
	token, acquired, err := client.AcquireExecutionLock(ctx, lockedTask, 4*time.Second)
	if err != nil || !acquired {
		t.Fatalf("AcquireExecutionLock failed: acquired=%v err=%v", acquired, err)
	}
	t.Cleanup(func() {
		_ = client.ReleaseExecutionLock(context.Background(), lockedTask, token)
	})

	beforeMs := redisNow(t, client).UnixMilli()
	if err := client.DeferContendedTask(ctx, lockedTask, 10*time.Second); err != nil {
		t.Fatalf("DeferContendedTask(locked) failed: %v", err)
	}
	score, err := client.Raw().ZScore(ctx, "ferry:delayed", lockedTask.String()).Result()
	if err != nil {
		t.Fatalf("expected lockedTask in ferry:delayed, got err=%v", err)
	}
	deltaMs := int64(score) - beforeMs
	if deltaMs < 2000 || deltaMs > 5000 {
		t.Fatalf("expected wake score based on ~4s lock PTTL (not 10s fallback), got deltaMs=%d", deltaMs)
	}

	// 若任务已存在排期，再次 DeferContendedTask 因 ZADD NX 不得改写现有分数。
	existingRunAt := redisNow(t, client).Add(45 * time.Second)
	if err := client.ScheduleDelayed(ctx, lockedTask, existingRunAt); err != nil {
		t.Fatalf("ScheduleDelayed overwrite failed: %v", err)
	}
	if err := client.DeferContendedTask(ctx, lockedTask, 10*time.Second); err != nil {
		t.Fatalf("second DeferContendedTask failed: %v", err)
	}
	afterScore, err := client.Raw().ZScore(ctx, "ferry:delayed", lockedTask.String()).Result()
	if err != nil || int64(afterScore) != existingRunAt.UnixMilli() {
		t.Fatalf("expected ZADD NX to preserve existing score=%d, got %v err=%v", existingRunAt.UnixMilli(), afterScore, err)
	}

	// 场景 2：锁已消失（PTTL=-2）-> 按 fallbackDelay 兜底写入唤醒点。
	unlockedTask := uuid.New()
	beforeFallbackMs := redisNow(t, client).UnixMilli()
	if err := client.DeferContendedTask(ctx, unlockedTask, 3*time.Second); err != nil {
		t.Fatalf("DeferContendedTask(fallback) failed: %v", err)
	}
	fbScore, err := client.Raw().ZScore(ctx, "ferry:delayed", unlockedTask.String()).Result()
	if err != nil {
		t.Fatalf("expected unlockedTask in ferry:delayed, got err=%v", err)
	}
	fbDeltaMs := int64(fbScore) - beforeFallbackMs
	if fbDeltaMs < 2500 || fbDeltaMs > 4000 {
		t.Fatalf("expected wake score based on ~3s fallbackDelay, got deltaMs=%d", fbDeltaMs)
	}
}

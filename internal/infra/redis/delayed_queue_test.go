package redis_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"

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
	batch, err := client.ClaimDelayed(ctx, 10, 5*time.Second)
	if err != nil {
		t.Fatalf("ClaimDelayed failed: %v", err)
	}
	if len(batch.TaskIDs) != 2 || batch.LeaseUntilMs <= now.UnixMilli() {
		t.Fatalf("expected 2 claimed due tasks with future lease, got batch=%+v", batch)
	}

	// 租约期内再次领取必须返回空。
	claimedAgain, err := client.ClaimDelayed(ctx, 10, 5*time.Second)
	if err != nil || len(claimedAgain.TaskIDs) != 0 {
		t.Fatalf("expected 0 tasks while leased, got batch=%+v err=%v", claimedAgain, err)
	}

	// 2. 并发期间 Worker 覆写了 dueRescheduled 的新排期，条件 Ack 只删除分数与租约均未变的 dueAcked。
	newRunAt := now.Add(20 * time.Second).Truncate(time.Millisecond)
	if err := client.ScheduleDelayed(ctx, dueRescheduled, newRunAt, 0); err != nil {
		t.Fatalf("ScheduleDelayed overwrite failed: %v", err)
	}
	if err := client.AckDelayed(ctx, batch); err != nil {
		t.Fatalf("AckDelayed failed: %v", err)
	}

	if _, err := client.Raw().ZScore(ctx, "ferry:delayed", dueAcked.String()).Result(); !errors.Is(err, goredis.Nil) {
		t.Fatalf("expected dueAcked removed after conditional ack, got err=%v", err)
	}
	if _, err := client.Raw().HGet(ctx, "ferry:delayed:lease", dueAcked.String()).Result(); !errors.Is(err, goredis.Nil) {
		t.Fatalf("expected dueAcked lease marker removed after ack, got err=%v", err)
	}
	reschedScore, err := client.Raw().ZScore(ctx, "ferry:delayed", dueRescheduled.String()).Result()
	if err != nil || int64(reschedScore) != newRunAt.UnixMilli() {
		t.Fatalf("expected dueRescheduled preserved with score=%d, got score=%v err=%v", newRunAt.UnixMilli(), reschedScore, err)
	}

	// 3. 显式 CancelDelayed 同时移除延迟池成员、租约登记并写入带 TTL 的取消标记。
	if err := client.CancelDelayed(ctx, futureTask, 2*time.Minute); err != nil {
		t.Fatalf("CancelDelayed failed: %v", err)
	}
	if _, err := client.Raw().ZScore(ctx, "ferry:delayed", futureTask.String()).Result(); !errors.Is(err, goredis.Nil) {
		t.Fatalf("expected futureTask removed by CancelDelayed, got err=%v", err)
	}

	// 4. 模拟 Forwarder 领取后未 Ack 崩溃：短租约到期后任务自动恢复可领取。
	if err := client.ScheduleDelayed(ctx, dueUnacked, redisNow(t, client).Add(-time.Second), 0); err != nil {
		t.Fatalf("ScheduleDelayed dueUnacked failed: %v", err)
	}
	firstBatch, err := client.ClaimDelayed(ctx, 10, 150*time.Millisecond)
	if err != nil || len(firstBatch.TaskIDs) != 1 || firstBatch.TaskIDs[0] != dueUnacked {
		t.Fatalf("expected first claim [%s], got %+v err=%v", dueUnacked, firstBatch, err)
	}

	deadline := time.Now().Add(2 * time.Second)
	var reclaimed []uuid.UUID
	for time.Now().Before(deadline) {
		reclaimBatch, claimErr := client.ClaimDelayed(ctx, 10, time.Second)
		if claimErr != nil {
			t.Fatalf("re-claim failed: %v", claimErr)
		}
		if len(reclaimBatch.TaskIDs) > 0 {
			reclaimed = reclaimBatch.TaskIDs
			if ackErr := client.AckDelayed(ctx, reclaimBatch); ackErr != nil {
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

func TestDelayedQueue_DeferContendedTaskUsesLockPTTLAndFallback(t *testing.T) {
	client := openCleanDelayedRedis(t)
	ctx := context.Background()

	// 场景 1：锁仍在有效期内 -> 按锁剩余 PTTL 写入唤醒点，且不覆盖已有真实排期。
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

	// 若任务已存在真实未来排期，再次 DeferContendedTask 不得改写现有分数。
	existingRunAt := redisNow(t, client).Add(45 * time.Second).Truncate(time.Millisecond)
	if err := client.ScheduleDelayed(ctx, lockedTask, existingRunAt, 0); err != nil {
		t.Fatalf("ScheduleDelayed overwrite failed: %v", err)
	}
	if err := client.DeferContendedTask(ctx, lockedTask, 10*time.Second); err != nil {
		t.Fatalf("second DeferContendedTask failed: %v", err)
	}
	afterScore, err := client.Raw().ZScore(ctx, "ferry:delayed", lockedTask.String()).Result()
	if err != nil || int64(afterScore) != existingRunAt.UnixMilli() {
		t.Fatalf("expected genuine schedule preserved score=%d, got %v err=%v", existingRunAt.UnixMilli(), afterScore, err)
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

func TestDelayedQueue_ContentionReplacesLeaseAndPreservesGenuineSchedule(t *testing.T) {
	client := openCleanDelayedRedis(t)
	ctx := context.Background()
	now := redisNow(t, client)

	// 场景 1：Forwarder 刚取走并加上 30s 临时租约、还没删租约时，Worker 没抢到锁调用 DeferContendedTask：
	// 应该识别出这只是临时租约并改写成新的唤醒时间，同时删掉租约记录，后面 AckDelayed（哪怕时间戳凑巧同毫秒）也不能误删。
	leasedContended := uuid.New()
	if err := client.ScheduleDelayed(ctx, leasedContended, now.Add(-time.Second), 0); err != nil {
		t.Fatalf("ScheduleDelayed(leasedContended) failed: %v", err)
	}
	batch, err := client.ClaimDelayed(ctx, 10, 30*time.Second)
	if err != nil || len(batch.TaskIDs) != 1 || batch.TaskIDs[0] != leasedContended {
		t.Fatalf("ClaimDelayed expected [%s], got %+v err=%v", leasedContended, batch, err)
	}
	if _, err := client.Raw().HGet(ctx, "ferry:delayed:lease", leasedContended.String()).Result(); err != nil {
		t.Fatalf("expected lease marker present in ferry:delayed:lease, got err=%v", err)
	}

	beforeDeferMs := redisNow(t, client).UnixMilli()
	if err := client.DeferContendedTask(ctx, leasedContended, 2*time.Second); err != nil {
		t.Fatalf("DeferContendedTask on leased member failed: %v", err)
	}

	wakeScore, err := client.Raw().ZScore(ctx, "ferry:delayed", leasedContended.String()).Result()
	if err != nil {
		t.Fatalf("expected member still in ferry:delayed after DeferContendedTask: %v", err)
	}
	wakeDelta := int64(wakeScore) - beforeDeferMs
	if wakeDelta < 1500 || wakeDelta > 4000 {
		t.Fatalf("expected temporary 30s lease replaced by ~2s wake score, got deltaMs=%d (leaseUntilMs=%d)", wakeDelta, batch.LeaseUntilMs)
	}
	if _, err := client.Raw().HGet(ctx, "ferry:delayed:lease", leasedContended.String()).Result(); !errors.Is(err, goredis.Nil) {
		t.Fatalf("expected lease marker cleared after DeferContendedTask, got err=%v", err)
	}

	// 无论 Forwarder 携带原 leaseUntilMs 还是碰巧与 wakeScore 同毫秒，AckDelayed 都不得误删。
	if err := client.AckDelayed(ctx, batch); err != nil {
		t.Fatalf("AckDelayed(original lease) failed: %v", err)
	}
	if err := client.AckDelayed(ctx, domain.DelayedLeaseBatch{
		TaskIDs:      []uuid.UUID{leasedContended},
		LeaseUntilMs: int64(wakeScore),
	}); err != nil {
		t.Fatalf("AckDelayed(same-ms boundary) failed: %v", err)
	}
	survivedScore, err := client.Raw().ZScore(ctx, "ferry:delayed", leasedContended.String()).Result()
	if err != nil || int64(survivedScore) != int64(wakeScore) {
		t.Fatalf("expected contended wake entry preserved across AckDelayed, got score=%v err=%v", survivedScore, err)
	}

	// 场景 2：持锁方 ScheduleDelayed 写入新排期（即使恰好等于 leaseUntilMs 同毫秒），
	// 随后的 DeferContendedTask 不得覆盖该排期，AckDelayed 也不得误删。
	sameMsTask := uuid.New()
	if err := client.ScheduleDelayed(ctx, sameMsTask, redisNow(t, client).Add(-time.Second), 0); err != nil {
		t.Fatalf("ScheduleDelayed(sameMsTask) failed: %v", err)
	}
	sameMsBatch, err := client.ClaimDelayed(ctx, 10, 15*time.Second)
	if err != nil || len(sameMsBatch.TaskIDs) != 1 || sameMsBatch.TaskIDs[0] != sameMsTask {
		t.Fatalf("ClaimDelayed(sameMsTask) got %+v err=%v", sameMsBatch, err)
	}
	exactLeaseTime := time.UnixMilli(sameMsBatch.LeaseUntilMs).UTC()
	if err := client.ScheduleDelayed(ctx, sameMsTask, exactLeaseTime, 0); err != nil {
		t.Fatalf("ScheduleDelayed(sameMs exact lease time) failed: %v", err)
	}
	if err := client.DeferContendedTask(ctx, sameMsTask, 2*time.Second); err != nil {
		t.Fatalf("DeferContendedTask(after genuine schedule) failed: %v", err)
	}
	if err := client.AckDelayed(ctx, sameMsBatch); err != nil {
		t.Fatalf("AckDelayed(sameMsBatch) failed: %v", err)
	}
	gotSameMsScore, err := client.Raw().ZScore(ctx, "ferry:delayed", sameMsTask.String()).Result()
	if err != nil || int64(gotSameMsScore) != sameMsBatch.LeaseUntilMs {
		t.Fatalf("expected genuine schedule at exact lease ms preserved (%d), got %v err=%v", sameMsBatch.LeaseUntilMs, gotSameMsScore, err)
	}

	// 场景 3：CancelDelayed 同步清理 ZSet 与 lease Hash，并立下取消标记。
	cancelLeased := uuid.New()
	if err := client.ScheduleDelayed(ctx, cancelLeased, redisNow(t, client).Add(-time.Second), 0); err != nil {
		t.Fatalf("ScheduleDelayed(cancelLeased) failed: %v", err)
	}
	if _, err := client.ClaimDelayed(ctx, 10, 10*time.Second); err != nil {
		t.Fatalf("ClaimDelayed(cancelLeased) failed: %v", err)
	}
	if err := client.CancelDelayed(ctx, cancelLeased, 2*time.Minute); err != nil {
		t.Fatalf("CancelDelayed(cancelLeased) failed: %v", err)
	}
	if _, err := client.Raw().ZScore(ctx, "ferry:delayed", cancelLeased.String()).Result(); !errors.Is(err, goredis.Nil) {
		t.Fatalf("expected cancelLeased absent from ZSet, got err=%v", err)
	}
	if _, err := client.Raw().HGet(ctx, "ferry:delayed:lease", cancelLeased.String()).Result(); !errors.Is(err, goredis.Nil) {
		t.Fatalf("expected cancelLeased absent from lease hash, got err=%v", err)
	}
}

func TestDelayedQueue_CancelMarkerBlocksLateWritesAndPreservesBatchSiblings(t *testing.T) {
	client := openCleanDelayedRedis(t)
	ctx := context.Background()
	now := redisNow(t, client)

	cancelledID := uuid.New()
	healthyID := uuid.New()

	if err := client.CancelDelayed(ctx, cancelledID, 0); err == nil {
		t.Fatal("expected error when cancelMarkerTTL <= 0")
	}

	// 先把任务放进延迟池并领出挂租约，再调 CancelDelayed 清理并立下 2 分钟取消标记。
	if err := client.ScheduleDelayed(ctx, cancelledID, now.Add(-time.Second), 0); err != nil {
		t.Fatalf("ScheduleDelayed(cancelledID) failed: %v", err)
	}
	if _, err := client.ClaimDelayed(ctx, 10, 30*time.Second); err != nil {
		t.Fatalf("ClaimDelayed(cancelledID) failed: %v", err)
	}
	if err := client.CancelDelayed(ctx, cancelledID, 2*time.Minute); err != nil {
		t.Fatalf("CancelDelayed failed: %v", err)
	}

	pttl, err := client.Raw().PTTL(ctx, "ferry:delayed:cancelled:"+cancelledID.String()).Result()
	if err != nil || pttl <= 0 || pttl > 2*time.Minute {
		t.Fatalf("expected cancel marker key PTTL in (0, 2m], got pttl=%v err=%v", pttl, err)
	}
	if _, err := client.Raw().ZScore(ctx, "ferry:delayed", cancelledID.String()).Result(); !errors.Is(err, goredis.Nil) {
		t.Fatalf("expected cancelledID absent from ferry:delayed, got err=%v", err)
	}
	if _, err := client.Raw().HGet(ctx, "ferry:delayed:lease", cancelledID.String()).Result(); !errors.Is(err, goredis.Nil) {
		t.Fatalf("expected cancelledID absent from ferry:delayed:lease, got err=%v", err)
	}

	// 取消标记生效期间：DeferContendedTask、ScheduleDelayed 和 ScheduleDelayedBatch 都不得把已取消任务重新写进延迟池，
	// 但同一批次里没被取消的 healthyID 必须照常入池。
	if err := client.DeferContendedTask(ctx, cancelledID, time.Second); err != nil {
		t.Fatalf("DeferContendedTask on cancelled task failed: %v", err)
	}
	if _, err := client.Raw().ZScore(ctx, "ferry:delayed", cancelledID.String()).Result(); !errors.Is(err, goredis.Nil) {
		t.Fatalf("expected DeferContendedTask rejected by cancel marker, got err=%v", err)
	}

	futureRunAt := now.Add(30 * time.Second).Truncate(time.Millisecond)
	if err := client.ScheduleDelayed(ctx, cancelledID, futureRunAt, time.Second); err != nil {
		t.Fatalf("ScheduleDelayed on cancelled task failed: %v", err)
	}
	if _, err := client.Raw().ZScore(ctx, "ferry:delayed", cancelledID.String()).Result(); !errors.Is(err, goredis.Nil) {
		t.Fatalf("expected ScheduleDelayed rejected by cancel marker, got err=%v", err)
	}

	if err := client.ScheduleDelayedBatch(ctx, []domain.OutboxItem{
		{TaskID: cancelledID, DeliverAt: futureRunAt},
		{TaskID: healthyID, DeliverAt: futureRunAt},
	}); err != nil {
		t.Fatalf("ScheduleDelayedBatch with cancelled sibling failed: %v", err)
	}
	if _, err := client.Raw().ZScore(ctx, "ferry:delayed", cancelledID.String()).Result(); !errors.Is(err, goredis.Nil) {
		t.Fatalf("expected cancelledID skipped in ScheduleDelayedBatch, got err=%v", err)
	}
	gotHealthy, err := client.Raw().ZScore(ctx, "ferry:delayed", healthyID.String()).Result()
	if err != nil || int64(gotHealthy) != futureRunAt.UnixMilli() {
		t.Fatalf("expected healthyID scheduled at %d, got score=%v err=%v", futureRunAt.UnixMilli(), gotHealthy, err)
	}
}

func TestDelayedQueue_ScheduleCeilsSubMillisecondAndEnforcesMinDelay(t *testing.T) {
	client := openCleanDelayedRedis(t)
	ctx := context.Background()

	// 1. 不足 1ms 的微秒尾数必须向上进到下一毫秒，整毫秒不变。
	exactMs := time.Date(2026, 11, 24, 15, 0, 0, 123_000_000, time.UTC)
	subMs := time.Date(2026, 11, 24, 15, 0, 0, 123_001_000, time.UTC)
	exactID, subBatchID, subSingleID := uuid.New(), uuid.New(), uuid.New()

	if err := client.ScheduleDelayedBatch(ctx, []domain.OutboxItem{
		{TaskID: exactID, DeliverAt: exactMs},
		{TaskID: subBatchID, DeliverAt: subMs},
	}); err != nil {
		t.Fatalf("ScheduleDelayedBatch failed: %v", err)
	}
	if err := client.ScheduleDelayed(ctx, subSingleID, subMs, 0); err != nil {
		t.Fatalf("ScheduleDelayed(subMs) failed: %v", err)
	}

	gotExact, err := client.Raw().ZScore(ctx, "ferry:delayed", exactID.String()).Result()
	if err != nil || int64(gotExact) != exactMs.UnixMilli() {
		t.Fatalf("expected exact ms %d unchanged, got %v err=%v", exactMs.UnixMilli(), gotExact, err)
	}
	gotSubBatch, err := client.Raw().ZScore(ctx, "ferry:delayed", subBatchID.String()).Result()
	if err != nil || int64(gotSubBatch) != subMs.UnixMilli()+1 {
		t.Fatalf("expected batch sub-ms rounded up to %d, got %v err=%v", subMs.UnixMilli()+1, gotSubBatch, err)
	}
	gotSubSingle, err := client.Raw().ZScore(ctx, "ferry:delayed", subSingleID.String()).Result()
	if err != nil || int64(gotSubSingle) != subMs.UnixMilli()+1 {
		t.Fatalf("expected single sub-ms rounded up to %d, got %v err=%v", subMs.UnixMilli()+1, gotSubSingle, err)
	}

	// 2. 模拟 Redis 时钟比 Worker 快 200ms：传进去的 runAt 在 Redis 看来已经过去了，但带上 minDelay=300ms；
	//    脚本按 Redis 自己的当前时间把分数抬到 now + 300ms，避免刚放回去又被 Forwarder 立刻捞出、来回空转。
	skewedID := uuid.New()
	beforeMs := redisNow(t, client).UnixMilli()
	pastInRedis := time.UnixMilli(beforeMs - 200).UTC()
	if err := client.ScheduleDelayed(ctx, skewedID, pastInRedis, 300*time.Millisecond); err != nil {
		t.Fatalf("ScheduleDelayed(minDelay) failed: %v", err)
	}
	gotSkewed, err := client.Raw().ZScore(ctx, "ferry:delayed", skewedID.String()).Result()
	if err != nil {
		t.Fatalf("ZScore(skewedID) failed: %v", err)
	}
	if delta := int64(gotSkewed) - beforeMs; delta < 280 || delta > 1500 {
		t.Fatalf("expected minDelay floor >= ~300ms ahead of Redis TIME, got delta=%dms", delta)
	}
}

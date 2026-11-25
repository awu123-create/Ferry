package integration_test

import (
	"context"
	"encoding/json/v2"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"github.com/awu123-create/ferry/internal/domain"
	"github.com/awu123-create/ferry/internal/scheduler"
)

type failingDelayedEnqueuer struct {
	err error
}

func (f failingDelayedEnqueuer) ScheduleDelayedBatch(_ context.Context, _ []domain.OutboxItem) error {
	return f.err
}

func (h *flowHarness) redisNow() time.Time {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ts, err := h.redisCli.Raw().Time(ctx).Result()
	if err != nil {
		h.t.Fatalf("read redis TIME failed: %v", err)
	}
	return ts.UTC()
}

func (h *flowHarness) delayedScore(taskID uuid.UUID) (float64, bool) {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	score, err := h.redisCli.Raw().ZScore(ctx, "ferry:delayed", taskID.String()).Result()
	if errors.Is(err, goredis.Nil) {
		return 0, false
	}
	if err != nil {
		h.t.Fatalf("ZScore ferry:delayed %s failed: %v", taskID, err)
	}
	return score, true
}

func (h *flowHarness) delayedLeaseExists(taskID uuid.UUID) bool {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := h.redisCli.Raw().HGet(ctx, "ferry:delayed:lease", taskID.String()).Result()
	if errors.Is(err, goredis.Nil) {
		return false
	}
	if err != nil {
		h.t.Fatalf("HGet ferry:delayed:lease %s failed: %v", taskID, err)
	}
	return true
}

func (h *flowHarness) createTaskWithDeliverAt(epID uuid.UUID, idemKey string, deliverAt *time.Time) (int, taskResp) {
	h.t.Helper()
	body := map[string]any{
		"endpoint_id": epID.String(),
		"payload":     map[string]any{"key": idemKey},
	}
	if deliverAt != nil {
		body["deliver_at"] = deliverAt.UTC().Format(time.RFC3339Nano)
	}
	raw, err := json.Marshal(body)
	if err != nil {
		h.t.Fatalf("marshal create task request: %v", err)
	}
	code, respBody := h.doJSON(http.MethodPost, "/v1/tasks", raw, map[string]string{
		"Idempotency-Key": idemKey,
	})
	var out taskResp
	if code == http.StatusAccepted || code == http.StatusOK {
		if err := json.Unmarshal(respBody, &out); err != nil {
			h.t.Fatalf("decode task response: %v (body=%s)", err, respBody)
		}
	}
	return code, out
}

// TestDelayedDeliveryAndCancelIntegration 覆盖未来定时投递、过去时间立即投递，
// 以及延迟排队期间取消任务后同步清理 Redis 延迟池且到期零出站请求。
func TestDelayedDeliveryAndCancelIntegration(t *testing.T) {
	var mu sync.Mutex
	receivedAt := make(map[string]time.Time)
	var cancelTargetHits atomic.Int32
	var h *flowHarness

	// 先注册 downstream.Close 再初始化 harness，保证 LIFO 清理时先停 Worker/Forwarder/Relay 再关模拟下游。
	downstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/deliver":
			taskID := r.Header.Get("X-Ferry-Task-ID")
			now := h.redisNow()
			mu.Lock()
			receivedAt[taskID] = now
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
		case "/cancelled":
			cancelTargetHits.Add(1)
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(downstream.Close)

	h = newFlowHarness(t)
	h.relayRunner.start()
	h.forwarderRunner.start()
	h.workerRunner.start()

	epDeliver := h.createEndpoint("delayed-deliver", downstream.URL+"/deliver", "", 5000)
	epCancel := h.createEndpoint("delayed-cancel", downstream.URL+"/cancelled", "", 5000)

	// 1. 过去时间的 deliver_at 按立即投递处理。
	past := h.redisNow().Add(-5 * time.Second)
	pastCode, pastTask := h.createTaskWithDeliverAt(epDeliver.ID, "past-immediate-1", &past)
	if pastCode != http.StatusAccepted {
		t.Fatalf("expected 202 for past deliver_at, got %d", pastCode)
	}
	waitUntil(t, defaultIntegrationWaitTime, "past deliver_at task succeeds immediately", func() bool {
		return h.getTask(pastTask.ID).Status == string(domain.TaskStatusSucceeded)
	})

	// 2. 未来 1.8s 定时任务：先落入 ferry:delayed，到期前下游未收到请求，到期后准确投递并迁移 SUCCEEDED。
	futureDeliverAt := h.redisNow().Add(1800 * time.Millisecond)
	futCode, futTask := h.createTaskWithDeliverAt(epDeliver.ID, "future-1", &futureDeliverAt)
	if futCode != http.StatusAccepted {
		t.Fatalf("expected 202 for future deliver_at, got %d", futCode)
	}
	waitUntil(t, 5*time.Second, "future task enters ferry:delayed", func() bool {
		_, ok := h.delayedScore(futTask.ID)
		return ok
	})

	if h.redisNow().Before(futureDeliverAt.Add(-300 * time.Millisecond)) {
		mu.Lock()
		_, premature := receivedAt[futTask.ID.String()]
		mu.Unlock()
		if premature {
			t.Fatalf("future task %s delivered before scheduled deliver_at %v", futTask.ID, futureDeliverAt)
		}
	}

	waitUntil(t, defaultIntegrationWaitTime, "future task transitions to SUCCEEDED after due time", func() bool {
		return h.getTask(futTask.ID).Status == string(domain.TaskStatusSucceeded)
	})
	mu.Lock()
	gotAt, ok := receivedAt[futTask.ID.String()]
	mu.Unlock()
	if !ok {
		t.Fatalf("expected downstream hit for future task %s", futTask.ID)
	}
	if gotAt.Before(futureDeliverAt.Add(-50 * time.Millisecond)) {
		t.Fatalf("future task delivered too early: received=%v deliver_at=%v", gotAt, futureDeliverAt)
	}

	// 3. 延迟排队中的任务调用取消接口：置为 CANCELLED、立即移出 ferry:delayed、到期后零出站请求。
	cancelAt := h.redisNow().Add(1200 * time.Millisecond)
	cCode, cTask := h.createTaskWithDeliverAt(epCancel.ID, "cancel-delayed-1", &cancelAt)
	if cCode != http.StatusAccepted {
		t.Fatalf("expected 202 for cancel delayed task, got %d", cCode)
	}
	waitUntil(t, 5*time.Second, "cancel task enters ferry:delayed", func() bool {
		_, exists := h.delayedScore(cTask.ID)
		return exists
	})

	cancelHTTPCode, cancelBody := h.doJSON(http.MethodPost, "/v1/tasks/"+cTask.ID.String()+"/cancel", nil, nil)
	if cancelHTTPCode != http.StatusOK {
		t.Fatalf("cancel delayed task expected 200, got %d: %s", cancelHTTPCode, cancelBody)
	}
	if _, exists := h.delayedScore(cTask.ID); exists {
		t.Fatalf("expected cancelled task %s removed from ferry:delayed", cTask.ID)
	}
	if got := h.getTask(cTask.ID); got.Status != string(domain.TaskStatusCancelled) {
		t.Fatalf("expected CANCELLED status, got %+v", got)
	}

	waitUntil(t, 5*time.Second, "original cancelAt passes", func() bool {
		return h.redisNow().After(cancelAt.Add(300 * time.Millisecond))
	})
	if cancelTargetHits.Load() != 0 || len(h.listAttempts(cTask.ID)) != 0 {
		t.Fatalf("expected zero deliveries for cancelled delayed task, hits=%d attempts=%d", cancelTargetHits.Load(), len(h.listAttempts(cTask.ID)))
	}
}

// TestWorkerContentionAndEarlyArrivalIntegration 验证两类常见并发边界：
//  1. 预约任务提前 ~600ms 到达 Worker 时不能提前发 HTTP，放回延迟池等到点再投递；
//  2. 同一条预约任务先后经历“早到放回 -> 到点被 Forwarder 挂上 30s 租约 -> 并发 Worker 没抢到锁改写唤醒时间 ->
//     Forwarder 随后调用 AckDelayed”，成员不会被误删，锁过期后正常投递成功。
func TestWorkerContentionAndEarlyArrivalIntegration(t *testing.T) {
	ctx := context.Background()

	var mu sync.Mutex
	hits := make(map[string]int)
	receivedAt := make(map[string]time.Time)
	var h *flowHarness

	downstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Ferry-Task-ID")
		now := h.redisNow()
		mu.Lock()
		hits[id]++
		receivedAt[id] = now
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(downstream.Close)

	h = newFlowHarness(t)
	ep := h.createEndpoint("worker-guard", downstream.URL+"/hook", "", 5000)

	// 先跑通一条热身立即任务，确保 Kafka 消费组已完成初次 Join，避免初始重平衡耗时吃掉 500ms 早到窗口。
	h.workerRunner.start()
	warmCode, warmTask := h.createTaskWithDeliverAt(ep.ID, "warmup-1", nil)
	if warmCode != http.StatusAccepted {
		t.Fatalf("expected 202 for warmup task, got %d", warmCode)
	}
	if err := h.producer.PublishTaskIDs(ctx, []uuid.UUID{warmTask.ID}); err != nil {
		t.Fatalf("PublishTaskIDs(warmup) failed: %v", err)
	}
	waitUntil(t, defaultIntegrationWaitTime, "warmup task establishes kafka consumer group", func() bool {
		return h.getTask(warmTask.ID).Status == string(domain.TaskStatusSucceeded)
	})

	// 场景 1（预约任务提前 ~600ms 到达）：严格拦截、零 HTTP 请求并覆写写回 ferry:delayed，到点后经 Forwarder 准时投递。
	earlyAt := h.redisNow().Add(700 * time.Millisecond)
	earlyCode, earlyTask := h.createTaskWithDeliverAt(ep.ID, "early-arrival-500ms", &earlyAt)
	if earlyCode != http.StatusAccepted {
		t.Fatalf("expected 202 for early task, got %d", earlyCode)
	}
	if err := h.producer.PublishTaskIDs(ctx, []uuid.UUID{earlyTask.ID}); err != nil {
		t.Fatalf("PublishTaskIDs(early) failed: %v", err)
	}
	waitUntil(t, 5*time.Second, "early arrival task requeued to ferry:delayed before due time", func() bool {
		score, ok := h.delayedScore(earlyTask.ID)
		return ok && int64(score) >= earlyAt.UnixMilli()
	})
	mu.Lock()
	earlyPrematureHits := hits[earlyTask.ID.String()]
	mu.Unlock()
	if earlyPrematureHits != 0 {
		t.Fatalf("expected 0 hits when scheduled task arrived ~500ms early, got %d", earlyPrematureHits)
	}

	h.forwarderRunner.start()
	waitUntil(t, defaultIntegrationWaitTime, "early arrival task delivers on time and succeeds", func() bool {
		return h.getTask(earlyTask.ID).Status == string(domain.TaskStatusSucceeded)
	})
	h.forwarderRunner.stop()

	mu.Lock()
	earlyDeliveredAt := receivedAt[earlyTask.ID.String()]
	mu.Unlock()
	if earlyDeliveredAt.Before(earlyAt.Add(-50 * time.Millisecond)) {
		t.Fatalf("early task delivered before NextRunAt: got=%v want>=%v", earlyDeliveredAt, earlyAt)
	}

	// 场景 2（同一条预约任务依次经过：早到放回 -> Forwarder 挂 30s 租约 -> 并发 Worker 撞锁改写 -> Forwarder 延迟删租约）：
	// 1) 预约任务先被 Worker 拦下放回 ferry:delayed，并释放 Execution Lock；
	// 2) 到点后模拟 Forwarder 领走并挂上 30s Schedule Lease，还没来得及删租约；
	// 3) 另一个持锁方先占着 Execution Lock（500ms），并发 Worker 消费 Kafka 消息没抢到锁，改写唤醒时间并清掉租约记录；
	// 4) Forwarder 随后执行 AckDelayed，不能把刚改写的唤醒时间误删；锁过期后正常投递至 SUCCEEDED。
	racedAt := h.redisNow().Add(450 * time.Millisecond)
	racedCode, racedTask := h.createTaskWithDeliverAt(ep.ID, "early-lease-contend-1", &racedAt)
	if racedCode != http.StatusAccepted {
		t.Fatalf("expected 202 for raced task, got %d", racedCode)
	}
	var earlyAcked bool
	if err := h.worker.ProcessDelivery(ctx, domain.ReadyDelivery{
		TaskID: racedTask.ID,
		Ack:    func() { earlyAcked = true },
	}); err != nil {
		t.Fatalf("ProcessDelivery(early on racedTask) failed: %v", err)
	}
	if !earlyAcked {
		t.Fatal("expected early-arrival worker to ack racedTask after requeuing")
	}
	mu.Lock()
	racedPrematureHits := hits[racedTask.ID.String()]
	mu.Unlock()
	if racedPrematureHits != 0 {
		t.Fatalf("expected 0 premature hits on racedTask early arrival, got %d", racedPrematureHits)
	}

	// 等到 racedAt 到点，模拟 Forwarder 领走该任务并挂上 30s Schedule Lease。
	var leasedBatch domain.DelayedLeaseBatch
	waitUntil(t, 5*time.Second, "forwarder claims racedTask once due", func() bool {
		batch, claimErr := h.redisCli.ClaimDelayed(ctx, 10, 30*time.Second)
		if claimErr != nil {
			t.Fatalf("ClaimDelayed(racedTask) failed: %v", claimErr)
		}
		if len(batch.TaskIDs) == 1 && batch.TaskIDs[0] == racedTask.ID {
			leasedBatch = batch
			return true
		}
		return false
	})
	if !h.delayedLeaseExists(racedTask.ID) {
		t.Fatal("expected temporary lease marker in ferry:delayed:lease after ClaimDelayed")
	}

	// 模拟别的持锁方占着 Execution Lock（500ms），并发 Worker 消费到这条消息没抢到锁，改写延迟池并清掉租约记录。
	if _, acquired, err := h.redisCli.AcquireExecutionLock(ctx, racedTask.ID, 500*time.Millisecond); err != nil || !acquired {
		t.Fatalf("pre-acquire execution lock failed: acquired=%v err=%v", acquired, err)
	}
	if err := h.producer.PublishTaskIDs(ctx, []uuid.UUID{racedTask.ID}); err != nil {
		t.Fatalf("PublishTaskIDs(racedTask contended) failed: %v", err)
	}
	waitUntil(t, 5*time.Second, "contended worker replaces temporary lease marker on racedTask", func() bool {
		return !h.delayedLeaseExists(racedTask.ID)
	})

	// 此时 Forwarder 才来执行 AckDelayed：因为租约记录已经被清掉，延迟池里的新唤醒时间必须完好保留。
	if err := h.redisCli.AckDelayed(ctx, leasedBatch); err != nil {
		t.Fatalf("AckDelayed after worker contention failed: %v", err)
	}
	if _, ok := h.delayedScore(racedTask.ID); !ok {
		t.Fatalf("racedTask %s was wrongly deleted by Forwarder AckDelayed", racedTask.ID)
	}

	// 启动 DelayedForwarder：等锁过期后，racedTask 恢复并投递成功。
	h.forwarderRunner.start()
	waitUntil(t, defaultIntegrationWaitTime, "raced task recovers and succeeds", func() bool {
		return h.getTask(racedTask.ID).Status == string(domain.TaskStatusSucceeded)
	})

	mu.Lock()
	racedDeliveredAt := receivedAt[racedTask.ID.String()]
	mu.Unlock()
	if racedDeliveredAt.Before(racedAt.Add(-50 * time.Millisecond)) {
		t.Fatalf("raced task delivered before NextRunAt: got=%v want>=%v", racedDeliveredAt, racedAt)
	}
}

// TestCancelConcurrentWithDelayedProducersZeroResidueIntegration 验证取消任务和两类延迟写入并发撞车时，
// Redis 延迟池、租约表零残留且不会触发无效 Kafka 转发：
//  1. 取消请求撞上正锁着 outbox 行往 Redis 写的 Relay -> 等 Relay 提交完后再由 CancelDelayed 清理并立取消标记；
//  2. Worker B 查完 PG（还看到 PENDING）后在写延迟池前停顿 -> Worker A 放锁 + Cancel 接口返回 200 OK ->
//     放行 Worker B 执行迟写 -> 真实 DelayedForwarder 轮询越过唤醒时长 -> 延迟池零残留且 Kafka 收到 0 条转发消息。
func TestCancelConcurrentWithDelayedProducersZeroResidueIntegration(t *testing.T) {
	ctx := context.Background()
	var downstreamHits atomic.Int32

	downstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		downstreamHits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(downstream.Close)

	h := newFlowHarness(t)
	ep := h.createEndpoint("cancel-race-ep", downstream.URL+"/never", "", 5000)

	// 子场景 1：Cancel vs 已持 FOR UPDATE 行锁正在写 Redis 的延迟 Outbox Relay。
	futureAt := h.redisNow().Add(2 * time.Minute)
	code1, relayRacedTask := h.createTaskWithDeliverAt(ep.ID, "cancel-vs-relay-1", &futureAt)
	if code1 != http.StatusAccepted {
		t.Fatalf("expected 202 for relayRacedTask, got %d", code1)
	}

	relayHolding := make(chan struct{})
	releaseRelay := make(chan struct{})
	relayErrCh := make(chan error, 1)
	go func() {
		_, err := h.pgStore.ClaimAndDispatchDelayedOutbox(ctx, h.redisNow(), 10, func(cbCtx context.Context, items []domain.OutboxItem) error {
			close(relayHolding)
			<-releaseRelay
			return h.redisCli.ScheduleDelayedBatch(cbCtx, items)
		})
		relayErrCh <- err
	}()

	select {
	case <-relayHolding:
	case <-time.After(3 * time.Second):
		close(releaseRelay)
		t.Fatal("timed out waiting for delayed outbox claim to lock row")
	}

	cancelStatusCh := make(chan int, 1)
	go func() {
		status, _ := h.doJSON(http.MethodPost, "/v1/tasks/"+relayRacedTask.ID.String()+"/cancel", nil, nil)
		cancelStatusCh <- status
	}()

	// Relay 尚未提交前，Cancel 事务的 DELETE FROM outbox 必须等待行锁释放，不能抢跑返回 200。
	select {
	case status := <-cancelStatusCh:
		close(releaseRelay)
		t.Fatalf("cancel API returned %d before delayed relay committed outbox row lock", status)
	case <-time.After(150 * time.Millisecond):
	}

	close(releaseRelay)
	if err := <-relayErrCh; err != nil {
		t.Fatalf("delayed outbox claim failed: %v", err)
	}
	select {
	case status := <-cancelStatusCh:
		if status != http.StatusOK {
			t.Fatalf("expected cancel API 200 OK after relay commit, got %d", status)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for cancel API to finish after relay commit")
	}

	if _, exists := h.delayedScore(relayRacedTask.ID); exists {
		t.Fatalf("expected zero ferry:delayed residue after cancel vs relay race for task %s", relayRacedTask.ID)
	}
	if h.delayedLeaseExists(relayRacedTask.ID) {
		t.Fatalf("expected zero ferry:delayed:lease residue for task %s", relayRacedTask.ID)
	}

	// 子场景 2（真实 Redis + 真实 Forwarder + 真实 Kafka 夹缝交错）：
	// 1) Worker A 持锁期间，Worker B 抢锁失败并先查 PG 看到 PENDING，随后停顿在 DeferContendedTask 之前；
	// 2) Worker A 放锁，Cancel 接口返回 200 OK 并立下取消标记；
	// 3) 放行滞后的 Worker B 执行 DeferContendedTask，被取消标记原子拒写；
	// 4) 启动真实 DelayedForwarder 跑过唤醒时间窗口，断言延迟池零残留且真实 Kafka 收到 0 条转发消息。
	code2, gapTask := h.createTaskWithDeliverAt(ep.ID, "cancel-vs-lagged-worker-gap", &futureAt)
	if code2 != http.StatusAccepted {
		t.Fatalf("expected 202 for gapTask, got %d", code2)
	}
	workerAToken, acquired, err := h.redisCli.AcquireExecutionLock(ctx, gapTask.ID, 5*time.Second)
	if err != nil || !acquired {
		t.Fatalf("Worker A AcquireExecutionLock failed: acquired=%v err=%v", acquired, err)
	}

	// Worker B 抢锁失败后先查 PG，此时确实读到 PENDING，并在调用 DeferContendedTask 前挂起。
	if _, acquiredB, err := h.redisCli.AcquireExecutionLock(ctx, gapTask.ID, 5*time.Second); err != nil || acquiredB {
		t.Fatalf("expected Worker B lock contention, acquired=%v err=%v", acquiredB, err)
	}
	seenByWorkerB, err := h.pgStore.GetTask(ctx, gapTask.ID)
	if err != nil || seenByWorkerB.Status != domain.TaskStatusPending {
		t.Fatalf("expected Worker B to observe PENDING before cancel, got %+v err=%v", seenByWorkerB, err)
	}

	// Worker A 放锁，上游调用真实 Cancel 接口返回 200 OK（PG 落 CANCELLED + Redis 立取消标记清池）。
	if err := h.redisCli.ReleaseExecutionLock(ctx, gapTask.ID, workerAToken); err != nil {
		t.Fatalf("Worker A ReleaseExecutionLock failed: %v", err)
	}
	cancelCode, cancelBody := h.doJSON(http.MethodPost, "/v1/tasks/"+gapTask.ID.String()+"/cancel", nil, nil)
	if cancelCode != http.StatusOK {
		t.Fatalf("expected cancel 200 OK, got %d: %s", cancelCode, cancelBody)
	}

	// 放行滞后的 Worker B 发起迟写（同时验证滞后 ScheduleDelayed 同样被拒写）。
	const fallbackDelay = 120 * time.Millisecond
	if err := h.redisCli.DeferContendedTask(ctx, gapTask.ID, fallbackDelay); err != nil {
		t.Fatalf("lagged DeferContendedTask failed: %v", err)
	}
	if err := h.redisCli.ScheduleDelayed(ctx, gapTask.ID, h.redisNow().Add(fallbackDelay), fallbackDelay); err != nil {
		t.Fatalf("lagged ScheduleDelayed failed: %v", err)
	}
	if _, exists := h.delayedScore(gapTask.ID); exists {
		t.Fatalf("expected cancel tombstone to reject lagged writes into ferry:delayed for task %s", gapTask.ID)
	}

	// 同池放一条比 fallbackDelay 更晚到期的对照任务，启动真实 DelayedForwarder 后一直消费到对照任务到达 Kafka；
	// 这样可以确凿证明 Forwarder 已扫过 fallbackDelay 且冷启动 Kafka 消费组已完成 Join，绝不是超时空等导致的假通过。
	controlCode, controlTask := h.createTaskWithDeliverAt(ep.ID, "cancel-gap-control-task", &futureAt)
	if controlCode != http.StatusAccepted {
		t.Fatalf("expected 202 for controlTask, got %d", controlCode)
	}
	controlDue := h.redisNow().Add(fallbackDelay + 150*time.Millisecond)
	if err := h.redisCli.ScheduleDelayed(ctx, controlTask.ID, controlDue, 0); err != nil {
		t.Fatalf("ScheduleDelayed(controlTask) failed: %v", err)
	}

	h.forwarderRunner.start()
	pollCtx, cancelPoll := context.WithTimeout(ctx, defaultIntegrationWaitTime)
	defer cancelPoll()
	sawControl := false
	for !sawControl {
		deliveries, pollErr := h.reader.PollReady(pollCtx, 10)
		if pollErr != nil {
			t.Fatalf("PollReady failed waiting for control task %s: %v", controlTask.ID, pollErr)
		}
		for _, d := range deliveries {
			if d.Ack != nil {
				d.Ack()
			}
			if d.TaskID == gapTask.ID {
				t.Fatalf("received spurious Kafka delivery for cancelled task %s", gapTask.ID)
			}
			if d.TaskID == controlTask.ID {
				sawControl = true
			}
		}
	}

	// 同步验证 Worker 撞锁快路径：已取消任务即便遇到锁争用也直接 Ack、不写延迟池、不发 HTTP。
	holdToken, holdAcq, err := h.redisCli.AcquireExecutionLock(ctx, gapTask.ID, 3*time.Second)
	if err != nil || !holdAcq {
		t.Fatalf("re-acquire lock for fast-path check failed: acquired=%v err=%v", holdAcq, err)
	}
	t.Cleanup(func() {
		_ = h.redisCli.ReleaseExecutionLock(context.Background(), gapTask.ID, holdToken)
	})

	var acked bool
	if err := h.worker.ProcessDelivery(ctx, domain.ReadyDelivery{
		TaskID: gapTask.ID,
		Ack:    func() { acked = true },
	}); err != nil {
		t.Fatalf("ProcessDelivery fast path on cancelled contended task failed: %v", err)
	}
	if !acked {
		t.Fatal("expected contended worker to ack cancelled task via PG fast path")
	}
	if _, exists := h.delayedScore(gapTask.ID); exists {
		t.Fatalf("expected zero ferry:delayed residue for task %s", gapTask.ID)
	}
	if h.delayedLeaseExists(gapTask.ID) {
		t.Fatalf("expected zero ferry:delayed:lease residue for task %s", gapTask.ID)
	}
	if downstreamHits.Load() != 0 {
		t.Fatalf("expected zero downstream HTTP hits for cancelled tasks, got %d", downstreamHits.Load())
	}
}

// TestOutboxDualChannelRedisFailureIsolationIntegration 覆盖 Redis 故障下 Outbox 双通道隔离的两层边界：
// 1) 真实 *redis.Client 断开不可用时，立即通道照常直推 Kafka 并物理删除 outbox 行，不被延迟通道阻塞；
// 2) 仅延迟通道故障而 Worker 锁存储正常时，立即任务与跨过当前时间的短延迟任务均端到端投递至 SUCCEEDED。
func TestOutboxDualChannelRedisFailureIsolationIntegration(t *testing.T) {
	t.Run("closed_real_redis_client_publishes_immediate_and_deletes_outbox", func(t *testing.T) {
		h := newFlowHarness(t)
		ctx := context.Background()

		// 使用独立连接并主动 Close 模拟真实 Redis 客户端不可用；此时不启动 Worker，严格遵守 Fail-Safe 约束。
		deadRedis := openIntegrationRedis(t, ctx)
		if err := deadRedis.Close(); err != nil {
			t.Fatalf("close dead redis client failed: %v", err)
		}

		relay, err := scheduler.NewOutboxRelay(zap.NewNop(), h.pgStore, h.producer, deadRedis, scheduler.OutboxRelayConfig{
			BatchSize:       50,
			PollInterval:    80 * time.Millisecond,
			BatchTimeout:    5 * time.Second,
			ErrorBackoffCap: 300 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("NewOutboxRelay(deadRedis) failed: %v", err)
		}
		relayRunner := &bgRunner{t: t, name: "DeadRedisRelay", run: relay.Run, stopTimeout: 5 * time.Second}
		relayRunner.start()
		t.Cleanup(relayRunner.stop)

		ep := h.createEndpoint("dead-redis-ep", "http://127.0.0.1:18081/noop", "", 5000)
		farFuture := time.Now().UTC().Add(10 * time.Minute)
		if code, _ := h.createTaskWithDeliverAt(ep.ID, "dead-redis-delayed", &farFuture); code != http.StatusAccepted {
			t.Fatalf("expected 202 for delayed task, got %d", code)
		}
		immCode, immTask := h.createTaskWithDeliverAt(ep.ID, "dead-redis-immediate", nil)
		if immCode != http.StatusAccepted {
			t.Fatalf("expected 202 for immediate task, got %d", immCode)
		}

		// 验证立即任务已直推至真实 Kafka 就绪流。
		pollCtx, cancelPoll := context.WithTimeout(ctx, defaultIntegrationWaitTime)
		defer cancelPoll()
		var gotTaskID uuid.UUID
		for gotTaskID != immTask.ID {
			deliveries, pollErr := h.reader.PollReady(pollCtx, 10)
			if pollErr != nil {
				t.Fatalf("PollReady failed while waiting for immediate task under redis outage: %v", pollErr)
			}
			for _, d := range deliveries {
				if d.Ack != nil {
					d.Ack()
				}
				if d.TaskID == immTask.ID {
					gotTaskID = d.TaskID
				}
			}
		}

		// 验证该立即任务的 outbox 行已被物理删除，而延迟任务仍在 outbox 中等待恢复。
		remainingImm, err := h.pgStore.ClaimAndDispatchImmediateOutbox(ctx, time.Now().UTC(), 10, func(_ context.Context, _ []uuid.UUID) error {
			return nil
		})
		if err != nil || remainingImm != 0 {
			t.Fatalf("expected immediate outbox physically deleted, got remaining=%d err=%v", remainingImm, err)
		}
	})

	t.Run("injected_delayed_failure_completes_immediate_and_due_takeover_e2e", func(t *testing.T) {
		downstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
		t.Cleanup(downstream.Close)

		h := newFlowHarness(t)

		isolatedRelay, err := scheduler.NewOutboxRelay(
			zap.NewNop(),
			h.pgStore,
			h.producer,
			failingDelayedEnqueuer{err: errors.New("redis delayed pool down")},
			scheduler.OutboxRelayConfig{
				BatchSize:       50,
				PollInterval:    80 * time.Millisecond,
				BatchTimeout:    5 * time.Second,
				ErrorBackoffCap: 300 * time.Millisecond,
			},
		)
		if err != nil {
			t.Fatalf("NewOutboxRelay(failing delayed) failed: %v", err)
		}
		relayRunner := &bgRunner{t: t, name: "IsolatedRelay", run: isolatedRelay.Run, stopTimeout: 5 * time.Second}
		relayRunner.start()
		t.Cleanup(relayRunner.stop)

		h.workerRunner.start()
		ep := h.createEndpoint("outbox-isolation", downstream.URL+"/ok", "", 5000)

		farFuture := time.Now().UTC().Add(10 * time.Minute)
		if code, _ := h.createTaskWithDeliverAt(ep.ID, "stuck-far-delayed", &farFuture); code != http.StatusAccepted {
			t.Fatalf("expected 202 for far delayed task, got %d", code)
		}
		shortFuture := time.Now().UTC().Add(600 * time.Millisecond)
		shortCode, shortTask := h.createTaskWithDeliverAt(ep.ID, "takeover-short-delayed", &shortFuture)
		if shortCode != http.StatusAccepted {
			t.Fatalf("expected 202 for short delayed task, got %d", shortCode)
		}
		immCode, immTask := h.createTaskWithDeliverAt(ep.ID, "immediate-under-redis-fault", nil)
		if immCode != http.StatusAccepted {
			t.Fatalf("expected 202 for immediate task, got %d", immCode)
		}

		waitUntil(t, defaultIntegrationWaitTime, "immediate task succeeds while delayed channel failing", func() bool {
			return h.getTask(immTask.ID).Status == string(domain.TaskStatusSucceeded)
		})
		waitUntil(t, defaultIntegrationWaitTime, "short delayed task taken over by immediate channel after crossing now", func() bool {
			return h.getTask(shortTask.ID).Status == string(domain.TaskStatusSucceeded)
		})
	})
}

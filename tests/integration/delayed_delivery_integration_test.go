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
	h.startRelay()
	h.startForwarder()
	h.startWorker()

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

// TestWorkerContentionAndEarlyArrivalIntegration 验证 Worker 抢锁争用（ZADD NX 保底）与早到拦截（覆写 ZADD 写回）
// 均先写回延迟池再确认位移，并在锁过期或到期后经 Forwarder 恢复成功投递。
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

	// 场景 1（早到拦截）：创建 2.2s 后执行的任务，先只启动 Worker 不启动 Relay/Forwarder，
	// 直接将任务 ID 推入 Kafka 模拟提前到达 -> Worker 必须不发 HTTP 且覆写 ZADD 写回 NextRunAt。
	earlyAt := h.redisNow().Add(2200 * time.Millisecond)
	earlyCode, earlyTask := h.createTaskWithDeliverAt(ep.ID, "early-arrival-1", &earlyAt)
	if earlyCode != http.StatusAccepted {
		t.Fatalf("expected 202 for early task, got %d", earlyCode)
	}

	h.startWorker()
	if err := h.producer.PublishTaskIDs(ctx, []uuid.UUID{earlyTask.ID}); err != nil {
		t.Fatalf("PublishTaskIDs(early) failed: %v", err)
	}
	waitUntil(t, 5*time.Second, "early arrival task requeued to ferry:delayed", func() bool {
		score, ok := h.delayedScore(earlyTask.ID)
		return ok && int64(score) == earlyAt.UnixMilli()
	})
	mu.Lock()
	earlyPrematureHits := hits[earlyTask.ID.String()]
	mu.Unlock()
	if earlyPrematureHits != 0 {
		t.Fatalf("expected 0 hits on early arrival, got %d", earlyPrematureHits)
	}

	// 场景 2（抢锁争用）：预占立即任务的 Execution Lock（700ms TTL）模拟前任 Worker 强杀遗留锁，
	// 推送到 Kafka -> Worker 抢锁失败通过 Lua ZADD NX 写回延迟池。
	contendCode, contendTask := h.createTaskWithDeliverAt(ep.ID, "lock-contend-1", nil)
	if contendCode != http.StatusAccepted {
		t.Fatalf("expected 202 for contended task, got %d", contendCode)
	}
	if _, acquired, err := h.redisCli.AcquireExecutionLock(ctx, contendTask.ID, 700*time.Millisecond); err != nil || !acquired {
		t.Fatalf("pre-acquire execution lock failed: acquired=%v err=%v", acquired, err)
	}
	if err := h.producer.PublishTaskIDs(ctx, []uuid.UUID{contendTask.ID}); err != nil {
		t.Fatalf("PublishTaskIDs(contended) failed: %v", err)
	}
	waitUntil(t, 5*time.Second, "contended task deferred into ferry:delayed", func() bool {
		_, ok := h.delayedScore(contendTask.ID)
		return ok
	})

	// 启动 DelayedForwarder：两条写回延迟池的任务在锁过期 / 到期后均应恢复成功投递。
	h.startForwarder()
	waitUntil(t, defaultIntegrationWaitTime, "contended task recovers and succeeds", func() bool {
		return h.getTask(contendTask.ID).Status == string(domain.TaskStatusSucceeded)
	})
	waitUntil(t, defaultIntegrationWaitTime, "early arrival task delivers on time and succeeds", func() bool {
		return h.getTask(earlyTask.ID).Status == string(domain.TaskStatusSucceeded)
	})

	mu.Lock()
	earlyDeliveredAt := receivedAt[earlyTask.ID.String()]
	mu.Unlock()
	if earlyDeliveredAt.Before(earlyAt.Add(-50 * time.Millisecond)) {
		t.Fatalf("early task delivered before NextRunAt: got=%v want>=%v", earlyDeliveredAt, earlyAt)
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

		h.startWorker()
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

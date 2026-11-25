package integration_test

import (
	"encoding/json/v2"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/awu123-create/ferry/internal/domain"
)

// TestCancelAndLockIntegration 覆盖取消与执行锁互斥链路：
// 1) Worker 在途持锁执行期间调用 POST /cancel 返回 409 Conflict，不破坏在途任务完成；
// 2) 未开始执行前取消成功置为 CANCELLED，随后被 Relay/Worker 消费时跳过且零出站请求。
func TestCancelAndLockIntegration(t *testing.T) {
	h := newFlowHarness(t)

	inFlightEntered := make(chan struct{}, 1)
	releaseInFlight := make(chan struct{})
	var skippedHits atomic.Int32

	downstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/hold":
			select {
			case inFlightEntered <- struct{}{}:
			default:
			}
			<-releaseInFlight
			w.WriteHeader(http.StatusOK)
		case "/should-not-deliver":
			skippedHits.Add(1)
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer downstream.Close()

	// 场景 1：先创建并取消一条待投递任务（此时尚未启动 Relay/Worker），再启动消费验证跳过投递。
	epSkip := h.createEndpoint("cancel-before-delivery", downstream.URL+"/should-not-deliver", "", 5000)
	code1, body1 := h.doJSON(http.MethodPost, "/v1/tasks",
		[]byte(`{"endpoint_id":"`+epSkip.ID.String()+`","payload":{"cancel":"early"}}`),
		map[string]string{"Idempotency-Key": "cancel-early-1"},
	)
	if code1 != http.StatusAccepted {
		t.Fatalf("create early-cancel task expected 202, got %d: %s", code1, body1)
	}
	var earlyTask taskResp
	if err := json.Unmarshal(body1, &earlyTask); err != nil {
		t.Fatalf("decode early task: %v", err)
	}

	cancelCode, cancelBody := h.doJSON(http.MethodPost, "/v1/tasks/"+earlyTask.ID.String()+"/cancel", nil, nil)
	if cancelCode != http.StatusOK {
		t.Fatalf("cancel pending task expected 200, got %d: %s", cancelCode, cancelBody)
	}
	if got := h.getTask(earlyTask.ID); got.Status != string(domain.TaskStatusCancelled) {
		t.Fatalf("expected CANCELLED, got %+v", got)
	}

	// 启动 Relay 与 Worker；该已取消任务在 PG 取消事务内已同步清掉 outbox，即便有在途消息到达 Worker 也会跳过且不发 HTTP。
	h.relayRunner.start()
	h.workerRunner.start()

	// 场景 2：提交一条会阻塞在 /hold 的任务，等 Worker 持锁进入出站请求时调用 Cancel，验证返回 409。
	epHold := h.createEndpoint("cancel-in-flight", downstream.URL+"/hold", "", 10000)
	code2, body2 := h.doJSON(http.MethodPost, "/v1/tasks",
		[]byte(`{"endpoint_id":"`+epHold.ID.String()+`","payload":{"cancel":"in-flight"}}`),
		map[string]string{"Idempotency-Key": "cancel-inflight-1"},
	)
	if code2 != http.StatusAccepted {
		t.Fatalf("create in-flight task expected 202, got %d: %s", code2, body2)
	}
	var inFlightTask taskResp
	if err := json.Unmarshal(body2, &inFlightTask); err != nil {
		t.Fatalf("decode in-flight task: %v", err)
	}

	select {
	case <-inFlightEntered:
	case <-time.After(defaultIntegrationWaitTime):
		close(releaseInFlight)
		t.Fatal("timed out waiting for worker to enter /hold")
	}

	// Worker 正在持锁执行，Cancel 必须抢锁失败并返回 409 Conflict。
	conflictCode, conflictBody := h.doJSON(http.MethodPost, "/v1/tasks/"+inFlightTask.ID.String()+"/cancel", nil, nil)
	if conflictCode != http.StatusConflict {
		close(releaseInFlight)
		t.Fatalf("expected 409 Conflict cancelling in-flight task, got %d: %s", conflictCode, conflictBody)
	}

	// 放行 /hold，等待任务正常落盘 SUCCEEDED。
	close(releaseInFlight)
	waitUntil(t, defaultIntegrationWaitTime, "in-flight task transitions to SUCCEEDED", func() bool {
		return h.getTask(inFlightTask.ID).Status == string(domain.TaskStatusSucceeded)
	})

	// 确认已取消任务从未触发下游 HTTP 请求，且无任何投递尝试记录。
	if skippedHits.Load() != 0 {
		t.Fatalf("expected cancelled task never delivered, got %d hits", skippedHits.Load())
	}
	if attempts := h.listAttempts(earlyTask.ID); len(attempts) != 0 {
		t.Fatalf("expected 0 attempts for cancelled task, got %+v", attempts)
	}
}

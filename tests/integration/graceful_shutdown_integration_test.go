package integration_test

import (
	"context"
	"encoding/json/v2"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/awu123-create/ferry/internal/domain"
)

// TestGracefulShutdownIntegration 覆盖 Worker 优雅停机链路：
// 在途慢 HTTP 请求执行中收到停机取消（SIGTERM 语义）-> Ready() 立即变为 false ->
// 不腰斩在途请求，等待排空写库 SUCCEEDED、释放 Redis 执行锁并完成最终 Offset 提交后干净退出。
func TestGracefulShutdownIntegration(t *testing.T) {
	h := newFlowHarness(t)

	inFlightStarted := make(chan struct{}, 1)
	releaseHTTP := make(chan struct{})

	downstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/slow" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		select {
		case inFlightStarted <- struct{}{}:
		default:
		}
		<-releaseHTTP
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("drained"))
	}))
	defer downstream.Close()

	h.relayRunner.start()
	h.workerRunner.start()

	ep := h.createEndpoint("slow-shutdown", downstream.URL+"/slow", "", 10000)
	code, body := h.doJSON(http.MethodPost, "/v1/tasks",
		[]byte(`{"endpoint_id":"`+ep.ID.String()+`","payload":{"shutdown":"graceful"}}`),
		map[string]string{"Idempotency-Key": "graceful-1"},
	)
	if code != http.StatusAccepted {
		t.Fatalf("create task expected 202, got %d: %s", code, body)
	}
	var task taskResp
	if err := json.Unmarshal(body, &task); err != nil {
		t.Fatalf("decode task: %v", err)
	}

	select {
	case <-inFlightStarted:
	case <-time.After(defaultIntegrationWaitTime):
		close(releaseHTTP)
		t.Fatal("timed out waiting for worker to start slow HTTP delivery")
	}

	if !h.worker.Ready() {
		close(releaseHTTP)
		t.Fatal("expected worker.Ready() == true before shutdown")
	}

	// 触发停机取消（模拟 SIGTERM），确认就绪状态立即摘除且不会中断在途 HTTP。
	workerDone := h.workerRunner.triggerStop()
	waitUntil(t, 2*time.Second, "worker.Ready() flips to false immediately", func() bool {
		return !h.worker.Ready()
	})

	// 放行在途请求，确认排空完成且 Worker.Run 返回 nil。
	close(releaseHTTP)
	select {
	case err := <-workerDone:
		if err != nil {
			t.Fatalf("Worker.Run returned error on graceful shutdown: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for Worker graceful drain")
	}

	got := h.getTask(task.ID)
	if got.Status != string(domain.TaskStatusSucceeded) || got.Attempt != 1 {
		t.Fatalf("expected drained task SUCCEEDED attempt=1, got %+v", got)
	}
	attempts := h.listAttempts(task.ID)
	if len(attempts) != 1 || attempts[0].HTTPStatus != http.StatusOK {
		t.Fatalf("unexpected attempts after drain: %+v", attempts)
	}

	// 确认停机前已释放 Redis Execution Lock。
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	token, acquired, err := h.redisCli.AcquireExecutionLock(ctx, task.ID, time.Second)
	if err != nil || !acquired {
		t.Fatalf("expected execution lock released before worker exit, acquired=%v err=%v", acquired, err)
	}
	_ = h.redisCli.ReleaseExecutionLock(ctx, task.ID, token)
}

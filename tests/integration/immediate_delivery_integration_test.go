package integration_test

import (
	"bytes"
	"encoding/json/v2"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"

	"github.com/awu123-create/ferry/internal/domain"
)

// TestImmediateDeliveryIntegration 覆盖 Milestone 1 主投递链路：
// 注册 Endpoint -> 并发幂等创建 Task -> OutboxRelay 搬运至 Kafka -> Worker 验签直投 ->
// 200 迁移 SUCCEEDED，400 与 302（不跟随重定向）分别直接迁移 DEAD_LETTER。
func TestImmediateDeliveryIntegration(t *testing.T) {
	h := newFlowHarness(t)
	const signingSecret = "whsec_integration_secret"

	type receivedDelivery struct {
		body      []byte
		taskID    string
		attempt   string
		timestamp string
		signature string
		customHdr string
	}

	var mu sync.Mutex
	var received []receivedDelivery
	var redirectTargetHits atomic.Int32

	downstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ok":
			body, err := io.ReadAll(r.Body)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			mu.Lock()
			received = append(received, receivedDelivery{
				body:      body,
				taskID:    r.Header.Get("X-Ferry-Task-ID"),
				attempt:   r.Header.Get("X-Ferry-Attempt"),
				timestamp: r.Header.Get("X-Ferry-Timestamp"),
				signature: r.Header.Get("X-Ferry-Signature"),
				customHdr: r.Header.Get("X-Order-Source"),
			})
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
		case "/bad":
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte("bad contract"))
		case "/redirect":
			http.Redirect(w, r, "/redirect-target", http.StatusFound)
		case "/redirect-target":
			redirectTargetHits.Add(1)
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer downstream.Close()

	// 先启动 Relay 与 Worker，验证整条链路实时推进。
	h.relayRunner.start()
	h.workerRunner.start()

	epOK := h.createEndpoint("ok-endpoint", downstream.URL+"/ok", signingSecret, 5000)
	if !epOK.HasSigningSecret {
		t.Fatal("expected has_signing_secret=true")
	}
	epBad := h.createEndpoint("bad-endpoint", downstream.URL+"/bad", "", 5000)
	epRedir := h.createEndpoint("redirect-endpoint", downstream.URL+"/redirect", "", 5000)

	// 1. 并发 6 次相同 Idempotency-Key 创建立即任务（保留非规范空白符验证 BYTEA 原始字节签名）。
	rawPayload := `{"z":  42,   "a": "paid"}`
	reqBody := []byte(`{"endpoint_id":"` + epOK.ID.String() + `","payload":` + rawPayload + `,"headers":{"X-Order-Source":"checkout"}}`)

	const callers = 6
	startCh := make(chan struct{})
	statuses := make([]int, callers)
	ids := make([]uuid.UUID, callers)

	var wg sync.WaitGroup
	for i := range callers {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-startCh
			code, respBody := h.doJSON(http.MethodPost, "/v1/tasks", reqBody, map[string]string{
				"Idempotency-Key": "flow-idem-1",
			})
			statuses[idx] = code
			var tr taskResp
			if err := json.Unmarshal(respBody, &tr); err == nil {
				ids[idx] = tr.ID
			}
		}(i)
	}
	close(startCh)
	wg.Wait()

	var acceptedCount, okCount int
	taskOKID := ids[0]
	for i := range callers {
		switch statuses[i] {
		case http.StatusAccepted:
			acceptedCount++
		case http.StatusOK:
			okCount++
		default:
			t.Fatalf("caller %d got unexpected status %d", i, statuses[i])
		}
		if ids[i] == uuid.Nil || ids[i] != taskOKID {
			t.Fatalf("expected all callers receive same task id %s, got %s", taskOKID, ids[i])
		}
	}
	if acceptedCount != 1 || okCount != callers-1 {
		t.Fatalf("expected 1x202 and %dx200, got 202=%d 200=%d", callers-1, acceptedCount, okCount)
	}

	// 等待链路完成投递并落盘 SUCCEEDED。
	waitUntil(t, defaultIntegrationWaitTime, "ok task transitions to SUCCEEDED", func() bool {
		return h.getTask(taskOKID).Status == string(domain.TaskStatusSucceeded)
	})

	finalOK := h.getTask(taskOKID)
	if finalOK.Attempt != 1 || finalOK.PayloadSHA256 != domain.ComputePayloadSHA256([]byte(rawPayload)) {
		t.Fatalf("unexpected final ok task: %+v", finalOK)
	}
	attemptsOK := h.listAttempts(taskOKID)
	if len(attemptsOK) != 1 || attemptsOK[0].HTTPStatus != http.StatusOK || attemptsOK[0].AttemptNo != 1 {
		t.Fatalf("unexpected attempts for ok task: %+v", attemptsOK)
	}

	mu.Lock()
	if len(received) != 1 {
		mu.Unlock()
		t.Fatalf("expected downstream /ok hit once, got %d", len(received))
	}
	got := received[0]
	mu.Unlock()

	if !bytes.Equal(got.body, []byte(rawPayload)) {
		t.Fatalf("expected exact raw payload bytes %q, got %q", rawPayload, got.body)
	}
	if got.taskID != taskOKID.String() || got.attempt != "1" || got.customHdr != "checkout" {
		t.Fatalf("unexpected delivery headers: %+v", got)
	}
	ts, err := strconv.ParseInt(got.timestamp, 10, 64)
	if err != nil {
		t.Fatalf("parse X-Ferry-Timestamp %q: %v", got.timestamp, err)
	}
	if !domain.VerifyPayloadSignature(signingSecret, got.signature, ts, got.body) {
		t.Fatalf("HMAC signature verification failed: sig=%q", got.signature)
	}

	// 2. 不可重试 400 任务直接进入 DEAD_LETTER(NON_RETRYABLE_4XX)。
	badCode, badBody := h.doJSON(http.MethodPost, "/v1/tasks",
		[]byte(`{"endpoint_id":"`+epBad.ID.String()+`","payload":{"k":1}}`),
		map[string]string{"Idempotency-Key": "flow-bad-400"},
	)
	if badCode != http.StatusAccepted {
		t.Fatalf("create 400 task expected 202, got %d: %s", badCode, badBody)
	}
	var taskBad taskResp
	if err := json.Unmarshal(badBody, &taskBad); err != nil {
		t.Fatalf("decode 400 task: %v", err)
	}
	waitUntil(t, defaultIntegrationWaitTime, "400 task transitions to DEAD_LETTER", func() bool {
		return h.getTask(taskBad.ID).Status == string(domain.TaskStatusDeadLetter)
	})
	if got := h.getTask(taskBad.ID); got.LastError != domain.DeadLetterReasonNonRetryable4xx || got.Attempt != 1 {
		t.Fatalf("unexpected 400 dead-letter state: %+v", got)
	}

	// 3. 下游 302 重定向不跟随，直接进入 DEAD_LETTER(UNEXPECTED_REDIRECT)，且目标页零命中。
	redirCode, redirBody := h.doJSON(http.MethodPost, "/v1/tasks",
		[]byte(`{"endpoint_id":"`+epRedir.ID.String()+`","payload":{"k":2}}`),
		map[string]string{"Idempotency-Key": "flow-redir-302"},
	)
	if redirCode != http.StatusAccepted {
		t.Fatalf("create 302 task expected 202, got %d: %s", redirCode, redirBody)
	}
	var taskRedir taskResp
	if err := json.Unmarshal(redirBody, &taskRedir); err != nil {
		t.Fatalf("decode 302 task: %v", err)
	}
	waitUntil(t, defaultIntegrationWaitTime, "302 task transitions to DEAD_LETTER", func() bool {
		return h.getTask(taskRedir.ID).Status == string(domain.TaskStatusDeadLetter)
	})
	if got := h.getTask(taskRedir.ID); got.LastError != domain.DeadLetterReasonUnexpectedRedirect || got.Attempt != 1 {
		t.Fatalf("unexpected 302 dead-letter state: %+v", got)
	}
	if redirectTargetHits.Load() != 0 {
		t.Fatalf("expected redirect target never called, got %d hits", redirectTargetHits.Load())
	}
}

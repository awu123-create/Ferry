package pg_test

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/awu123-create/ferry/internal/domain"
	"github.com/awu123-create/ferry/internal/infra/pg"
)

const defaultTestPGDSN = "postgres://ferry:ferry@localhost:5432/ferry?sslmode=disable"

func openIsolatedStore(t *testing.T) *pg.Store {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping real PostgreSQL test in -short mode")
	}

	baseDSN := strings.TrimSpace(os.Getenv("FERRY_TEST_PG_DSN"))
	if baseDSN == "" {
		baseDSN = defaultTestPGDSN
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	adminDB, err := sql.Open("pgx", baseDSN)
	if err != nil {
		t.Fatalf("open admin postgres connection: %v", err)
	}
	if err := adminDB.PingContext(ctx); err != nil {
		_ = adminDB.Close()
		t.Fatalf("connect test postgres (run `make up` first): %v", err)
	}

	dbName := fmt.Sprintf("ferry_pg_%d_%d", os.Getpid(), time.Now().UnixNano())
	if _, err := adminDB.ExecContext(ctx, "CREATE DATABASE "+dbName); err != nil {
		_ = adminDB.Close()
		t.Fatalf("create isolated test database %s: %v", dbName, err)
	}

	isolatedDSN, err := replaceDatabaseName(baseDSN, dbName)
	if err != nil {
		dropDatabase(adminDB, dbName)
		_ = adminDB.Close()
		t.Fatalf("build isolated database DSN: %v", err)
	}

	store, err := pg.Open(ctx, isolatedDSN, nil)
	if err != nil {
		dropDatabase(adminDB, dbName)
		_ = adminDB.Close()
		t.Fatalf("open isolated pg.Store: %v", err)
	}
	if err := store.RunMigrations(ctx); err != nil {
		_ = store.Close()
		dropDatabase(adminDB, dbName)
		_ = adminDB.Close()
		t.Fatalf("run migrations on %s: %v", dbName, err)
	}

	t.Cleanup(func() {
		_ = store.Close()
		dropDatabase(adminDB, dbName)
		_ = adminDB.Close()
	})
	return store
}

func replaceDatabaseName(rawDSN, dbName string) (string, error) {
	parsed, err := url.Parse(rawDSN)
	if err != nil {
		return "", err
	}
	parsed.Path = "/" + dbName
	return parsed.String(), nil
}

func dropDatabase(adminDB *sql.DB, dbName string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, _ = adminDB.ExecContext(ctx, "DROP DATABASE IF EXISTS "+dbName+" WITH (FORCE)")
}

func mustCreateTestEndpoint(t *testing.T, store *pg.Store, name string) domain.Endpoint {
	t.Helper()
	ep, err := domain.NewEndpoint(domain.NewEndpointParams{
		Name:          name,
		TargetURL:     "http://localhost:8081/webhook",
		SigningSecret: "whsec_pg_test",
	}, time.Now().UTC())
	if err != nil {
		t.Fatalf("NewEndpoint failed: %v", err)
	}
	if err := store.CreateEndpoint(context.Background(), ep); err != nil {
		t.Fatalf("CreateEndpoint failed: %v", err)
	}
	return ep
}

func TestStore_CreateTaskIdempotentConcurrencyAndRawByteFidelity(t *testing.T) {
	store := openIsolatedStore(t)
	ctx := context.Background()
	ep := mustCreateTestEndpoint(t, store, "payments")

	rawPayload := []byte(`{"z":  1,   "a": "raw-bytes"}`)
	headers := map[string]string{"X-Trace-ID": "trace-001"}

	const workers = 8
	startCh := make(chan struct{})
	results := make([]domain.CreateTaskResult, workers)
	errs := make([]error, workers)

	var wg sync.WaitGroup
	for i := range workers {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			task, err := domain.NewTask(domain.NewTaskParams{
				Endpoint:       ep,
				IdempotencyKey: "idem-concurrent-1",
				RawPayload:     rawPayload,
				Headers:        headers,
				MaxAttempts:    1,
				DefaultWindow:  time.Hour,
				Now:            time.Now().UTC(),
			})
			if err != nil {
				errs[idx] = err
				return
			}
			<-startCh
			results[idx], errs[idx] = store.CreateTaskIdempotent(ctx, task)
		}(i)
	}
	close(startCh)
	wg.Wait()

	var createdCount int
	var firstID uuid.UUID
	for i := range workers {
		if errs[i] != nil {
			t.Fatalf("worker %d CreateTaskIdempotent failed: %v", i, errs[i])
		}
		if results[i].Created {
			createdCount++
		}
		if firstID == uuid.Nil {
			firstID = results[i].Task.ID
		} else if results[i].Task.ID != firstID {
			t.Fatalf("expected all concurrent callers to observe task %s, got %s", firstID, results[i].Task.ID)
		}
	}
	if createdCount != 1 {
		t.Fatalf("expected exactly 1 created task under concurrency, got %d", createdCount)
	}

	// 验证 PostgreSQL BYTEA 原样保存原始载荷字节，不重排 JSON 键和空白符。
	loaded, err := store.GetTask(ctx, firstID)
	if err != nil {
		t.Fatalf("GetTask failed: %v", err)
	}
	if !bytes.Equal(loaded.Payload, rawPayload) {
		t.Fatalf("raw payload corrupted in PG: got %q want %q", loaded.Payload, rawPayload)
	}
	if loaded.Headers["X-Trace-ID"] != "trace-001" {
		t.Fatalf("unexpected persisted headers: %+v", loaded.Headers)
	}

	// 同键异载荷与同键异 header 均应返回 ErrIdempotencyConflict。
	diffPayloadTask, err := domain.NewTask(domain.NewTaskParams{
		Endpoint:       ep,
		IdempotencyKey: "idem-concurrent-1",
		RawPayload:     []byte(`{"z":1,"a":"different"}`),
		Headers:        headers,
		MaxAttempts:    1,
		DefaultWindow:  time.Hour,
		Now:            time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("NewTask failed: %v", err)
	}
	if _, err := store.CreateTaskIdempotent(ctx, diffPayloadTask); !errors.Is(err, domain.ErrIdempotencyConflict) {
		t.Fatalf("expected ErrIdempotencyConflict for different payload, got %v", err)
	}

	diffHeaderTask, err := domain.NewTask(domain.NewTaskParams{
		Endpoint:       ep,
		IdempotencyKey: "idem-concurrent-1",
		RawPayload:     rawPayload,
		Headers:        map[string]string{"X-Trace-ID": "trace-002"},
		MaxAttempts:    1,
		DefaultWindow:  time.Hour,
		Now:            time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("NewTask failed: %v", err)
	}
	if _, err := store.CreateTaskIdempotent(ctx, diffHeaderTask); !errors.Is(err, domain.ErrIdempotencyConflict) {
		t.Fatalf("expected ErrIdempotencyConflict for different headers, got %v", err)
	}
}

func TestStore_SaveDeliveryOutcomeCASAndAuditPreservation(t *testing.T) {
	store := openIsolatedStore(t)
	ctx := context.Background()
	ep := mustCreateTestEndpoint(t, store, "orders")

	// 场景 1：正常投递成功，单事务落盘 SUCCEEDED 与 task_attempts 审计行。
	taskOK, err := domain.NewTask(domain.NewTaskParams{
		Endpoint:       ep,
		IdempotencyKey: "cas-ok",
		RawPayload:     []byte(`{"ok":true}`),
		MaxAttempts:    1,
		DefaultWindow:  time.Hour,
		Now:            time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("NewTask failed: %v", err)
	}
	if _, err := store.CreateTaskIdempotent(ctx, taskOK); err != nil {
		t.Fatalf("CreateTaskIdempotent failed: %v", err)
	}

	transitionOK, err := taskOK.RecordOutcome(domain.DeliveryResult{
		HTTPStatus:      200,
		DurationMs:      15,
		ErrorKind:       domain.AttemptErrorNone,
		ResponseSnippet: "accepted",
	}, time.Now().UTC())
	if err != nil {
		t.Fatalf("RecordOutcome failed: %v", err)
	}
	applied, err := store.SaveDeliveryOutcome(ctx, 0, 0, taskOK, transitionOK.Attempt)
	if err != nil || !applied {
		t.Fatalf("expected CAS applied=true err=nil, got applied=%v err=%v", applied, err)
	}

	gotOK, err := store.GetTask(ctx, taskOK.ID)
	if err != nil {
		t.Fatalf("GetTask failed: %v", err)
	}
	if gotOK.Status != domain.TaskStatusSucceeded || gotOK.Attempt != 1 || gotOK.CompletedAt == nil {
		t.Fatalf("unexpected persisted task after success: %+v", gotOK)
	}
	attemptsOK, err := store.ListDeliveryAttempts(ctx, taskOK.ID)
	if err != nil || len(attemptsOK) != 1 || attemptsOK[0].HTTPStatus != 200 || attemptsOK[0].ResponseSnippet != "accepted" {
		t.Fatalf("unexpected persisted attempts: %+v err=%v", attemptsOK, err)
	}

	// 场景 2：并发取消先将任务置为 CANCELLED，随后 Worker 结算 CAS 未命中时不得覆盖终态，但必须保留真实投递审计行。
	taskRace, err := domain.NewTask(domain.NewTaskParams{
		Endpoint:       ep,
		IdempotencyKey: "cas-race",
		RawPayload:     []byte(`{"race":1}`),
		MaxAttempts:    1,
		DefaultWindow:  time.Hour,
		Now:            time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("NewTask failed: %v", err)
	}
	if _, err := store.CreateTaskIdempotent(ctx, taskRace); err != nil {
		t.Fatalf("CreateTaskIdempotent failed: %v", err)
	}

	cancelled, err := store.CancelTask(ctx, taskRace.ID, time.Now().UTC())
	if err != nil || cancelled.Status != domain.TaskStatusCancelled {
		t.Fatalf("expected CANCELLED, got %+v err=%v", cancelled, err)
	}

	// 再次取消终态任务应返回 ErrTaskNotCancellable。
	if _, err := store.CancelTask(ctx, taskRace.ID, time.Now().UTC()); !errors.Is(err, domain.ErrTaskNotCancellable) {
		t.Fatalf("expected ErrTaskNotCancellable on second cancel, got %v", err)
	}

	transitionRace, err := taskRace.RecordOutcome(domain.DeliveryResult{
		HTTPStatus:      200,
		DurationMs:      30,
		ErrorKind:       domain.AttemptErrorNone,
		ResponseSnippet: "late-200",
	}, time.Now().UTC())
	if err != nil {
		t.Fatalf("RecordOutcome failed: %v", err)
	}
	appliedRace, err := store.SaveDeliveryOutcome(ctx, 0, 0, taskRace, transitionRace.Attempt)
	if err != nil {
		t.Fatalf("SaveDeliveryOutcome failed: %v", err)
	}
	if appliedRace {
		t.Fatal("expected CAS applied=false after task was already CANCELLED")
	}

	gotRace, err := store.GetTask(ctx, taskRace.ID)
	if err != nil {
		t.Fatalf("GetTask failed: %v", err)
	}
	if gotRace.Status != domain.TaskStatusCancelled {
		t.Fatalf("expected task status preserved as CANCELLED, got %s", gotRace.Status)
	}
	attemptsRace, err := store.ListDeliveryAttempts(ctx, taskRace.ID)
	if err != nil || len(attemptsRace) != 1 || attemptsRace[0].ResponseSnippet != "late-200" {
		t.Fatalf("expected audit attempt preserved on CAS miss, got %+v err=%v", attemptsRace, err)
	}
}

func TestStore_OutboxSkipLockedRollbackAndListenNotify(t *testing.T) {
	store := openIsolatedStore(t)
	ctx := context.Background()
	ep := mustCreateTestEndpoint(t, store, "relay")

	// 先建立专用 LISTEN 连接，避免首次 LISTEN 初始化与插入触发器竞争。
	initCtx, cancelInit := context.WithTimeout(ctx, 100*time.Millisecond)
	_ = store.WaitForOutboxNotification(initCtx)
	cancelInit()

	notifyErrCh := make(chan error, 1)
	go func() {
		waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		notifyErrCh <- store.WaitForOutboxNotification(waitCtx)
	}()

	// 插入 6 条立即任务（触发 outbox_notify_trg 并生成 6 行 outbox）。
	expectedIDs := make(map[uuid.UUID]struct{}, 6)
	for i := range 6 {
		task, err := domain.NewTask(domain.NewTaskParams{
			Endpoint:       ep,
			IdempotencyKey: fmt.Sprintf("outbox-%d", i),
			RawPayload:     []byte(`{"n":1}`),
			MaxAttempts:    1,
			DefaultWindow:  time.Hour,
			Now:            time.Now().UTC(),
		})
		if err != nil {
			t.Fatalf("NewTask failed: %v", err)
		}
		if _, err := store.CreateTaskIdempotent(ctx, task); err != nil {
			t.Fatalf("CreateTaskIdempotent failed: %v", err)
		}
		expectedIDs[task.ID] = struct{}{}
	}

	if err := <-notifyErrCh; err != nil {
		t.Fatalf("expected LISTEN/NOTIFY wakeup from outbox trigger, got %v", err)
	}

	// 验证回调返回错误时事务回滚，outbox 行不被删除。
	boom := errors.New("kafka down")
	if _, err := store.ClaimAndDispatchImmediateOutbox(ctx, time.Now().UTC(), 2, func(_ context.Context, _ []uuid.UUID) error {
		return boom
	}); !errors.Is(err, boom) {
		t.Fatalf("expected callback error propagated, got %v", err)
	}

	// 两个并发 Relay 各抢占 3 条，通过同步栅栏确保两事务同时持锁，验证 SKIP LOCKED 零重叠。
	bothEntered := make(chan struct{}, 2)
	releaseBoth := make(chan struct{})
	var mu sync.Mutex
	claimed := make(map[uuid.UUID]int)

	var wg sync.WaitGroup
	claimErrs := make([]error, 2)
	claimCounts := make([]int, 2)
	for i := range 2 {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			n, err := store.ClaimAndDispatchImmediateOutbox(ctx, time.Now().UTC(), 3, func(_ context.Context, taskIDs []uuid.UUID) error {
				bothEntered <- struct{}{}
				<-releaseBoth
				mu.Lock()
				for _, id := range taskIDs {
					claimed[id]++
				}
				mu.Unlock()
				return nil
			})
			claimCounts[idx] = n
			claimErrs[idx] = err
		}(i)
	}

	for range 2 {
		select {
		case <-bothEntered:
		case <-time.After(3 * time.Second):
			close(releaseBoth)
			t.Fatal("timed out waiting for both SKIP LOCKED transactions to hold locks concurrently")
		}
	}
	close(releaseBoth)
	wg.Wait()

	for i := range 2 {
		if claimErrs[i] != nil || claimCounts[i] != 3 {
			t.Fatalf("worker %d claim count=%d err=%v", i, claimCounts[i], claimErrs[i])
		}
	}
	if len(claimed) != len(expectedIDs) {
		t.Fatalf("expected %d unique claimed tasks, got %d", len(expectedIDs), len(claimed))
	}
	for id, count := range claimed {
		if count != 1 {
			t.Fatalf("task %s claimed %d times under SKIP LOCKED", id, count)
		}
	}

	// 再次抢占应返回 0，确认已派发行被物理删除。
	remaining, err := store.ClaimAndDispatchImmediateOutbox(ctx, time.Now().UTC(), 10, func(_ context.Context, _ []uuid.UUID) error {
		t.Fatal("dispatch should not be called when outbox is empty")
		return nil
	})
	if err != nil || remaining != 0 {
		t.Fatalf("expected empty outbox after delete, got remaining=%d err=%v", remaining, err)
	}
}

func TestStore_UpdateEndpointOptimisticLock(t *testing.T) {
	store := openIsolatedStore(t)
	ctx := context.Background()
	ep := mustCreateTestEndpoint(t, store, "billing")

	ep1, err := store.GetEndpoint(ctx, ep.ID)
	if err != nil {
		t.Fatalf("GetEndpoint 1 failed: %v", err)
	}
	ep2, err := store.GetEndpoint(ctx, ep.ID)
	if err != nil {
		t.Fatalf("GetEndpoint 2 failed: %v", err)
	}

	expected1 := ep1.UpdatedAt
	disabled := false
	if err := ep1.ApplyUpdate(domain.UpdateEndpointParams{Enabled: &disabled}, time.Now().UTC()); err != nil {
		t.Fatalf("ApplyUpdate 1 failed: %v", err)
	}
	if err := store.UpdateEndpoint(ctx, ep1, expected1); err != nil {
		t.Fatalf("UpdateEndpoint 1 failed: %v", err)
	}

	// 持有旧 updated_at 的并发更新应返回 ErrEndpointConflict，不得覆盖禁用状态。
	expected2 := ep2.UpdatedAt
	qps := 120.0
	if err := ep2.ApplyUpdate(domain.UpdateEndpointParams{RateLimitQPS: &qps}, time.Now().UTC()); err != nil {
		t.Fatalf("ApplyUpdate 2 failed: %v", err)
	}
	if err := store.UpdateEndpoint(ctx, ep2, expected2); !errors.Is(err, domain.ErrEndpointConflict) {
		t.Fatalf("expected ErrEndpointConflict on stale updated_at, got %v", err)
	}

	latest, err := store.GetEndpoint(ctx, ep.ID)
	if err != nil {
		t.Fatalf("GetEndpoint latest failed: %v", err)
	}
	if latest.Enabled || latest.RateLimitQPS == 120.0 {
		t.Fatalf("expected first update preserved and stale update rejected, got %+v", latest)
	}

	// 不存在的 Endpoint 应返回 ErrEndpointNotFound。
	missing := ep1
	missing.ID = uuid.New()
	if err := store.UpdateEndpoint(ctx, missing, time.Now().UTC()); !errors.Is(err, domain.ErrEndpointNotFound) {
		t.Fatalf("expected ErrEndpointNotFound for missing endpoint, got %v", err)
	}
}

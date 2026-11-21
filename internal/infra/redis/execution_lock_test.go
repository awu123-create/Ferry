package redis_test

import (
	"context"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/awu123-create/ferry/internal/infra/redis"
)

const (
	defaultTestRedisAddr = "localhost:6379"
	defaultTestRedisDB   = 15
)

func openTestRedis(t *testing.T) *redis.Client {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping real Redis test in -short mode")
	}

	addr := strings.TrimSpace(os.Getenv("FERRY_TEST_REDIS_ADDR"))
	if addr == "" {
		addr = defaultTestRedisAddr
	}
	password := os.Getenv("FERRY_TEST_REDIS_PASSWORD")
	db := defaultTestRedisDB
	if rawDB := strings.TrimSpace(os.Getenv("FERRY_TEST_REDIS_DB")); rawDB != "" {
		parsed, err := strconv.Atoi(rawDB)
		if err != nil {
			t.Fatalf("invalid FERRY_TEST_REDIS_DB %q: %v", rawDB, err)
		}
		db = parsed
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, err := redis.New(ctx, addr, password, db)
	if err != nil {
		t.Fatalf("connect test redis %s db=%d (run `make up` first): %v", addr, db, err)
	}
	t.Cleanup(func() {
		_ = client.Close()
	})
	return client
}

func TestExecutionLock_AcquireMutexExtendAndOwnerRelease(t *testing.T) {
	client := openTestRedis(t)
	ctx := context.Background()
	taskID := uuid.New()

	token1, acquired, err := client.AcquireExecutionLock(ctx, taskID, 2*time.Second)
	if err != nil || !acquired || token1 == "" {
		t.Fatalf("expected first acquire to succeed, token=%q acquired=%v err=%v", token1, acquired, err)
	}
	t.Cleanup(func() {
		_ = client.ReleaseExecutionLock(context.Background(), taskID, token1)
	})

	// 锁被持有期间，第二方抢锁必须返回 acquired=false。
	if token2, ok, err := client.AcquireExecutionLock(ctx, taskID, 2*time.Second); err != nil || ok || token2 != "" {
		t.Fatalf("expected mutex rejection while locked, token=%q acquired=%v err=%v", token2, ok, err)
	}

	// 错误 token 无法续期，正确 token 续期成功。
	if extended, err := client.ExtendExecutionLock(ctx, taskID, "wrong-token", 5*time.Second); err != nil || extended {
		t.Fatalf("expected wrong-token extend=false, got extended=%v err=%v", extended, err)
	}
	if extended, err := client.ExtendExecutionLock(ctx, taskID, token1, 5*time.Second); err != nil || !extended {
		t.Fatalf("expected owner extend=true, got extended=%v err=%v", extended, err)
	}

	// 错误 token 释放不得误删持有人锁。
	if err := client.ReleaseExecutionLock(ctx, taskID, "wrong-token"); err != nil {
		t.Fatalf("ReleaseExecutionLock with wrong token returned error: %v", err)
	}
	if _, ok, err := client.AcquireExecutionLock(ctx, taskID, 2*time.Second); err != nil || ok {
		t.Fatalf("lock was erroneously deleted by non-owner; acquired=%v err=%v", ok, err)
	}

	// 持有人正确释放后，新调用方可立刻抢到锁。
	if err := client.ReleaseExecutionLock(ctx, taskID, token1); err != nil {
		t.Fatalf("owner ReleaseExecutionLock failed: %v", err)
	}
	token3, ok, err := client.AcquireExecutionLock(ctx, taskID, 2*time.Second)
	if err != nil || !ok || token3 == "" {
		t.Fatalf("expected acquire after release to succeed, token=%q acquired=%v err=%v", token3, ok, err)
	}
	_ = client.ReleaseExecutionLock(ctx, taskID, token3)
}

func TestExecutionLock_ExpiryAllowsTakeoverAndRejectsStaleOwner(t *testing.T) {
	client := openTestRedis(t)
	ctx := context.Background()
	taskID := uuid.New()

	token1, acquired, err := client.AcquireExecutionLock(ctx, taskID, 150*time.Millisecond)
	if err != nil || !acquired {
		t.Fatalf("first acquire failed: acquired=%v err=%v", acquired, err)
	}

	time.Sleep(250 * time.Millisecond)

	// 锁自然过期后，旧持有人续期必须失败。
	if extended, err := client.ExtendExecutionLock(ctx, taskID, token1, 2*time.Second); err != nil || extended {
		t.Fatalf("expected expired lock extend=false, got extended=%v err=%v", extended, err)
	}

	// 新持有人接管后，旧持有人调用释放不得误删新持有人锁。
	token2, acquired, err := client.AcquireExecutionLock(ctx, taskID, 3*time.Second)
	if err != nil || !acquired {
		t.Fatalf("takeover acquire failed: acquired=%v err=%v", acquired, err)
	}
	t.Cleanup(func() {
		_ = client.ReleaseExecutionLock(context.Background(), taskID, token2)
	})

	if err := client.ReleaseExecutionLock(ctx, taskID, token1); err != nil {
		t.Fatalf("stale release returned error: %v", err)
	}
	if _, ok, err := client.AcquireExecutionLock(ctx, taskID, time.Second); err != nil || ok {
		t.Fatalf("stale token deleted takeover owner lock; acquired=%v err=%v", ok, err)
	}
}

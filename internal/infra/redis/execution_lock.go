package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
)

var (
	extendLockScript = goredis.NewScript(`
		if redis.call("GET", KEYS[1]) == ARGV[1] then
			return redis.call("PEXPIRE", KEYS[1], ARGV[2])
		else
			return 0
		end
	`)
	releaseLockScript = goredis.NewScript(`
		if redis.call("GET", KEYS[1]) == ARGV[1] then
			return redis.call("DEL", KEYS[1])
		else
			return 0
		end
	`)
)

// AcquireExecutionLock 尝试为指定 taskID 获取短时排他执行互斥锁（Execution Lock），
// Key 格式为 ferry:lock:task:<task_id>，抢锁成功返回唯一持有人 token（ADR-0008 §3）。
func (c *Client) AcquireExecutionLock(ctx context.Context, taskID uuid.UUID, ttl time.Duration) (string, bool, error) {
	if ttl <= 0 {
		return "", false, fmt.Errorf("execution lock ttl must be > 0")
	}
	if taskID == uuid.Nil {
		return "", false, fmt.Errorf("execution lock task id must not be empty")
	}

	id, err := uuid.NewV7()
	if err != nil {
		return "", false, fmt.Errorf("generate execution lock token: %w", err)
	}
	token := id.String()

	acquired, err := c.rdb.SetNX(ctx, executionLockKey(taskID), token, ttl).Result()
	if err != nil {
		return "", false, fmt.Errorf("acquire execution lock: %w", err)
	}
	if !acquired {
		return "", false, nil
	}
	return token, true, nil
}

// ExtendExecutionLock 仅当 Redis 中锁的持有人仍为 token 时原子续期（PEXPIRE）；
// 若锁已过期或被其他持有人接管则返回 (false, nil)。
func (c *Client) ExtendExecutionLock(ctx context.Context, taskID uuid.UUID, token string, ttl time.Duration) (bool, error) {
	if taskID == uuid.Nil {
		return false, fmt.Errorf("execution lock task id must not be empty")
	}
	if token == "" {
		return false, fmt.Errorf("execution lock token must not be empty")
	}
	if ttl <= 0 {
		return false, fmt.Errorf("execution lock ttl must be > 0")
	}

	ttlMs := ttl.Milliseconds()
	if ttlMs <= 0 {
		ttlMs = 1
	}

	res, err := extendLockScript.Run(ctx, c.rdb, []string{executionLockKey(taskID)}, token, ttlMs).Int()
	if err != nil {
		return false, fmt.Errorf("extend execution lock: %w", err)
	}
	return res == 1, nil
}

func executionLockKey(taskID uuid.UUID) string {
	return "ferry:lock:task:" + taskID.String()
}

// ReleaseExecutionLock 仅当 Redis 中存储的值等于 token 时才通过 Lua 原子释放 Execution Lock，
// 防止锁过期后误删新持有者的互斥锁（ADR-0008 §3）。
func (c *Client) ReleaseExecutionLock(ctx context.Context, taskID uuid.UUID, token string) error {
	if taskID == uuid.Nil {
		return fmt.Errorf("execution lock task id must not be empty")
	}
	if token == "" {
		return fmt.Errorf("execution lock token must not be empty")
	}

	if err := releaseLockScript.Run(ctx, c.rdb, []string{executionLockKey(taskID)}, token).Err(); err != nil {
		return fmt.Errorf("release execution lock: %w", err)
	}
	return nil
}

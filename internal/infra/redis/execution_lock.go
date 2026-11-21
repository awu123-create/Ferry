package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"

	"github.com/awu123-create/ferry/internal/domain"
)

// AcquireExecutionLock 尝试为指定 taskID 获取短时排他执行互斥锁（Execution Lock）。
// Key 格式：ferry:lock:task:<task_id>
//
// 实现指引（Milestone 1 核心逻辑，留待实现，见 ADR-0008 §3）：
//  1. 校验 ttl > 0。
//  2. 生成唯一随机 token（如密码学随机数或 UUIDv7），确保仅锁持有者本人可释放。
//  3. 执行 SetNX(ctx, executionLockKey(taskID), token, ttl).Result()。
//  4. 抢锁成功返回 (token, true, nil)；锁已被其他调用方持有则返回 ("", false, nil)。
func (c *Client) AcquireExecutionLock(ctx context.Context, taskID uuid.UUID, ttl time.Duration) (string, bool, error) {
	if ttl <= 0 {
		return "", false, domain.NewValidationError("ttl", "must > 0")
	}
	if taskID == uuid.Nil {
		return "", false, domain.NewValidationError("taskID", "must not be empty")
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

func executionLockKey(taskID uuid.UUID) string {
	return "ferry:lock:task:" + taskID.String()
}

// ReleaseExecutionLock 仅当 Redis 中存储的值等于 token 时才原子释放 Execution Lock。
//
// 实现指引（Milestone 1 核心逻辑，留待实现，见 ADR-0008 §3）：
//   - 使用 redis.NewScript 执行原子 Lua 脚本：
//     if redis.call("GET", KEYS[1]) == ARGV[1] then return redis.call("DEL", KEYS[1]) else return 0 end
//   - 严禁执行无条件 DEL，防止锁过期后误删新持有者的互斥锁。
func (c *Client) ReleaseExecutionLock(ctx context.Context, taskID uuid.UUID, token string) error {
	if taskID == uuid.Nil {
		return domain.NewValidationError("taskID", "must not be empty")
	}
	if token == "" {
		return domain.NewValidationError("token", "must not be empty")
	}

	var script = goredis.NewScript(`
		if redis.call("GET",KEYS[1]) == ARGV[1] then
			return redis.call("DEL",KEYS[1])
		else
			return 0
		end
	`)

	if err := script.Run(ctx, c.rdb, []string{executionLockKey(taskID)}, token).Err(); err != nil {
		return fmt.Errorf("release execution lock: %w", err)
	}
	return nil
}

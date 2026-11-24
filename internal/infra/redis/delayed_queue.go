package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"

	"github.com/awu123-create/ferry/internal/domain"
)

const delayedQueueKey = "ferry:delayed"

var (
	// claimDelayedScript 基于 Redis TIME 原子领取已到期任务并施加 Schedule Lease
	// （详见 docs/adr/0003-redis-zset-lease-and-pg-reconciler.md 与 docs/adr/0008-concurrency-fencing-and-flow-control-hardening.md）。
	claimDelayedScript = goredis.NewScript(`
		local t = redis.call("TIME")
		local now_ms = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
		local batch_size = tonumber(ARGV[1])
		local lease_ms = tonumber(ARGV[2])
		local lease_until_ms = now_ms + lease_ms

		local members = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", now_ms, "LIMIT", 0, batch_size)
		if #members == 0 then
			return {0}
		end
		for i = 1, #members do
			redis.call("ZADD", KEYS[1], "XX", lease_until_ms, members[i])
		end
		local out = {lease_until_ms}
		for i = 1, #members do
			out[i + 1] = members[i]
		end
		return out
	`)

	// ackDelayedScript 仅移除分数仍等于本轮 Schedule Lease 到期时间的成员，防止误删并发期间新写入的排期
	// （详见 docs/adr/0008-concurrency-fencing-and-flow-control-hardening.md）。
	ackDelayedScript = goredis.NewScript(`
		local expected = tonumber(ARGV[1])
		local removed = 0
		for i = 2, #ARGV do
			local score = redis.call("ZSCORE", KEYS[1], ARGV[i])
			if score and tonumber(score) == expected then
				removed = removed + redis.call("ZREM", KEYS[1], ARGV[i])
			end
		end
		return removed
	`)

	// deferContendedTaskScript 在 Worker 抢占 Execution Lock 争用失败时，基于锁剩余 PTTL（缺失或已过期时取兜底时长）
	// 以 ZADD NX 写入保底唤醒时间，不覆盖持锁方已写入的新排期（详见 docs/adr/0008-concurrency-fencing-and-flow-control-hardening.md）。
	deferContendedTaskScript = goredis.NewScript(`
		local t = redis.call("TIME")
		local now_ms = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
		local pttl = redis.call("PTTL", KEYS[1])
		local delay_ms = tonumber(ARGV[2])
		if pttl and pttl > 0 then
			delay_ms = pttl
		end
		local wake_at_ms = now_ms + delay_ms
		redis.call("ZADD", KEYS[2], "NX", wake_at_ms, ARGV[1])
		return wake_at_ms
	`)
)

// ScheduleDelayedBatch 将 Outbox 延迟通道领取的一批任务按目标毫秒时间戳批量写入 ferry:delayed。
func (c *Client) ScheduleDelayedBatch(ctx context.Context, items []domain.OutboxItem) error {
	if len(items) == 0 {
		return nil
	}
	members := make([]goredis.Z, 0, len(items))
	for _, item := range items {
		if item.TaskID == uuid.Nil {
			return fmt.Errorf("schedule delayed batch: task id must not be empty")
		}
		members = append(members, goredis.Z{
			Score:  float64(item.DeliverAt.UTC().UnixMilli()),
			Member: item.TaskID.String(),
		})
	}
	if err := c.rdb.ZAdd(ctx, delayedQueueKey, members...).Err(); err != nil {
		return fmt.Errorf("schedule delayed batch: %w", err)
	}
	return nil
}

// ScheduleDelayed 以覆写模式（普通 ZADD，非 NX）将单条任务写回 ferry:delayed。
// 用于持有 Execution Lock 的早到守卫，确保覆盖 Forwarder 尚未条件删除的 Schedule Lease 分数。
func (c *Client) ScheduleDelayed(ctx context.Context, taskID uuid.UUID, runAt time.Time) error {
	if taskID == uuid.Nil {
		return fmt.Errorf("schedule delayed task: task id must not be empty")
	}
	member := goredis.Z{
		Score:  float64(runAt.UTC().UnixMilli()),
		Member: taskID.String(),
	}
	if err := c.rdb.ZAdd(ctx, delayedQueueKey, member).Err(); err != nil {
		return fmt.Errorf("schedule delayed task: %w", err)
	}
	return nil
}

// RemoveDelayed 从 ferry:delayed 中移除指定任务（用于取消任务后的清理）。
func (c *Client) RemoveDelayed(ctx context.Context, taskID uuid.UUID) error {
	if taskID == uuid.Nil {
		return fmt.Errorf("remove delayed task: task id must not be empty")
	}
	if err := c.rdb.ZRem(ctx, delayedQueueKey, taskID.String()).Err(); err != nil {
		return fmt.Errorf("remove delayed task: %w", err)
	}
	return nil
}

// ClaimDelayed 通过原子 Lua 脚本领取最多 limit 条已到期任务，并施加 leaseTTL 时长的 Schedule Lease。
func (c *Client) ClaimDelayed(ctx context.Context, limit int, leaseTTL time.Duration) ([]uuid.UUID, int64, error) {
	if limit <= 0 {
		return nil, 0, fmt.Errorf("claim delayed tasks: limit must be > 0")
	}
	if leaseTTL <= 0 {
		return nil, 0, fmt.Errorf("claim delayed tasks: lease ttl must be > 0")
	}

	leaseMs := durationToPositiveMillis(leaseTTL)
	raw, err := claimDelayedScript.Run(ctx, c.rdb, []string{delayedQueueKey}, limit, leaseMs).Slice()
	if err != nil {
		return nil, 0, fmt.Errorf("claim delayed tasks: %w", err)
	}
	if len(raw) == 0 {
		return nil, 0, fmt.Errorf("claim delayed tasks: empty script response")
	}

	leaseUntilMs, ok := raw[0].(int64)
	if !ok {
		return nil, 0, fmt.Errorf("claim delayed tasks: unexpected lease timestamp type %T", raw[0])
	}
	if len(raw) == 1 {
		return nil, 0, nil
	}

	taskIDs := make([]uuid.UUID, 0, len(raw)-1)
	for _, elem := range raw[1:] {
		member, ok := elem.(string)
		if !ok {
			return nil, 0, fmt.Errorf("claim delayed tasks: unexpected member type %T", elem)
		}
		id, err := uuid.Parse(member)
		if err != nil {
			return nil, 0, fmt.Errorf("claim delayed tasks: parse member %q: %w", member, err)
		}
		taskIDs = append(taskIDs, id)
	}
	return taskIDs, leaseUntilMs, nil
}

// AckDelayed 条件删除 ferry:delayed 中分数仍等于 leaseUntilMs 的任务成员。
func (c *Client) AckDelayed(ctx context.Context, taskIDs []uuid.UUID, leaseUntilMs int64) error {
	if len(taskIDs) == 0 {
		return nil
	}
	if leaseUntilMs <= 0 {
		return fmt.Errorf("ack delayed tasks: lease timestamp must be > 0")
	}

	args := make([]any, 0, len(taskIDs)+1)
	args = append(args, leaseUntilMs)
	for _, id := range taskIDs {
		if id == uuid.Nil {
			return fmt.Errorf("ack delayed tasks: task id must not be empty")
		}
		args = append(args, id.String())
	}
	if err := ackDelayedScript.Run(ctx, c.rdb, []string{delayedQueueKey}, args...).Err(); err != nil {
		return fmt.Errorf("ack delayed tasks: %w", err)
	}
	return nil
}

// DeferContendedTask 在 Worker 抢锁争用失败时原子读取锁剩余有效期（缺失时取 fallbackDelay）并以 ZADD NX 写回 ferry:delayed。
func (c *Client) DeferContendedTask(ctx context.Context, taskID uuid.UUID, fallbackDelay time.Duration) error {
	if taskID == uuid.Nil {
		return fmt.Errorf("defer contended task: task id must not be empty")
	}
	if fallbackDelay <= 0 {
		return fmt.Errorf("defer contended task: fallback delay must be > 0")
	}

	fallbackMs := durationToPositiveMillis(fallbackDelay)
	keys := []string{executionLockKey(taskID), delayedQueueKey}
	if err := deferContendedTaskScript.Run(ctx, c.rdb, keys, taskID.String(), fallbackMs).Err(); err != nil {
		return fmt.Errorf("defer contended task: %w", err)
	}
	return nil
}

func durationToPositiveMillis(d time.Duration) int64 {
	if ms := d.Milliseconds(); ms > 0 {
		return ms
	}
	return 1
}

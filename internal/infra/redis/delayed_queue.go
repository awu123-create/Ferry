package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"

	"github.com/awu123-create/ferry/internal/domain"
)

const (
	delayedQueueKey = "ferry:delayed"
	// delayedLeaseKey 记录正在转发途中的任务以及对应的 Schedule Lease 到期时间（毫秒）。
	// 靠这张 Hash 表区分延迟池里的记录是“Forwarder 临时借走时留下的租约”，还是“已经排好的正式下次执行时间”。
	delayedLeaseKey = "ferry:delayed:lease"
)

var (
	// scheduleDelayedScript 批量把任务写入 ferry:delayed，同时删掉它们在 ferry:delayed:lease 里的旧租约记录。
	// 每条任务对应的取消标记 Key 通过 KEYS[3..] 传入：一旦发现该任务已有取消标记，就拒绝写回并顺手清理延迟池与租约记录。
	// 如果传了 ARGV[1]（最小等待毫秒数 > 0），会以 Redis 自己的当前时间做底线，确保写入的时间不早于 now + min_delay_ms；
	// 这样就算 Redis 时钟比 Worker 快，早到任务放回延迟池后也不会在 Redis 看来“已经到期”，避免立刻被捞出、反复空转。
	scheduleDelayedScript = goredis.NewScript(`
		local min_delay_ms = tonumber(ARGV[1])
		local min_score_ms = nil
		if min_delay_ms and min_delay_ms > 0 then
			local t = redis.call("TIME")
			local now_ms = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
			min_score_ms = now_ms + min_delay_ms
		end
		local written = 0
		for i = 2, #ARGV, 2 do
			local member = ARGV[i + 1]
			local tombstone_key = KEYS[2 + (i / 2)]
			if redis.call("EXISTS", tombstone_key) == 1 then
				redis.call("ZREM", KEYS[1], member)
				redis.call("HDEL", KEYS[2], member)
			else
				local score_ms = tonumber(ARGV[i])
				if min_score_ms and score_ms < min_score_ms then
					score_ms = min_score_ms
				end
				redis.call("ZADD", KEYS[1], string.format("%.0f", score_ms), member)
				redis.call("HDEL", KEYS[2], member)
				written = written + 1
			end
		end
		return written
	`)

	// cancelDelayedScript 在同一条原子脚本里立下带过期时间的取消标记，并从延迟池和租约表里一起删掉该任务；
	// 后续晚到的 Worker 或 Relay 再想往延迟池写回这条任务时，看到取消标记就会直接拒绝写入。
	cancelDelayedScript = goredis.NewScript(`
		redis.call("SET", KEYS[3], "1", "PX", ARGV[2])
		local removed = redis.call("ZREM", KEYS[1], ARGV[1])
		redis.call("HDEL", KEYS[2], ARGV[1])
		return removed
	`)

	// claimDelayedScript 用 Redis 自己的当前时间取出一批到期任务，把它们的执行时间往后推 lease_ms 作为 Schedule Lease，
	// 并把租约到期时间记进 ferry:delayed:lease（见 docs/adr/0003-redis-zset-lease-and-pg-reconciler.md 与 docs/adr/0008-concurrency-fencing-and-flow-control-hardening.md）。
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
		local lease_str = string.format("%.0f", lease_until_ms)
		for i = 1, #members do
			redis.call("ZADD", KEYS[1], "XX", lease_str, members[i])
			redis.call("HSET", KEYS[2], members[i], lease_str)
		end
		local out = {lease_until_ms}
		for i = 1, #members do
			out[i + 1] = members[i]
		end
		return out
	`)

	// ackDelayedScript 只有在租约表里的记录和延迟池里的分数都还等于本轮租约时间时，才真正删除成员；
	// 只要中途有 Worker 改写了新的执行时间或接管了保底唤醒，这里就不会误删（见 docs/adr/0008-concurrency-fencing-and-flow-control-hardening.md）。
	ackDelayedScript = goredis.NewScript(`
		local expected = tonumber(ARGV[1])
		local removed = 0
		for i = 2, #ARGV do
			local member = ARGV[i]
			local leased = redis.call("HGET", KEYS[2], member)
			if leased and tonumber(leased) == expected then
				redis.call("HDEL", KEYS[2], member)
				local score = redis.call("ZSCORE", KEYS[1], member)
				if score and tonumber(score) == expected then
					removed = removed + redis.call("ZREM", KEYS[1], member)
				end
			end
		end
		return removed
	`)

	// deferContendedTaskScript 在 Worker 没抢到 Execution Lock 时，先检查该任务是否已有取消标记：
	// - 已有取消标记：拒绝写回，并顺手清掉延迟池与租约表里的残留，直接返回 0；
	// - 没有取消标记：按当前锁还剩多久过期（锁没了就用默认等待时间）在延迟池里记一个下次唤醒时间：
	//   - 任务不在延迟池里，或者池里留着的只是 Forwarder 转发途中临时续的租约：直接写成这次算出的唤醒时间，并删掉租约记录，
	//     免得 Forwarder 随后清理租约时顺手把它删丢；
	//   - 任务已经在延迟池里且不是临时租约（说明别人已经写好了正式的下次执行时间）：保持原时间不动
	// （见 docs/adr/0008-concurrency-fencing-and-flow-control-hardening.md）。
	deferContendedTaskScript = goredis.NewScript(`
		local member = ARGV[1]
		if redis.call("EXISTS", KEYS[4]) == 1 then
			redis.call("ZREM", KEYS[2], member)
			redis.call("HDEL", KEYS[3], member)
			return 0
		end

		local t = redis.call("TIME")
		local now_ms = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
		local pttl = redis.call("PTTL", KEYS[1])
		local delay_ms = tonumber(ARGV[2])
		if pttl and pttl > 0 then
			delay_ms = pttl
		end
		local wake_at_ms = now_ms + delay_ms

		local cur_score = redis.call("ZSCORE", KEYS[2], member)
		local leased = cur_score and redis.call("HGET", KEYS[3], member)
		if not cur_score or (leased and tonumber(cur_score) == tonumber(leased)) then
			redis.call("ZADD", KEYS[2], string.format("%.0f", wake_at_ms), member)
			redis.call("HDEL", KEYS[3], member)
			return wake_at_ms
		end

		return tonumber(cur_score)
	`)
)

func cancelledDelayedKey(taskID uuid.UUID) string {
	return "ferry:delayed:cancelled:" + taskID.String()
}

// ScheduleDelayedBatch 批量把延迟通道取出的任务写入 ferry:delayed（时间按毫秒向上取整），同时清掉旧租约记录；
// 如果批里某条任务已有取消标记，则跳过该条并顺手清理，同批其他正常任务照常写入。
func (c *Client) ScheduleDelayedBatch(ctx context.Context, items []domain.OutboxItem) error {
	if len(items) == 0 {
		return nil
	}
	keys := make([]string, 0, len(items)+2)
	keys = append(keys, delayedQueueKey, delayedLeaseKey)
	args := make([]any, 0, len(items)*2+1)
	args = append(args, int64(0))
	for _, item := range items {
		if item.TaskID == uuid.Nil {
			return fmt.Errorf("schedule delayed batch: task id must not be empty")
		}
		keys = append(keys, cancelledDelayedKey(item.TaskID))
		args = append(args, ceilUnixMilli(item.DeliverAt), item.TaskID.String())
	}
	if err := scheduleDelayedScript.Run(ctx, c.rdb, keys, args...).Err(); err != nil {
		return fmt.Errorf("schedule delayed batch: %w", err)
	}
	return nil
}

// ScheduleDelayed 把单条任务覆盖写回 ferry:delayed 并清掉旧租约记录；若已有取消标记则拒绝写回。
// 当 minDelay > 0 时，会在 Redis 里按 Redis 当前时间保证至少延后 minDelay，避免两台机器时间不一致导致任务放回去又立刻被捞出。
func (c *Client) ScheduleDelayed(ctx context.Context, taskID uuid.UUID, runAt time.Time, minDelay time.Duration) error {
	if taskID == uuid.Nil {
		return fmt.Errorf("schedule delayed task: task id must not be empty")
	}
	keys := []string{delayedQueueKey, delayedLeaseKey, cancelledDelayedKey(taskID)}
	if err := scheduleDelayedScript.Run(ctx, c.rdb, keys, ceilDurationMillis(minDelay), ceilUnixMilli(runAt), taskID.String()).Err(); err != nil {
		return fmt.Errorf("schedule delayed task: %w", err)
	}
	return nil
}

// CancelDelayed 为已取消任务写入带过期时间的取消标记，并一次性从延迟池和租约表里删掉它，防止后续迟到的写回把任务重新塞进延迟池。
func (c *Client) CancelDelayed(ctx context.Context, taskID uuid.UUID, tombstoneTTL time.Duration) error {
	if taskID == uuid.Nil {
		return fmt.Errorf("cancel delayed task: task id must not be empty")
	}
	if tombstoneTTL <= 0 {
		return fmt.Errorf("cancel delayed task: tombstone ttl must be > 0")
	}
	ttlMs := ceilDurationMillis(tombstoneTTL)
	keys := []string{delayedQueueKey, delayedLeaseKey, cancelledDelayedKey(taskID)}
	if err := cancelDelayedScript.Run(ctx, c.rdb, keys, taskID.String(), ttlMs).Err(); err != nil {
		return fmt.Errorf("cancel delayed task: %w", err)
	}
	return nil
}

// ClaimDelayed 取出最多 limit 条已到期任务，给它们加上 leaseTTL 时长的 Schedule Lease 并记进租约表。
func (c *Client) ClaimDelayed(ctx context.Context, limit int, leaseTTL time.Duration) (domain.DelayedLeaseBatch, error) {
	if limit <= 0 {
		return domain.DelayedLeaseBatch{}, fmt.Errorf("claim delayed tasks: limit must be > 0")
	}
	if leaseTTL <= 0 {
		return domain.DelayedLeaseBatch{}, fmt.Errorf("claim delayed tasks: lease ttl must be > 0")
	}

	leaseMs := ceilDurationMillis(leaseTTL)
	keys := []string{delayedQueueKey, delayedLeaseKey}
	raw, err := claimDelayedScript.Run(ctx, c.rdb, keys, limit, leaseMs).Slice()
	if err != nil {
		return domain.DelayedLeaseBatch{}, fmt.Errorf("claim delayed tasks: %w", err)
	}
	if len(raw) == 0 {
		return domain.DelayedLeaseBatch{}, fmt.Errorf("claim delayed tasks: empty script response")
	}

	leaseUntilMs, ok := raw[0].(int64)
	if !ok {
		return domain.DelayedLeaseBatch{}, fmt.Errorf("claim delayed tasks: unexpected lease timestamp type %T", raw[0])
	}
	if len(raw) == 1 {
		return domain.DelayedLeaseBatch{}, nil
	}

	taskIDs := make([]uuid.UUID, 0, len(raw)-1)
	for _, elem := range raw[1:] {
		member, ok := elem.(string)
		if !ok {
			return domain.DelayedLeaseBatch{}, fmt.Errorf("claim delayed tasks: unexpected member type %T", elem)
		}
		id, err := uuid.Parse(member)
		if err != nil {
			return domain.DelayedLeaseBatch{}, fmt.Errorf("claim delayed tasks: parse member %q: %w", member, err)
		}
		taskIDs = append(taskIDs, id)
	}
	return domain.DelayedLeaseBatch{
		TaskIDs:      taskIDs,
		LeaseUntilMs: leaseUntilMs,
	}, nil
}

// AckDelayed 只在租约表记录和延迟池分数都还等于 batch.LeaseUntilMs 时，才把这批任务从延迟池删掉。
func (c *Client) AckDelayed(ctx context.Context, batch domain.DelayedLeaseBatch) error {
	if len(batch.TaskIDs) == 0 {
		return nil
	}
	if batch.LeaseUntilMs <= 0 {
		return fmt.Errorf("ack delayed tasks: lease timestamp must be > 0")
	}

	args := make([]any, 0, len(batch.TaskIDs)+1)
	args = append(args, batch.LeaseUntilMs)
	for _, id := range batch.TaskIDs {
		if id == uuid.Nil {
			return fmt.Errorf("ack delayed tasks: task id must not be empty")
		}
		args = append(args, id.String())
	}
	keys := []string{delayedQueueKey, delayedLeaseKey}
	if err := ackDelayedScript.Run(ctx, c.rdb, keys, args...).Err(); err != nil {
		return fmt.Errorf("ack delayed tasks: %w", err)
	}
	return nil
}

// DeferContendedTask 在 Worker 没抢到 Execution Lock 时，按锁的剩余过期时间（锁已不存在时用 fallbackDelay）写回延迟池。
// 如果任务已有取消标记会直接拒绝写回并清理残留；如果池里留着的只是 Forwarder 转发途中临时续的租约，会改写成新的唤醒时间并清掉租约记录。
func (c *Client) DeferContendedTask(ctx context.Context, taskID uuid.UUID, fallbackDelay time.Duration) error {
	if taskID == uuid.Nil {
		return fmt.Errorf("defer contended task: task id must not be empty")
	}
	if fallbackDelay <= 0 {
		return fmt.Errorf("defer contended task: fallback delay must be > 0")
	}

	fallbackMs := ceilDurationMillis(fallbackDelay)
	keys := []string{executionLockKey(taskID), delayedQueueKey, delayedLeaseKey, cancelledDelayedKey(taskID)}
	if err := deferContendedTaskScript.Run(ctx, c.rdb, keys, taskID.String(), fallbackMs).Err(); err != nil {
		return fmt.Errorf("defer contended task: %w", err)
	}
	return nil
}

// ceilUnixMilli 把时间戳按毫秒向上取整。
// PostgreSQL 存的时间带微秒，直接转毫秒会舍掉不足 1ms 的尾数，可能导致任务提前不到 1ms 被捞出，又被 Worker 判成没到点放回延迟池。
func ceilUnixMilli(t time.Time) int64 {
	utc := t.UTC()
	ms := utc.UnixMilli()
	if utc.Nanosecond()%int(time.Millisecond) != 0 {
		return ms + 1
	}
	return ms
}

// ceilDurationMillis 把正时长按毫秒向上取整（非正时长返回 0），避免不足 1ms 或非整毫秒的 TTL 被向下截短。
func ceilDurationMillis(d time.Duration) int64 {
	if d <= 0 {
		return 0
	}
	ms := d / time.Millisecond
	if d%time.Millisecond != 0 {
		return int64(ms) + 1
	}
	return int64(ms)
}

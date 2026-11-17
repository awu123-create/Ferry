# ADR-0003: 延迟与退避重试采用 Redis ZSet 租约倒计时池加 PG 对账兜底

- **状态**：已采纳（Accepted）
- **日期**：2026-11-16

## 背景与问题（Context）

Ferry 的投递引擎必须支持三类“非立即执行”的时间调度需求：
1. 用户提交任务时显式指定未来某个任意时刻 `deliver_at`（如 17 分钟后或次日 09:00 执行）；
2. 下游返回 `5xx` 或超时后的指数退避重试（`base_delay * 2^(attempt-1) + full_jitter`）；
3. 下游返回 `429 Too Many Requests` 时解析 `Retry-After` 响应头，或触发限流/熔断后动态顺延几秒重试。

Kafka 本质上是面向即时吞吐的顺序追加日志，不支持按任意时间戳投递或单条跳过等待；而如果像 `x-data` 中的 River 那样在高频调度与重试场景下持续对 PostgreSQL 执行 `SELECT ... WHERE scheduled_at <= now()` 与频繁状态更新，高并发重试风暴会导致 PG 索引写放大、锁竞争和 Vacuum 抖动。同时，一旦把倒计时放进内存型组件 Redis，就必须解决“捞出后未发 Kafka 前崩溃丢单”和“Redis 宕机内存清空”两个可靠性死穴。

## 备选方案对比（Considered Options）

### 方案 1：Redis `ZSet` 统一倒计时池 + 30 秒 Lua 租约回灌 + PG Reconciler 低频对账兜底（采纳）
- 所有未到期的定时任务、退避重试任务和流控顺延任务统一写入单一 Redis `ZSet`（`ferry:delayed`），`score` 为目标触发毫秒时间戳，`member` 为 `task_id`。
- 后台 Forwarder 每 `100ms` 通过原子 Lua 脚本续租 30 秒捞取到期任务回灌 Kafka；PG `tasks.next_run_at` 保持持久化真相，由 `60s` 一次的低频对账协程（Reconciler）兜底恢复任何丢失。

### 方案 2：Kafka 多级固定阶梯重试 Topic（`retry-10s` / `retry-1m` / `retry-5m` / `retry-30m`）
- 每个重试档位对应独立 Topic，消费者读出消息头时间戳，未到期则 `Pause` 分区等待。
- **缺点**：无法支持用户指定的任意精确延迟时间（如 7 分钟或 23 小时后执行），也无法精确遵循下游 `Retry-After: 42` 响应头，且多级 Topic 与消费者状态管理复杂。

### 方案 3：纯 PostgreSQL 高频定时扫表
- 直接用 PG 表字段 `next_run_at` 做每秒轮询扫表唤醒。
- **缺点**：无法体现 Redis 在时间轮/有序集合调度上的高并发优势，在数千 QPS 失败重试或限流回退风暴下会把大量读写 IOPS 压回 PostgreSQL。

## 最终决策（Decision）

采纳 **方案 1（Redis `ZSet` 30s 租约倒计时池 + PG Reconciler 兜底）**：

1. **轻量级 ZSet 载荷设计**：
   - `ferry:delayed` 仅存储 36 字节 UUID 字符串 `task_id`，完整任务载荷与 HTTP 头只存储一份在 PG `tasks` 表中，避免内存放大与双份数据不一致。
2. **Forwarder 30 秒租约（Visibility Timeout）防崩协议**：
   - 严禁“先 `ZPOPMIN` 删除再写 Kafka”，否则两步之间进程崩溃即永久丢消息；
   - Forwarder 每 `100ms` 执行 Lua 脚本 `claim_delayed.lua`：
     1. `ZRANGEBYSCORE ferry:delayed -inf <now_ms> LIMIT 0 100` 取出一批到期任务；
     2. 对该批任务原子执行 `ZADD ferry:delayed XX <now_ms + 30000> <task_id>`（将 `score` 往后推迟 30 秒作为处理租约）；
     3. 返回该批 `task_id` 给 Go 进程。
   - Go 进程将这批 `task_id` 批量发往 Kafka `ferry.tasks.ready`（`acks=all`），收到 Broker 确认后再执行 `ZREM ferry:delayed <task_id>...`；
   - 若 Forwarder 在写 Kafka 前崩溃或发生网络分区，该批任务在 30 秒租约过期后会自动重新变为到期状态，被其他实例安全捞起。
3. **PG Reconciler 低频对账自愈（兜底 Redis 极端丢失）**：
   - 利用 PG `tasks` 表的部分索引 `tasks_reconcile_idx (next_run_at) WHERE status IN ('PENDING', 'RETRYING')`（体积极小，终态行不进索引），每 `60s` 扫描一批到期超过宽限期的卡滞任务：
     ```sql
     SELECT id, next_run_at
     FROM tasks
     WHERE status IN ('PENDING', 'RETRYING')
       AND next_run_at < now() - interval '60 seconds'
     ORDER BY next_run_at ASC
     LIMIT 500;
     ```
   - 对捞出的任务先批量 Pipeline 查询 Redis `EXISTS ferry:lock:task:<id>` 与 `ZSCORE ferry:delayed <id>`：
     - **若任务仍在 `ferry:delayed` 中等待**（典型原因：Endpoint 熔断或限流顺延了触发时间，Redis 中的时间晚于 PG 记录的 `next_run_at`），则执行 `UPDATE tasks SET next_run_at = to_timestamp(zscore / 1000.0)` 将 PG 对齐到更晚时间，防止同一批合法顺延任务反复占据前 500 扫描名额造成“头部饿死”；
     - **若任务既无活跃执行锁也不在 `ferry:delayed` 中**（典型原因：Redis 发生故障重启丢内存，或极端写入异常），Reconciler 立即补推该 `task_id` 到 Kafka `ferry.tasks.ready`，并将 PG `next_run_at` 顺延 `30s`（防止下一轮重复补推）。

## 影响与取舍（Consequences & Trade-offs）

- **正向收益**：
  - 平时所有延迟定时、指数退避与限流回退均在 Redis 内存以 $O(\log N)$ 复杂度完成，支持任意秒级精度时间，零 PG 高频扫描开销；
  - 即使 Redis 容器被销毁重建（`FLUSHALL` 演练验证），PG Reconciler 也能在 1 分钟内自动全量捞回所有在途 `PENDING` 与 `RETRYING` 任务，做到**零丢单**。
- **明确取舍**：
  - 当 Redis 集群处于“完全断联不可访问”窗口期时，新的延迟定时与退避重试任务无法写入 ZSet，但 PG 真相表与 `outbox` 会稳妥保留状态，待 Redis 恢复后自动续接（若延迟任务等待期间已到期，Outbox Relay 直接转直推 Kafka）。

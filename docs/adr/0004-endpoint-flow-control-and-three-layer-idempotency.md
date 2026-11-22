# ADR-0004: Endpoint 级三维流控隔离与全链路三层幂等机制

- **状态**：已采纳（部分细则已由 [ADR-0007](./0007-tactical-ddd-and-deep-module-seams.md) 与 [ADR-0008](./0008-concurrency-fencing-and-flow-control-hardening.md) 修订：顺延改为按 `1/QPS` 虚拟排号摊平、半开试探加超时、`deadline_at` 改为首次触发时间起算、补齐执行锁四步协议与复合乐观版本 CAS）
- **日期**：2026-11-16

## 背景与问题（Context）

在实际多目标投递场景中，不同下游接收方（Endpoint）的承载能力悬殊：有的健康服务每秒能扛数千并发，有的脆弱服务超过 20 QPS 即崩溃，还有的服务可能彻底宕机导致每个请求阻塞至 10 秒超时才返回错误。如果只靠全局固定 Worker 协程池硬推（类似早期单机固定并发模式），慢接口和宕机接口会迅速占满全部协程，产生严重的队头阻塞（Noisy Neighbor），拖垮全平台所有健康下游的投递 SLA。

此外，基于 Kafka + Outbox + 租约重试的分布式引擎天然保证“至少一次（At-Least-Once）”语义，必须在入口、Worker 并发以及下游接收方建立闭环幂等防线。

## 备选方案对比（Considered Options）

### 方案 1：基于单一 Redis Lua 脚本的 Endpoint 级三维流控 + 三层全链路幂等（采纳）
- Worker 领取 Kafka 消息后、发起 HTTP 请求前，先在 Redis 执行单次原子 Lua 脚本 `admit_endpoint.lua`，依次判定：**熔断状态 -> QPS 令牌桶 -> 基于带过期时间 ZSet 的并发信号量**；未通过准入则立即将任务顺延回退至 `ferry:delayed`，微秒级释放 Worker 协程。
- 配合 PG 唯一索引 + Redis 执行短锁/PG 状态守卫 + 下游标准请求头构成三层幂等。

### 方案 2：为每个 Endpoint 动态创建独立 Kafka Topic 或专属 Partition
- **缺点**：Endpoint 数量达到数千时会导致 Kafka 分区元数据和消费者组重平衡风暴（Rebalance Storm），无法支持动态扩缩容与毫秒级改配限流参数。

### 方案 3：仅使用全局限流 + Redis `INCR/DECR` 维护并发数
- **缺点**：无法隔离单点故障商户；且传统 `INCR/DECR` 计数在 Worker 进程遭遇 `kill -9` 或 OOM 强杀时会丢失 `DECR` 回调，导致并发名额永久泄漏直至该 Endpoint 彻底假死。

## 最终决策（Decision）

采纳 **方案 1（Endpoint 级三维流控隔离 + 三层全链路幂等）**：

### 一、Endpoint 三维流控保护（`admit_endpoint.lua` 单次原子判定）

Worker 在从 PG 读取任务详情并通过状态守卫后，向 Redis 提交一次 Lua 准入判定：

1. **连续失败自动熔断器（`ferry:cb:<endpoint_id>`，Hash 结构）**：
   - 记录 `state`（`CLOSED` / `OPEN` / `HALF_OPEN`）、`consecutive_failures` 与 `open_until_ms`；
   - 当连续可重试失败（网络超时、`5xx`、`408`、`429`）累计达到 `Endpoint.circuit_threshold`（默认 `30`），切换至 `OPEN` 态并设定冷却期 `30s`；
   - `OPEN` 期间所有新到达的任务在微秒级被 Lua 拦截，**不占用 HTTP 连接、不递增 `attempt`**，直接写回 `ferry:delayed` 顺延；初版曾采用固定窗口随机顺延，现行虚拟排号顺延（`next_free_ms`）、`HALF_OPEN` 单试探名额超时保护、`429`/`503` `Retry-After` 冷却同步、对称 `CompleteDelivery` 结算及脚本内 `redis.call('TIME')` 统一以 [ADR-0008](./0008-concurrency-fencing-and-flow-control-hardening.md) 为准。
2. **QPS 令牌桶限流（`ferry:rl:<endpoint_id>`，Hash 结构）**：
   - 按 `Endpoint.rate_limit_qps`（如 `50/s`）以微秒/毫秒精度补充令牌（允许短时 `burst = max(1, ceil(qps))`）；
   - 无可用令牌时拒绝准入并退回 `ferry:delayed`；初版曾采用 `now_ms + 1000 + random` 固定顺延，现行按 `1 / rate_limit_qps` 虚拟排号摊平顺延见 [ADR-0008](./0008-concurrency-fencing-and-flow-control-hardening.md)。
3. **防泄漏并发信号量（`ferry:inflight:<endpoint_id>`，ZSet 结构）**：
   - **关键防死锁设计**：不使用 `INCR/DECR`，而是将当前正在投递的任务以 `member = task_id`、`score = now_ms + endpoint.timeout_ms + 5000` 写入 ZSet；
   - 每次准入前先执行 `ZREMRANGEBYSCORE ferry:inflight:<ep_id> -inf <now_ms>` 自动清理因进程崩溃遗留的过期僵死槽位，再校验 `ZCARD < endpoint.max_concurrency`；
   - 投递结束通过 `CompleteDelivery` 主动释放；即便 Worker 进程被 `kill -9` 强杀，槽位也最晚在 `timeout_ms + 5s` 后自动失效释放，彻底杜绝并发死锁。
4. **有界循环兜底（`deadline_at` 硬边界）**：
   - 为防止持续限流或永久熔断导致任务在 `Kafka <-> ZSet` 之间无限循环空转，每条任务在创建或重放时均记录 `deadline_at`（初版曾按 `created_at + 24h` 计算，现行起算基准改为首次 `next_run_at + default_ttl`，见 [ADR-0008](./0008-concurrency-fencing-and-flow-control-hardening.md)）；
   - Worker 判定当前时间或下次排期已超过 `deadline_at` 时直接终止流转，通过复合乐观锁落入终态 `DEAD_LETTER`（标记 `TTL_EXPIRED`）。

### 二、全链路三层幂等契约

1. **入口幂等（防业务方重复提交）**：
   - PG 建立唯一索引 `UNIQUE(endpoint_id, idempotency_key)`；
   - 同键同载荷（比对 `payload_sha256`）重复调用 `POST /v1/tasks` 时返回 `200 OK` 与原始 `task_id`；同键不同载荷返回 `409 Conflict` 拒绝静默覆盖。
2. **Worker 并发与重放幂等（防 Kafka 重平衡或 Relay 重复推）**：
   - Worker 在发起 HTTP 请求前先抢占 Redis 执行锁 `ferry:lock:task:<task_id>` 并核对 PG `tasks` 状态，若已终态直接确认 Kafka Offset；
   - _演进说明_：初版曾拟定抢锁失败直接跳过确认、并使用无版本号的 `status NOT IN (...)` 条件更新；现行执行锁四步协议（含提交后验主删锁、争用时 `ZADD NX` 保底顺延、`Cancel` 主动抢锁互斥）及 `(replay_count, attempt)` 复合乐观版本 CAS 守卫统一以 [ADR-0007](./0007-tactical-ddd-and-deep-module-seams.md) 与 [ADR-0008](./0008-concurrency-fencing-and-flow-control-hardening.md) 为准。
3. **下游幂等契约（防“下游已处理成功但 200 响应丢包”）**：
   - 遵循分布式传输“至少一次”客观规律，每次重试固定携带同一个 `X-Ferry-Task-ID: <task_id>` 请求头，供下游接收服务基于该 ID 做业务幂等去重，同时结合 `X-Ferry-Signature` 防止恶意伪造重放。

## 影响与取舍（Consequences & Trade-offs）

- **正向收益**：
  - 单个下游发生 10 秒超时雪崩时，仅占用短暂探测窗口，随即触发熔断与并发封顶，将大量失败请求快速折返至 Redis ZSet 休眠，Worker 协程资源 100% 留给健康 Endpoint；
  - `admit_endpoint.lua` 在单次 Redis RTT 内完成三重判定，并发安全且无竞态条件。
- **明确取舍**：
  - Worker 对 Redis 存在强安全依赖：当 Redis 彻底断连不可用时，为避免失控放行将脆性下游打挂（Fail-Safe），Worker 主动暂停从 Kafka 拉取新消息并让出 `/readyz` 就绪状态，待 Redis 恢复后自动恢复消费（在途任务照常收尾）；同时入口 `POST /v1/tasks` 仍完全可用（只依赖 PG 落库）。

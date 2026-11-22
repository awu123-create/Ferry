# ADR-0002: 入口事务一致性采用 PG 同事务 Outbox 与 Go 批量搬运协程

- **状态**：已采纳（部分细则已由 [ADR-0008](./0008-concurrency-fencing-and-flow-control-hardening.md) 修订：Outbox 拆分为立即/延迟双通道取数、升级 `(deliver_at, id)` 复合索引、固定 `task_id` 分区键并增加单批 `5s` 事务硬超时）
- **日期**：2026-11-16

## 背景与问题（Context）

当 Ferry 作为独立网关接收 `POST /v1/tasks` 请求时，系统需要同时完成两件事：**将任务持久化到 PostgreSQL（账本真相）**，以及**将可执行任务推入 Kafka 或延迟调度池**。

直觉上最简单的做法是“入口直接同步写 Kafka（`acks=all`）”，但在 Ferry 的场景下存在三个硬伤：
1. Kafka 无法在写入路径上做同步唯一索引幂等校验（即 `UNIQUE(endpoint_id, idempotency_key)` 拦截重复请求并校验载荷哈希冲突）；
2. 仅写 Kafka 后立即返回 `202 Accepted`，下游消费落库前存在可见性空窗期，导致上游紧接着调用 `GET /v1/tasks/{id}` 查不到任务初始状态；
3. 若应用层先写 PG 再调 Kafka Producer（双写无事务），在 PG 提交成功而 Kafka 发送前进程发生宕机或网络闪断，任务将永久停留在 `PENDING` 态形成“幽灵丢单”。

## 备选方案对比（Considered Options）

### 方案 1：PG 同事务 `tasks + outbox` + Go 内置无状态批量 Relay 协程（采纳）
- 沿用单库事务队列“队列载荷与业务真相同事务提交”的设计思想：入口在同一个 PG 事务中写入 `tasks` 与 `outbox`，并在提交后触发 `NOTIFY ferry_outbox`。
- Go 服务内部启动多个无状态 Outbox Relay 协程，以 `LISTEN/NOTIFY` 瞬时唤醒（辅以 `500ms` 兜底轮询定时器），通过 `SELECT ... FOR UPDATE SKIP LOCKED LIMIT 100` 抢占批次，推入 Kafka 或 Redis ZSet 成功后再直接 `DELETE`。

### 方案 2：Debezium + Kafka Connect 外部 CDC 链路
- 开启 PostgreSQL 逻辑复制，通过 JVM 生态的 Debezium 连接器监听 WAL 变更事件自动同步到 Kafka。
- **缺点**：需要额外部署与运维 Debezium / Kafka Connect 容器，技术栈引入 Java 运行时，与单人 Go 后端项目的简洁部署目标冲突，且出问题时跨语言排查链路过长。

### 方案 3：Go 进程内基于 `pglogrepl` 订阅 PG WAL 逻辑复制槽
- 使用纯 Go 驱动解析 PostgreSQL 逻辑复制流（或 `pg_logical_emit_message`），彻底消除对 `outbox` 表的 `SELECT/DELETE` 轮询。
- **缺点**：单个逻辑复制槽同一时刻只能被一个活跃消费者连接（主备切换复杂），一旦消费端出现短暂阻塞会导致 PG 服务端 WAL 日志持续积压甚至打满磁盘；同时仍无法直接按 `deliver_at` 区分路由逻辑。

## 最终决策（Decision）

采纳 **方案 1（PG 同事务 Outbox + Go 无状态批量 Relay）**：

1. **同事务写入与幂等冲突处理（基于 GORM 原语，详见 [ADR-0006](./0006-tech-stack-and-engineering-tooling.md)）**：
   - `POST /v1/tasks` 开启 GORM 事务，通过 `clause.OnConflict{Columns: ... , DoNothing: true}` 插入 `tasks` 触发唯一约束守卫；
   - 若 `RowsAffected == 0` 命中冲突，回查已有行比对 `payload_sha256`：载荷一致直接返回 `200 OK` 原任务回执，载荷不一致返回 `409 Conflict`；
   - 新建任务成功则在同一事务内写入 `outbox(task_id, deliver_at)`，事务提交后由 `goose` 迁移脚本挂载在 `outbox` 表上的 `AFTER INSERT` 触发器自动执行 `pg_notify('ferry_outbox', '')`，无需业务代码手写通知 SQL。
2. **双触发批量搬运与分流路由**：
   - Relay 协程复用底层 `pgx` 连接调用 `WaitForNotification` 监听 `ferry_outbox` 通道，收到通知或 `500ms` 定时器触发后通过 GORM `clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}` 按批拉取，直到 `outbox` 排空；
   - _演进说明_：初版曾按单通道 `ORDER BY id ASC LIMIT 100` 混取立即/延迟任务且未固定 Kafka 分区键；现行取数协议、复合索引与事务超时统一以 [ADR-0008](./0008-concurrency-fencing-and-flow-control-hardening.md) 为准——拆分为**立即通道**（`deliver_at <= now()` 直推 Kafka，固定以 `task_id` 为分区键，`acks=all` 确认后物理删除）与**延迟通道**（`deliver_at > now()` 通过 Redis Pipeline 批量 `ZADD ferry:delayed` 确认后删除），升级复合索引 `outbox_deliver_idx (deliver_at ASC, id ASC)`，并对单批回调事务绑定 `5s` 硬超时防长事务阻塞 Autovacuum。
3. **Redis 不可用时的 Relay 隔离保护**：
   - 当 Redis 不可用时，立即通道照常推 Kafka 并物理删除对应 `outbox` 行，延迟通道独立退避且不阻塞立即通道（见 [ADR-0008](./0008-concurrency-fencing-and-flow-control-hardening.md)）；待 Redis 恢复或 `deliver_at` 到期转入立即通道时自动续接。

## 影响与取舍（Consequences & Trade-offs）

- **正向收益**：
  - 完全使用 Go + PG 原生机制实现，支持部署多个 `ferry-server` 实例水平扩展，依靠 `FOR UPDATE SKIP LOCKED` 无冲突分摊搬运负载；
  - 搬运成功即执行物理 `DELETE`（而非软删除 `status='SENT'`），配合 PostgreSQL Autovacuum，使 `outbox` 常驻行数始终接近 0，规避表膨胀；
  - 入口 API 与后台异步化彻底解耦，响应耗时稳定在单次轻量 PG 事务级。
- **明确取舍**：
  - 存在极小概率的“已写入 Kafka `acks=all` 但未及执行 `DELETE FROM outbox` 时 Relay 进程崩溃”窗口，恢复后会产生重复投递消息（At-Least-Once），该重复由 Worker 侧 Redis 执行锁与 PG 复合乐观锁守卫（[ADR-0004](./0004-endpoint-flow-control-and-three-layer-idempotency.md)、[ADR-0008](./0008-concurrency-fencing-and-flow-control-hardening.md)）幂等消解。

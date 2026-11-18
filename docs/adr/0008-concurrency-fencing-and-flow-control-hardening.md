# ADR-0008: 调度并发围栏、流控排号防空转与载荷字节保真加固

- **状态**：已采纳（Accepted）
- **日期**：2026-11-18
- **关联**：局部修订并加固 [ADR-0001](./0001-unified-http-task-delivery-model.md) 至 [ADR-0007](./0007-tactical-ddd-and-deep-module-seams.md) 中的并发时序、流控顺延、建表契约与里程碑切分边界

## 背景与问题（Context）

在完成初版架构（ADR-0001 ~ 0006）与轻量战术 DDD 边界设计（ADR-0007）后，通过一轮针对分布式异常时序与极端积压场景的对抗审查，识别出在进入 Milestone 1 编码与建表前必须闭环的五类隐患：

1. **非终态重复消息与重放轮次歧义**：仅检查“非终态”无法拦截任务处于 `RETRYING` 退避等待期时到达的重复消息（会跳过退避提前重发并多扣配额），并发超时下无版本号的 CAS 也会重复累加 `attempt`；同时死信重放将 `attempt` 归零后会导致 `task_attempts` 出现重复 `attempt_no`。
2. **协调原语竞态窗口**：`Execution Lock` 若在写库前释放、裸 `DEL`、抢锁失败直接丢弃消息或 `Cancel`“先探活再改库”，均存在重发、误删锁或取消竞态；Forwarder 推 Kafka 后的无条件 `ZREM` 也可能误删 Worker 刚写入的新顺延记录；Reconciler 篡改 `next_run_at + 30s` 会与提前到达校验直接冲突。
3. **`Outbox` 队头阻塞与长事务隐患**：按 `ORDER BY id ASC LIMIT 100` 混取任务时，Redis 宕机留存的 100 条延迟任务会堵死后续所有立即任务；且回调式事务若无硬超时会在 Kafka 抖动时长期持有行锁阻塞 Autovacuum。
4. **流控固定顺延引发空转风暴（Noisy Neighbor 复辟）**：限流被拒时固定顺延 `1s + jitter` 与积压量无关，单个低速 Endpoint 积压万单时会导致每秒数千条消息在延迟池、Kafka 与 PG 之间无效空转，侵占健康 Endpoint 的转发与消费带宽。
5. **数据保真、时间预算与阶段倒挂**：`JSONB` 存储会重排字段与空白符破坏原始字节签名；`deadline_at = created_at + 24h` 会误杀次日定时任务，而旧退避曲线仅 30 秒即耗尽；Milestone 1 的取消互斥与失败状态也存在对 Milestone 2 组件的隐性依赖。

## 最终决策（Decision）

### 一、端到端载荷字节保真与 `Endpoint` 地址契约（修订 ADR-0001）

1. **`Endpoint` 一对一目标地址契约**：明确单个 `Endpoint` 唯一绑定一个完整 `target_url` 与独立流控舱壁；上游调用 `POST /v1/tasks` 仅指定已注册的 `endpoint_id`，`Task` 不拆分子路径字段。
2. **`tasks.payload` 改用 `BYTEA` 原样存储**：
   - 入站通过 `json.RawMessage` 原样截取请求载荷字节，经 `json.Valid` 校验合法性后绝不重序列化；
   - `payload_sha256` 幂等比对、PG `BYTEA` 落盘、`Endpoint.SignPayload` HMAC-SHA256 签名以及出站 HTTP 请求体全程使用同一份原始字节切片（`headers` 仍保留 `JSONB`）。

### 二、复合乐观版本围栏、重放轮次审计与可配置时间预算（修订 ADR-0003 / 0004 / 0005）

1. **复合乐观版本 `(replay_count, attempt)`**：不新增冗余版本列，Worker 在 Step B（过期/禁用转死信）与 Step E（投递结算）更新 `tasks` 时统一附加版本守卫：
   `WHERE id = $1 AND replay_count = $loaded_replay AND attempt = $loaded_attempt AND status IN ('PENDING', 'RETRYING')`。
2. **`task_attempts` 记录重放轮次**：`task_attempts` 表新增 `replay_count INT NOT NULL DEFAULT 0` 列，建立普通索引 `(task_id, replay_count, attempt_no)`——既精确区分各次重放轮次，又允许极端锁超时脑裂双发时落败方安全写入真实发生的 HTTP 审计行而不触发唯一键冲突。
3. **`deadline_at` 起算基准与默认退避曲线（支持环境变量覆盖）**：
   - `deadline_at` 默认设为**首次计划触发时间 `next_run_at + default_ttl`**（默认 `24h`，可通过环境变量覆盖；若入站显式指定则强制校验 `deadline_at > deliver_at`）；`Task.RecordOutcome` 计算的下次退避时间若已越过 `deadline_at`，当场直接转入 `DEAD_LETTER(TTL_EXPIRED)`；
   - 完整模式下默认 `max_attempts = 6`（含首次共 6 次尝试，最多退避 5 次），采用带 ±20% 均衡抖动的指数退避：`base = 5s, multiplier = 5, cap = 4h`（等待序列约 `5s -> 25s -> ~2m -> ~10m -> ~52m`，参数均支持通过环境变量覆盖以支撑秒级自动化验收）。

### 三、`Execution Lock` 四步闭环与延迟池 / Reconciler 时序统一（修订 ADR-0003 / 0004 / 0007）

1. **`Execution Lock` 四步协议**：
   - **提交后放锁**：锁生命周期覆盖 Step B ~ Step E 全过程，必须在 PG 结算事务提交并完成流控回报后才释放；
   - **持有人验主删锁**：加锁写入唯一随机 `token`，释放时通过 Lua 比对 `GET == token` 后再 `DEL`；
   - **抢锁争用保底顺延**：Worker `SET NX` 抢锁失败时，读取当前锁剩余过期时间（兜底 `+5s`）执行 `ZADD NX ferry:delayed <wake_at_ms> <task_id>` 埋下保底唤醒点后再确认 Kafka Offset，杜绝前任 Worker 强杀后重平衡消息被吞；
   - **`Cancel` 主动抢锁真互斥**：`POST /v1/tasks/{id}/cancel` 自身先执行 `SET NX` 抢占该任务的 `Execution Lock`（抢不到直接返回 `409 Conflict`），持锁期间执行 PG CAS 置 `CANCELLED`、清理延迟池并在 `defer` 中验主放锁，彻底消除“先探活再改库”的竞态窗口。
2. **延迟池分数比对删除与早到守卫**：
   - Forwarder 在单轮循环内持有局部 `lease_until_ms`，推 Kafka 确认后执行 Lua 条件删除——**仅当 `ZSCORE == lease_until_ms` 时才 `ZREM`**，保护期间被 Worker 新写入的顺延或退避计划；
   - Worker Step B 校验发现 `now < task.next_run_at` 时拦截退避期内抢跑，执行一次保底 `ZADD ferry:delayed <next_run_at_ms> <task_id>` 后提交 Offset。
3. **Reconciler 游标排空与归口延迟池**：
   - 每 `60s` 对账周期采用 `(next_run_at, id)` 游标循环分批扫描直至排空全部超期在途任务；
   - 发现任务在 Redis 中缺失时，不再直推 Kafka、也不再篡改 PG `next_run_at + 30s`，而是执行 `ZADD NX ferry:delayed <now_ms> <task_id>` 交由 Forwarder 按标准租约协议转发，保持 `NextRunAt` 语义绝对单一（Retry 路径遵循“先提交 PG 状态、再写延迟池”，极端断连由 Reconciler 兜底）。

### 四、`Outbox` 双通道防堵、固定分区键与流控“叫号机排期”加固（修订 ADR-0002 / 0004 / 0007）

1. **`Outbox` 双通道隔离与 `5s` 事务硬超时**：
   - 将索引升级为 `outbox_deliver_idx (deliver_at ASC, id ASC)`，拆分为**立即通道**（`deliver_at <= now()` 直推 Kafka）与**延迟通道**（`deliver_at > now()` 写 Redis，遇 Redis 故障暂停响应 `NOTIFY` 独立退避），凭 `FOR UPDATE SKIP LOCKED` 保证临界点绝不重搬；
   - 单批 `ClaimAndDispatch` 强制绑定 `5s` 超时 Context，超时立即中断回滚以保护 PG Autovacuum；消费方接口回调参数采用 Go 原生匿名 `func` 签名以保持零跨包耦合。
2. **固定 Kafka 分区键为 `task_id`**：统一按 `task_id` 哈希将大商户突发流量均匀打散至全部分区消费。
3. **流控虚拟排号（`next_free_ms`）、对称切口与 Lua 边界**：
   - **虚拟排号防空转**：限流超限与熔断 `OPEN` 期间，Lua 脚本按 `Endpoint` 的 `1 / rate_limit_qps` 维护虚拟空闲游标 `next_free_ms` 依次分时排号写入 `ferry:delayed`（若排号越过任务 `deadline_at` 则由 Worker 直接转入 `DEAD_LETTER(TTL_EXPIRED)`）；Worker 侧对 `Endpoint` 配置做 `3s` 本地短缓存；
   - **Lua 细节加固**：① `HALF_OPEN` 单试探名额附带 `now + timeout_ms + 5s` 过期时间防进程强杀锁死；② 收到带 `Retry-After` 的 `429`/`503` 时同步推迟整个 `Endpoint` 的流控冷却窗口；③ 脚本内部统一调用 `redis.call('TIME')` 消除多机时钟偏差；
   - **对称切口与原子性前提**：流控接口配对定义 `AdmitOrDefer` 与 `CompleteDelivery`（原子归还并发槽位并更新熔断/冷却）；明确单脚本跨 Key 原子性以**单实例/主从 Redis** 为前提（由真实 Redis 集成测试覆盖；若未来演进至 Redis Cluster 分片则拆分为准入判定与延迟池写入两步并由 Reconciler 兜底）。

### 五、Milestone 1/2 无倒挂切分与 `depguard` 白名单收紧（修订 ADR-0006 / 0007）

1. **消除里程碑依赖倒挂**：将 `Execution Lock` 前移至 **Milestone 1**；M1 严格界定为“立即单次直投闭环”（入口暂拒未来 `deliver_at` 返回 `400`、默认 `max_attempts = 1` 不产生悬空 `RETRYING`）；到 **Milestone 2** 随延迟池 Forwarder 与 Reconciler 落地，一并开放任意定时任务与默认 `6` 次多轮退避重试。
2. **`depguard` 编排层升级为 `strict` 白名单**：将 `internal/{api,scheduler,worker}` 从黑名单模式收紧为 `strict` 白名单并显式封禁 `database/sql`，杜绝绕过仓储切口直连底层驱动。

## 影响与取舍（Consequences & Trade-offs）

- **正向收益**：彻底封堵了非终态重复消息抢跑、ZSet 误删、取消竞态、Redis 故障堵死发件箱以及高积压限流空转风暴五类系统性隐患，使“慢/故障下游不拖垮健康下游”的核心立项承诺在极端积压下依然成立。
- **明确取舍**：Worker 本地 `3s` `Endpoint` 配置短缓存意味着流控配额或禁用开关变更最多存在 `3s` 传播延迟，对于异步投递治理完全可接受；`payload` 改为 `BYTEA` 后不再支持在 PG 内直接对载荷内部字段建 JSON 路径索引，符合 Ferry 将载荷视为不透明签名报文的设计定位。

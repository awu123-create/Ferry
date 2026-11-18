# Ferry 架构设计蓝图（Architecture Blueprint）

> **定位**：基于 **Go + PostgreSQL + Kafka + Redis** 构建的分布式异步 HTTP 任务调度与 Webhook 可靠投递平台。  
> **核心铁律**：**PostgreSQL 是唯一业务真相（Source of Truth），Kafka 负责高吞吐削峰流转，Redis 负责毫秒级时间调度与目标级流控加速。**

---

## 一、项目定位与演进背景（From `x-data` PG+River to Ferry）

在实习项目 `x-data` 的控制面中，异步任务采用的是 **PostgreSQL + River** 单库队列架构：控制面与业务运行表在同一个数据库事务内调用 `river.InsertTx` 插入任务，后台 Worker 轮询 PostgreSQL 领取任务，等待外部异步工作流结果时使用 `river.JobSnooze` 推迟下一次执行，队列并发数在启动配置中静态固定（如 `DecodeQueueWorkers = 2`）。

这种模式在内部同库、低外部不确定性的场景下十分干净；但当需要将其演进为全公司跨业务线共用的**通用异步 HTTP 任务调度与外部 Webhook 投递平台（Ferry）**时，单库 PG 队列会撞上三道物理瓶颈：

1. **跨业务线事务解耦**：不同上游微服务不可能直连 Ferry 的 PostgreSQL 去调 `InsertTx`，必须演进为独立 HTTP 网关 + 入口幂等约束 + 事务 Outbox 模式；
2. **调度与慢速派发解耦及水平扩缩容瓶颈**：单库轮询队列将“时间唤醒”与“慢 HTTP 阻塞调用”绑在同一层，既难以做分区级暂停背压，也限制了多实例消费吞吐——引入 **Kafka** 承接发件箱/延迟池与 Worker 之间的异步解耦削峰、多副本消费组水平扩展以及 Redis 故障时的分区级 `Pause` 背压阀门；
3. **不可信慢下游队头阻塞与高频流控落库写放大**：外部商户 Webhook 与内部慢接口的吞吐能力、健康度差异极大（有的承受 500 QPS，有的 20 QPS 即雪崩，有的 10 秒超时挂死），若将高频限流顺延（Deferral）、熔断计数与倒计时排期直接频繁 `UPDATE` 落回 PostgreSQL，会引发严重的行锁竞争与 MVCC 死元组膨胀——引入 **Redis** 承担 Endpoint 级动态限流、带超时并发槽位、自动熔断与毫秒级延迟调度，把海量无损顺延与流控状态变更全部消纳在内存层。

同时，Ferry 完整继承了 `x-data` 最核心的可靠性工程哲学：**领域表是唯一真相，队列只是调度载荷**——即使 Kafka 重复投递或 Redis 内存全部丢失，系统依然能以 PostgreSQL `tasks` 表为准自愈恢复，不丢单、不重复乱改终态。

---

## 二、工业界与开源方案对标（Benchmark & Trade-offs）

| 对标方案 | 借鉴采纳的核心机制 | 刻意裁剪/简化的能力与原因 |
| :--- | :--- | :--- |
| **River** (`riverqueue`) | 事务同落盘原子性思想（从 `InsertTx` 演进为 `tasks + outbox` 同事务）；显式可重试与不可重试错误区分 | 裁剪纯 PG 轮询队列、单库队列锁表与 `LISTEN/NOTIFY` 直接驱动高频任务消费，避免高频状态抖动冲击 PG |
| **Svix** (`svix-server`) | PG 真相落库 + Redis 调度分离架构；按 Endpoint 隔离防止慢商户拖垮健康商户（Noisy Neighbor Isolation）；HMAC-SHA256 防篡改签名头；逐次投递流水与 DLQ 重放 | 裁剪多租户组织鉴权、EventType 目录与 `1:N` 订阅扇出路由层（见 [ADR-0001](./adr/0001-unified-http-task-delivery-model.md)），首期聚焦点对点可靠 HTTP Push 主链路 |
| **Asynq & Hookdeck** | Redis ZSet 按时间戳 `score` 排布延迟任务与失败重试；**原子 Lua 租约推迟（Visibility Lease）防捞出崩溃丢单**；`Retry-After` 与连续失败熔断保护 | 裁剪 Go SDK 函数序列化协议（改用语言无关的 HTTP JSON 契约）以及 Hookdeck 复杂的在线 Payload Transform 脚本引擎 |

---

## 三、三组件职责边界与完整端到端数据流

### 3.1 存储与中间件职责矩阵

| 组件 | 承担职责 | 关键数据结构 / Topic / 表 | 数据丢失能否自愈 |
| :--- | :--- | :--- | :--- |
| **PostgreSQL** | 唯一业务真相、入口幂等防重、事务 Outbox、投递审计流水、状态机最终仲裁 | `endpoints`、`tasks`、`task_attempts`、`outbox` | 不可丢失（依赖 PG WAL 与持久化卷，单点真源） |
| **Kafka** (KRaft) | 调度与派发异步解耦、消费组水平扩缩容与分区级 `Pause` 背压（`acks=all`、至少一次投递） | Topic `ferry.tasks.ready`（多分区，固定按 `task_id` 哈希分区） | 可自愈（未搬运还在 `outbox`；已搬运未完成由 Reconciler 补回延迟池后重发） |
| **Redis** | 任意时刻延迟倒计时池、Endpoint 级三维流控（熔断/虚拟排号限流/并发槽位）、Worker 执行互斥锁 | `ferry:delayed` (ZSet)、`ferry:lock:task:<id>` (String NX PX)、`ferry:cb:<ep>` (Hash)、`ferry:rl:<ep>` (Hash)、`ferry:inflight:<ep>` (ZSet) | **可完全自愈**（即使 `FLUSHALL` 清空内存，`60s` PG Reconciler 游标流式扫描在途 `PENDING`/`RETRYING` 任务并回灌延迟池恢复） |

### 3.2 完整端到端数据流（ASCII 架构图）

```text
  [ Upstream Services ]
           |
           | POST /v1/tasks (Idempotency-Key, EndpointID, Payload, DeliverAt?)
           v
+---------------------------------------------------------------------------------+
|                         ferry-server (API + Control)                            |
|                                                                                 |
|  1. 校验 Endpoint 启用状态与合法 JSON，保留原始字节计算 payload_sha256            |
|  2. PG 单事务原子写入 (deadline_at 默认首次 next_run_at + 24h):                  |
|     - tasks (UNIQUE(endpoint_id, idempotency_key), payload=BYTEA, status=PENDING)|
|     - outbox (task_id, deliver_at)                                              |
|     - COMMIT 后触发 pg_notify('ferry_outbox', '')                                |
+---------------------------------------------------------------------------------+
           |                                                  ^
           v                                                  | (60s 游标对账兜底)
+----------------------------------+                          |
|  PostgreSQL (Source of Truth)    |<-------------------------+------------------+
|  [endpoints] [tasks] [attempts]  |                          |                  |
|  [outbox (deliver_at, id)]       |                          |                  |
+----------------------------------+                          |                  |
           |                                                  |                  |
           | Outbox Relay 双通道独立取数 (SKIP LOCKED LIMIT 100, 单批 5s 硬超时)  |
           v                                                  |                  |
   +-----------------------+                                  |                  |
   | 双通道按 deliver_at 拆 |                                  |                  |
   +-----------------------+                                  |                  |
     |                   |                                    |                  |
     | 立即通道 <= now()  | 延迟通道 > now()                    |                  |
     | (独立直推，无视Redis)| (Redis断连时独立退避，不堵立即通道) |                  |
     | (Kafka acks=all   |                                    | (Redis Pipeline   |
     |  确认后 DELETE)    +-----------------------------+      |  ZADD 后 DELETE) |
     v                                                 v      |                  |
+--------------------------+              +-------------------+----------------+ |
| Kafka: ferry.tasks.ready |<-------------| Redis ZSet: ferry:delayed          | |
| (固定按 task_id 分区)     |  100ms 轮询   | member=task_id, score=run_at_ms    | |
+--------------------------+  Lua 续租30s  +------------------------------------+ |
     |                    比对分数条件ZREM     ^                ^                |
     v                                        | 虚拟排号顺延    | 指数退避/429    |
+---------------------------------------------+----------------+--------------+ |
|                          ferry-worker (Kafka Consumer)                       | |
|                                                                              | |
|  Step A: SET ferry:lock:task:<id> <token> NX PX 抢执行锁 (争用则 ZADD NX 保底)| |
|  Step B: 查 PG tasks + 3s 短缓存 Endpoint；早于 next_run_at 则回写延迟池跳过；| |
|          若已终态直接确认；若禁用或超 deadline_at -> 乐观锁 CAS 转 DLQ        | |
|  Step C: Lua [admit_endpoint.lua] (AdmitOrDefer, Redis TIME):                | |
|          1) 熔断检查 (含 HALF_OPEN 探测超时) -> 2) QPS 虚拟排号 next_free_ms  | |
|          3) ZSet 并发槽位 -> 未通过则按 1/QPS 排号顺延 (超 deadline 直转 DLQ) | |
|  Step D: 原样发送 BYTEA 载荷 + X-Ferry-Task-ID + HMAC-SHA256 签名头           | |
|  Step E: PG 单事务写 task_attempts(含 replay_count) + (replay_count, attempt) | |
|          乐观版本 CAS 流转；提交后调 CompleteDelivery 归还槽位/更新熔断与冷却 | |
|          最后通过 Lua 校验 token 安全释放执行互斥锁                           | |
+------------------------------------------------------------------------------+ |
     |                                                                           |
     v                                                                           |
[ Target Webhook / Internal HTTP Service ]                                       |
(或内置混沌靶场 chaos-receiver :8081)                                            |
                                                                                 |
* Reconciler 旁路协程 (每 60s): 游标循环扫完 next_run_at < now()-60s 的在途行     |
  -> 查 Redis ZSCORE/LOCK -> 仍在 ZSet 则对齐 PG next_run_at；若已丢失则执行     |
  ZADD NX ferry:delayed <now_ms> <id> 塞回延迟池交由 Forwarder 转发! ------------+
```

### 3.3 有界回退与防空转证明（同时约束“环路频率”与“总存活时长”）

1. **环路频率有界（杜绝大积压空转风暴）**：当某下游限流（如 `20 QPS`）或触发熔断 `OPEN` 且瞬间涌入上万条任务时，`admit_endpoint.lua` 不采用固定 `1s` 盲目重试，而是维护每个 `Endpoint` 的虚拟空闲游标 `next_free_ms`，按 `1 / rate_limit_qps`（如每单 `+50ms`）将积压任务依次排号分摊至未来时间轴。每条受限任务仅在轮到自身配额槽位时被唤醒一次，彻底消除高频回灌对 Forwarder、共享 Kafka Topic 与 PG 读库的无效消耗。
2. **总存活时长有界（`deadline_at` 硬截断）**：每条任务在创建或重放时均记录 `deadline_at`（默认**首次 `next_run_at + 24h`**，支持环境变量覆盖）。无论在 Step B 唤醒校验、流控虚拟排号越界还是退避重试计算阶段，一旦越过 `deadline_at`，Worker 立即将其通过乐观版本 CAS 写为终态 `DEAD_LETTER`（`last_error = 'TTL_EXPIRED'`）并停止入池，从数学上保证调度环路绝对收敛。

---

## 四、任务生命周期状态机（持久化五态 + 复合乐观锁 + 执行锁互斥，详见 [ADR-0007](./adr/0007-tactical-ddd-and-deep-module-seams.md) 与 [ADR-0008](./adr/0008-concurrency-fencing-and-flow-control-hardening.md)）

```text
                   +-------------------+
                   |  POST /v1/tasks   |
                   | (Idempotent Init) |
                   +---------+---------+
                             |
                             v
     +------------------> [ PENDING ] ---------------------+
     |                       |                             |
     | (DLQ Replay:          | (持 Execution Lock + 准入;  | (POST /cancel 主动
     |  attempt=0,           |  禁用/过期直转 DLQ;          |  SET NX 抢到执行锁)
     |  replay_count+1,      |  不写 PG RUNNING)           v
     |  重置 deadline_at)    |                        [ CANCELLED ] (终态)
     |         +-------------+-------------+               ^
     |         |             |             |               |
     |      (2xx)      (408/429/5xx/   (非408/429的4xx,    |
     |         |        网络超时且       attempt耗尽,      |
     |         |        未超上限/截止)   超deadline_at,     |
     |         v             |         或Endpoint禁用)     |
     |   [ SUCCEEDED ]       v             |               |
     |      (终态)      [ RETRYING ] ------+---------------+ (POST /cancel 主动
     |                     |   |           |                  SET NX 抢到执行锁)
     |   (到期经 ZSet       |   +-----------+ (下次调度发现禁用/过期/上限)
     |    回灌后再次执行)    |               |
     |                     v               v
     +---------------(同上分支流转)   [ DEAD_LETTER ] (终态，允许受控重放)
```

### 状态迁移守卫规则

| 目标操作 / 事件 | 允许的前置状态 | 状态机乐观版本与互斥守卫行为 |
| :--- | :--- | :--- |
| Worker 标记投递成功 (`SUCCEEDED`) | `PENDING`、`RETRYING` | 单事务内追加 `task_attempts`（含当前 `replay_count`）并执行 `WHERE id = $1 AND replay_count = $2 AND attempt = $3 AND status IN ('PENDING','RETRYING')`；极端锁超时与取消重叠时跳过状态覆盖但保留真实发生的审计行 |
| Worker 标记失败重试 (`RETRYING`) | `PENDING`、`RETRYING` | 单事务追加 `task_attempts`、递增 `attempt`、写入退避后的 `next_run_at`（带复合乐观锁条件）；事务提交后再将 `task_id` 写入 `ferry:delayed`（极端断连由 Reconciler 兜底） |
| Worker 标记死信 (`DEAD_LETTER`) | `PENDING`、`RETRYING` | 不可重试 4xx（写 `task_attempts`）、超过 `max_attempts`、下次退避或当前已超 `deadline_at`、或 `Endpoint` 禁用（未发 HTTP 时不写 `task_attempts`）时通过复合乐观锁 CAS 落入，记录 `completed_at` |
| 手动取消 (`CANCELLED`) | `PENDING`、`RETRYING`（且抢占到 `Execution Lock`） | 自身先执行 `SET NX` 抢占 `Execution Lock`（抢不到说明正在执行，立即返回 `409 Conflict`）；持锁期间执行 `WHERE id = $1 AND status IN ('PENDING','RETRYING')` 置 `CANCELLED`、清理延迟池并验主释放锁 |
| 死信重放 (`DEAD_LETTER -> PENDING`) | `DEAD_LETTER` | 使用 `FOR UPDATE SKIP LOCKED` 加锁更新 `status='PENDING', attempt=0, replay_count=replay_count+1, next_run_at=now(), deadline_at=now()+default_ttl`, 并同事务写 `outbox` |

---

## 五、核心数据库表结构设计（PostgreSQL 16）

全系统严格收敛为 **4 张核心表**（迁移文件 `migrations/0001_init.sql`）：

```sql
CREATE TABLE endpoints (
    id                UUID PRIMARY KEY,
    name              TEXT NOT NULL,
    target_url        TEXT NOT NULL,
    signing_secret    TEXT NOT NULL DEFAULT '',
    rate_limit_qps    DOUBLE PRECISION NOT NULL DEFAULT 50 CHECK (rate_limit_qps > 0),
    max_concurrency   INT NOT NULL DEFAULT 20 CHECK (max_concurrency > 0),
    circuit_threshold INT NOT NULL DEFAULT 30 CHECK (circuit_threshold > 0),
    timeout_ms        INT NOT NULL DEFAULT 5000 CHECK (timeout_ms BETWEEN 100 AND 60000),
    enabled           BOOLEAN NOT NULL DEFAULT TRUE,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE tasks (
    id              UUID PRIMARY KEY,
    endpoint_id     UUID NOT NULL REFERENCES endpoints(id),
    idempotency_key TEXT NOT NULL,
    payload         BYTEA NOT NULL, -- 原样保存经合法性校验的原始 JSON 字节，保证哈希与签名 100% 保真
    payload_sha256  TEXT NOT NULL,
    headers         JSONB NOT NULL DEFAULT '{}'::jsonb,
    status          TEXT NOT NULL CHECK (status IN (
                        'PENDING', 'RETRYING',
                        'SUCCEEDED', 'DEAD_LETTER', 'CANCELLED'
                    )),
    attempt         INT NOT NULL DEFAULT 0,
    max_attempts    INT NOT NULL DEFAULT 6 CHECK (max_attempts BETWEEN 1 AND 20),
    next_run_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    deadline_at     TIMESTAMPTZ NOT NULL, -- 应用层默认按首次 next_run_at + 默认有效窗口(24h，环境变量可配)赋值
    last_error      TEXT NOT NULL DEFAULT '',
    replay_count    INT NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at    TIMESTAMPTZ
);

-- 1. 入口幂等唯一约束：同一 Endpoint 下 idempotency_key 绝对唯一
CREATE UNIQUE INDEX tasks_endpoint_idempotency_uq
    ON tasks (endpoint_id, idempotency_key);

-- 2. 部分索引（Reconciler 专用）：只索引在途任务，终态不进索引，防止千万级历史表扫描退化
CREATE INDEX tasks_reconcile_idx
    ON tasks (next_run_at ASC, id ASC)
    WHERE status IN ('PENDING', 'RETRYING');

-- 3. 部分索引（DLQ 列表与批量重放专用）：加速按 Endpoint 与创建时间筛选死信
CREATE INDEX tasks_dlq_idx
    ON tasks (endpoint_id, created_at DESC)
    WHERE status = 'DEAD_LETTER';

CREATE TABLE task_attempts (
    id               BIGSERIAL PRIMARY KEY,
    task_id          UUID NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    replay_count     INT NOT NULL DEFAULT 0,
    attempt_no       INT NOT NULL,
    http_status      INT NOT NULL DEFAULT 0,
    duration_ms      INT NOT NULL DEFAULT 0,
    error_kind       TEXT NOT NULL DEFAULT '',
    error_message    TEXT NOT NULL DEFAULT '',
    response_snippet TEXT NOT NULL DEFAULT '',
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX task_attempts_task_round_idx ON task_attempts (task_id, replay_count, attempt_no);

-- 事务发件箱表：Relay 双通道推入 Kafka 或 Redis ZSet 后即刻物理 DELETE，常驻行数接近 0
CREATE TABLE outbox (
    id         BIGSERIAL PRIMARY KEY,
    task_id    UUID NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    deliver_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 复合索引支撑立即通道 (deliver_at <= now()) 与延迟通道 (deliver_at > now()) 独立无阻塞扫描
CREATE INDEX outbox_deliver_idx ON outbox (deliver_at ASC, id ASC);

-- Outbox 插入后自动触发 NOTIFY，避免业务层手写 pg_notify SQL
CREATE OR REPLACE FUNCTION notify_ferry_outbox() RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify('ferry_outbox', '');
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER outbox_notify_trg
    AFTER INSERT ON outbox
    FOR EACH STATEMENT EXECUTE FUNCTION notify_ferry_outbox();
```

---

## 六、高并发与外部依赖故障推演

### 6.1 六大核心故障场景逐跳恢复路径

| 故障场景 | 破坏面与直接后果 | Ferry 逐跳自愈与保护机制 |
| :--- | :--- | :--- |
| **1. PG 写完任务与 Outbox，未推 Kafka 前 API/Relay 宕机** | 任务已返回 `202 Accepted`，但 Kafka `ferry.tasks.ready` 无消息 | `outbox` 行与 `tasks` 行在同一 PG 事务提交；任意存活或重启后的 `ferry-server` Relay 协程通过 `500ms` 兜底定时器执行 `FOR UPDATE SKIP LOCKED`（绑 `5s` 硬超时）捞起该记录推入 Kafka 并物理删除 `outbox` |
| **2. Redis 不可用（分两阶段）** | ① **断连拒绝服务期间**：无法读/写熔断、限流、信号量与 `ferry:delayed`；<br>② **宕机重启内存清空后**：ZSet 倒计时任务全部蒸发 | ① **断连期间双通道隔离与 Fail-Safe**：入口 `POST /v1/tasks` 只依赖 PG 照常接单；Outbox 立即通道（`deliver_at <= now()`）完全不碰 Redis 照常直推 Kafka，延迟通道独立退避不堵队头；Worker 检测到连续 Redis 失败立即**暂停拉取 Kafka** 并让出 `/readyz`；<br>② **清空恢复后自愈**：`60s` PG Reconciler 游标循环扫完 `next_run_at < now()-60s` 且不在 Redis 中的在途任务，通过 `ZADD NX` 批量回灌延迟池恢复转发，做到**零丢单** |
| **3. 下游持续超时（10s）或 5xx 风暴** | 单慢接口占满全局 HTTP 协程或制造空转风暴（Noisy Neighbor） | 1. 前 `max_concurrency` 个在途请求触发超时，连续可重试失败达 `circuit_threshold`（默认 `30`）后 `ferry:cb:<ep>` 切为 `OPEN 30s`（收到 `429 Retry-After` 同样推迟 Endpoint 冷却窗口）；<br>2. 后续到达的同 Endpoint 任务在 `admit_endpoint.lua` 微秒级被拒，不增 `attempt`，按 `1/rate_limit_qps` 虚拟排号游标 `next_free_ms` 均匀摊开写回 `ferry:delayed`，既释放 Worker 协程又消除高频空转；<br>3. 冷却后放行 **1 个带过期保护**的 `HALF_OPEN` 试探请求，通则恢复 `CLOSED`，不通继续 `OPEN`；超 `max_attempts` 或 `deadline_at` 安全落入 `DEAD_LETTER`，恢复后经 `POST /v1/dlq/replay` 平滑重放 |
| **4. Worker 收到 `SIGTERM` 优雅停机** | 在途 HTTP 请求被腰斩、已发完的 Offset 未提交、未发完的却先提交了 Offset | 1. 收到 `SIGTERM` 立即将 `/readyz` 置为 `503`（通知外部摘流）并停止从 Kafka `Poll` 新消息；<br>2. 等待 `WaitGroup` 排空所有正在执行 HTTP 请求的协程（设置 `25s` 优雅上限），让其完整写完 `task_attempts` 与 `tasks` 状态，调用 `CompleteDelivery` 归还并发槽位并验主释放执行锁；<br>3. 仅提交已完整处理的连续 Kafka Offset 后关闭连接退出；<br>4. 若超过 `25s` 仍未排空被迫退出，未提交 Offset 的消息在 Kafka Rebalance 后重发，且旧槽位将在 `timeout_ms + 5s` 自动过期释放 |
| **5. Worker 执行中遭遇 `kill -9` / OOM 强杀** | 未能执行清理并发槽位与删锁逻辑，且重平衡消息可能撞上未过期旧锁 | `ferry:inflight:<ep>` 槽位与 `HALF_OPEN` 试探名额均携带到期时间戳自动失效回收；新 Worker 收到重平衡消息若遇旧 `Execution Lock` 尚未过期，不丢弃消息而是通过 `ZADD NX` 在延迟池埋下保底唤醒点后再确认 Offset，锁过期后立即无缝续接，**绝不死锁或漏单** |
| **6. 下游已业务处理成功，但回传 `200 OK` 时断网** | Ferry 判定网络超时并按退避规则重新投递 | 遵循网络投递“至少一次”客观定理，每次投递强制携带同一 `X-Ferry-Task-ID` 与 `X-Ferry-Signature`，下游以 `X-Ferry-Task-ID` 为幂等键去重，保证业务结果“精确一次” |

### 6.2 最脆弱假设（Premise Collapse）与 10x 流量扩展瓶颈

- **最脆弱假设（Premise Collapse）**：
  - 本架构假设 **PostgreSQL 连接池与单库写入 IOPS 能承接并发入口写入与逐次 `task_attempts` 追加**，且 **Redis 处于可用状态时 Worker 才允许消费投递**。
  - 若业务演进至“部分低优通知允许在 Redis 故障时直接降级为本地内存限流”，可在 `Endpoint` 表增加 `fail_open_on_redis_down` 策略字段；当前为了优先保证脆弱下游不被打崩，坚持 Fail-Safe 暂停策略。
- **10x 流量扩展瓶颈（当吞吐从 2,000 TPS 冲击至 20,000+ TPS 时最先出现瓶颈的位置）**：
  1. **第一瓶颈：PG `tasks + task_attempts` 单库写 IOPS**——单次投递触发 `INSERT task_attempts` + `UPDATE tasks`。演进抓手：按周对 `tasks`/`task_attempts` 按 `created_at` 做 PostgreSQL 声明式分区，并将成功态的流水 `task_attempts` 转为内存环形缓冲批量 `COPY` 写入或只对失败重试落细节；
  2. **第二瓶颈：Redis 单实例热点 `ferry:delayed` ZSet 大 Key 与 Lua 单线程耗时**——当前 `AdmitOrDefer` 单脚本跨 Key 原子性建立在单实例/主从 Redis 前提下；当百万级延迟任务堆积在单个 ZSet，演进抓手：按 `hash(task_id) % 16` 将 `ferry:delayed` 拆分为 16 个分片 ZSet（`ferry:delayed:{0..15}`），若进一步演进至多机 Redis Cluster 则将准入判定与延迟池写入拆为两步并由 Reconciler 兜底；
  3. **第三瓶颈：单 Kafka Topic 分区并发度上限**——演进抓手：将 `ferry.tasks.ready` 分区数从初始 `12` 扩容并配合多副本 `ferry-worker` 水平扩缩。

---

## 七、技术栈与工程化底座（Tech Stack & Tooling）

### 7.1 核心技术栈选型（详情见 [ADR-0006](./adr/0006-tech-stack-and-engineering-tooling.md)）

| 层级 | 定稿选型 | 关键配置与工程职责 |
| :--- | :--- | :--- |
| **Go 版本** | **`go 1.27`** | `go.mod` 与 `.golangci.yml` 统一锁定 `1.27`，升级 `golangci-lint v2` 为 `go1.27` 编译版以对齐工具链 |
| **ORM 数据访问** | **`gorm.io/gorm` + `gorm.io/driver/postgres` + `gorm.io/datatypes`** | 全量接管业务 CRUD 与事务（底层复用 `pgx/v5`）；开启 `SkipDefaultTransaction: true` 与 `PrepareStmt: true` |
| **数据库迁移** | **`pressly/goose/v3`**（`embed.FS` + CLI） | 唯一 DDL 迁移入口（管建表、部分索引、约束与 `outbox` 触发器）；**严禁使用 GORM `AutoMigrate`** |
| **主键生成** | **`google/uuid`**（`uuid.NewV7()`） | 按时间戳递增有序，防止高并发 `UUIDv4` 随机写引发 PG B-Tree 索引页分裂 |
| **HTTP 路由与投递** | **标准库 `net/http`**（Go 1.22+ 增强 `ServeMux` + 定制 `http.Transport`） | 零 Web 框架承载 REST API；定制连接池（`MaxIdleConns: 1000`、`MaxIdleConnsPerHost: 64`）与 `io.LimitReader` 防大响应体 OOM |
| **Kafka 客户端** | **`twmb/franz-go`**（含 `pkg/kadm`） | 纯 Go 零 CGO，默认开启幂等生产者，支持 `CooperativeSticky` 重平衡与分区级 `PauseFetchPartitions` 背压 |
| **Redis 客户端** | **`redis/go-redis/v9`** | 驱动 `ferry:delayed` ZSet、Pipeline、短锁与 `redis.NewScript`（自动 `EVALSHA` / `NOSCRIPT` 重载） |
| **结构化日志** | **`go.uber.org/zap`** | JSON 格式输出；启用 `zapcore.NewSamplerWithOptions` 防下游雪崩刷爆磁盘 I/O，并实现 GORM `logger.Interface` 接管慢 SQL 阈值告警与 `task_id` 透传 |
| **配置、组装与指标** | **纯环境变量结构体 + `main.go` 显式组装 + `prometheus/client_golang`** | 12-Factor 强类型环境变量解析（零 `viper`/`fx`），优雅停机逆序显式可控，独立暴露 `/metrics` |
| **本地容器栈** | `postgres:16-alpine` + `apache/kafka:3.9.0` (KRaft) + `redis:7.4-alpine` (`--maxmemory-policy noeviction`) + `prom/prometheus` + `grafana/grafana` | Docker Compose 一键编排，无 ZooKeeper |

### 7.2 GORM + Goose 协作边界

1. **DDL 归 Goose，DML 归 GORM**：业务层不手写裸 SQL 字符串，所有表结构、部分索引（`tasks_reconcile_idx`、`tasks_dlq_idx`）及 `outbox_notify_trg` 触发器由 `goose` 管理；
2. **并发原语走 GORM `clause`**：入口幂等用 `clause.OnConflict{DoNothing: true}`，Outbox 抢占与死信批量重放用 `clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}`，状态机用条件 `Updates` 校验 `RowsAffected`；
3. **Outbox 低延迟监听**：`outbox` 插入通知由迁移触发器 `notify_ferry_outbox()` 自动发出，Go 端 Relay 直接从 `gorm.io/driver/postgres` 底层复用的 `pgx` 连接调用 `WaitForNotification(ctx)` 唤醒，无需额外驱动。

### 7.3 工程化质量门禁

- **`Makefile`**：当前骨架阶段封装 `setup-hooks`、`fmt`、`lint`、`test`、`build`；`test-integration`、`migrate`、`up`、`down` 随 Milestone 1 容器与迁移脚本同步落地；
- **`.golangci.yml`（v2）**：强制开启 `govet`、`staticcheck`、`errcheck`、`errorlint`、`bodyclose`、`noctx`、`copyloopvar`、`durationcheck`、`gocritic`、`nolintlint` 与 `depguard`（对 `domain` 与 `{api,scheduler,worker}` 启用 `strict` 白名单门禁，锁死零跨层越界依赖）；
- **`.pre-commit-config.yaml`**：基础文本/YAML/私钥检查、`adr-number-unique`（校验 `docs/adr/NNNN-*.md` 编号唯一）、`gofmt`、`golangci-lint` 增量扫描与 `go test -short ./...`；
- **GitHub Actions CI（`.github/workflows/ci.yml`）**：当前骨架阶段运行 `verify`（格式、ADR 编号、全量 lint、竞态单测与二进制构建）；`integration`（基于 `deploy/docker-compose.yml` 跑 `goose` 迁移与真实 PG/Kafka/Redis 集成测试）随 Milestone 1 同步启用。

---

## 八、分阶段实施路线图（Milestone 1 ~ 4，阶段独立可运行、可验收）

严格遵循 **“每个 Milestone 独立可用、可编译运行、有明确自动化验收脚本，且后端核心先于极简前端控制台”** 原则：

### 固定端口与模块拓扑规划

- **服务端口矩阵**：
  - `ferry-server`（API + Relay + Forwarder + Reconciler）：REST API `:8080`、Prometheus `/metrics` `:2112`
  - `ferry-worker`（Kafka 投递 Worker，支持与 server 独立进程或多实例部署）：健康探针 `/readyz` `:8082`、Prometheus `/metrics` `:2113`
  - `chaos-receiver`（内置混沌下游靶场）：`:8081`
  - 基础设施容器：PostgreSQL `:5432`、Kafka (KRaft) `:9092`、Redis `:6379`、Prometheus `:9090`、Grafana `:3000`
- **最终代码目录规划**：
  ```text
  Ferry/
  ├── GLOSSARY.md              # 统一语言词表（单一限界上下文，零基础设施实现泄漏）
  ├── cmd/
  │   ├── ferry-server/        # 组装根：HTTP API 网关 + Outbox Relay + Delayed Forwarder + PG Reconciler
  │   ├── ferry-worker/        # 组装根：Kafka 消费者 + 流控准入 + HTTP 签名投递 + 优雅停机
  │   ├── chaos-receiver/      # 混沌故障注入靶场 (模拟延迟/500/429/验签)
  │   └── ferry-bench/         # 并发压测与隔离性验证 CLI
  ├── internal/
  │   ├── domain/              # 纯 Go 领域内核：Endpoint/Task 聚合、DeliveryAttempt、值对象、签名与退避策略
  │   ├── api/                 # 入站 HTTP 控制面深模块：ServeMux 路由、DTO 校验、直接编排幂等创建/取消/重放
  │   ├── scheduler/           # 后台调度深模块：OutboxRelay、DelayedForwarder、Reconciler
  │   ├── worker/              # 投递执行深模块：Step A~E 状态机、25s 优雅停机排空与 Redis 断连背压暂停
  │   ├── infra/               # 出站基础设施适配器环（消费方接口实现，禁止向上依赖）
  │   │   ├── pg/              # GORM 仓储（私有 PO、同事务 Outbox、SKIP LOCKED、Goose 迁移挂载）
  │   │   ├── kafka/           # twmb/franz-go 幂等生产者/消费者封装（含分区 Pause/Resume）
  │   │   ├── redis/           # go-redis/v9 适配器（延迟池租约 Lua、admit_endpoint.lua 流控、Execution Lock）
  │   │   └── httpclient/      # 调优 http.Transport 连接池、LimitReader 截断与 Retry-After 解析
  │   ├── config/              # 12-Factor 强类型环境变量解析
  │   ├── logger/              # zap JSON 日志、Sampler 采样限流与 GORM 慢 SQL Logger 适配
  │   └── metrics/             # Prometheus 指标定义
  ├── migrations/              # 0001_init.sql 与 embed.go（暴露 embed.FS 供 Goose 挂载）
  ├── deploy/                  # docker-compose.yml, prometheus.yml, grafana/dashboards/
  ├── scripts/                 # verify_m1.sh ~ verify_m4.sh 可重复自动化验收脚本
  └── web/                     # Milestone 4 独立静态控制台 (仅消费 /v1/* REST API)
  ```
- **模块分层与单向依赖边界**（详见 [ADR-0007](./adr/0007-tactical-ddd-and-deep-module-seams.md)、[ADR-0008](./adr/0008-concurrency-fencing-and-flow-control-hardening.md) 与 [GLOSSARY.md](../GLOSSARY.md)）：
  - **零透传原则**：不设立浅层 `internal/service` 包，由 `api`、`scheduler`、`worker` 直接消费 `domain` 与消费方窄接口；`worker` 通过纯方法 `Ready() bool` 交由组装根 `cmd/ferry-worker` 暴露 `/readyz`；
  - **DO/PO 隔离与隐藏式 Outbox**：`domain` 零 ORM/JSON 标签；`gorm` PO 结构体私有化于 `internal/infra/pg` 内，并以 `CreateTaskIdempotent`、`SaveDeliveryOutcome`、`ReplayDeadLetters` 与绑定 `5s` 硬超时的双通道回调切口 `ClaimAndDispatchOutbox` 隐藏全部事务与发件箱细节；
  - **依赖方向门禁**：仅 `cmd/*` 可同时引用编排模块与 `internal/infra/*` 完成组装与编译期接口断言，其余各层通过 `.golangci.yml` `depguard` 的 `strict` 白名单与跨包隔离规则硬性拦截。

---

### Milestone 1：最小端到端可靠立即直投闭环（PG + Outbox 立即通道 + Kafka + 执行锁 + 验签靶场）

- **独立可交付目标**：系统已可作为“立即可靠单次 HTTP 投递网关”零悬空独立运行——支持创建 Endpoint、原始字节保真幂等创建立即任务（暂拒未来 `deliver_at`，默认 `max_attempts = 1` 不产生悬空 `RETRYING`）、Outbox 立即通道直推 Kafka（固定 `task_id` 分区键）、Redis `Execution Lock` 防重与 `POST /cancel` 抢锁互斥、Worker 签名直投下游记录 `task_attempts`、以及 `SIGTERM` 优雅停机。
- **核心模块与表结构**：
  - `deploy/docker-compose.yml`（启动 PostgreSQL 16 + Kafka KRaft + Redis 7 基础编排）
  - `migrations/0001_init.sql` + `embed.go`（建齐 4 张核心表与全套索引，含 `payload BYTEA`、`task_attempts.replay_count`、`outbox(deliver_at, id)` 复合索引，后续 Milestone 零破坏性改表）
  - `internal/infra/redis`（基础版：提供带持有人 `token` 验主释放的 `Execution Lock`，同时服务于 Worker 执行互斥与 API `Cancel` 抢锁互斥）
  - `cmd/ferry-server`（API：`POST/GET /v1/endpoints`、`POST/GET /v1/tasks`、`POST /v1/tasks/{id}/cancel`、`/healthz`、`/readyz`；内置 Outbox 立即通道处理 `deliver_at <= now()` 直推 Kafka）
  - `cmd/ferry-worker`（Kafka 消费、`Execution Lock` + 复合乐观锁守卫、HMAC-SHA256 原始字节签名投递、写 `task_attempts`、2xx 迁移 `SUCCEEDED` / 失败迁移 `DEAD_LETTER`、25s `SIGTERM` 优雅停机）
  - `cmd/chaos-receiver`（基础版：校验 `X-Ferry-Signature` 签名正确性，记录 `X-Ferry-Task-ID` 收到次数，支持返回指定状态码 `200`/`400`）
- **可运行验收命令（`scripts/verify_m1.sh`）**：
  ```bash
  docker compose -f deploy/docker-compose.yml up -d postgres kafka redis
  go test -race ./...
  # 自动化脚本依次启动 chaos-receiver(:8081)、ferry-server(:8080)、ferry-worker(:8082) 并验证：
  # 1. 创建带 signing_secret 的 Endpoint
  # 2. 携带同一 Idempotency-Key 并发发 10 次 POST /v1/tasks -> 仅生成 1 条任务，异载荷返回 409
  # 3. chaos-receiver 验证收到带合法 HMAC 签名的原始字节请求，PG tasks.status == 'SUCCEEDED'，task_attempts == 1
  # 4. 发一条指向返回 400 的任务 -> 立即进 DEAD_LETTER 且 attempt == 1
  # 5. 验证在途持锁期间调用 POST /cancel 返回 409 Conflict，向 ferry-worker 发 SIGTERM 确认在途任务完成后安全退出
  ./scripts/verify_m1.sh
  ```

---

### Milestone 2：任意延迟调度、指数退避重试、PG 游标对账自愈与死信批量重放

- **独立可交付目标**：打通全时间维度与灾难自愈闭环——开放任意未来 `deliver_at` 定时任务、默认 `6` 次指数退避（`5s -> 25s -> ~2m -> ~10m -> ~52m` ±20% 抖动，支持环境变量覆盖加速测试）与 `Retry-After` 解析、Forwarder 30s 租约与比对分数条件 `ZREM`、Redis 内存全清空后的 `60s` PG Reconciler 游标流式回灌延迟池恢复，以及单条/按条件分批死信重放。
- **核心模块增量**：
  - `internal/infra/redis` + `internal/scheduler`：`claim_delayed.lua`（30s `Schedule Lease`）与比对分数条件删除 `ack_delayed.lua`、Outbox 延迟通道（`deliver_at > now()` 写延迟池，遇 Redis 故障独立退避）、抢锁争用 `ZADD NX` 保底顺延
  - `internal/domain` + `internal/infra/httpclient`：可配置指数退避策略与 `Retry-After` 响应头解析，更新 `RETRYING` 提交后再写 `ferry:delayed`
  - `internal/scheduler`：`60s`（支持环境变量配置如验收期 `2s` 加速验证）PG Reconciler，游标循环扫完超期在途任务，识别顺延时间对齐并以 `ZADD NX` 补回丢失调度
  - `internal/api` + `internal/infra/pg`：`POST /v1/tasks/{id}/replay` 与 `POST /v1/dlq/replay`（`FOR UPDATE SKIP LOCKED LIMIT <= 500` 分批重置 `attempt=0, replay_count+1` 并同事务写 `outbox`）
  - `cmd/chaos-receiver` 扩展：支持按次数/概率返回 `500`、`429`（附带 `Retry-After: 2`）及固定延迟
- **可运行验收命令（`scripts/verify_m2.sh`）**：
  ```bash
  go test -race ./...
  # 自动化脚本（通过环境变量将基础退避与对账周期覆写为秒级）验证：
  # 1. 提交 deliver_at=+3s 定时任务 -> 确认在 3s 前未投递，3~4s 间精准变为 SUCCEEDED
  # 2. 目标前 2 次返回 500、第 3 次返回 200 -> 验证指数退避后成功，task_attempts 记录 3 行流水
  # 3. 故意耗尽 max_attempts=2 落入 DEAD_LETTER -> 调 POST /v1/dlq/replay 批量重放并成功投递，replay_count == 1
  # 4. 灾难演练：提交 20 条 +5s 延迟任务，立即执行 docker compose exec redis redis-cli FLUSHALL 清空内存 -> Reconciler 游标扫描自动塞回延迟池并全部投递成功（0 丢单）
  ./scripts/verify_m2.sh
  ```

---

### Milestone 3：Endpoint 三维流控虚拟排号、Prometheus/Grafana 可观测闭环与混沌压测

- **独立可交付目标**：交付完整生产级流控保护与秋招简历级压测监控数据——完成基于 Redis `TIME` 的熔断器（含半开探测超时）/ `1/QPS` 虚拟排号令牌桶 / 防泄漏并发槽位与对称 `CompleteDelivery` 结算、Worker `Endpoint` 本地短缓存、Redis 断连 Fail-Safe 背压保护、全套 Prometheus/Grafana 看板与多场景混沌压测报告脚本。
- **核心模块增量**：
  - `internal/infra/redis`（`admit_endpoint.lua` + `complete_delivery.lua`）+ `internal/worker`：`AdmitOrDefer` 原子准入与按 `1/QPS` 虚拟排号摊平顺延、`CompleteDelivery` 对称归还槽位并更新熔断/`429` 冷却、`Endpoint` `3s` 短缓存；Worker 检测 Redis 断连自动停拉 Kafka 并通过 `Ready()` 置 `/readyz=503`
  - `internal/metrics` + `deploy/prometheus.yml` + `deploy/grafana/dashboards/ferry.json`：完整采集 Outbox 积压、ZSet 深度、Kafka Lag、各 Endpoint QPS/耗时分布（P50/P95/P99）、限流顺延数与熔断状态变更
  - `cmd/chaos-receiver` 完整混沌模式 + `cmd/ferry-bench` 压测发压器
- **可运行验收命令（`scripts/verify_m3.sh`）**：
  ```bash
  docker compose -f deploy/docker-compose.yml up -d
  go test -race ./...
  # 自动化压测与混沌隔离验收：
  # 1. 同时压测健康 Endpoint-A（QPS=200，50ms响应）与故障 Endpoint-B（模拟 3s 慢响应 + 100% 500）
  # 2. 验证 Endpoint-B 触发熔断 OPEN（ferry_endpoint_circuit_state == 1），Worker 协程不被占满
  # 3. 验证 Endpoint-A 的 P99 投递延迟不受 Endpoint-B 拖累，且超出 rate_limit_qps 的请求平滑回退不超限
  # 4. 校验 http://localhost:2112/metrics 与 :2113/metrics 核心指标非零输出，Grafana (:3000) 面板正常拉取
  ./scripts/verify_m3.sh
  ```

---

### Milestone 4（最后阶段低复杂度收尾）：极简只读与死信重放控制台

- **独立可交付目标**：在不侵入后端核心代码的独立目录 `web/`（建议独立 Git Worktree 并行生成）内，仅通过冻结的 `/v1/*` REST API 构建极简静态单页控制台，方便本地演示、死信人工排查与一键批量重放操作截图。
- **页面与能力边界（仅 4 个核心视图，零复杂前端构建依赖或轻量纯静态）**：
  1. **系统总览**：展示各状态任务统计与核心 Endpoint 健康度摘要；
  2. **Endpoint 管理**：查看/编辑 `rate_limit_qps`、`max_concurrency`、`circuit_threshold`、`timeout_ms`；
  3. **任务与流水时间线**：按 `status` / `endpoint_id` 检索任务，展开单条任务的 `task_attempts` 逐次耗时、HTTP 状态码与响应片段；
  4. **死信队列批量重放**：勾选或按时间窗筛选 `DEAD_LETTER`，触发 `POST /v1/dlq/replay` 查看重放进度。
- **可运行验收命令（`scripts/verify_m4.sh`）**：
  ```bash
  # 启动全套服务端（内嵌或反向代理 web/ 静态资源）并校验页面可达与接口联调闭环
  ./scripts/verify_m4.sh
  ```

---

## 九、统一语言与架构决策索引（Glossary & ADR Index）

- [统一语言词表（`GLOSSARY.md`）](../GLOSSARY.md)
- [ADR-0001: 统一核心业务模型为可靠 HTTP 任务投递引擎](./adr/0001-unified-http-task-delivery-model.md)
- [ADR-0002: 入口事务一致性采用 PG 同事务 Outbox 与 Go 批量搬运协程](./adr/0002-transactional-outbox-batch-relay.md)
- [ADR-0003: 延迟与退避重试采用 Redis ZSet 租约倒计时池加 PG 对账兜底](./adr/0003-redis-zset-lease-and-pg-reconciler.md)
- [ADR-0004: Endpoint 级三维流控隔离与全链路三层幂等机制](./adr/0004-endpoint-flow-control-and-three-layer-idempotency.md)
- [ADR-0005: 状态机与投递明细、死信受控重放及可观测优先交付边界](./adr/0005-state-machine-dlq-replay-and-observability-boundary.md)
- [ADR-0006: Go 1.27 技术栈选型、GORM+Goose 职责边界与工程化门禁](./adr/0006-tech-stack-and-engineering-tooling.md)
- [ADR-0007: 轻量战术 DDD 聚合边界、五态持久化修正与深模块切口设计](./adr/0007-tactical-ddd-and-deep-module-seams.md)
- [ADR-0008: 调度并发围栏、流控排号防空转与载荷字节保真加固](./adr/0008-concurrency-fencing-and-flow-control-hardening.md)

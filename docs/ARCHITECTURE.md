# Ferry 架构设计蓝图（Architecture Blueprint）

> **定位**：基于 **Go + PostgreSQL + Kafka + Redis** 构建的分布式异步 HTTP 任务调度与 Webhook 可靠投递平台。  
> **核心铁律**：**PostgreSQL 是唯一业务真相（Source of Truth），Kafka 负责高吞吐削峰流转，Redis 负责毫秒级时间调度与目标级流控加速。**

---

## 一、项目定位与演进背景（From `x-data` PG+River to Ferry）

在实习项目 `x-data` 的控制面中，异步任务采用的是 **PostgreSQL + River** 单库队列架构：控制面与业务运行表在同一个数据库事务内调用 `river.InsertTx` 插入任务，后台 Worker 轮询 PostgreSQL 领取任务，等待外部异步工作流结果时使用 `river.JobSnooze` 推迟下一次执行，队列并发数在启动配置中静态固定（如 `DecodeQueueWorkers = 2`）。

这种模式在内部同库、低外部不确定性的场景下十分干净；但当需要将其演进为全公司跨业务线共用的**通用异步 HTTP 任务调度与外部 Webhook 投递平台（Ferry）**时，单库 PG 队列会撞上三道物理瓶颈：

1. **跨业务线事务解耦**：不同上游微服务不可能直连 Ferry 的 PostgreSQL 去调 `InsertTx`，必须演进为独立 HTTP 网关 + 入口幂等约束 + 事务 Outbox 模式；
2. **高频流转写放大与 MVCC 膨胀**：成千上万的任务进出队列、退避重试、限流顺延如果全部直接频繁 `UPDATE` PG 队列表行，行锁竞争与 Vacuum 抖动会直接拖垮核心状态查询——引入 **Kafka** 承接高吞吐即时调度削峰；
3. **不可信外部下游与静态并发瓶颈**：外部商户 Webhook 与内部慢接口的吞吐能力、健康度差异极大（有的承受 500 QPS，有的 20 QPS 即雪崩，有的 10 秒超时挂死），静态全局并发池会被慢下游拖成队头阻塞——引入 **Redis** 承担 Endpoint 级动态限流、带租约并发信号量、连续失败熔断与任意时刻倒计时调度。

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
| **Kafka** (KRaft) | 高吞吐即时任务缓冲与并发消费削峰（`acks=all`、至少一次投递） | Topic `ferry.tasks.ready`（多分区，按 `endpoint_id` 或 `task_id` 哈希分区） | 可自愈（未搬运还在 `outbox`；已搬运未完成由 Reconciler 从 PG `tasks` 补推） |
| **Redis** | 任意时刻延迟倒计时池、Endpoint 级三维流控（熔断/QPS/并发信号量）、Worker 执行短锁 | `ferry:delayed` (ZSet)、`ferry:lock:task:<id>` (String NX PX)、`ferry:cb:<ep>` (Hash)、`ferry:rl:<ep>` (Hash)、`ferry:inflight:<ep>` (ZSet) | **可完全自愈**（即使 `FLUSHALL` 清空内存，PG Reconciler 在 60s 内全量捞回在途 `PENDING`/`RETRYING` 任务） |

### 3.2 完整端到端数据流（ASCII 架构图）

```text
  [ Upstream Services ]
           |
           | POST /v1/tasks (Idempotency-Key, EndpointID, Payload, DeliverAt?)
           v
+---------------------------------------------------------------------------------+
|                         ferry-server (API + Control)                            |
|                                                                                 |
|  1. 校验 Endpoint 启用状态，计算 payload_sha256                                  |
|  2. PG 单事务原子写入:                                                           |
|     - tasks (UNIQUE(endpoint_id, idempotency_key), status=PENDING, deadline_at) |
|     - outbox (task_id, deliver_at)                                              |
|     - COMMIT 后触发 pg_notify('ferry_outbox', '')                                |
+---------------------------------------------------------------------------------+
           |                                                  ^
           v                                                  | (60s 低频对账兜底)
+----------------------------------+                          |
|  PostgreSQL (Source of Truth)    |<-------------------------+------------------+
|  [endpoints] [tasks] [attempts]  |                          |                  |
|  [outbox]                        |                          |                  |
+----------------------------------+                          |                  |
           |                                                  |                  |
           | Outbox Relay (NOTIFY唤醒 + 500ms轮询)             |                  |
           | SELECT ... FOR UPDATE SKIP LOCKED LIMIT 100      |                  |
           v                                                  |                  |
   +---------------+                                          |                  |
   | deliver_at ?  |                                          |                  |
   +---------------+                                          |                  |
     |           |                                            |                  |
     | <= now()  | > now()                                    |                  |
     |           +-------------------------------------+      |                  |
     | (Kafka Producer acks=all, 确认后 DELETE outbox) |      | (Redis Pipeline   |
     v                                                 v      |  ZADD 后 DELETE) |
+--------------------------+              +-------------------+----------------+ |
| Kafka: ferry.tasks.ready |<-------------| Redis ZSet: ferry:delayed          | |
| (即时投递队列)            |  100ms 轮询   | member=task_id, score=run_at_ms    | |
+--------------------------+  Lua 续租30s  +------------------------------------+ |
     |                        推Kafka后ZREM      ^                ^              |
     |                                           |                |              |
     v                                           | 限流/熔断顺延    | 指数退避/429  |
+------------------------------------------------+----------------+------------+ |
|                          ferry-worker (Kafka Consumer)                       | |
|                                                                              | |
|  Step A: SET ferry:lock:task:<id> NX PX (timeout+15s) 抢占执行短锁            | |
|  Step B: 查 PG tasks + endpoints，校验状态非终态；若 now() > deadline_at -> DLQ | |
|  Step C: 执行 Redis Lua [admit_endpoint.lua]:                                 | |
|          1) 熔断器 CLOSED/HALF_OPEN 检查 (OPEN 则返回 open_until+jitter)       | |
|          2) QPS 令牌桶检查 (超限则返回 now+1s+jitter)                          | |
|          3) ZSet 并发信号量 (清理过期槽位后校验 ZCARD < max_concurrency)         | |
|          -> 若任一未通过：ZADD ferry:delayed 顺延 (不增 attempt)，直接 Commit!   | |
|  Step D: 拼装 HTTP POST + X-Ferry-Task-ID + X-Ferry-Timestamp/Signature       | |
|          发起 HTTP 调用 (defer ZREM ferry:inflight & DEL lock)                | |
|  Step E: PG 事务记录 task_attempts + 状态机流转 (SUCCEEDED/RETRYING/DEAD_LETTER)| |
|          并更新 Redis 熔断计数 (2xx 归零闭合; 5xx/超时累加触发 OPEN 30s)        | |
+------------------------------------------------------------------------------+ |
     |                                                                           |
     v                                                                           |
[ Target Webhook / Internal HTTP Service ]                                       |
(或内置混沌靶场 chaos-receiver :8081)                                            |
                                                                                 |
* Reconciler 旁路协程 (每 60s): 扫描 PG 中 status IN ('PENDING','RETRYING')       |
  且 next_run_at < now()-60s 的行 -> 查 Redis ZSCORE/LOCK -> 若仍在 ZSet 则对齐    |
  PG next_run_at，若已丢失则直接补推 Kafka! --------------------------------------+
```

### 3.3 有界回退环路证明（防止 `Kafka <-> Redis ZSet` 无限空转）

如果下游持续限流或永久熔断，任务在 `Kafka -> Worker -> admit_endpoint.lua 拒绝 -> ferry:delayed -> Forwarder -> Kafka` 之间回退，**不会递增业务重试计数 `attempt`**，但每条任务都在 PG 中持有硬性的 `deadline_at`（默认 `created_at + 24h`，单次或批量死信重放时重置为 `now() + 24h`）。Worker 在准入前优先校验 `now() > task.deadline_at`：一旦过期，立即写为终态 `DEAD_LETTER`（`last_error = 'TTL_EXPIRED'`）并停止回灌，从数学上保证调度环路严格有界。

---

## 四、任务生命周期状态机

```text
                   +-------------------+
                   |  POST /v1/tasks   |
                   | (Idempotent Init) |
                   +---------+---------+
                             |
                             v
     +------------------> [ PENDING ] ---------------------+
     |                       |                             |
     | (DLQ Replay:          | (Worker 抢锁 + 准入通过)     | (POST /cancel)
     |  attempt=0,           v                             v
     |  replay_count+1,   [ RUNNING ]                 [ CANCELLED ] (终态)
     |  重置 deadline_at)    |                             ^
     |         +-------------+-------------+               |
     |         |             |             |               |
     |      (2xx)      (408/429/5xx/   (非408/429的4xx,    |
     |         |        网络超时且       或 attempt 耗尽,    |
     |         |        未超上限)       或超 deadline_at)   |
     |         v             |             |               |
     |   [ SUCCEEDED ]       v             |               |
     |      (终态)      [ RETRYING ] ------+---------------+ (POST /cancel)
     |                     |   |           |
     |   (到期经 ZSet       |   +-----------+ (下次调度发现超时/已达上限)
     |    回灌后再次准入)    |               |
     |                     v               v
     +---------------(返回 RUNNING)   [ DEAD_LETTER ] (终态，允许受控重放)
```

### 状态迁移守卫规则

| 目标操作 / 事件 | 允许的前置状态 | 状态机 CAS 守卫行为 |
| :--- | :--- | :--- |
| Worker 标记投递成功 (`SUCCEEDED`) | `PENDING`、`RUNNING`、`RETRYING` | `WHERE id = $1 AND status IN ('PENDING','RUNNING','RETRYING')`；若已被他人处理为终态，直接跳过不覆盖 |
| Worker 标记失败重试 (`RETRYING`) | `PENDING`、`RUNNING`、`RETRYING` | 递增 `attempt`、写入退避后的 `next_run_at`，同时将 `task_id` 写入 `ferry:delayed` |
| Worker 标记死信 (`DEAD_LETTER`) | `PENDING`、`RUNNING`、`RETRYING` | 不可重试 4xx、超过 `max_attempts` 或超过 `deadline_at` 时落入，记录 `completed_at` |
| 手动取消 (`CANCELLED`) | `PENDING`、`RETRYING` | 仅在非在途且非终态时允许更新；若已 `RUNNING` 或已进入终态，API 返回 `409 Conflict` |
| 死信重放 (`DEAD_LETTER -> PENDING`) | `DEAD_LETTER` | 使用 `FOR UPDATE SKIP LOCKED` 加锁更新 `status='PENDING', attempt=0, replay_count=replay_count+1, deadline_at=now()+'24h'`, 并同事务写 `outbox` |

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
    payload         JSONB NOT NULL,
    payload_sha256  TEXT NOT NULL,
    headers         JSONB NOT NULL DEFAULT '{}'::jsonb,
    status          TEXT NOT NULL CHECK (status IN (
                        'PENDING', 'RUNNING', 'RETRYING',
                        'SUCCEEDED', 'DEAD_LETTER', 'CANCELLED'
                    )),
    attempt         INT NOT NULL DEFAULT 0,
    max_attempts    INT NOT NULL DEFAULT 5 CHECK (max_attempts BETWEEN 1 AND 20),
    next_run_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    deadline_at     TIMESTAMPTZ NOT NULL DEFAULT (now() + interval '24 hours'),
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
    ON tasks (next_run_at ASC)
    WHERE status IN ('PENDING', 'RETRYING');

-- 3. 部分索引（DLQ 列表与批量重放专用）：加速按 Endpoint 与创建时间筛选死信
CREATE INDEX tasks_dlq_idx
    ON tasks (endpoint_id, created_at DESC)
    WHERE status = 'DEAD_LETTER';

CREATE TABLE task_attempts (
    id               BIGSERIAL PRIMARY KEY,
    task_id          UUID NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    attempt_no       INT NOT NULL,
    http_status      INT NOT NULL DEFAULT 0,
    duration_ms      INT NOT NULL DEFAULT 0,
    error_kind       TEXT NOT NULL DEFAULT '',
    error_message    TEXT NOT NULL DEFAULT '',
    response_snippet TEXT NOT NULL DEFAULT '',
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX task_attempts_task_id_idx ON task_attempts (task_id, attempt_no);

-- 事务发件箱表：Relay 推入 Kafka 或 Redis ZSet 后即刻物理 DELETE，常驻行数接近 0
CREATE TABLE outbox (
    id         BIGSERIAL PRIMARY KEY,
    task_id    UUID NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    deliver_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX outbox_deliver_idx ON outbox (id ASC);

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
| **1. PG 写完任务与 Outbox，未推 Kafka 前 API/Relay 宕机** | 任务已返回 `202 Accepted`，但 Kafka `ferry.tasks.ready` 无消息 | `outbox` 行与 `tasks` 行在同一 PG 事务提交；任意存活或重启后的 `ferry-server` Relay 协程通过 `500ms` 兜底定时器执行 `FOR UPDATE SKIP LOCKED` 捞起该记录并推入 Kafka，随后删除 `outbox` |
| **2. Redis 不可用（分两阶段）** | ① **断连拒绝服务期间**：无法读/写熔断、限流、信号量与 `ferry:delayed`；<br>② **宕机重启内存清空后**：ZSet 倒计时任务全部蒸发 | ① **断连期间 Fail-Safe**：入口 `POST /v1/tasks` 只依赖 PG，照常接单落库；Outbox 延迟任务保留不删；Worker 检测到连续 Redis 失败立即**暂停拉取 Kafka** 并让出 `/readyz`（防止无流控裸奔打挂脆弱下游），直到 Redis 重连；<br>② **清空恢复后自愈**：`60s` PG Reconciler 扫到 `next_run_at < now()-60s` 且 Redis 中已不存在的任务，全量补推回 Kafka，做到**零丢单** |
| **3. 下游持续超时（10s）或 5xx 风暴** | 单慢接口占满全局 HTTP 协程产生队头阻塞（Noisy Neighbor） | 1. 前 `max_concurrency` 个在途请求触发超时，连续可重试失败达 `circuit_threshold`（默认 `30`）后 `ferry:cb:<ep>` 切为 `OPEN 30s`；<br>2. 后续所有该 Endpoint 的任务在 `admit_endpoint.lua` 微秒级被拒，不增 `attempt`，直接写回 `ferry:delayed` 顺延 `30s + jitter`，释放协程给健康下游；<br>3. `30s` 后放行 **1 个** `HALF_OPEN` 探测请求，通则恢复 `CLOSED`，不通继续 `OPEN`；超 `max_attempts` 或 `deadline_at` 后安全落入 `DEAD_LETTER`，下游恢复后通过 `POST /v1/dlq/replay` 按限流节奏平滑重放 |
| **4. Worker 收到 `SIGTERM` 优雅停机** | 在途 HTTP 请求被腰斩、已发完的 Offset 未提交、未发完的却先提交了 Offset | 1. 收到 `SIGTERM` 立即将 `/readyz` 置为 `503`（通知外部摘流）并停止从 Kafka `Poll` 新消息；<br>2. 等待 `WaitGroup` 排空所有正在执行 HTTP 请求的协程（设置 `25s` 优雅上限），让其完整写完 `task_attempts` 与 `tasks` 状态，释放 `ZREM` 并发信号量与执行短锁；<br>3. 仅提交已完整处理的连续 Kafka Offset 后关闭连接退出；<br>4. 若超过 `25s` 仍未排空被迫退出，未提交 Offset 的消息在 Kafka Rebalance 后重发，且旧槽位将在 `timeout_ms + 5s` 自动过期释放 |
| **5. Worker 执行中遭遇 `kill -9` / OOM 强杀** | 未能执行 `defer` 中的释放信号量与删锁逻辑 | `ferry:inflight:<ep>` 使用带到期时间戳的 ZSet（`score = now + timeout_ms + 5s`）而非 `INCR/DECR`，下次准入自动 `ZREMRANGEBYSCORE` 清理僵死槽位；执行短锁同样带 `PX` TTL；Kafka 未提交 Offset 重投后安全续接，**绝不死锁** |
| **6. 下游已业务处理成功，但回传 `200 OK` 时断网** | Ferry 判定网络超时并按退避规则重新投递 | 遵循网络投递“至少一次”客观定理，每次投递强制携带同一 `X-Ferry-Task-ID` 与 `X-Ferry-Signature`，下游以 `X-Ferry-Task-ID` 为幂等键去重，保证业务结果“精确一次” |

### 6.2 最脆弱假设（Premise Collapse）与 10x 流量扩展瓶颈

- **最脆弱假设（Premise Collapse）**：
  - 本架构假设 **PostgreSQL 连接池与单库写入 IOPS 能承接并发入口写入与逐次 `task_attempts` 追加**，且 **Redis 处于可用状态时 Worker 才允许消费投递**。
  - 若业务演进至“部分低优通知允许在 Redis 故障时直接降级为本地内存限流”，可在 `Endpoint` 表增加 `fail_open_on_redis_down` 策略字段；当前为了优先保证脆弱下游不被打崩，坚持 Fail-Safe 暂停策略。
- **10x 流量扩展瓶颈（当吞吐从 2,000 TPS 冲击至 20,000+ TPS 时最先出现瓶颈的位置）**：
  1. **第一瓶颈：PG `tasks + task_attempts` 单库写 IOPS**——单次投递触发 `INSERT task_attempts` + `UPDATE tasks`。演进抓手：按周对 `tasks`/`task_attempts` 按 `created_at` 做 PostgreSQL 声明式分区，并将成功态的流水 `task_attempts` 转为内存环形缓冲批量 `COPY` 写入或只对失败重试落细节；
  2. **第二瓶颈：Redis 单实例热点 `ferry:delayed` ZSet 大 Key 与 Lua 单线程耗时**——当百万级延迟任务堆积在单个 ZSet，演进抓手：按 `hash(task_id) % 16` 将 `ferry:delayed` 拆分为 16 个分片 ZSet（`ferry:delayed:{0..15}`），Forwarder 并行按分片扫描；
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

- **`Makefile`**：封装 `setup-hooks`、`fmt`、`lint`、`test`、`test-integration`、`migrate`、`up`、`down`；
- **`.golangci.yml`（v2）**：强制开启 `govet`、`staticcheck`、`errcheck`、`errorlint`、`bodyclose`、`noctx`、`copyloopvar`、`durationcheck`、`gocritic`、`nolintlint` 与 `depguard`（锁死领域层零基础设施反向依赖）；
- **`.pre-commit-config.yaml`**：基础文本/YAML/私钥检查、`adr-number-unique`（校验 `docs/adr/NNNN-*.md` 编号唯一）、`gofmt`、`golangci-lint` 增量扫描与 `go test -short ./...`；
- **GitHub Actions CI（`.github/workflows/ci.yml`）**：包含 `lint-and-unit` 与 `integration`（基于 `deploy/docker-compose.yml` 跑 `goose` 迁移与真实 PG/Kafka/Redis 集成测试）。

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
  ├── cmd/
  │   ├── ferry-server/      # HTTP API 网关 + Outbox Relay + ZSet Forwarder + PG Reconciler
  │   ├── ferry-worker/      # Kafka 消费者 + Redis Lua 准入 + HTTP 签名投递 + 优雅停机
  │   ├── chaos-receiver/    # 混沌故障注入靶场 (模拟延迟/500/429/验签)
  │   └── ferry-bench/       # 并发压测与隔离性验证 CLI
  ├── internal/
  │   ├── api/               # 标准库 net/http ServeMux 路由与参数校验
  │   ├── store/pg/          # GORM 仓储、clause.Locking/OnConflict 与 Goose 迁移挂载
  │   ├── queue/kafka/       # twmb/franz-go 幂等生产者/消费者封装
  │   ├── scheduler/redis/   # ZSet Forwarder Lua、分布式短锁、admit_endpoint.lua
  │   ├── delivery/          # 调优 http.Transport 连接池、HMAC-SHA256 签名器、Retry-After 解析
  │   ├── logger/            # zap JSON 日志、Sampler 采样限流与 GORM 慢 SQL Logger 适配
  │   └── metrics/           # Prometheus 指标定义
  ├── migrations/            # 0001_init.sql (4 张核心表、部分索引与 outbox_notify_trg 触发器)
  ├── deploy/                # docker-compose.yml, prometheus.yml, grafana/dashboards/
  ├── scripts/               # verify_m1.sh ~ verify_m4.sh 可重复自动化验收脚本
  └── web/                   # Milestone 4 独立静态控制台 (仅消费 /v1/* REST API)
  ```

---

### Milestone 1：最小端到端可靠直投闭环（PG + Outbox + Kafka + Worker + 验签靶场）

- **独立可交付目标**：系统已可作为“立即可靠 HTTP 投递网关”独立上线使用——支持创建 Endpoint、幂等创建立即任务、PG 同事务 Outbox 搬运推 Kafka、Worker 签名直投下游并记录 `task_attempts`、不可重试 4xx 直接进死信、以及 `SIGTERM` 优雅停机。
- **核心模块与表结构**：
  - `deploy/docker-compose.yml`（先启 PostgreSQL 16 + Kafka KRaft + Redis 7 基础编排）
  - `migrations/0001_init.sql`（建齐 4 张核心表与全套索引，确保后续 Milestone 零破坏性表结构变更）
  - `cmd/ferry-server`（API：`POST/GET /v1/endpoints`、`POST/GET /v1/tasks`、`POST /v1/tasks/{id}/cancel`、`/healthz`、`/readyz`；内置 Outbox Relay 处理 `deliver_at <= now()` 直推 Kafka 路径）
  - `cmd/ferry-worker`（Kafka 消费、PG 状态守卫、HMAC-SHA256 签名投递、写 `task_attempts`、2xx 迁移 `SUCCEEDED` / 非 408&429 的 4xx 直接迁移 `DEAD_LETTER`、25s `SIGTERM` 优雅停机）
  - `cmd/chaos-receiver`（基础版：校验 `X-Ferry-Signature` 签名正确性，记录 `X-Ferry-Task-ID` 收到次数，支持返回指定状态码 `200`/`400`）
- **可运行验收命令（`scripts/verify_m1.sh`）**：
  ```bash
  docker compose -f deploy/docker-compose.yml up -d postgres kafka redis
  go test -race ./...
  # 自动化脚本依次启动 chaos-receiver(:8081)、ferry-server(:8080)、ferry-worker(:8082) 并验证：
  # 1. 创建带 signing_secret 的 Endpoint
  # 2. 携带同一 Idempotency-Key 并发发 10 次 POST /v1/tasks -> 仅生成 1 条任务，异载荷返回 409
  # 3. chaos-receiver 验证收到带合法 HMAC 签名的请求，PG tasks.status == 'SUCCEEDED'，task_attempts == 1
  # 4. 发一条指向返回 400 的任务 -> 立即进 DEAD_LETTER 且 attempt == 1（不浪费重试）
  # 5. 向 ferry-worker 发 SIGTERM 确认在途任务完成后安全退出
  ./scripts/verify_m1.sh
  ```

---

### Milestone 2：任意延迟调度、指数退避重试、PG 对账自愈与死信批量重放

- **独立可交付目标**：打通全时间维度与灾难自愈闭环——支持任意 `deliver_at` 定时任务、`5xx`/`408`/`429` 指数退避与 `Retry-After` 解析、Forwarder 30s 租约防崩、Redis 内存全清空后的 60s PG Reconciler 自动捞回，以及单条/按条件分批死信重放。
- **核心模块增量**：
  - `internal/scheduler/redis`：`claim_delayed.lua`（30s 租约续期捞取）、Outbox Relay 完善 `deliver_at > now()` 写入 `ferry:delayed` 分支、Worker `SET ferry:lock:task:<id> NX PX` 执行短锁
  - `internal/delivery`：指数退避计算器（`min(300s, 2s * 2^(attempt-1)) + full_jitter`）与 `Retry-After` 响应头解析，更新 `RETRYING` 时同步写 `ferry:delayed`
  - `internal/reconciler`：`60s`（支持环境变量配置如验收期 `2s` 加速验证）PG Reconciler，识别 Redis 残留顺延对齐与内存丢失补推
  - `internal/api` + `internal/store/pg`：`POST /v1/tasks/{id}/replay` 与 `POST /v1/dlq/replay`（`FOR UPDATE SKIP LOCKED LIMIT <= 500` 分批重置写 `outbox`）
  - `cmd/chaos-receiver` 扩展：支持按次数/概率返回 `500`、`429`（附带 `Retry-After: 2`）及固定延迟
- **可运行验收命令（`scripts/verify_m2.sh`）**：
  ```bash
  go test -race ./...
  # 自动化脚本验证：
  # 1. 提交 deliver_at=+3s 定时任务 -> 确认在 3s 前未投递，3~4s 间精准变为 SUCCEEDED
  # 2. 目标前 2 次返回 500、第 3 次返回 200 -> 验证指数退避后成功，task_attempts 记录 3 行流水
  # 3. 故意耗尽 max_attempts=2 落入 DEAD_LETTER -> 调 POST /v1/dlq/replay 批量重放并成功投递，replay_count == 1
  # 4. 灾难演练：提交 20 条 +5s 延迟任务，立即执行 docker compose exec redis redis-cli FLUSHALL 清空内存 -> Reconciler 自动从 PG 全量捞回并全部投递成功（0 丢单）
  ./scripts/verify_m2.sh
  ```

---

### Milestone 3：Endpoint 三维流控熔断、Prometheus/Grafana 可观测闭环与混沌压测

- **独立可交付目标**：交付完整生产级流控保护与秋招简历级压测监控数据——完成基于单次 Lua 的熔断器/QPS/防泄漏并发信号量、Redis 断连 Fail-Safe 背压保护、`deadline_at` 过期死信截断、全套 Prometheus/Grafana 看板与多场景混沌压测报告脚本。
- **核心模块增量**：
  - `internal/scheduler/redis/admit_endpoint.lua`：熔断（`CLOSED/OPEN/HALF_OPEN`）+ QPS 令牌桶 + ZSet 并发信号量原子准入，超限零阻塞回退 `ferry:delayed`；Worker 检测 Redis 断连自动停拉 Kafka 并置 `/readyz=503`
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

## 九、架构决策记录索引（ADR Index）

- [ADR-0001: 统一核心业务模型为可靠 HTTP 任务投递引擎](./adr/0001-unified-http-task-delivery-model.md)
- [ADR-0002: 入口事务一致性采用 PG 同事务 Outbox 与 Go 批量搬运协程](./adr/0002-transactional-outbox-batch-relay.md)
- [ADR-0003: 延迟与退避重试采用 Redis ZSet 租约倒计时池加 PG 对账兜底](./adr/0003-redis-zset-lease-and-pg-reconciler.md)
- [ADR-0004: Endpoint 级三维流控隔离与全链路三层幂等机制](./adr/0004-endpoint-flow-control-and-three-layer-idempotency.md)
- [ADR-0005: 状态机与投递明细、死信受控重放及可观测优先交付边界](./adr/0005-state-machine-dlq-replay-and-observability-boundary.md)
- [ADR-0006: Go 1.27 技术栈选型、GORM+Goose 职责边界与工程化门禁](./adr/0006-tech-stack-and-engineering-tooling.md)

# ADR-0007: 轻量战术 DDD 聚合边界、五态持久化修正与深模块切口设计

- **状态**：已采纳（Accepted）
- **日期**：2026-11-17
- **关联**：局部修正 [ADR-0005](./0005-state-machine-dlq-replay-and-observability-boundary.md) 中 `RUNNING` 持久化细节，细化 [ADR-0006](./0006-tech-stack-and-engineering-tooling.md) 代码分层与 `depguard` 门禁规则；并发围栏与流控排号加固详见 [ADR-0008](./0008-concurrency-fencing-and-flow-control-hardening.md)

## 背景与问题（Context）

在 [ADR-0001](./0001-unified-http-task-delivery-model.md) 至 [ADR-0006](./0006-tech-stack-and-engineering-tooling.md) 锁定了 Ferry 的中间件拓扑与工程底座后，进入 `internal/` 代码结构设计与业务编码前，必须进一步厘清四类容易导致架构腐化或样板膨胀的设计分叉点：

1. **术语混用与实现耦合**：文档中同时存在“顺延 vs 重试”、“`deliver_at` vs `next_run_at`”以及“租约 / 短锁 / 信号量槽位”三类期限协调机制，若不在领域语言层面正交拆分，极易在状态流转与指标埋点中发生误用；
2. **热路径 `RUNNING` 状态张力**：[ADR-0005](./0005-state-machine-dlq-replay-and-observability-boundary.md) 将 `RUNNING` 列为任务状态之一，但主链路为了减少数据库写放大并没有在发送 HTTP 前执行 `UPDATE status = 'RUNNING'`，且部分索引 `tasks_reconcile_idx` 仅覆盖 `('PENDING', 'RETRYING')`，若强行持久化 `RUNNING` 反而会引发双倍 MVCC 死元组与进程强杀后的孤儿任务隐患；
3. **聚合膨胀与教条分层陷阱**：在 Go 语言中机械套用“大而全仓储接口 + 通用 `UnitOfWork` + 浅层透传 `service` 包 + 显式 `DomainEvent` 总线”，不仅会在单行加载 `Task` 时误拉取历史 `task_attempts` 引发内存膨胀，还会制造大量无实际深度的胶水代码；
4. **领域层纯净性约束缺乏硬门禁**：若无编译器或静态检查器锁死依赖方向，业务迭代中极易把 `gorm`、`go-redis`、`franz-go` 或 `net/http` 泄漏进核心状态机，导致核心规则脱离中间件无法快速单测。

## 备选方案对比（Considered Options）

### 方案 1：轻量战术 DDD + 消费方窄接口 + 深模块切口 + `depguard` 硬门禁（采纳）
- 以单一限界上下文收口 `GLOSSARY.md`；将 `tasks` 持久化状态收口为 5 个稳态并由 `Execution Lock` 承担在途守卫；以纯 Go 充血聚合承载核心不变量并产出独立的只追加 `DeliveryAttempt`；按 `domain` + `{api,scheduler,worker}` 深模块 + `infra/*` 适配器环切分包结构，由消费方包声明最小 Outbound Port 接口，用 `depguard` 锁死依赖方向。

### 方案 2：传统四层重型 DDD（独立 `service/` 层 + `domain` 持全量仓储接口与 `DomainEvent` + `UnitOfWork`）
- **缺点**：`api` 与后台调度逻辑大部分被 `service` 机械透传，违反深模块“杠杆率”原则；`UnitOfWork` 暴露持久化事务句柄给应用编排层，既破坏切口抽象，也难以优雅支持 `FOR UPDATE SKIP LOCKED` 回调式局部确认语义；显式 `DomainEvent` 在仅服务于“新建/重放同事务入队”单一场景下纯属冗余样板。

### 方案 3：事务脚本直连基础设施（按技术目录平铺，无独立 `domain` 包）
- **缺点**：HTTP 响应分级、指数退避、`Retry-After` 解析、签名校验与状态守卫逻辑会散落在 `api`、`worker` 与数据库适配器之间，核心状态机测试被迫强依赖真实 PostgreSQL/Redis，维护局部性（Locality）急剧恶化。

## 最终决策（Decision）

采纳 **方案 1**，核心边界与契约规范如下：

### 一、统一语言正交化与五态持久化修正（局部更新 ADR-0005）

1. **单一限界上下文词表**：在仓库根目录维护无基础设施实现泄漏的 [`GLOSSARY.md`](../../GLOSSARY.md)，确立关键正交概念：
   - **Deferral（流控顺延）vs Retry（失败退避重试）**：前者在准入阶段被熔断/QPS/并发上限拦截时发生，不发 HTTP、不写 `DeliveryAttempt`、不递增 `attempt`、准入热路径不改写任务持久化状态（仅受 `Deadline` 封顶，并由后台对账惰性对齐 `NextRunAt`）；后者仅在实际发起 HTTP 请求并遭遇可重试故障后发生，生成 `DeliveryAttempt`、递增 `attempt`、迁移至 `RETRYING` 并推进 `NextRunAt`；
   - **NextRunAt**：领域内唯一的计划触发时间属性（`deliver_at` 仅作入站 HTTP DTO 参数与 `outbox` 表字段名）；
   - **三类带期限协调原语**：彻底拆分为 `Schedule Lease`（30s 延迟转发可见性租约）、`Execution Lock`（单任务单次投递互斥锁）与 `Concurrency Slot`（Endpoint 级带超时回收的在途并发槽位）。
2. **持久化五态模型（局部修正 ADR-0005）**：
   - 取消热路径投递前向 PG 写入 `UPDATE status = 'RUNNING'`，将 PG `tasks.status` 持久化状态严格收敛为 **`PENDING`、`RETRYING`、`SUCCEEDED`、`DEAD_LETTER`、`CANCELLED` 五个状态**；
   - **工程依据**：① 避免每笔投递使 `tasks` 表写放大与 MVCC 死元组翻倍；② 防止 Worker 在 HTTP 期间遭遇 `kill -9` 且 Kafka 消息未重投时，停留在 `RUNNING` 的行脱离 `tasks_reconcile_idx`（仅索引 `PENDING, RETRYING`）成为永久孤儿；
   - **在途状态与取消真互斥**：任务“是否正在执行中”由 `Execution Lock` 唯一表达；`POST /v1/tasks/{id}/cancel` 自身先通过 `SET NX` 抢占 `Execution Lock`（抢锁失败说明在途执行中，直接返回 `409 Conflict`），持锁后再以 PG CAS `WHERE id = $1 AND status IN ('PENDING', 'RETRYING')` 迁移至 `CANCELLED` 并释放锁。Worker 侧更新统一携带 `(replay_count, attempt)` 复合乐观版本守卫；若极端锁超时下取消与在途请求重叠，Worker 结算事务的 CAS 未命中（返回 `applied = false`）不覆盖终态，但**依然同事务追加写入该次已实际发出的 `DeliveryAttempt` 审计行**。
3. **`Endpoint` 禁用不变量**：
   - `Endpoint.enabled == false` 时，入口同步拦截新任务创建；已入队的存量任务在到期进入 Worker 校验时直接迁移至 `DEAD_LETTER`（记 `last_error = 'ENDPOINT_DISABLED'`，不发 HTTP、不产生 `DeliveryAttempt`），避免反复穿透调度环路，待 `Endpoint` 重新启用后统一经 `POST /v1/dlq/replay` 按流控节奏平滑重放。

### 二、聚合充血边界、独立审计流水与隐藏式 `Outbox`

1. **`Endpoint` 聚合根**：内聚创建/更新参数不变量校验（URL 合法性、`rate_limit_qps > 0`、`max_concurrency > 0`、`circuit_threshold > 0`、`timeout_ms ∈ [100, 60000]`）及基于 Go 标准库密码包实现的 `SignPayload(ts, body)`。
2. **`Task` 聚合根（零历史切片膨胀）**：
   - `Task` 不持有 `[]DeliveryAttempt` 子实体切片，状态流转永远按单行加载；
   - 内聚入口 `payload_sha256` 幂等与冲突校验、前置 `Deadline` 过期与 `Endpoint` 禁用守卫、`Cancel` 与 `Replay` 状态迁移守卫；
   - 核心方法 `RecordOutcome` 内聚 HTTP 响应分级（`2xx` 成功；`408/429/5xx`/网络错误可重试并反馈熔断失败；其余 `4xx` 直接落入 `DEAD_LETTER` 且**不计入熔断连续失败**）、`Retry-After` 解析（封顶 `3600s`）与指数退避计算，演进自身状态的同时**按值返回一条独立的只追加 `DeliveryAttempt` 与熔断反馈信号**，由外层单事务原子落盘。
3. **隐藏式 `Outbox`**：不在 `domain` 层引入教条式 `DomainEvent` 抽象；将 `outbox` 同事务写入完全隐藏在持久化仓储的原子入队与原子重放切口（Seam）背后，消除“更新 `tasks` 却漏写 `outbox`”的调用侧失误。

### 三、深模块包拓扑、DO/PO 隔离、消费方 Port 与 `depguard` 门禁

```text
internal/
├── domain/             # 纯 Go 领域核：Endpoint、Task、DeliveryAttempt、值对象、签名、退避与领域错误
├── api/                # 入站 HTTP 控制面深模块：ServeMux 路由、DTO 校验、直接编排幂等创建/取消/重放
├── scheduler/          # 后台调度深模块：OutboxRelay、DelayedForwarder、Reconciler
├── worker/             # 投递执行深模块：单任务 Step A~E 状态机、25s 优雅停机排空与 Redis 断连背压暂停
├── infra/              # 出站基础设施适配器环（统一归口隔离）
│   ├── pg/             # GORM 适配器（私有 PO、同事务 Outbox、SKIP LOCKED、Goose 挂载）
│   ├── kafka/          # franz-go 适配器（幂等生产者、消费者组、分区 Pause/Resume）
│   ├── redis/          # go-redis/v9 适配器（延迟池租约 Lua、admit_endpoint.lua 流控、Execution Lock）
│   └── httpclient/     # 出站 HTTP 投递适配器（Transport 连接池调优、LimitReader、Retry-After 解析）
├── config/             # 12-Factor 强类型环境变量解析
├── logger/             # zap JSON 采样日志与 GORM 慢查询 Logger 适配
└── metrics/            # Prometheus 指标定义与采集封装
migrations/             # 0001_init.sql + embed.go（暴露 embed.FS 供 Goose 挂载）
```

1. **零透传原则**：不设浅层 `internal/service` 包，由 `api`、`scheduler`、`worker` 三个编排深模块直接消费 `domain` 模型与自身窄接口。
2. **私有 PO 与业务原子切口**：`domain` 实体保持纯 Go 类型且零 ORM/JSON 标签；带 `gorm` 与 `datatypes.JSON` 标签的 PO 结构体全部非导出（unexported）地封闭在 `internal/infra/pg` 内。`infra/pg` 不向上暴露通用 `UnitOfWork / TxManager`，而是暴露 `CreateTaskIdempotent`、`SaveDeliveryOutcome`、`ReplayTask` / `ReplayDeadLetters` 与回调式持有行锁生命周期的 `ClaimAndDispatchOutbox`（拆分为立即/延迟双通道，单批强制绑定 `5s` 硬超时防长事务阻塞 Autovacuum，回调参数采用 Go 原生匿名 `func` 签名保持零跨包耦合）。
3. **消费方声明窄接口 + 对称流控深切口**：Outbound Port 接口按最小需要声明在 `api`、`scheduler`、`worker` 各自包内；跨切口共享纯值载体（`DeliveryRequest`、`DeliveryResult`、`AdmissionDecision`、`OutboxItem`、查询过滤条件及领域错误）定义在 `domain`，使 `infra/*` 仅依赖 `domain` 即可隐式实现全部消费方接口。流控切口配对定义为 `AdmitOrDefer`（准入占槽或按虚拟排号顺延）与 `CompleteDelivery`（归还并发槽位并更新熔断/`429` 冷却），该单脚本跨 Key 原子性以**单实例/主从 Redis** 为前提（若未来演进至 Redis Cluster 分片需拆为两步调用）；`worker` 包不引入 `net/http`，通过纯方法 `Ready() bool` 将健康状态交由组装根 `cmd/ferry-worker` 暴露 `/readyz`。
4. **静态架构门禁**：仅允许 `cmd/*` 的 `main.go` 作为组装根同时引入编排模块与 `infra/*` 并附带编译期接口断言；在 [`.golangci.yml`](../../.golangci.yml) 中配置 `depguard` 规则，强制检查：
   - `internal/domain/**` 采用 `strict` 白名单模式（仅允许 Go 标准库、`github.com/google/uuid` 与自身包，显式禁止 `net/http` 与 `database/sql`）；
   - `internal/{api,scheduler,worker}/**` 同样采用 `strict` 白名单模式（显式禁止 `database/sql`，其中 `scheduler` 与 `worker` 额外禁止 `net/http`），彻底封死直引 `internal/infra/**`、兄弟编排模块、`cmd` 或绕过 ORM 直连底层驱动的路径；
   - `internal/infra/{pg,kafka,redis,httpclient}/**` 严禁反向依赖编排模块、`cmd` 及兄弟适配器；
   - `internal/{config,logger,metrics}/**` 严禁向上依赖业务或适配器层。

## 影响与取舍（Consequences & Trade-offs）

- **正向收益**：
  - `domain` 包脱离外部中间件与传输层运行时，聚合状态流转、签名、幂等比对、错误分级与退避算法均可毫秒级纯内存单测；而为换取微秒级跨结构原子性下沉至 `infra/redis` Lua 脚本的三维流控与熔断规则，则通过真实 Redis 集成测试端到端验证，职责与测试边界清晰诚实；
  - 五态模型与单事务 `SaveDeliveryOutcome` 在降低 PG 写放大的同时彻底消除了孤儿状态风险，并守住了并发取消场景的真实审计记录；
  - `depguard` 在本地 `pre-commit` 与 CI 中自动拦截任何跨层违规引用，防止项目在后续 Milestone 迭代中滑向紧耦合。
- **明确取舍**：
  - `GET /v1/tasks/{id}` 在不查 `Execution Lock` 的情况下直接返回 `tasks` 表的持久化状态（`PENDING` 或 `RETRYING`），前端不再区分独立的数据库 `RUNNING` 字段（如需实时展示在途，可按需在控制面查询中附加探活信号）；
  - 当新增加基础包或跨模块共享值对象时，需遵循依赖单向性将其沉淀至 `domain` 或调整 `.golangci.yml` 中的 `depguard` 规则并经过评审。

# ADR-0001: 统一核心业务模型为可靠 HTTP 任务投递引擎

- **状态**：已采纳（Accepted）
- **日期**：2026-11-16
- **修订说明**：载荷 `BYTEA` 原始字节级保真契约详见 [ADR-0008](./0008-concurrency-fencing-and-flow-control-hardening.md)

## 背景与问题（Context）

在实习项目 `x-data` 中，控制面与执行 Worker 共享同一台 PostgreSQL 实例：业务逻辑在更新运行表的同一个数据库事务中调用 `river.InsertTx` 写入队列任务，Worker 通过轮询 PostgreSQL 领取任务，并在等待外部远程结果时利用 `river.JobSnooze` 推迟重试，并发度在服务配置中按队列静态固定（如 `DecodeQueueWorkers = 2`）。

当需要将这套异步调度能力独立抽离为面向多业务方与外部商户的通用平台 **Ferry** 时，单库直连队列不再适用，必须首先明确系统的核心领域边界：究竟是做成“只面向外部回调的 1:N 事件广播式 Webhook 网关”，还是做成“引入客户端 SDK 执行任意 Go 函数的分布式任务框架”，或是收口成更纯粹的统一传输抽象。

## 备选方案对比（Considered Options）

### 方案 1：统一可靠 HTTP 任务投递引擎（Point-to-Endpoint HTTP Push，采纳）
- **核心抽象**：所有异步任务与 Webhook 统一抽象为“在指定投递时间 `deliver_at`，向已注册目标 `Endpoint` 发起一次带超时控制、幂等请求头与可选 HMAC 签名的 HTTP POST 投递”。
- **适用范围**：
  - **内部微服务异步/延迟任务**：上游指定已注册的目标 `Endpoint` ID、JSON 载荷及可选延迟时间 `deliver_at`（每个 `Endpoint` 一对一绑定完整 `target_url` 与独立流控配额），Ferry 负责削峰、定时触发、失败退避与流控保护；
  - **外部 Webhook 回调**：目标 `Endpoint` 配置 `signing_secret` 后，Ferry 在发送请求时自动签发 `X-Ferry-Timestamp` 与 `X-Ferry-Signature`，满足商户验签防篡改需求。

### 方案 2：纯事件订阅扇出网关（`Application -> EventType -> Subscription -> 1:N Delivery`）
- 对标 Svix 全量模型，上游只需发事件名称与载荷，Ferry 维护事件类型目录与多对多订阅关系并扇出为 N 条投递任务。
- **缺点**：引入额外的组织/应用/事件类型/订阅匹配管理面与扇出放大事务复杂度，分散了对跨 PG/Kafka/Redis 核心调度链路与流控可靠性的打磨精力；且无法直接覆盖单次点对点延迟异步任务。

### 方案 3：带客户端 Go SDK 的代码级分布式任务调度框架（如 Asynq / XXL-Job）
- 业务方需引入 Ferry Go SDK 并注册 Handler，Worker 在业务进程内通过长连接或轮询拉取函数任务执行。
- **缺点**：跨语言复用差，需要维护 SDK 协议版本兼容、进程心跳与任务租约回收，与 Webhook 投递场景割裂。

## 最终决策（Decision）

采纳 **方案 1（统一可靠 HTTP 任务投递引擎）**：

1. **单一发送引擎承接双场景**：底层只有一套无状态 HTTP Push Worker 引擎，对内作为异步任务/延迟回调调度器，对外作为标准 Webhook 投递网关。
2. **标准 HTTP 投递契约**：
   - 固定采用 HTTP `POST` 发送 `application/json` 载荷（支持用户透传自定义安全头）；
   - 每次投递强制携带 `X-Ferry-Task-ID: <task_id>` 与 `X-Ferry-Attempt: <attempt_no>`；
   - 当 `Endpoint.signing_secret` 非空时，生成当前 Unix 秒级时间戳 `ts`，计算签名 `sha256=<hex(HMAC-SHA256(signing_secret, ts + "." + raw_body))>`，写入 `X-Ferry-Timestamp` 与 `X-Ferry-Signature` 请求头。
3. **跨服务独立网关接入**：上游服务统一通过 REST API（`POST /v1/tasks`）向 Ferry 提交任务，不再与调度器共享底层数据库事务，彻底解除上游业务库与调度存储的耦合。

## 影响与取舍（Consequences & Trade-offs）

- **正向收益**：
  - 领域表极简（仅 `endpoints`、`tasks`、`task_attempts`、`outbox` 四张核心表），全链路压测吞吐、端到端延迟与故障恢复表现纯净可量化；
  - 调度引擎与目标接收方语言完全解耦，任何支持 HTTP 的内部微服务或外部商户均可零 SDK 接入。
- **明确取舍**：
  - 首期不建设“事件类型注册表 + 1:N 自动订阅扇出”模块；若未来需要支持 `1:N` 事件广播，只需在 `POST /v1/tasks` 之上叠加一层轻量的无状态事件路由前置接口（将单事件拆分为同事务的多条 `tasks`），核心投递、流控与重试引擎无需任何改动。

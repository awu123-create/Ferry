# Go 代码规范（Go Code Standard）

编写或评审 `internal/` 与 `cmd/` 下的非测试 Go 代码时阅读本文件。通用规则与分级定义见 [CODING_STANDARDS.md](../../../CODING_STANDARDS.md)；跨包依赖白名单（`depguard`）、HTTP 常量（`usestdlibvars`）与 `internal/{domain,infra/pg}` 禁裸调 `time.Now`（`forbidigo`）等机械规则由 `.golangci.yml` 强制执行，本文只保留架构与设计判断条款。

## 文件分工与数据库模型隔离

代码按四层职责组织（详见 [docs/adr/0007-tactical-ddd-and-deep-module-seams.md](../../adr/0007-tactical-ddd-and-deep-module-seams.md)）：

- `internal/domain/`：纯 Go 充血领域核（`endpoint.go`、`task.go`、`delivery.go`、`errors.go` 等），零 ORM、JSON 标签、HTTP 或数据库驱动依赖。
- `internal/{api,scheduler,worker}/`：三个编排深模块，不建浅层透传的 `internal/service` 包。消费方在各自 `ports.go` 中声明最小 Outbound Port 接口，只放本包真会调用的方法。
- `internal/infra/{pg,kafka,redis,httpclient}/`：出站基础设施适配器环；适配器只依赖 `domain` 及基础包隐式实现消费方接口，适配器之间互不依赖。
- `cmd/*/`：唯一组装根，显式构造组件并附编译期接口断言（如 `var _ api.TaskStore = (*pg.Store)(nil)`），不引入 `fx` 或 `viper`。
- 带 `gorm` 与 `datatypes` 标签的持久化结构体（`endpointPO`、`taskPO`、`deliveryAttemptPO`、`outboxPO` 等）必须非导出，统一收敛在 `internal/infra/pg/models.go` 中；每张表只维护一套 `toXxxPO` / `toDomainXxx` 转换函数，新增列只改一处并统一规范化 UTC 时间与枚举解析。

**Judgement.** 单文件同时承载多块无关能力、单个能力散落多文件、或文件接近 1000 行时，按领域概念重新切分。

**Smell baseline override.** `internal/infra/pg` 中完成单次 GORM 查询并做 PO ↔ Domain 映射的方法属于既定适配边界，不判为 **Middle Man**。

## 构造与依赖组装（Construction）

构造函数（如 `NewHandler`、`NewWorker`、`NewOutboxRelay`）返回的组件必须是**装配完整的**：

- 必需依赖缺一不可，构造函数传入 `nil` 直接返回错误。构造成功后的运行路径（如 `worker.Run`、HTTP Handler）直接信任装配结果，禁止再重复判空或写 `hasAllDependencies` 类二次防空检查。
- 能力通过消费方 Port 接口显式声明，禁止在运行期对依赖做类型断言探测可选方法，避免假实现漏实现方法却通过编译并静默跳过校验。
- 进程角色独享的依赖只在对应 `cmd/ferry-server` 或 `cmd/ferry-worker` 组装；例如 `internal/worker` 不引入 `net/http`，仅暴露 `Ready() bool` 供 `cmd/ferry-worker` 挂载 `/readyz`。
- 测试切口不污染生产 API 签名，不为测试把必需依赖放宽成可空参数或增加测试模式开关；确需向黑盒测试暴露内部构造时，通过同包 `export_test.go` 导出。

**Judgement.** 配置参数统一放进配置结构体（如 `HandlerConfig`、`worker.Config`）在构造时一次校验并填默认值，少用链式 `With*` 方法在构造后改状态。

## 边界解析与载荷保真（Parsing at boundaries）

承接 [CODING_STANDARDS.md](../../../CODING_STANDARDS.md) 的边界解析规则：

- 全仓库 JSON 编解码统一使用 Go 1.27 标准库 `encoding/json/v2` 与 `encoding/json/jsontext`，禁止引入旧 `encoding/json`（详见 `docs/adr/0006-tech-stack-and-engineering-tooling.md`，对应 `depguard` `json_v2` 门禁随 M1 首个代码 PR 启用）。
- `Task.Payload` 端到端字节保真：入站校验合法 JSON 后全程按原始 `[]byte` 透传，绝不反序列化再重序列化；`payload_sha256` 幂等比对、PG `BYTEA` 落盘、`Endpoint.SignPayload` HMAC-SHA256 签名与出站 HTTP 请求体共用同一原始字节切片（`docs/adr/0008-concurrency-fencing-and-flow-control-hardening.md`）。
- 所有封闭值域状态（`domain.TaskStatus`、`domain.AttemptErrorKind` 等）以命名类型与常量流转；从数据库读回字符串时在 `internal/infra/pg/models.go` 统一走 `domain.Parse*` 校验，拒绝未知值，禁止直接类型强转。

**Smell baseline override.** 本节规则裁决 **Primitive Obsession**；`uuid.UUID`（UUIDv7）与毫秒配置整数（如 `TimeoutMs`）在既有 Port 与模型中直接传递。

## 持久化切口与并发围栏（Persistence seams and concurrency）

- `internal/infra/pg` 不向上暴露通用的 `BeginTx` / `UnitOfWork`，而是暴露完整的业务原子切口（如 `CreateTaskIdempotent`、`SaveDeliveryOutcome`、`CancelTask`、`ClaimAndDispatchImmediateOutbox`），把同事务写 `outbox` 与锁生命周期收敛在仓储内部。
- 并发检查必须在提交前始终成立，禁止“先普通 SELECT 判断、事务外或非锁状态下再 UPDATE”：
  - 入口幂等使用数据库 `UNIQUE` 索引 + GORM `clause.OnConflict{DoNothing: true}`，零行命中时在同事务回查并经 `VerifyIdempotentReplay` 校验载荷哈希与 Headers；
  - Outbox 抢占与死信批量重放使用 `clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}`，回调式事务强制绑定 `5s` 超时 Context 防止长事务阻塞 Autovacuum；
  - 状态迁移使用条件 `Updates` 检查 `RowsAffected`：Worker 结算与预投递死信携带 `(replay_count, attempt)` 复合乐观锁，`CancelTask` 在持有 Redis `Execution Lock` 前提下以 `status IN ('PENDING', 'RETRYING')` 执行 CAS，`UpdateEndpoint` 携带 `expectedUpdatedAt` 乐观锁。
- Redis 跨 Key 或“先验持有人再续期/删除”的原子操作（`Execution Lock` 验主续期与释锁、延迟池 30s 租约与比对分数条件 `ZREM`、三维流控脚本）统一用 `redis.NewScript` 封装，且脚本内统一调用 `redis.call('TIME')` 消除多机时钟偏差；禁止对锁执行裸 `DEL`（详见 `docs/adr/0008-concurrency-fencing-and-flow-control-hardening.md`）。

## 错误处理（Errors）

错误单向向上传递并只处理一次（见 [CODING_STANDARDS.md](../../../CODING_STANDARDS.md)）：

1. `internal/domain` 定义业务 sentinel（`ErrXxx`）与结构化错误类型（如 `ValidationError`）；
2. `internal/infra/*` 将底层 SQL/Redis/Kafka 语义错误翻译为对应的 `domain.Err*`，其余错误按 `<小写动词> <领域概念>: %w` 包装返回（如 `fmt.Errorf("insert task: %w", err)`）；
3. `internal/api` 在 `writeDomainError` 集中通过 `errors.Is` / `errors.As` 映射 HTTP 状态码与固定错误码（未识别错误只记一条 `Error` 日志，对外返回脱敏的 `500 INTERNAL_ERROR`）；`internal/scheduler` 与 `internal/worker` 在循环顶层记录并决定退避或重投。

- 错误分支只匹配 sentinel 或错误类型，禁止匹配 `err.Error()` 文本。
- 运行期与外部输入错误一律返回 `error`，禁止用 `panic` 处理运行期异常；`os.Exit` 只允许出现在 `cmd/*/main.go`。

**Judgement.** 上层需要通过 `errors.Is` / `errors.As` 识别原因时用 `%w`；在有意截断底层实现类型泄漏的边界用 `%v`。

## 出站调用（Outbound calls）

- 禁止在 PostgreSQL 数据库事务内发起外部 HTTP 请求或不可控外部调用（Outbox 双通道回调推 Kafka 或写 Redis 延迟池是绑定 `5s` 硬超时的特例）。
- 所有出站 HTTP 投递统一走 `internal/infra/httpclient.Sender`：必须携带带超时的 `ctx`、禁用自动跟随 3xx 重定向（`CheckRedirect` 返回 `http.ErrUseLastResponse`）、响应体用有界 `io.LimitReader` 截断防 OOM 并确保 `resp.Body.Close()`、单次尝试不内置重试、过滤上游透传的 `Host` 与 `X-Ferry-*` 保留头。

## 日志与指标（Logging and metrics）

- 日志只在边界层输出（`api`、`scheduler`、`worker`、GORM 慢查询适配器、`cmd/` 启动收尾）；`internal/domain` 零日志依赖。
- 需要打日志的组件通过构造函数注入带采样的 `*zap.Logger`；日志 message 写固定短语，变化值放 `snake_case` 结构化字段，错误携带 `zap.Error(err)`。
- 所有处理已知 `task_id` 的请求与执行链路，必须通过 `logger.WithTaskID(ctx, taskID)` 将 `task_id` 绑定到 `ctx`，供边界日志与 GORM 慢查询日志统一串联。
- 严禁将 `signing_secret`、数据库 DSN、Redis 密码、`Authorization` 请求头或未脱敏敏感载荷写入日志或错误响应。
- Prometheus 指标标签只允许取封闭集合（状态类别、错误类型、HTTP 状态段等），严禁将 `task_id`、原始目标 URL 等高基数字段设为标签。

**Judgement.** 日志级别约定：

| 级别 | 含义 |
| :--- | :--- |
| `Error` | 需要人工介入排查的异常。 |
| `Warn` | 进程已自愈或按预期降级处理（如重试、探针失败）。 |
| `Info` | 启停生命周期与关键业务节点完成。 |
| `Debug` | 本地排障细节，生产默认关闭。 |

## 时间、Context 与 Goroutine

- 所有持久化与领域时间统一规范化为 `.UTC()`。
- 涉及业务时间判断与落库的时间必须可注入：`internal/domain` 与 `internal/infra/pg` 业务方法由调用方传入 `now time.Time`（机械禁令由 `.golangci.yml` `forbidigo` 强制）；`internal/{api,scheduler,worker}` 通过可注入时钟（如 `clock func() time.Time`，构造默认 `time.Now`）取当前时间。仅纯耗时测量（如 `time.Since` 统计 HTTP 请求 `duration_ms`）允许直接调用 `time.Now()`。
- `context.Context` 必须是所有 I/O 函数的第一个参数。
- 每个启动的 goroutine 必须有明确归属方负责停止并等待退出（`sync.WaitGroup` 或 `errgroup`），杜绝野 goroutine；优雅停机须将“停止拉取新任务的 `ctx`”与“排空在途投递的 `drainCtx`”分离，确保 `SIGTERM` 下在途投递写完 PG 并释锁。
- `init()` 禁止执行任何 I/O 或初始化全局运行时状态，启动装配一律在 `cmd/*/main.go` 显式完成。

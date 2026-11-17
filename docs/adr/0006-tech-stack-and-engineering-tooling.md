# ADR-0006: Go 1.27 技术栈选型、GORM+Goose 职责边界与工程化门禁

- **状态**：已采纳（Accepted）
- **日期**：2026-11-16

## 背景与问题（Context）

Ferry 的总体架构与可靠性契约已在 [ADR-0001](./0001-unified-http-task-delivery-model.md) 至 [ADR-0005](./0005-state-machine-dlq-replay-and-observability-boundary.md) 中定稿。在进入代码骨架搭建前，需要冻结一套既能充分发挥 Go 并发与工程规范、又能避开重型框架拖累的运行期技术栈，同时解决三个具体工程诉求：
1. 本机 Go 工具链为 `1.27`，需统一代码版本声明与静态检查工具编译版本；
2. 业务仓储层希望零手写裸 SQL（采用 GORM 提升开发与维护效率），但又不能丧失 `SKIP LOCKED`、`ON CONFLICT` 幂等、部分索引与 `LISTEN/NOTIFY` 等 PostgreSQL 关键并发原语；
3. 下游故障风暴（高并发 `5xx`/超时）期间，日志组件必须防止每秒数千条报错刷爆磁盘 I/O 进而反噬 Worker 吞吐，并统一接管 GORM 慢 SQL 审计。

## 备选方案对比（Considered Options）

### 方案 1：Go 1.27 + 标准库 `net/http` + `GORM` / `Goose` 职责分离 + `franz-go` + `go-redis/v9` + `zap`（采纳）
- 数据访问使用 `gorm.io/gorm` + `gorm.io/driver/postgres`（业务零手写 SQL），迁移与数据库高级对象（部分索引、`pg_notify` 触发器）严格归口 `pressly/goose/v3`；
- HTTP 路由使用 Go 1.22+ 增强 `net/http.ServeMux`，日志采用 `go.uber.org/zap`（内置采样限流 + 接管 GORM 慢查询）；
- Kafka 与 Redis 分别采用纯 Go 的 `twmb/franz-go` 与 `redis/go-redis/v9`。

### 方案 2：`pgx` 手写 SQL + `log/slog` + `gin` + `viper` + `uber/fx`
- **缺点**：手写 SQL 虽透明但增加样板拼装成本（尤其动态条件过滤死信）；`slog` 缺少原生日志采样限流器，故障风暴期需自行封装限频逻辑；`gin`、`viper`、`fx` 会额外引入数十个间接依赖并掩盖优雅停机的逆序关闭顺序。

## 最终决策（Decision）

采纳 **方案 1**，各层技术栈与严格边界锁定如下：

### 一、核心技术栈锁定

| 层级 | 定稿选型 | 关键工程配置与选型理由 |
| :--- | :--- | :--- |
| **Go 语言版本** | **`go 1.27`** | `go.mod` 与 `.golangci.yml` 统一锁定 `1.27`；使用本机 `go1.27` 升级 `golangci-lint v2` 至对应编译版以消除工具链版本撕裂 |
| **ORM 数据访问** | **`gorm.io/gorm` + `gorm.io/driver/postgres` + `gorm.io/datatypes`** | 开启 `SkipDefaultTransaction: true` 与 `PrepareStmt: true`；业务层不写裸 SQL，全部通过 GORM 模型、链式条件与 `clause` 构建 |
| **数据库迁移** | **`pressly/goose/v3`**（`embed.FS` + CLI） | 唯一 DDL 迁移入口，负责建表、部分索引、约束与 `outbox` 触发器；**严禁开启 GORM `AutoMigrate`** |
| **主键生成** | **`google/uuid`**（`uuid.NewV7()`） | 时间序有序主键，防止高并发随机 `UUIDv4` 写入引发 PG B-Tree 索引页分裂 |
| **HTTP 路由与投递** | **标准库 `net/http`**（增强 `ServeMux` + 定制 `http.Transport`） | 零框架承载 ~9 个 REST 接口；投递端手动调优连接池（`MaxIdleConns: 1000`、`MaxIdleConnsPerHost: 64`、`IdleConnTimeout: 90s`）并使用 `io.LimitReader` 截断大响应体防 OOM |
| **Kafka 客户端** | **`twmb/franz-go`**（含 `pkg/kadm`） | 纯 Go 零 CGO，原生默认启用幂等生产者、`CooperativeSticky` 重平衡及分区级 `PauseFetchPartitions`（直接支撑 ADR-0004 Redis 断连背压暂停） |
| **Redis 客户端** | **`redis/go-redis/v9`** | 驱动 `ferry:delayed` ZSet、Pipeline、短锁与 `redis.NewScript`（自动 `EVALSHA` 并在 `FLUSHALL`/重启 `NOSCRIPT` 后自动重载 `EVAL`） |
| **结构化日志** | **`go.uber.org/zap`** | JSON 格式输出；启用 `zapcore.NewSamplerWithOptions` 防下游雪崩刷屏拖垮磁盘；实现 GORM `logger.Interface` 接管慢 SQL 阈值告警（如 `> 200ms`）并透传 `task_id` |
| **配置与组装** | **环境变量结构体 + `main.go` 显式构造** | 12-Factor 纯环境变量强校验，不引 `viper`/`fx`，保证优雅停机逆序排空路径完全显式可读 |
| **指标与容器栈** | **`prometheus/client_golang`**；`postgres:16-alpine`、`apache/kafka:3.9.0` (KRaft)、`redis:7.4-alpine` (`--maxmemory-policy noeviction`) | 独立暴露 `:2112`/`:2113` `/metrics`；Redis 强制禁止内存淘汰以保护 `ferry:delayed` |

### 二、GORM 与 Goose 协作边界（零手写业务 SQL 且不丢底层并发能力）

1. **DDL 归 Goose，DML 归 GORM**：所有部分索引（`tasks_reconcile_idx`、`tasks_dlq_idx`）、唯一约束以及 `outbox` 写入通知均定义在 `migrations/0001_init.sql`；
2. **并发原语全走 GORM 原生 `clause`**：
   - 入口幂等冲突：`tx.Clauses(clause.OnConflict{Columns: ... , DoNothing: true}).Create(&task)`；
   - Outbox 抢占与死信分批重放：`tx.Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).Order("id ASC").Limit(100).Find(&rows)`；
   - 状态机 CAS 守卫：`db.Model(&Task{}).Where("id = ? AND status IN ?", id, allowed).Updates(...)` 检查 `RowsAffected`；
3. **Outbox 毫秒级唤醒与监听**：通过迁移脚本在 `outbox` 表挂载 `AFTER INSERT FOR EACH STATEMENT` 触发器执行 `PERFORM pg_notify('ferry_outbox', '')`；Go 端 Relay 直接从 `gorm.io/driver/postgres` 底层复用的 `jackc/pgx/v5` 连接池取单条连接调用 `WaitForNotification(ctx)`，无需业务层手写 SQL 也不额外引入第二个 PG 驱动。

### 三、工程化质量门禁

1. **`Makefile`**：统一封装 `setup-hooks`、`fmt`、`lint`、`test`、`test-integration`、`migrate`、`up`、`down`；
2. **`.golangci.yml`（v2）**：开启 `govet`、`staticcheck`、`errcheck`、`errorlint`、`bodyclose`（防漏关 HTTP 响应体泄漏连接）、`noctx`（强迫 HTTP/DB 调用带 `context.Context`）、`copyloopvar`、`durationcheck`、`gocritic`、`nolintlint` 与 `depguard`（锁死领域层不反向依赖基础设施实现）；
3. **`.pre-commit-config.yaml`**：串联基础文件检查、`adr-number-unique`（校验 `docs/adr/NNNN-*.md` 编号唯一）、`gofmt`、`golangci-lint` 增量扫描与 `go test -short ./...`；
4. **GitHub Actions CI（`.github/workflows/ci.yml`）**：包含 `lint-and-unit`（全量 lint + 竞态单测）与 `integration`（拉起 `deploy/docker-compose.yml` 真实容器跑 `goose` 迁移与集成测试，不引入 `testcontainers-go`）。

## 影响与取舍（Consequences & Trade-offs）

- **正向收益**：
  - 彻底避免业务层手写枯燥 CRUD SQL 的同时，完整保留 `SKIP LOCKED`、唯一约束冲突、部分索引与 `LISTEN/NOTIFY` 等数据库核心特性；
  - `zap` 采样器 + GORM 慢查询统一日志使高并发混沌压测期兼具可观测性与性能稳定性；
  - 本地 `pre-commit` 与 CI 使用同一套 `docker-compose` 中间件栈，消除“本机跑通、CI 漂移”问题。
- **明确取舍**：
  - 引入 GORM 会带来轻微反射封装开销，通过开启 `SkipDefaultTransaction: true` 与 `PrepareStmt: true` 将热点开销压到最低；在 `2,000~5,000 TPS` 级 HTTP I/O 密集场景下瓶颈仍在下游网络与 PG 磁盘持久化，而非 ORM 层。

# 开发与贡献指南（Contributing）

本文件说明 Ferry 的本地开发环境准备、日常校验命令、测试分层归属、真容器隔离规则与项目级代码约定。系统架构与并发协议见 [docs/ARCHITECTURE.md](./docs/ARCHITECTURE.md)，通用编码与技术栈规范见 [CODING_STANDARDS.md](./CODING_STANDARDS.md) 及 `docs/standards/`。

## 环境准备与本地工具

`Makefile` 不内置依赖安装命令；本地开发与提交前检查请按以下固定版本自行准备工具，与 [.github/workflows/ci.yml](./.github/workflows/ci.yml) 保持一致：

- **Go 1.27** 与 **Docker Compose**（真容器测试与本地运行）
- **`golangci-lint` `v2.15.0`**（需确保 `$(go env GOBIN)` 或 `$(go env GOPATH)/bin` 在 `PATH` 中，且优先于系统包管理器里的旧版本）：

```bash
go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.15.0
```

- **[`shellcheck`](https://www.shellcheck.net/)**（供 `make check-scripts` 与本地 `pre-commit` 校验 Shell 脚本，macOS 可用 `brew install shellcheck`，Linux 使用系统包管理器安装）
- **`pre-commit` `>= 3.2.0`**（按习惯任选其一，如 `uv tool install pre-commit`、`pipx install pre-commit` 或 `brew install pre-commit`）

工具就绪后，挂载 Git 提交钩子：

```bash
make setup-hooks               # 执行 pre-commit install --install-hooks
```

具体静态检查规则、提交钩子与 CI 门禁直接以 [.golangci.yml](./.golangci.yml)、[.pre-commit-config.yaml](./.pre-commit-config.yaml)、[Makefile](./Makefile) 与 [.github/workflows/ci.yml](./.github/workflows/ci.yml) 为准。

## 日常开发与校验命令

纯内存快速校验（1~2 秒，零容器依赖）：

```bash
make fmt                       # 格式化全仓 Go 源码
make check-adr                 # 校验 ADR 编号唯一及全仓 ADR 引用带完整路径/链接
make check-scripts             # 校验全仓 *.sh 语法与系统 shellcheck
make lint                      # 运行 golangci-lint 配置校验与全量静态检查
make test                      # go test -race -short ./...
make build                     # 编译 cmd/* 二进制至 .build/
```

涉及 PostgreSQL、Redis、Kafka 或跨模块投递链路时，启动本地容器运行全量集成测试：

```bash
make up                        # 启动 PG、Kafka、Redis 并等待健康检查通过
make test-integration          # go test -race -count=1 ./...
make verify-m1                 # 多进程黑盒验收（刚跑完集成测试可传 SKIP_TESTS=1 make verify-m1）
make down                      # 停止容器（清卷使用 docker compose -f deploy/docker-compose.yml down -v）
```

## 测试分层归属

每个行为只在拥有它的层级验证一次，通用测试准则见 [docs/standards/go/testing.md](./docs/standards/go/testing.md)：

| 层级 | 位置 | 测什么 | 依赖 |
| :--- | :--- | :--- | :--- |
| 领域逻辑 | `internal/domain/*_test.go` | `Endpoint`/`Task` 不变量、边界常量、Header 校验、幂等比对、前置守卫、状态流转、HMAC 签名、退避计算 | 纯内存，零外部依赖 |
| HTTP 控制面 | `internal/api/*_test.go` | 路由注册、JSON 解析、错误码映射、内部错误脱敏、取消抢锁流程、`Endpoint` 并发更新 409 冲突 | 内存 fake 与 `httptest` |
| 调度与执行 | `internal/scheduler/*_test.go`、`internal/worker/*_test.go` | `Outbox` `LISTEN/NOTIFY` 与定时兜底唤醒、积压连批搬空、退避重试；`Worker` Step A~E 流程、锁续期、并发上限与优雅停机 | 内存 fake |
| 配置与日志 | `internal/config/*_test.go`、`internal/logger/*_test.go` | 环境变量解析与 `.env.example` 对齐、采样日志与 `WithTaskID` 透传、全仓静态日志短语 AST 门禁、GORM 慢查询适配 | 纯内存 |
| 出站 HTTP 客户端 | `internal/infra/httpclient/*_test.go` | 原始字节与签名头透传、超时、响应截断、禁 3xx 重定向、2xx 读体失败保护、状态码分类与 `Retry-After` 解析 | 本地 `httptest.Server` |
| PostgreSQL 存储 | `internal/infra/pg/store_test.go` | SQL 事务与约束：`ON CONFLICT DO NOTHING` 并发幂等、`BYTEA` 保真、乐观锁更新与取消竞态、`SKIP LOCKED` 锁定与回滚、`LISTEN/NOTIFY` 触发器、`Endpoint` `updated_at` 乐观锁 | 跑完迁移的真实 PostgreSQL |
| Redis 适配器 | `internal/infra/redis/*_test.go` | 原子操作与 Lua 脚本：`SetNX` 互斥、核对持有人 token 续期（`PEXPIRE`）与释放、过期接管防误删、延迟池租约与取消标记脚本 | 真实 Redis（DB 14） |
| Kafka 适配器 | `internal/infra/kafka/reader_test.go`、`stream_test.go` | `reader_test.go` 测连续 Offset 水位推进与分区撤销边界；`stream_test.go` 测真实 Broker 生产、消费、乱序 `Ack` 与重启恢复 | 内存 + 真实 Kafka |
| 端到端集成测试 | `tests/integration/<scene>_integration_test.go` | 一条核心业务主链路一个文件：真实组装 `api`、`scheduler`、`worker`、`pg`、`redis`、`kafka`、`httpclient` 与 `httptest` 下游，验证跨层协作 | 真实 PG + Redis + Kafka |
| 多进程黑盒脚本 | `scripts/verify_m*.sh` | 编译并启动真实 `ferry-server`、`ferry-worker`、`chaos-receiver` 独立进程，验证端口监听、健康探针与真实 `SIGTERM` 优雅停机 | 真实容器与编译产物 |

## 真容器测试环境与数据隔离

真容器测试默认直连 [deploy/docker-compose.yml](./deploy/docker-compose.yml) 暴露在本机的端口，支持通过环境变量覆盖（完整列表与 [.env.example](./.env.example) 保持 1:1 同步）：

| 环境变量 | 默认值 | 隔离方式 |
| :--- | :--- | :--- |
| `FERRY_TEST_PG_DSN` | `postgres://ferry:ferry@localhost:5432/ferry?sslmode=disable` | 每条测试创建独立临时库 `ferry_<scope>_<pid>_<ts>` 并执行内嵌 Goose 迁移，`t.Cleanup` 运行 `DROP DATABASE ... WITH (FORCE)` 清理 |
| `FERRY_TEST_REDIS_ADDR`、`FERRY_TEST_REDIS_UNIT_DB`、`FERRY_TEST_REDIS_DB`、`FERRY_TEST_REDIS_PASSWORD` | `localhost:6379`、`14`、`15`、空 | `internal/infra/redis` 固定使用 DB 14、`tests/integration` 固定使用 DB 15，避免多包并行冲突；任务级 Key 用新 UUIDv7 隔离，测全局共享 Key（如 `ferry:delayed`）前后清理本库 |
| `FERRY_TEST_KAFKA_BROKERS` | `localhost:9092` | 每条测试创建随机 Topic（`ferry.test.ready.<short-id>` 或 `ferry.it.ready.<short-id>`）与独立消费组，`t.Cleanup` 用 `kadm.DeleteTopics` 删除 |

## 项目级代码约定

在遵循 [docs/standards/go/code.md](./docs/standards/go/code.md) 通用规则的基础上，Ferry 代码遵循以下项目约定：

- **模块边界与持久化映射**：四层包职责、消费方窄接口与仓储原子切口见 [docs/ARCHITECTURE.md](./docs/ARCHITECTURE.md)；GORM 持久化结构体收敛于 `internal/infra/pg/models.go`，枚举列读取统一走 `domain.Parse*`；`uuid.UUID`（UUIDv7）与毫秒配置整数在既有接口与模型中直接传递。
- **进程角色隔离**：组件专属依赖只在对应 `cmd/ferry-server` 或 `cmd/ferry-worker` 组装（例如 `internal/worker` 不引入 `net/http`，仅暴露 `Ready() bool` 供 `cmd/ferry-worker` 挂载 `/readyz`）。
- **出站请求头保护**：所有出站 Webhook 投递经 `internal/infra/httpclient.Sender` 发送，自动过滤上游透传的 `Host` 与 `X-Ferry-*` 保留头。
- **日志串联与项目敏感字段**：边界组件注入带采样的 `*zap.Logger`，错误字段统一使用 `zap.Error(err)`；所有处理已知 `task_id` 的 API 请求与投递执行链路通过 `logger.WithTaskID(ctx, taskID)` 绑定上下文，供边界日志与 GORM 慢 SQL 日志统一输出 `task_id`。受脱敏规则约束的项目具体字段包括 `signing_secret`、数据库 DSN、Redis 密码、`Authorization` 请求头与原始载荷。

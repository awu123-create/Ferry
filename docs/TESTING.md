# Ferry Go 测试规范

写测试、审测试，或者拿不准某个逻辑该放到哪一层测的时候，看这份文档。

## 基本原则

一个行为只在负责它的那一层测一次。写新测试前先想清楚归哪一层管，别的层已经测过的分支，不在其他层重复复制。

保留任何测试文件前先问一句：删掉它之后，有没有真实 bug 会漏过去？没有就删。比如 `internal/infra/pg/store_test.go` 已经连着真实 PostgreSQL 覆盖了所有表的读写映射，就没必要再留一份纯内存的模型互转测试。

依赖真实 PostgreSQL、Redis、Kafka 的测试，跑短测试（`-short`）时跳过；不带 `-short` 跑时，只要连不上服务必须直接 `t.Fatalf` 报错，不允许静默 `t.Skip` 混成通过。

## 各层测什么

| 层级 | 位置 | 测什么 | 依赖 |
| :--- | :--- | :--- | :--- |
| 领域逻辑 | `internal/domain/*_test.go` | 任务与 Endpoint 校验、边界常量、RFC 7230 Header 校验、幂等比对、执行前条件检查（含时钟容差）、状态流转、HMAC 签名、退避计算 | 纯内存，零外部依赖 |
| HTTP 接口 | `internal/api/*_test.go` | 路由注册、JSON 解析、错误码映射、内部错误脱敏、取消抢锁流程、Endpoint 并发更新 409 冲突响应 | 内存 fake 与 `httptest` |
| 调度与执行 | `internal/scheduler/*_test.go`、`internal/worker/*_test.go` | Outbox 靠 `LISTEN/NOTIFY` 与定时兜底轮询唤醒、积压连批搬空、报错等待重试；Worker Step A~E 流程、锁续期 TTL、并发上限与优雅停机 | 内存 fake |
| 配置与日志 | `internal/config/*_test.go`、`internal/logger/*_test.go` | 环境变量解析与校验、日志脱敏、GORM 慢查询日志适配 | 纯内存 |
| 出站 HTTP 客户端 | `internal/infra/httpclient/*_test.go` | 原始字节与签名头透传、超时、响应片段截断、`CheckRedirect` 禁重定向、2xx 读体失败保护、状态码分类 | 本地 `httptest.Server` |
| PostgreSQL 存储 | `internal/infra/pg/store_test.go` | SQL 事务与数据库约束：`ON CONFLICT DO NOTHING` 并发幂等、`BYTEA` 原始字节保真、乐观锁更新与取消竞态、`FOR UPDATE SKIP LOCKED` 队列锁定与回滚、`LISTEN/NOTIFY` 触发器、Endpoint `updated_at` 乐观锁 | 跑完迁移的真实 PostgreSQL |
| Redis 客户端 | `internal/infra/redis/*_test.go` | Redis 原子操作与 Lua 脚本：`SetNX` 互斥、校验持锁人后再续期（`PEXPIRE`）与释放、过期接管与防误删；M2/M3 的延迟池与流控脚本 | 真实 Redis（DB 15） |
| Kafka 读写 | `internal/infra/kafka/reader_test.go`、`stream_test.go` | `reader_test.go` 测内存里的连续 Offset 水位推进与分区撤销边界；`stream_test.go` 测真实 Broker 生产、消费、乱序 `Ack` 与重启 Offset 恢复 | 内存 + 真实 Kafka |
| 端到端链路测试 | `tests/integration/<flow>_flow_test.go` | 一个文件对应一条完整业务链路：真实组装 `api`、`scheduler`、`worker`、`pg`、`redis`、`kafka`、`httpclient` 和 `httptest` 下游，验证跨层协作，不重复测单测里已经覆盖的细碎参数分支 | 真实 PG + Redis + Kafka |
| 多进程黑盒脚本 | `scripts/verify_m*.sh` | 编译并启动真实的 `ferry-server`、`ferry-worker`、`chaos-receiver` 进程，验证端口监听、健康探针与真实 `SIGTERM` 优雅停机 | 真实容器与编译产物 |

## 怎么跑测试

平时改完代码做快速检查，不用起 Docker，直接跑纯内存测试：

```bash
make test                      # 实际执行 go test -race -short ./...
```

这条命令一般 1 到 2 秒就能跑完，`pre-commit` 钩子和 GitHub Actions 的 `verify` 任务跑的也是它。

要验证数据库、Redis、Kafka 或完整链路时，先起本机容器，再跑全量测试：

```bash
make up                        # 启动 PG、Kafka、Redis 并等待健康检查通过
make test-integration          # 实际执行 go test -race -count=1 ./...
```

`make test-integration` 会把纯内存单测、`internal/infra` 里的真服务测试，以及 `tests/integration` 里的全部链路测试一起跑完。命令里的 `-count=1` 用来关掉 Go 的测试结果缓存，避免代码没变、但外部服务状态变了时直接复用上次的 `(cached)` 结果。

调试单个包或单条链路时，不用等全量跑完，在容器启动后用 `-run` 指定测试名正则：

```bash
go test -v -race -count=1 -run '^TestStore_' ./internal/infra/pg
go test -v -race -count=1 -run '^TestImmediateDeliveryFlow$' ./tests/integration
```

用 `-run` 时要留意输出里有没有 `[no tests to run]`。正则没匹配到任何用例时，`go test` 依然会退出 0。

阶段里程碑验收时，在容器运行状态下再跑多进程黑盒脚本：

```bash
./scripts/verify_m1.sh
```

`make test-integration` 是在单个 Go 测试进程里把各模块对象拼起来测，`verify_m1.sh` 则是启动三个独立二进制进程走真实 HTTP 端口和系统信号。日常改动不用每次都跑它，里程碑收口前跑一遍即可。

测完不需要容器时，执行 `make down` 停掉；如果要连数据卷一起清掉，跑 `docker compose -f deploy/docker-compose.yml down -v`。

### `-short` 是怎么起作用的

Go 内置了 `-short` 开关，代码里可以通过 `testing.Short()` 读到当前是不是短测试模式。所有需要连真实 PG、Redis、Kafka 的测试，都会在准备连接的辅助函数开头加上这三行：

```go
if testing.Short() {
	t.Skip("skipping real PostgreSQL test in -short mode")
}
```

跑 `make test` 时带了 `-short`，`testing.Short()` 为 `true`，需要连容器的测试自动跳过，只留纯内存测试运行。跑 `make test-integration` 时不带 `-short`，`testing.Short()` 为 `false`，所有测试都会真正连容器执行；如果忘了先 `make up`，连不上服务会立刻 `t.Fatalf` 报错。

仓库里不给这类测试加 `//go:build integration` 标签，也不起 `*_integration_test.go` 后缀。加了 build tag 之后，日常 `go test ./...` 和 `golangci-lint` 根本不会编译这些文件，改了函数签名或字段名时测试悄悄坏掉也没人知道，只能等到 CI 跑集成任务时才暴露。保持普通 `*_test.go` 文件名配合 `-short` 跳过，每次本地构建和 lint 都会检查全部测试源码，只在执行阶段按需跳过。

## 测试环境与数据隔离

真容器测试默认直连 `deploy/docker-compose.yml` 暴露在本机的端口。CI 或其他环境可以通过环境变量覆盖连接地址；GitHub Actions 的 `integration` 任务已经在 `.github/workflows/ci.yml` 里填好了这些默认值，不需要额外配置 GitHub Secrets。

| 环境变量 | 默认值 | 隔离方式 |
| :--- | :--- | :--- |
| `FERRY_TEST_PG_DSN` | `postgres://ferry:ferry@localhost:5432/ferry?sslmode=disable` | 每条测试创建独立临时库 `ferry_<scope>_<pid>_<ts>` 并运行内嵌 Goose 迁移，`t.Cleanup` 自动执行 `DROP DATABASE ... WITH (FORCE)` 删除 |
| `FERRY_TEST_REDIS_ADDR`、`FERRY_TEST_REDIS_DB`、`FERRY_TEST_REDIS_PASSWORD` | `localhost:6379`、`15`、空 | 固定连测试专用 DB 15，不碰开发常用的 DB 0。任务级 Key 用新生成的 UUIDv7 天然隔离，后续测到全局共享 Key（如 `ferry:delayed`）时在测试前后清理 DB 15 |
| `FERRY_TEST_KAFKA_BROKERS` | `localhost:9092` | 每条测试创建随机 Topic（`ferry.test.ready.<short-id>` 或 `ferry.it.ready.<short-id>`）和独立消费组 `groupID`，`t.Cleanup` 通过 `kadm.DeleteTopics` 删除 |

## 怎么写新测试

### 文件和函数命名

模块内测试统一按 `<subject>_test.go` 命名，比如 `task_test.go`、`store_test.go`、`execution_lock_test.go`、`stream_test.go`。优先用黑盒包名（`package pg_test`、`package redis_test`、`package kafka_test`），只从对外接口测；只有确实要测非导出内部算法时才用同包名，比如 `kafka/reader_test.go`。

端到端链路测试放在 `tests/integration/<flow>_flow_test.go`，比如 `immediate_delivery_flow_test.go`、`cancel_and_lock_flow_test.go`、`graceful_shutdown_flow_test.go`，公共启动与清理代码放在 `tests/integration/harness_test.go`。

测试函数按 `Test<Subject>_<Behavior>` 或清晰的场景名来起，端到端链路统一叫 `Test<Flow>Flow`。同一主题有多组边界输入时，写成表驱动测试并用 `t.Run` 拆出子用例。

### Fake 与异步等待

同一个包内，一个依赖接口只维护一份内存 fake 和一个构造函数，包内所有测试共用它。新用例需要特殊行为（如注入并发冲突或记录回调）时，直接在现有 fake 上加字段或钩子函数，不另起一套类型。包里只有一个测试文件时把 fake 放在同文件顶部即可，拆出多个测试文件后挪到同包的 `fake_<port>_test.go`（如 `fake_task_store_test.go`）。不同包之间不抽跨包共用的大一统 mock，各包只实现自己依赖的那几个接口方法。

异步链路禁止靠 `time.Sleep` 盲等结果。用带超时的条件轮询 `waitUntil`、明确的就绪信号，或者用 channel 控制并发交错时序，比如在 `SKIP LOCKED` 测试里用 `bothEntered` 和 `releaseBoth` 让两个事务同时进锁。

集成测试脚手架在 `t.Cleanup` 里必须按反顺序收尾：先停 `Worker` 和 `OutboxRelay`，再关 `httptest.Server`、Kafka、Redis，最后删临时 PG 库。如果先关了连接、后台 goroutine 还在跑，收尾时很容易触发竞态或冒出一堆无意义的报错日志。

### 静态检查约束

测试文件同样受 `.golangci.yml` 检查（`run.tests: true`）：

- 构造 HTTP 请求一律用 `http.NewRequestWithContext` 或 `httptest.NewRequestWithContext`（满足 `noctx`）
- 每个 `*http.Response.Body` 都要关闭（满足 `bodyclose`）
- `internal/**` 下的测试同样受包依赖边界 `depguard` 约束，需要跨包拼装完整链路时放到根目录 `tests/integration/`

## 后续 Milestone 怎么补测试

Milestone 2（延迟调度、退避重试、对账与死信重放）：

- `internal/domain/backoff_test.go`：指数退避抖动范围与 `Retry-After` 封顶计算
- `internal/infra/redis/delayed_pool_test.go`：30s 租约 Lua 与按分数比对后再 `ZREM`
- `internal/infra/pg/store_test.go`：补充死信分批重放与游标对账扫描查询
- `tests/integration/delayed_retry_flow_test.go`、`dlq_replay_flow_test.go`、`redis_flush_reconcile_flow_test.go`：三条新增端到端链路

Milestone 3（三维流控与背压隔离）：

- `internal/infra/redis/flow_control_test.go`：`admit_endpoint.lua` 与 `complete_delivery.lua` 熔断、虚拟排号限流与并发槽位回收
- `tests/integration/endpoint_isolation_flow_test.go`：慢或故障 Endpoint 熔断不拖垮健康 Endpoint 的链路

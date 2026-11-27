# Go 测试规范（Go Testing Standard）

编写或评审 Go 测试、或决定一条行为归哪一层测时阅读本文件。通用分级与评审口径见 [CODING_STANDARDS.md](../../../CODING_STANDARDS.md)。

## 基本原则与测试归属（Owners）

一个行为只在拥有它的那一层测一次。写测试前先明确归属层，别的层已经兜住的分支不重复复制。

- 并发锁与竞态（`FOR UPDATE SKIP LOCKED`、复合乐观锁、Redis `Execution Lock` 互斥）必须在真实中间件中验证；内存 fake 证明不了数据库与 Redis 的锁语义。
- 若真服务测试已经覆盖完整表读写映射（如 `internal/infra/pg/store_test.go`），删除纯内存的重复 PO 互转测试；端到端集成测试不重复覆盖单测已测过的细碎参数分支。

| 层级 | 位置 | 测什么 | 依赖 |
| :--- | :--- | :--- | :--- |
| 领域逻辑 | `internal/domain/*_test.go` | `Endpoint`/`Task` 不变量、边界常量、Header 校验、幂等比对、前置守卫、状态流转、HMAC 签名、退避计算 | 纯内存，零外部依赖 |
| HTTP 控制面 | `internal/api/*_test.go` | 路由注册、JSON 解析、错误码映射、内部错误脱敏、取消抢锁流程、`Endpoint` 并发更新 409 冲突 | 内存 fake 与 `httptest` |
| 调度与执行 | `internal/scheduler/*_test.go`、`internal/worker/*_test.go` | Outbox `LISTEN/NOTIFY` 与定时兜底唤醒、积压连批搬空、退避重试；Worker Step A~E 流程、锁续期 TTL、并发上限与优雅停机 | 内存 fake |
| 配置与日志 | `internal/config/*_test.go`、`internal/logger/*_test.go` | 环境变量解析与强校验、采样日志与 `WithTaskID` 透传、GORM 慢查询日志适配 | 纯内存 |
| 出站 HTTP 客户端 | `internal/infra/httpclient/*_test.go` | 原始字节与签名头透传、超时、响应截断、禁 3xx 重定向、2xx 读体失败保护、状态码分类与 `Retry-After` 解析 | 本地 `httptest.Server` |
| PostgreSQL 存储 | `internal/infra/pg/store_test.go` | SQL 事务与约束：`ON CONFLICT DO NOTHING` 并发幂等、`BYTEA` 保真、乐观锁更新与取消竞态、`SKIP LOCKED` 锁定与回滚、`LISTEN/NOTIFY` 触发器、`Endpoint` `updated_at` 乐观锁 | 跑完迁移的真实 PostgreSQL |
| Redis 适配器 | `internal/infra/redis/*_test.go` | 原子操作与 Lua 脚本：`SetNX` 互斥、验主续期（`PEXPIRE`）与释放、过期接管防误删、延迟池租约与流控脚本 | 真实 Redis（DB 14） |
| Kafka 适配器 | `internal/infra/kafka/reader_test.go`、`stream_test.go` | `reader_test.go` 测连续 Offset 水位推进与分区撤销边界；`stream_test.go` 测真实 Broker 生产、消费、乱序 `Ack` 与重启恢复 | 内存 + 真实 Kafka |
| 端到端集成测试 | `tests/integration/<scene>_integration_test.go` | 一条核心业务主链路一个文件：真实组装 `api`、`scheduler`、`worker`、`pg`、`redis`、`kafka`、`httpclient` 与 `httptest` 下游，验证跨层协作 | 真实 PG + Redis + Kafka |
| 多进程黑盒脚本 | `scripts/verify_m*.sh` | 编译并启动真实 `ferry-server`、`ferry-worker`、`chaos-receiver` 独立进程，验证端口监听、健康探针与真实 `SIGTERM` 优雅停机 | 真实容器与编译产物 |

**Judgement.** 保留任何测试文件前反问一句：删掉它之后，有没有真实 bug 会漏过去？没有就删。

## 运行与校验入口（Verifying a change）

平时改代码做快速反馈，直接跑纯内存测试（1~2 秒，对应 `pre-commit` 与 CI `Lint, Test & Build`）：

```bash
make test                      # go test -race -short ./...
```

涉及数据库、Redis、Kafka 或端到端链路时，先启动容器再跑全量测试（对应 CI `Integration Tests (PG + Kafka + Redis)`）：

```bash
make up                        # 启动 PG、Kafka、Redis 并等待健康检查通过
make test-integration          # go test -race -count=1 ./...
```

`-count=1` 用于禁用 Go 测试结果缓存，避免代码未变但容器状态已变时命中 `(cached)` 假通过。

单包或单用例调试时，使用 `-run` 并**必须核对输出不含 `[no tests to run]`**（正则未匹配任何用例时 `go test` 依然退出 0）：

```bash
go test -v -race -count=1 -run '^TestStore_' ./internal/infra/pg
go test -v -race -count=1 -run '^TestImmediateDeliveryIntegration$' ./tests/integration
```

里程碑收口前在容器运行状态下执行对应黑盒脚本 `make verify-m1`（刚跑过 `make test-integration` 或在 CI 中执行时可传 `SKIP_TESTS=1 make verify-m1` 跳过脚本内重复单测）。测完用 `make down` 停止容器，需要清卷时运行 `docker compose -f deploy/docker-compose.yml down -v`。

### `-short` 机制（禁止 `//go:build integration`）

需要真实 PG、Redis 或 Kafka 的测试，在准备连接的辅助函数开头统一写入：

```go
if testing.Short() {
	t.Skip("skipping real PostgreSQL test in -short mode")
}
```

- 跑 `make test`（带 `-short`）时自动跳过容器测试；
- 跑 `make test-integration`（不带 `-short`）时，只要连不上对应中间件必须直接 `t.Fatalf` 失败闭合，严禁静默 `t.Skip` 混成通过；
- **禁止**给集成测试加 `//go:build integration` 标签（即便端到端文件名使用 `*_integration_test.go` 也不加 build tag），确保平时 `go test` 和 `golangci-lint` 始终编译检查全部集成测试源码，避免改了函数签名后集成测试悄悄损坏。

## 测试环境与数据隔离

真容器测试默认直连 `deploy/docker-compose.yml` 暴露在本机的端口，支持通过环境变量覆盖：

| 环境变量 | 默认值 | 隔离方式 |
| :--- | :--- | :--- |
| `FERRY_TEST_PG_DSN` | `postgres://ferry:ferry@localhost:5432/ferry?sslmode=disable` | 每条测试创建独立临时库 `ferry_<scope>_<pid>_<ts>` 并执行内嵌 Goose 迁移，`t.Cleanup` 运行 `DROP DATABASE ... WITH (FORCE)` 清理 |
| `FERRY_TEST_REDIS_ADDR`、`FERRY_TEST_REDIS_UNIT_DB`、`FERRY_TEST_REDIS_DB`、`FERRY_TEST_REDIS_PASSWORD` | `localhost:6379`、`14`、`15`、空 | `internal/infra/redis` 固定使用 DB 14、`tests/integration` 固定使用 DB 15，避免多包并行冲突；任务级 Key 用新 UUIDv7 隔离，测全局共享 Key（如 `ferry:delayed`）前后清理本库 |
| `FERRY_TEST_KAFKA_BROKERS` | `localhost:9092` | 每条测试创建随机 Topic（`ferry.test.ready.<short-id>` 或 `ferry.it.ready.<short-id>`）与独立消费组，`t.Cleanup` 用 `kadm.DeleteTopics` 删除 |

## 文件、包名与测试写法

- **黑盒包名优先**：测试对外行为的测试文件使用黑盒包名 `package <pkg>_test`（由 `.golangci.yml` `testpackage` 强制校验）；仅在独立验证非导出核心算法（如 `internal/infra/kafka/reader_test.go` 水位算法）时才显式豁免白盒包名，或在同包放 `export_test.go` 暴露必要切口。
- **文件命名**：
  - 单元测试：`<subject>_test.go`；
  - 同包多个测试文件共用内存 fake 时，拆出 `fake_<port>_test.go`；
  - 端到端集成测试：`tests/integration/<scene>_integration_test.go`，公共启停与清理放在 `tests/integration/harness_test.go`。
- **函数与表驱动命名**：
  - 单元测试按 `Test<Subject>_<Behavior>` 命名；端到端集成测试统一按 `Test<Scene>Integration` 命名；
  - 同类多组边界输入写成表驱动测试并用 `t.Run` 拆分，子用例名只用字母、数字、下划线或连字符，**不得包含 `/`**（防止 `go test -run` 按斜杠错切）。
- **辅助函数规范**：会调用 `t.Error` / `t.Fatal` 的测试辅助函数，必须将 `t *testing.T` 作为第一参数并在函数开头调用 `t.Helper()`，确保报错行号指向调用方用例。

## Fake 纪律、异步等待与清理顺序

- 同一个包内，一个消费方 Port 接口只维护一份内存 fake，新用例需要注入并发冲突或钩子时扩展现有 fake，不另起新类型；不跨包抽大一统 mock。
- **禁止 Fake 通过嵌入接口（embedded interface）满足编译检查**（漏实现的方法会在运行期 nil-panic）。单场景 stub 未配置的方法必须显式失败（`t.Fatalf` 或返回错误），完整内存模拟 fake 按真实语义返回（如查无此物返回 `domain.ErrTaskNotFound`）。
- Fake 上的调用计数器或记录切片，只在真有断言读取它时才添加。
- **异步链路禁止 `time.Sleep` 盲等结果**：用带超时的条件轮询 `waitUntil`、就绪信号或 channel 精确编排并发交错（例如 `SKIP LOCKED` 用 `bothEntered` 与 `releaseBoth` 卡住两个事务同时进锁）。
- 集成测试脚手架在 `t.Cleanup` 中必须**逆序收尾**：先停 `Worker` 与 `OutboxRelay`，再关 `httptest.Server`、Kafka、Redis，最后删临时 PostgreSQL 库，避免先断连接引发后台 goroutine 竞态或刷屏报错。

## 静态检查与迁移测试

- 测试源码受 `.golangci.yml`（`run.tests: true`）全量约束：构造 HTTP 请求一律用 `http.NewRequestWithContext` 或 `httptest.NewRequestWithContext`（`noctx`）；关闭每个 `*http.Response.Body`（`bodyclose`）；`internal/**` 下的测试同样受 `depguard` 约束，需要跨包拼装完整链路时放到 `tests/integration/`。
- 普通建表/加索引迁移由 `internal/infra/pg/store_test.go` 在临时库运行 `RunMigrations` 验证；涉及**搬迁、转换或回填存量数据**的迁移，必须补写迁移测试：先将临时库升至 `N-1` 版本写入旧格式数据 → 升至 `N` 版本断言转换结果 → 再执行 `Down` 验证回滚后旧版代码仍可正确读回。

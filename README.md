# Ferry

**分布式异步 HTTP 任务调度与 Webhook 可靠投递平台**

[![CI](https://github.com/awu123-create/ferry/actions/workflows/ci.yml/badge.svg)](https://github.com/awu123-create/ferry/actions/workflows/ci.yml)
[![Go Version](https://img.shields.io/badge/Go-1.27-00ADD8?logo=go)](./go.mod)
[![Architecture](https://img.shields.io/badge/Docs-Architecture_%26_ADRs-0969da)](./docs/ARCHITECTURE.md)

Ferry 是一个基于 **Go + PostgreSQL + Kafka + Redis** 构建的高可靠异步 HTTP 调度与 Webhook 投递引擎。系统将**内部微服务的立即/延迟异步任务回调**与**面向外部商户的防篡改签名 Webhook 投递**统一在单一 HTTP Push 内核之上，以 **PostgreSQL 为唯一业务真相（Source of Truth）**、**Kafka 承接高吞吐削峰分发**、**Redis 驱动毫秒级时间调度与 Endpoint 级流控隔离**，确保任意单跳进程崩溃或缓存清空下任务不丢、不乱、不拖垮健康下游。

---

## 为什么构建 Ferry（Why Ferry?）

在常规业务内部，同库异步队列（如 `PostgreSQL + River`）能够以最小基础设施成本提供事务入队保障；但当调度系统独立演进为跨业务线共用的 HTTP 任务与外部 Webhook 平台时，单存储方案会面临明显边界：

- **高频流控顺延与状态抖动冲击数据库**：大量限流回退、熔断计数与退避排期若直接频繁 `UPDATE` 同一张数据库队列行，会引发严重的行锁竞争与 MVCC 死元组膨胀；
- **不可信慢下游队头阻塞与调度派发耦合**：某个外部商户接口持续超时（如 10 秒无响应）会迅速耗尽全局固定 Worker 并发槽位，且单库轮询难以实现消费组弹性扩缩容与分区级暂停背压；
- **单中间件短板**：纯内存队列（Redis）难以在全库重启或清空后自行恢复在途账本，纯日志流（Kafka）无法在入口同步做唯一键幂等拦截与任意时间定时触发。

| 能力维度 | 单库队列 (`PG + River`) | 纯内存队列 (`Redis`) | 纯消息流 (`Kafka`) | **Ferry (`PG + Kafka + Redis`)** |
| :--- | :---: | :---: | :---: | :--- |
| 入口同步唯一键幂等与即时查状态 | 支持 | 弱（非持久化账本） | 不支持 | **支持**（PG `UNIQUE` 约束 + 原始字节 `BYTEA` 落库即查） |
| 高频流控防 DB 膨胀与派发水平扩展 | 较弱（MVCC 写放大） | 流控强 / 账本弱 | 扩展强 / 无任意延迟 | **支持**（Redis 内存消纳顺延与流控，Kafka 解耦派发与消费组扩缩） |
| 任意时间戳定时与动态退避重试 | 支持 | 支持（ZSet） | 仅固定阶梯 Topic | **支持**（`ferry:delayed` ZSet + 小时级指数退避 + `Retry-After`） |
| `Endpoint` 级动态限流与防空转隔离 | 需自建外挂组件 | 支持 | 分区重平衡成本高 | **支持**（Redis Lua 原子准入 + 按 `1/QPS` 虚拟排号摊平顺延） |
| 缓存节点内存清空后零丢单自愈 | 原生支持 | 依赖 AOF，极端仍丢 | 不涉及 | **支持**（`60s` PG Reconciler 游标流式扫描并回灌延迟池） |

---

## 核心机制与可靠性特性（Core Features）

- **双通道事务发件箱零丢单（Dual-Channel Transactional Outbox）**
  `POST /v1/tasks` 在单次 PG 事务内原子写入 `tasks` 与 `outbox`。Relay 协程拆分为**立即通道**（`deliver_at <= now()` 直推 Kafka）与**延迟通道**（`deliver_at > now()` 写延迟池，遇 Redis 故障独立退避不堵队头），配合 `FOR UPDATE SKIP LOCKED` 与单批 `5s` 硬超时推入后物理 `DELETE`，使 `outbox` 常驻行数趋近于 0。
- **30 秒租约延迟池 + PG 游标对账自愈（Lease ZSet & Cursor Reconciler）**
  Redis `ferry:delayed`（ZSet）仅存轻量 `task_id` 与目标毫秒时间戳 `score`。Forwarder 通过 Lua 续期 `30s` 调度租约推入 Kafka（固定按 `task_id` 分区），确认后**比对租约分数条件 `ZREM`** 防误删新调度；配合 `60s` PG Reconciler 游标循环扫描超期任务并以 `ZADD NX` 回灌延迟池，即使执行 `redis-cli FLUSHALL` 清空内存也可无损自愈。
- **Endpoint 级三维流控与虚拟排号防空转（3D Flow Control & Virtual Pacing）**
  Worker 投递前执行单次 Redis Lua 脚本 `admit_endpoint.lua`（统一采用 Redis `TIME`），原子完成 **连续失败自动熔断（含 `HALF_OPEN` 试探过期保护） -> QPS 令牌桶 -> 带过期时间戳的 ZSet 并发槽位**，并通过对称的 `CompleteDelivery` 归还槽位及更新熔断/`429` 冷却。受限或熔断任务按 `1/QPS` 虚拟排号游标分时顺延回退 `ferry:delayed`（不扣减重试次数，受 `deadline_at` 硬边界截断），杜绝大积压空转风暴。
- **全链路三层幂等与原始字节签名（Three-Layer Idempotency & Raw-Byte Signing）**
  1. **入口层**：PG `UNIQUE(endpoint_id, idempotency_key)` + 原始 JSON 字节 `BYTEA` 保真与 `payload_sha256` 校验（同键同载荷返回 `200`，异载荷返回 `409 Conflict`）；
  2. **执行与取消层**：Redis `Execution Lock`（写库后验主放锁、争用保底顺延、`Cancel` 主动抢锁互斥）+ PG `(replay_count, attempt)` 复合乐观版本五态 CAS 守卫；
  3. **投递层**：每次请求固定携带 `X-Ferry-Task-ID`、`X-Ferry-Attempt`、`X-Ferry-Timestamp` 与基于原始字节签发的 `X-Ferry-Signature`（`sha256=HMAC-SHA256(secret, timestamp.body)`）。
- **严格错误分级与受控死信重放（State Machine & Controlled DLQ Replay）**
  区分可重试错误（`5xx`、`408`、`429`、网络超时，默认最多 6 次尝试、跨度达小时级且支持环境变量覆盖）与客户端契约错误（其余 `4xx` 单次尝试后直入 `DEAD_LETTER`），独立 `task_attempts` 明细表按 `(replay_count, attempt_no)` 记录完整轨迹。支持单条及分批（`<= 500`）重放死信并重走主链路流控，防止重放洪峰二次打崩下游。
- **全链路可观测与混沌压测靶场（Observability & Chaos Target）**
  暴露独立 Prometheus `/metrics` 端点（Outbox 积压、ZSet 深度、Kafka Lag、Endpoint 熔断状态与 P50/P95/P99 投递耗时），配套内置 `chaos-receiver` 故障注入靶场（按需模拟慢响应、`500` 错误风暴与 `429` 限流）和 `ferry-bench` 并发发压器。

---

## 架构概览（Architecture）

```text
  [ Upstream Services ]
           |
           | POST /v1/tasks (Idempotency-Key, EndpointID, Payload, DeliverAt?)
           v
  [ ferry-server API ] ---> [ PostgreSQL: tasks + outbox (唯一真相) ] <--- 60s Reconciler 对账
                                     |
              +----------------------+----------------------+
              | Outbox Relay 双通道 (SKIP LOCKED LIMIT 100 + 5s 超时)
              v (deliver_at <= now)                         v (deliver_at > now)
  [ Kafka: ferry.tasks.ready ] <====(100ms Lua 30s租约)==== [ Redis ZSet: ferry:delayed ]
              |                    推Kafka确认后比对分数ZREM         ^           ^
              v                                                   |           |
  [ ferry-worker ]                                                | 虚拟排号   | 指数退避/429
    1. Redis 执行互斥锁 + 早到校验 + 复合乐观锁守卫 + deadline 截断  | 顺延回退   |
    2. Lua [admit_endpoint.lua]: 熔断 -> 1/QPS 排号 -> 并发槽位 ---+           |
    3. HTTP POST (BYTEA 原文 + X-Ferry-Task-ID + HMAC 签名) ---> [ Target ]
    4. 写 task_attempts + CAS 流转 & CompleteDelivery 结算 + 验主放锁 ---------+
```

关于 PG 写完未发 Kafka 宕机、Redis 断连背压与内存清空自愈、下游超时风暴熔断以及 `SIGTERM` 优雅停机排空的逐跳推演与完整 DDL 定义，详见 [`docs/ARCHITECTURE.md`](./docs/ARCHITECTURE.md)。

---

## 技术栈（Tech Stack）

| 层级 | 选型 | 关键工程配置 |
| :--- | :--- | :--- |
| 语言与 JSON | `Go 1.27` + `encoding/json/v2` | 统一锁定 `go.mod` 与 `golangci-lint v2` 工具链版本，业务 JSON 编解码统一采用 `encoding/json/v2` 与 `jsontext` |
| HTTP 接入与投递 | 标准库 `net/http` | Go 1.22+ 增强 `ServeMux`；手动调优 `http.Transport` 连接池并用 `io.LimitReader` 防大包 OOM |
| 数据访问与迁移 | `GORM` + `Goose` + `UUIDv7` | DML 归 GORM（`SkipDefaultTransaction`、`clause.OnConflict`/`SKIP LOCKED`），DDL/部分索引/触发器归 `goose`，禁用 `AutoMigrate` |
| 消息总线与调度 | `twmb/franz-go` + `redis/go-redis/v9` | Kafka 原生幂等生产者与分区 `Pause` 背压；Redis `NewScript` 自动处理 `EVALSHA` 与重启后 `NOSCRIPT` 重载 |
| 日志与可观测 | `go.uber.org/zap` + `prometheus/client_golang` | JSON 日志启用 `Sampler` 采样防故障刷爆磁盘，统一接管 GORM 慢 SQL 告警；独立暴露 `/metrics` |
| 基础设施容器 | `PostgreSQL 16` + `Kafka 3.9 (KRaft)` + `Redis 7.4` | 无 ZooKeeper；Redis 强制 `--maxmemory-policy noeviction`；配套 `Prometheus` + `Grafana` |

---

## 仓库结构与本地开发（Repository Layout & Development）

当前仓库已落地 Milestone 1 立即投递主链路与工程规范结构（后续 M2 ~ M4 规划见 [`docs/specs/roadmap.md`](./docs/specs/roadmap.md)）：

```text
Ferry/
├── .env.example                 # 与 internal/config/config.go 对齐的环境变量示例
├── .github/
│   ├── pull_request_template.md # GitHub PR 正文模板
│   └── workflows/ci.yml         # GitHub Actions: verify（格式/lint/短单测/构建）与 integration（真容器集成测试）
├── .golangci.yml                # golangci-lint v2 静态检查、depguard 分层与 json_v2 门禁
├── .pre-commit-config.yaml      # 本地提交钩子（基础检查、ADR 编号、gofmt、增量 lint、短单测）
├── AGENTS.md                    # 场景路由入口（指向工程规范与协作文档）
├── CODING_STANDARDS.md          # 全局编码基线（Hard / Judgement / Smell baseline override）
├── GLOSSARY.md                  # 统一语言词表（单一限界上下文，零基础设施实现泄漏）
├── Makefile                     # 开发、容器编排、集成测试与验收入口 (setup-hooks / fmt / lint / test / test-integration / up / down / migrate / verify-m1 / build)
├── go.mod                       # Go 1.27 模块定义
├── cmd/
│   ├── ferry-server/            # 组装根：API 网关与 OutboxRelay
│   ├── ferry-worker/            # 组装根：Kafka 消费、Execution Lock 与 HMAC 签名投递
│   ├── chaos-receiver/          # 本地验证用下游接收靶场（验签、计数、指定状态码与延迟）
│   └── ferry-bench/             # 并发压测 CLI 占位入口（Milestone 3）
├── internal/
│   ├── domain/                  # 纯 Go 领域核（Endpoint / Task / DeliveryAttempt / 签名与状态机）
│   ├── api/                     # 入站 HTTP 控制面深模块
│   ├── scheduler/               # OutboxRelay 发件箱搬运深模块
│   ├── worker/                  # Step A~E 投递执行与优雅停机深模块
│   ├── infra/                   # PG / Redis / Kafka / HTTP 出站适配器及真容器测试
│   └── config/, logger/, metrics/
├── migrations/                  # Goose SQL 迁移脚本与 embed.go
├── tests/integration/           # 真实 PG + Kafka + Redis 端到端集成测试
├── deploy/                      # docker-compose.yml（PostgreSQL 16 / Kafka 3.9 KRaft / Redis 7.4）
├── scripts/                     # verify_m1.sh 多进程黑盒验收脚本
└── docs/
    ├── ARCHITECTURE.md          # 架构设计总纲
    ├── adr/                     # ADR 0001 ~ 0008
    ├── agents/                  # Issue tracker、Triage 标签、领域文档与 GitHub 协作规约
    ├── specs/                   # 演进路线图与切片规划（roadmap.md）
    └── standards/               # 分层工程规范（go/code.md、go/testing.md、database.md、records.md）
```

### 快速校验与本地运行

前置要求：本机已安装 Go 1.27、Docker Compose、[`golangci-lint v2`](https://golangci-lint.run/) 与 [`pre-commit`](https://pre-commit.com/)。

```bash
# 1. 安装 Git pre-commit 钩子（首次拉取仓库后执行）
make setup-hooks

# 2. 纯内存快速校验（零容器依赖）
make fmt
make lint
make test
make build

# 3. 启动本机 PG / Kafka / Redis 并运行全量真容器测试
make up
make test-integration

# 4. 多进程黑盒验收（Milestone 1）
make verify-m1

# 5. 停止本机容器
make down
```

---

## 演进路线图（Roadmap）

详细的 Milestone 1 ~ 4 切片边界、端口矩阵与验收规划见 [`docs/specs/roadmap.md`](./docs/specs/roadmap.md)（活工单状态与进度统一维护在 GitHub Issues）：

- [x] **Foundation**：架构总纲、统一语言词表（`GLOSSARY.md`）、ADR 0001 ~ 0008、分层工程规范（`CODING_STANDARDS.md` 与 `docs/standards/`）、Go 1.27 工程骨架、`Makefile`、`golangci-lint v2`、`pre-commit` 与 GitHub Actions CI
- [x] **Milestone 1**：最小端到端可靠立即直投链路（Docker Compose、增量初始迁移、幂等入口 API、Outbox 立即通道、`Execution Lock`、Kafka Worker HMAC 签名投递与 `SIGTERM` 优雅停机）
- [ ] **Milestone 2**：任意未来延迟调度、小时级指数退避与 `Retry-After`、Redis 租约延迟池、PG 游标对账自愈与死信批量重放
- [ ] **Milestone 3**：Endpoint 三维流控（熔断 / `1/QPS` 虚拟排号 / 并发槽位）、Prometheus + Grafana 可观测闭环与混沌压测
- [ ] **Milestone 4**：基于 `/v1/*` REST API 的极简只读监控与死信批量重放控制台

---

## 架构文档、规范与设计决策（Documentation & ADRs）

- [统一语言词表 (`GLOSSARY.md`)](./GLOSSARY.md)
- [全局编码规范 (`CODING_STANDARDS.md`)](./CODING_STANDARDS.md)
- [分层工程规范 (`docs/standards/`)](./docs/standards/go/code.md)（[Go 代码](./docs/standards/go/code.md) · [Go 测试](./docs/standards/go/testing.md) · [数据库与迁移](./docs/standards/database.md) · [记录同步](./docs/standards/records.md)）
- [架构设计总纲 (`docs/ARCHITECTURE.md`)](./docs/ARCHITECTURE.md)
- [分阶段演进路线图 (`docs/specs/roadmap.md`)](./docs/specs/roadmap.md)
- [ADR-0001: 统一核心业务模型为可靠 HTTP 任务投递引擎](./docs/adr/0001-unified-http-task-delivery-model.md)
- [ADR-0002: 入口事务一致性采用 PG 同事务 Outbox 与 Go 批量搬运协程](./docs/adr/0002-transactional-outbox-batch-relay.md)
- [ADR-0003: 延迟与退避重试采用 Redis ZSet 租约倒计时池加 PG 对账兜底](./docs/adr/0003-redis-zset-lease-and-pg-reconciler.md)
- [ADR-0004: Endpoint 级三维流控隔离与全链路三层幂等机制](./docs/adr/0004-endpoint-flow-control-and-three-layer-idempotency.md)
- [ADR-0005: 状态机与投递明细、死信受控重放及可观测优先交付边界](./docs/adr/0005-state-machine-dlq-replay-and-observability-boundary.md)
- [ADR-0006: Go 1.27 技术栈选型、GORM+Goose 职责边界与工程化门禁](./docs/adr/0006-tech-stack-and-engineering-tooling.md)
- [ADR-0007: 轻量战术 DDD 聚合边界、五态持久化修正与深模块切口设计](./docs/adr/0007-tactical-ddd-and-deep-module-seams.md)
- [ADR-0008: 调度并发围栏、流控排号防空转与载荷字节保真加固](./docs/adr/0008-concurrency-fencing-and-flow-control-hardening.md)

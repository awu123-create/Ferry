# Ferry

**分布式异步 HTTP 任务调度与 Webhook 可靠投递平台**

[![CI](https://github.com/awu123-create/ferry/actions/workflows/ci.yml/badge.svg)](https://github.com/awu123-create/ferry/actions/workflows/ci.yml)
[![Go Version](https://img.shields.io/badge/Go-1.27-00ADD8?logo=go)](./go.mod)
[![Architecture](https://img.shields.io/badge/Docs-Architecture_%26_ADRs-0969da)](./docs/ARCHITECTURE.md)

Ferry 是一个基于 **Go + PostgreSQL + Kafka + Redis** 构建的高可靠异步 HTTP 调度与 Webhook 投递引擎。系统将**内部微服务的立即/延迟异步任务回调**与**面向外部商户的防篡改签名 Webhook 投递**统一在单一 HTTP Push 内核之上，以 **PostgreSQL 为唯一业务真相（Source of Truth）**、**Kafka 承接高吞吐削峰分发**、**Redis 驱动毫秒级时间调度与 Endpoint 级流控隔离**，确保任意单跳进程崩溃或缓存清空下任务不丢、不乱、不拖垮健康下游。

---

## 为什么构建 Ferry（Why Ferry?）

在常规业务内部，同库异步队列（如 `PostgreSQL + River`）能够以最小基础设施成本提供事务入队保障；但当调度系统独立演进为跨业务线共用的 HTTP 任务与外部 Webhook 平台时，单存储方案会面临明显边界：

- **高频重试与 MVCC 膨胀**：大量状态变更、退避重试直接更新同一张数据库队列行，会引发锁竞争与 Autovacuum 抖动；
- **不可信外部慢下游队头阻塞（Head-of-Line Blocking）**：某个外部商户接口持续超时（如 10 秒无响应）会迅速耗尽全局固定 Worker 并发槽位，连累所有健康下游；
- **单中间件短板**：纯内存队列（Redis）难以在全库重启或清空后自行恢复在途账本，纯日志流（Kafka）无法在入口同步做唯一键幂等拦截与任意时间定时触发。

| 能力维度 | 单库队列 (`PG + River`) | 纯内存队列 (`Redis`) | 纯消息流 (`Kafka`) | **Ferry (`PG + Kafka + Redis`)** |
| :--- | :---: | :---: | :---: | :--- |
| 入口同步唯一键幂等与即时查状态 | 支持 | 弱（非持久化账本） | 不支持 | **支持**（PG `UNIQUE` 约束 + `tasks` 落库即查） |
| 高并发状态流转防 DB 表膨胀 | 较弱（MVCC 写放大） | 支持 | 支持 | **支持**（Kafka 承接即时排队，Outbox 搬运即删） |
| 任意时间戳定时与动态退避重试 | 支持 | 支持（ZSet） | 仅固定阶梯 Topic | **支持**（`ferry:delayed` ZSet + `Retry-After`） |
| `Endpoint` 级动态限流与熔断隔离 | 需自建外挂组件 | 支持 | 分区重平衡成本高 | **支持**（单次 Redis Lua 原子准入 + 顺延回退） |
| 缓存节点内存清空后零丢单自愈 | 原生支持 | 依赖 AOF，极端仍丢 | 不涉及 | **支持**（`60s` PG Reconciler 对账全量捞回） |

---

## 核心机制与可靠性特性（Core Features）

- **事务发件箱零丢单（Transactional Outbox）**
  `POST /v1/tasks` 在单次 PG 事务内原子写入 `tasks` 与 `outbox`。数据库 `AFTER INSERT` 触发器通过 `pg_notify` 低延迟唤醒无状态 Go Relay 协程，结合 `500ms` 兜底定时器以 `FOR UPDATE SKIP LOCKED` 批量抢占，推入 Kafka（`acks=all`）或延迟池后物理 `DELETE`，使 `outbox` 常驻行数趋近于 0。
- **30 秒租约延迟池 + PG 对账自愈（Lease ZSet & Reconciler）**
  Redis `ferry:delayed`（ZSet）仅存轻量 `task_id` 与目标时间戳 `score`，统一承载定时任务、指数退避重试与 `429 Retry-After` 动态等待。Forwarder 通过原子 Lua 脚本续期 `30s` 可见性租约后再推 Kafka、确认后 `ZREM`；配合 `60s` PG Reconciler 基于部分索引扫描卡滞任务，即使执行 `redis-cli FLUSHALL` 清空内存，也可在 1 分钟内全量捞回。
- **Endpoint 级三维流控与舱壁隔离（3D Flow Control & Bulkhead）**
  Worker 投递前执行单次 Redis Lua 脚本 `admit_endpoint.lua`，原子完成 **连续失败自动熔断（`CLOSED / OPEN / HALF_OPEN`） -> QPS 令牌桶 -> 带过期时间戳的 ZSet 并发信号量**。信号量槽位携带超时时间自动回收，彻底杜绝 Worker 被 `kill -9` 强杀导致传统 `INCR/DECR` 永久死锁；受限或熔断任务微秒级顺延回退 `ferry:delayed`（不扣减重试次数，受 `deadline_at` 生命周期上限截断有界），把 Worker 协程 100% 留给健康下游。
- **全链路三层幂等与安全签名（Three-Layer Idempotency & Signing）**
  1. **入口层**：PG `UNIQUE(endpoint_id, idempotency_key)` + `payload_sha256` 校验（同键同载荷幂等返回 `200`，异载荷返回 `409 Conflict`）；
  2. **执行层**：Redis 任务执行短锁 + PG 状态机条件更新（CAS Guard）拦截重复与终态覆盖；
  3. **投递层**：每次请求固定携带 `X-Ferry-Task-ID`、`X-Ferry-Attempt`、`X-Ferry-Timestamp` 与 `X-Ferry-Signature`（`sha256=HMAC-SHA256(secret, timestamp.body)`），供下游业务去重与防篡改验签。
- **严格错误分级与受控死信重放（State Machine & Controlled DLQ Replay）**
  区分可重试错误（`5xx`、`408`、`429`、网络超时）与客户端契约错误（其余 `4xx` 单次尝试后直接落入 `DEAD_LETTER`），独立 `task_attempts` 明细表记录逐次耗时、状态码与响应片段。支持单条及按 `Endpoint`/时间窗分批（`<= 500`）重放死信，重放任务重走 Outbox -> Kafka -> Endpoint 限流主链路，防止瞬时洪峰二次打崩刚恢复的下游。
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
              | Outbox Relay (NOTIFY + SKIP LOCKED LIMIT 100)
              v (deliver_at <= now)                         v (deliver_at > now)
  [ Kafka: ferry.tasks.ready ] <====(100ms Lua 30s租约)==== [ Redis ZSet: ferry:delayed ]
              |                     推Kafka确认后 ZREM             ^           ^
              v                                                   |           |
  [ ferry-worker ]                                                | 限流/熔断  | 指数退避/429
    1. Redis 执行短锁 + PG 状态非终态守卫 + deadline_at 截断          | 顺延回退   |
    2. Lua [admit_endpoint.lua]: 熔断 -> QPS -> ZSet并发信号量 -----+           |
    3. HTTP POST (X-Ferry-Task-ID + HMAC 签名) ---> [ Target / chaos-receiver ]
    4. 写 task_attempts + 状态机流转 & 更新熔断计数 -----------------------------+
```

关于 PG 写完未发 Kafka 宕机、Redis 断连背压与内存清空自愈、下游超时风暴熔断以及 `SIGTERM` 优雅停机排空的逐跳推演与完整 DDL 定义，详见 [`docs/ARCHITECTURE.md`](./docs/ARCHITECTURE.md)。

---

## 技术栈（Tech Stack）

| 层级 | 选型 | 关键工程配置 |
| :--- | :--- | :--- |
| 语言与编译 | `Go 1.27` | 统一锁定 `go.mod` 与 `golangci-lint v2` 工具链版本 |
| HTTP 接入与投递 | 标准库 `net/http` | Go 1.22+ 增强 `ServeMux`；手动调优 `http.Transport` 连接池并用 `io.LimitReader` 防大包 OOM |
| 数据访问与迁移 | `GORM` + `Goose` + `UUIDv7` | DML 归 GORM（`SkipDefaultTransaction`、`clause.OnConflict`/`SKIP LOCKED`），DDL/部分索引/触发器归 `goose`，禁用 `AutoMigrate` |
| 消息总线与调度 | `twmb/franz-go` + `redis/go-redis/v9` | Kafka 原生幂等生产者与分区 `Pause` 背压；Redis `NewScript` 自动处理 `EVALSHA` 与重启后 `NOSCRIPT` 重载 |
| 日志与可观测 | `go.uber.org/zap` + `prometheus/client_golang` | JSON 日志启用 `Sampler` 采样防故障刷爆磁盘，统一接管 GORM 慢 SQL 告警；独立暴露 `/metrics` |
| 基础设施容器 | `PostgreSQL 16` + `Kafka 3.9 (KRaft)` + `Redis 7.4` | 无 ZooKeeper；Redis 强制 `--maxmemory-policy noeviction`；配套 `Prometheus` + `Grafana` |

---

## 仓库结构与本地开发（Repository Layout & Development）

```text
Ferry/
├── .github/workflows/ci.yml   # GitHub Actions: gofmt、ADR 唯一性、golangci-lint v2、race 单测与二进制构建
├── .golangci.yml              # golangci-lint v2 静态检查与分层依赖规则
├── .pre-commit-config.yaml    # 本地提交钩子 (基础检查、ADR 编号、gofmt、增量 lint、短单测)
├── Makefile                   # 统一开发与构建入口 (setup-hooks / fmt / lint / test / build)
├── go.mod                     # Go 1.27 模块定义
├── cmd/
│   ├── ferry-server/          # API 网关 + Outbox Relay + ZSet Forwarder + PG Reconciler
│   ├── ferry-worker/          # Kafka 消费 + Redis Lua 准入 + HTTP 签名投递 + 优雅停机
│   ├── chaos-receiver/        # 混沌故障注入靶场 (模拟延迟 / 500 / 429 / HMAC 验签)
│   └── ferry-bench/           # 并发压测与多目标隔离性验证 CLI
├── docs/                      # 架构蓝图 (ARCHITECTURE.md) 与 ADR 0001~0006
├── internal/                  # Milestone 1~3 核心业务包 (api / store/pg / queue/kafka / scheduler/redis / delivery / logger / metrics)
├── migrations/                # Milestone 1 Goose SQL 迁移脚本 (4 张核心表、部分索引与 NOTIFY 触发器)
├── deploy/                    # Milestone 1~3 Docker Compose、Prometheus 与 Grafana 看板配置
├── scripts/                   # Milestone 1~4 可重复自动化验收脚本 (verify_m1.sh ~ verify_m4.sh)
└── web/                       # Milestone 4 极简只读监控与死信批量重放静态控制台
```

### 快速校验（当前工程骨架）

前置要求：本机已安装 Go 1.27、[`golangci-lint v2`](https://golangci-lint.run/) 与 [`pre-commit`](https://pre-commit.com/)。

```bash
# 1. 安装 Git pre-commit 钩子（首次拉取仓库后执行）
make setup-hooks

# 2. 格式化检查、golangci-lint 静态扫描与竞态单元测试
make fmt
make lint
make test

# 3. 编译全部 4 个核心二进制文件至 .build/
make build
```

---

## 演进路线图（Roadmap）

- [x] **Foundation**：架构蓝图与 ADR 0001~0006 定稿、Go 1.27 工程骨架、`Makefile`、`golangci-lint v2`、`pre-commit` 与 GitHub Actions CI
- [ ] **Milestone 1（可靠直投闭环）**：Docker Compose 基础设施、Goose 初始表迁移、幂等入口 API、Outbox 搬运协程、Kafka Worker HMAC 签名投递与 `SIGTERM` 优雅停机
- [ ] **Milestone 2（延迟调度、重试与灾难自愈）**：Redis ZSet `30s` 租约调度、指数退避与 `Retry-After`、Redis 断连/重启后 `60s` PG Reconciler 自动捞回、单条与分批死信重放
- [ ] **Milestone 3（三维流控与混沌压测）**：`admit_endpoint.lua` 熔断/QPS/ZSet 并发信号量、Prometheus + Grafana 看板、`chaos-receiver` 与 `ferry-bench` 隔离压测验证
- [ ] **Milestone 4（极简控制台）**：基于公开 `/v1/*` REST API 的概览、Endpoint 配置、`task_attempts` 时间线查看与批量死信重放静态控制台

---

## 架构文档与设计决策（Documentation & ADRs）

- [架构设计总纲 (`docs/ARCHITECTURE.md`)](./docs/ARCHITECTURE.md)
- [ADR-0001: 统一核心业务模型为可靠 HTTP 任务投递引擎](./docs/adr/0001-unified-http-task-delivery-model.md)
- [ADR-0002: 入口事务一致性采用 PG 同事务 Outbox 与 Go 批量搬运协程](./docs/adr/0002-transactional-outbox-batch-relay.md)
- [ADR-0003: 延迟与退避重试采用 Redis ZSet 租约倒计时池加 PG 对账兜底](./docs/adr/0003-redis-zset-lease-and-pg-reconciler.md)
- [ADR-0004: Endpoint 级三维流控隔离与全链路三层幂等机制](./docs/adr/0004-endpoint-flow-control-and-three-layer-idempotency.md)
- [ADR-0005: 状态机与投递明细、死信受控重放及可观测优先交付边界](./docs/adr/0005-state-machine-dlq-replay-and-observability-boundary.md)
- [ADR-0006: Go 1.27 技术栈选型、GORM+Goose 职责边界与工程化门禁](./docs/adr/0006-tech-stack-and-engineering-tooling.md)

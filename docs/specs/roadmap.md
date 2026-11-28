# Ferry 分阶段演进路线图（Milestone 1 ~ 4）

> **规划边界**：本文件位于 `docs/specs/`，仅作为里程碑排序、切片范围与验收边界的规划上下文，**不构成自动创建 GitHub Issue 的授权**，也不在文件内镜像活工单状态、负责人或阻塞依赖（见 [docs/agents/issue-tracker.md](../agents/issue-tracker.md) 与 [docs/standards/records.md](../standards/records.md)）。现行已落地代码树以 [README.md](../../README.md) 与 [docs/ARCHITECTURE.md](../ARCHITECTURE.md) 为准。

本路线图遵循 **“每个 Milestone 独立可编译、可运行、可自动化验收，数据库迁移随生产调用方按需增量演进，核心后端与可观测先于控制台”** 的交付原则。

---

## 一、固定端口与目标模块拓扑规划

### 服务端口矩阵

- `ferry-server`（API + Outbox Relay + Delayed Forwarder + PG Reconciler）：REST API `:8080`、Prometheus `/metrics` `:2112`
- `ferry-worker`（Kafka 投递 Worker，支持独立进程或多实例部署）：健康探针 `/readyz` `:8082`、Prometheus `/metrics` `:2113`
- `chaos-receiver`（内置混沌下游靶场）：`:8081`
- 基础设施容器：PostgreSQL `:5432`、Kafka (KRaft) `:9092`、Redis `:6379`、Prometheus `:9090`、Grafana `:3000`

### 目标代码目录规划（随 M1 ~ M4 逐步落地）

```text
Ferry/
├── cmd/
│   ├── ferry-server/        # 组装根：HTTP API 网关 + Outbox Relay + Delayed Forwarder + PG Reconciler
│   ├── ferry-worker/        # 组装根：Kafka 消费者 + 流控准入 + HTTP 签名投递 + 优雅停机
│   ├── chaos-receiver/      # 混沌故障注入靶场 (模拟延迟 / 500 / 429 / HMAC 验签)
│   └── ferry-bench/         # 并发压测与隔离性验证 CLI
├── internal/
│   ├── domain/              # 纯 Go 领域核：Endpoint/Task 聚合、DeliveryAttempt、签名与退避策略
│   ├── api/                 # 入站 HTTP 控制面深模块：ServeMux 路由、DTO 校验、幂等创建/取消/重放
│   ├── scheduler/           # 后台调度深模块：OutboxRelay、DelayedForwarder、Reconciler
│   ├── worker/              # 投递执行深模块：Step A~E 状态机、25s 优雅停机排空与 Redis 断连背压暂停
│   ├── infra/               # 出站基础设施适配器环（消费方接口实现，互不依赖）
│   │   ├── pg/              # GORM 仓储（私有 PO、同事务 Outbox、SKIP LOCKED、Goose 迁移挂载）
│   │   ├── kafka/           # twmb/franz-go 幂等生产者/消费者封装（含分区 Pause/Resume）
│   │   ├── redis/           # go-redis/v9 适配器（延迟池租约 Lua、流控 Lua、Execution Lock）
│   │   └── httpclient/      # 调优 http.Transport 连接池、LimitReader 截断与 Retry-After 解析
│   ├── config/              # 12-Factor 强类型环境变量解析
│   ├── logger/              # zap JSON 采样日志与 GORM 慢查询 Logger 适配
│   └── metrics/             # Prometheus 指标定义与注册
├── migrations/              # Goose 增量 SQL 迁移脚本与 embed.go（按 Milestone 随调用方演进）
├── tests/integration/       # 真实 PG + Kafka + Redis 端到端集成测试 (*_integration_test.go)
├── deploy/                  # docker-compose.yml、prometheus.yml、grafana/dashboards/
├── scripts/                 # verify_m1.sh ~ verify_m4.sh 多进程黑盒验收脚本
└── web/                     # Milestone 4 独立静态控制台（仅消费 /v1/* REST API）
```

---

## 二、Milestone 1：最小端到端可靠立即直投闭环

- **独立可交付目标**：作为“立即可靠单次 HTTP 投递网关”零悬空独立运行——支持创建/管理 `Endpoint`、原始字节保真幂等创建立即任务（暂拒未来 `deliver_at`，默认 `max_attempts = 1` 不产生悬空 `RETRYING`）、Outbox 立即通道直推 Kafka（固定 `task_id` 分区键）、Redis `Execution Lock` 防重与 `POST /cancel` 抢锁互斥、Worker 签名直投下游并记录 `task_attempts`，以及 `SIGTERM` 优雅停机排空。
- **规划模块与迁移范围**（遵循 [docs/standards/database.md](../standards/database.md) 调用方原则，仅建 M1 实际读写的表、列与索引）：
  - `deploy/docker-compose.yml`（PostgreSQL 16 + Kafka KRaft + Redis 7 基础编排）与 `.github/workflows/ci.yml` 集成测试任务；
  - `.golangci.yml` 新增 `json_v2` `depguard` 规则（随 M1 首个代码 PR 启用）；
  - `migrations/` 初始迁移 + `embed.go`（建立 M1 所需的 `endpoints`、`tasks`、`task_attempts`、`outbox` 核心结构、幂等唯一约束、`outbox` 索引与 `NOTIFY` 触发器；M2/M3 专属索引与列留待对应 Milestone 增量迁移引入）；
  - `internal/infra/redis`（带持有人 `token` 核对续期与释放的 `Execution Lock`）；
  - `cmd/ferry-server`（API：`POST/GET/PATCH /v1/endpoints`、`POST/GET /v1/tasks`、`GET /v1/tasks/{id}/attempts`、`POST /v1/tasks/{id}/cancel`、`/healthz`、`/readyz`；内置 Outbox 立即通道处理 `deliver_at <= now()` 直推 Kafka）；
  - `cmd/ferry-worker`（Kafka 消费、`Execution Lock` + 乐观锁守卫、HMAC-SHA256 原始字节签名投递、写 `task_attempts`、2xx 迁移 `SUCCEEDED` / 失败迁移 `DEAD_LETTER`、25s `SIGTERM` 优雅停机）；
  - `cmd/chaos-receiver`（基础版：校验 `X-Ferry-Signature` 签名正确性、记录 `X-Ferry-Task-ID` 收到次数、支持返回指定状态码 `200`/`400`）。
- **验收边界（`scripts/verify_m1.sh`）**：
  1. 启动 PG/Kafka/Redis 容器，通过 `make lint`、`make test` 与 `make test-integration`；
  2. 多进程启动 `chaos-receiver`（`:8081`）、`ferry-server`（`:8080`）、`ferry-worker`（`:8082`）：
     - 创建带 `signing_secret` 的 `Endpoint`；
     - 携带同一 `Idempotency Key` 并发发 10 次 `POST /v1/tasks` → 仅生成 1 条任务，异载荷返回 `409 Conflict`；
     - `chaos-receiver` 验证收到合法 HMAC 签名的原始字节请求，`tasks.status == 'SUCCEEDED'`，`task_attempts == 1`；
     - 投递返回 `400` 的任务 → 立即进 `DEAD_LETTER` 且 `attempt == 1`；
     - 在途持锁期间调 `POST /v1/tasks/{id}/cancel` 返回 `409 Conflict`，向 `ferry-worker` 发 `SIGTERM` 确认在途任务完成后安全退出。

---

## 三、Milestone 2：任意延迟调度、指数退避重试、PG 游标对账自愈与死信批量重放

- **独立可交付目标**：打通时间调度与灾难自愈闭环——开放任意未来 `deliver_at` 定时任务、默认 `6` 次小时级指数退避（`5s -> 25s -> ~2m -> ~10m -> ~52m` ±20% 抖动，支持环境变量覆盖加速测试）与 `Retry-After` 解析、Forwarder 30s `Schedule Lease` 与比对分数条件 `ZREM`、Redis 内存全清空后的 `60s` PG Reconciler 游标流式回灌延迟池恢复，以及单条/按条件分批死信重放。
- **规划模块与增量迁移**：
  - `migrations/` 增量迁移：补充 M2 扫描所需的 `tasks_reconcile_idx`、`tasks_dlq_idx` 部分索引及 `replay_count` 相关列/索引（如有在 M1 延后落地的字段）；
  - `internal/infra/redis` + `internal/scheduler`：`claim_delayed.lua`（30s 租约）与比对分数条件删除 `ack_delayed.lua`、Outbox 延迟通道（`deliver_at > now()` 写延迟池，遇 Redis 故障独立退避）、抢锁争用 `ZADD NX` 保底顺延；
  - `internal/domain` + `internal/infra/httpclient`：可配置指数退避策略与 `Retry-After` 响应头解析，更新 `RETRYING` 提交后再写 `ferry:delayed`；
  - `internal/scheduler`：`60s`（支持环境变量覆写加速验收）PG Reconciler，游标循环扫完超期在途任务，识别顺延时间对齐并以 `ZADD NX` 补回丢失调度；
  - `internal/api` + `internal/infra/pg`：`POST /v1/tasks/{id}/replay` 与 `POST /v1/dlq/replay`（`FOR UPDATE SKIP LOCKED LIMIT <= 500` 分批重置 `attempt=0, replay_count+1` 并同事务写 `outbox`）；
  - `cmd/chaos-receiver` 扩展：支持按次数/概率返回 `500`、`429`（附带 `Retry-After: 2`）及固定延迟。
- **验收边界（`scripts/verify_m2.sh`）**：
  1. 提交 `deliver_at = +3s` 定时任务 → 3s 前未投递，到期后变为 `SUCCEEDED`；
  2. 目标前 2 次返回 `500`、第 3 次返回 `200` → 经退避后成功，`task_attempts` 记录 3 行；
  3. 耗尽 `max_attempts = 2` 落入 `DEAD_LETTER` → 调 `POST /v1/dlq/replay` 批量重放并投递成功，`replay_count == 1`；
  4. 灾难演练：提交 20 条 `+5s` 延迟任务后立即执行 `redis-cli FLUSHALL` 清空 Redis → Reconciler 游标扫描自动回灌延迟池并全部成功（0 丢单）。

---

## 四、Milestone 3：Endpoint 三维流控虚拟排号、可观测闭环与混沌压测

- **独立可交付目标**：交付生产级目标隔离保护与可量化压测监控——完成基于 Redis `TIME` 的熔断器（含半开探测超时）/ `1/QPS` 虚拟排号令牌桶 / 防泄漏并发槽位与对称 `CompleteDelivery` 结算、Worker `Endpoint` 本地 `3s` 短缓存、Redis 断连 Fail-Safe 背压暂停、全套 Prometheus/Grafana 看板与多场景混沌压测脚本。
- **规划模块与增量迁移**：
  - `migrations/` 增量迁移：补充 M3 才开始读写的流控配额列（如有在 M1/M2 延后落地的字段）；
  - `internal/infra/redis`（`admit_endpoint.lua` + `complete_delivery.lua`）+ `internal/worker`：`AdmitOrDefer` 原子准入与按 `1/QPS` 虚拟排号摊平顺延、`CompleteDelivery` 对称归还槽位并更新熔断/`429` 冷却、`Endpoint` `3s` 短缓存；Worker 检测 Redis 断连自动暂停拉取 Kafka 并通过 `Ready()` 置 `/readyz = 503`；
  - `internal/metrics` + `deploy/prometheus.yml` + `deploy/grafana/dashboards/ferry.json`：采集 Outbox 积压、ZSet 深度、Kafka Lag、各 Endpoint 吞吐与耗时分布（P50/P95/P99）、限流顺延数与熔断状态变更；
  - `cmd/chaos-receiver` 完整混沌模式 + `cmd/ferry-bench` 并发发压器。
- **验收边界（`scripts/verify_m3.sh`）**：
  1. 同时压测健康 `Endpoint-A`（`QPS = 200`，50ms 响应）与故障 `Endpoint-B`（3s 慢响应 + 100% `500`）；
  2. 验证 `Endpoint-B` 触发熔断 `OPEN`（`ferry_endpoint_circuit_state == 1`），Worker 协程不被占满；
  3. 验证 `Endpoint-A` 的 P99 投递延迟不受 `Endpoint-B` 拖累，超额流量按 `rate_limit_qps` 平滑顺延；
  4. 校验 `:2112/metrics` 与 `:2113/metrics` 核心指标非零输出，Grafana（`:3000`）看板正常拉取。

---

## 五、Milestone 4：极简只读监控与死信重放控制台

- **独立可交付目标**：在不侵入后端核心代码的独立目录 `web/` 内，仅消费冻结后的 `/v1/*` REST API 构建极简静态控制台，覆盖 4 个视图：
  1. **系统总览**：各状态任务统计与核心 `Endpoint` 健康度摘要；
  2. **Endpoint 管理**：查看/编辑 `rate_limit_qps`、`max_concurrency`、`circuit_threshold`、`timeout_ms`；
  3. **任务与流水时间线**：按 `status` / `endpoint_id` 检索任务，展开 `task_attempts` 逐次耗时、HTTP 状态码与响应片段；
  4. **死信队列批量重放**：勾选或按时间窗筛选 `DEAD_LETTER`，触发 `POST /v1/dlq/replay` 查看重放进度。
- **验收边界（`scripts/verify_m4.sh`）**：启动服务端与静态控制台，验证页面可达及四个视图接口联调闭环。

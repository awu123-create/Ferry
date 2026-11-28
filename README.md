# Ferry

**分布式异步 HTTP 任务调度与 Webhook 可靠投递平台**

[![CI](https://github.com/awu123-create/ferry/actions/workflows/ci.yml/badge.svg)](https://github.com/awu123-create/ferry/actions/workflows/ci.yml)
[![Go Version](https://img.shields.io/badge/Go-1.27-00ADD8?logo=go)](./go.mod)
[![Architecture](https://img.shields.io/badge/Docs-Architecture_%26_ADRs-0969da)](./docs/ARCHITECTURE.md)

Ferry 是一个基于 **Go + PostgreSQL + Kafka + Redis** 构建的异步 HTTP 调度与 Webhook 投递引擎。系统将**内部微服务的立即/定时异步回调**与**面向外部商户的签名 Webhook 投递**统一在同一套 HTTP Push 内核之上：以 **PostgreSQL** 作为唯一业务真相，**Kafka** 承担削峰与消费组水平扩展，**Redis** 驱动毫秒级延迟调度与 `Endpoint` 级流控隔离。

---

## 为什么构建 Ferry

单库异步队列（如 `PostgreSQL + River`）适合服务内同库入队，但在跨业务线共用的异步 HTTP 与外部 Webhook 场景下会遇到三类瓶颈：

- **高频流控顺延与重试写放大**：限流顺延、熔断计数与退避排期若频繁 `UPDATE` 数据库队列行，会引发严重的行锁竞争与 MVCC 死元组膨胀；
- **慢下游队头阻塞**：外部接口超时挂起会迅速占满全局固定并发槽位，拖慢所有健康下游；
- **单中间件能力短板**：纯内存队列在实例重建或清空后难以自愈账本，纯消息流不支持入口唯一键同步幂等与任意时刻定时触发。

Ferry 用三层协同解决上述问题：入口通过 PostgreSQL 唯一约束与事务 `Outbox` 保障接单不丢，高频倒计时与流控状态留在 Redis 内存消纳，就绪任务经 Kafka 分发至无状态 `Worker` 投递，并由后台 `Reconciliation` 兜底恢复缓存异常丢失。

---

## 核心特性

- **双通道事务发件箱（Transactional Outbox）**：`POST /v1/tasks` 同事务写入任务与发件箱，立即任务直推 Kafka，未来预约任务独立进入延迟池；Redis 故障期间不阻塞立即投递通道。
- **任意时刻延迟调度与对账自愈**：支持未来 30 天内任意时间预约投递、指数退避重试与 `Retry-After` 冷却；采用带 `Schedule Lease` 的 Redis 延迟池转发，配合 PostgreSQL 游标对账保障缓存清空后不丢单。
- **`Endpoint` 级三维流控隔离**：按 `Endpoint` 独立执行自动熔断、QPS 虚拟排号顺延与带超时回收的并发槽位控制，避免慢下游拖垮全局吞吐。
- **端到端三层幂等与原始字节签名**：入口 `Idempotency Key` 防重与冲突拦截、执行期 `Execution Lock` 与复合乐观版本守卫、出站保留原始 JSON 字节计算 `HMAC-SHA256` 签名并携带固定 `X-Ferry-Task-ID`。
- **投递审计与受控死信重放**：逐次记录 `Delivery Attempt` 状态码、耗时与响应片段，支持按条件筛选与分批重放 `Dead Letter` 任务。

完整的组件边界、数据流图、状态机、数据库表结构与故障恢复推演见 [docs/ARCHITECTURE.md](./docs/ARCHITECTURE.md)。

---

## 快速开始

前置要求：Go 1.27、Docker Compose。

### 1. 启动依赖并执行数据库迁移

```bash
make up                        # 拉起 PostgreSQL 16、Kafka 3.9 (KRaft)、Redis 7.4
make build                     # 编译 ferry-server、ferry-worker、chaos-receiver
make migrate                   # 执行内嵌 Goose 数据库迁移
```

环境配置变量与默认值见 [.env.example](./.env.example)。

### 2. 一键运行端到端黑盒验收

```bash
make verify-m1                 # 启动本地接收靶场、server 与 worker，验证幂等创建、签名投递、取消互斥与优雅停机
```

### 3. 手动启动服务并提交任务

在不同终端分别启动接收靶场、控制面与工作节点：

```bash
./.build/chaos-receiver        # 默认监听 :8081
./.build/ferry-server          # HTTP API :8080, Metrics :2112
./.build/ferry-worker          # Probe :8082, Metrics :2113
```

注册 `Endpoint` 并提交一条异步投递任务：

```bash
# 1. 创建投递端点
curl -sS -X POST http://127.0.0.1:8080/v1/endpoints \
  -H "Content-Type: application/json" \
  -d '{"name":"orders-hook","target_url":"http://127.0.0.1:8081/webhook","signing_secret":"whsec_demo"}'

# 2. 提交立即或定时任务（可选传入 RFC3339 格式的 deliver_at）
curl -sS -X POST http://127.0.0.1:8080/v1/tasks \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: order-1001" \
  -d '{"endpoint_id":"<ENDPOINT_ID>","payload":{"order_id":1001,"status":"paid"}}'
```

体验结束后停止本地容器：

```bash
make down
```

---

## 文档导航

- [开发与贡献指南 (`CONTRIBUTING.md`)](./CONTRIBUTING.md)：本地工具安装、测试分层、真容器隔离与开发校验流程
- [系统架构蓝图 (`docs/ARCHITECTURE.md`)](./docs/ARCHITECTURE.md)：三组件职责、端到端数据流、状态机、DDL 与故障恢复路径
- [统一语言词表 (`GLOSSARY.md`)](./GLOSSARY.md)：领域核心术语定义
- [全局编码规范 (`CODING_STANDARDS.md`)](./CODING_STANDARDS.md) 与 [分层技术栈规范 (`docs/standards/`)](./docs/standards/go/code.md)
- [分阶段演进路线图 (`docs/specs/roadmap.md`)](./docs/specs/roadmap.md)
- 历史架构决策记录（`docs/adr/`）：[ADR-0001](./docs/adr/0001-unified-http-task-delivery-model.md) · [ADR-0002](./docs/adr/0002-transactional-outbox-batch-relay.md) · [ADR-0003](./docs/adr/0003-redis-zset-lease-and-pg-reconciler.md) · [ADR-0004](./docs/adr/0004-endpoint-flow-control-and-three-layer-idempotency.md) · [ADR-0005](./docs/adr/0005-state-machine-dlq-replay-and-observability-boundary.md) · [ADR-0006](./docs/adr/0006-tech-stack-and-engineering-tooling.md) · [ADR-0007](./docs/adr/0007-tactical-ddd-and-deep-module-seams.md) · [ADR-0008](./docs/adr/0008-concurrency-fencing-and-flow-control-hardening.md)

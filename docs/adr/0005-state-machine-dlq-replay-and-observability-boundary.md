# ADR-0005: 状态机与投递明细、死信受控重放及可观测优先交付边界

- **状态**：已采纳（Accepted）
- **日期**：2026-11-16

## 背景与问题（Context）

可靠的投递系统不能只把最终成败记成布尔值，必须清晰回答三个工程问题：
1. 下游返回 `400 Bad Request` 和返回 `503 Service Unavailable` 是否都该无脑重试？状态迁移规则如何防止并发乱序覆盖？
2. 当下游故障恢复后，积压在死信队列里的上万条任务如果一股脑全部重发，极易瞬间把刚恢复的下游再次打挂（“重放雪崩”），如何做到受控批量重放？
3. 作为重点展示 Go 后端架构、分布式可靠性与压测调优能力的秋招核心项目，如何在开发周期内合理分配后端可观测性与前端管理页面的精力？

## 备选方案对比（Considered Options）

### 方案 1：`tasks` 真相主表 + `task_attempts` 独立流水表 + 走标准链路的受控批量死信重放 + 可观测/靶场先行、轻量控制台收尾（采纳）
- 每次 HTTP 投递均追加写入 `task_attempts` 明细表；精确区分可重试错误与不可重试错误；死信重放复用 `tasks + outbox` 标准入口链路自然受 Kafka 与 Endpoint 限流管控；前三阶段全部聚焦后端、Prometheus/Grafana 与混沌压测靶场，极简控制台放在最后一个独立阶段收尾。

### 方案 2：仅维护 `tasks` 单表覆盖报错 + 前后端同步开发
- **缺点**：单表覆盖历史报错会丢失每次重试的具体延迟、状态码演变与重放溯源；在核心引擎尚未通过压测验证前同步维护前端界面，会严重分散注意力并频繁因 API 微调返工。

## 最终决策（Decision）

采纳 **方案 1**：

### 一、任务状态机与错误分类规则
1. **6 个核心状态**：
   - 非终态：`PENDING`（待立即调度或定时等待）、`RUNNING`（Worker 已准入并正在发送 HTTP）、`RETRYING`（失败后进入退避倒计时等待下次投递）；
   - 终态：`SUCCEEDED`（收到 `2xx` 响应）、`DEAD_LETTER`（死信）、`CANCELLED`（在投递完成前被用户主动取消）。
2. **严格的 HTTP 响应分类**：
   - `2xx`（200~299）：立即迁移至 `SUCCEEDED`；
   - `408 Request Timeout`、`429 Too Many Requests`、`5xx`（500~599）、DNS/TCP/TLS/读超时等网络错误：判定为**可重试错误**。若存在有效 `Retry-After` 响应头（仅限 `429`/`503`），优先按其计算下一次调度时间（封顶不超过 `3600s`），否则按指数退避 `min(max_backoff, base * 2^(attempt-1)) + full_jitter`；当 `attempt >= max_attempts` 或当前已超过 `deadline_at` 时落入 `DEAD_LETTER`；
   - 其余 `4xx`（`400`/`401`/`403`/`404`/`410`/`422` 等）：判定为**不可重试契约/权限错误**，直接落入 `DEAD_LETTER`（记 `last_error_code = NON_RETRYABLE_4XX`），不浪费重试配额与熔断计数器；
   - 取消守卫：`POST /v1/tasks/{id}/cancel` 仅允许从 `PENDING` 或 `RETRYING` 迁移至 `CANCELLED`，对已在途 `RUNNING` 或终态请求返回 `409 Conflict`。

### 二、投递流水审计（`task_attempts`）与受控死信重放
1. **独立流水表**：每次实际发出 HTTP 请求（不含 Redis 限流/熔断直接回退）均写入一行 `task_attempts(task_id, attempt_no, http_status, duration_ms, error_kind, error_message, response_snippet, created_at)`，保留完整排查轨迹（响应片段截断至前 `1024` 字节防大包撑爆存储）。
2. **单条与分批死信重放**：
   - 单条重放：`POST /v1/tasks/{id}/replay`；
   - 批量重放：`POST /v1/dlq/replay`，支持按 `endpoint_id`、时间范围 `[from, to]` 与每批上限 `limit`（强制封顶 `<= 500`）筛选 `status = 'DEAD_LETTER'` 的任务；
   - **防二次雪崩机制**：批量重放执行事务内使用 `FOR UPDATE SKIP LOCKED` 选中目标批，将状态重置为 `PENDING`、`attempt = 0`、`replay_count = replay_count + 1`、重新设定 `deadline_at = now() + 24h` 与 `next_run_at = now()`，并在**同事务写入 `outbox`**。重放任务重新经过 Outbox Relay -> Kafka -> Endpoint 限流器，严格受限于目标 `rate_limit_qps` 与 `max_concurrency` 平滑消费，绝不会一次性洪峰打崩下游。

### 三、可观测性优先与前端最后阶段隔离边界
1. **Milestone 1~3 纯 Go 后端 + 基础设施闭环**：
   - 内置 `cmd/chaos-receiver` 独立 HTTP 靶场进程（支持按请求头或 Endpoint 查询参数实时注入延迟分布、500 错误率与 429 限流率）；
   - 通过 `/metrics`（端口 `:2112`）暴露核心 Prometheus 指标：`ferry_outbox_pending_rows`、`ferry_outbox_lag_seconds`、`ferry_delayed_zset_size`、`ferry_kafka_consumer_lag`、`ferry_delivery_total{status_class,endpoint}`、`ferry_delivery_duration_seconds`、`ferry_endpoint_circuit_state`、`ferry_ratelimit_deferred_total`；
   - 仓库随附开箱即用的 Grafana Dashboard JSON 与内置压测工具 `cmd/ferry-bench`。
2. **Milestone 4 极简控制台压后**：
   - 极简管理控制台置于最后一个 Milestone 交付，严格以独立前端静态包形式消费冻结后的 `/v1/*` REST API（提供：概览大盘、Endpoint 流控配置、任务状态与 `task_attempts` 时间线查看、死信筛选与批量重放操作），不引入复杂状态框架，不反过来修改核心引擎语义。

## 影响与取舍（Consequences & Trade-offs）

- **正向收益**：
  - 死信重放“复用主链路”而非“另写旁路直发脚本”，天然继承限流、熔断与审计日志，架构高度一致；
  - `task_attempts` 采用只追加写（Append-Only），配合部分索引 `tasks_dlq_idx`，查询死信列表不扫描海量成功历史；
  - 研发重心 100% 对齐 Go 后端、中间件调优与可观测指标，产出可直接用于简历与面试演示的硬核数据曲线。
- **明确取舍**：
  - 每次实际投递会多一次 `INSERT INTO task_attempts` 与 `UPDATE tasks` 事务（对常规异步吞吐完全可接受；若未来需要 10 倍以上写扩展，可参考扩展路线将流水异步批量落 ClickHouse 或分区归档，见 `ARCHITECTURE.md`）。

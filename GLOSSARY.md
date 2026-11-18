# Ferry

Ferry 是一个面向内部微服务异步回调与外部商户 Webhook 的点对点可靠 HTTP 任务调度与投递平台，整体作为一个单一限界上下文运作。系统以持久化任务账本为唯一业务真相，通过统一的状态流转、流控隔离与失败恢复契约保障投递不丢失、不乱改终态且不拖垮健康下游。

## Language

### Core Model（核心模型）

**Endpoint（投递端点）**:
受托管的下游 HTTP 接收目标及其投递治理契约的聚合根，承载目标地址、请求超时、可选签名密钥、启用状态与三维流控配额。
_Avoid_: Target, Destination, Webhook URL, Subscriber, Consumer

**Task（投递任务）**:
向指定已注册 `Endpoint` 发起一次可靠异步 HTTP POST 载荷投递的工作单元与生命周期聚合根。
_Avoid_: Job, Message, Event, Notification

**Delivery Attempt（投递尝试记录）**:
单次实际向目标 `Endpoint` 发出 HTTP 请求后生成的不可变、只追加审计凭证，承载尝试序号、耗时、响应状态与错误摘要。
_Avoid_: Retry Log, Execution History, Task Log

**Idempotency Key（幂等键）**:
上游在单个 `Endpoint` 命名空间下标识唯一投递意图的客户端凭据，配合载荷摘要消解重复提交并拦截同键异载荷冲突。
_Avoid_: Dedup ID, Request ID, Message Key

**Payload Signature（载荷签名）**:
当 `Endpoint` 配置签名密钥时，基于发送时间戳与原始载荷生成的防篡改与防重放密码学凭据。
_Avoid_: API Token, Auth Header, Checksum

### Lifecycle & Scheduling（生命周期与调度）

**NextRunAt（下次触发时间）**:
`Task` 在领域内唯一的计划触发时间属性；创建时由入站初始期望时间（或缺省当前时间）初始化，并在退避重试或对账对齐时向后推进。
_Avoid_: DeliverAt（仅作入站 DTO 参数与发件箱字段名）, ExecuteTime, TriggerAt

**Deadline（生命周期截止时间）**:
`Task` 允许继续被调度或顺延的绝对时间硬边界；一旦越过该时刻仍未成功投递，任务立即终止并转入死信，确保调度回退环路严格有界。
_Avoid_: TTL（易与缓存过期混淆）, ExpireAt, Timeout（仅指单次 HTTP 调用超时）

**Deferral（流控顺延）**:
`Task` 在投递准入阶段因下游限流、并发满载或熔断开启而被无损推迟调度的行为；不发起 HTTP 请求、不生成 `Delivery Attempt`、不扣减重试配额，且在准入热路径上不改写任务持久化状态（仅由后台 `Reconciliation` 按需惰性对齐计划时间）。
_Avoid_: Retry, Backoff, Reject

**Retry（失败退避重试）**:
`Task` 实际发起 HTTP 请求并遭遇可重试故障（网络超时、`408`、`429`、`5xx`）后，生成 `Delivery Attempt`、递增尝试计数、迁移至 `RETRYING` 并按退避策略推进 `NextRunAt` 的再调度行为。
_Avoid_: Deferral, Replay, Resend

**Dead Letter（死信）**:
因遭遇不可重试客户端契约错误、尝试配额耗尽、越过 `Deadline` 或目标 `Endpoint` 处于禁用态而终止自动调度的终态 `Task`。
_Avoid_: Failed Task, Dropped Task, Poison Message

**Replay（死信重放）**:
显式将处于 `Dead Letter` 终态的 `Task` 重置尝试计数与 `Deadline`、重新送入标准调度与流控主链路的受控恢复动作。
_Avoid_: Retry, Force Send, Bypass Resend

**Outbox（事务发件箱）**:
在持久化创建或重放 `Task` 的同一原子边界内登记调度意图的内部中转账本，保障任务状态落盘与异步入队同生共死。
_Avoid_: Domain Event Bus, Event Store

**Reconciliation（对账自愈）**:
周期性以持久化任务账本为唯一真相核验非终态在途任务、对齐顺延时间偏差并补发遗失调度的自愈机制。
_Avoid_: Queue Polling, Cron Retry

### Flow Control & Coordination（流控与并发协调）

**Schedule Lease（调度可见性租约）**:
延迟调度器取出到期任务并转发至就绪流期间持有的短暂排他可见性窗口；转发确认后解除，若中途崩溃则窗口到期后自动恢复可调度性。
_Avoid_: Task Lock, Execution Lock, Redis Lease

**Execution Lock（执行互斥锁）**:
单个工作节点驱动某条 `Task` 单次投递周期时持有的短时排他执行权，防止至少一次投递语义下的消息重复引发并发双发。
_Avoid_: Schedule Lease, Concurrency Slot, Row Lock

**Concurrency Slot（目标并发槽位）**:
单个 `Endpoint` 允许同时处于在途 HTTP 请求中的有界且带超时自动回收上限的执行配额，防止慢下游耗尽全局资源或因进程强杀永久泄漏配额。
_Avoid_: Global Worker Pool, Task Lock

**Circuit Breaker（目标熔断器）**:
按单个 `Endpoint` 统计连续可重试故障并在达阈值后切入冷却拦截（`OPEN`）与单请求探测（`HALF_OPEN`）的动态故障隔离状态机。
_Avoid_: Endpoint Disable（后者为静态管理态关停）, Global Pause

package domain

import (
	"time"

	"github.com/google/uuid"
)

// NewTaskParams 封装创建 Task 及其初始 Outbox 意图所需的校验后入参。
type NewTaskParams struct {
	Endpoint       Endpoint
	IdempotencyKey string
	RawPayload     []byte
	Headers        map[string]string
	DeliverAt      *time.Time
	MaxAttempts    int
	DeadlineAt     *time.Time
	DefaultWindow  time.Duration
	Now            time.Time
}

// PreDeliveryDecision 表示 Worker 在发起出站 HTTP 请求前的守卫判定结果。
type PreDeliveryDecision struct {
	ShouldDeliver  bool
	AlreadyFinal   bool
	EarlyArrival   bool
	DeadLetterCode string
}

// OutcomeTransition 封装 Task.RecordOutcome 返回的状态迁移结果、只追加 DeliveryAttempt 记录与熔断反馈信号，
// 供外层在单次数据库事务中原子落盘。
type OutcomeTransition struct {
	NextStatus       TaskStatus
	NextAttempt      int
	NextRunAt        time.Time
	LastError        string
	CompletedAt      *time.Time
	Attempt          DeliveryAttempt
	CircuitSuccess   bool
	CircuitRetryable bool
}

// NewTask 校验任务创建不变量并初始化 PENDING 状态的 Task 与对应的 OutboxItem。
//
// 契约要求（Milestone 1 核心逻辑，留待实现）：
//  1. 要求 params.Endpoint.Enabled == true（否则返回 ErrEndpointDisabled）。
//  2. 要求去除首尾空白后的 IdempotencyKey 非空（否则返回 NewValidationError("idempotency_key", ...)）。
//  3. 要求 len(RawPayload) > 0 且满足合法 JSON 语法，但严禁重新序列化（原样保留原始字节切片）。
//  4. 在 Milestone 1 中，若 params.DeliverAt 非空且 params.DeliverAt.After(params.Now)，
//     返回 ErrFutureDeliveryUnsupported（ADR-0008 §5.1）。
//  5. 要求 MaxAttempts 位于 [1, 20]；NextRunAt 默认取 DeliverAt（或 Now），
//     DeadlineAt 为 nil 时默认取 NextRunAt + DefaultWindow（非 nil 时校验 DeadlineAt.After(NextRunAt)）。
//  6. 为 Task.ID 生成 UUIDv7，通过 ComputePayloadSHA256(RawPayload) 计算 PayloadSHA256，
//     设置 Status = TaskStatusPending、Attempt = 0、ReplayCount = 0，并返回配对的 OutboxItem。
func NewTask(params NewTaskParams) (Task, OutboxItem, error) {
	_ = params
	return Task{}, OutboxItem{}, ErrNotImplemented
}

// VerifyIdempotentReplay 校验重复提交的请求是否与已持久化 Task 的幂等快照一致。
//
// 契约要求（ADR-0004 & ADR-0008）：
//   - 校验 EndpointID 与 IdempotencyKey 匹配，并比对 PayloadSHA256。
//   - 完全一致时返回 nil（调用方返回 200 OK 与原 Task）。
//   - PayloadSHA256 不一致时返回 ErrIdempotencyConflict（调用方返回 409 Conflict）。
func (t Task) VerifyIdempotentReplay(endpointID uuid.UUID, idempotencyKey, payloadSHA256 string) error {
	_ = endpointID
	_ = idempotencyKey
	_ = payloadSHA256
	return ErrNotImplemented
}

// EvaluatePreDelivery 在 Worker Step B 发起出站 HTTP 投递前执行前置守卫检查。
//
// 契约要求（ADR-0007 & ADR-0008）：
//   - 若 t.Status.IsTerminal()：返回 PreDeliveryDecision{AlreadyFinal: true}, nil。
//   - 若 now.Before(t.NextRunAt)：返回 PreDeliveryDecision{EarlyArrival: true}, nil。
//   - 若 !ep.Enabled：返回 PreDeliveryDecision{DeadLetterCode: DeadLetterReasonEndpointDisabled}, nil。
//   - 若 !now.Before(t.DeadlineAt)：返回 PreDeliveryDecision{DeadLetterCode: DeadLetterReasonDeadlineExceeded}, nil。
//   - 其余情况返回 PreDeliveryDecision{ShouldDeliver: true}, nil。
func (t Task) EvaluatePreDelivery(ep Endpoint, now time.Time) (PreDeliveryDecision, error) {
	_ = ep
	_ = now
	return PreDeliveryDecision{}, ErrNotImplemented
}

// RecordOutcome 对出站 DeliveryResult 进行分级，演进 Task 状态并生成只追加 DeliveryAttempt 记录。
//
// 契约要求（ADR-0005、ADR-0007 & ADR-0008）：
//   - 要求 t.Status.IsActive()（否则返回 ErrValidation）。
//   - 递增 Attempt（attemptNo = t.Attempt + 1）并构造包含 TaskID、ReplayCount、AttemptNo 等字段的 DeliveryAttempt。
//   - HTTP 2xx：迁移至 SUCCEEDED，设置 CompletedAt = &now，CircuitSuccess = true。
//   - 不可重试 4xx（除 408 和 429 之外的 4xx）：直接迁移至 DEAD_LETTER，LastError = DeadLetterReasonNonRetryable4xx
//     （注意：不得设置 CircuitRetryable = true）。
//   - 可重试故障（408、429、5xx、网络/超时错误）：设置 CircuitRetryable = true；在 Milestone 1 中
//     当 Attempt >= MaxAttempts 时直接迁移至 DEAD_LETTER，设置 DeadLetterReasonMaxAttemptsExhausted
//     与 CompletedAt = &now（Milestone 2 会扩展为指数退避 + Retry-After -> RETRYING）。
func (t *Task) RecordOutcome(result DeliveryResult, now time.Time) (OutcomeTransition, error) {
	_ = result
	_ = now
	return OutcomeTransition{}, ErrNotImplemented
}

// Cancel 将处于非终态（PENDING 或 RETRYING）的 Task 迁移为 CANCELLED。
//
// 契约要求（ADR-0007 & ADR-0008）：
//   - 若 t.Status.IsTerminal()：返回 ErrTaskNotCancellable。
//   - 设置 Status = TaskStatusCancelled、CompletedAt = &now、UpdatedAt = now.UTC()。
func (t *Task) Cancel(now time.Time) error {
	_ = now
	return ErrNotImplemented
}

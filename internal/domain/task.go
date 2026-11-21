package domain

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	// MinTaskMaxAttempts 与 MaxTaskMaxAttempts 定义 Task.MaxAttempts 的合法闭区间（与数据库 CHECK 约束一致）。
	MinTaskMaxAttempts = 1
	MaxTaskMaxAttempts = 20
)

// TaskStatus 表示 Task 持久化生命周期的五种状态。
type TaskStatus string

const (
	TaskStatusPending    TaskStatus = "PENDING"
	TaskStatusRetrying   TaskStatus = "RETRYING"
	TaskStatusSucceeded  TaskStatus = "SUCCEEDED"
	TaskStatusDeadLetter TaskStatus = "DEAD_LETTER"
	TaskStatusCancelled  TaskStatus = "CANCELLED"
)

// ParseTaskStatus 校验并将持久化的状态字符串解析为 TaskStatus。
func ParseTaskStatus(raw string) (TaskStatus, error) {
	switch TaskStatus(raw) {
	case TaskStatusPending,
		TaskStatusRetrying,
		TaskStatusSucceeded,
		TaskStatusDeadLetter,
		TaskStatusCancelled:
		return TaskStatus(raw), nil
	default:
		return "", fmt.Errorf("unknown task status %q", raw)
	}
}

// IsTerminal 判断 Task 是否已进入终态。
func (s TaskStatus) IsTerminal() bool {
	return s == TaskStatusSucceeded || s == TaskStatusDeadLetter || s == TaskStatusCancelled
}

// IsActive 判断 Task 是否仍处于可调度或可取消的非终态。
func (s TaskStatus) IsActive() bool {
	return s == TaskStatusPending || s == TaskStatusRetrying
}

const (
	// DeadLetterReasonDeadlineExceeded 记录因越过 DeadlineAt 截止时间而终止投递的死信原因码。
	DeadLetterReasonDeadlineExceeded = "TTL_EXPIRED"
	// DeadLetterReasonEndpointDisabled 记录因目标 Endpoint 已被禁用而终止投递的死信原因码。
	DeadLetterReasonEndpointDisabled = "ENDPOINT_DISABLED"
	// DeadLetterReasonNonRetryable4xx 记录因遭遇不可重试 4xx 契约错误而转入死信的原因码。
	DeadLetterReasonNonRetryable4xx = "NON_RETRYABLE_4XX"
	// DeadLetterReasonMaxAttemptsExhausted 记录因耗尽最大尝试次数而转入死信的原因码。
	DeadLetterReasonMaxAttemptsExhausted = "MAX_ATTEMPTS_EXHAUSTED"
)

// Task 是表示单次可靠 HTTP POST 投递单元与生命周期的聚合根。
type Task struct {
	ID             uuid.UUID
	EndpointID     uuid.UUID
	IdempotencyKey string
	Payload        []byte
	PayloadSHA256  string
	Headers        map[string]string
	Status         TaskStatus
	Attempt        int
	MaxAttempts    int
	NextRunAt      time.Time
	DeadlineAt     time.Time
	LastError      string
	ReplayCount    int
	CreatedAt      time.Time
	UpdatedAt      time.Time
	CompletedAt    *time.Time
}

// CreateTaskResult 表示任务创建结果以及本次提交是否新建了 Task 记录。
type CreateTaskResult struct {
	Task    Task
	Created bool
}

// NewTaskParams 封装创建 Task 所需的校验后入参。
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

// NewTask 校验任务创建不变量并初始化 PENDING 状态的 Task。
//
// 契约要求（Milestone 1 核心逻辑）：
//  1. 要求 params.Endpoint.Enabled == true（否则返回 ErrEndpointDisabled）。
//  2. 要求去除首尾空白后的 IdempotencyKey 非空（否则返回 NewValidationError("idempotency_key", ...)）。
//  3. 要求 len(RawPayload) > 0 且满足合法 JSON 语法，但严禁重新序列化（原样保留原始字节切片）。
//  4. 在 Milestone 1 中，若 params.DeliverAt 非空且 params.DeliverAt.After(params.Now)，
//     返回 ErrFutureDeliveryUnsupported（ADR-0008 §5.1）。
//  5. 要求 MaxAttempts 位于 [1, 20]；NextRunAt 默认取 DeliverAt（或 Now），
//     DeadlineAt 为 nil 时默认取 NextRunAt + DefaultWindow（非 nil 时校验 DeadlineAt.After(NextRunAt)）。
//  6. 为 Task.ID 生成 UUIDv7，通过 ComputePayloadSHA256(RawPayload) 计算 PayloadSHA256，
//     设置 Status = TaskStatusPending、Attempt = 0、ReplayCount = 0。
func NewTask(params NewTaskParams) (Task, error) {
	if !params.Endpoint.Enabled {
		return Task{}, ErrEndpointDisabled
	}

	idempotencyKey := strings.TrimSpace(params.IdempotencyKey)
	if idempotencyKey == "" {
		return Task{}, NewValidationError("idempotency_key", "must not be empty")
	}

	if len(params.RawPayload) == 0 || !json.Valid(params.RawPayload) {
		return Task{}, NewValidationError("payload", "must be a non-empty valid JSON value")
	}

	if params.MaxAttempts < MinTaskMaxAttempts || params.MaxAttempts > MaxTaskMaxAttempts {
		return Task{}, NewValidationError("max_attempts", fmt.Sprintf("must be between %d and %d", MinTaskMaxAttempts, MaxTaskMaxAttempts))
	}

	now := params.Now.UTC()
	nextRunAt := now
	if params.DeliverAt != nil {
		deliverAt := params.DeliverAt.UTC()
		if deliverAt.After(now) {
			return Task{}, ErrFutureDeliveryUnsupported
		}
		nextRunAt = deliverAt
	}

	var deadlineAt time.Time
	if params.DeadlineAt != nil {
		deadlineAt = params.DeadlineAt.UTC()
		if !deadlineAt.After(nextRunAt) || !deadlineAt.After(now) {
			return Task{}, NewValidationError("deadline_at", "must be after next_run_at and now")
		}
	} else {
		if params.DefaultWindow <= 0 {
			return Task{}, NewValidationError("default_window", "must be > 0")
		}
		deadlineAt = nextRunAt.Add(params.DefaultWindow)
	}

	id, err := uuid.NewV7()
	if err != nil {
		return Task{}, fmt.Errorf("generate task uuid: %w", err)
	}

	headers := make(map[string]string, len(params.Headers))
	maps.Copy(headers, params.Headers)

	task := Task{
		ID:             id,
		EndpointID:     params.Endpoint.ID,
		IdempotencyKey: idempotencyKey,
		Payload:        bytes.Clone(params.RawPayload),
		PayloadSHA256:  ComputePayloadSHA256(params.RawPayload),
		Headers:        headers,
		Status:         TaskStatusPending,
		Attempt:        0,
		MaxAttempts:    params.MaxAttempts,
		NextRunAt:      nextRunAt,
		DeadlineAt:     deadlineAt,
		ReplayCount:    0,
		CreatedAt:      now,
		UpdatedAt:      now,
	}

	return task, nil
}

// VerifyIdempotentReplay 校验重复提交的请求是否与已持久化 Task 的幂等快照一致。
//
// 契约要求（ADR-0004 & ADR-0008）：
//   - 校验 EndpointID 与 IdempotencyKey 匹配，并比对 PayloadSHA256。
//   - 完全一致时返回 nil（调用方返回 200 OK 与原 Task）。
//   - PayloadSHA256 不一致时返回 ErrIdempotencyConflict（调用方返回 409 Conflict）。
func (t Task) VerifyIdempotentReplay(endpointID uuid.UUID, idempotencyKey, payloadSHA256 string) error {
	if t.EndpointID != endpointID || t.IdempotencyKey != strings.TrimSpace(idempotencyKey) {
		return NewValidationError("idempotency_key", "does not match existing task")
	}
	if t.PayloadSHA256 != payloadSHA256 {
		return ErrIdempotencyConflict
	}
	return nil
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
	ts := now.UTC()

	switch {
	case t.Status.IsTerminal():
		return PreDeliveryDecision{AlreadyFinal: true}, nil
	case ts.Before(t.NextRunAt):
		return PreDeliveryDecision{EarlyArrival: true}, nil
	case !ep.Enabled:
		return PreDeliveryDecision{DeadLetterCode: DeadLetterReasonEndpointDisabled}, nil
	case !ts.Before(t.DeadlineAt):
		return PreDeliveryDecision{DeadLetterCode: DeadLetterReasonDeadlineExceeded}, nil
	default:
		return PreDeliveryDecision{ShouldDeliver: true}, nil
	}
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
	if !t.Status.IsActive() {
		return OutcomeTransition{}, NewValidationError("status", "task must be in an active state (PENDING or RETRYING) to record outcome")
	}

	ts := now.UTC()
	t.Attempt++

	transition := OutcomeTransition{
		NextAttempt: t.Attempt,
		NextRunAt:   t.NextRunAt,
		Attempt: DeliveryAttempt{
			TaskID:          t.ID,
			ReplayCount:     t.ReplayCount,
			AttemptNo:       t.Attempt,
			HTTPStatus:      result.HTTPStatus,
			DurationMs:      result.DurationMs,
			ErrorKind:       result.ErrorKind,
			ErrorMessage:    result.ErrorMessage,
			ResponseSnippet: result.ResponseSnippet,
			CreatedAt:       ts,
		},
	}

	switch result.ErrorKind {
	case AttemptErrorNone:
		transition.NextStatus = TaskStatusSucceeded
		transition.LastError = ""
		transition.CompletedAt = &ts
		transition.CircuitSuccess = true
	case AttemptErrorHTTP4xx:
		transition.NextStatus = TaskStatusDeadLetter
		transition.LastError = DeadLetterReasonNonRetryable4xx
		transition.CompletedAt = &ts
	default:
		transition.CircuitRetryable = true
		switch {
		case !ts.Before(t.DeadlineAt):
			transition.NextStatus = TaskStatusDeadLetter
			transition.LastError = DeadLetterReasonDeadlineExceeded
			transition.CompletedAt = &ts
		case t.Attempt >= t.MaxAttempts:
			transition.NextStatus = TaskStatusDeadLetter
			transition.LastError = DeadLetterReasonMaxAttemptsExhausted
			transition.CompletedAt = &ts
		default:
			transition.NextStatus = TaskStatusRetrying
			transition.LastError = result.ErrorMessage
		}
	}

	t.Status = transition.NextStatus
	t.LastError = transition.LastError
	t.UpdatedAt = ts
	t.CompletedAt = transition.CompletedAt

	return transition, nil
}

// Cancel 将处于非终态（PENDING 或 RETRYING）的 Task 迁移为 CANCELLED。
//
// 契约要求（ADR-0007 & ADR-0008）：
//   - 若 t.Status.IsTerminal()：返回 ErrTaskNotCancellable。
//   - 设置 Status = TaskStatusCancelled、CompletedAt = &now、UpdatedAt = now.UTC()。
func (t *Task) Cancel(now time.Time) error {
	if t.Status.IsTerminal() {
		return ErrTaskNotCancellable
	}

	ts := now.UTC()
	t.Status = TaskStatusCancelled
	t.CompletedAt = &ts
	t.UpdatedAt = ts
	return nil
}

// ComputePayloadSHA256 计算原始载荷字节切片的小写十六进制 SHA-256 摘要。
func ComputePayloadSHA256(rawPayload []byte) string {
	sum := sha256.Sum256(rawPayload)
	return hex.EncodeToString(sum[:])
}

package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/google/uuid"
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

// AttemptErrorKind 对单次投递尝试（Delivery Attempt）的出站 HTTP 结果进行错误分类。
type AttemptErrorKind string

const (
	AttemptErrorNone    AttemptErrorKind = ""
	AttemptErrorTimeout AttemptErrorKind = "TIMEOUT"
	AttemptErrorNetwork AttemptErrorKind = "NETWORK"
	AttemptErrorHTTP4xx AttemptErrorKind = "HTTP_4XX"
	AttemptErrorHTTP429 AttemptErrorKind = "HTTP_429"
	AttemptErrorHTTP5xx AttemptErrorKind = "HTTP_5XX"
)

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

// DeliveryAttempt 表示一次实际发出 HTTP 请求后生成的不可变、只追加投递尝试记录。
type DeliveryAttempt struct {
	ID              int64
	TaskID          uuid.UUID
	ReplayCount     int
	AttemptNo       int
	HTTPStatus      int
	DurationMs      int
	ErrorKind       AttemptErrorKind
	ErrorMessage    string
	ResponseSnippet string
	CreatedAt       time.Time
}

// OutboxItem 表示与 Task 在同一事务边界内登记的事务发件箱（Outbox）调度意图。
type OutboxItem struct {
	ID        int64
	TaskID    uuid.UUID
	DeliverAt time.Time
	CreatedAt time.Time
}

// DeliveryRequest 承载出站 HTTP 适配器发起单次投递尝试所需的全部数据。
type DeliveryRequest struct {
	TaskID        uuid.UUID
	EndpointURL   string
	SigningSecret string
	AttemptNo     int
	Timeout       time.Duration
	Headers       map[string]string
	RawPayload    []byte
	SentAt        time.Time
}

// DeliveryResult 汇总单次出站 HTTP 投递尝试的返回结果。
type DeliveryResult struct {
	HTTPStatus      int
	DurationMs      int
	ErrorKind       AttemptErrorKind
	ErrorMessage    string
	ResponseSnippet string
	RetryAfter      time.Duration
}

// CreateTaskResult 表示任务创建结果以及本次提交是否新建了 Task 记录。
type CreateTaskResult struct {
	Task    Task
	Created bool
}

// ReadyTicket 标识从就绪流中拉取到的 Task 及其偏移量坐标。
type ReadyTicket struct {
	TaskID    uuid.UUID
	Topic     string
	Partition int32
	Offset    int64
	Epoch     int32
}

// ComputePayloadSHA256 计算原始载荷字节切片的小写十六进制 SHA-256 摘要。
func ComputePayloadSHA256(rawPayload []byte) string {
	sum := sha256.Sum256(rawPayload)
	return hex.EncodeToString(sum[:])
}

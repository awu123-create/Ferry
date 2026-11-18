package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// TaskStatus represents the five persisted lifecycle states of a Task.
type TaskStatus string

const (
	TaskStatusPending    TaskStatus = "PENDING"
	TaskStatusRetrying   TaskStatus = "RETRYING"
	TaskStatusSucceeded  TaskStatus = "SUCCEEDED"
	TaskStatusDeadLetter TaskStatus = "DEAD_LETTER"
	TaskStatusCancelled  TaskStatus = "CANCELLED"
)

// ParseTaskStatus validates and converts a persisted status string into TaskStatus.
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

// IsTerminal reports whether the Task has reached a final state.
func (s TaskStatus) IsTerminal() bool {
	return s == TaskStatusSucceeded || s == TaskStatusDeadLetter || s == TaskStatusCancelled
}

// IsActive reports whether the Task is still eligible for delivery or cancellation.
func (s TaskStatus) IsActive() bool {
	return s == TaskStatusPending || s == TaskStatusRetrying
}

// AttemptErrorKind classifies the outbound HTTP outcome of a single Delivery Attempt.
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
	// DeadLetterReasonDeadlineExceeded records termination due to crossing DeadlineAt.
	DeadLetterReasonDeadlineExceeded = "TTL_EXPIRED"
	// DeadLetterReasonEndpointDisabled records termination because the Endpoint was disabled.
	DeadLetterReasonEndpointDisabled = "ENDPOINT_DISABLED"
	// DeadLetterReasonNonRetryable4xx records termination on a non-retryable 4xx contract error.
	DeadLetterReasonNonRetryable4xx = "NON_RETRYABLE_4XX"
	// DeadLetterReasonMaxAttemptsExhausted records termination after exhausting MaxAttempts.
	DeadLetterReasonMaxAttemptsExhausted = "MAX_ATTEMPTS_EXHAUSTED"
)

// Task is the aggregate root representing a single reliable HTTP POST delivery unit.
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

// DeliveryAttempt is an immutable append-only audit record for one HTTP attempt.
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

// OutboxItem represents a pending dispatch intention recorded in the same transaction as a Task.
type OutboxItem struct {
	ID        int64
	TaskID    uuid.UUID
	DeliverAt time.Time
	CreatedAt time.Time
}

// DeliveryRequest carries all data needed by the outbound HTTP adapter to dispatch one attempt.
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

// DeliveryResult summarizes the outcome of a single outbound HTTP attempt.
type DeliveryResult struct {
	HTTPStatus      int
	DurationMs      int
	ErrorKind       AttemptErrorKind
	ErrorMessage    string
	ResponseSnippet string
	RetryAfter      time.Duration
}

// CreateTaskResult reports the Task and whether this submission created a new record.
type CreateTaskResult struct {
	Task    Task
	Created bool
}

// ReadyTicket identifies a Task read from the ready stream along with its offset coordinates.
type ReadyTicket struct {
	TaskID    uuid.UUID
	Topic     string
	Partition int32
	Offset    int64
	Epoch     int32
}

// ComputePayloadSHA256 returns the lowercase hex SHA-256 digest of rawPayload bytes.
func ComputePayloadSHA256(rawPayload []byte) string {
	sum := sha256.Sum256(rawPayload)
	return hex.EncodeToString(sum[:])
}

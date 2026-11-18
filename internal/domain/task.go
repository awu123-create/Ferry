package domain

import (
	"time"

	"github.com/google/uuid"
)

// NewTaskParams captures validated input parameters for creating a Task and its initial Outbox intention.
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

// PreDeliveryDecision reports what the Worker should do before calling the outbound HTTP endpoint.
type PreDeliveryDecision struct {
	ShouldDeliver  bool
	AlreadyFinal   bool
	EarlyArrival   bool
	DeadLetterCode string
}

// OutcomeTransition captures the state mutation, append-only DeliveryAttempt, and circuit feedback
// returned by Task.RecordOutcome for single-transaction persistence.
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

// NewTask validates Task creation invariants and initializes a PENDING Task alongside its OutboxItem.
//
// Contract (Milestone 1 - core logic to be implemented):
//  1. Require params.Endpoint.Enabled == true (return ErrEndpointDisabled otherwise).
//  2. Require trimmed IdempotencyKey non-empty (return NewValidationError("idempotency_key", ...)).
//  3. Require len(RawPayload) > 0 and valid JSON syntax without re-marshaling (keep raw bytes verbatim).
//  4. In Milestone 1, if params.DeliverAt is non-nil and params.DeliverAt.After(params.Now),
//     return ErrFutureDeliveryUnsupported (ADR-0008 §5.1).
//  5. Require MaxAttempts in [1, 20]. Default NextRunAt to DeliverAt (or Now), and DeadlineAt to
//     NextRunAt + DefaultWindow when nil (or verify DeadlineAt.After(NextRunAt)).
//  6. Generate UUIDv7 for Task.ID, compute PayloadSHA256 via ComputePayloadSHA256(RawPayload),
//     set Status = TaskStatusPending, Attempt = 0, ReplayCount = 0, and return the paired OutboxItem.
func NewTask(params NewTaskParams) (Task, OutboxItem, error) {
	_ = params
	return Task{}, OutboxItem{}, ErrNotImplemented
}

// VerifyIdempotentReplay checks whether an incoming submission matches the persisted Task.
//
// Contract (ADR-0004 & ADR-0008):
//   - Verify EndpointID and IdempotencyKey match, and compare PayloadSHA256.
//   - Return nil when identical (caller responds 200 OK with the existing Task).
//   - Return ErrIdempotencyConflict when PayloadSHA256 differs (caller responds 409 Conflict).
func (t Task) VerifyIdempotentReplay(endpointID uuid.UUID, idempotencyKey, payloadSHA256 string) error {
	_ = endpointID
	_ = idempotencyKey
	_ = payloadSHA256
	return ErrNotImplemented
}

// EvaluatePreDelivery enforces Worker Step B guards prior to dispatching an HTTP attempt.
//
// Contract (ADR-0007 & ADR-0008):
//   - If t.Status.IsTerminal(): return PreDeliveryDecision{AlreadyFinal: true}, nil.
//   - If now.Before(t.NextRunAt): return PreDeliveryDecision{EarlyArrival: true}, nil.
//   - If !ep.Enabled: return PreDeliveryDecision{DeadLetterCode: DeadLetterReasonEndpointDisabled}, nil.
//   - If !now.Before(t.DeadlineAt): return PreDeliveryDecision{DeadLetterCode: DeadLetterReasonDeadlineExceeded}, nil.
//   - Otherwise return PreDeliveryDecision{ShouldDeliver: true}, nil.
func (t Task) EvaluatePreDelivery(ep Endpoint, now time.Time) (PreDeliveryDecision, error) {
	_ = ep
	_ = now
	return PreDeliveryDecision{}, ErrNotImplemented
}

// RecordOutcome classifies the DeliveryResult, mutates Task state, and emits an append-only DeliveryAttempt.
//
// Contract (ADR-0005, ADR-0007 & ADR-0008):
//   - Require t.Status.IsActive() (return ErrValidation otherwise).
//   - Increment Attempt (attemptNo = t.Attempt + 1) and construct DeliveryAttempt with TaskID, ReplayCount, AttemptNo, etc.
//   - HTTP 2xx: transition to SUCCEEDED, set CompletedAt = &now, CircuitSuccess = true.
//   - Non-retryable 4xx (excluding 408 and 429): transition immediately to DEAD_LETTER with
//     LastError = DeadLetterReasonNonRetryable4xx (do NOT mark CircuitRetryable = true).
//   - Retryable errors (408, 429, 5xx, network/timeout): mark CircuitRetryable = true; in Milestone 1
//     (when Attempt >= MaxAttempts) transition immediately to DEAD_LETTER with DeadLetterReasonMaxAttemptsExhausted
//     and CompletedAt = &now. (Milestone 2 will compute exponential backoff + Retry-After -> RETRYING.)
func (t *Task) RecordOutcome(result DeliveryResult, now time.Time) (OutcomeTransition, error) {
	_ = result
	_ = now
	return OutcomeTransition{}, ErrNotImplemented
}

// Cancel transitions an active Task (PENDING or RETRYING) to CANCELLED.
//
// Contract (ADR-0007 & ADR-0008):
//   - If t.Status.IsTerminal(): return ErrTaskNotCancellable.
//   - Set Status = TaskStatusCancelled, CompletedAt = &now, UpdatedAt = now.UTC().
func (t *Task) Cancel(now time.Time) error {
	_ = now
	return ErrNotImplemented
}

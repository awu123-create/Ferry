package pg

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/awu123-create/ferry/internal/domain"
)

// CreateTaskIdempotent persists a new Task and its OutboxItem in one PostgreSQL transaction,
// or detects an existing record on (endpoint_id, idempotency_key) and validates payload SHA-256.
//
// Implementation notes (Milestone 1 - core logic to be implemented):
//  1. Open a single transaction via s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error { ... }).
//  2. Convert task to toTaskPO(task) and insert with:
//     tx.Clauses(clause.OnConflict{
//     Columns:   []clause.Column{{Name: "endpoint_id"}, {Name: "idempotency_key"}},
//     DoNothing: true,
//     }).Create(&po)
//  3. If res.RowsAffected == 1:
//     Convert outbox via toOutboxPO(outbox), insert into outbox (DB trigger notifies ferry_outbox),
//     and return CreateTaskResult{Task: task, Created: true}.
//  4. If res.RowsAffected == 0:
//     Query the existing taskPO by (endpoint_id, idempotency_key), convert via toDomainTask,
//     call existing.VerifyIdempotentReplay(task.EndpointID, task.IdempotencyKey, task.PayloadSHA256),
//     and return CreateTaskResult{Task: existing, Created: false} on match (or ErrIdempotencyConflict).
func (s *Store) CreateTaskIdempotent(ctx context.Context, task domain.Task, outbox domain.OutboxItem) (domain.CreateTaskResult, error) {
	_ = ctx
	_ = task
	_ = outbox
	return domain.CreateTaskResult{}, domain.ErrNotImplemented
}

// SavePreDeliveryDeadLetter atomically transitions a Task to DEAD_LETTER before any HTTP call
// using the composite optimistic guard (replay_count, attempt, status IN ('PENDING', 'RETRYING')).
//
// Implementation notes (Milestone 1 - core logic to be implemented):
//   - Execute conditional Update on taskPO where:
//     id = ? AND replay_count = ? AND attempt = ? AND status IN ('PENDING', 'RETRYING')
//   - Set status = DEAD_LETTER, last_error = reason, completed_at = now, updated_at = now.
//   - Return (res.RowsAffected == 1, res.Error).
func (s *Store) SavePreDeliveryDeadLetter(
	ctx context.Context,
	taskID uuid.UUID,
	expectedReplayCount, expectedAttempt int,
	reason string,
	now time.Time,
) (bool, error) {
	_ = ctx
	_ = taskID
	_ = expectedReplayCount
	_ = expectedAttempt
	_ = reason
	_ = now
	return false, domain.ErrNotImplemented
}

// SaveDeliveryOutcome appends the DeliveryAttempt audit row and conditionally transitions the Task
// inside a single PostgreSQL transaction (ADR-0007 & ADR-0008).
//
// Implementation notes (Milestone 1 - core logic to be implemented):
//  1. Open a single transaction via s.db.WithContext(ctx).Transaction(...) (keep it concise and fast).
//  2. Always insert toDeliveryAttemptPO(attempt) into task_attempts so real outbound HTTP calls
//     remain auditable even if a concurrent Cancel won the race under extreme lock expiry.
//  3. Execute conditional CAS Update on taskPO:
//     WHERE id = ? AND replay_count = ? AND attempt = ? AND status IN ('PENDING', 'RETRYING')
//     updating status, attempt, next_run_at, last_error, completed_at, updated_at.
//  4. Return (res.RowsAffected == 1, nil).
func (s *Store) SaveDeliveryOutcome(
	ctx context.Context,
	expectedReplayCount, expectedAttempt int,
	task domain.Task,
	attempt domain.DeliveryAttempt,
) (bool, error) {
	_ = ctx
	_ = expectedReplayCount
	_ = expectedAttempt
	_ = task
	_ = attempt
	return false, domain.ErrNotImplemented
}

// CancelTask atomically moves an active Task (PENDING or RETRYING) to CANCELLED while the caller
// already holds the Redis Execution Lock (ADR-0007 & ADR-0008).
//
// Implementation notes (Milestone 1 - core logic to be implemented):
//  1. Run conditional Update on taskPO where id = ? AND status IN ('PENDING', 'RETRYING')
//     setting status = CANCELLED, completed_at = now, updated_at = now.
//  2. If res.RowsAffected == 1, load and return the updated Task via s.GetTask(ctx, taskID).
//  3. If res.RowsAffected == 0, call s.GetTask(ctx, taskID): return ErrTaskNotFound if missing,
//     or ErrTaskNotCancellable if already in a terminal state.
func (s *Store) CancelTask(ctx context.Context, taskID uuid.UUID, now time.Time) (domain.Task, error) {
	_ = ctx
	_ = taskID
	_ = now
	return domain.Task{}, domain.ErrNotImplemented
}

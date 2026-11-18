package pg

import (
	"context"

	"github.com/awu123-create/ferry/internal/domain"
)

// ClaimAndDispatchImmediateOutbox claims up to limit immediately due outbox rows
// (deliver_at <= now()) using FOR UPDATE SKIP LOCKED inside a bounded transaction,
// invokes dispatch, and physically deletes the rows on success (ADR-0002 & ADR-0008).
//
// Implementation notes (Milestone 1 - core logic to be implemented):
//  1. Bound the transaction context with the configured 5s hard timeout (passed by caller or derived)
//     so a slow downstream Kafka publish never holds row locks and blocks PostgreSQL Autovacuum.
//  2. Inside s.db.WithContext(ctx).Transaction:
//     Query outboxPO with:
//     tx.Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).
//     Where("deliver_at <= ?", time.Now().UTC()).
//     Order("deliver_at ASC, id ASC").
//     Limit(limit).
//     Find(&rows)
//  3. If len(rows) == 0: return 0, nil.
//  4. Convert rows via toDomainOutboxItem and call dispatch(txCtx, items).
//     If dispatch returns an error, return that error to roll back and release row locks.
//  5. Physically delete the dispatched row IDs:
//     tx.Where("id IN ?", ids).Delete(&outboxPO{})
//     and return len(items), nil.
func (s *Store) ClaimAndDispatchImmediateOutbox(
	ctx context.Context,
	limit int,
	dispatch func(ctx context.Context, items []domain.OutboxItem) error,
) (int, error) {
	_ = ctx
	_ = limit
	_ = dispatch
	return 0, domain.ErrNotImplemented
}

// WaitForOutboxNotification listens on PostgreSQL channel 'ferry_outbox' until a notification arrives
// or ctx is cancelled/timed out (ADR-0006 §2.3).
//
// Implementation notes (Milestone 1 - core logic to be implemented):
//  1. Acquire a dedicated connection from s.sqlDB.Conn(ctx) and unwrap the underlying pgx connection:
//     var pgxConn *pgx.Conn
//     conn.Raw(func(driverConn any) error {
//     stdlibConn, ok := driverConn.(*stdlib.Conn) // github.com/jackc/pgx/v5/stdlib
//     ...
//     pgxConn = stdlibConn.Conn()
//     return nil
//     })
//  2. Execute LISTEN ferry_outbox (on first acquire or keep a dedicated listener connection) and call
//     pgxConn.WaitForNotification(ctx) so Relay wakes instantaneously without tight polling.
func (s *Store) WaitForOutboxNotification(ctx context.Context) error {
	_ = ctx
	return domain.ErrNotImplemented
}

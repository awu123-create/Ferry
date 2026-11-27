package pg

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/awu123-create/ferry/internal/domain"
)

const (
	defaultOutboxBatchLimit   = 100
	defaultOutboxBatchTimeout = 5 * time.Second
	outboxNotifyChannel       = "ferry_outbox"
	listenerCloseTimeout      = 2 * time.Second
)

// ClaimAndDispatchImmediateOutbox 在有界事务内通过 FOR UPDATE SKIP LOCKED 抢占最多 limit 条
// 立即到期（deliver_at <= now）的 outbox 记录，调用 dispatch 派发并在成功后物理删除
// （详见 docs/adr/0002-transactional-outbox-batch-relay.md 与 docs/adr/0008-concurrency-fencing-and-flow-control-hardening.md）。
func (s *Store) ClaimAndDispatchImmediateOutbox(
	ctx context.Context,
	now time.Time,
	limit int,
	dispatch func(ctx context.Context, taskIDs []uuid.UUID) error,
) (int, error) {
	if dispatch == nil {
		return 0, fmt.Errorf("outbox dispatch callback must not be nil")
	}

	filter := clause.Lte{Column: clause.Column{Name: "deliver_at"}, Value: now.UTC()}
	return s.claimAndDispatchOutbox(ctx, filter, limit, func(txCtx context.Context, rows []outboxPO) error {
		taskIDs := make([]uuid.UUID, 0, len(rows))
		for _, row := range rows {
			taskIDs = append(taskIDs, row.TaskID)
		}
		return dispatch(txCtx, taskIDs)
	})
}

// ClaimAndDispatchDelayedOutbox 在带超时的事务里，用 FOR UPDATE SKIP LOCKED 取出最多 limit 条
// 还没到点的 outbox 记录（deliver_at > now），交给 dispatch 写入延迟池，成功后再删除这些记录
// （见 docs/adr/0002-transactional-outbox-batch-relay.md 与 docs/adr/0008-concurrency-fencing-and-flow-control-hardening.md）。
func (s *Store) ClaimAndDispatchDelayedOutbox(
	ctx context.Context,
	now time.Time,
	limit int,
	dispatch func(ctx context.Context, items []domain.OutboxItem) error,
) (int, error) {
	if dispatch == nil {
		return 0, fmt.Errorf("outbox dispatch callback must not be nil")
	}

	// 额外加上 deliver_at > created_at：立即任务创建时 deliver_at 一定 <= created_at，
	// 就算并发或不同机器时钟稍有先后，也不会被延迟通道误领进 Redis 延迟池。
	filter := clause.And(
		clause.Gt{Column: clause.Column{Name: "deliver_at"}, Value: now.UTC()},
		clause.Gt{Column: clause.Column{Name: "deliver_at"}, Value: clause.Column{Name: "created_at"}},
	)
	return s.claimAndDispatchOutbox(ctx, filter, limit, func(txCtx context.Context, rows []outboxPO) error {
		items := make([]domain.OutboxItem, 0, len(rows))
		for _, row := range rows {
			items = append(items, toDomainOutboxItem(row))
		}
		return dispatch(txCtx, items)
	})
}

func (s *Store) claimAndDispatchOutbox(
	ctx context.Context,
	filter clause.Expression,
	limit int,
	dispatch func(ctx context.Context, rows []outboxPO) error,
) (int, error) {
	if limit <= 0 {
		limit = defaultOutboxBatchLimit
	}

	txCtx := ctx
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		txCtx, cancel = context.WithTimeout(ctx, defaultOutboxBatchTimeout)
		defer cancel()
	}

	var claimed int
	err := s.db.WithContext(txCtx).Transaction(func(tx *gorm.DB) error {
		var rows []outboxPO
		if err := tx.
			Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}, filter).
			Order("deliver_at ASC, id ASC").
			Limit(limit).
			Find(&rows).Error; err != nil {
			return fmt.Errorf("claim outbox rows: %w", err)
		}
		if len(rows) == 0 {
			return nil
		}

		if err := dispatch(txCtx, rows); err != nil {
			return err
		}

		ids := make([]int64, 0, len(rows))
		for _, row := range rows {
			ids = append(ids, row.ID)
		}
		if err := tx.Where("id IN ?", ids).Delete(&outboxPO{}).Error; err != nil {
			return fmt.Errorf("delete dispatched outbox rows: %w", err)
		}

		claimed = len(rows)
		return nil
	})
	if err != nil {
		return 0, err
	}
	return claimed, nil
}

// WaitForOutboxNotification 复用专用 pgx 连接监听 PostgreSQL 通道 'ferry_outbox'，
// 直到收到通知或 ctx 取消/超时（ADR-0006 §2.3）。
func (s *Store) WaitForOutboxNotification(ctx context.Context) error {
	s.listenMu.Lock()
	defer s.listenMu.Unlock()

	conn, pgxConn, err := s.ensureOutboxListenerLocked(ctx)
	if err != nil {
		return err
	}

	_, waitErr := pgxConn.WaitForNotification(ctx)
	if waitErr == nil {
		return nil
	}

	if pgxConn.IsClosed() || (!errors.Is(waitErr, context.DeadlineExceeded) && !errors.Is(waitErr, context.Canceled)) {
		s.dropOutboxListenerLocked(conn, pgxConn)
		return fmt.Errorf("wait for outbox notification: %w", waitErr)
	}
	return waitErr
}

func (s *Store) ensureOutboxListenerLocked(ctx context.Context) (*sql.Conn, *pgx.Conn, error) {
	if s.listenConn != nil {
		pgxConn, err := unwrapPGXConn(s.listenConn)
		if err == nil && !pgxConn.IsClosed() {
			return s.listenConn, pgxConn, nil
		}
		s.dropOutboxListenerLocked(s.listenConn, pgxConn)
	}

	conn, err := s.sqlDB.Conn(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("acquire outbox listener connection: %w", err)
	}

	pgxConn, err := unwrapPGXConn(conn)
	if err != nil {
		_ = conn.Close()
		return nil, nil, err
	}

	if _, err := pgxConn.Exec(ctx, "LISTEN "+outboxNotifyChannel); err != nil {
		s.dropOutboxListenerLocked(conn, pgxConn)
		return nil, nil, fmt.Errorf("listen on %s: %w", outboxNotifyChannel, err)
	}

	s.listenConn = conn
	return conn, pgxConn, nil
}

func (s *Store) closeOutboxListener() {
	s.listenMu.Lock()
	defer s.listenMu.Unlock()

	if s.listenConn == nil {
		return
	}
	pgxConn, _ := unwrapPGXConn(s.listenConn)
	s.dropOutboxListenerLocked(s.listenConn, pgxConn)
}

func (s *Store) dropOutboxListenerLocked(conn *sql.Conn, pgxConn *pgx.Conn) {
	s.listenConn = nil
	// 显式关闭底层 pgx 连接，防止携带 LISTEN 订阅状态的连接被放回 sql.DB 连接池。
	if pgxConn != nil && !pgxConn.IsClosed() {
		closeCtx, cancel := context.WithTimeout(context.Background(), listenerCloseTimeout)
		_ = pgxConn.Close(closeCtx)
		cancel()
	}
	if conn != nil {
		_ = conn.Close()
	}
}

func unwrapPGXConn(conn *sql.Conn) (*pgx.Conn, error) {
	var pgxConn *pgx.Conn
	if err := conn.Raw(func(driverConn any) error {
		stdlibConn, ok := driverConn.(*stdlib.Conn)
		if !ok {
			return fmt.Errorf("unexpected sql driver connection type %T", driverConn)
		}
		pgxConn = stdlibConn.Conn()
		return nil
	}); err != nil {
		return nil, fmt.Errorf("unwrap pgx connection: %w", err)
	}
	if pgxConn == nil {
		return nil, fmt.Errorf("unwrapped pgx connection is nil")
	}
	return pgxConn, nil
}

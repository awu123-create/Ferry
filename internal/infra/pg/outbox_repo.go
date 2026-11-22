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
	if limit <= 0 {
		limit = defaultOutboxBatchLimit
	}

	txCtx := ctx
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		txCtx, cancel = context.WithTimeout(ctx, defaultOutboxBatchTimeout)
		defer cancel()
	}

	cutoff := now.UTC()
	var claimed int
	err := s.db.WithContext(txCtx).Transaction(func(tx *gorm.DB) error {
		var rows []outboxPO
		if err := tx.
			Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).
			Where("deliver_at <= ?", cutoff).
			Order("deliver_at ASC, id ASC").
			Limit(limit).
			Find(&rows).Error; err != nil {
			return fmt.Errorf("claim immediate outbox rows: %w", err)
		}
		if len(rows) == 0 {
			return nil
		}

		taskIDs := make([]uuid.UUID, 0, len(rows))
		ids := make([]int64, 0, len(rows))
		for _, row := range rows {
			taskIDs = append(taskIDs, row.TaskID)
			ids = append(ids, row.ID)
		}

		if err := dispatch(txCtx, taskIDs); err != nil {
			return err
		}

		if err := tx.Where("id IN ?", ids).Delete(&outboxPO{}).Error; err != nil {
			return fmt.Errorf("delete dispatched outbox rows: %w", err)
		}

		claimed = len(taskIDs)
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

package pg

import (
	"context"

	"github.com/awu123-create/ferry/internal/domain"
)

// ClaimAndDispatchImmediateOutbox 在有界事务内通过 FOR UPDATE SKIP LOCKED 抢占最多 limit 条
// 立即到期（deliver_at <= now()）的 outbox 记录，调用 dispatch 派发并在成功后物理删除（ADR-0002 & ADR-0008）。
//
// 实现指引（Milestone 1 核心逻辑，留待实现）：
//  1. 调用方（或本方法内部）应为该批次事务绑定 5s 硬超时 Context，
//     防止下游 Kafka 抖动时长期持有行锁阻塞 PostgreSQL Autovacuum。
//  2. 在 s.db.WithContext(ctx).Transaction 中执行抢占查询：
//     tx.Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).
//     Where("deliver_at <= ?", time.Now().UTC()).
//     Order("deliver_at ASC, id ASC").
//     Limit(limit).
//     Find(&rows)
//  3. 若 len(rows) == 0：直接返回 0, nil。
//  4. 将行记录转为 []domain.OutboxItem 后调用 dispatch(txCtx, items)；
//     若 dispatch 报错，返回错误以回滚事务并立即释放行锁。
//  5. 物理删除已派发行的 ID：
//     tx.Where("id IN ?", ids).Delete(&outboxPO{})
//     并返回 len(items), nil。
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

// WaitForOutboxNotification 监听 PostgreSQL 通道 'ferry_outbox'，直到收到通知或 ctx 取消/超时（ADR-0006 §2.3）。
//
// 实现指引（Milestone 1 核心逻辑，留待实现）：
//  1. 从 s.sqlDB.Conn(ctx) 获取专用连接，并解包底层 pgx 连接：
//     var pgxConn *pgx.Conn
//     conn.Raw(func(driverConn any) error {
//     stdlibConn, ok := driverConn.(*stdlib.Conn) // github.com/jackc/pgx/v5/stdlib
//     ...
//     pgxConn = stdlibConn.Conn()
//     return nil
//     })
//  2. 执行 LISTEN ferry_outbox（首次获取或复用专用监听连接），并调用
//     pgxConn.WaitForNotification(ctx)，使 OutboxRelay 能够瞬时唤醒而非高频轮询。
func (s *Store) WaitForOutboxNotification(ctx context.Context) error {
	_ = ctx
	return domain.ErrNotImplemented
}

package pg

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/awu123-create/ferry/internal/domain"
)

// CreateTaskIdempotent 在单次 PostgreSQL 事务中原子写入新 Task 与 OutboxItem；
// 若命中 (endpoint_id, idempotency_key) 唯一约束冲突，则回查已有记录并校验载荷 SHA-256。
//
// 实现指引（Milestone 1 核心逻辑，留待实现）：
//  1. 通过 s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error { ... }) 开启单次事务。
//  2. 使用 toTaskPO(task) 转换后执行冲突忽略插入：
//     tx.Clauses(clause.OnConflict{
//     Columns:   []clause.Column{{Name: "endpoint_id"}, {Name: "idempotency_key"}},
//     DoNothing: true,
//     }).Create(&po)
//  3. 若 res.RowsAffected == 1：
//     通过 toOutboxPO(outbox) 转换并写入 outbox（数据库触发器会自动通知 ferry_outbox），
//     返回 CreateTaskResult{Task: task, Created: true}。
//  4. 若 res.RowsAffected == 0：
//     按 (endpoint_id, idempotency_key) 查询已有 taskPO 并转为领域模型，
//     调用 existing.VerifyIdempotentReplay(task.EndpointID, task.IdempotencyKey, task.PayloadSHA256)，
//     一致则返回 CreateTaskResult{Task: existing, Created: false}，不一致返回 ErrIdempotencyConflict。
func (s *Store) CreateTaskIdempotent(ctx context.Context, task domain.Task, outbox domain.OutboxItem) (domain.CreateTaskResult, error) {
	var result domain.CreateTaskResult

	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		po := toTaskPO(task)
		res := tx.Clauses(clause.OnConflict{
			Columns: []clause.Column{
				{Name: "endpoint_id"},
				{Name: "idempotency_key"},
			},
			DoNothing: true,
		}).Create(&po)
		if res.Error != nil {
			return fmt.Errorf("insert task: %w", res.Error)
		}

		switch res.RowsAffected {
		case 1:
			// 说明 task 成功创建了
			outboxPO := toOutboxPO(outbox)
			if err := tx.Create(&outboxPO).Error; err != nil {
				return fmt.Errorf("insert outbox: %w", err)
			}

			result = domain.CreateTaskResult{
				Task:    task,
				Created: true,
			}
		case 0:
			var existPO taskPO
			if err := tx.Where("endpoint_id = ? AND idempotency_key = ?", po.EndpointID, po.IdempotencyKey).First(&existPO).Error; err != nil {
				return fmt.Errorf("query existing idempotent task: %w", err)
			}

			exist, err := toDomainTask(existPO)
			if err != nil {
				return fmt.Errorf("decode existing task: %w", err)
			}
			if err := exist.VerifyIdempotentReplay(task.EndpointID, task.IdempotencyKey, task.PayloadSHA256); err != nil {
				return err
			}

			result = domain.CreateTaskResult{
				Task:    exist,
				Created: false,
			}
		}

		return nil
	})

	if err != nil {
		return domain.CreateTaskResult{}, err
	}
	return result, nil
}

// SavePreDeliveryDeadLetter 在未发起任何 HTTP 请求前，使用复合乐观锁条件
// (replay_count, attempt, status IN ('PENDING', 'RETRYING')) 原子迁移任务至 DEAD_LETTER。
//
// 实现指引（Milestone 1 核心逻辑，留待实现）：
//   - 对 taskPO 执行条件更新：
//     WHERE id = ? AND replay_count = ? AND attempt = ? AND status IN ('PENDING', 'RETRYING')
//   - 更新 status = DEAD_LETTER、last_error = reason、completed_at = now、updated_at = now。
//   - 返回 (res.RowsAffected == 1, res.Error)。
func (s *Store) SavePreDeliveryDeadLetter(
	ctx context.Context,
	taskID uuid.UUID,
	expectedReplayCount, expectedAttempt int,
	reason string,
	now time.Time,
) (bool, error) {
	ts := now.UTC()
	res := s.db.WithContext(ctx).Model(&taskPO{}).Where("id = ? AND replay_count = ? AND attempt = ? AND status IN ('PENDING', 'RETRYING')", taskID, expectedReplayCount, expectedAttempt).Updates(map[string]any{
		"status":       string(domain.TaskStatusDeadLetter),
		"last_error":   reason,
		"completed_at": ts,
		"updated_at":   ts,
	})
	return res.RowsAffected == 1, res.Error
}

// SaveDeliveryOutcome 在单次 PostgreSQL 事务中追加 DeliveryAttempt 审计流水，
// 并按复合乐观锁条件迁移 Task 状态（ADR-0007 & ADR-0008）。
//
// 实现指引（Milestone 1 核心逻辑，留待实现）：
//  1. 通过 s.db.WithContext(ctx).Transaction(...) 开启短事务。
//  2. 始终插入 toDeliveryAttemptPO(attempt) 到 task_attempts 表，确保即使极端锁过期与取消并发重叠，
//     实际发出的 HTTP 请求依然保留完整审计轨迹。
//  3. 对 taskPO 执行复合乐观锁 CAS 更新：
//     WHERE id = ? AND replay_count = ? AND attempt = ? AND status IN ('PENDING', 'RETRYING')
//     更新 status、attempt、next_run_at、last_error、completed_at、updated_at。
//  4. 返回 (res.RowsAffected == 1, nil)。
func (s *Store) SaveDeliveryOutcome(
	ctx context.Context,
	expectedReplayCount, expectedAttempt int,
	task domain.Task,
	attempt domain.DeliveryAttempt,
) (bool, error) {
	var applied bool

	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		poAttempt := toDeliveryAttemptPO(attempt)
		if err := tx.Create(&poAttempt).Error; err != nil {
			return fmt.Errorf("insert delivery attempt: %w", err)
		}

		poTask := toTaskPO(task)
		res := tx.Model(&taskPO{}).Where("id = ? AND replay_count = ? AND attempt = ? AND status IN ('PENDING','RETRYING')",
			poTask.ID, expectedReplayCount, expectedAttempt).Updates(map[string]any{
			"status":       poTask.Status,
			"attempt":      poTask.Attempt,
			"next_run_at":  poTask.NextRunAt,
			"last_error":   poTask.LastError,
			"completed_at": poTask.CompletedAt,
			"updated_at":   poTask.UpdatedAt,
		})
		if res.Error != nil {
			return fmt.Errorf("update task outcome: %w", res.Error)
		}

		applied = res.RowsAffected == 1
		return nil
	})
	if err != nil {
		return false, err
	}
	return applied, nil
}

// CancelTask 在调用方已持有 Redis Execution Lock 的前提下，
// 原子将非终态（PENDING 或 RETRYING）的 Task 迁移为 CANCELLED（ADR-0007 & ADR-0008）。
//
// 实现指引（Milestone 1 核心逻辑，留待实现）：
//  1. 对 taskPO 执行条件更新：WHERE id = ? AND status IN ('PENDING', 'RETRYING')，
//     设置 status = CANCELLED、completed_at = now、updated_at = now。
//  2. 若 res.RowsAffected == 1，调用 s.GetTask(ctx, taskID) 并返回更新后的 Task。
//  3. 若 res.RowsAffected == 0，调用 s.GetTask(ctx, taskID)：不存在则返回 ErrTaskNotFound，
//     已处于终态则返回 ErrTaskNotCancellable。
func (s *Store) CancelTask(ctx context.Context, taskID uuid.UUID, now time.Time) (domain.Task, error) {
	ts := now.UTC()
	res := s.db.WithContext(ctx).Model(&taskPO{}).Where("id = ? AND status IN ('PENDING', 'RETRYING')", taskID).Updates(map[string]any{
		"status":       string(domain.TaskStatusCancelled),
		"completed_at": ts,
		"updated_at":   ts,
	})
	if res.Error != nil {
		return domain.Task{}, fmt.Errorf("cancel task: %w", res.Error)
	}

	if res.RowsAffected == 1 {
		return s.GetTask(ctx, taskID)
	}

	task, err := s.GetTask(ctx, taskID)
	if err != nil {
		return domain.Task{}, err
	}
	if task.Status.IsTerminal() {
		return domain.Task{}, domain.ErrTaskNotCancellable
	}
	return domain.Task{}, fmt.Errorf("cancel task %s: concurrent state transition (status=%s): %w", taskID, task.Status, domain.ErrTaskNotCancellable)
}

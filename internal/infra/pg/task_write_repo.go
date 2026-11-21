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

// CreateTaskIdempotent 在单次 PostgreSQL 事务中原子写入新 Task 与对应 outbox 行；
// 若命中 (endpoint_id, idempotency_key) 唯一约束冲突，则回查已有记录并校验载荷 SHA-256 与自定义 Headers。
func (s *Store) CreateTaskIdempotent(ctx context.Context, task domain.Task) (domain.CreateTaskResult, error) {
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
			// 说明 task 成功创建，在同事务内写入 outbox 行供 OutboxRelay 搬运。
			outbox := outboxPO{
				TaskID:    task.ID,
				DeliverAt: task.NextRunAt.UTC(),
				CreatedAt: task.CreatedAt.UTC(),
			}
			if err := tx.Create(&outbox).Error; err != nil {
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
			if err := exist.VerifyIdempotentReplay(task.EndpointID, task.IdempotencyKey, task.PayloadSHA256, task.Headers); err != nil {
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

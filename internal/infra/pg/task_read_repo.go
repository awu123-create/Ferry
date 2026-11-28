package pg

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/awu123-create/ferry/internal/domain"
)

// GetTask 根据 ID 读取单行 Task，不加载历史 DeliveryAttempt 明细以防聚合膨胀
// （见 docs/adr/0007-tactical-ddd-and-deep-module-seams.md）。
func (s *Store) GetTask(ctx context.Context, id uuid.UUID) (domain.Task, error) {
	var po taskPO
	err := s.db.WithContext(ctx).Where("id = ?", id).First(&po).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return domain.Task{}, domain.ErrTaskNotFound
	}
	if err != nil {
		return domain.Task{}, fmt.Errorf("select task: %w", err)
	}
	task, err := toDomainTask(po)
	if err != nil {
		return domain.Task{}, fmt.Errorf("decode task row: %w", err)
	}
	return task, nil
}

// ListDeliveryAttempts 按时间顺序返回指定 Task 的只追加 DeliveryAttempt 流水记录。
func (s *Store) ListDeliveryAttempts(ctx context.Context, taskID uuid.UUID) ([]domain.DeliveryAttempt, error) {
	if _, err := s.GetTask(ctx, taskID); err != nil {
		return nil, err
	}

	var pos []deliveryAttemptPO
	if err := s.db.WithContext(ctx).
		Where("task_id = ?", taskID).
		Order("replay_count ASC, attempt_no ASC, id ASC").
		Find(&pos).Error; err != nil {
		return nil, fmt.Errorf("list delivery attempts: %w", err)
	}
	out := make([]domain.DeliveryAttempt, 0, len(pos))
	for _, po := range pos {
		att, err := toDomainDeliveryAttempt(po)
		if err != nil {
			return nil, fmt.Errorf("decode delivery attempt row: %w", err)
		}
		out = append(out, att)
	}
	return out, nil
}

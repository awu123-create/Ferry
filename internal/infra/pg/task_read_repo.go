package pg

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/awu123-create/ferry/internal/domain"
)

// GetTask loads a single Task row by ID without loading DeliveryAttempt history (ADR-0007).
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

// ListDeliveryAttempts returns append-only DeliveryAttempt rows for a Task ordered chronologically.
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
		out = append(out, toDomainDeliveryAttempt(po))
	}
	return out, nil
}

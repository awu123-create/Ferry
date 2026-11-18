package pg

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/awu123-create/ferry/internal/domain"
)

// CreateEndpoint persists a new Endpoint row.
func (s *Store) CreateEndpoint(ctx context.Context, ep domain.Endpoint) error {
	po := toEndpointPO(ep)
	if err := s.db.WithContext(ctx).Create(&po).Error; err != nil {
		return fmt.Errorf("insert endpoint: %w", err)
	}
	return nil
}

// GetEndpoint loads a single Endpoint by ID or returns domain.ErrEndpointNotFound.
func (s *Store) GetEndpoint(ctx context.Context, id uuid.UUID) (domain.Endpoint, error) {
	var po endpointPO
	err := s.db.WithContext(ctx).Where("id = ?", id).First(&po).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return domain.Endpoint{}, domain.ErrEndpointNotFound
	}
	if err != nil {
		return domain.Endpoint{}, fmt.Errorf("select endpoint: %w", err)
	}
	return toDomainEndpoint(po), nil
}

// ListEndpoints returns all registered Endpoints ordered newest first.
func (s *Store) ListEndpoints(ctx context.Context) ([]domain.Endpoint, error) {
	var pos []endpointPO
	if err := s.db.WithContext(ctx).Order("created_at DESC, id DESC").Find(&pos).Error; err != nil {
		return nil, fmt.Errorf("list endpoints: %w", err)
	}
	out := make([]domain.Endpoint, 0, len(pos))
	for _, po := range pos {
		out = append(out, toDomainEndpoint(po))
	}
	return out, nil
}

// UpdateEndpoint persists changes to an existing Endpoint or returns domain.ErrEndpointNotFound.
func (s *Store) UpdateEndpoint(ctx context.Context, ep domain.Endpoint) error {
	po := toEndpointPO(ep)
	res := s.db.WithContext(ctx).
		Model(&endpointPO{}).
		Where("id = ?", ep.ID).
		Updates(map[string]any{
			"name":              po.Name,
			"target_url":        po.TargetURL,
			"signing_secret":    po.SigningSecret,
			"rate_limit_qps":    po.RateLimitQPS,
			"max_concurrency":   po.MaxConcurrency,
			"circuit_threshold": po.CircuitThreshold,
			"timeout_ms":        po.TimeoutMs,
			"enabled":           po.Enabled,
			"updated_at":        po.UpdatedAt,
		})
	if res.Error != nil {
		return fmt.Errorf("update endpoint: %w", res.Error)
	}
	if res.RowsAffected == 0 {
		return domain.ErrEndpointNotFound
	}
	return nil
}

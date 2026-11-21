package pg

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"gorm.io/datatypes"

	"github.com/awu123-create/ferry/internal/domain"
)

type endpointPO struct {
	ID             uuid.UUID `gorm:"column:id;type:uuid;primaryKey"`
	Name           string    `gorm:"column:name;type:text;not null"`
	TargetURL      string    `gorm:"column:target_url;type:text;not null"`
	SigningSecret  string    `gorm:"column:signing_secret;type:text;not null;default:''"`
	RateLimitQPS   float64   `gorm:"column:rate_limit_qps;type:double precision;not null"`
	MaxConcurrency int       `gorm:"column:max_concurrency;type:int;not null"`

	// CircuitThreshold 表示连续失败次数达到该阈值时触发熔断，后续投递尝试将立即返回 ErrCircuitOpen。
	CircuitThreshold int       `gorm:"column:circuit_threshold;type:int;not null"`
	TimeoutMs        int       `gorm:"column:timeout_ms;type:int;not null"`
	Enabled          bool      `gorm:"column:enabled;type:boolean;not null"`
	CreatedAt        time.Time `gorm:"column:created_at;type:timestamptz;not null"`
	UpdatedAt        time.Time `gorm:"column:updated_at;type:timestamptz;not null"`
}

func (endpointPO) TableName() string {
	return "endpoints"
}

type taskPO struct {
	ID             uuid.UUID         `gorm:"column:id;type:uuid;primaryKey"`
	EndpointID     uuid.UUID         `gorm:"column:endpoint_id;type:uuid;not null"`
	IdempotencyKey string            `gorm:"column:idempotency_key;type:text;not null"`
	Payload        []byte            `gorm:"column:payload;type:bytea;not null"`
	PayloadSHA256  string            `gorm:"column:payload_sha256;type:text;not null"`
	Headers        datatypes.JSONMap `gorm:"column:headers;type:jsonb;not null"`
	Status         string            `gorm:"column:status;type:text;not null"`
	Attempt        int               `gorm:"column:attempt;type:int;not null"`
	MaxAttempts    int               `gorm:"column:max_attempts;type:int;not null"`
	NextRunAt      time.Time         `gorm:"column:next_run_at;type:timestamptz;not null"`
	DeadlineAt     time.Time         `gorm:"column:deadline_at;type:timestamptz;not null"`
	LastError      string            `gorm:"column:last_error;type:text;not null"`
	ReplayCount    int               `gorm:"column:replay_count;type:int;not null"`
	CreatedAt      time.Time         `gorm:"column:created_at;type:timestamptz;not null"`
	UpdatedAt      time.Time         `gorm:"column:updated_at;type:timestamptz;not null"`
	CompletedAt    *time.Time        `gorm:"column:completed_at;type:timestamptz"`
}

func (taskPO) TableName() string {
	return "tasks"
}

type deliveryAttemptPO struct {
	ID              int64     `gorm:"column:id;primaryKey;autoIncrement"`
	TaskID          uuid.UUID `gorm:"column:task_id;type:uuid;not null"`
	ReplayCount     int       `gorm:"column:replay_count;type:int;not null"`
	AttemptNo       int       `gorm:"column:attempt_no;type:int;not null"`
	HTTPStatus      int       `gorm:"column:http_status;type:int;not null"`
	DurationMs      int       `gorm:"column:duration_ms;type:int;not null"`
	ErrorKind       string    `gorm:"column:error_kind;type:text;not null"`
	ErrorMessage    string    `gorm:"column:error_message;type:text;not null"`
	ResponseSnippet string    `gorm:"column:response_snippet;type:text;not null"`
	CreatedAt       time.Time `gorm:"column:created_at;type:timestamptz;not null"`
}

func (deliveryAttemptPO) TableName() string {
	return "task_attempts"
}

type outboxPO struct {
	ID        int64     `gorm:"column:id;primaryKey;autoIncrement"`
	TaskID    uuid.UUID `gorm:"column:task_id;type:uuid;not null"`
	DeliverAt time.Time `gorm:"column:deliver_at;type:timestamptz;not null"`
	CreatedAt time.Time `gorm:"column:created_at;type:timestamptz;not null"`
}

func (outboxPO) TableName() string {
	return "outbox"
}

func toEndpointPO(ep domain.Endpoint) endpointPO {
	return endpointPO{
		ID:               ep.ID,
		Name:             ep.Name,
		TargetURL:        ep.TargetURL,
		SigningSecret:    ep.SigningSecret,
		RateLimitQPS:     ep.RateLimitQPS,
		MaxConcurrency:   ep.MaxConcurrency,
		CircuitThreshold: ep.CircuitThreshold,
		TimeoutMs:        ep.TimeoutMs,
		Enabled:          ep.Enabled,
		CreatedAt:        ep.CreatedAt.UTC(),
		UpdatedAt:        ep.UpdatedAt.UTC(),
	}
}

func toDomainEndpoint(po endpointPO) domain.Endpoint {
	return domain.Endpoint{
		ID:               po.ID,
		Name:             po.Name,
		TargetURL:        po.TargetURL,
		SigningSecret:    po.SigningSecret,
		RateLimitQPS:     po.RateLimitQPS,
		MaxConcurrency:   po.MaxConcurrency,
		CircuitThreshold: po.CircuitThreshold,
		TimeoutMs:        po.TimeoutMs,
		Enabled:          po.Enabled,
		CreatedAt:        po.CreatedAt.UTC(),
		UpdatedAt:        po.UpdatedAt.UTC(),
	}
}

func toTaskPO(task domain.Task) taskPO {
	headers := make(datatypes.JSONMap, len(task.Headers))
	for k, v := range task.Headers {
		headers[k] = v
	}
	var completedAt *time.Time
	if task.CompletedAt != nil {
		ts := task.CompletedAt.UTC()
		completedAt = &ts
	}
	return taskPO{
		ID:             task.ID,
		EndpointID:     task.EndpointID,
		IdempotencyKey: task.IdempotencyKey,
		Payload:        task.Payload,
		PayloadSHA256:  task.PayloadSHA256,
		Headers:        headers,
		Status:         string(task.Status),
		Attempt:        task.Attempt,
		MaxAttempts:    task.MaxAttempts,
		NextRunAt:      task.NextRunAt.UTC(),
		DeadlineAt:     task.DeadlineAt.UTC(),
		LastError:      task.LastError,
		ReplayCount:    task.ReplayCount,
		CreatedAt:      task.CreatedAt.UTC(),
		UpdatedAt:      task.UpdatedAt.UTC(),
		CompletedAt:    completedAt,
	}
}

func toDomainTask(po taskPO) (domain.Task, error) {
	status, err := domain.ParseTaskStatus(po.Status)
	if err != nil {
		return domain.Task{}, err
	}
	headers := make(map[string]string, len(po.Headers))
	for k, v := range po.Headers {
		if v == nil {
			continue
		}
		if s, ok := v.(string); ok {
			headers[k] = s
		} else {
			headers[k] = fmt.Sprint(v)
		}
	}
	var completedAt *time.Time
	if po.CompletedAt != nil {
		ts := po.CompletedAt.UTC()
		completedAt = &ts
	}
	return domain.Task{
		ID:             po.ID,
		EndpointID:     po.EndpointID,
		IdempotencyKey: po.IdempotencyKey,
		Payload:        po.Payload,
		PayloadSHA256:  po.PayloadSHA256,
		Headers:        headers,
		Status:         status,
		Attempt:        po.Attempt,
		MaxAttempts:    po.MaxAttempts,
		NextRunAt:      po.NextRunAt.UTC(),
		DeadlineAt:     po.DeadlineAt.UTC(),
		LastError:      po.LastError,
		ReplayCount:    po.ReplayCount,
		CreatedAt:      po.CreatedAt.UTC(),
		UpdatedAt:      po.UpdatedAt.UTC(),
		CompletedAt:    completedAt,
	}, nil
}

func toDeliveryAttemptPO(att domain.DeliveryAttempt) deliveryAttemptPO {
	return deliveryAttemptPO{
		ID:              att.ID,
		TaskID:          att.TaskID,
		ReplayCount:     att.ReplayCount,
		AttemptNo:       att.AttemptNo,
		HTTPStatus:      att.HTTPStatus,
		DurationMs:      att.DurationMs,
		ErrorKind:       string(att.ErrorKind),
		ErrorMessage:    att.ErrorMessage,
		ResponseSnippet: att.ResponseSnippet,
		CreatedAt:       att.CreatedAt.UTC(),
	}
}

func toDomainDeliveryAttempt(po deliveryAttemptPO) domain.DeliveryAttempt {
	return domain.DeliveryAttempt{
		ID:              po.ID,
		TaskID:          po.TaskID,
		ReplayCount:     po.ReplayCount,
		AttemptNo:       po.AttemptNo,
		HTTPStatus:      po.HTTPStatus,
		DurationMs:      po.DurationMs,
		ErrorKind:       domain.AttemptErrorKind(po.ErrorKind),
		ErrorMessage:    po.ErrorMessage,
		ResponseSnippet: po.ResponseSnippet,
		CreatedAt:       po.CreatedAt.UTC(),
	}
}

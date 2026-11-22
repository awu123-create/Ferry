package api

import (
	"context"
	"encoding/json/jsontext"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/awu123-create/ferry/internal/domain"
	"github.com/awu123-create/ferry/internal/logger"
)

const taskEnvelopeOverheadBytes = 64 << 10

type createTaskRequest struct {
	EndpointID     string            `json:"endpoint_id"`
	IdempotencyKey string            `json:"idempotency_key,omitempty"`
	Payload        jsontext.Value    `json:"payload"`
	Headers        map[string]string `json:"headers,omitempty"`
	DeliverAt      *time.Time        `json:"deliver_at,omitempty"`
	MaxAttempts    *int              `json:"max_attempts,omitempty"`
	DeadlineAt     *time.Time        `json:"deadline_at,omitempty"`
}

type createTaskInput struct {
	EndpointID     uuid.UUID
	IdempotencyKey string
	RawPayload     []byte
	Headers        map[string]string
	DeliverAt      *time.Time
	MaxAttempts    int
	DeadlineAt     *time.Time
}

type taskResponse struct {
	ID             uuid.UUID         `json:"id"`
	EndpointID     uuid.UUID         `json:"endpoint_id"`
	IdempotencyKey string            `json:"idempotency_key"`
	Payload        jsontext.Value    `json:"payload"`
	PayloadSHA256  string            `json:"payload_sha256"`
	Headers        map[string]string `json:"headers"`
	Status         string            `json:"status"`
	Attempt        int               `json:"attempt"`
	MaxAttempts    int               `json:"max_attempts"`
	NextRunAt      time.Time         `json:"next_run_at"`
	DeadlineAt     time.Time         `json:"deadline_at"`
	LastError      string            `json:"last_error,omitempty"`
	ReplayCount    int               `json:"replay_count"`
	CreatedAt      time.Time         `json:"created_at"`
	UpdatedAt      time.Time         `json:"updated_at"`
	CompletedAt    *time.Time        `json:"completed_at,omitempty"`
}

type deliveryAttemptResponse struct {
	ID              int64     `json:"id"`
	TaskID          uuid.UUID `json:"task_id"`
	ReplayCount     int       `json:"replay_count"`
	AttemptNo       int       `json:"attempt_no"`
	HTTPStatus      int       `json:"http_status"`
	DurationMs      int       `json:"duration_ms"`
	ErrorKind       string    `json:"error_kind,omitempty"`
	ErrorMessage    string    `json:"error_message,omitempty"`
	ResponseSnippet string    `json:"response_snippet,omitempty"`
	CreatedAt       time.Time `json:"created_at"`
}

func toTaskResponse(task domain.Task) taskResponse {
	rawPayload := jsontext.Value(task.Payload)
	if !rawPayload.IsValid() {
		rawPayload = jsontext.Value("null")
	}
	return taskResponse{
		ID:             task.ID,
		EndpointID:     task.EndpointID,
		IdempotencyKey: task.IdempotencyKey,
		Payload:        rawPayload,
		PayloadSHA256:  task.PayloadSHA256,
		Headers:        task.Headers,
		Status:         string(task.Status),
		Attempt:        task.Attempt,
		MaxAttempts:    task.MaxAttempts,
		NextRunAt:      task.NextRunAt,
		DeadlineAt:     task.DeadlineAt,
		LastError:      task.LastError,
		ReplayCount:    task.ReplayCount,
		CreatedAt:      task.CreatedAt,
		UpdatedAt:      task.UpdatedAt,
		CompletedAt:    task.CompletedAt,
	}
}

func toDeliveryAttemptResponse(att domain.DeliveryAttempt) deliveryAttemptResponse {
	return deliveryAttemptResponse{
		ID:              att.ID,
		TaskID:          att.TaskID,
		ReplayCount:     att.ReplayCount,
		AttemptNo:       att.AttemptNo,
		HTTPStatus:      att.HTTPStatus,
		DurationMs:      att.DurationMs,
		ErrorKind:       string(att.ErrorKind),
		ErrorMessage:    att.ErrorMessage,
		ResponseSnippet: att.ResponseSnippet,
		CreatedAt:       att.CreatedAt,
	}
}

func (h *Handler) handleCreateTask(w http.ResponseWriter, r *http.Request) {
	var req createTaskRequest
	maxBody := int64(h.cfg.MaxPayloadBytes) + taskEnvelopeOverheadBytes
	if err := decodeJSONBody(w, r, maxBody, &req); err != nil {
		writeErrorJSON(w, http.StatusBadRequest, errCodeInvalidJSON, err.Error())
		return
	}

	endpointID, err := uuid.Parse(strings.TrimSpace(req.EndpointID))
	if err != nil {
		writeErrorJSON(w, http.StatusBadRequest, errCodeInvalidEndpointID, "endpoint_id must be a valid UUID")
		return
	}

	idempotencyKey, err := resolveIdempotencyKey(r.Header.Get("Idempotency-Key"), req.IdempotencyKey)
	if err != nil {
		h.writeDomainError(w, err)
		return
	}

	if len(req.Payload) == 0 || !req.Payload.IsValid() {
		h.writeDomainError(w, domain.NewValidationError("payload", "must be a non-empty valid JSON value"))
		return
	}
	if len(req.Payload) > h.cfg.MaxPayloadBytes {
		h.writeDomainError(w, domain.NewValidationError("payload", "exceeds maximum allowed byte size"))
		return
	}

	maxAttempts := h.cfg.DefaultMaxAttempts
	if req.MaxAttempts != nil {
		maxAttempts = *req.MaxAttempts
	}

	res, err := h.createTask(r.Context(), createTaskInput{
		EndpointID:     endpointID,
		IdempotencyKey: idempotencyKey,
		RawPayload:     []byte(req.Payload),
		Headers:        req.Headers,
		DeliverAt:      req.DeliverAt,
		MaxAttempts:    maxAttempts,
		DeadlineAt:     req.DeadlineAt,
	})
	if err != nil {
		h.writeDomainError(w, err)
		return
	}

	status := http.StatusOK
	if res.Created {
		status = http.StatusAccepted
	}
	writeJSON(w, status, toTaskResponse(res.Task))
}

func (h *Handler) handleGetTask(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeErrorJSON(w, http.StatusBadRequest, errCodeInvalidTaskID, "task id must be a valid UUID")
		return
	}

	ctx := logger.WithTaskID(r.Context(), id)
	task, err := h.tasks.GetTask(ctx, id)
	if err != nil {
		h.writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toTaskResponse(task))
}

func (h *Handler) handleListTaskAttempts(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeErrorJSON(w, http.StatusBadRequest, errCodeInvalidTaskID, "task id must be a valid UUID")
		return
	}

	ctx := logger.WithTaskID(r.Context(), id)
	attempts, err := h.tasks.ListDeliveryAttempts(ctx, id)
	if err != nil {
		h.writeDomainError(w, err)
		return
	}
	resp := make([]deliveryAttemptResponse, 0, len(attempts))
	for _, att := range attempts {
		resp = append(resp, toDeliveryAttemptResponse(att))
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": resp})
}

func (h *Handler) handleCancelTask(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeErrorJSON(w, http.StatusBadRequest, errCodeInvalidTaskID, "task id must be a valid UUID")
		return
	}

	ctx := logger.WithTaskID(r.Context(), id)
	task, err := h.cancelTask(ctx, id)
	if err != nil {
		h.writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toTaskResponse(task))
}

func resolveIdempotencyKey(headerVal, bodyVal string) (string, error) {
	hKey := strings.TrimSpace(headerVal)
	bKey := strings.TrimSpace(bodyVal)
	if hKey != "" && bKey != "" && hKey != bKey {
		return "", domain.NewValidationError("idempotency_key", "header and body values do not match")
	}
	if hKey != "" {
		return hKey, nil
	}
	if bKey != "" {
		return bKey, nil
	}
	return "", domain.NewValidationError("idempotency_key", "must be provided via Idempotency-Key header or body")
}

// createTask 编排幂等 Task 原子创建流程（Outbox 行由存储层在同一事务中自动写入）。
func (h *Handler) createTask(ctx context.Context, input createTaskInput) (domain.CreateTaskResult, error) {
	ep, err := h.endpoints.GetEndpoint(ctx, input.EndpointID)
	if err != nil {
		return domain.CreateTaskResult{}, err
	}

	task, err := domain.NewTask(domain.NewTaskParams{
		Endpoint:       ep,
		IdempotencyKey: input.IdempotencyKey,
		RawPayload:     input.RawPayload,
		Headers:        input.Headers,
		DeliverAt:      input.DeliverAt,
		MaxAttempts:    input.MaxAttempts,
		DeadlineAt:     input.DeadlineAt,
		DefaultWindow:  h.cfg.DefaultTaskWindow,
		Now:            h.now(),
	})
	if err != nil {
		return domain.CreateTaskResult{}, err
	}

	return h.tasks.CreateTaskIdempotent(ctx, task)
}

// cancelTask 先抢占 Execution Lock，再在 PostgreSQL 中原子取消处于非终态的 Task（ADR-0008 §3）。
func (h *Handler) cancelTask(ctx context.Context, taskID uuid.UUID) (domain.Task, error) {
	token, acquired, err := h.locker.AcquireExecutionLock(ctx, taskID, h.cfg.CancelLockTTL)
	if err != nil {
		return domain.Task{}, err
	}
	if !acquired {
		return domain.Task{}, domain.ErrTaskExecutionInFlight
	}
	defer func() {
		releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if relErr := h.locker.ReleaseExecutionLock(releaseCtx, taskID, token); relErr != nil {
			h.log.Warn(
				"release execution lock after cancel failed",
				zap.String("task_id", taskID.String()),
				zap.Error(relErr),
			)
		}
	}()

	return h.tasks.CancelTask(ctx, taskID, h.now())
}

package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/awu123-create/ferry/internal/domain"
)

const taskEnvelopeOverheadBytes = 64 << 10

type createTaskRequest struct {
	EndpointID     string            `json:"endpoint_id"`
	IdempotencyKey string            `json:"idempotency_key,omitempty"`
	Payload        json.RawMessage   `json:"payload"`
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
	Payload        json.RawMessage   `json:"payload"`
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
	rawPayload := json.RawMessage(task.Payload)
	if !json.Valid(rawPayload) {
		rawPayload = json.RawMessage("null")
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
		writeErrorJSON(w, http.StatusBadRequest, "INVALID_JSON", err.Error())
		return
	}

	endpointID, err := uuid.Parse(strings.TrimSpace(req.EndpointID))
	if err != nil {
		writeErrorJSON(w, http.StatusBadRequest, "INVALID_ENDPOINT_ID", "endpoint_id must be a valid UUID")
		return
	}

	idempotencyKey, err := resolveIdempotencyKey(r.Header.Get("Idempotency-Key"), req.IdempotencyKey)
	if err != nil {
		h.writeDomainError(w, err)
		return
	}

	if len(req.Payload) == 0 || !json.Valid(req.Payload) {
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
		writeErrorJSON(w, http.StatusBadRequest, "INVALID_TASK_ID", "task id must be a valid UUID")
		return
	}

	task, err := h.tasks.GetTask(r.Context(), id)
	if err != nil {
		h.writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toTaskResponse(task))
}

func (h *Handler) handleListTaskAttempts(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeErrorJSON(w, http.StatusBadRequest, "INVALID_TASK_ID", "task id must be a valid UUID")
		return
	}

	attempts, err := h.tasks.ListDeliveryAttempts(r.Context(), id)
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
		writeErrorJSON(w, http.StatusBadRequest, "INVALID_TASK_ID", "task id must be a valid UUID")
		return
	}

	task, err := h.cancelTask(r.Context(), id)
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

// createTask orchestrates idempotent Task + Outbox creation.
//
// Contract (Milestone 1 - core logic to be implemented):
//  1. Load Endpoint via h.endpoints.GetEndpoint(ctx, input.EndpointID).
//  2. Construct domain Task & OutboxItem via domain.NewTask(domain.NewTaskParams{
//     Endpoint:      ep,
//     IdempotencyKey: input.IdempotencyKey,
//     RawPayload:    input.RawPayload,
//     Headers:       input.Headers,
//     DeliverAt:     input.DeliverAt,
//     MaxAttempts:   input.MaxAttempts,
//     DeadlineAt:    input.DeadlineAt,
//     DefaultWindow: h.cfg.DefaultTaskWindow,
//     Now:           time.Now().UTC(),
//     }).
//  3. Persist atomically via h.tasks.CreateTaskIdempotent(ctx, task, outbox) and return the result.
func (h *Handler) createTask(ctx context.Context, input createTaskInput) (domain.CreateTaskResult, error) {
	_ = ctx
	_ = input
	return domain.CreateTaskResult{}, domain.ErrNotImplemented
}

// cancelTask acquires the Execution Lock before cancelling an active Task in PostgreSQL.
//
// Contract (Milestone 1 - core logic to be implemented, ADR-0008 §3):
//  1. Acquire lock via token, acquired, err := h.locker.AcquireExecutionLock(ctx, taskID, h.cfg.CancelLockTTL).
//  2. If !acquired: return domain.Task{}, domain.ErrTaskExecutionInFlight (409 Conflict).
//  3. Defer h.locker.ReleaseExecutionLock(context.WithoutCancel(ctx), taskID, token).
//  4. Execute h.tasks.CancelTask(ctx, taskID, time.Now().UTC()) and return the cancelled Task.
func (h *Handler) cancelTask(ctx context.Context, taskID uuid.UUID) (domain.Task, error) {
	if h.locker == nil {
		return domain.Task{}, domain.ErrNotImplemented
	}
	_ = ctx
	_ = taskID
	return domain.Task{}, domain.ErrNotImplemented
}

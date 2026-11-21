package api

import (
	"encoding/json"
	"errors"
	"net/http"

	"go.uber.org/zap"

	"github.com/awu123-create/ferry/internal/domain"
)

const (
	errCodeNotReady                  = "NOT_READY"
	errCodeInvalidJSON               = "INVALID_JSON"
	errCodeInvalidEndpointID         = "INVALID_ENDPOINT_ID"
	errCodeInvalidTaskID             = "INVALID_TASK_ID"
	errCodeValidation                = "VALIDATION_ERROR"
	errCodeFutureDeliveryUnsupported = "FUTURE_DELIVERY_UNSUPPORTED"
	errCodeEndpointDisabled          = "ENDPOINT_DISABLED"
	errCodeEndpointNotFound          = "ENDPOINT_NOT_FOUND"
	errCodeEndpointConflict          = "ENDPOINT_CONFLICT"
	errCodeTaskNotFound              = "TASK_NOT_FOUND"
	errCodeIdempotencyConflict       = "IDEMPOTENCY_CONFLICT"
	errCodeTaskNotCancellable        = "TASK_NOT_CANCELLABLE"
	errCodeTaskExecutionInFlight     = "TASK_EXECUTION_IN_FLIGHT"
	errCodeInternal                  = "INTERNAL_ERROR"

	errMsgNotReady = "dependency readiness check failed"
	errMsgInternal = "internal server error"
)

type errorEnvelope struct {
	Error errorDetail `json:"error"`
}

type errorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeErrorJSON(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, errorEnvelope{
		Error: errorDetail{
			Code:    code,
			Message: message,
		},
	})
}

func (h *Handler) writeDomainError(w http.ResponseWriter, err error) {
	var valErr domain.ValidationError
	switch {
	case errors.As(err, &valErr):
		writeErrorJSON(w, http.StatusBadRequest, errCodeValidation, valErr.Error())
	case errors.Is(err, domain.ErrValidation):
		h.writeSentinelError(w, http.StatusBadRequest, errCodeValidation, domain.ErrValidation, err)
	case errors.Is(err, domain.ErrFutureDeliveryUnsupported):
		h.writeSentinelError(w, http.StatusBadRequest, errCodeFutureDeliveryUnsupported, domain.ErrFutureDeliveryUnsupported, err)
	case errors.Is(err, domain.ErrEndpointDisabled):
		h.writeSentinelError(w, http.StatusBadRequest, errCodeEndpointDisabled, domain.ErrEndpointDisabled, err)
	case errors.Is(err, domain.ErrEndpointNotFound):
		h.writeSentinelError(w, http.StatusNotFound, errCodeEndpointNotFound, domain.ErrEndpointNotFound, err)
	case errors.Is(err, domain.ErrEndpointConflict):
		h.writeSentinelError(w, http.StatusConflict, errCodeEndpointConflict, domain.ErrEndpointConflict, err)
	case errors.Is(err, domain.ErrTaskNotFound):
		h.writeSentinelError(w, http.StatusNotFound, errCodeTaskNotFound, domain.ErrTaskNotFound, err)
	case errors.Is(err, domain.ErrIdempotencyConflict):
		h.writeSentinelError(w, http.StatusConflict, errCodeIdempotencyConflict, domain.ErrIdempotencyConflict, err)
	case errors.Is(err, domain.ErrTaskNotCancellable):
		h.writeSentinelError(w, http.StatusConflict, errCodeTaskNotCancellable, domain.ErrTaskNotCancellable, err)
	case errors.Is(err, domain.ErrTaskExecutionInFlight):
		h.writeSentinelError(w, http.StatusConflict, errCodeTaskExecutionInFlight, domain.ErrTaskExecutionInFlight, err)
	default:
		h.log.Error("unhandled api error", zap.Error(err))
		writeErrorJSON(w, http.StatusInternalServerError, errCodeInternal, errMsgInternal)
	}
}

func (h *Handler) writeSentinelError(w http.ResponseWriter, status int, code string, sentinel, err error) {
	if err.Error() != sentinel.Error() {
		h.log.Warn("domain error with wrapped context", zap.String("code", code), zap.Error(err))
	}
	writeErrorJSON(w, status, code, sentinel.Error())
}

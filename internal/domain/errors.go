package domain

import (
	"errors"
	"fmt"
)

var (
	// ErrNotImplemented marks core Milestone 1 methods intentionally left for the developer to implement.
	ErrNotImplemented = errors.New("not implemented")
	// ErrValidation indicates domain input or invariant validation failed.
	ErrValidation = errors.New("validation failed")
	// ErrEndpointNotFound indicates the requested Endpoint does not exist.
	ErrEndpointNotFound = errors.New("endpoint not found")
	// ErrEndpointDisabled indicates the target Endpoint is currently disabled.
	ErrEndpointDisabled = errors.New("endpoint is disabled")
	// ErrTaskNotFound indicates the requested Task does not exist.
	ErrTaskNotFound = errors.New("task not found")
	// ErrIdempotencyConflict indicates an Idempotency Key was reused with a different payload.
	ErrIdempotencyConflict = errors.New("idempotency key conflict")
	// ErrTaskNotCancellable indicates the Task is already in a terminal state and cannot be cancelled.
	ErrTaskNotCancellable = errors.New("task cannot be cancelled")
	// ErrTaskExecutionInFlight indicates another worker holds the Execution Lock for the Task.
	ErrTaskExecutionInFlight = errors.New("task execution is in flight")
	// ErrFutureDeliveryUnsupported indicates Milestone 1 rejects future deliver_at timestamps.
	ErrFutureDeliveryUnsupported = errors.New("future deliver_at is not supported in milestone 1")
)

// ValidationError carries field-level validation failure context wrapping ErrValidation.
type ValidationError struct {
	Field   string
	Message string
}

// Error implements the error interface.
func (e ValidationError) Error() string {
	if e.Field == "" {
		return fmt.Sprintf("%s: %s", ErrValidation.Error(), e.Message)
	}
	return fmt.Sprintf("%s: %s %s", ErrValidation.Error(), e.Field, e.Message)
}

// Unwrap allows errors.Is(err, domain.ErrValidation).
func (e ValidationError) Unwrap() error {
	return ErrValidation
}

// NewValidationError builds a field-scoped validation error.
func NewValidationError(field, message string) error {
	return ValidationError{Field: field, Message: message}
}

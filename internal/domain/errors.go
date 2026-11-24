package domain

import (
	"errors"
	"fmt"
)

var (
	// ErrValidation 表示领域输入或业务不变量校验失败。
	ErrValidation = errors.New("validation failed")
	// ErrEndpointNotFound 表示请求的 Endpoint 不存在。
	ErrEndpointNotFound = errors.New("endpoint not found")
	// ErrEndpointDisabled 表示目标 Endpoint 当前处于禁用状态。
	ErrEndpointDisabled = errors.New("endpoint is disabled")
	// ErrEndpointConflict 表示 Endpoint 在更新前已被其他并发请求修改。
	ErrEndpointConflict = errors.New("endpoint was modified concurrently")
	// ErrTaskNotFound 表示请求的 Task 不存在。
	ErrTaskNotFound = errors.New("task not found")
	// ErrIdempotencyConflict 表示同一个 Idempotency Key 被不同载荷重复使用。
	ErrIdempotencyConflict = errors.New("idempotency key conflict")
	// ErrTaskNotCancellable 表示 Task 已处于终态，不允许再取消。
	ErrTaskNotCancellable = errors.New("task cannot be cancelled")
	// ErrTaskExecutionInFlight 表示另一个 Worker 正在持有该 Task 的 Execution Lock（执行互斥锁）。
	ErrTaskExecutionInFlight = errors.New("task execution is in flight")
)

// ValidationError 携带字段级校验失败上下文，并包装 ErrValidation。
type ValidationError struct {
	Field   string
	Message string
}

// Error 实现标准 error 接口。
func (e ValidationError) Error() string {
	if e.Field == "" {
		return fmt.Sprintf("%s: %s", ErrValidation.Error(), e.Message)
	}
	return fmt.Sprintf("%s: %s %s", ErrValidation.Error(), e.Field, e.Message)
}

// Unwrap 使调用方可通过 errors.Is(err, domain.ErrValidation) 匹配该错误。
func (e ValidationError) Unwrap() error {
	return ErrValidation
}

// NewValidationError 构造带字段名的校验错误。
func NewValidationError(field, message string) error {
	return ValidationError{Field: field, Message: message}
}

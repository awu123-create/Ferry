package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"go.uber.org/zap"

	"github.com/awu123-create/ferry/internal/domain"
)

const (
	defaultMaxPayloadBytes = 1 << 20
	defaultCancelLockTTL   = 60 * time.Second
	readinessTimeout       = 2 * time.Second
)

// HandlerConfig 配置 HTTP API 的入站校验上限与默认任务参数。
type HandlerConfig struct {
	DefaultMaxAttempts int
	DefaultTaskWindow  time.Duration
	MaxPayloadBytes    int
	CancelLockTTL      time.Duration
}

// Handler 通过标准库 net/http 暴露 Ferry 控制面 REST 接口与健康检查端点。
type Handler struct {
	log       *zap.Logger
	endpoints EndpointStore
	tasks     TaskStore
	locker    ExecutionLocker
	probes    []ReadinessProbe
	cfg       HandlerConfig
	mux       *http.ServeMux
}

// NewHandler 构造 HTTP 控制面处理器并注册全部 Milestone 1 路由。
func NewHandler(
	log *zap.Logger,
	endpoints EndpointStore,
	tasks TaskStore,
	locker ExecutionLocker,
	probes []ReadinessProbe,
	cfg HandlerConfig,
) (*Handler, error) {
	if endpoints == nil {
		return nil, fmt.Errorf("api handler requires an EndpointStore")
	}
	if tasks == nil {
		return nil, fmt.Errorf("api handler requires a TaskStore")
	}
	if locker == nil {
		return nil, fmt.Errorf("api handler requires an ExecutionLocker")
	}
	if log == nil {
		log = zap.NewNop()
	}
	if cfg.DefaultMaxAttempts <= 0 {
		cfg.DefaultMaxAttempts = 1
	}
	if cfg.DefaultTaskWindow <= 0 {
		cfg.DefaultTaskWindow = 24 * time.Hour
	}
	if cfg.MaxPayloadBytes <= 0 {
		cfg.MaxPayloadBytes = defaultMaxPayloadBytes
	}
	if cfg.CancelLockTTL <= 0 {
		cfg.CancelLockTTL = defaultCancelLockTTL
	}

	h := &Handler{
		log:       log,
		endpoints: endpoints,
		tasks:     tasks,
		locker:    locker,
		probes:    probes,
		cfg:       cfg,
		mux:       http.NewServeMux(),
	}
	h.registerRoutes()
	return h, nil
}

// ServeHTTP 将入站 HTTP 请求分发给已配置的 ServeMux。
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mux.ServeHTTP(w, r)
}

func (h *Handler) registerRoutes() {
	h.mux.HandleFunc("GET /healthz", h.handleHealthz)
	h.mux.HandleFunc("GET /readyz", h.handleReadyz)

	h.mux.HandleFunc("POST /v1/endpoints", h.handleCreateEndpoint)
	h.mux.HandleFunc("GET /v1/endpoints", h.handleListEndpoints)
	h.mux.HandleFunc("GET /v1/endpoints/{id}", h.handleGetEndpoint)
	h.mux.HandleFunc("PATCH /v1/endpoints/{id}", h.handleUpdateEndpoint)

	h.mux.HandleFunc("POST /v1/tasks", h.handleCreateTask)
	h.mux.HandleFunc("GET /v1/tasks/{id}", h.handleGetTask)
	h.mux.HandleFunc("GET /v1/tasks/{id}/attempts", h.handleListTaskAttempts)
	h.mux.HandleFunc("POST /v1/tasks/{id}/cancel", h.handleCancelTask)
}

func (h *Handler) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) handleReadyz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), readinessTimeout)
	defer cancel()

	for _, probe := range h.probes {
		if probe == nil {
			continue
		}
		if err := probe.Ping(ctx); err != nil {
			h.log.Warn("readiness probe failed", zap.Error(err))
			writeErrorJSON(w, http.StatusServiceUnavailable, "NOT_READY", "dependency readiness check failed")
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

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
	switch {
	case errors.Is(err, domain.ErrNotImplemented):
		writeErrorJSON(w, http.StatusNotImplemented, "NOT_IMPLEMENTED", err.Error())
	case errors.Is(err, domain.ErrValidation):
		writeErrorJSON(w, http.StatusBadRequest, "VALIDATION_ERROR", err.Error())
	case errors.Is(err, domain.ErrFutureDeliveryUnsupported):
		writeErrorJSON(w, http.StatusBadRequest, "FUTURE_DELIVERY_UNSUPPORTED", err.Error())
	case errors.Is(err, domain.ErrEndpointDisabled):
		writeErrorJSON(w, http.StatusBadRequest, "ENDPOINT_DISABLED", err.Error())
	case errors.Is(err, domain.ErrEndpointNotFound):
		writeErrorJSON(w, http.StatusNotFound, "ENDPOINT_NOT_FOUND", err.Error())
	case errors.Is(err, domain.ErrTaskNotFound):
		writeErrorJSON(w, http.StatusNotFound, "TASK_NOT_FOUND", err.Error())
	case errors.Is(err, domain.ErrIdempotencyConflict):
		writeErrorJSON(w, http.StatusConflict, "IDEMPOTENCY_CONFLICT", err.Error())
	case errors.Is(err, domain.ErrTaskNotCancellable):
		writeErrorJSON(w, http.StatusConflict, "TASK_NOT_CANCELLABLE", err.Error())
	case errors.Is(err, domain.ErrTaskExecutionInFlight):
		writeErrorJSON(w, http.StatusConflict, "TASK_EXECUTION_IN_FLIGHT", err.Error())
	default:
		h.log.Error("unhandled api error", zap.Error(err))
		writeErrorJSON(w, http.StatusInternalServerError, "INTERNAL_ERROR", "internal server error")
	}
}

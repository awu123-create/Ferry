package api

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"go.uber.org/zap"

	"github.com/awu123-create/ferry/internal/domain"
)

const (
	defaultMaxPayloadBytes = 1 << 20
	defaultCancelLockTTL   = 10 * time.Second
	readinessTimeout       = 2 * time.Second
)

// HandlerConfig 配置 HTTP API 的入站校验上限、默认任务参数与时钟来源。
type HandlerConfig struct {
	DefaultMaxAttempts int
	DefaultTaskWindow  time.Duration
	MaxPayloadBytes    int
	CancelLockTTL      time.Duration
	Clock              func() time.Time
}

// Handler 通过标准库 net/http 暴露 Ferry 控制面 REST 接口与健康检查端点。
type Handler struct {
	log       *zap.Logger
	endpoints EndpointStore
	tasks     TaskStore
	locker    ExecutionLocker
	delayed   DelayedCanceller
	probes    []ReadinessProbe
	cfg       HandlerConfig
	mux       *http.ServeMux
}

// NewHandler 构造 HTTP 控制面处理器并注册全部路由。
func NewHandler(
	log *zap.Logger,
	endpoints EndpointStore,
	tasks TaskStore,
	locker ExecutionLocker,
	delayed DelayedCanceller,
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
	if delayed == nil {
		return nil, fmt.Errorf("api handler requires a DelayedCanceller")
	}
	if log == nil {
		log = zap.NewNop()
	}
	if cfg.DefaultMaxAttempts <= 0 {
		cfg.DefaultMaxAttempts = domain.MinTaskMaxAttempts
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
	if cfg.Clock == nil {
		cfg.Clock = time.Now
	}

	h := &Handler{
		log:       log,
		endpoints: endpoints,
		tasks:     tasks,
		locker:    locker,
		delayed:   delayed,
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

func (h *Handler) now() time.Time {
	return h.cfg.Clock().UTC()
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
			writeErrorJSON(w, http.StatusServiceUnavailable, errCodeNotReady, errMsgNotReady)
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

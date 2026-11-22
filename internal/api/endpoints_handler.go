package api

import (
	"encoding/json/v2"
	"net/http"
	"time"

	"github.com/google/uuid"

	"github.com/awu123-create/ferry/internal/domain"
)

type createEndpointRequest struct {
	Name             string   `json:"name"`
	TargetURL        string   `json:"target_url"`
	SigningSecret    string   `json:"signing_secret,omitempty"`
	RateLimitQPS     *float64 `json:"rate_limit_qps,omitempty"`
	MaxConcurrency   *int     `json:"max_concurrency,omitempty"`
	CircuitThreshold *int     `json:"circuit_threshold,omitempty"`
	TimeoutMs        *int     `json:"timeout_ms,omitempty"`
	Enabled          *bool    `json:"enabled,omitempty"`
}

type updateEndpointRequest struct {
	Name             *string  `json:"name,omitempty"`
	TargetURL        *string  `json:"target_url,omitempty"`
	SigningSecret    *string  `json:"signing_secret,omitempty"`
	RateLimitQPS     *float64 `json:"rate_limit_qps,omitempty"`
	MaxConcurrency   *int     `json:"max_concurrency,omitempty"`
	CircuitThreshold *int     `json:"circuit_threshold,omitempty"`
	TimeoutMs        *int     `json:"timeout_ms,omitempty"`
	Enabled          *bool    `json:"enabled,omitempty"`
}

type endpointResponse struct {
	ID               uuid.UUID `json:"id"`
	Name             string    `json:"name"`
	TargetURL        string    `json:"target_url"`
	HasSigningSecret bool      `json:"has_signing_secret"`
	RateLimitQPS     float64   `json:"rate_limit_qps"`
	MaxConcurrency   int       `json:"max_concurrency"`
	CircuitThreshold int       `json:"circuit_threshold"`
	TimeoutMs        int       `json:"timeout_ms"`
	Enabled          bool      `json:"enabled"`
	CreatedAt        time.Time `json:"created_at"`
	UpdatedAt        time.Time `json:"updated_at"`
}

func toEndpointResponse(ep domain.Endpoint) endpointResponse {
	return endpointResponse{
		ID:               ep.ID,
		Name:             ep.Name,
		TargetURL:        ep.TargetURL,
		HasSigningSecret: ep.SigningSecret != "",
		RateLimitQPS:     ep.RateLimitQPS,
		MaxConcurrency:   ep.MaxConcurrency,
		CircuitThreshold: ep.CircuitThreshold,
		TimeoutMs:        ep.TimeoutMs,
		Enabled:          ep.Enabled,
		CreatedAt:        ep.CreatedAt,
		UpdatedAt:        ep.UpdatedAt,
	}
}

func (h *Handler) handleCreateEndpoint(w http.ResponseWriter, r *http.Request) {
	var req createEndpointRequest
	if err := decodeJSONBody(w, r, 64<<10, &req); err != nil {
		writeErrorJSON(w, http.StatusBadRequest, errCodeInvalidJSON, err.Error())
		return
	}

	ep, err := domain.NewEndpoint(domain.NewEndpointParams{
		Name:             req.Name,
		TargetURL:        req.TargetURL,
		SigningSecret:    req.SigningSecret,
		RateLimitQPS:     req.RateLimitQPS,
		MaxConcurrency:   req.MaxConcurrency,
		CircuitThreshold: req.CircuitThreshold,
		TimeoutMs:        req.TimeoutMs,
		Enabled:          req.Enabled,
	}, h.now())
	if err != nil {
		h.writeDomainError(w, err)
		return
	}

	if err := h.endpoints.CreateEndpoint(r.Context(), ep); err != nil {
		h.writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, toEndpointResponse(ep))
}

func (h *Handler) handleListEndpoints(w http.ResponseWriter, r *http.Request) {
	items, err := h.endpoints.ListEndpoints(r.Context())
	if err != nil {
		h.writeDomainError(w, err)
		return
	}
	resp := make([]endpointResponse, 0, len(items))
	for _, ep := range items {
		resp = append(resp, toEndpointResponse(ep))
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": resp})
}

func (h *Handler) handleGetEndpoint(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeErrorJSON(w, http.StatusBadRequest, errCodeInvalidEndpointID, "endpoint id must be a valid UUID")
		return
	}

	ep, err := h.endpoints.GetEndpoint(r.Context(), id)
	if err != nil {
		h.writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toEndpointResponse(ep))
}

func (h *Handler) handleUpdateEndpoint(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeErrorJSON(w, http.StatusBadRequest, errCodeInvalidEndpointID, "endpoint id must be a valid UUID")
		return
	}

	var req updateEndpointRequest
	if err := decodeJSONBody(w, r, 64<<10, &req); err != nil {
		writeErrorJSON(w, http.StatusBadRequest, errCodeInvalidJSON, err.Error())
		return
	}

	ep, err := h.endpoints.GetEndpoint(r.Context(), id)
	if err != nil {
		h.writeDomainError(w, err)
		return
	}
	expectedUpdatedAt := ep.UpdatedAt

	if err := ep.ApplyUpdate(domain.UpdateEndpointParams{
		Name:             req.Name,
		TargetURL:        req.TargetURL,
		SigningSecret:    req.SigningSecret,
		RateLimitQPS:     req.RateLimitQPS,
		MaxConcurrency:   req.MaxConcurrency,
		CircuitThreshold: req.CircuitThreshold,
		TimeoutMs:        req.TimeoutMs,
		Enabled:          req.Enabled,
	}, h.now()); err != nil {
		h.writeDomainError(w, err)
		return
	}

	if err := h.endpoints.UpdateEndpoint(r.Context(), ep, expectedUpdatedAt); err != nil {
		h.writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toEndpointResponse(ep))
}

func decodeJSONBody(w http.ResponseWriter, r *http.Request, maxBytes int64, dst any) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxBytes)
	return json.UnmarshalRead(r.Body, dst)
}

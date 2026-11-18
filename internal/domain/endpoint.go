package domain

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	defaultRateLimitQPS     = 50.0
	defaultMaxConcurrency   = 20
	defaultCircuitThreshold = 30
	defaultTimeoutMs        = 5000
	minTimeoutMs            = 100
	maxTimeoutMs            = 60000
	signaturePrefix         = "sha256="
)

// Endpoint is the aggregate root owning an HTTP delivery address and governance contract.
type Endpoint struct {
	ID               uuid.UUID
	Name             string
	TargetURL        string
	SigningSecret    string
	RateLimitQPS     float64
	MaxConcurrency   int
	CircuitThreshold int
	TimeoutMs        int
	Enabled          bool
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

// NewEndpointParams carries validated inputs or defaults for constructing an Endpoint.
type NewEndpointParams struct {
	Name             string
	TargetURL        string
	SigningSecret    string
	RateLimitQPS     *float64
	MaxConcurrency   *int
	CircuitThreshold *int
	TimeoutMs        *int
	Enabled          *bool
}

// UpdateEndpointParams carries optional field mutations for an existing Endpoint.
type UpdateEndpointParams struct {
	Name             *string
	TargetURL        *string
	SigningSecret    *string
	RateLimitQPS     *float64
	MaxConcurrency   *int
	CircuitThreshold *int
	TimeoutMs        *int
	Enabled          *bool
}

// NewEndpoint creates an Endpoint aggregate while enforcing all domain invariants.
func NewEndpoint(params NewEndpointParams, now time.Time) (Endpoint, error) {
	name := strings.TrimSpace(params.Name)
	if name == "" {
		return Endpoint{}, NewValidationError("name", "must not be empty")
	}
	targetURL := strings.TrimSpace(params.TargetURL)
	if err := validateTargetURL(targetURL); err != nil {
		return Endpoint{}, err
	}

	rateLimitQPS := defaultRateLimitQPS
	if params.RateLimitQPS != nil {
		rateLimitQPS = *params.RateLimitQPS
	}
	if err := validateRateLimitQPS(rateLimitQPS); err != nil {
		return Endpoint{}, err
	}

	maxConcurrency := defaultMaxConcurrency
	if params.MaxConcurrency != nil {
		maxConcurrency = *params.MaxConcurrency
	}
	if maxConcurrency <= 0 {
		return Endpoint{}, NewValidationError("max_concurrency", "must be > 0")
	}

	circuitThreshold := defaultCircuitThreshold
	if params.CircuitThreshold != nil {
		circuitThreshold = *params.CircuitThreshold
	}
	if circuitThreshold <= 0 {
		return Endpoint{}, NewValidationError("circuit_threshold", "must be > 0")
	}

	timeoutMs := defaultTimeoutMs
	if params.TimeoutMs != nil {
		timeoutMs = *params.TimeoutMs
	}
	if err := validateTimeoutMs(timeoutMs); err != nil {
		return Endpoint{}, err
	}

	enabled := true
	if params.Enabled != nil {
		enabled = *params.Enabled
	}

	id, err := uuid.NewV7()
	if err != nil {
		return Endpoint{}, fmt.Errorf("generate endpoint uuid: %w", err)
	}

	ts := now.UTC()
	return Endpoint{
		ID:               id,
		Name:             name,
		TargetURL:        targetURL,
		SigningSecret:    params.SigningSecret,
		RateLimitQPS:     rateLimitQPS,
		MaxConcurrency:   maxConcurrency,
		CircuitThreshold: circuitThreshold,
		TimeoutMs:        timeoutMs,
		Enabled:          enabled,
		CreatedAt:        ts,
		UpdatedAt:        ts,
	}, nil
}

// ApplyUpdate validates and atomically applies partial updates onto the Endpoint.
func (e *Endpoint) ApplyUpdate(params UpdateEndpointParams, now time.Time) error {
	next := *e

	if params.Name != nil {
		trimmed := strings.TrimSpace(*params.Name)
		if trimmed == "" {
			return NewValidationError("name", "must not be empty")
		}
		next.Name = trimmed
	}
	if params.TargetURL != nil {
		trimmed := strings.TrimSpace(*params.TargetURL)
		if err := validateTargetURL(trimmed); err != nil {
			return err
		}
		next.TargetURL = trimmed
	}
	if params.SigningSecret != nil {
		next.SigningSecret = *params.SigningSecret
	}
	if params.RateLimitQPS != nil {
		if err := validateRateLimitQPS(*params.RateLimitQPS); err != nil {
			return err
		}
		next.RateLimitQPS = *params.RateLimitQPS
	}
	if params.MaxConcurrency != nil {
		if *params.MaxConcurrency <= 0 {
			return NewValidationError("max_concurrency", "must be > 0")
		}
		next.MaxConcurrency = *params.MaxConcurrency
	}
	if params.CircuitThreshold != nil {
		if *params.CircuitThreshold <= 0 {
			return NewValidationError("circuit_threshold", "must be > 0")
		}
		next.CircuitThreshold = *params.CircuitThreshold
	}
	if params.TimeoutMs != nil {
		if err := validateTimeoutMs(*params.TimeoutMs); err != nil {
			return err
		}
		next.TimeoutMs = *params.TimeoutMs
	}
	if params.Enabled != nil {
		next.Enabled = *params.Enabled
	}

	next.UpdatedAt = now.UTC()
	*e = next
	return nil
}

// SignPayload generates the Payload Signature header ("sha256=<hex>") over "<ts>.<rawPayload>".
// It returns an empty string when no signing secret is configured.
func (e Endpoint) SignPayload(timestampUnix int64, rawPayload []byte) string {
	return signWithSecret(e.SigningSecret, timestampUnix, rawPayload)
}

// VerifyPayloadSignature verifies an incoming X-Ferry-Signature header in constant time.
func VerifyPayloadSignature(secret, signatureHeader string, timestampUnix int64, rawPayload []byte) bool {
	if secret == "" {
		return false
	}
	hexPart, ok := strings.CutPrefix(strings.TrimSpace(signatureHeader), signaturePrefix)
	if !ok {
		return false
	}
	providedMAC, err := hex.DecodeString(hexPart)
	if err != nil || len(providedMAC) != sha256.Size {
		return false
	}
	expected := computeHMAC(secret, timestampUnix, rawPayload)
	return hmac.Equal(providedMAC, expected)
}

func signWithSecret(secret string, timestampUnix int64, rawPayload []byte) string {
	if secret == "" {
		return ""
	}
	mac := computeHMAC(secret, timestampUnix, rawPayload)
	return signaturePrefix + hex.EncodeToString(mac)
}

func computeHMAC(secret string, timestampUnix int64, rawPayload []byte) []byte {
	mac := hmac.New(sha256.New, []byte(secret))
	var buf [32]byte
	tsBytes := strconv.AppendInt(buf[:0], timestampUnix, 10)
	_, _ = mac.Write(tsBytes)
	_, _ = mac.Write([]byte{'.'})
	_, _ = mac.Write(rawPayload)
	return mac.Sum(nil)
}

func validateTargetURL(raw string) error {
	if raw == "" {
		return NewValidationError("target_url", "must not be empty")
	}
	parsed, err := url.ParseRequestURI(raw)
	if err != nil {
		return NewValidationError("target_url", "must be a valid HTTP or HTTPS URL")
	}
	if (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return NewValidationError("target_url", "must use http or https scheme with host")
	}
	return nil
}

func validateRateLimitQPS(qps float64) error {
	if math.IsNaN(qps) || math.IsInf(qps, 0) || qps <= 0 {
		return NewValidationError("rate_limit_qps", "must be > 0")
	}
	return nil
}

func validateTimeoutMs(timeoutMs int) error {
	if timeoutMs < minTimeoutMs || timeoutMs > maxTimeoutMs {
		return NewValidationError("timeout_ms", "must be between 100 and 60000")
	}
	return nil
}

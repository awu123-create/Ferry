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
	// MinEndpointTimeoutMs 与 MaxEndpointTimeoutMs 定义 Endpoint.TimeoutMs 的合法闭区间（与数据库 CHECK 约束一致）。
	MinEndpointTimeoutMs = 100
	MaxEndpointTimeoutMs = 60000

	defaultRateLimitQPS     = 50.0
	defaultMaxConcurrency   = 20
	defaultCircuitThreshold = 30
	defaultTimeoutMs        = 5000
	signaturePrefix         = "sha256="
)

// Endpoint 是托管下游 HTTP 接收地址及其投递治理契约的聚合根。
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

// NewEndpointParams 承载构造 Endpoint 所需的校验后参数或默认值。
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

// UpdateEndpointParams 承载对现有 Endpoint 的可选字段局部更新参数。
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

// NewEndpoint 创建 Endpoint 聚合根并校验全部领域不变量。
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

// ApplyUpdate 先校验所有待修改字段，再原子性地应用到 Endpoint 上。
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

	ts := now.UTC()
	if !ts.After(e.UpdatedAt) {
		ts = e.UpdatedAt.Add(time.Microsecond)
	}
	next.UpdatedAt = ts
	*e = next
	return nil
}

// SignPayload 基于 "<ts>.<rawPayload>" 生成载荷签名头（"sha256=<hex>"）。
// 未配置签名密钥时返回空字符串。
func (e Endpoint) SignPayload(timestampUnix int64, rawPayload []byte) string {
	return signWithSecret(e.SigningSecret, timestampUnix, rawPayload)
}

// VerifyPayloadSignature 以常量时间比对校验传入的 X-Ferry-Signature 请求头是否合法。
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
	if timeoutMs < MinEndpointTimeoutMs || timeoutMs > MaxEndpointTimeoutMs {
		return NewValidationError("timeout_ms", fmt.Sprintf("must be between %d and %d", MinEndpointTimeoutMs, MaxEndpointTimeoutMs))
	}
	return nil
}

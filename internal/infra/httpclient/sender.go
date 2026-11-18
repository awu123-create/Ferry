package httpclient

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/awu123-create/ferry/internal/domain"
)

const (
	defaultMaxSnippetBytes = 1024
	maxDrainBytes          = 64 << 10
	maxRetryAfter          = 3600 * time.Second
	maxErrorMsgBytes       = 512
)

// Sender delivers HTTP POST requests with tuned pooling, raw-byte HMAC signing, and bounded responses.
type Sender struct {
	client          *http.Client
	maxSnippetBytes int
}

// NewSender constructs an outbound HTTP delivery client with tuned connection pooling.
func NewSender(maxSnippetBytes int) *Sender {
	if maxSnippetBytes <= 0 {
		maxSnippetBytes = defaultMaxSnippetBytes
	}
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          1000,
		MaxIdleConnsPerHost:   64,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
	return &Sender{
		client:          &http.Client{Transport: transport},
		maxSnippetBytes: maxSnippetBytes,
	}
}

// Send executes a single outbound HTTP POST request using req.RawPayload verbatim.
func (s *Sender) Send(ctx context.Context, req domain.DeliveryRequest) domain.DeliveryResult {
	callCtx := ctx
	var cancel context.CancelFunc
	if req.Timeout > 0 {
		callCtx, cancel = context.WithTimeout(ctx, req.Timeout)
		defer cancel()
	}

	started := time.Now()
	httpReq, err := http.NewRequestWithContext(callCtx, http.MethodPost, req.EndpointURL, bytes.NewReader(req.RawPayload))
	if err != nil {
		return domain.DeliveryResult{
			DurationMs:   int(time.Since(started).Milliseconds()),
			ErrorKind:    domain.AttemptErrorNetwork,
			ErrorMessage: truncateText(fmt.Sprintf("build request: %v", err), maxErrorMsgBytes),
		}
	}

	applyDeliveryHeaders(httpReq, req)

	resp, err := s.client.Do(httpReq)
	durationMs := int(time.Since(started).Milliseconds())
	if err != nil {
		return domain.DeliveryResult{
			DurationMs:   durationMs,
			ErrorKind:    classifyTransportError(err),
			ErrorMessage: truncateText(err.Error(), maxErrorMsgBytes),
		}
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	snippet, readErr := readBoundedSnippet(resp.Body, s.maxSnippetBytes)
	retryAfter := parseRetryAfter(resp.Header.Get("Retry-After"), time.Now())

	if readErr != nil {
		return domain.DeliveryResult{
			HTTPStatus:      resp.StatusCode,
			DurationMs:      durationMs,
			ErrorKind:       classifyTransportError(readErr),
			ErrorMessage:    truncateText(readErr.Error(), maxErrorMsgBytes),
			ResponseSnippet: snippet,
			RetryAfter:      retryAfter,
		}
	}

	kind, errMsg := classifyHTTPStatus(resp.StatusCode)
	return domain.DeliveryResult{
		HTTPStatus:      resp.StatusCode,
		DurationMs:      durationMs,
		ErrorKind:       kind,
		ErrorMessage:    errMsg,
		ResponseSnippet: snippet,
		RetryAfter:      retryAfter,
	}
}

func applyDeliveryHeaders(httpReq *http.Request, req domain.DeliveryRequest) {
	for k, v := range req.Headers {
		if strings.EqualFold(k, "Host") || strings.HasPrefix(strings.ToLower(k), "x-ferry-") {
			continue
		}
		httpReq.Header.Set(k, v)
	}
	if httpReq.Header.Get("Content-Type") == "" {
		httpReq.Header.Set("Content-Type", "application/json")
	}

	httpReq.Header.Set("X-Ferry-Task-ID", req.TaskID.String())
	httpReq.Header.Set("X-Ferry-Attempt", strconv.Itoa(req.AttemptNo))

	if req.SigningSecret != "" {
		sentAt := req.SentAt
		if sentAt.IsZero() {
			sentAt = time.Now().UTC()
		}
		ts := sentAt.Unix()
		sig := domain.Endpoint{SigningSecret: req.SigningSecret}.SignPayload(ts, req.RawPayload)
		httpReq.Header.Set("X-Ferry-Timestamp", strconv.FormatInt(ts, 10))
		httpReq.Header.Set("X-Ferry-Signature", sig)
	}
}

func readBoundedSnippet(body io.Reader, limit int) (string, error) {
	raw, err := io.ReadAll(io.LimitReader(body, int64(limit)))
	_, _ = io.Copy(io.Discard, io.LimitReader(body, maxDrainBytes))
	if err != nil {
		return string(raw), err
	}
	return string(raw), nil
}

func classifyTransportError(err error) domain.AttemptErrorKind {
	if errors.Is(err, context.DeadlineExceeded) {
		return domain.AttemptErrorTimeout
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return domain.AttemptErrorTimeout
	}
	return domain.AttemptErrorNetwork
}

func classifyHTTPStatus(status int) (domain.AttemptErrorKind, string) {
	switch {
	case status >= 200 && status < 300:
		return domain.AttemptErrorNone, ""
	case status == http.StatusRequestTimeout:
		return domain.AttemptErrorTimeout, http.StatusText(status)
	case status == http.StatusTooManyRequests:
		return domain.AttemptErrorHTTP429, http.StatusText(status)
	case status >= 400 && status < 500:
		return domain.AttemptErrorHTTP4xx, fmt.Sprintf("HTTP %d", status)
	default:
		return domain.AttemptErrorHTTP5xx, fmt.Sprintf("HTTP %d", status)
	}
}

func parseRetryAfter(raw string, now time.Time) time.Duration {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return 0
	}
	if secs, err := strconv.Atoi(trimmed); err == nil {
		if secs <= 0 {
			return 0
		}
		d := time.Duration(secs) * time.Second
		if d > maxRetryAfter {
			return maxRetryAfter
		}
		return d
	}
	if ts, err := http.ParseTime(trimmed); err == nil {
		d := ts.Sub(now)
		if d <= 0 {
			return 0
		}
		if d > maxRetryAfter {
			return maxRetryAfter
		}
		return d
	}
	return 0
}

func truncateText(s string, maxBytes int) string {
	if len(s) <= maxBytes {
		return s
	}
	return s[:maxBytes]
}

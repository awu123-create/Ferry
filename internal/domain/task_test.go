package domain

import (
	"errors"
	"testing"
	"time"
)

func TestNewTaskPastDeliverAtUsesNowForDefaultDeadlineAndValidatesHeaders(t *testing.T) {
	now := time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)
	ep, err := NewEndpoint(NewEndpointParams{
		Name:      "orders",
		TargetURL: "http://localhost:8081/webhook",
	}, now)
	if err != nil {
		t.Fatalf("NewEndpoint failed: %v", err)
	}

	// 传入两天前的 deliver_at：默认 deadline 应以 now + DefaultWindow 算，不致一出生就过期。
	pastDeliverAt := now.Add(-48 * time.Hour)
	task, err := NewTask(NewTaskParams{
		Endpoint:       ep,
		IdempotencyKey: "past-1",
		RawPayload:     []byte(`{"ok":true}`),
		Headers:        map[string]string{"X-Trace-ID": "abc-123"},
		DeliverAt:      &pastDeliverAt,
		MaxAttempts:    1,
		DefaultWindow:  24 * time.Hour,
		Now:            now,
	})
	if err != nil {
		t.Fatalf("NewTask with past deliver_at failed: %v", err)
	}
	wantDeadline := now.Add(24 * time.Hour)
	if !task.DeadlineAt.Equal(wantDeadline) {
		t.Fatalf("expected deadline %v, got %v", wantDeadline, task.DeadlineAt)
	}

	// 非法 header 名与含 CRLF 的 header 值均应返回 ErrValidation。
	badHeaders := []map[string]string{
		{"": "empty-name"},
		{"Bad Header": "space"},
		{"X-Inject": "ok\r\nEvil: 1"},
		{"X-Null": "a\x00b"},
	}
	for _, hdrs := range badHeaders {
		_, err := NewTask(NewTaskParams{
			Endpoint:       ep,
			IdempotencyKey: "bad-hdr",
			RawPayload:     []byte(`{"ok":true}`),
			Headers:        hdrs,
			MaxAttempts:    1,
			DefaultWindow:  time.Hour,
			Now:            now,
		})
		if !errors.Is(err, ErrValidation) {
			t.Fatalf("expected ErrValidation for headers %v, got %v", hdrs, err)
		}
	}
}

func TestVerifyIdempotentReplayChecksHeaders(t *testing.T) {
	now := time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)
	ep, err := NewEndpoint(NewEndpointParams{
		Name:      "orders",
		TargetURL: "http://localhost:8081/webhook",
	}, now)
	if err != nil {
		t.Fatalf("NewEndpoint failed: %v", err)
	}

	task, err := NewTask(NewTaskParams{
		Endpoint:       ep,
		IdempotencyKey: "idem-hdr",
		RawPayload:     []byte(`{"k":1}`),
		Headers:        map[string]string{"X-Env": "prod"},
		MaxAttempts:    1,
		DefaultWindow:  time.Hour,
		Now:            now,
	})
	if err != nil {
		t.Fatalf("NewTask failed: %v", err)
	}

	if err := task.VerifyIdempotentReplay(ep.ID, "idem-hdr", task.PayloadSHA256, map[string]string{"X-Env": "prod"}); err != nil {
		t.Fatalf("expected identical replay to pass, got %v", err)
	}
	if err := task.VerifyIdempotentReplay(ep.ID, "idem-hdr", task.PayloadSHA256, map[string]string{"X-Env": "staging"}); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("expected ErrIdempotencyConflict on header mismatch, got %v", err)
	}

	noHdrTask, err := NewTask(NewTaskParams{
		Endpoint:       ep,
		IdempotencyKey: "idem-empty",
		RawPayload:     []byte(`{"k":1}`),
		MaxAttempts:    1,
		DefaultWindow:  time.Hour,
		Now:            now,
	})
	if err != nil {
		t.Fatalf("NewTask failed: %v", err)
	}
	if err := noHdrTask.VerifyIdempotentReplay(ep.ID, "idem-empty", noHdrTask.PayloadSHA256, nil); err != nil {
		t.Fatalf("expected nil and empty headers to match, got %v", err)
	}
}

func TestEvaluatePreDeliveryClockSkewToleranceAndRecordOutcome3xx(t *testing.T) {
	now := time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)
	ep, err := NewEndpoint(NewEndpointParams{
		Name:      "orders",
		TargetURL: "http://localhost:8081/webhook",
	}, now)
	if err != nil {
		t.Fatalf("NewEndpoint failed: %v", err)
	}
	task, err := NewTask(NewTaskParams{
		Endpoint:       ep,
		IdempotencyKey: "skew-1",
		RawPayload:     []byte(`{"k":1}`),
		MaxAttempts:    3,
		DefaultWindow:  time.Hour,
		Now:            now,
	})
	if err != nil {
		t.Fatalf("NewTask failed: %v", err)
	}

	// Worker 时钟比 API 慢 500ms（在 1s 容差内）：应允许投递而非误判早到。
	withinSkew, err := task.EvaluatePreDelivery(ep, now.Add(-500*time.Millisecond))
	if err != nil || !withinSkew.ShouldDeliver {
		t.Fatalf("expected delivery within clock skew tolerance, got decision=%+v err=%v", withinSkew, err)
	}

	// 明显未到触发时间（提前 5s）：应拦截为 EarlyArrival。
	tooEarly, err := task.EvaluatePreDelivery(ep, now.Add(-5*time.Second))
	if err != nil || !tooEarly.EarlyArrival {
		t.Fatalf("expected EarlyArrival beyond tolerance, got decision=%+v err=%v", tooEarly, err)
	}

	// 3xx 重定向（不跟随）直接转入 DEAD_LETTER 且不触发熔断计数。
	tr, err := task.RecordOutcome(DeliveryResult{
		HTTPStatus:   302,
		DurationMs:   8,
		ErrorKind:    AttemptErrorHTTP3xx,
		ErrorMessage: "HTTP 302 (redirect not followed)",
	}, now)
	if err != nil {
		t.Fatalf("RecordOutcome failed: %v", err)
	}
	if tr.NextStatus != TaskStatusDeadLetter || tr.LastError != DeadLetterReasonUnexpectedRedirect || tr.CircuitRetryable {
		t.Fatalf("unexpected 3xx transition: %+v", tr)
	}
}

func TestNewTaskValidatesJSONPayloadStrictly(t *testing.T) {
	now := time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)
	ep, err := NewEndpoint(NewEndpointParams{
		Name:      "orders",
		TargetURL: "http://localhost:8081/webhook",
	}, now)
	if err != nil {
		t.Fatalf("NewEndpoint failed: %v", err)
	}

	cases := []struct {
		name    string
		payload []byte
	}{
		{name: "empty", payload: nil},
		{name: "broken_syntax", payload: []byte(`{"ok":`)},
		{name: "duplicate_key", payload: []byte(`{"amount":100,"amount":9999}`)},
		{name: "invalid_utf8", payload: []byte("{\"msg\":\"\xff\"}")},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewTask(NewTaskParams{
				Endpoint:       ep,
				IdempotencyKey: "bad-json-" + tc.name,
				RawPayload:     tc.payload,
				MaxAttempts:    1,
				DefaultWindow:  time.Hour,
				Now:            now,
			})
			if !errors.Is(err, ErrValidation) {
				t.Fatalf("expected ErrValidation for %s, got %v", tc.name, err)
			}
		})
	}
}

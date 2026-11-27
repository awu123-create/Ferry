package domain_test

import (
	"errors"
	"testing"
	"time"

	"github.com/awu123-create/ferry/internal/domain"
)

func TestNewTask_PastDeliverAtUsesNowForDefaultDeadlineAndValidatesHeaders(t *testing.T) {
	now := time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)
	ep, err := domain.NewEndpoint(domain.NewEndpointParams{
		Name:      "orders",
		TargetURL: "http://localhost:8081/webhook",
	}, now)
	if err != nil {
		t.Fatalf("NewEndpoint failed: %v", err)
	}

	// 传入两天前的 deliver_at：默认 deadline 应以 now + DefaultWindow 算，不致一出生就过期。
	pastDeliverAt := now.Add(-48 * time.Hour)
	task, err := domain.NewTask(domain.NewTaskParams{
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
		_, err := domain.NewTask(domain.NewTaskParams{
			Endpoint:       ep,
			IdempotencyKey: "bad-hdr",
			RawPayload:     []byte(`{"ok":true}`),
			Headers:        hdrs,
			MaxAttempts:    1,
			DefaultWindow:  time.Hour,
			Now:            now,
		})
		if !errors.Is(err, domain.ErrValidation) {
			t.Fatalf("expected ErrValidation for headers %v, got %v", hdrs, err)
		}
	}
}

func TestTask_VerifyIdempotentReplayChecksHeaders(t *testing.T) {
	now := time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)
	ep, err := domain.NewEndpoint(domain.NewEndpointParams{
		Name:      "orders",
		TargetURL: "http://localhost:8081/webhook",
	}, now)
	if err != nil {
		t.Fatalf("NewEndpoint failed: %v", err)
	}

	task, err := domain.NewTask(domain.NewTaskParams{
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
	if err := task.VerifyIdempotentReplay(ep.ID, "idem-hdr", task.PayloadSHA256, map[string]string{"X-Env": "staging"}); !errors.Is(err, domain.ErrIdempotencyConflict) {
		t.Fatalf("expected ErrIdempotencyConflict on header mismatch, got %v", err)
	}

	noHdrTask, err := domain.NewTask(domain.NewTaskParams{
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

func TestTask_EvaluatePreDeliveryClockSkewToleranceAndRecordOutcome3xx(t *testing.T) {
	now := time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)
	ep, err := domain.NewEndpoint(domain.NewEndpointParams{
		Name:      "orders",
		TargetURL: "http://localhost:8081/webhook",
	}, now)
	if err != nil {
		t.Fatalf("NewEndpoint failed: %v", err)
	}
	task, err := domain.NewTask(domain.NewTaskParams{
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
	tr, err := task.RecordOutcome(domain.DeliveryResult{
		HTTPStatus:   302,
		DurationMs:   8,
		ErrorKind:    domain.AttemptErrorHTTP3xx,
		ErrorMessage: "HTTP 302 (redirect not followed)",
	}, now)
	if err != nil {
		t.Fatalf("RecordOutcome failed: %v", err)
	}
	if tr.NextStatus != domain.TaskStatusDeadLetter || tr.LastError != domain.DeadLetterReasonUnexpectedRedirect || tr.CircuitRetryable {
		t.Fatalf("unexpected 3xx transition: %+v", tr)
	}
}

func TestNewTask_ValidatesJSONPayloadStrictly(t *testing.T) {
	now := time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)
	ep, err := domain.NewEndpoint(domain.NewEndpointParams{
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
			_, err := domain.NewTask(domain.NewTaskParams{
				Endpoint:       ep,
				IdempotencyKey: "bad-json-" + tc.name,
				RawPayload:     tc.payload,
				MaxAttempts:    1,
				DefaultWindow:  time.Hour,
				Now:            now,
			})
			if !errors.Is(err, domain.ErrValidation) {
				t.Fatalf("expected ErrValidation for %s, got %v", tc.name, err)
			}
		})
	}
}

func TestParseAttemptErrorKind_ValidAndUnknown(t *testing.T) {
	valid := []domain.AttemptErrorKind{
		domain.AttemptErrorNone,
		domain.AttemptErrorTimeout,
		domain.AttemptErrorNetwork,
		domain.AttemptErrorHTTP3xx,
		domain.AttemptErrorHTTP4xx,
		domain.AttemptErrorHTTP429,
		domain.AttemptErrorHTTP5xx,
	}
	for _, want := range valid {
		got, err := domain.ParseAttemptErrorKind(string(want))
		if err != nil || got != want {
			t.Fatalf("ParseAttemptErrorKind(%q) = (%q, %v), want (%q, nil)", want, got, err, want)
		}
	}
	if _, err := domain.ParseAttemptErrorKind("UNKNOWN_KIND"); err == nil {
		t.Fatal("expected error for unknown AttemptErrorKind")
	}
}

func TestNewTask_DeliverAtWindowBounds(t *testing.T) {
	now := time.Date(2026, 11, 23, 10, 0, 0, 0, time.UTC)
	ep, err := domain.NewEndpoint(domain.NewEndpointParams{
		Name:      "orders",
		TargetURL: "http://localhost:8081/webhook",
	}, now)
	if err != nil {
		t.Fatalf("NewEndpoint failed: %v", err)
	}

	const maxWindow = 30 * 24 * time.Hour
	validCases := []struct {
		name         string
		deliverAt    time.Time
		wantDeadline time.Time
	}{
		{
			name:         "equal_now_immediate",
			deliverAt:    now,
			wantDeadline: now.Add(24 * time.Hour),
		},
		{
			name:         "future_ten_minutes",
			deliverAt:    now.Add(10 * time.Minute),
			wantDeadline: now.Add(10*time.Minute + 24*time.Hour),
		},
		{
			name:         "exact_thirty_days_boundary",
			deliverAt:    now.Add(maxWindow),
			wantDeadline: now.Add(maxWindow + 24*time.Hour),
		},
	}

	for _, tc := range validCases {
		t.Run(tc.name, func(t *testing.T) {
			deliverAt := tc.deliverAt
			task, err := domain.NewTask(domain.NewTaskParams{
				Endpoint:       ep,
				IdempotencyKey: "deliver-" + tc.name,
				RawPayload:     []byte(`{"ok":true}`),
				DeliverAt:      &deliverAt,
				MaxAttempts:    1,
				DefaultWindow:  24 * time.Hour,
				Now:            now,
			})
			if err != nil {
				t.Fatalf("NewTask(%s) unexpected error: %v", tc.name, err)
			}
			if !task.NextRunAt.Equal(tc.deliverAt) {
				t.Fatalf("expected NextRunAt=%v, got %v", tc.deliverAt, task.NextRunAt)
			}
			if !task.DeadlineAt.Equal(tc.wantDeadline) {
				t.Fatalf("expected DeadlineAt=%v, got %v", tc.wantDeadline, task.DeadlineAt)
			}
		})
	}

	t.Run("beyond_thirty_days_rejected", func(t *testing.T) {
		tooFar := now.Add(maxWindow + time.Millisecond)
		_, err := domain.NewTask(domain.NewTaskParams{
			Endpoint:       ep,
			IdempotencyKey: "deliver-too-far",
			RawPayload:     []byte(`{"ok":true}`),
			DeliverAt:      &tooFar,
			MaxAttempts:    1,
			DefaultWindow:  24 * time.Hour,
			Now:            now,
		})
		var valErr domain.ValidationError
		if !errors.As(err, &valErr) || valErr.Field != "deliver_at" {
			t.Fatalf("expected ValidationError on deliver_at, got %v", err)
		}
	})

	t.Run("explicit_deadline_must_exceed_future_deliver_at", func(t *testing.T) {
		future := now.Add(2 * time.Hour)
		badDeadline := future
		_, err := domain.NewTask(domain.NewTaskParams{
			Endpoint:       ep,
			IdempotencyKey: "deliver-bad-deadline",
			RawPayload:     []byte(`{"ok":true}`),
			DeliverAt:      &future,
			DeadlineAt:     &badDeadline,
			MaxAttempts:    1,
			DefaultWindow:  24 * time.Hour,
			Now:            now,
		})
		if !errors.Is(err, domain.ErrValidation) {
			t.Fatalf("expected ErrValidation when deadline_at <= deliver_at, got %v", err)
		}
	})
}

func TestTask_EvaluatePreDeliveryStrictForScheduledAndRetryingTasks(t *testing.T) {
	now := time.Date(2026, 11, 24, 10, 0, 0, 0, time.UTC)
	ep, err := domain.NewEndpoint(domain.NewEndpointParams{
		Name:      "scheduled-guard",
		TargetURL: "http://localhost:8081/webhook",
	}, now)
	if err != nil {
		t.Fatalf("NewEndpoint failed: %v", err)
	}

	futureDeliverAt := now.Add(10 * time.Second)
	scheduled, err := domain.NewTask(domain.NewTaskParams{
		Endpoint:       ep,
		IdempotencyKey: "sched-strict-1",
		RawPayload:     []byte(`{"k":1}`),
		DeliverAt:      &futureDeliverAt,
		MaxAttempts:    3,
		DefaultWindow:  time.Hour,
		Now:            now,
	})
	if err != nil {
		t.Fatalf("NewTask(scheduled) failed: %v", err)
	}

	immediate, err := domain.NewTask(domain.NewTaskParams{
		Endpoint:       ep,
		IdempotencyKey: "imm-skew-1",
		RawPayload:     []byte(`{"k":1}`),
		MaxAttempts:    3,
		DefaultWindow:  time.Hour,
		Now:            now,
	})
	if err != nil {
		t.Fatalf("NewTask(immediate) failed: %v", err)
	}

	retrying := immediate
	retrying.Status = domain.TaskStatusRetrying
	retrying.Attempt = 1
	retrying.NextRunAt = now.Add(5 * time.Second)

	cases := []struct {
		name              string
		task              domain.Task
		evalNow           time.Time
		wantEarlyArrival  bool
		wantShouldDeliver bool
	}{
		{
			name:             "scheduled_500ms_early_blocked",
			task:             scheduled,
			evalNow:          futureDeliverAt.Add(-500 * time.Millisecond),
			wantEarlyArrival: true,
		},
		{
			name:             "scheduled_1ms_early_blocked",
			task:             scheduled,
			evalNow:          futureDeliverAt.Add(-time.Millisecond),
			wantEarlyArrival: true,
		},
		{
			name:              "scheduled_exact_due_allowed",
			task:              scheduled,
			evalNow:           futureDeliverAt,
			wantShouldDeliver: true,
		},
		{
			name:             "retrying_500ms_early_blocked",
			task:             retrying,
			evalNow:          retrying.NextRunAt.Add(-500 * time.Millisecond),
			wantEarlyArrival: true,
		},
		{
			name:              "immediate_500ms_clock_skew_allowed",
			task:              immediate,
			evalNow:           now.Add(-500 * time.Millisecond),
			wantShouldDeliver: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			decision, err := tc.task.EvaluatePreDelivery(ep, tc.evalNow)
			if err != nil {
				t.Fatalf("EvaluatePreDelivery failed: %v", err)
			}
			if decision.EarlyArrival != tc.wantEarlyArrival || decision.ShouldDeliver != tc.wantShouldDeliver {
				t.Fatalf("got decision=%+v, want EarlyArrival=%v ShouldDeliver=%v", decision, tc.wantEarlyArrival, tc.wantShouldDeliver)
			}
		})
	}
}

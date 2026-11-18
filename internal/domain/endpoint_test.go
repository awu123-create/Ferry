package domain

import (
	"errors"
	"testing"
	"time"
)

func TestNewEndpointDefaultsAndInvariants(t *testing.T) {
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)

	ep, err := NewEndpoint(NewEndpointParams{
		Name:          "billing-callback",
		TargetURL:     "https://example.com/hook",
		SigningSecret: "whsec_test",
	}, now)
	if err != nil {
		t.Fatalf("unexpected error creating endpoint: %v", err)
	}
	if ep.ID.Version() != 7 {
		t.Fatalf("expected UUIDv7, got version %d", ep.ID.Version())
	}
	if ep.RateLimitQPS != 50 || ep.MaxConcurrency != 20 || ep.CircuitThreshold != 30 || ep.TimeoutMs != 5000 || !ep.Enabled {
		t.Fatalf("unexpected endpoint defaults: %+v", ep)
	}

	cases := []struct {
		name   string
		params NewEndpointParams
	}{
		{
			name:   "empty name",
			params: NewEndpointParams{Name: "   ", TargetURL: "https://example.com/hook"},
		},
		{
			name:   "non-http scheme",
			params: NewEndpointParams{Name: "bad", TargetURL: "ftp://example.com/hook"},
		},
		{
			name:   "missing host",
			params: NewEndpointParams{Name: "bad", TargetURL: "https:///hook"},
		},
		{
			name:   "non-positive qps",
			params: NewEndpointParams{Name: "bad", TargetURL: "https://example.com/hook", RateLimitQPS: ptr(0.0)},
		},
		{
			name:   "non-positive max concurrency",
			params: NewEndpointParams{Name: "bad", TargetURL: "https://example.com/hook", MaxConcurrency: ptr(0)},
		},
		{
			name:   "timeout too low",
			params: NewEndpointParams{Name: "bad", TargetURL: "https://example.com/hook", TimeoutMs: ptr(50)},
		},
		{
			name:   "timeout too high",
			params: NewEndpointParams{Name: "bad", TargetURL: "https://example.com/hook", TimeoutMs: ptr(70000)},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewEndpoint(tc.params, now)
			if !errors.Is(err, ErrValidation) {
				t.Fatalf("expected ErrValidation, got %v", err)
			}
		})
	}
}

func TestEndpointApplyUpdateAtomic(t *testing.T) {
	created := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	updated := created.Add(time.Minute)

	ep, err := NewEndpoint(NewEndpointParams{
		Name:      "orders",
		TargetURL: "http://localhost:8081/webhook",
	}, created)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := ep.ApplyUpdate(UpdateEndpointParams{
		Name:      ptr("orders-v2"),
		TimeoutMs: ptr(10),
	}, updated); !errors.Is(err, ErrValidation) {
		t.Fatalf("expected ErrValidation, got %v", err)
	}
	if ep.Name != "orders" || ep.UpdatedAt != created {
		t.Fatalf("expected failed ApplyUpdate to leave endpoint untouched, got %+v", ep)
	}

	if err := ep.ApplyUpdate(UpdateEndpointParams{
		Name:    ptr("orders-v2"),
		Enabled: ptr(false),
	}, updated); err != nil {
		t.Fatalf("unexpected update error: %v", err)
	}
	if ep.Name != "orders-v2" || ep.Enabled || !ep.UpdatedAt.Equal(updated) {
		t.Fatalf("unexpected updated endpoint state: %+v", ep)
	}
}

func TestEndpointSignAndVerifyRawPayload(t *testing.T) {
	ep := Endpoint{SigningSecret: "top-secret"}
	rawBody := []byte(`{"z":1,  "a": "hello"}`)
	ts := int64(1763456789)

	sig := ep.SignPayload(ts, rawBody)
	if sig == "" {
		t.Fatal("expected non-empty signature")
	}
	if !VerifyPayloadSignature(ep.SigningSecret, sig, ts, rawBody) {
		t.Fatal("expected raw payload signature verification to succeed")
	}
	if VerifyPayloadSignature(ep.SigningSecret, sig, ts, []byte(`{"z":1,"a":"hello"}`)) {
		t.Fatal("expected whitespace-modified payload to fail signature verification")
	}
	if VerifyPayloadSignature(ep.SigningSecret, sig, ts+1, rawBody) {
		t.Fatal("expected timestamp mismatch to fail signature verification")
	}
	if VerifyPayloadSignature("", sig, ts, rawBody) {
		t.Fatal("expected empty secret to fail verification")
	}

	unsigned := Endpoint{}
	if got := unsigned.SignPayload(ts, rawBody); got != "" {
		t.Fatalf("expected empty signature without secret, got %q", got)
	}
}

func ptr[T any](v T) *T {
	return &v
}

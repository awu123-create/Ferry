package httpclient

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/awu123-create/ferry/internal/domain"
)

func TestSenderPreservesRawBytesAndSignsPayload(t *testing.T) {
	rawPayload := []byte(`{"z":  1, "a":"raw-bytes"}`)
	taskID := uuid.New()
	sentAt := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)

	var gotBody []byte
	var gotSig, gotTS, gotTaskID, gotAttempt, gotCustom string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var err error
		gotBody, err = io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read body: %v", err)
		}
		gotSig = r.Header.Get("X-Ferry-Signature")
		gotTS = r.Header.Get("X-Ferry-Timestamp")
		gotTaskID = r.Header.Get("X-Ferry-Task-ID")
		gotAttempt = r.Header.Get("X-Ferry-Attempt")
		gotCustom = r.Header.Get("X-Custom")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("accepted"))
	}))
	defer srv.Close()

	sender := NewSender(1024)
	res := sender.Send(context.Background(), domain.DeliveryRequest{
		TaskID:        taskID,
		EndpointURL:   srv.URL,
		SigningSecret: "whsec_123",
		AttemptNo:     1,
		Timeout:       2 * time.Second,
		Headers: map[string]string{
			"X-Custom":        "val",
			"X-Ferry-Task-ID": "spoofed",
		},
		RawPayload: rawPayload,
		SentAt:     sentAt,
	})

	if res.ErrorKind != domain.AttemptErrorNone || res.HTTPStatus != 200 {
		t.Fatalf("expected 200 success, got %+v", res)
	}
	if string(gotBody) != string(rawPayload) {
		t.Fatalf("expected exact raw bytes %q, got %q", rawPayload, gotBody)
	}
	if gotTaskID != taskID.String() || gotAttempt != "1" || gotCustom != "val" || gotTS != strconv.FormatInt(sentAt.Unix(), 10) {
		t.Fatalf("unexpected headers: task=%q attempt=%q custom=%q ts=%q", gotTaskID, gotAttempt, gotCustom, gotTS)
	}
	if !domain.VerifyPayloadSignature("whsec_123", gotSig, sentAt.Unix(), gotBody) {
		t.Fatalf("signature %q failed verification", gotSig)
	}
}

func TestSenderStatusClassificationTimeoutAndSnippetTruncation(t *testing.T) {
	largeBody := strings.Repeat("a", 2000)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/slow":
			time.Sleep(150 * time.Millisecond)
			w.WriteHeader(http.StatusOK)
		case "/429":
			w.Header().Set("Retry-After", "7200")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(largeBody))
		case "/400":
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte("bad request"))
		case "/503":
			w.Header().Set("Retry-After", "5")
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	}))
	defer srv.Close()

	sender := NewSender(128)
	id := uuid.New()

	timeoutRes := sender.Send(context.Background(), domain.DeliveryRequest{
		TaskID:      id,
		EndpointURL: srv.URL + "/slow",
		AttemptNo:   1,
		Timeout:     30 * time.Millisecond,
	})
	if timeoutRes.ErrorKind != domain.AttemptErrorTimeout {
		t.Fatalf("expected TIMEOUT, got %+v", timeoutRes)
	}

	rateRes := sender.Send(context.Background(), domain.DeliveryRequest{
		TaskID:      id,
		EndpointURL: srv.URL + "/429",
		AttemptNo:   1,
		Timeout:     time.Second,
	})
	if rateRes.ErrorKind != domain.AttemptErrorHTTP429 || rateRes.RetryAfter != 3600*time.Second || len(rateRes.ResponseSnippet) != 128 {
		t.Fatalf("unexpected 429 result: %+v", rateRes)
	}

	badReqRes := sender.Send(context.Background(), domain.DeliveryRequest{
		TaskID:      id,
		EndpointURL: srv.URL + "/400",
		AttemptNo:   1,
		Timeout:     time.Second,
	})
	if badReqRes.ErrorKind != domain.AttemptErrorHTTP4xx || badReqRes.ResponseSnippet != "bad request" {
		t.Fatalf("unexpected 400 result: %+v", badReqRes)
	}

	srvErrRes := sender.Send(context.Background(), domain.DeliveryRequest{
		TaskID:      id,
		EndpointURL: srv.URL + "/503",
		AttemptNo:   1,
		Timeout:     time.Second,
	})
	if srvErrRes.ErrorKind != domain.AttemptErrorHTTP5xx || srvErrRes.RetryAfter != 5*time.Second {
		t.Fatalf("unexpected 503 result: %+v", srvErrRes)
	}
}

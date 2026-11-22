package httpclient_test

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
	"github.com/awu123-create/ferry/internal/infra/httpclient"
)

func TestSender_PreservesRawBytesAndSignsPayload(t *testing.T) {
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

	sender := httpclient.NewSender(1024)
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

	if res.ErrorKind != domain.AttemptErrorNone || res.HTTPStatus != http.StatusOK {
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

func TestSender_StatusClassificationTimeoutAndSnippetTruncation(t *testing.T) {
	largeBody := strings.Repeat("a", 2000)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/slow":
			time.Sleep(150 * time.Millisecond)
			w.WriteHeader(http.StatusOK)
		case "/429":
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(largeBody))
		case "/400":
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte("bad request"))
		case "/503":
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	}))
	defer srv.Close()

	sender := httpclient.NewSender(128)
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
	if rateRes.ErrorKind != domain.AttemptErrorHTTP429 || len(rateRes.ResponseSnippet) != 128 {
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
	if srvErrRes.ErrorKind != domain.AttemptErrorHTTP5xx {
		t.Fatalf("unexpected 503 result: %+v", srvErrRes)
	}
}

func TestSender_DoesNotFollowRedirectsAndTreats2xxBodyReadErrorAsSuccess(t *testing.T) {
	var targetHits int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/redirect":
			http.Redirect(w, r, "/target", http.StatusFound)
		case "/target":
			targetHits++
			w.WriteHeader(http.StatusOK)
		case "/200-truncated":
			// 声明 100 字节响应体但写完状态码和部分字节后直接断开连接，模拟下游响应体截断。
			w.Header().Set("Content-Length", "100")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok-part"))
			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}
			if hj, ok := w.(http.Hijacker); ok {
				if conn, _, err := hj.Hijack(); err == nil {
					_ = conn.Close()
				}
			}
		}
	}))
	defer srv.Close()

	sender := httpclient.NewSender(128)
	id := uuid.New()

	redirRes := sender.Send(context.Background(), domain.DeliveryRequest{
		TaskID:      id,
		EndpointURL: srv.URL + "/redirect",
		AttemptNo:   1,
		Timeout:     time.Second,
		RawPayload:  []byte(`{"event":"paid"}`),
	})
	if redirRes.HTTPStatus != http.StatusFound || redirRes.ErrorKind != domain.AttemptErrorHTTP3xx {
		t.Fatalf("expected 302 HTTP_3XX without following redirect, got %+v", redirRes)
	}
	if targetHits != 0 {
		t.Fatalf("expected redirect target not called, got %d hits", targetHits)
	}

	truncRes := sender.Send(context.Background(), domain.DeliveryRequest{
		TaskID:      id,
		EndpointURL: srv.URL + "/200-truncated",
		AttemptNo:   1,
		Timeout:     time.Second,
		RawPayload:  []byte(`{"event":"paid"}`),
	})
	if truncRes.HTTPStatus != http.StatusOK || truncRes.ErrorKind != domain.AttemptErrorNone {
		t.Fatalf("expected 200 success even when body read fails, got %+v", truncRes)
	}
}

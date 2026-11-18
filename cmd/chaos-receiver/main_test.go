package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/awu123-create/ferry/internal/config"
	"github.com/awu123-create/ferry/internal/domain"
)

func TestChaosReceiverSignatureAndStats(t *testing.T) {
	rec := newReceiver(zap.NewNop(), config.ReceiverConfig{
		DefaultStatus: http.StatusOK,
	})
	srv := httptest.NewServer(rec.routes())
	defer srv.Close()

	client := srv.Client()
	ctx := t.Context()

	rawBody := []byte(`{"z":1,   "a":"hello"}`)
	taskID := uuid.NewString()
	secret := "whsec_demo"
	ts := time.Now().Unix()
	sig := domain.Endpoint{SigningSecret: secret}.SignPayload(ts, rawBody)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+"/webhook?secret="+secret, bytes.NewReader(rawBody))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("X-Ferry-Task-ID", taskID)
	req.Header.Set("X-Ferry-Attempt", "1")
	req.Header.Set("X-Ferry-Timestamp", strconv.FormatInt(ts, 10))
	req.Header.Set("X-Ferry-Signature", sig)

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", resp.StatusCode)
	}

	statsReq, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/stats/"+taskID, nil)
	if err != nil {
		t.Fatalf("new stats request: %v", err)
	}
	statsResp, err := client.Do(statsReq)
	if err != nil {
		t.Fatalf("get stats: %v", err)
	}
	defer func() {
		_ = statsResp.Body.Close()
	}()
	if statsResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 on /stats/{task_id}, got %d", statsResp.StatusCode)
	}

	var stat taskDeliveryStat
	if err := json.NewDecoder(statsResp.Body).Decode(&stat); err != nil {
		t.Fatalf("decode stats: %v", err)
	}
	if stat.Count != 1 || !stat.SignatureChecked || !stat.SignatureValid {
		t.Fatalf("unexpected stats: %+v", stat)
	}
	if stat.LastPayloadSHA256 != domain.ComputePayloadSHA256(rawBody) {
		t.Fatalf("payload sha256 mismatch: %s", stat.LastPayloadSHA256)
	}

	// Bad signature must return 401.
	badReq, err := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+"/webhook?secret="+secret, bytes.NewReader(rawBody))
	if err != nil {
		t.Fatalf("new bad request: %v", err)
	}
	badReq.Header.Set("X-Ferry-Task-ID", uuid.NewString())
	badReq.Header.Set("X-Ferry-Timestamp", strconv.FormatInt(ts, 10))
	badReq.Header.Set("X-Ferry-Signature", "sha256=deadbeef")

	badResp, err := client.Do(badReq)
	if err != nil {
		t.Fatalf("do bad request: %v", err)
	}
	defer func() {
		_ = badResp.Body.Close()
	}()
	if badResp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 on invalid signature, got %d", badResp.StatusCode)
	}

	// Path status override works when unsigned.
	failReq, err := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+"/webhook/400", bytes.NewReader(rawBody))
	if err != nil {
		t.Fatalf("new 400 request: %v", err)
	}
	failResp, err := client.Do(failReq)
	if err != nil {
		t.Fatalf("do 400 request: %v", err)
	}
	defer func() {
		_ = failResp.Body.Close()
	}()
	if failResp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 on /webhook/400, got %d", failResp.StatusCode)
	}
}

package integration_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"

	"github.com/awu123-create/ferry/internal/api"
	"github.com/awu123-create/ferry/internal/infra/httpclient"
	"github.com/awu123-create/ferry/internal/infra/kafka"
	"github.com/awu123-create/ferry/internal/infra/pg"
	"github.com/awu123-create/ferry/internal/infra/redis"
	"github.com/awu123-create/ferry/internal/scheduler"
	"github.com/awu123-create/ferry/internal/worker"
)

const (
	defaultIntegrationPGDSN    = "postgres://ferry:ferry@localhost:5432/ferry?sslmode=disable"
	defaultIntegrationRedis    = "localhost:6379"
	defaultIntegrationRedisDB  = 15
	defaultIntegrationKafka    = "localhost:9092"
	defaultIntegrationWaitTime = 15 * time.Second
)

type flowHarness struct {
	t        *testing.T
	pgStore  *pg.Store
	redisCli *redis.Client
	producer *kafka.ReadyProducer
	reader   *kafka.ReadyReader
	handler  *api.Handler
	apiSrv   *httptest.Server
	relay    *scheduler.OutboxRelay
	worker   *worker.Worker

	mu           sync.Mutex
	relayCancel  context.CancelFunc
	relayDone    chan error
	workerCancel context.CancelFunc
	workerDone   chan error
}

func newFlowHarness(t *testing.T) *flowHarness {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping end-to-end integration test in -short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	pgStore, cleanupPG := openIsolatedPG(t, ctx)
	redisCli := openIntegrationRedis(t, ctx)
	brokers := integrationKafkaBrokers(t)

	topic := fmt.Sprintf("ferry.it.ready.%s", uuid.NewString()[:8])
	groupID := fmt.Sprintf("ferry-it-worker-%s", uuid.NewString()[:8])

	producer, err := kafka.NewReadyProducer(brokers, topic)
	if err != nil {
		cleanupPG()
		t.Fatalf("NewReadyProducer failed: %v", err)
	}
	if err := producer.Ping(ctx); err != nil {
		producer.Close()
		cleanupPG()
		t.Fatalf("connect test kafka %v (run `make up` first): %v", brokers, err)
	}
	if err := producer.EnsureTopic(ctx, 2); err != nil {
		producer.Close()
		cleanupPG()
		t.Fatalf("EnsureTopic %s failed: %v", topic, err)
	}

	reader, err := kafka.NewReadyReader(brokers, topic, groupID)
	if err != nil {
		producer.Close()
		deleteKafkaTopic(brokers, topic)
		cleanupPG()
		t.Fatalf("NewReadyReader failed: %v", err)
	}

	log := zap.NewNop()
	handler, err := api.NewHandler(
		log,
		pgStore,
		pgStore,
		redisCli,
		[]api.ReadinessProbe{pgStore, producer, redisCli},
		api.HandlerConfig{
			DefaultMaxAttempts: 1,
			DefaultTaskWindow:  24 * time.Hour,
			MaxPayloadBytes:    1 << 20,
			CancelLockTTL:      10 * time.Second,
		},
	)
	if err != nil {
		reader.Close()
		producer.Close()
		deleteKafkaTopic(brokers, topic)
		cleanupPG()
		t.Fatalf("NewHandler failed: %v", err)
	}
	apiSrv := httptest.NewServer(handler)

	relay, err := scheduler.NewOutboxRelay(log, pgStore, producer, scheduler.OutboxRelayConfig{
		BatchSize:    50,
		PollInterval: 100 * time.Millisecond,
		BatchTimeout: 5 * time.Second,
	})
	if err != nil {
		apiSrv.Close()
		reader.Close()
		producer.Close()
		deleteKafkaTopic(brokers, topic)
		cleanupPG()
		t.Fatalf("NewOutboxRelay failed: %v", err)
	}

	sender := httpclient.NewSender(1024)
	w, err := worker.NewWorker(log, reader, pgStore, pgStore, redisCli, sender, worker.Config{
		Concurrency:          8,
		LockGrace:            10 * time.Second,
		ShutdownDrainTimeout: 10 * time.Second,
	})
	if err != nil {
		apiSrv.Close()
		reader.Close()
		producer.Close()
		deleteKafkaTopic(brokers, topic)
		cleanupPG()
		t.Fatalf("NewWorker failed: %v", err)
	}

	h := &flowHarness{
		t:        t,
		pgStore:  pgStore,
		redisCli: redisCli,
		producer: producer,
		reader:   reader,
		handler:  handler,
		apiSrv:   apiSrv,
		relay:    relay,
		worker:   w,
	}

	t.Cleanup(func() {
		h.stopWorker()
		h.stopRelay()
		apiSrv.Close()
		reader.Close()
		producer.Close()
		deleteKafkaTopic(brokers, topic)
		_ = redisCli.Close()
		cleanupPG()
	})
	return h
}

func (h *flowHarness) startRelay() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.relayCancel != nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	h.relayCancel = cancel
	h.relayDone = done
	go func() {
		done <- h.relay.Run(ctx)
	}()
}

func (h *flowHarness) stopRelay() {
	h.mu.Lock()
	cancel := h.relayCancel
	done := h.relayDone
	h.relayCancel = nil
	h.relayDone = nil
	h.mu.Unlock()

	if cancel == nil {
		return
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			h.t.Errorf("OutboxRelay.Run exited with error: %v", err)
		}
	case <-time.After(5 * time.Second):
		h.t.Error("timed out waiting for OutboxRelay to stop")
	}
}

func (h *flowHarness) startWorker() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.workerCancel != nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	h.workerCancel = cancel
	h.workerDone = done
	go func() {
		done <- h.worker.Run(ctx)
	}()
}

func (h *flowHarness) triggerWorkerShutdown() chan error {
	h.mu.Lock()
	cancel := h.workerCancel
	done := h.workerDone
	h.workerCancel = nil
	h.workerDone = nil
	h.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	return done
}

func (h *flowHarness) stopWorker() {
	done := h.triggerWorkerShutdown()
	if done == nil {
		return
	}
	select {
	case err := <-done:
		if err != nil {
			h.t.Errorf("Worker.Run exited with error: %v", err)
		}
	case <-time.After(12 * time.Second):
		h.t.Error("timed out waiting for Worker to stop")
	}
}

func (h *flowHarness) doJSON(method, path string, body []byte, headers map[string]string) (int, []byte) {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, h.apiSrv.URL+path, reader)
	if err != nil {
		h.t.Fatalf("build request %s %s: %v", method, path, err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		h.t.Fatalf("http %s %s failed: %v", method, path, err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		h.t.Fatalf("read response %s %s: %v", method, path, err)
	}
	return resp.StatusCode, raw
}

type endpointResp struct {
	ID               uuid.UUID `json:"id"`
	Name             string    `json:"name"`
	TargetURL        string    `json:"target_url"`
	HasSigningSecret bool      `json:"has_signing_secret"`
}

type taskResp struct {
	ID            uuid.UUID      `json:"id"`
	EndpointID    uuid.UUID      `json:"endpoint_id"`
	Payload       jsontext.Value `json:"payload"`
	PayloadSHA256 string         `json:"payload_sha256"`
	Status        string         `json:"status"`
	Attempt       int            `json:"attempt"`
	LastError     string         `json:"last_error"`
}

type attemptResp struct {
	HTTPStatus int    `json:"http_status"`
	ErrorKind  string `json:"error_kind"`
	AttemptNo  int    `json:"attempt_no"`
}

type attemptsListResp struct {
	Items []attemptResp `json:"items"`
}

func (h *flowHarness) createEndpoint(name, targetURL, signingSecret string, timeoutMs int) endpointResp {
	h.t.Helper()
	payload := map[string]any{
		"name":           name,
		"target_url":     targetURL,
		"signing_secret": signingSecret,
	}
	if timeoutMs > 0 {
		payload["timeout_ms"] = timeoutMs
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		h.t.Fatalf("marshal endpoint: %v", err)
	}
	status, body := h.doJSON(http.MethodPost, "/v1/endpoints", raw, nil)
	if status != http.StatusCreated {
		h.t.Fatalf("create endpoint expected 201, got %d: %s", status, body)
	}
	var ep endpointResp
	if err := json.Unmarshal(body, &ep); err != nil {
		h.t.Fatalf("decode endpoint response: %v", err)
	}
	return ep
}

func (h *flowHarness) getTask(id uuid.UUID) taskResp {
	h.t.Helper()
	status, body := h.doJSON(http.MethodGet, "/v1/tasks/"+id.String(), nil, nil)
	if status != http.StatusOK {
		h.t.Fatalf("get task %s expected 200, got %d: %s", id, status, body)
	}
	var out taskResp
	if err := json.Unmarshal(body, &out); err != nil {
		h.t.Fatalf("decode task response: %v", err)
	}
	return out
}

func (h *flowHarness) listAttempts(id uuid.UUID) []attemptResp {
	h.t.Helper()
	status, body := h.doJSON(http.MethodGet, "/v1/tasks/"+id.String()+"/attempts", nil, nil)
	if status != http.StatusOK {
		h.t.Fatalf("list attempts %s expected 200, got %d: %s", id, status, body)
	}
	var out attemptsListResp
	if err := json.Unmarshal(body, &out); err != nil {
		h.t.Fatalf("decode attempts response: %v", err)
	}
	return out.Items
}

func waitUntil(t *testing.T, timeout time.Duration, desc string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(30 * time.Millisecond)
	}
	t.Fatalf("timed out after %v waiting for: %s", timeout, desc)
}

func openIsolatedPG(t *testing.T, ctx context.Context) (*pg.Store, func()) {
	t.Helper()
	baseDSN := strings.TrimSpace(os.Getenv("FERRY_TEST_PG_DSN"))
	if baseDSN == "" {
		baseDSN = defaultIntegrationPGDSN
	}

	adminDB, err := sql.Open("pgx", baseDSN)
	if err != nil {
		t.Fatalf("open admin postgres: %v", err)
	}
	if err := adminDB.PingContext(ctx); err != nil {
		_ = adminDB.Close()
		t.Fatalf("connect test postgres (run `make up` first): %v", err)
	}

	dbName := fmt.Sprintf("ferry_it_%d_%d", os.Getpid(), time.Now().UnixNano())
	if _, err := adminDB.ExecContext(ctx, "CREATE DATABASE "+dbName); err != nil {
		_ = adminDB.Close()
		t.Fatalf("create isolated db %s: %v", dbName, err)
	}

	parsed, err := url.Parse(baseDSN)
	if err != nil {
		dropPGDatabase(adminDB, dbName)
		_ = adminDB.Close()
		t.Fatalf("parse postgres DSN: %v", err)
	}
	parsed.Path = "/" + dbName

	store, err := pg.Open(ctx, parsed.String(), nil)
	if err != nil {
		dropPGDatabase(adminDB, dbName)
		_ = adminDB.Close()
		t.Fatalf("open pg.Store: %v", err)
	}
	if err := store.RunMigrations(ctx); err != nil {
		_ = store.Close()
		dropPGDatabase(adminDB, dbName)
		_ = adminDB.Close()
		t.Fatalf("run migrations: %v", err)
	}

	return store, func() {
		_ = store.Close()
		dropPGDatabase(adminDB, dbName)
		_ = adminDB.Close()
	}
}

func dropPGDatabase(adminDB *sql.DB, dbName string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, _ = adminDB.ExecContext(ctx, "DROP DATABASE IF EXISTS "+dbName+" WITH (FORCE)")
}

func openIntegrationRedis(t *testing.T, ctx context.Context) *redis.Client {
	t.Helper()
	addr := strings.TrimSpace(os.Getenv("FERRY_TEST_REDIS_ADDR"))
	if addr == "" {
		addr = defaultIntegrationRedis
	}
	password := os.Getenv("FERRY_TEST_REDIS_PASSWORD")
	db := defaultIntegrationRedisDB
	if raw := strings.TrimSpace(os.Getenv("FERRY_TEST_REDIS_DB")); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil {
			t.Fatalf("invalid FERRY_TEST_REDIS_DB %q: %v", raw, err)
		}
		db = parsed
	}

	cli, err := redis.New(ctx, addr, password, db)
	if err != nil {
		t.Fatalf("connect test redis %s db=%d (run `make up` first): %v", addr, db, err)
	}
	return cli
}

func integrationKafkaBrokers(t *testing.T) []string {
	t.Helper()
	raw := strings.TrimSpace(os.Getenv("FERRY_TEST_KAFKA_BROKERS"))
	if raw == "" {
		return []string{defaultIntegrationKafka}
	}
	var out []string
	for _, part := range strings.Split(raw, ",") {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	if len(out) == 0 {
		t.Fatal("FERRY_TEST_KAFKA_BROKERS parsed empty broker list")
	}
	return out
}

func deleteKafkaTopic(brokers []string, topic string) {
	cli, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return
	}
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _ = kadm.NewClient(cli).DeleteTopics(ctx, topic)
}

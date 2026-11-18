package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"go.uber.org/zap"

	"github.com/awu123-create/ferry/internal/config"
	"github.com/awu123-create/ferry/internal/domain"
	"github.com/awu123-create/ferry/internal/logger"
)

const maxReceiverBodyBytes = 4 << 20

type taskDeliveryStat struct {
	TaskID            string    `json:"task_id"`
	Count             int       `json:"count"`
	SignatureValid    bool      `json:"signature_valid"`
	SignatureChecked  bool      `json:"signature_checked"`
	LastAttempt       int       `json:"last_attempt"`
	LastStatus        int       `json:"last_status"`
	LastPayloadSHA256 string    `json:"last_payload_sha256"`
	LastBody          string    `json:"last_body"`
	LastReceivedAt    time.Time `json:"last_received_at"`
}

type receiver struct {
	log           *zap.Logger
	defaultSecret string
	defaultStatus int
	defaultDelay  time.Duration

	mu            sync.RWMutex
	totalReceived int
	tasks         map[string]taskDeliveryStat
}

func newReceiver(log *zap.Logger, cfg config.ReceiverConfig) *receiver {
	if log == nil {
		log = zap.NewNop()
	}
	return &receiver{
		log:           log,
		defaultSecret: cfg.SigningSecret,
		defaultStatus: cfg.DefaultStatus,
		defaultDelay:  cfg.DefaultDelay,
		tasks:         make(map[string]taskDeliveryStat),
	}
}

func (rec *receiver) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", rec.handleHealthz)
	mux.HandleFunc("POST /webhook", rec.handleWebhook)
	mux.HandleFunc("POST /webhook/{status}", rec.handleWebhook)
	mux.HandleFunc("GET /stats", rec.handleStats)
	mux.HandleFunc("GET /stats/{task_id}", rec.handleTaskStats)
	mux.HandleFunc("POST /reset", rec.handleReset)
	return mux
}

func (rec *receiver) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSONResponse(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (rec *receiver) handleWebhook(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxReceiverBodyBytes))
	if err != nil {
		writeJSONResponse(w, http.StatusBadRequest, map[string]string{"error": "failed to read body"})
		return
	}

	taskID := strings.TrimSpace(r.Header.Get("X-Ferry-Task-ID"))
	attemptNo, _ := strconv.Atoi(strings.TrimSpace(r.Header.Get("X-Ferry-Attempt")))
	sigHeader := r.Header.Get("X-Ferry-Signature")
	tsHeader := strings.TrimSpace(r.Header.Get("X-Ferry-Timestamp"))

	secret := rec.defaultSecret
	if querySecret := r.URL.Query().Get("secret"); querySecret != "" {
		secret = querySecret
	}

	sigChecked := false
	sigValid := true
	if secret != "" {
		sigChecked = true
		ts, parseErr := strconv.ParseInt(tsHeader, 10, 64)
		sigValid = parseErr == nil && domain.VerifyPayloadSignature(secret, sigHeader, ts, body)
	}

	delay := rec.resolveDelay(r)
	if delay > 0 {
		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-r.Context().Done():
			timer.Stop()
			return
		}
	}

	statusCode := rec.resolveStatus(r)
	if !sigValid {
		statusCode = http.StatusUnauthorized
	}

	if retryAfter := strings.TrimSpace(r.URL.Query().Get("retry_after")); retryAfter != "" {
		w.Header().Set("Retry-After", retryAfter)
	}

	sha := domain.ComputePayloadSHA256(body)
	rec.record(taskID, attemptNo, statusCode, sha, string(body), sigChecked, sigValid)

	rec.log.Info(
		"chaos receiver handled delivery",
		zap.String("task_id", taskID),
		zap.Int("attempt", attemptNo),
		zap.Int("status", statusCode),
		zap.Bool("signature_checked", sigChecked),
		zap.Bool("signature_valid", sigValid),
	)

	writeJSONResponse(w, statusCode, map[string]any{
		"received":          true,
		"task_id":           taskID,
		"attempt":           attemptNo,
		"status":            statusCode,
		"signature_checked": sigChecked,
		"signature_valid":   sigValid,
		"payload_sha256":    sha,
	})
}

func (rec *receiver) resolveDelay(r *http.Request) time.Duration {
	if raw := strings.TrimSpace(r.URL.Query().Get("delay_ms")); raw != "" {
		if ms, err := strconv.Atoi(raw); err == nil && ms >= 0 {
			return time.Duration(ms) * time.Millisecond
		}
	}
	return rec.defaultDelay
}

func (rec *receiver) resolveStatus(r *http.Request) int {
	if raw := strings.TrimSpace(r.PathValue("status")); raw != "" {
		if code, err := strconv.Atoi(raw); err == nil && code >= 100 && code <= 599 {
			return code
		}
	}
	if raw := strings.TrimSpace(r.URL.Query().Get("status")); raw != "" {
		if code, err := strconv.Atoi(raw); err == nil && code >= 100 && code <= 599 {
			return code
		}
	}
	return rec.defaultStatus
}

func (rec *receiver) record(
	taskID string,
	attemptNo, statusCode int,
	sha, body string,
	sigChecked, sigValid bool,
) {
	rec.mu.Lock()
	defer rec.mu.Unlock()

	rec.totalReceived++
	if taskID == "" {
		return
	}
	stat := rec.tasks[taskID]
	stat.TaskID = taskID
	stat.Count++
	stat.SignatureChecked = sigChecked
	stat.SignatureValid = sigValid
	stat.LastAttempt = attemptNo
	stat.LastStatus = statusCode
	stat.LastPayloadSHA256 = sha
	stat.LastBody = body
	stat.LastReceivedAt = time.Now().UTC()
	rec.tasks[taskID] = stat
}

func (rec *receiver) handleStats(w http.ResponseWriter, _ *http.Request) {
	rec.mu.RLock()
	defer rec.mu.RUnlock()

	tasksCopy := make(map[string]taskDeliveryStat, len(rec.tasks))
	for k, v := range rec.tasks {
		tasksCopy[k] = v
	}
	writeJSONResponse(w, http.StatusOK, map[string]any{
		"total_received": rec.totalReceived,
		"tasks":          tasksCopy,
	})
}

func (rec *receiver) handleTaskStats(w http.ResponseWriter, r *http.Request) {
	taskID := strings.TrimSpace(r.PathValue("task_id"))

	rec.mu.RLock()
	stat, ok := rec.tasks[taskID]
	rec.mu.RUnlock()

	if !ok {
		writeJSONResponse(w, http.StatusNotFound, map[string]string{"error": "task not recorded"})
		return
	}
	writeJSONResponse(w, http.StatusOK, stat)
}

func (rec *receiver) handleReset(w http.ResponseWriter, _ *http.Request) {
	rec.mu.Lock()
	rec.totalReceived = 0
	rec.tasks = make(map[string]taskDeliveryStat)
	rec.mu.Unlock()
	writeJSONResponse(w, http.StatusOK, map[string]string{"status": "reset"})
}

func writeJSONResponse(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "chaos-receiver failed: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.LoadReceiverConfig()
	if err != nil {
		return err
	}
	log, err := logger.New(cfg.LogLevel)
	if err != nil {
		return err
	}
	defer func() {
		_ = log.Sync()
	}()

	rec := newReceiver(log, cfg)
	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           rec.routes(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)
	go func() {
		log.Info("chaos-receiver listening", zap.String("addr", cfg.HTTPAddr))
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
		close(errCh)
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return srv.Shutdown(shutdownCtx)
	}
}

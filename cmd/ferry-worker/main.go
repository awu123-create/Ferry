package main

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/awu123-create/ferry/internal/config"
	"github.com/awu123-create/ferry/internal/infra/httpclient"
	"github.com/awu123-create/ferry/internal/infra/kafka"
	"github.com/awu123-create/ferry/internal/infra/pg"
	"github.com/awu123-create/ferry/internal/infra/redis"
	"github.com/awu123-create/ferry/internal/logger"
	"github.com/awu123-create/ferry/internal/metrics"
	"github.com/awu123-create/ferry/internal/worker"
)

var (
	_ worker.ReadyStream     = (*kafka.ReadyReader)(nil)
	_ worker.TaskStore       = (*pg.Store)(nil)
	_ worker.EndpointReader  = (*pg.Store)(nil)
	_ worker.ExecutionLocker = (*redis.Client)(nil)
	_ worker.HTTPSender      = (*httpclient.Sender)(nil)
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "ferry-worker failed: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.LoadWorkerConfig()
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

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	initCtx, cancelInit := context.WithTimeout(ctx, 15*time.Second)
	defer cancelInit()

	pgStore, err := pg.Open(initCtx, cfg.PostgresDSN, logger.NewGormLogger(log, cfg.SlowSQLThreshold))
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := pgStore.Close(); closeErr != nil {
			log.Warn("close postgres store failed", zap.Error(closeErr))
		}
	}()

	reader, err := kafka.NewReadyReader(cfg.KafkaBrokers, cfg.KafkaReadyTopic, cfg.KafkaGroupID)
	if err != nil {
		return err
	}
	defer reader.Close()

	if err := reader.Ping(initCtx); err != nil {
		return fmt.Errorf("ping kafka: %w", err)
	}

	redisClient, err := redis.New(initCtx, cfg.RedisAddr, cfg.RedisPassword, cfg.RedisDB)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := redisClient.Close(); closeErr != nil {
			log.Warn("close redis client failed", zap.Error(closeErr))
		}
	}()

	reg, err := metrics.NewRegistry()
	if err != nil {
		return err
	}

	sender := httpclient.NewSender(cfg.MaxResponseSnippetBytes)
	w, err := worker.NewWorker(
		log,
		reader,
		pgStore,
		pgStore,
		redisClient,
		sender,
		worker.Config{
			Concurrency:          cfg.WorkerConcurrency,
			LockGrace:            cfg.ExecutionLockGrace,
			ShutdownDrainTimeout: cfg.ShutdownDrainTimeout,
		},
	)
	if err != nil {
		return err
	}

	probeMux := http.NewServeMux()
	probeMux.HandleFunc("GET /healthz", func(rw http.ResponseWriter, _ *http.Request) {
		writeProbeStatus(rw, http.StatusOK, "ok")
	})
	probeMux.HandleFunc("GET /readyz", func(rw http.ResponseWriter, _ *http.Request) {
		if w.Ready() {
			writeProbeStatus(rw, http.StatusOK, "ready")
			return
		}
		writeProbeStatus(rw, http.StatusServiceUnavailable, "not_ready")
	})
	probeSrv := &http.Server{
		Addr:              cfg.ProbeAddr,
		Handler:           probeMux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	metricsMux := http.NewServeMux()
	metricsMux.Handle("GET /metrics", reg.Handler())
	metricsSrv := &http.Server{
		Addr:              cfg.MetricsAddr,
		Handler:           metricsMux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	g, groupCtx := errgroup.WithContext(ctx)

	g.Go(func() error {
		log.Info("ferry-worker probe server listening", zap.String("addr", cfg.ProbeAddr))
		if err := probeSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("probe http server: %w", err)
		}
		return nil
	})

	g.Go(func() error {
		log.Info("ferry-worker metrics listening", zap.String("addr", cfg.MetricsAddr))
		if err := metricsSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("metrics http server: %w", err)
		}
		return nil
	})

	g.Go(func() error {
		return w.Run(groupCtx)
	})

	g.Go(func() error {
		<-groupCtx.Done()
		shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), cfg.ShutdownDrainTimeout)
		defer cancelShutdown()

		if err := probeSrv.Shutdown(shutdownCtx); err != nil {
			log.Warn("shutdown probe server failed", zap.Error(err))
		}
		if err := metricsSrv.Shutdown(shutdownCtx); err != nil {
			log.Warn("shutdown metrics server failed", zap.Error(err))
		}
		return nil
	})

	return g.Wait()
}

func writeProbeStatus(w http.ResponseWriter, status int, state string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.MarshalWrite(w, map[string]string{"status": state})
}

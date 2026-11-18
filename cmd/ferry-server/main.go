package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/awu123-create/ferry/internal/api"
	"github.com/awu123-create/ferry/internal/config"
	"github.com/awu123-create/ferry/internal/infra/kafka"
	"github.com/awu123-create/ferry/internal/infra/pg"
	"github.com/awu123-create/ferry/internal/infra/redis"
	"github.com/awu123-create/ferry/internal/logger"
	"github.com/awu123-create/ferry/internal/metrics"
	"github.com/awu123-create/ferry/internal/scheduler"
)

var (
	_ api.EndpointStore        = (*pg.Store)(nil)
	_ api.TaskStore            = (*pg.Store)(nil)
	_ api.ExecutionLocker      = (*redis.Client)(nil)
	_ api.ReadinessProbe       = (*pg.Store)(nil)
	_ api.ReadinessProbe       = (*kafka.ReadyProducer)(nil)
	_ api.ReadinessProbe       = (*redis.Client)(nil)
	_ scheduler.OutboxStore    = (*pg.Store)(nil)
	_ scheduler.ReadyPublisher = (*kafka.ReadyProducer)(nil)
)

func main() {
	migrateOnly := flag.Bool("migrate-only", false, "apply database migrations and exit")
	flag.Parse()

	if err := run(*migrateOnly); err != nil {
		fmt.Fprintf(os.Stderr, "ferry-server failed: %v\n", err)
		os.Exit(1)
	}
}

func run(migrateOnly bool) error {
	cfg, err := config.LoadServerConfig()
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

	if err := pgStore.RunMigrations(initCtx); err != nil {
		return err
	}
	if migrateOnly {
		log.Info("database migrations applied")
		return nil
	}

	producer, err := kafka.NewReadyProducer(cfg.KafkaBrokers, cfg.KafkaReadyTopic)
	if err != nil {
		return err
	}
	defer producer.Close()

	if err := producer.EnsureTopic(initCtx, cfg.KafkaReadyPartitions); err != nil {
		return err
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

	apiHandler, err := api.NewHandler(
		log,
		pgStore,
		pgStore,
		redisClient,
		[]api.ReadinessProbe{pgStore, producer, redisClient},
		api.HandlerConfig{
			DefaultMaxAttempts: cfg.DefaultMaxAttempts,
			DefaultTaskWindow:  cfg.DefaultTaskWindow,
			MaxPayloadBytes:    cfg.MaxPayloadBytes,
		},
	)
	if err != nil {
		return err
	}

	relay, err := scheduler.NewOutboxRelay(
		log,
		pgStore,
		producer,
		scheduler.OutboxRelayConfig{
			BatchSize:    cfg.OutboxBatchSize,
			PollInterval: cfg.OutboxPollInterval,
			BatchTimeout: cfg.OutboxBatchTimeout,
		},
	)
	if err != nil {
		return err
	}

	apiSrv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           apiHandler,
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
		log.Info("ferry-server HTTP API listening", zap.String("addr", cfg.HTTPAddr))
		if err := apiSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("api http server: %w", err)
		}
		return nil
	})

	g.Go(func() error {
		log.Info("ferry-server metrics listening", zap.String("addr", cfg.MetricsAddr))
		if err := metricsSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("metrics http server: %w", err)
		}
		return nil
	})

	g.Go(func() error {
		return relay.Run(groupCtx)
	})

	g.Go(func() error {
		<-groupCtx.Done()
		shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), cfg.ShutdownDrainTimeout)
		defer cancelShutdown()

		if err := apiSrv.Shutdown(shutdownCtx); err != nil {
			log.Warn("shutdown api server failed", zap.Error(err))
		}
		if err := metricsSrv.Shutdown(shutdownCtx); err != nil {
			log.Warn("shutdown metrics server failed", zap.Error(err))
		}
		return nil
	})

	return g.Wait()
}

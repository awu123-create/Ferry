package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// ServerConfig 保存 ferry-server 的 12-Factor 环境变量配置。
type ServerConfig struct {
	HTTPAddr             string
	MetricsAddr          string
	PostgresDSN          string
	KafkaBrokers         []string
	KafkaReadyTopic      string
	KafkaReadyPartitions int32
	RedisAddr            string
	RedisPassword        string
	RedisDB              int
	LogLevel             string
	SlowSQLThreshold     time.Duration
	DefaultMaxAttempts   int
	DefaultTaskWindow    time.Duration
	OutboxBatchSize      int
	OutboxPollInterval   time.Duration
	OutboxBatchTimeout   time.Duration
	MaxPayloadBytes      int
	ShutdownDrainTimeout time.Duration
}

// WorkerConfig 保存 ferry-worker 的 12-Factor 环境变量配置。
type WorkerConfig struct {
	ProbeAddr               string
	MetricsAddr             string
	PostgresDSN             string
	KafkaBrokers            []string
	KafkaReadyTopic         string
	KafkaGroupID            string
	RedisAddr               string
	RedisPassword           string
	RedisDB                 int
	LogLevel                string
	SlowSQLThreshold        time.Duration
	WorkerConcurrency       int
	ExecutionLockGrace      time.Duration
	MaxResponseSnippetBytes int
	ShutdownDrainTimeout    time.Duration
}

// ReceiverConfig 保存 chaos-receiver 的 12-Factor 环境变量配置。
type ReceiverConfig struct {
	HTTPAddr      string
	SigningSecret string
	DefaultStatus int
	DefaultDelay  time.Duration
	LogLevel      string
}

// LoadServerConfig 解析并校验 ferry-server 的环境变量配置。
func LoadServerConfig() (ServerConfig, error) {
	cfg := ServerConfig{
		HTTPAddr:             envString("FERRY_HTTP_ADDR", ":8080"),
		MetricsAddr:          envString("FERRY_METRICS_ADDR", ":2112"),
		PostgresDSN:          envString("FERRY_PG_DSN", "postgres://ferry:ferry@localhost:5432/ferry?sslmode=disable"),
		KafkaBrokers:         envCSV("FERRY_KAFKA_BROKERS", []string{"localhost:9092"}),
		KafkaReadyTopic:      envString("FERRY_KAFKA_READY_TOPIC", "ferry.tasks.ready"),
		KafkaReadyPartitions: 12,
		RedisAddr:            envString("FERRY_REDIS_ADDR", "localhost:6379"),
		RedisPassword:        envString("FERRY_REDIS_PASSWORD", ""),
		RedisDB:              0,
		LogLevel:             envString("FERRY_LOG_LEVEL", "info"),
		SlowSQLThreshold:     200 * time.Millisecond,
		DefaultMaxAttempts:   1,
		DefaultTaskWindow:    24 * time.Hour,
		OutboxBatchSize:      100,
		OutboxPollInterval:   500 * time.Millisecond,
		OutboxBatchTimeout:   5 * time.Second,
		MaxPayloadBytes:      1 << 20,
		ShutdownDrainTimeout: 25 * time.Second,
	}

	var err error
	if cfg.KafkaReadyPartitions, err = envInt32("FERRY_KAFKA_READY_PARTITIONS", cfg.KafkaReadyPartitions); err != nil {
		return ServerConfig{}, err
	}
	if cfg.RedisDB, err = envInt("FERRY_REDIS_DB", cfg.RedisDB); err != nil {
		return ServerConfig{}, err
	}
	if cfg.SlowSQLThreshold, err = envDuration("FERRY_SLOW_SQL_THRESHOLD", cfg.SlowSQLThreshold); err != nil {
		return ServerConfig{}, err
	}
	if cfg.DefaultMaxAttempts, err = envInt("FERRY_DEFAULT_MAX_ATTEMPTS", cfg.DefaultMaxAttempts); err != nil {
		return ServerConfig{}, err
	}
	if cfg.DefaultTaskWindow, err = envDuration("FERRY_DEFAULT_TASK_WINDOW", cfg.DefaultTaskWindow); err != nil {
		return ServerConfig{}, err
	}
	if cfg.OutboxBatchSize, err = envInt("FERRY_OUTBOX_BATCH_SIZE", cfg.OutboxBatchSize); err != nil {
		return ServerConfig{}, err
	}
	if cfg.OutboxPollInterval, err = envDuration("FERRY_OUTBOX_POLL_INTERVAL", cfg.OutboxPollInterval); err != nil {
		return ServerConfig{}, err
	}
	if cfg.OutboxBatchTimeout, err = envDuration("FERRY_OUTBOX_BATCH_TIMEOUT", cfg.OutboxBatchTimeout); err != nil {
		return ServerConfig{}, err
	}
	if cfg.MaxPayloadBytes, err = envInt("FERRY_MAX_PAYLOAD_BYTES", cfg.MaxPayloadBytes); err != nil {
		return ServerConfig{}, err
	}
	if cfg.ShutdownDrainTimeout, err = envDuration("FERRY_SHUTDOWN_DRAIN_TIMEOUT", cfg.ShutdownDrainTimeout); err != nil {
		return ServerConfig{}, err
	}

	if err := cfg.validate(); err != nil {
		return ServerConfig{}, err
	}
	return cfg, nil
}

func (c ServerConfig) validate() error {
	if strings.TrimSpace(c.HTTPAddr) == "" {
		return fmt.Errorf("FERRY_HTTP_ADDR must not be empty")
	}
	if strings.TrimSpace(c.MetricsAddr) == "" {
		return fmt.Errorf("FERRY_METRICS_ADDR must not be empty")
	}
	if strings.TrimSpace(c.PostgresDSN) == "" {
		return fmt.Errorf("FERRY_PG_DSN must not be empty")
	}
	if len(c.KafkaBrokers) == 0 {
		return fmt.Errorf("FERRY_KAFKA_BROKERS must not be empty")
	}
	if strings.TrimSpace(c.KafkaReadyTopic) == "" {
		return fmt.Errorf("FERRY_KAFKA_READY_TOPIC must not be empty")
	}
	if c.KafkaReadyPartitions <= 0 {
		return fmt.Errorf("FERRY_KAFKA_READY_PARTITIONS must be > 0")
	}
	if strings.TrimSpace(c.RedisAddr) == "" {
		return fmt.Errorf("FERRY_REDIS_ADDR must not be empty")
	}
	if c.RedisDB < 0 {
		return fmt.Errorf("FERRY_REDIS_DB must be >= 0")
	}
	if c.SlowSQLThreshold < 0 {
		return fmt.Errorf("FERRY_SLOW_SQL_THRESHOLD must be >= 0")
	}
	if c.DefaultMaxAttempts < 1 || c.DefaultMaxAttempts > 20 {
		return fmt.Errorf("FERRY_DEFAULT_MAX_ATTEMPTS must be between 1 and 20")
	}
	if c.DefaultTaskWindow <= 0 {
		return fmt.Errorf("FERRY_DEFAULT_TASK_WINDOW must be > 0")
	}
	if c.OutboxBatchSize <= 0 {
		return fmt.Errorf("FERRY_OUTBOX_BATCH_SIZE must be > 0")
	}
	if c.OutboxPollInterval <= 0 {
		return fmt.Errorf("FERRY_OUTBOX_POLL_INTERVAL must be > 0")
	}
	if c.OutboxBatchTimeout <= 0 {
		return fmt.Errorf("FERRY_OUTBOX_BATCH_TIMEOUT must be > 0")
	}
	if c.MaxPayloadBytes <= 0 {
		return fmt.Errorf("FERRY_MAX_PAYLOAD_BYTES must be > 0")
	}
	if c.ShutdownDrainTimeout <= 0 {
		return fmt.Errorf("FERRY_SHUTDOWN_DRAIN_TIMEOUT must be > 0")
	}
	return nil
}

// LoadWorkerConfig 解析并校验 ferry-worker 的环境变量配置。
func LoadWorkerConfig() (WorkerConfig, error) {
	cfg := WorkerConfig{
		ProbeAddr:               envString("FERRY_WORKER_PROBE_ADDR", ":8082"),
		MetricsAddr:             envString("FERRY_WORKER_METRICS_ADDR", ":2113"),
		PostgresDSN:             envString("FERRY_PG_DSN", "postgres://ferry:ferry@localhost:5432/ferry?sslmode=disable"),
		KafkaBrokers:            envCSV("FERRY_KAFKA_BROKERS", []string{"localhost:9092"}),
		KafkaReadyTopic:         envString("FERRY_KAFKA_READY_TOPIC", "ferry.tasks.ready"),
		KafkaGroupID:            envString("FERRY_KAFKA_GROUP_ID", "ferry-worker"),
		RedisAddr:               envString("FERRY_REDIS_ADDR", "localhost:6379"),
		RedisPassword:           envString("FERRY_REDIS_PASSWORD", ""),
		RedisDB:                 0,
		LogLevel:                envString("FERRY_LOG_LEVEL", "info"),
		SlowSQLThreshold:        200 * time.Millisecond,
		WorkerConcurrency:       32,
		ExecutionLockGrace:      15 * time.Second,
		MaxResponseSnippetBytes: 1024,
		ShutdownDrainTimeout:    25 * time.Second,
	}

	var err error
	if cfg.RedisDB, err = envInt("FERRY_REDIS_DB", cfg.RedisDB); err != nil {
		return WorkerConfig{}, err
	}
	if cfg.SlowSQLThreshold, err = envDuration("FERRY_SLOW_SQL_THRESHOLD", cfg.SlowSQLThreshold); err != nil {
		return WorkerConfig{}, err
	}
	if cfg.WorkerConcurrency, err = envInt("FERRY_WORKER_CONCURRENCY", cfg.WorkerConcurrency); err != nil {
		return WorkerConfig{}, err
	}
	if cfg.ExecutionLockGrace, err = envDuration("FERRY_EXECUTION_LOCK_GRACE", cfg.ExecutionLockGrace); err != nil {
		return WorkerConfig{}, err
	}
	if cfg.MaxResponseSnippetBytes, err = envInt("FERRY_MAX_RESPONSE_SNIPPET_BYTES", cfg.MaxResponseSnippetBytes); err != nil {
		return WorkerConfig{}, err
	}
	if cfg.ShutdownDrainTimeout, err = envDuration("FERRY_SHUTDOWN_DRAIN_TIMEOUT", cfg.ShutdownDrainTimeout); err != nil {
		return WorkerConfig{}, err
	}

	if err := cfg.validate(); err != nil {
		return WorkerConfig{}, err
	}
	return cfg, nil
}

func (c WorkerConfig) validate() error {
	if strings.TrimSpace(c.ProbeAddr) == "" {
		return fmt.Errorf("FERRY_WORKER_PROBE_ADDR must not be empty")
	}
	if strings.TrimSpace(c.MetricsAddr) == "" {
		return fmt.Errorf("FERRY_WORKER_METRICS_ADDR must not be empty")
	}
	if strings.TrimSpace(c.PostgresDSN) == "" {
		return fmt.Errorf("FERRY_PG_DSN must not be empty")
	}
	if len(c.KafkaBrokers) == 0 {
		return fmt.Errorf("FERRY_KAFKA_BROKERS must not be empty")
	}
	if strings.TrimSpace(c.KafkaReadyTopic) == "" {
		return fmt.Errorf("FERRY_KAFKA_READY_TOPIC must not be empty")
	}
	if strings.TrimSpace(c.KafkaGroupID) == "" {
		return fmt.Errorf("FERRY_KAFKA_GROUP_ID must not be empty")
	}
	if strings.TrimSpace(c.RedisAddr) == "" {
		return fmt.Errorf("FERRY_REDIS_ADDR must not be empty")
	}
	if c.RedisDB < 0 {
		return fmt.Errorf("FERRY_REDIS_DB must be >= 0")
	}
	if c.SlowSQLThreshold < 0 {
		return fmt.Errorf("FERRY_SLOW_SQL_THRESHOLD must be >= 0")
	}
	if c.WorkerConcurrency <= 0 {
		return fmt.Errorf("FERRY_WORKER_CONCURRENCY must be > 0")
	}
	if c.ExecutionLockGrace <= 0 {
		return fmt.Errorf("FERRY_EXECUTION_LOCK_GRACE must be > 0")
	}
	if c.MaxResponseSnippetBytes <= 0 {
		return fmt.Errorf("FERRY_MAX_RESPONSE_SNIPPET_BYTES must be > 0")
	}
	if c.ShutdownDrainTimeout <= 0 {
		return fmt.Errorf("FERRY_SHUTDOWN_DRAIN_TIMEOUT must be > 0")
	}
	return nil
}

// LoadReceiverConfig 解析并校验 chaos-receiver 的环境变量配置。
func LoadReceiverConfig() (ReceiverConfig, error) {
	cfg := ReceiverConfig{
		HTTPAddr:      envString("FERRY_RECEIVER_ADDR", ":8081"),
		SigningSecret: envString("FERRY_RECEIVER_SECRET", ""),
		DefaultStatus: 200,
		DefaultDelay:  0,
		LogLevel:      envString("FERRY_LOG_LEVEL", "info"),
	}

	var err error
	if cfg.DefaultStatus, err = envInt("FERRY_RECEIVER_DEFAULT_STATUS", cfg.DefaultStatus); err != nil {
		return ReceiverConfig{}, err
	}
	if cfg.DefaultDelay, err = envDuration("FERRY_RECEIVER_DEFAULT_DELAY", cfg.DefaultDelay); err != nil {
		return ReceiverConfig{}, err
	}

	if strings.TrimSpace(cfg.HTTPAddr) == "" {
		return ReceiverConfig{}, fmt.Errorf("FERRY_RECEIVER_ADDR must not be empty")
	}
	if cfg.DefaultStatus < 100 || cfg.DefaultStatus > 599 {
		return ReceiverConfig{}, fmt.Errorf("FERRY_RECEIVER_DEFAULT_STATUS must be between 100 and 599")
	}
	if cfg.DefaultDelay < 0 {
		return ReceiverConfig{}, fmt.Errorf("FERRY_RECEIVER_DEFAULT_DELAY must be >= 0")
	}
	return cfg, nil
}

func envString(key, fallback string) string {
	val, ok := os.LookupEnv(key)
	if !ok {
		return fallback
	}
	trimmed := strings.TrimSpace(val)
	if trimmed == "" {
		return ""
	}
	return trimmed
}

func envCSV(key string, fallback []string) []string {
	val, ok := os.LookupEnv(key)
	if !ok {
		return fallback
	}
	parts := strings.Split(val, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if trimmed := strings.TrimSpace(p); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

func envInt(key string, fallback int) (int, error) {
	val, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(val) == "" {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(strings.TrimSpace(val))
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", key, err)
	}
	return parsed, nil
}

func envInt32(key string, fallback int32) (int32, error) {
	val, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(val) == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseInt(strings.TrimSpace(val), 10, 32)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", key, err)
	}
	return int32(parsed), nil
}

func envDuration(key string, fallback time.Duration) (time.Duration, error) {
	val, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(val) == "" {
		return fallback, nil
	}
	parsed, err := time.ParseDuration(strings.TrimSpace(val))
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", key, err)
	}
	return parsed, nil
}

package config

import (
	"testing"
	"time"
)

func TestLoadServerConfigDefaults(t *testing.T) {
	cfg, err := LoadServerConfig()
	if err != nil {
		t.Fatalf("LoadServerConfig returned unexpected error: %v", err)
	}
	if cfg.HTTPAddr != ":8080" || cfg.MetricsAddr != ":2112" {
		t.Fatalf("unexpected default addrs: http=%q metrics=%q", cfg.HTTPAddr, cfg.MetricsAddr)
	}
	if cfg.DefaultMaxAttempts != 1 {
		t.Fatalf("expected Milestone 1 default max attempts = 1, got %d", cfg.DefaultMaxAttempts)
	}
	if cfg.DefaultTaskWindow != 24*time.Hour {
		t.Fatalf("expected default task window = 24h, got %v", cfg.DefaultTaskWindow)
	}
	if cfg.OutboxBatchSize != 100 || cfg.OutboxPollInterval != 500*time.Millisecond || cfg.OutboxBatchTimeout != 5*time.Second {
		t.Fatalf("unexpected outbox defaults: %+v", cfg)
	}
}

func TestLoadServerConfigValidationErrors(t *testing.T) {
	t.Run("invalid max attempts", func(t *testing.T) {
		t.Setenv("FERRY_DEFAULT_MAX_ATTEMPTS", "25")
		if _, err := LoadServerConfig(); err == nil {
			t.Fatal("expected error for FERRY_DEFAULT_MAX_ATTEMPTS=25")
		}
	})

	t.Run("invalid duration", func(t *testing.T) {
		t.Setenv("FERRY_OUTBOX_POLL_INTERVAL", "not-a-duration")
		if _, err := LoadServerConfig(); err == nil {
			t.Fatal("expected error for invalid FERRY_OUTBOX_POLL_INTERVAL")
		}
	})

	t.Run("empty kafka brokers", func(t *testing.T) {
		t.Setenv("FERRY_KAFKA_BROKERS", " , ")
		if _, err := LoadServerConfig(); err == nil {
			t.Fatal("expected error for empty FERRY_KAFKA_BROKERS")
		}
	})
}

func TestLoadWorkerConfigDefaultsAndOverrides(t *testing.T) {
	t.Setenv("FERRY_WORKER_CONCURRENCY", "16")
	t.Setenv("FERRY_SHUTDOWN_DRAIN_TIMEOUT", "10s")

	cfg, err := LoadWorkerConfig()
	if err != nil {
		t.Fatalf("LoadWorkerConfig returned unexpected error: %v", err)
	}
	if cfg.WorkerConcurrency != 16 {
		t.Fatalf("expected WorkerConcurrency=16, got %d", cfg.WorkerConcurrency)
	}
	if cfg.ShutdownDrainTimeout != 10*time.Second {
		t.Fatalf("expected ShutdownDrainTimeout=10s, got %v", cfg.ShutdownDrainTimeout)
	}
	if cfg.MaxResponseSnippetBytes != 1024 {
		t.Fatalf("expected MaxResponseSnippetBytes=1024, got %d", cfg.MaxResponseSnippetBytes)
	}
}

func TestLoadWorkerConfigValidationErrors(t *testing.T) {
	t.Setenv("FERRY_WORKER_CONCURRENCY", "0")
	if _, err := LoadWorkerConfig(); err == nil {
		t.Fatal("expected error for FERRY_WORKER_CONCURRENCY=0")
	}
}

func TestLoadReceiverConfig(t *testing.T) {
	t.Setenv("FERRY_RECEIVER_DEFAULT_STATUS", "400")
	t.Setenv("FERRY_RECEIVER_DEFAULT_DELAY", "50ms")

	cfg, err := LoadReceiverConfig()
	if err != nil {
		t.Fatalf("LoadReceiverConfig returned unexpected error: %v", err)
	}
	if cfg.DefaultStatus != 400 || cfg.DefaultDelay != 50*time.Millisecond {
		t.Fatalf("unexpected receiver config: %+v", cfg)
	}

	t.Setenv("FERRY_RECEIVER_DEFAULT_STATUS", "999")
	if _, err := LoadReceiverConfig(); err == nil {
		t.Fatal("expected error for invalid FERRY_RECEIVER_DEFAULT_STATUS")
	}
}

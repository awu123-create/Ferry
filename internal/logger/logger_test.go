package logger

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	"gorm.io/gorm"
)

func TestNewLoggerInvalidLevel(t *testing.T) {
	if _, err := New("verbose"); err == nil {
		t.Fatal("expected error for invalid log level")
	}
	log, err := New("debug")
	if err != nil {
		t.Fatalf("unexpected error creating debug logger: %v", err)
	}
	_ = log.Sync()
}

func TestTaskIDContextRoundTrip(t *testing.T) {
	if _, ok := TaskIDFromContext(context.Background()); ok {
		t.Fatal("expected no task_id in bare context")
	}
	id := uuid.New()
	ctx := WithTaskID(context.Background(), id)
	got, ok := TaskIDFromContext(ctx)
	if !ok || got != id {
		t.Fatalf("expected %s, got %s (ok=%v)", id, got, ok)
	}
}

func TestGormLoggerTraceSlowSQLAndNotFoundIgnored(t *testing.T) {
	core, logs := observer.New(zapcore.DebugLevel)
	zlog := newWithCore(core)
	glog := NewGormLogger(zlog, 10*time.Millisecond)

	taskID := uuid.New()
	ctx := WithTaskID(context.Background(), taskID)

	glog.Trace(ctx, time.Now(), func() (string, int64) {
		return "SELECT * FROM tasks WHERE id = $1", 0
	}, gorm.ErrRecordNotFound)
	if logs.Len() != 0 {
		t.Fatalf("expected ErrRecordNotFound not to emit logs, got %d", logs.Len())
	}

	glog.Trace(ctx, time.Now().Add(-50*time.Millisecond), func() (string, int64) {
		return "SELECT * FROM tasks", 5
	}, nil)
	if logs.Len() != 1 {
		t.Fatalf("expected 1 slow sql log entry, got %d", logs.Len())
	}
	entry := logs.All()[0]
	if entry.Level != zapcore.WarnLevel {
		t.Fatalf("expected warn level, got %v", entry.Level)
	}
	if entry.ContextMap()["task_id"] != taskID.String() {
		t.Fatalf("expected task_id=%s in log fields, got %v", taskID, entry.ContextMap()["task_id"])
	}

	glog.Trace(ctx, time.Now(), func() (string, int64) {
		return "INSERT INTO outbox ...", 0
	}, errors.New("boom"))
	if logs.Len() != 2 {
		t.Fatalf("expected 2 log entries, got %d", logs.Len())
	}
	if logs.All()[1].Level != zapcore.ErrorLevel {
		t.Fatalf("expected error level on query error, got %v", logs.All()[1].Level)
	}

	_ = zap.NewNop()
}

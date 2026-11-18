package logger

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type taskIDContextKey struct{}

// New builds a sampled JSON logger writing to stdout.
func New(level string) (*zap.Logger, error) {
	parsedLevel, err := parseLevel(level)
	if err != nil {
		return nil, err
	}

	encoderCfg := zap.NewProductionEncoderConfig()
	encoderCfg.TimeKey = "ts"
	encoderCfg.EncodeTime = zapcore.ISO8601TimeEncoder
	encoderCfg.EncodeLevel = zapcore.LowercaseLevelEncoder

	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderCfg),
		zapcore.Lock(os.Stdout),
		parsedLevel,
	)
	return newWithCore(core), nil
}

func newWithCore(core zapcore.Core) *zap.Logger {
	sampled := zapcore.NewSamplerWithOptions(core, time.Second, 100, 10)
	return zap.New(sampled, zap.AddCaller())
}

func parseLevel(level string) (zapcore.Level, error) {
	normalized := strings.ToLower(strings.TrimSpace(level))
	if normalized == "" {
		return zapcore.InfoLevel, nil
	}
	var lvl zapcore.Level
	if err := lvl.UnmarshalText([]byte(normalized)); err != nil {
		return zapcore.InfoLevel, fmt.Errorf("parse log level %q: %w", level, err)
	}
	return lvl, nil
}

// WithTaskID attaches a task identifier to ctx for downstream SQL and boundary logs.
func WithTaskID(ctx context.Context, taskID uuid.UUID) context.Context {
	return context.WithValue(ctx, taskIDContextKey{}, taskID)
}

// TaskIDFromContext retrieves an attached task identifier from ctx if present.
func TaskIDFromContext(ctx context.Context) (uuid.UUID, bool) {
	if ctx == nil {
		return uuid.Nil, false
	}
	id, ok := ctx.Value(taskIDContextKey{}).(uuid.UUID)
	if !ok || id == uuid.Nil {
		return uuid.Nil, false
	}
	return id, true
}

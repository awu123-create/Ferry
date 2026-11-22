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

// New 构建输出到标准输出、带采样限流的 JSON 日志记录器。
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
	sampled := zapcore.NewSamplerWithOptions(core, time.Second, 100, 10)
	return zap.New(sampled, zap.AddCaller()), nil
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

// WithTaskID 将任务 ID 绑定到 ctx，供下游 SQL 日志与边界日志透传。
func WithTaskID(ctx context.Context, taskID uuid.UUID) context.Context {
	return context.WithValue(ctx, taskIDContextKey{}, taskID)
}

// TaskIDFromContext 从 ctx 中读取已绑定的任务 ID（如存在）。
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

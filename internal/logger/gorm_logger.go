package logger

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"
)

// GormLogger adapts zap.Logger to GORM's logger interface with slow SQL warnings
// and task_id context correlation.
type GormLogger struct {
	log           *zap.Logger
	slowThreshold time.Duration
	level         gormlogger.LogLevel
}

// NewGormLogger constructs a GORM logger adapter.
func NewGormLogger(log *zap.Logger, slowThreshold time.Duration) *GormLogger {
	if log == nil {
		log = zap.NewNop()
	}
	return &GormLogger{
		log:           log,
		slowThreshold: slowThreshold,
		level:         gormlogger.Warn,
	}
}

// LogMode returns a copy of the logger configured with the requested level.
func (l *GormLogger) LogMode(level gormlogger.LogLevel) gormlogger.Interface {
	cp := *l
	cp.level = level
	return &cp
}

// Info emits informational GORM messages when Info level is enabled.
func (l *GormLogger) Info(ctx context.Context, msg string, data ...any) {
	if l.level < gormlogger.Info {
		return
	}
	l.log.Info(fmt.Sprintf(msg, data...), contextFields(ctx)...)
}

// Warn emits warning GORM messages when Warn level is enabled.
func (l *GormLogger) Warn(ctx context.Context, msg string, data ...any) {
	if l.level < gormlogger.Warn {
		return
	}
	l.log.Warn(fmt.Sprintf(msg, data...), contextFields(ctx)...)
}

// Error emits error GORM messages when Error level is enabled.
func (l *GormLogger) Error(ctx context.Context, msg string, data ...any) {
	if l.level < gormlogger.Error {
		return
	}
	l.log.Error(fmt.Sprintf(msg, data...), contextFields(ctx)...)
}

// Trace logs SQL execution errors (excluding gorm.ErrRecordNotFound) and slow queries.
func (l *GormLogger) Trace(ctx context.Context, begin time.Time, fc func() (string, int64), err error) {
	if l.level <= gormlogger.Silent {
		return
	}

	elapsed := time.Since(begin)
	sql, rows := fc()
	fields := append(
		contextFields(ctx),
		zap.Duration("elapsed", elapsed),
		zap.Int64("rows", rows),
		zap.String("sql", sql),
	)

	switch {
	case err != nil && !errors.Is(err, gorm.ErrRecordNotFound) && l.level >= gormlogger.Error:
		l.log.Error("sql query failed", append(fields, zap.Error(err))...)
	case l.slowThreshold > 0 && elapsed > l.slowThreshold && l.level >= gormlogger.Warn:
		l.log.Warn("slow sql query", fields...)
	case l.level >= gormlogger.Info:
		l.log.Debug("sql query executed", fields...)
	}
}

func contextFields(ctx context.Context) []zap.Field {
	taskID, ok := TaskIDFromContext(ctx)
	if !ok {
		return nil
	}
	return []zap.Field{zap.String("task_id", taskID.String())}
}

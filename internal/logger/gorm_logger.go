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

// GormLogger 将 zap.Logger 适配为 GORM 日志接口，支持慢 SQL 告警与 task_id 透传。
type GormLogger struct {
	log           *zap.Logger
	slowThreshold time.Duration
	level         gormlogger.LogLevel
}

// NewGormLogger 构造 GORM 日志适配器。
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

// LogMode 返回按指定日志级别复制的日志适配器副本。
func (l *GormLogger) LogMode(level gormlogger.LogLevel) gormlogger.Interface {
	cp := *l
	cp.level = level
	return &cp
}

// Info 在开启 Info 级别时输出 GORM 信息日志。
func (l *GormLogger) Info(ctx context.Context, msg string, data ...any) {
	if l.level < gormlogger.Info {
		return
	}
	l.log.Info(fmt.Sprintf(msg, data...), contextFields(ctx)...)
}

// Warn 在开启 Warn 级别时输出 GORM 告警日志。
func (l *GormLogger) Warn(ctx context.Context, msg string, data ...any) {
	if l.level < gormlogger.Warn {
		return
	}
	l.log.Warn(fmt.Sprintf(msg, data...), contextFields(ctx)...)
}

// Error 在开启 Error 级别时输出 GORM 错误日志。
func (l *GormLogger) Error(ctx context.Context, msg string, data ...any) {
	if l.level < gormlogger.Error {
		return
	}
	l.log.Error(fmt.Sprintf(msg, data...), contextFields(ctx)...)
}

// Trace 记录 SQL 执行错误（忽略 gorm.ErrRecordNotFound）与超过阈值的慢查询。
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

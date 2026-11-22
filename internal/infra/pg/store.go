package pg

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/pressly/goose/v3"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"

	"github.com/awu123-create/ferry/migrations"
)

// Store 封装 PostgreSQL 持久化所需的 GORM 句柄与底层 sql.DB 连接池。
type Store struct {
	db         *gorm.DB
	sqlDB      *sql.DB
	listenMu   sync.Mutex
	listenConn *sql.Conn
}

// Open 通过 GORM 连接 PostgreSQL、配置连接池参数并校验连通性。
func Open(ctx context.Context, dsn string, gormLog gormlogger.Interface) (*Store, error) {
	if gormLog == nil {
		gormLog = gormlogger.Default.LogMode(gormlogger.Silent)
	}

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		SkipDefaultTransaction: true,
		PrepareStmt:            true,
		Logger:                 gormLog,
	})
	if err != nil {
		return nil, fmt.Errorf("open postgres connection: %w", err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("extract sql.DB handle: %w", err)
	}
	sqlDB.SetMaxOpenConns(50)
	sqlDB.SetMaxIdleConns(10)
	sqlDB.SetConnMaxLifetime(30 * time.Minute)

	if err := sqlDB.PingContext(ctx); err != nil {
		_ = sqlDB.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}

	return &Store{db: db, sqlDB: sqlDB}, nil
}

// RunMigrations 将内嵌的 Goose SQL 迁移脚本应用到 PostgreSQL。
func (s *Store) RunMigrations(ctx context.Context) error {
	provider, err := goose.NewProvider(goose.DialectPostgres, s.sqlDB, migrations.FS)
	if err != nil {
		return fmt.Errorf("create goose migration provider: %w", err)
	}
	if _, err := provider.Up(ctx); err != nil {
		return fmt.Errorf("apply postgres migrations: %w", err)
	}
	return nil
}

// Ping 在 ctx 超时范围内校验 PostgreSQL 可用性。
func (s *Store) Ping(ctx context.Context) error {
	return s.sqlDB.PingContext(ctx)
}

// Close 释放专用监听连接与全部数据库连接池资源。
func (s *Store) Close() error {
	if s == nil {
		return nil
	}
	s.closeOutboxListener()
	if s.sqlDB == nil {
		return nil
	}
	return s.sqlDB.Close()
}

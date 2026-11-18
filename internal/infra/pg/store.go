package pg

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/pressly/goose/v3"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"

	"github.com/awu123-create/ferry/migrations"
)

// Store wraps the GORM handle and underlying sql.DB pool for PostgreSQL persistence.
type Store struct {
	db    *gorm.DB
	sqlDB *sql.DB
}

// Open connects to PostgreSQL via GORM, tunes connection pooling, and verifies connectivity.
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

// RunMigrations applies all embedded Goose SQL migrations against PostgreSQL.
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

// Ping verifies PostgreSQL readiness within ctx.
func (s *Store) Ping(ctx context.Context) error {
	return s.sqlDB.PingContext(ctx)
}

// Close releases all pooled database connections.
func (s *Store) Close() error {
	if s == nil || s.sqlDB == nil {
		return nil
	}
	return s.sqlDB.Close()
}

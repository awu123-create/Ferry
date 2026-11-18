package redis

import (
	"context"
	"fmt"
	"strings"

	goredis "github.com/redis/go-redis/v9"
)

// Client wraps go-redis/v9 lifecycle and exposes the client for Redis adapters.
type Client struct {
	rdb *goredis.Client
}

// New connects to Redis and verifies availability with a PING command.
func New(ctx context.Context, addr, password string, db int) (*Client, error) {
	if strings.TrimSpace(addr) == "" {
		return nil, fmt.Errorf("redis address must not be empty")
	}

	rdb := goredis.NewClient(&goredis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})
	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		return nil, fmt.Errorf("ping redis: %w", err)
	}
	return &Client{rdb: rdb}, nil
}

// Ping verifies Redis readiness within ctx.
func (c *Client) Ping(ctx context.Context) error {
	return c.rdb.Ping(ctx).Err()
}

// Raw returns the underlying go-redis client for Milestone 2/3 Lua adapters.
func (c *Client) Raw() *goredis.Client {
	return c.rdb
}

// Close closes the underlying Redis connection pool.
func (c *Client) Close() error {
	if c == nil || c.rdb == nil {
		return nil
	}
	return c.rdb.Close()
}

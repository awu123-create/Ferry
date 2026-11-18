package redis

import (
	"context"
	"fmt"
	"strings"

	goredis "github.com/redis/go-redis/v9"
)

// Client 封装 go-redis/v9 连接生命周期，并向上层适配器暴露客户端实例。
type Client struct {
	rdb *goredis.Client
}

// New 连接 Redis 并执行 PING 验证可用性。
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

// Ping 在 ctx 超时范围内校验 Redis 可用性。
func (c *Client) Ping(ctx context.Context) error {
	return c.rdb.Ping(ctx).Err()
}

// Raw 返回底层 go-redis 客户端，供 Milestone 2/3 Lua 脚本适配器使用。
func (c *Client) Raw() *goredis.Client {
	return c.rdb
}

// Close 关闭底层 Redis 连接池。
func (c *Client) Close() error {
	if c == nil || c.rdb == nil {
		return nil
	}
	return c.rdb.Close()
}

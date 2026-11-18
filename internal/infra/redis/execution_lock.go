package redis

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/awu123-create/ferry/internal/domain"
)

// AcquireExecutionLock attempts an exclusive short-lived Execution Lock for taskID.
// Key format: ferry:lock:task:<task_id>
//
// Implementation notes (Milestone 1 - core logic to be implemented, ADR-0008 §3):
//  1. Require ttl > 0.
//  2. Generate a cryptographically random or UUIDv7 token string so only the lock holder can release it.
//  3. Execute SetNX(ctx, executionLockKey(taskID), token, ttl).Result().
//  4. Return (token, true, nil) when acquired, or ("", false, nil) when already held by another caller.
func (c *Client) AcquireExecutionLock(ctx context.Context, taskID uuid.UUID, ttl time.Duration) (string, bool, error) {
	_ = ctx
	_ = taskID
	_ = ttl
	return "", false, domain.ErrNotImplemented
}

// ReleaseExecutionLock releases the Execution Lock only if the stored value matches token.
//
// Implementation notes (Milestone 1 - core logic to be implemented, ADR-0008 §3):
//   - Use a redis.NewScript Lua script that atomically runs:
//     if redis.call("GET", KEYS[1]) == ARGV[1] then return redis.call("DEL", KEYS[1]) else return 0 end
//   - Never run an unconditional DEL so an expired lock belonging to another holder is never deleted.
func (c *Client) ReleaseExecutionLock(ctx context.Context, taskID uuid.UUID, token string) error {
	_ = ctx
	_ = taskID
	_ = token
	return domain.ErrNotImplemented
}

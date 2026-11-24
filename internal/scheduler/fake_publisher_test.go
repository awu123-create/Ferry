package scheduler_test

import (
	"context"
	"sync"

	"github.com/google/uuid"
)

type fakeReadyPublisher struct {
	mu          sync.Mutex
	published   []uuid.UUID
	failErrs    []error
	hasDeadline bool
	onPublish   func(taskIDs []uuid.UUID, total int) error
}

func (f *fakeReadyPublisher) PublishTaskIDs(ctx context.Context, taskIDs []uuid.UUID) error {
	_, hasDeadline := ctx.Deadline()
	f.mu.Lock()
	f.hasDeadline = hasDeadline
	if len(f.failErrs) > 0 {
		err := f.failErrs[0]
		f.failErrs = f.failErrs[1:]
		cb := f.onPublish
		total := len(f.published)
		f.mu.Unlock()
		if cb != nil {
			_ = cb(taskIDs, total)
		}
		return err
	}
	f.published = append(f.published, taskIDs...)
	total := len(f.published)
	cb := f.onPublish
	f.mu.Unlock()
	if cb != nil {
		return cb(taskIDs, total)
	}
	return nil
}

func (f *fakeReadyPublisher) snapshot() []uuid.UUID {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]uuid.UUID, len(f.published))
	copy(out, f.published)
	return out
}

func (f *fakeReadyPublisher) lastHasDeadline() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.hasDeadline
}

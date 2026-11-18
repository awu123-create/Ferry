package kafka

import (
	"testing"

	"github.com/google/uuid"

	"github.com/awu123-create/ferry/internal/domain"
)

func TestOffsetTrackerContiguousWatermarks(t *testing.T) {
	tracker := newOffsetTracker()
	topic := "ferry.tasks.ready"

	t10 := domain.ReadyTicket{TaskID: uuid.New(), Topic: topic, Partition: 0, Offset: 10, Epoch: 1}
	t11 := domain.ReadyTicket{TaskID: uuid.New(), Topic: topic, Partition: 0, Offset: 11, Epoch: 1}
	t12 := domain.ReadyTicket{TaskID: uuid.New(), Topic: topic, Partition: 0, Offset: 12, Epoch: 1}

	tracker.recordPolled([]domain.ReadyTicket{t10, t11, t12})

	tracker.markDone(t11)
	if got := tracker.takeCommittable(); len(got) != 0 {
		t.Fatalf("expected no committable offset while offset 10 is still in flight, got %+v", got)
	}

	tracker.markDone(t12)
	if got := tracker.takeCommittable(); len(got) != 0 {
		t.Fatalf("expected no committable offset until offset 10 finishes, got %+v", got)
	}

	tracker.markDone(t10)
	got := tracker.takeCommittable()
	eo, ok := got[topic][0]
	if !ok || eo.Offset != 13 || eo.Epoch != 1 {
		t.Fatalf("expected contiguous watermark 13 (epoch 1), got %+v", got)
	}

	// Simulate commit failure followed by a newer watermark advancing.
	tracker.restoreCommittable(got)
	t13 := domain.ReadyTicket{TaskID: uuid.New(), Topic: topic, Partition: 0, Offset: 13, Epoch: 2}
	tracker.recordPolled([]domain.ReadyTicket{t13})
	tracker.markDone(t13)

	next := tracker.takeCommittable()
	if next[topic][0].Offset != 14 || next[topic][0].Epoch != 2 {
		t.Fatalf("expected watermark 14 (epoch 2), got %+v", next)
	}
}

func TestOffsetTrackerRevokePartitions(t *testing.T) {
	tracker := newOffsetTracker()
	topic := "ferry.tasks.ready"

	p0 := domain.ReadyTicket{TaskID: uuid.New(), Topic: topic, Partition: 0, Offset: 5, Epoch: 1}
	p1 := domain.ReadyTicket{TaskID: uuid.New(), Topic: topic, Partition: 1, Offset: 8, Epoch: 1}
	tracker.recordPolled([]domain.ReadyTicket{p0, p1})
	tracker.markDone(p0)
	tracker.markDone(p1)

	revoked := tracker.revokePartitions(map[string][]int32{topic: {0}})
	if revoked[topic][0].Offset != 6 {
		t.Fatalf("expected revoked partition 0 offset=6, got %+v", revoked)
	}
	remaining := tracker.takeCommittable()
	if remaining[topic][1].Offset != 9 || len(remaining[topic]) != 1 {
		t.Fatalf("expected only partition 1 remaining, got %+v", remaining)
	}
}

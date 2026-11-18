package kafka

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/awu123-create/ferry/internal/domain"
)

// ReadyReader 从 Kafka 拉取待投递 Task 票据，并仅按分区连续完成水位提交 Offset。
type ReadyReader struct {
	client  *kgo.Client
	tracker *offsetTracker
}

// NewReadyReader 构造禁用自动提交、按连续完成水位手动提交的 Kafka 消费组读取器。
func NewReadyReader(brokers []string, topic, groupID string) (*ReadyReader, error) {
	if len(brokers) == 0 {
		return nil, fmt.Errorf("kafka reader requires at least one broker")
	}
	trimmedTopic := strings.TrimSpace(topic)
	if trimmedTopic == "" {
		return nil, fmt.Errorf("kafka reader requires a topic name")
	}
	trimmedGroup := strings.TrimSpace(groupID)
	if trimmedGroup == "" {
		return nil, fmt.Errorf("kafka reader requires a group id")
	}

	reader := &ReadyReader{
		tracker: newOffsetTracker(),
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(trimmedGroup),
		kgo.ConsumeTopics(trimmedTopic),
		kgo.Balancers(kgo.CooperativeStickyBalancer()),
		kgo.DisableAutoCommit(),
		kgo.OnPartitionsRevoked(reader.onPartitionsRevoked),
		kgo.OnPartitionsLost(reader.onPartitionsLost),
	)
	if err != nil {
		return nil, fmt.Errorf("create kafka reader: %w", err)
	}
	reader.client = client
	return reader, nil
}

// PollReady 从 Kafka 拉取最多 maxRecords 条票据，并将其偏移量登记到水位追踪器中。
func (r *ReadyReader) PollReady(ctx context.Context, maxRecords int) ([]domain.ReadyTicket, error) {
	if maxRecords <= 0 {
		maxRecords = 1
	}
	fetches := r.client.PollRecords(ctx, maxRecords)
	if err := fetches.Err0(); err != nil && fetches.Empty() {
		return nil, err
	}

	var tickets []domain.ReadyTicket
	var malformed []domain.ReadyTicket
	fetches.EachRecord(func(rec *kgo.Record) {
		ticket := domain.ReadyTicket{
			Topic:     rec.Topic,
			Partition: rec.Partition,
			Offset:    rec.Offset,
			Epoch:     rec.LeaderEpoch,
		}
		taskID, err := parseTaskUUID(rec)
		if err != nil {
			malformed = append(malformed, ticket)
			return
		}
		ticket.TaskID = taskID
		tickets = append(tickets, ticket)
	})

	if len(tickets) > 0 || len(malformed) > 0 {
		all := make([]domain.ReadyTicket, 0, len(tickets)+len(malformed))
		all = append(all, tickets...)
		all = append(all, malformed...)
		r.tracker.recordPolled(all)
		for _, bad := range malformed {
			r.tracker.markDone(bad)
		}
	}

	if err := fetches.Err0(); err != nil && len(tickets) == 0 {
		return nil, err
	}
	return tickets, nil
}

// Ack 将指定票据标记为已完成；当同分区更早偏移量也全部完成后，提交水位随之推进。
func (r *ReadyReader) Ack(ticket domain.ReadyTicket) {
	r.tracker.markDone(ticket)
}

// CommitCompleted 同步提交所有已分配分区上连续完成的 Offset 水位。
func (r *ReadyReader) CommitCompleted(ctx context.Context) error {
	offsets := r.tracker.takeCommittable()
	if len(offsets) == 0 {
		return nil
	}
	if err := commitOffsetsSync(ctx, r.client, offsets); err != nil {
		r.tracker.restoreCommittable(offsets)
		return err
	}
	return nil
}

// Ping 在 ctx 超时范围内校验 Kafka Broker 连通性。
func (r *ReadyReader) Ping(ctx context.Context) error {
	return r.client.Ping(ctx)
}

// Close 关闭底层 Kafka 客户端连接。
func (r *ReadyReader) Close() {
	if r != nil && r.client != nil {
		r.client.Close()
	}
}

func (r *ReadyReader) onPartitionsRevoked(ctx context.Context, client *kgo.Client, revoked map[string][]int32) {
	offsets := r.tracker.revokePartitions(revoked)
	if len(offsets) == 0 {
		return
	}
	_ = commitOffsetsSync(ctx, client, offsets)
}

func (r *ReadyReader) onPartitionsLost(_ context.Context, _ *kgo.Client, lost map[string][]int32) {
	_ = r.tracker.revokePartitions(lost)
}

func commitOffsetsSync(
	ctx context.Context,
	client *kgo.Client,
	offsets map[string]map[int32]kgo.EpochOffset,
) error {
	var commitErr error
	client.CommitOffsetsSync(ctx, offsets, func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest, resp *kmsg.OffsetCommitResponse, err error) {
		if err != nil {
			commitErr = fmt.Errorf("commit kafka offsets: %w", err)
			return
		}
		if resp == nil {
			return
		}
		for _, topic := range resp.Topics {
			for _, part := range topic.Partitions {
				if partErr := kerr.ErrorForCode(part.ErrorCode); partErr != nil {
					commitErr = fmt.Errorf("commit offset %s[%d]: %w", topic.Topic, part.Partition, partErr)
					return
				}
			}
		}
	})
	return commitErr
}

func parseTaskUUID(rec *kgo.Record) (uuid.UUID, error) {
	if len(rec.Value) > 0 {
		if id, err := uuid.ParseBytes(rec.Value); err == nil {
			return id, nil
		}
	}
	if len(rec.Key) > 0 {
		return uuid.ParseBytes(rec.Key)
	}
	return uuid.Nil, fmt.Errorf("empty task id record at %s[%d]@%d", rec.Topic, rec.Partition, rec.Offset)
}

type topicPartition struct {
	topic     string
	partition int32
}

type trackedEntry struct {
	offset int64
	epoch  int32
	done   bool
}

type offsetTracker struct {
	mu          sync.Mutex
	inFlight    map[topicPartition][]trackedEntry
	committable map[string]map[int32]kgo.EpochOffset
}

func newOffsetTracker() *offsetTracker {
	return &offsetTracker{
		inFlight:    make(map[topicPartition][]trackedEntry),
		committable: make(map[string]map[int32]kgo.EpochOffset),
	}
}

func (t *offsetTracker) recordPolled(tickets []domain.ReadyTicket) {
	t.mu.Lock()
	defer t.mu.Unlock()

	for _, ticket := range tickets {
		tp := topicPartition{topic: ticket.Topic, partition: ticket.Partition}
		queue := t.inFlight[tp]
		if n := len(queue); n > 0 && ticket.Offset <= queue[n-1].offset {
			// 发生重平衡后，该分区可能从更早的未提交偏移量重新拉取，此处清理重叠或后续残留条目。
			filtered := queue[:0]
			for _, item := range queue {
				if item.offset < ticket.Offset {
					filtered = append(filtered, item)
				}
			}
			queue = filtered
		}
		t.inFlight[tp] = append(queue, trackedEntry{
			offset: ticket.Offset,
			epoch:  ticket.Epoch,
		})
	}
}

func (t *offsetTracker) markDone(ticket domain.ReadyTicket) {
	t.mu.Lock()
	defer t.mu.Unlock()

	tp := topicPartition{topic: ticket.Topic, partition: ticket.Partition}
	queue := t.inFlight[tp]
	if len(queue) == 0 {
		return
	}
	for i := range queue {
		if queue[i].offset == ticket.Offset {
			queue[i].done = true
			break
		}
	}

	advanced := 0
	var lastDone trackedEntry
	for advanced < len(queue) && queue[advanced].done {
		lastDone = queue[advanced]
		advanced++
	}
	if advanced == 0 {
		t.inFlight[tp] = queue
		return
	}

	remaining := queue[advanced:]
	if len(remaining) == 0 {
		delete(t.inFlight, tp)
	} else {
		t.inFlight[tp] = append([]trackedEntry(nil), remaining...)
	}

	partMap, ok := t.committable[ticket.Topic]
	if !ok {
		partMap = make(map[int32]kgo.EpochOffset)
		t.committable[ticket.Topic] = partMap
	}
	nextCommit := kgo.EpochOffset{Offset: lastDone.offset + 1, Epoch: lastDone.epoch}
	if existing, exists := partMap[ticket.Partition]; !exists || existing.Offset < nextCommit.Offset {
		partMap[ticket.Partition] = nextCommit
	}
}

func (t *offsetTracker) takeCommittable() map[string]map[int32]kgo.EpochOffset {
	t.mu.Lock()
	defer t.mu.Unlock()

	if len(t.committable) == 0 {
		return nil
	}
	out := t.committable
	t.committable = make(map[string]map[int32]kgo.EpochOffset)
	return out
}

func (t *offsetTracker) restoreCommittable(failed map[string]map[int32]kgo.EpochOffset) {
	t.mu.Lock()
	defer t.mu.Unlock()

	for topic, parts := range failed {
		partMap, ok := t.committable[topic]
		if !ok {
			partMap = make(map[int32]kgo.EpochOffset, len(parts))
			t.committable[topic] = partMap
		}
		for part, eo := range parts {
			if existing, exists := partMap[part]; !exists || existing.Offset < eo.Offset {
				partMap[part] = eo
			}
		}
	}
}

func (t *offsetTracker) revokePartitions(revoked map[string][]int32) map[string]map[int32]kgo.EpochOffset {
	t.mu.Lock()
	defer t.mu.Unlock()

	out := make(map[string]map[int32]kgo.EpochOffset)
	for topic, parts := range revoked {
		for _, part := range parts {
			delete(t.inFlight, topicPartition{topic: topic, partition: part})
			if partMap, ok := t.committable[topic]; ok {
				if eo, exists := partMap[part]; exists {
					if out[topic] == nil {
						out[topic] = make(map[int32]kgo.EpochOffset)
					}
					out[topic][part] = eo
					delete(partMap, part)
				}
				if len(partMap) == 0 {
					delete(t.committable, topic)
				}
			}
		}
	}
	return out
}

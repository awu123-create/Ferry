package kafka

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// ReadyProducer publishes due Task IDs onto Kafka using task_id as the fixed partition key.
type ReadyProducer struct {
	client *kgo.Client
	topic  string
}

// NewReadyProducer constructs an idempotent Kafka producer configured with AllISRAcks.
func NewReadyProducer(brokers []string, topic string) (*ReadyProducer, error) {
	if len(brokers) == 0 {
		return nil, fmt.Errorf("kafka producer requires at least one broker")
	}
	trimmedTopic := strings.TrimSpace(topic)
	if trimmedTopic == "" {
		return nil, fmt.Errorf("kafka producer requires a topic name")
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)),
	)
	if err != nil {
		return nil, fmt.Errorf("create kafka producer: %w", err)
	}
	return &ReadyProducer{client: client, topic: trimmedTopic}, nil
}

// EnsureTopic creates the ready topic if it does not already exist.
func (p *ReadyProducer) EnsureTopic(ctx context.Context, partitions int32) error {
	if partitions <= 0 {
		return fmt.Errorf("topic partitions must be > 0")
	}
	adm := kadm.NewClient(p.client)
	_, err := adm.CreateTopic(ctx, partitions, 1, nil, p.topic)
	if err != nil && !errors.Is(err, kerr.TopicAlreadyExists) {
		return fmt.Errorf("ensure topic %q: %w", p.topic, err)
	}
	return nil
}

// PublishTaskIDs synchronously produces a batch of Task IDs to Kafka with task_id partitioning.
func (p *ReadyProducer) PublishTaskIDs(ctx context.Context, taskIDs []uuid.UUID) error {
	if len(taskIDs) == 0 {
		return nil
	}
	records := make([]*kgo.Record, 0, len(taskIDs))
	for _, id := range taskIDs {
		raw := []byte(id.String())
		records = append(records, &kgo.Record{
			Topic: p.topic,
			Key:   raw,
			Value: raw,
		})
	}
	if err := p.client.ProduceSync(ctx, records...).FirstErr(); err != nil {
		return fmt.Errorf("produce ready tasks: %w", err)
	}
	return nil
}

// Ping verifies broker connectivity within ctx.
func (p *ReadyProducer) Ping(ctx context.Context) error {
	return p.client.Ping(ctx)
}

// Close flushes pending buffered records and closes the underlying Kafka client.
func (p *ReadyProducer) Close() {
	if p != nil && p.client != nil {
		p.client.Close()
	}
}

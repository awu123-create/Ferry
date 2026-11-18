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

// ReadyProducer 将就绪状态的 Task ID 发送到 Kafka，并固定以 task_id 作为分区键。
type ReadyProducer struct {
	client *kgo.Client
	topic  string
}

// NewReadyProducer 构造启用 AllISRAcks 的幂等 Kafka 生产者。
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

// EnsureTopic 若就绪 Topic 尚未创建，则自动按指定分区数创建该 Topic。
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

// PublishTaskIDs 以 task_id 为分区键同步批量发布一组 Task ID 至 Kafka。
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

// Ping 在 ctx 超时范围内校验 Kafka Broker 连通性。
func (p *ReadyProducer) Ping(ctx context.Context) error {
	return p.client.Ping(ctx)
}

// Close 刷新缓冲中的待发送消息并关闭底层 Kafka 客户端。
func (p *ReadyProducer) Close() {
	if p != nil && p.client != nil {
		p.client.Close()
	}
}

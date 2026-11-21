package kafka_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/awu123-create/ferry/internal/domain"
	"github.com/awu123-create/ferry/internal/infra/kafka"
)

func testKafkaBrokers(t *testing.T) []string {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping real Kafka test in -short mode")
	}

	raw := strings.TrimSpace(os.Getenv("FERRY_TEST_KAFKA_BROKERS"))
	if raw == "" {
		return []string{"localhost:9092"}
	}
	var out []string
	for _, part := range strings.Split(raw, ",") {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	if len(out) == 0 {
		t.Fatal("FERRY_TEST_KAFKA_BROKERS parsed empty broker list")
	}
	return out
}

func deleteTestTopic(brokers []string, topic string) {
	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _ = kadm.NewClient(client).DeleteTopics(ctx, topic)
}

func pollUntilCount(t *testing.T, reader *kafka.ReadyReader, want int, timeout time.Duration) []domain.ReadyDelivery {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var out []domain.ReadyDelivery
	for len(out) < want && time.Now().Before(deadline) {
		pollCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		batch, err := reader.PollReady(pollCtx, want-len(out))
		cancel()
		if err != nil && pollCtx.Err() == nil {
			t.Fatalf("PollReady failed: %v", err)
		}
		out = append(out, batch...)
	}
	if len(out) < want {
		t.Fatalf("timed out waiting for %d kafka deliveries, got %d", want, len(out))
	}
	return out
}

func TestReadyStream_OutOfOrderAckContiguousWatermarkAndRestart(t *testing.T) {
	brokers := testKafkaBrokers(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	topic := fmt.Sprintf("ferry.test.ready.%s", uuid.NewString()[:8])
	groupID := fmt.Sprintf("ferry-test-group-%s", uuid.NewString()[:8])

	producer, err := kafka.NewReadyProducer(brokers, topic)
	if err != nil {
		t.Fatalf("NewReadyProducer failed: %v", err)
	}
	t.Cleanup(func() {
		producer.Close()
		deleteTestTopic(brokers, topic)
	})

	if err := producer.Ping(ctx); err != nil {
		t.Fatalf("connect test kafka %v (run `make up` first): %v", brokers, err)
	}
	if err := producer.EnsureTopic(ctx, 1); err != nil {
		t.Fatalf("EnsureTopic failed: %v", err)
	}

	id0, id1, id2 := uuid.New(), uuid.New(), uuid.New()
	if err := producer.PublishTaskIDs(ctx, []uuid.UUID{id0, id1, id2}); err != nil {
		t.Fatalf("PublishTaskIDs failed: %v", err)
	}

	// 1. Reader #1 收到全部 3 条，但故意只确认第 2、3 条，留第 1 条未确认；提交后水位不得越过 offset 0。
	reader1, err := kafka.NewReadyReader(brokers, topic, groupID)
	if err != nil {
		t.Fatalf("NewReadyReader 1 failed: %v", err)
	}
	batch1 := pollUntilCount(t, reader1, 3, 15*time.Second)
	if batch1[0].TaskID != id0 || batch1[1].TaskID != id1 || batch1[2].TaskID != id2 {
		t.Fatalf("unexpected delivery order: %v, %v, %v", batch1[0].TaskID, batch1[1].TaskID, batch1[2].TaskID)
	}
	batch1[1].Ack()
	batch1[2].Ack()
	if err := reader1.CommitCompleted(ctx); err != nil {
		t.Fatalf("CommitCompleted 1 failed: %v", err)
	}
	reader1.Close()

	// 2. Reader #2 同消费组重启，应从 offset 0 重放全部 3 条；此次只确认第 1 条并提交，水位推进到 offset 1。
	reader2, err := kafka.NewReadyReader(brokers, topic, groupID)
	if err != nil {
		t.Fatalf("NewReadyReader 2 failed: %v", err)
	}
	batch2 := pollUntilCount(t, reader2, 3, 15*time.Second)
	if batch2[0].TaskID != id0 {
		t.Fatalf("expected uncommitted offset 0 (%s) redelivered, got %s", id0, batch2[0].TaskID)
	}
	batch2[0].Ack()
	if err := reader2.CommitCompleted(ctx); err != nil {
		t.Fatalf("CommitCompleted 2 failed: %v", err)
	}
	reader2.Close()

	// 3. Reader #3 再次重启，应跳过已提交的 id0，仅消费 id1 与 id2。
	reader3, err := kafka.NewReadyReader(brokers, topic, groupID)
	if err != nil {
		t.Fatalf("NewReadyReader 3 failed: %v", err)
	}
	defer reader3.Close()

	batch3 := pollUntilCount(t, reader3, 2, 15*time.Second)
	if batch3[0].TaskID != id1 || batch3[1].TaskID != id2 {
		t.Fatalf("expected remaining deliveries (%s, %s), got (%s, %s)", id1, id2, batch3[0].TaskID, batch3[1].TaskID)
	}
	batch3[0].Ack()
	batch3[1].Ack()
	if err := reader3.CommitCompleted(ctx); err != nil {
		t.Fatalf("CommitCompleted 3 failed: %v", err)
	}
}

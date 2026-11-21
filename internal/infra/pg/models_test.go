package pg

import (
	"bytes"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/awu123-create/ferry/internal/domain"
)

func TestPOMappingRoundTrip(t *testing.T) {
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	completed := now.Add(time.Second)
	rawPayload := []byte(`{"b":1,  "a":"x"}`)

	task := domain.Task{
		ID:             uuid.New(),
		EndpointID:     uuid.New(),
		IdempotencyKey: "order-123",
		Payload:        rawPayload,
		PayloadSHA256:  domain.ComputePayloadSHA256(rawPayload),
		Headers:        map[string]string{"X-Trace": "abc"},
		Status:         domain.TaskStatusSucceeded,
		Attempt:        1,
		MaxAttempts:    1,
		NextRunAt:      now,
		DeadlineAt:     now.Add(24 * time.Hour),
		ReplayCount:    0,
		CreatedAt:      now,
		UpdatedAt:      completed,
		CompletedAt:    &completed,
	}

	po := toTaskPO(task)
	if po.TableName() != "tasks" {
		t.Fatalf("unexpected table name %q", po.TableName())
	}
	roundTrip, err := toDomainTask(po)
	if err != nil {
		t.Fatalf("toDomainTask failed: %v", err)
	}
	if !bytes.Equal(roundTrip.Payload, rawPayload) {
		t.Fatalf("expected raw payload bytes preserved, got %q", roundTrip.Payload)
	}
	if roundTrip.Headers["X-Trace"] != "abc" || roundTrip.Status != domain.TaskStatusSucceeded {
		t.Fatalf("unexpected task round trip: %+v", roundTrip)
	}

	att := domain.DeliveryAttempt{
		ID:              1,
		TaskID:          task.ID,
		ReplayCount:     0,
		AttemptNo:       1,
		HTTPStatus:      200,
		DurationMs:      12,
		ErrorKind:       domain.AttemptErrorNone,
		ResponseSnippet: "ok",
		CreatedAt:       completed,
	}
	attPO := toDeliveryAttemptPO(att)
	if attPO.TableName() != "task_attempts" {
		t.Fatalf("unexpected table name %q", attPO.TableName())
	}
	if got := toDomainDeliveryAttempt(attPO); got != att {
		t.Fatalf("attempt round trip mismatch: got %+v want %+v", got, att)
	}

	outPO := outboxPO{
		ID:        9,
		TaskID:    task.ID,
		DeliverAt: now,
		CreatedAt: now,
	}
	if outPO.TableName() != "outbox" {
		t.Fatalf("unexpected table name %q", outPO.TableName())
	}
}

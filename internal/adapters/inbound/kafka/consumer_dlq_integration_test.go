//go:build integration

package kafka_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"

	inboundkafka "github.com/claudioed/labor-performance/internal/adapters/inbound/kafka"
	"github.com/claudioed/labor-performance/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/labor-performance/internal/adapters/outbound/events"
	"github.com/claudioed/labor-performance/internal/adapters/outbound/memory"
	"github.com/claudioed/labor-performance/internal/application/ports"
	"github.com/claudioed/labor-performance/internal/application/usecases"
	"github.com/claudioed/labor-performance/internal/domain/shared"
)

// alwaysFailingProcessedEventsFor wraps a real ports.ProcessedEvents so
// MarkProcessed fails with a genuine infrastructure error for exactly
// poisonEventID, on EVERY call, while every other event_id is delegated
// unchanged -- letting one poison message coexist in the SAME test with
// a normal, successfully-processed message on the SAME partition. This
// mirrors order-management's RepromiseConsumer DLQ acceptance test
// (ADR-0025) exactly, ported for this service's own idempotency gate
// (ports.ProcessedEvents.MarkProcessed, the very first thing
// RecordTaskPerformance.Execute calls — see that use case's Execute —
// so the injected failure never has a side effect to undo).
type alwaysFailingProcessedEventsFor struct {
	ports.ProcessedEvents
	poisonEventID string
}

func (p *alwaysFailingProcessedEventsFor) MarkProcessed(ctx context.Context, eventID string) (bool, error) {
	if eventID == p.poisonEventID {
		return false, fmt.Errorf("simulated poison-message infrastructure failure for event %s", eventID)
	}
	return p.ProcessedEvents.MarkProcessed(ctx, eventID)
}

// TestConsumer_PoisonMessage_GoesToDeadLetterTopicWithoutBlockingPartition
// is the ADR-0017 §DLQ acceptance test (this service's own resilience
// phase, mirroring order-management's ADR-0025 for the DLQ piece only):
// a message whose handler ALWAYS fails (simulated infrastructure error)
// must, after exactly maxHandlerAttempts (3) in-process retries, land on
// "<topic>.dlq" with the raw original payload plus error context, and
// the consumer must commit past it and keep processing -- a well-formed
// message published right after the poison one must be handled without
// delay, proving the partition was never blocked on the one bad message.
func TestConsumer_PoisonMessage_GoesToDeadLetterTopicWithoutBlockingPartition(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	container, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1",
		tckafka.WithClusterID(fmt.Sprintf("lp-consumer-dlq-itest-%d", time.Now().UnixNano())))
	if err != nil {
		t.Fatalf("start Kafka container: %v", err)
	}
	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			t.Errorf("terminate Kafka container: %v", err)
		}
	})

	brokers, err := container.Brokers(ctx)
	if err != nil {
		t.Fatalf("resolve Kafka brokers: %v", err)
	}
	topic := fmt.Sprintf("warehouse.fulfillment.events.dlq-itest-%d", time.Now().UnixNano())
	dlqTopic := topic + ".dlq"
	createTopicAndWaitForLeader(t, ctx, brokers[0], topic)
	createTopicAndWaitForLeader(t, ctx, brokers[0], dlqTopic)

	standards := memory.NewStandardRepo()
	performances := memory.NewPerformanceRepo()
	processed := memory.NewProcessedEventRepo()

	poisonEventID := fmt.Sprintf("evt-dlq-poison-%d", time.Now().UnixNano())
	poisonAssociateID := fmt.Sprintf("assoc-dlq-poison-%d", time.Now().UnixNano())
	failingProcessed := &alwaysFailingProcessedEventsFor{ProcessedEvents: processed, poisonEventID: poisonEventID}

	recordTaskPerformance := &usecases.RecordTaskPerformance{
		Performances: performances,
		Standards:    standards,
		Processed:    failingProcessed,
		Events:       events.NewLogPublisher(nil),
		Clock:        memory.SystemClock{},
	}

	consumer := inboundkafka.NewConsumerForTopic(
		brokers,
		fmt.Sprintf("labor-performance-dlq-itest-%d", time.Now().UnixNano()),
		topic,
		recordTaskPerformance,
		nil,
	)
	defer func() { _ = consumer.Close() }()

	consumeCtx, consumeCancel := context.WithCancel(ctx)
	runErr := make(chan error, 1)
	go func() { runErr <- consumer.Run(consumeCtx) }()

	// Start reading the DLQ topic BEFORE publishing, so the poison
	// message's eventual dead-letter write is never missed to a race.
	dlqReader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:     brokers,
		Topic:       dlqTopic,
		GroupID:     fmt.Sprintf("dlq-reader-%d", time.Now().UnixNano()),
		StartOffset: kafkago.FirstOffset,
	})
	defer func() { _ = dlqReader.Close() }()

	writer := &kafkago.Writer{Addr: kafkago.TCP(brokers...), Topic: topic}
	defer func() { _ = writer.Close() }()
	if err := writer.WriteMessages(ctx, kafkago.Message{
		Key:     []byte(poisonEventID),
		Headers: []kafkago.Header{cloudevents.ContentTypeHeader()},
		Value: mustTaskCompletedCloudEvent(t, poisonEventID, map[string]any{
			"task_id": "task-dlq-poison-1", "station_id": "station-1", "work_unit_id": "wu-1",
			"associate_id": poisonAssociateID, "duration_seconds": 52,
		}),
	}); err != nil {
		t.Fatalf("publish poison TaskCompleted: %v", err)
	}

	// Assert the poison message lands on the DLQ topic with the raw
	// payload and error context, after the retry budget is exhausted.
	dlqCtx, dlqCancel := context.WithTimeout(ctx, 60*time.Second)
	defer dlqCancel()
	dlqMsg, err := dlqReader.ReadMessage(dlqCtx)
	if err != nil {
		t.Fatalf("read DLQ message: %v", err)
	}
	if string(dlqMsg.Key) != poisonEventID {
		t.Errorf("DLQ message key = %q, want %q (raw key preserved)", string(dlqMsg.Key), poisonEventID)
	}
	var dlqPayload map[string]any
	if err := json.Unmarshal(dlqMsg.Value, &dlqPayload); err != nil {
		t.Fatalf("DLQ message value is not the raw original JSON payload: %v", err)
	}
	if dlqPayload["id"] != poisonEventID {
		t.Errorf("DLQ payload id = %v, want %q -- payload must be byte-identical to the original for manual replay", dlqPayload["event_id"], poisonEventID)
	}
	assertHeader(t, dlqMsg.Headers, "x-dlq-source-topic", topic)
	if h := headerValue(dlqMsg.Headers, "x-dlq-error"); h == "" {
		t.Error("DLQ message missing x-dlq-error header with failure context")
	}
	if h := headerValue(dlqMsg.Headers, "x-dlq-failed-at"); h == "" {
		t.Error("DLQ message missing x-dlq-failed-at header")
	}

	// Now publish a well-formed message for a healthy associate right
	// after the poison one, and confirm it is processed without delay
	// -- proving the partition was not blocked behind the poison
	// message.
	healthyAssociateID := fmt.Sprintf("assoc-dlq-healthy-%d", time.Now().UnixNano())
	if err := writer.WriteMessages(ctx, kafkago.Message{
		Key:     []byte(fmt.Sprintf("evt-dlq-good-%d", time.Now().UnixNano())),
		Headers: []kafkago.Header{cloudevents.ContentTypeHeader()},
		Value: mustTaskCompletedCloudEvent(t, fmt.Sprintf("evt-dlq-good-%d", time.Now().UnixNano()), map[string]any{
			"task_id": "task-dlq-good-1", "station_id": "station-1", "work_unit_id": "wu-1",
			"associate_id": healthyAssociateID, "duration_seconds": 45,
		}),
	}); err != nil {
		t.Fatalf("publish well-formed TaskCompleted: %v", err)
	}
	waitForPerformance(t, ctx, performances, shared.AssociateId(healthyAssociateID))

	consumeCancel()
	select {
	case err := <-runErr:
		if err != nil && ctx.Err() == nil {
			t.Errorf("run consumer: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Error("consumer did not stop after context cancellation")
	}

	// The poison associate was never recorded -- the DLQ path commits
	// the offset (so the partition advances) without ever having
	// driven the use case successfully.
	poisonRecorded, err := performances.ExistsByAssociateID(ctx, shared.AssociateId(poisonAssociateID))
	if err != nil {
		t.Fatalf("ExistsByAssociateID: %v", err)
	}
	if poisonRecorded {
		t.Error("poison associate should not have a recorded TaskPerformance -- the handler always failed for its event_id")
	}
}

// TestConsumer_LegacyFlatEnvelope_IsDeadLetteredNotParsed proves ADR
// 0021's consumer rule against a real broker: a retired flat-envelope
// message is NOT a CloudEvent, so it is never parsed and never retried —
// it is dead-lettered once (raw payload preserved) and committed past,
// and a valid CloudEvent right behind it on the same partition is still
// processed.
func TestConsumer_LegacyFlatEnvelope_IsDeadLetteredNotParsed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	container, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1",
		tckafka.WithClusterID(fmt.Sprintf("lp-consumer-legacy-itest-%d", time.Now().UnixNano())))
	if err != nil {
		t.Fatalf("start Kafka container: %v", err)
	}
	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			t.Errorf("terminate Kafka container: %v", err)
		}
	})
	brokers, err := container.Brokers(ctx)
	if err != nil {
		t.Fatalf("resolve Kafka brokers: %v", err)
	}
	topic := fmt.Sprintf("warehouse.fulfillment.events.legacy-itest-%d", time.Now().UnixNano())
	dlqTopic := topic + ".dlq"
	createTopicAndWaitForLeader(t, ctx, brokers[0], topic)
	createTopicAndWaitForLeader(t, ctx, brokers[0], dlqTopic)

	performances := memory.NewPerformanceRepo()
	recordTaskPerformance := &usecases.RecordTaskPerformance{
		Performances: performances,
		Standards:    memory.NewStandardRepo(),
		Processed:    memory.NewProcessedEventRepo(),
		Events:       events.NewLogPublisher(nil),
		Clock:        memory.SystemClock{},
	}
	consumer := inboundkafka.NewConsumerForTopic(brokers,
		fmt.Sprintf("labor-performance-legacy-itest-%d", time.Now().UnixNano()), topic, recordTaskPerformance, nil)
	defer func() { _ = consumer.Close() }()

	consumeCtx, consumeCancel := context.WithCancel(ctx)
	runErr := make(chan error, 1)
	go func() { runErr <- consumer.Run(consumeCtx) }()

	dlqReader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers: brokers, Topic: dlqTopic,
		GroupID:     fmt.Sprintf("dlq-legacy-reader-%d", time.Now().UnixNano()),
		StartOffset: kafkago.FirstOffset,
	})
	defer func() { _ = dlqReader.Close() }()

	legacyAssociateID := fmt.Sprintf("assoc-legacy-%d", time.Now().UnixNano())
	healthyAssociateID := fmt.Sprintf("assoc-legacy-healthy-%d", time.Now().UnixNano())
	legacyID := fmt.Sprintf("evt-legacy-%d", time.Now().UnixNano())

	writer := &kafkago.Writer{Addr: kafkago.TCP(brokers...), Topic: topic}
	defer func() { _ = writer.Close() }()
	if err := writer.WriteMessages(ctx,
		kafkago.Message{Key: []byte(legacyID), Value: legacyFlatTaskCompleted(t, legacyID, map[string]any{
			"task_id": "task-legacy-1", "station_id": "station-1", "work_unit_id": "wu-1",
			"associate_id": legacyAssociateID, "duration_seconds": 52,
		})},
		kafkago.Message{
			Key:     []byte("evt-legacy-good"),
			Headers: []kafkago.Header{cloudevents.ContentTypeHeader()},
			Value: mustTaskCompletedCloudEvent(t, fmt.Sprintf("evt-legacy-good-%d", time.Now().UnixNano()), map[string]any{
				"task_id": "task-legacy-good-1", "station_id": "station-1", "work_unit_id": "wu-1",
				"associate_id": healthyAssociateID, "duration_seconds": 45,
			}),
		},
	); err != nil {
		t.Fatalf("publish messages: %v", err)
	}

	dlqCtx, dlqCancel := context.WithTimeout(ctx, 60*time.Second)
	defer dlqCancel()
	dlqMsg, err := dlqReader.ReadMessage(dlqCtx)
	if err != nil {
		t.Fatalf("read DLQ message: %v", err)
	}
	var dlqPayload map[string]any
	if err := json.Unmarshal(dlqMsg.Value, &dlqPayload); err != nil {
		t.Fatalf("DLQ value is not the raw original payload: %v", err)
	}
	if dlqPayload["event_id"] != legacyID {
		t.Errorf("DLQ payload = %v, want the raw legacy message", dlqPayload)
	}
	if h := headerValue(dlqMsg.Headers, "x-dlq-error"); h == "" {
		t.Error("DLQ message missing x-dlq-error header")
	}

	waitForPerformance(t, ctx, performances, shared.AssociateId(healthyAssociateID))

	consumeCancel()
	select {
	case err := <-runErr:
		if err != nil && ctx.Err() == nil {
			t.Errorf("run consumer: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Error("consumer did not stop after context cancellation")
	}

	if recorded, _ := performances.ExistsByAssociateID(ctx, shared.AssociateId(legacyAssociateID)); recorded {
		t.Error("a legacy flat-envelope message was parsed and recorded; it must be rejected")
	}
}

func assertHeader(t *testing.T, headers []kafkago.Header, key, want string) {
	t.Helper()
	got := headerValue(headers, key)
	if got != want {
		t.Errorf("header %q = %q, want %q", key, got, want)
	}
}

func headerValue(headers []kafkago.Header, key string) string {
	for _, h := range headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

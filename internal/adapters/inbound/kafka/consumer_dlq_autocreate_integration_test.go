//go:build integration

package kafka_test

import (
	"context"
	"encoding/json"
	"errors"
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
	"github.com/claudioed/labor-performance/internal/application/usecases"
	"github.com/claudioed/labor-performance/internal/domain/shared"
)

// TestConsumer_PoisonMessage_AutoCreatesMissingDeadLetterTopic is the
// regression test for the "DLQ topic never created" outage: the source
// topic exists but "<topic>.dlq" was NEVER created (only topics that
// have been written to exist in this fleet — every writer relies on
// AllowAutoTopicCreation, warehouse-infra/terraform/kafka.tf). Before the
// fix the DLQ writer did not set AllowAutoTopicCreation, so dead-lettering
// failed with "[3] Unknown Topic Or Partition" and Run returned, stopping
// the consumer for good. The poison message must now land on a freshly
// auto-created DLQ topic and a valid CloudEvent behind it on the same
// partition must still be processed by the SAME Run loop.
func TestConsumer_PoisonMessage_AutoCreatesMissingDeadLetterTopic(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	container, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1",
		tckafka.WithClusterID(fmt.Sprintf("lp-consumer-dlq-autocreate-itest-%d", time.Now().UnixNano())))
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

	topic := fmt.Sprintf("warehouse.fulfillment.events.dlq-autocreate-itest-%d", time.Now().UnixNano())
	dlqTopic := topic + ".dlq"
	// Only the SOURCE topic is created; the DLQ topic deliberately is not.
	createTopicAndWaitForLeader(t, ctx, brokers[0], topic)
	assertTopicAbsent(t, ctx, brokers, dlqTopic)

	performances := memory.NewPerformanceRepo()
	recordTaskPerformance := &usecases.RecordTaskPerformance{
		Performances: performances,
		Standards:    memory.NewStandardRepo(),
		Processed:    memory.NewProcessedEventRepo(),
		Events:       events.NewLogPublisher(nil),
		Clock:        memory.SystemClock{},
	}
	consumer := inboundkafka.NewConsumerForTopic(brokers,
		fmt.Sprintf("labor-performance-dlq-autocreate-itest-%d", time.Now().UnixNano()), topic, recordTaskPerformance, nil)
	defer func() { _ = consumer.Close() }()

	consumeCtx, consumeCancel := context.WithCancel(ctx)
	defer consumeCancel()
	runErr := make(chan error, 1)
	go func() { runErr <- consumer.Run(consumeCtx) }()

	poisonID := fmt.Sprintf("evt-autocreate-poison-%d", time.Now().UnixNano())
	healthyAssociateID := fmt.Sprintf("assoc-autocreate-healthy-%d", time.Now().UnixNano())

	writer := &kafkago.Writer{Addr: kafkago.TCP(brokers...), Topic: topic}
	defer func() { _ = writer.Close() }()
	if err := writer.WriteMessages(ctx,
		// Not a CloudEvent at all: deterministic poison, dead-lettered once.
		kafkago.Message{Key: []byte(poisonID), Value: []byte(`{"not":"a cloudevent","poison_id":"` + poisonID + `"}`)},
		kafkago.Message{
			Key:     []byte("evt-autocreate-good"),
			Headers: []kafkago.Header{cloudevents.ContentTypeHeader()},
			Value: mustTaskCompletedCloudEvent(t, fmt.Sprintf("evt-autocreate-good-%d", time.Now().UnixNano()), map[string]any{
				"task_id": "task-autocreate-good-1", "station_id": "station-1", "work_unit_id": "wu-1",
				"associate_id": healthyAssociateID, "duration_seconds": 45,
			}),
		},
	); err != nil {
		t.Fatalf("publish messages: %v", err)
	}

	// The healthy message behind the poison one is processed: the consumer
	// survived the DLQ write. Fail fast if Run exits instead.
	for {
		exists, err := performances.ExistsByAssociateID(ctx, shared.AssociateId(healthyAssociateID))
		if err == nil && exists {
			break
		}
		select {
		case err := <-runErr:
			t.Fatalf("consumer stopped instead of dead-lettering into a missing DLQ topic: %v", err)
		case <-ctx.Done():
			t.Fatalf("healthy TaskCompleted behind the poison message was never processed: %v", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}

	dlqReader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:     brokers,
		Topic:       dlqTopic,
		GroupID:     fmt.Sprintf("dlq-autocreate-reader-%d", time.Now().UnixNano()),
		StartOffset: kafkago.FirstOffset,
	})
	defer func() { _ = dlqReader.Close() }()
	dlqCtx, dlqCancel := context.WithTimeout(ctx, 60*time.Second)
	defer dlqCancel()
	dlqMsg, err := dlqReader.ReadMessage(dlqCtx)
	if err != nil {
		t.Fatalf("read DLQ message from auto-created %q: %v", dlqTopic, err)
	}
	var payload map[string]any
	if err := json.Unmarshal(dlqMsg.Value, &payload); err != nil {
		t.Fatalf("DLQ value is not the raw original payload: %v", err)
	}
	if payload["poison_id"] != poisonID {
		t.Errorf("DLQ payload = %v, want the raw poison message %q", payload, poisonID)
	}
	assertHeader(t, dlqMsg.Headers, "x-dlq-source-topic", topic)

	consumeCancel()
	select {
	case err := <-runErr:
		if err != nil {
			t.Errorf("run consumer: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Error("consumer did not stop after context cancellation")
	}
}

// assertTopicAbsent proves the precondition with a Metadata request that
// does NOT auto-create (kafka-go's Client.Metadata leaves
// AllowAutoTopicCreation false), unlike Conn.ReadPartitions, which would
// create the very topic this test needs to be missing.
func assertTopicAbsent(t *testing.T, ctx context.Context, brokers []string, topic string) {
	t.Helper()
	client := &kafkago.Client{Addr: kafkago.TCP(brokers...), Timeout: 10 * time.Second}
	res, err := client.Metadata(ctx, &kafkago.MetadataRequest{Topics: []string{topic}})
	if err != nil {
		t.Fatalf("metadata for %q: %v", topic, err)
	}
	for _, tp := range res.Topics {
		if tp.Name == topic && !errors.Is(tp.Error, kafkago.UnknownTopicOrPartition) {
			t.Fatalf("precondition: DLQ topic %q must not exist yet (metadata error = %v, partitions = %d)", topic, tp.Error, len(tp.Partitions))
		}
	}
}

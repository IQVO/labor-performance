//go:build integration

package kafka_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strconv"
	"testing"
	"time"

	ce "github.com/cloudevents/sdk-go/v2/event"
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

// TestConsumer_ProjectsRealBrokerMessages runs the real consumer against an
// isolated Kafka Testcontainers broker, then verifies a CloudEvents TaskCompleted
// records task performance.
func TestConsumer_ProjectsRealBrokerMessages(t *testing.T) {
	testCtx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	container, err := tckafka.Run(testCtx, "confluentinc/confluent-local:7.6.1",
		tckafka.WithClusterID(fmt.Sprintf("labor-performance-itest-%d", time.Now().UnixNano())))
	if err != nil {
		t.Fatalf("start Kafka container: %v", err)
	}
	defer func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			t.Errorf("terminate Kafka container: %v", err)
		}
	}()

	brokers, err := container.Brokers(testCtx)
	if err != nil {
		t.Fatalf("get Kafka brokers: %v", err)
	}

	topic := fmt.Sprintf("labor-performance-itest-%d", time.Now().UnixNano())
	createTopicAndWaitForLeader(t, testCtx, brokers[0], topic)

	taskID := fmt.Sprintf("integration-kafka-task-%d", time.Now().UnixNano())
	associateID := fmt.Sprintf("integration-kafka-assoc-%d", time.Now().UnixNano())
	eventID := fmt.Sprintf("integration-kafka-evt-%d", time.Now().UnixNano())

	standards := memory.NewStandardRepo()
	performances := memory.NewPerformanceRepo()
	processed := memory.NewProcessedEventRepo()
	recordTaskPerformance := &usecases.RecordTaskPerformance{
		Performances: performances,
		Standards:    standards,
		Processed:    processed,
		Events:       events.NewLogPublisher(nil),
		Clock:        memory.SystemClock{},
	}

	consumer := inboundkafka.NewConsumerForTopic(
		brokers,
		fmt.Sprintf("labor-performance-integration-test-%d", time.Now().UnixNano()),
		topic,
		recordTaskPerformance,
		nil,
	)
	defer consumer.Close()

	consumeCtx, consumeCancel := context.WithCancel(testCtx)
	runErr := make(chan error, 1)
	go func() { runErr <- consumer.Run(consumeCtx) }()

	writer := &kafkago.Writer{Addr: kafkago.TCP(brokers...), Topic: topic}
	defer writer.Close()
	if err := writer.WriteMessages(testCtx, kafkago.Message{
		Key: []byte(eventID),
		Value: mustTaskCompletedCloudEvent(t, eventID, map[string]any{
			"task_id": taskID, "station_id": "station-1", "work_unit_id": "wu-1",
			"associate_id": associateID, "duration_seconds": 52,
		}),
		Headers: []kafkago.Header{cloudevents.ContentTypeHeader()},
	}); err != nil {
		t.Fatalf("publish TaskCompleted: %v", err)
	}

	waitForPerformance(t, testCtx, performances, shared.AssociateId(associateID))
	consumeCancel()
	select {
	case err := <-runErr:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("run consumer: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Error("consumer did not stop after context cancellation")
	}
}

// TestConsumer_RedeliveredCloudEventFiresOnce is the load-bearing
// idempotency assertion (ADR 0021): the real consumer, against a real
// Kafka broker, must fire the use case exactly once when the same
// CloudEvent (same `id`) is delivered twice — an at-least-once redelivery
// or an outbox relay replay.
func TestConsumer_RedeliveredCloudEventFiresOnce(t *testing.T) {
	testCtx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	container, err := tckafka.Run(testCtx, "confluentinc/confluent-local:7.6.1",
		tckafka.WithClusterID(fmt.Sprintf("labor-performance-itest-redeliver-%d", time.Now().UnixNano())))
	if err != nil {
		t.Fatalf("start Kafka container: %v", err)
	}
	defer func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			t.Errorf("terminate Kafka container: %v", err)
		}
	}()

	brokers, err := container.Brokers(testCtx)
	if err != nil {
		t.Fatalf("get Kafka brokers: %v", err)
	}

	topic := fmt.Sprintf("labor-performance-itest-redeliver-%d", time.Now().UnixNano())
	createTopicAndWaitForLeader(t, testCtx, brokers[0], topic)

	taskID := fmt.Sprintf("integration-kafka-redeliver-task-%d", time.Now().UnixNano())
	associateID := fmt.Sprintf("integration-kafka-redeliver-assoc-%d", time.Now().UnixNano())
	eventID := fmt.Sprintf("integration-kafka-redeliver-evt-%d", time.Now().UnixNano())

	standards := memory.NewStandardRepo()
	performances := memory.NewPerformanceRepo()
	processed := memory.NewProcessedEventRepo()
	recordTaskPerformance := &usecases.RecordTaskPerformance{
		Performances: performances,
		Standards:    standards,
		Processed:    processed,
		Events:       events.NewLogPublisher(nil),
		Clock:        memory.SystemClock{},
	}

	consumer := inboundkafka.NewConsumerForTopic(
		brokers,
		fmt.Sprintf("labor-performance-integration-test-redeliver-%d", time.Now().UnixNano()),
		topic,
		recordTaskPerformance,
		nil,
	)
	defer consumer.Close()

	consumeCtx, consumeCancel := context.WithCancel(testCtx)
	runErr := make(chan error, 1)
	go func() { runErr <- consumer.Run(consumeCtx) }()

	writer := &kafkago.Writer{Addr: kafkago.TCP(brokers...), Topic: topic}
	defer writer.Close()

	ceValue := mustTaskCompletedCloudEvent(t, eventID, map[string]any{
		"task_id": taskID, "station_id": "station-1", "work_unit_id": "wu-1",
		"associate_id": associateID, "duration_seconds": 52, "task_type": "PICK",
	})

	if err := writer.WriteMessages(testCtx,
		kafkago.Message{Key: []byte(eventID), Value: ceValue, Headers: []kafkago.Header{cloudevents.ContentTypeHeader()}},
		kafkago.Message{Key: []byte(eventID), Value: ceValue, Headers: []kafkago.Header{cloudevents.ContentTypeHeader()}},
	); err != nil {
		t.Fatalf("publish redelivered TaskCompleted messages: %v", err)
	}

	waitForPerformance(t, testCtx, performances, shared.AssociateId(associateID))

	// Give the second message time to arrive and (incorrectly, if there
	// were a bug) double-record before asserting the final count.
	time.Sleep(3 * time.Second)

	finalScorecard, err := performances.ScorecardFor(testCtx, shared.AssociateId(associateID))
	if err != nil {
		t.Fatalf("ScorecardFor: %v", err)
	}
	if finalScorecard.TaskCount != 1 {
		t.Fatalf("TaskCount = %d, want 1 — the use case must fire exactly once for two deliveries of the same CloudEvents id", finalScorecard.TaskCount)
	}

	consumeCancel()
	select {
	case err := <-runErr:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("run consumer: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Error("consumer did not stop after context cancellation")
	}
}

// mustTaskCompletedCloudEvent builds fulfillment-execution's
// TaskCompleted exactly as it appears on the wire: a CloudEvents 1.0
// structured-mode event with the full cross-service type string.
func mustTaskCompletedCloudEvent(t *testing.T, id string, data map[string]any) []byte {
	t.Helper()
	e := ce.New(cloudevents.SpecVersion)
	e.SetID(id)
	e.SetSource("/warehouse/fulfillment-execution")
	e.SetType(cloudevents.TypeFulfillmentTaskCompleted)
	e.SetSubject(fmt.Sprint(data["task_id"]))
	e.SetTime(time.Now().UTC())
	e.SetDataSchema("urn:warehouse:fulfillment-execution:events:TaskCompleted:v1")
	if err := e.SetData(cloudevents.DataContentType, data); err != nil {
		t.Fatalf("set data: %v", err)
	}
	body, err := json.Marshal(e)
	if err != nil {
		t.Fatalf("marshal cloudevent: %v", err)
	}
	return body
}

// legacyFlatTaskCompleted is the RETIRED flat envelope shape. It exists
// only so tests can prove the consumer rejects it.
func legacyFlatTaskCompleted(t *testing.T, eventID string, data map[string]any) []byte {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"event_id": eventID, "event_type": "TaskCompleted",
		"occurred_at": time.Now().UTC(), "source": "fulfillment-execution", "data": data,
	})
	if err != nil {
		t.Fatalf("marshal legacy flat message: %v", err)
	}
	return body
}

func createTopicAndWaitForLeader(t *testing.T, ctx context.Context, broker, topic string) {
	t.Helper()

	conn, err := kafkago.DialContext(ctx, "tcp", broker)
	if err != nil {
		t.Fatalf("dial Kafka broker: %v", err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		t.Fatalf("get Kafka controller: %v", err)
	}
	controllerConn, err := kafkago.DialContext(ctx, "tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		t.Fatalf("dial Kafka controller: %v", err)
	}
	defer controllerConn.Close()

	if err := controllerConn.CreateTopics(kafkago.TopicConfig{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}); err != nil {
		t.Fatalf("create Kafka topic %q: %v", topic, err)
	}

	deadline := time.Now().Add(20 * time.Second)
	for {
		partitions, err := conn.ReadPartitions(topic)
		if err == nil && len(partitions) > 0 && partitions[0].Leader.ID >= 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("Kafka topic %q did not receive a partition leader: %v", topic, err)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for Kafka topic leader: %v", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func waitForPerformance(t *testing.T, ctx context.Context, performances *memory.PerformanceRepo, associateID shared.AssociateId) {
	t.Helper()

	for {
		exists, err := performances.ExistsByAssociateID(ctx, associateID)
		if err == nil && exists {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("task performance was not recorded from TaskCompleted event: %v", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
}

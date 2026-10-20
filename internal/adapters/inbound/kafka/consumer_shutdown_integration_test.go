//go:build integration

package kafka_test

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"

	inboundkafka "github.com/claudioed/labor-performance/internal/adapters/inbound/kafka"
	"github.com/claudioed/labor-performance/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/labor-performance/internal/adapters/outbound/events"
	"github.com/claudioed/labor-performance/internal/adapters/outbound/memory"
	"github.com/claudioed/labor-performance/internal/adapters/outbound/system"
	"github.com/claudioed/labor-performance/internal/application/ports"
	"github.com/claudioed/labor-performance/internal/application/usecases"
)

// blockingProcessedEvents parks MarkProcessed -- the very first thing
// RecordTaskPerformance.Execute calls -- until the handler's context is
// cancelled, then reports that cancellation: exactly what a real Postgres
// call does when the pod is told to stop mid-handle.
type blockingProcessedEvents struct {
	ports.ProcessedEvents
	entered chan struct{}
}

func (p *blockingProcessedEvents) MarkProcessed(ctx context.Context, _ string) (bool, error) {
	select {
	case p.entered <- struct{}{}:
	default:
	}
	<-ctx.Done()
	return false, ctx.Err()
}

// recordingHandler keeps every log message so the test can prove the
// consumer never even ATTEMPTED the dead-letter path on shutdown.
type recordingHandler struct {
	mu   sync.Mutex
	msgs []string
}

func (h *recordingHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *recordingHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.msgs = append(h.msgs, r.Message)
	return nil
}
func (h *recordingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *recordingHandler) WithGroup(string) slog.Handler      { return h }

func (h *recordingHandler) messages() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.msgs...)
}

// TestConsumer_ShutdownMidHandle_IsCleanAndDeadLettersNothing pins the
// shutdown contract: cancelling Run's context while a HEALTHY message is
// being handled is a clean stop (Run returns nil), and that message is NOT
// dead-lettered -- its offset stays uncommitted, so it is redelivered on the
// next start and the processed-events claim absorbs the replay. Before the
// fix the retry wrapper's cancellation error was routed into the dead-letter
// path as if the message were poison ("exhausted retries, sending to
// dead-letter topic"), and a DLQ batch already enqueued on the cancelled
// context could still reach <topic>.dlq.
func TestConsumer_ShutdownMidHandle_IsCleanAndDeadLettersNothing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	container, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1",
		tckafka.WithClusterID(fmt.Sprintf("lp-consumer-shutdown-itest-%d", time.Now().UnixNano())))
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
	topic := fmt.Sprintf("warehouse.fulfillment.events.shutdown-itest-%d", time.Now().UnixNano())
	dlqTopic := topic + ".dlq"
	createTopicAndWaitForLeader(t, ctx, brokers[0], topic)
	createTopicAndWaitForLeader(t, ctx, brokers[0], dlqTopic)

	processed := &blockingProcessedEvents{ProcessedEvents: memory.NewProcessedEventRepo(), entered: make(chan struct{}, 1)}
	recordTaskPerformance := &usecases.RecordTaskPerformance{
		Performances: memory.NewPerformanceRepo(),
		Standards:    memory.NewStandardRepo(),
		Processed:    processed,
		Events:       events.NewLogPublisher(nil),
		Clock:        system.Clock{},
	}
	logs := &recordingHandler{}
	consumer := inboundkafka.NewConsumerForTopic(brokers,
		fmt.Sprintf("labor-performance-shutdown-itest-%d", time.Now().UnixNano()),
		topic, recordTaskPerformance, slog.New(logs))
	defer func() { _ = consumer.Close() }()

	consumeCtx, consumeCancel := context.WithCancel(ctx)
	defer consumeCancel()
	runErr := make(chan error, 1)
	go func() { runErr <- consumer.Run(consumeCtx) }()

	writer := &kafkago.Writer{Addr: kafkago.TCP(brokers...), Topic: topic}
	defer func() { _ = writer.Close() }()
	eventID := fmt.Sprintf("evt-shutdown-%d", time.Now().UnixNano())
	if err := writer.WriteMessages(ctx, kafkago.Message{
		Key:     []byte(eventID),
		Headers: []kafkago.Header{cloudevents.ContentTypeHeader()},
		Value: mustTaskCompletedCloudEvent(t, eventID, map[string]any{
			"task_id": "task-shutdown-1", "station_id": "station-1", "work_unit_id": "wu-1",
			"associate_id": "assoc-shutdown-1", "duration_seconds": 40,
		}),
	}); err != nil {
		t.Fatalf("publish TaskCompleted: %v", err)
	}

	select {
	case <-processed.entered:
	case <-time.After(60 * time.Second):
		t.Fatal("the consumer never started handling the message")
	}
	consumeCancel()

	select {
	case err := <-runErr:
		if err != nil {
			t.Fatalf("Run after its context was cancelled mid-handle = %v, want nil (clean shutdown)", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("consumer did not stop after context cancellation")
	}

	// Close flushes any DLQ batch a cancelled WriteMessages had already
	// enqueued, so a dead-letter that "failed" on the cancelled context but
	// still reached the broker is caught below.
	_ = consumer.Close()

	for _, m := range logs.messages() {
		if strings.Contains(m, "dead-letter") {
			t.Fatalf("the consumer took the dead-letter path for a message only shutdown interrupted: %q", m)
		}
	}

	dlqReader := kafkago.NewReader(kafkago.ReaderConfig{Brokers: brokers, Topic: dlqTopic, Partition: 0, MinBytes: 1, MaxBytes: 10e6})
	defer func() { _ = dlqReader.Close() }()
	readCtx, readCancel := context.WithTimeout(ctx, 5*time.Second)
	defer readCancel()
	if msg, err := dlqReader.ReadMessage(readCtx); err == nil {
		t.Fatalf("a healthy message was dead-lettered on shutdown: key=%q headers=%v", string(msg.Key), msg.Headers)
	}
}

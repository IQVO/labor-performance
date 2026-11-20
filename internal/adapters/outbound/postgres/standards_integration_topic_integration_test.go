//go:build integration

// ADR 0038: LaborStandardDefined and LaborStandardRevised are also published
// on the integration topic, through the same transactional outbox, one more
// row per standard event. This proves the two new rows end to end: committed
// with the standard in Postgres, drained by the relay onto a real Kafka
// broker (testcontainers), keyed by task type, subject the standard id, the
// `:events:` dataschema, and the SAME data as the analytics copy.
package postgres_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"

	"github.com/claudioed/labor-performance/internal/adapters/kafka/cloudevents"
	outboundkafka "github.com/claudioed/labor-performance/internal/adapters/outbound/kafka"
	"github.com/claudioed/labor-performance/internal/adapters/outbound/memory"
	"github.com/claudioed/labor-performance/internal/adapters/outbound/postgres"
	"github.com/claudioed/labor-performance/internal/application/usecases"
	"github.com/claudioed/labor-performance/internal/domain/shared"
)

// startStandardsBroker boots this test's own disposable broker
// (testcontainers, never an external one) and pre-creates one-partition
// topics, waiting for each leader, so the relay's first write does not race
// topic auto-creation.
func startStandardsBroker(ctx context.Context, t *testing.T, topics ...string) []string {
	t.Helper()
	container, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1",
		tckafka.WithClusterID(fmt.Sprintf("labor-performance-standards-itest-%d", time.Now().UnixNano())))
	if err != nil {
		t.Fatalf("start Kafka container: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(container) })
	brokers, err := container.Brokers(ctx)
	if err != nil {
		t.Fatalf("resolve Kafka brokers: %v", err)
	}
	conn, err := kafkago.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		t.Fatalf("dial Kafka: %v", err)
	}
	defer func() { _ = conn.Close() }()
	for _, topic := range topics {
		if err := conn.CreateTopics(kafkago.TopicConfig{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}); err != nil {
			t.Fatalf("create topic %s: %v", topic, err)
		}
		for deadline := time.Now().Add(30 * time.Second); ; time.Sleep(200 * time.Millisecond) {
			if parts, err := conn.ReadPartitions(topic); err == nil && len(parts) > 0 && parts[0].Leader.Host != "" {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("topic %s has no partition leader", topic)
			}
		}
	}
	return brokers
}

// readN reads exactly n messages of a one-partition topic from its start.
func readN(ctx context.Context, t *testing.T, brokers []string, topic string, n int) []kafkago.Message {
	t.Helper()
	reader := kafkago.NewReader(kafkago.ReaderConfig{Brokers: brokers, Topic: topic, StartOffset: kafkago.FirstOffset, MaxWait: 2 * time.Second})
	defer func() { _ = reader.Close() }()
	out := make([]kafkago.Message, 0, n)
	for len(out) < n {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			t.Fatalf("read %s: got %d of %d: %v", topic, len(out), n, err)
		}
		out = append(out, msg)
	}
	return out
}

type wireEvent struct {
	ID         string          `json:"id"`
	Type       string          `json:"type"`
	Subject    string          `json:"subject"`
	DataSchema string          `json:"dataschema"`
	Data       json.RawMessage `json:"data"`
}

func decodeWire(t *testing.T, m kafkago.Message) wireEvent {
	t.Helper()
	var e wireEvent
	if err := json.Unmarshal(m.Value, &e); err != nil {
		t.Fatalf("not a CloudEvent: %v: %s", err, m.Value)
	}
	return e
}

func TestOutbox_StandardEventsRelayToTheIntegrationTopicOnARealBroker(t *testing.T) {
	pool := outboxDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	brokers := startStandardsBroker(ctx, t, cloudevents.TopicLaborPerformanceAnalytics, cloudevents.TopicLaborPerformanceEvents)

	now := time.Now().UTC().Truncate(time.Microsecond)
	define := &usecases.DefineStandard{
		Standards:  postgres.NewStandardRepo(pool),
		Events:     postgres.NewOutboxPublisher(pool, analyticsEncoder(), integrationEncoder()),
		Clock:      memory.FixedClock{At: now},
		UnitOfWork: postgres.NewUnitOfWork(pool),
	}
	first, err := define.Execute(ctx, shared.Pick, 45, nil)
	if err != nil {
		t.Fatalf("define: %v", err)
	}
	define.Clock = memory.FixedClock{At: now.Add(time.Minute)}
	second, err := define.Execute(ctx, shared.Pick, 40, nil)
	if err != nil {
		t.Fatalf("revise: %v", err)
	}

	// One more outbox row per standard event: two per topic.
	for _, topic := range []string{cloudevents.TopicLaborPerformanceAnalytics, cloudevents.TopicLaborPerformanceEvents} {
		for _, typ := range []string{cloudevents.TypeLaborStandardDefined, cloudevents.TypeLaborStandardRevised} {
			if got := countOutbox(t, pool, fmt.Sprintf("published_at IS NULL AND topic = '%s' AND event_type = '%s' AND key = 'PICK'", topic, typ)); got != 1 {
				t.Fatalf("topic %s: expected 1 unpublished %s row keyed PICK, got %d", topic, typ, got)
			}
		}
	}
	if got := countOutbox(t, pool, "true"); got != 4 {
		t.Fatalf("expected 4 outbox rows (2 events x 2 topics), got %d", got)
	}

	sink := outboundkafka.NewRelaySink(brokers)
	t.Cleanup(func() { _ = sink.Close() })
	relay := postgres.NewOutboxRelay(pool, sink, slog.Default(), postgres.WithMaxAttempts(20))
	if n, err := relay.RelayOnce(ctx); err != nil || n != 4 {
		t.Fatalf("relay: n=%d err=%v, want all 4 rows", n, err)
	}

	events := readN(ctx, t, brokers, cloudevents.TopicLaborPerformanceEvents, 2)
	analytics := readN(ctx, t, brokers, cloudevents.TopicLaborPerformanceAnalytics, 2)
	want := []struct {
		typ, name, subject string
	}{
		{cloudevents.TypeLaborStandardDefined, cloudevents.EventLaborStandardDefined, string(first.ID())},
		{cloudevents.TypeLaborStandardRevised, cloudevents.EventLaborStandardRevised, string(second.ID())},
	}
	for i, w := range want {
		ev, an := decodeWire(t, events[i]), decodeWire(t, analytics[i])
		if string(events[i].Key) != "PICK" || ev.Type != w.typ || ev.Subject != w.subject ||
			ev.DataSchema != cloudevents.DataSchema(cloudevents.StreamEvents, w.name, 1) {
			t.Fatalf("integration message %d: key %q type %q subject %q schema %q, want PICK %s %s events", i, events[i].Key, ev.Type, ev.Subject, ev.DataSchema, w.typ, w.subject)
		}
		if string(ev.Data) != string(an.Data) || ev.ID == an.ID {
			t.Fatalf("integration message %d must carry the analytics data under its own id: %s (%s) vs %s (%s)", i, ev.Data, ev.ID, an.Data, an.ID)
		}
	}
}

//go:build integration

// Gap fix D27: the W3C trace context of the request that wrote an outbox
// row crosses the outbox to Kafka. This repo already stores it (the
// encoders inject the active span into the message headers, which the
// outbox persists in outbox_events.headers, ADR 0010) and the relay
// already forwards those headers; these two tests prove the whole path end
// to end against a real Postgres and a real Kafka broker (testcontainers),
// never KAFKA_BROKERS or a fixed broker address.
package postgres_test

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/claudioed/labor-performance/internal/adapters/kafka/cloudevents"
	outboundkafka "github.com/claudioed/labor-performance/internal/adapters/outbound/kafka"
	"github.com/claudioed/labor-performance/internal/adapters/outbound/memory"
	"github.com/claudioed/labor-performance/internal/adapters/outbound/postgres"
	"github.com/claudioed/labor-performance/internal/application/usecases"
	"github.com/claudioed/labor-performance/internal/domain/shared"
)

// installW3CTracing installs a real SDK tracer provider and the W3C
// trace-context propagator globally (what telemetry.Setup does in
// production), restoring the previous globals when the test ends.
func installW3CTracing(t *testing.T) trace.Tracer {
	t.Helper()
	prevTP, prevProp := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	tp := sdktrace.NewTracerProvider()
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		_ = tp.Shutdown(context.Background())
		otel.SetTracerProvider(prevTP)
		otel.SetTextMapPropagator(prevProp)
	})
	return tp.Tracer("outbox-trace-itest")
}

// startKafka boots a disposable broker and pre-creates topics (waiting for
// each partition leader) so the relay's first write does not race topic
// auto-creation.
func startKafka(ctx context.Context, t *testing.T, topics ...string) []string {
	t.Helper()
	container, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1",
		tckafka.WithClusterID("labor-performance-outbox-trace-itest"))
	if err != nil {
		t.Fatalf("start Kafka container: %v", err)
	}
	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			t.Logf("terminate Kafka container: %v", err)
		}
	})
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
		deadline := time.Now().Add(30 * time.Second)
		for {
			parts, err := conn.ReadPartitions(topic)
			if err == nil && len(parts) > 0 && parts[0].Leader.Host != "" {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("topic %s has no partition leader: %v", topic, err)
			}
			time.Sleep(200 * time.Millisecond)
		}
	}
	return brokers
}

func readOne(ctx context.Context, t *testing.T, brokers []string, topic string) kafkago.Message {
	t.Helper()
	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers: brokers, Topic: topic, StartOffset: kafkago.FirstOffset, MaxWait: 2 * time.Second,
	})
	defer func() { _ = reader.Close() }()
	msg, err := reader.ReadMessage(ctx)
	if err != nil {
		t.Fatalf("read %s: %v", topic, err)
	}
	return msg
}

func headerValue(headers []kafkago.Header, key string) (string, bool) {
	for _, h := range headers {
		if h.Key == key {
			return string(h.Value), true
		}
	}
	return "", false
}

// TestOutboxTrace_TraceparentCrossesTheOutboxToKafka: a use case runs inside
// a request span; the outbox row stores that span's traceparent in its
// headers column; the relay (a later pass, no span of its own, a
// background context) sends it as a Kafka header; and a consumer extracting
// it with the global propagator lands on the request's trace and span.
func TestOutboxTrace_TraceparentCrossesTheOutboxToKafka(t *testing.T) {
	tracer := installW3CTracing(t)
	pool := outboxDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	brokers := startKafka(ctx, t, cloudevents.TopicLaborPerformanceAnalytics)

	define := &usecases.DefineStandard{
		Standards:  postgres.NewStandardRepo(pool),
		Events:     postgres.NewOutboxPublisher(pool, analyticsEncoder()),
		Clock:      memory.FixedClock{At: time.Now().UTC().Truncate(time.Microsecond)},
		UnitOfWork: postgres.NewUnitOfWork(pool),
	}

	reqCtx, reqSpan := tracer.Start(ctx, "POST /standards", trace.WithSpanKind(trace.SpanKindServer))
	if _, err := define.Execute(reqCtx, shared.Pick, 30, nil); err != nil {
		t.Fatalf("define: %v", err)
	}
	reqSpan.End()
	want := reqSpan.SpanContext()
	wantTraceparent := fmt.Sprintf("00-%s-%s-01", want.TraceID(), want.SpanID())

	var stored string
	if err := pool.QueryRow(ctx, `
		SELECT coalesce((SELECT h->>'value' FROM jsonb_array_elements(headers) h WHERE h->>'key' = 'traceparent'), '')
		FROM outbox_events WHERE topic = $1`, cloudevents.TopicLaborPerformanceAnalytics).Scan(&stored); err != nil {
		t.Fatalf("read stored traceparent: %v", err)
	}
	if stored != wantTraceparent {
		t.Fatalf("outbox row traceparent = %q, want the request span's %q", stored, wantTraceparent)
	}

	sink := outboundkafka.NewRelaySink(brokers)
	t.Cleanup(func() { _ = sink.Close() })
	relay := postgres.NewOutboxRelay(pool, sink, slog.Default(), postgres.WithMaxAttempts(20))
	// The relay runs without the request's context: continuity must come
	// from the stored row, not from the caller.
	if n, err := relay.RelayOnce(context.Background()); err != nil || n != 1 {
		t.Fatalf("relay: n=%d err=%v (want the one analytics row)", n, err)
	}

	msg := readOne(ctx, t, brokers, cloudevents.TopicLaborPerformanceAnalytics)
	got, ok := headerValue(msg.Headers, "traceparent")
	if !ok || got != wantTraceparent {
		t.Fatalf("Kafka traceparent = %q (present=%v), want %q", got, ok, wantTraceparent)
	}
	if ct, _ := headerValue(msg.Headers, "content-type"); ct == "" {
		t.Fatalf("content-type header lost on the outbox path: %+v", msg.Headers)
	}

	consumerCtx := otel.GetTextMapPropagator().Extract(context.Background(), kafkaHeaderCarrier(msg.Headers))
	parent := trace.SpanContextFromContext(consumerCtx)
	if parent.TraceID() != want.TraceID() || parent.SpanID() != want.SpanID() || !parent.IsRemote() {
		t.Fatalf("consumer would parent on %s/%s (remote=%v), want the request span %s/%s",
			parent.TraceID(), parent.SpanID(), parent.IsRemote(), want.TraceID(), want.SpanID())
	}
}

// TestOutboxTrace_RowWithoutTraceHeadersRelaysUnchanged pins the other half
// of D27: a row written with no trace context (no active span, or headers
// left at the column default '[]') relays with its key and value unchanged
// and carries no traceparent/tracestate a consumer could mis-parent onto.
func TestOutboxTrace_RowWithoutTraceHeadersRelaysUnchanged(t *testing.T) {
	installW3CTracing(t)
	pool := outboxDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	const topic = "warehouse.labor-performance.itest-no-trace"
	brokers := startKafka(ctx, t, topic)

	insertOutboxRow(t, pool, topic, "com.example.Untraced", "assoc-untraced")

	sink := outboundkafka.NewRelaySink(brokers)
	t.Cleanup(func() { _ = sink.Close() })
	relay := postgres.NewOutboxRelay(pool, sink, slog.Default(), postgres.WithMaxAttempts(20))
	if n, err := relay.RelayOnce(ctx); err != nil || n != 1 {
		t.Fatalf("relay: n=%d err=%v", n, err)
	}

	msg := readOne(ctx, t, brokers, topic)
	if string(msg.Key) != "assoc-untraced" || strings.TrimSpace(string(msg.Value)) != `{}` {
		t.Fatalf("row relayed changed: key=%q value=%q", msg.Key, msg.Value)
	}
	for _, h := range msg.Headers {
		if h.Key == "traceparent" || h.Key == "tracestate" {
			t.Fatalf("a row without stored trace context must relay without trace headers, got %+v", msg.Headers)
		}
	}
}

// kafkaHeaderCarrier adapts kafka-go headers to the OTel text-map carrier.
type kafkaHeaderCarrier []kafkago.Header

func (c kafkaHeaderCarrier) Get(key string) string {
	v, _ := headerValue(c, key)
	return v
}

func (c kafkaHeaderCarrier) Set(string, string) {}

func (c kafkaHeaderCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for _, h := range c {
		keys = append(keys, h.Key)
	}
	return keys
}

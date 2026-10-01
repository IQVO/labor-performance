package kafka

import (
	"context"
	"errors"
	"testing"

	kafkago "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/claudioed/labor-performance/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/labor-performance/internal/domain/shared"
)

func TestAnalyticsPublisherEncodeProducesOneWireMessagePerContractEvent(t *testing.T) {
	pct := 86.5
	p, w := newTestPublisher()

	msgs, err := p.Encode(context.Background(),
		shared.NewLaborStandardDefined(at(9), "std-1", shared.Pick, 45, nil, at(9)),
		unknownEvent{},
		shared.NewTaskPerformanceRecorded(at(11), "task-1", shared.AssociateId("assoc-1"), shared.Pack, 52, &pct, nil, at(9)),
	)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if len(w.msgs) != 0 {
		t.Fatalf("Encode must not write; the fake writer saw %d messages", len(w.msgs))
	}
	if len(msgs) != 2 {
		t.Fatalf("got %d encoded messages, want 2 (the unknown event is skipped)", len(msgs))
	}

	for i, want := range []struct {
		eventType string
		key       string
	}{
		{cloudevents.TypeLaborStandardDefined, "PICK"},
		{cloudevents.TypeTaskPerformanceRecorded, "PACK"},
	} {
		m := msgs[i]
		if m.Topic != cloudevents.TopicLaborPerformanceAnalytics {
			t.Errorf("msg %d topic = %q, want %q", i, m.Topic, cloudevents.TopicLaborPerformanceAnalytics)
		}
		if m.EventType != want.eventType {
			t.Errorf("msg %d EventType = %q, want %q", i, m.EventType, want.eventType)
		}
		if string(m.Key) != want.key {
			t.Errorf("msg %d key = %q, want %q", i, string(m.Key), want.key)
		}
		if m.Headers == nil {
			t.Errorf("msg %d headers are nil; the propagator has nowhere to inject", i)
		}
		env, _ := decode(t, kafkago.Message{Value: m.Value})
		if env.Type() != want.eventType || env.Source() != cloudevents.Source ||
			env.DataSchema() != cloudevents.DataSchema(cloudevents.StreamAnalytics, eventNameOf(want.eventType), 1) {
			t.Errorf("msg %d cloudevent = %+v", i, env.Context)
		}
		if env.ID() == "" {
			t.Errorf("msg %d id is empty", i)
		}
		assertContentTypeHeader(t, m.Headers)
	}
}

// Encode and Publish must agree byte-for-byte on the value: the outbox
// stores what Encode returns and the direct path writes what Publish
// builds, and the projector must not be able to tell them apart.
func TestAnalyticsPublisherPublishWritesExactlyWhatEncodeProduces(t *testing.T) {
	event := shared.NewLaborStandardDefined(at(9), "std-1", shared.Pick, 45, nil, at(9))

	encoder := &AnalyticsPublisher{NewID: func() string { return "evt-fixed" }}
	encoded, err := encoder.Encode(context.Background(), event)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	w := &recordingWriter{}
	publisher := &AnalyticsPublisher{Writer: w, NewID: func() string { return "evt-fixed" }}
	if err := publisher.Publish(context.Background(), event); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	if string(w.msgs[0].Value) != string(encoded[0].Value) {
		t.Errorf("Publish wrote\n%s\nbut Encode produced\n%s", w.msgs[0].Value, encoded[0].Value)
	}
	if string(w.msgs[0].Key) != string(encoded[0].Key) {
		t.Errorf("key mismatch: %q vs %q", w.msgs[0].Key, encoded[0].Key)
	}
	if w.msgs[0].Topic != "" {
		t.Errorf("a message written through the topic-pinned writer must carry no Topic, got %q", w.msgs[0].Topic)
	}
}

// With a real propagator and an active span the encoded headers carry
// the W3C traceparent, so an outbox row stores the request's trace
// context for the relay to replay.
func TestAnalyticsPublisherEncodeCapturesTraceHeadersFromContext(t *testing.T) {
	prevProp := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() { otel.SetTextMapPropagator(prevProp) })

	tp := sdktrace.NewTracerProvider()
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	ctx, span := tp.Tracer("test").Start(context.Background(), "request")
	defer span.End()

	p, _ := newTestPublisher()
	msgs, err := p.Encode(ctx, shared.NewLaborStandardDefined(at(9), "std-1", shared.Pick, 45, nil, at(9)))
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	var found bool
	for _, h := range msgs[0].Headers {
		if h.Key == "traceparent" && len(h.Value) > 0 {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected a traceparent header captured at Encode time, got %v", msgs[0].Headers)
	}
}

func TestRelaySinkSetsTopicPerMessageAndWritesInOneCall(t *testing.T) {
	w := &recordingWriter{}
	sink := &RelaySink{Writer: w}

	err := sink.Send(context.Background(),
		Encoded{Topic: "topic-a", EventType: "A", Key: []byte("k1"), Value: []byte("v1"), Headers: []kafkago.Header{{Key: "traceparent", Value: []byte("00-abc")}}},
		Encoded{Topic: "topic-b", EventType: "B", Key: []byte("k2"), Value: []byte("v2")},
	)
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if len(w.msgs) != 2 {
		t.Fatalf("got %d messages, want 2", len(w.msgs))
	}
	if w.msgs[0].Topic != "topic-a" || w.msgs[1].Topic != "topic-b" {
		t.Errorf("topics = %q, %q; want topic-a, topic-b", w.msgs[0].Topic, w.msgs[1].Topic)
	}
	if string(w.msgs[0].Key) != "k1" || string(w.msgs[0].Value) != "v1" {
		t.Errorf("msg 0 = key %q value %q", w.msgs[0].Key, w.msgs[0].Value)
	}
	if len(w.msgs[0].Headers) != 1 || w.msgs[0].Headers[0].Key != "traceparent" {
		t.Errorf("stored headers must travel unchanged, got %v", w.msgs[0].Headers)
	}
	if w.msgs[1].Headers == nil {
		t.Error("a message with no stored headers must still carry a non-nil header slice")
	}
}

func TestRelaySinkSendNothingIsANoOp(t *testing.T) {
	w := &recordingWriter{failWith: errors.New("must not be called")}
	if err := (&RelaySink{Writer: w}).Send(context.Background()); err != nil {
		t.Fatalf("an empty Send must not touch the writer: %v", err)
	}
}

func TestRelaySinkPropagatesWriteErrors(t *testing.T) {
	boom := errors.New("broker down")
	sink := &RelaySink{Writer: &recordingWriter{failWith: boom}}
	err := sink.Send(context.Background(), Encoded{Topic: "t", EventType: "A", Value: []byte("v")})
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want wrapping %v", err, boom)
	}
}

func TestRelaySinkCloseWithFakeWriterIsANoOp(t *testing.T) {
	if err := (&RelaySink{Writer: &recordingWriter{}}).Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

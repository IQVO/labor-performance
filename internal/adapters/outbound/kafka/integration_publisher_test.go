package kafka

import (
	"context"
	"errors"
	"testing"

	ce "github.com/cloudevents/sdk-go/v2/event"

	"github.com/claudioed/labor-performance/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/labor-performance/internal/domain/shared"
)

func newTestIntegrationPublisher() (*IntegrationPublisher, *recordingWriter) {
	w := &recordingWriter{}
	return &IntegrationPublisher{Writer: w, NewID: seqIDs()}, w
}

func decodePlain(t *testing.T, value []byte) (ce.Event, map[string]any) {
	t.Helper()
	return decodeCloudEvent(t, value)
}

func TestIntegrationPublisherPublishesTaskPerformanceRecorded(t *testing.T) {
	pct := 86.5
	p, w := newTestIntegrationPublisher()

	event := shared.NewTaskPerformanceRecorded(
		at(11), "task-1", shared.AssociateId("assoc-1"), shared.Pack, 52, &pct, nil, at(9))

	if err := p.Publish(context.Background(), event); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if len(w.msgs) != 1 {
		t.Fatalf("got %d messages, want 1", len(w.msgs))
	}

	env, data := decodePlain(t, w.msgs[0].Value)
	if env.Type() != cloudevents.TypeTaskPerformanceRecorded {
		t.Errorf("type = %q, want %q", env.Type(), cloudevents.TypeTaskPerformanceRecorded)
	}
	if env.Source() != cloudevents.Source {
		t.Errorf("source = %q, want %q", env.Source(), cloudevents.Source)
	}
	if env.ID() == "" {
		t.Error("id is empty; it is the downstream consumer's idempotency key")
	}
	if !env.Time().Equal(event.OccurredAt()) {
		t.Errorf("time = %v, want %v", env.Time(), event.OccurredAt())
	}
	if env.Subject() != "assoc-1" {
		t.Errorf("subject = %q, want the associate id", env.Subject())
	}
	if env.DataSchema() != "urn:warehouse:labor-performance:events:TaskPerformanceRecorded:v1" {
		t.Errorf("dataschema = %q", env.DataSchema())
	}
	assertContentTypeHeader(t, w.msgs[0].Headers)
	// Partition key is AssociateId on the integration topic — NOT
	// TaskType, which is what the analytics topic keys on. This is
	// deliberate: the intended first consumer (workforce-management)
	// builds a per-associate cache and needs per-associate ordering.
	if got := string(w.msgs[0].Key); got != "assoc-1" {
		t.Errorf("partition key = %q, want %q", got, "assoc-1")
	}
	if data["task_id"] != "task-1" {
		t.Errorf("task_id = %v, want task-1", data["task_id"])
	}
	if data["associate_id"] != "assoc-1" {
		t.Errorf("associate_id = %v, want assoc-1", data["associate_id"])
	}
	if data["task_type"] != "PACK" {
		t.Errorf("task_type = %v, want PACK", data["task_type"])
	}
	if data["efficiency_pct"] != 86.5 {
		t.Errorf("efficiency_pct = %v, want 86.5", data["efficiency_pct"])
	}
	if data["actual_seconds"] != float64(52) {
		t.Errorf("actual_seconds = %v, want 52", data["actual_seconds"])
	}
}

func TestIntegrationPublisherPreservesNilEfficiency(t *testing.T) {
	p, w := newTestIntegrationPublisher()

	event := shared.NewTaskPerformanceRecorded(
		at(9), "task-1", shared.AssociateId(""), shared.TaskType(""), 0, nil, nil, at(9))

	if err := p.Publish(context.Background(), event); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	_, data := decodePlain(t, w.msgs[0].Value)
	v, present := data["efficiency_pct"]
	if !present {
		t.Fatal("efficiency_pct is absent from the payload; it must be present as an explicit null")
	}
	if v != nil {
		t.Errorf("efficiency_pct = %v, want null", v)
	}
	// The empty-associate key is legitimate: every unattributed task
	// (e.g. a robot station) still lands on one shared partition,
	// ordered relative to every other one.
	if got := string(w.msgs[0].Key); got != "" {
		t.Errorf("partition key = %q, want empty string", got)
	}
	// ...but the CloudEvents subject is never empty: it falls back to
	// the task id.
	env, _ := decodePlain(t, w.msgs[0].Value)
	if env.Subject() != "task-1" {
		t.Errorf("subject = %q, want the task id fallback", env.Subject())
	}
}

func TestIntegrationPublisherSkipsEventsOutsideTheContract(t *testing.T) {
	p, w := newTestIntegrationPublisher()

	// LaborStandardDefined is analytics-only, NOT part of the
	// integration contract (ADR 0013 scopes the first cut to
	// TaskPerformanceRecorded only).
	if err := p.Publish(context.Background(), shared.NewLaborStandardDefined(at(9), "std-1", shared.Pick, 45, nil, at(9))); err != nil {
		t.Fatalf("an event outside the integration contract must be skipped, not an error: %v", err)
	}
	if len(w.msgs) != 0 {
		t.Errorf("got %d messages, want 0", len(w.msgs))
	}
}

func TestIntegrationPublisherMintsAUniqueEventIdPerMessage(t *testing.T) {
	pct1, pct2 := 90.0, 80.0
	p, w := newTestIntegrationPublisher()

	err := p.Publish(context.Background(),
		shared.NewTaskPerformanceRecorded(at(9), "task-1", shared.AssociateId("assoc-1"), shared.Pick, 45, &pct1, nil, at(9)),
		shared.NewTaskPerformanceRecorded(at(10), "task-2", shared.AssociateId("assoc-1"), shared.Pick, 50, &pct2, nil, at(10)),
	)
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if len(w.msgs) != 2 {
		t.Fatalf("got %d messages, want 2", len(w.msgs))
	}

	first, _ := decodePlain(t, w.msgs[0].Value)
	second, _ := decodePlain(t, w.msgs[1].Value)
	if first.ID() == second.ID() {
		t.Errorf("both messages carry id %q; a shared id would make a downstream consumer drop one", first.ID())
	}
}

func TestIntegrationPublisherPropagatesWriteErrors(t *testing.T) {
	w := &recordingWriter{failWith: errors.New("broker down")}
	p := &IntegrationPublisher{Writer: w, NewID: seqIDs()}

	err := p.Publish(context.Background(),
		shared.NewTaskPerformanceRecorded(at(9), "task-1", shared.AssociateId("assoc-1"), shared.Pick, 45, nil, nil, at(9)))
	if err == nil {
		t.Fatal("want the writer's error to surface")
	}
}

func TestIntegrationPublisherInjectsTraceHeaders(t *testing.T) {
	p, w := newTestIntegrationPublisher()

	event := shared.NewTaskPerformanceRecorded(at(9), "task-1", shared.AssociateId("assoc-1"), shared.Pick, 45, nil, nil, at(9))
	if err := p.Publish(context.Background(), event); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	// With no global propagator configured in a unit test the header
	// slice is legitimately empty; what must hold is that the message
	// carries a non-nil header slice for the propagator to write into.
	if w.msgs[0].Headers == nil {
		t.Error("message headers are nil; the trace propagator has nowhere to inject")
	}
}

func TestIntegrationPublisherEncodeDoesNotWrite(t *testing.T) {
	p, w := newTestIntegrationPublisher()

	msgs, err := p.Encode(context.Background(),
		shared.NewTaskPerformanceRecorded(at(9), "task-1", shared.AssociateId("assoc-1"), shared.Pick, 45, nil, nil, at(9)),
		unknownEvent{},
	)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if len(w.msgs) != 0 {
		t.Fatalf("Encode must not write; the fake writer saw %d messages", len(w.msgs))
	}
	if len(msgs) != 1 {
		t.Fatalf("got %d encoded messages, want 1 (the unknown event is skipped)", len(msgs))
	}
	m := msgs[0]
	if m.Topic != cloudevents.TopicLaborPerformanceEvents {
		t.Errorf("topic = %q, want %q", m.Topic, cloudevents.TopicLaborPerformanceEvents)
	}
	if m.EventType != cloudevents.TypeTaskPerformanceRecorded {
		t.Errorf("type = %q, want %q", m.EventType, cloudevents.TypeTaskPerformanceRecorded)
	}
}

// Encode and Publish must agree byte-for-byte on the value: the outbox
// stores what Encode returns and the direct path writes what Publish
// builds.
func TestIntegrationPublisherPublishWritesExactlyWhatEncodeProduces(t *testing.T) {
	event := shared.NewTaskPerformanceRecorded(at(9), "task-1", shared.AssociateId("assoc-1"), shared.Pick, 45, nil, nil, at(9))

	encoder := &IntegrationPublisher{NewID: func() string { return "evt-fixed" }}
	encoded, err := encoder.Encode(context.Background(), event)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	w := &recordingWriter{}
	publisher := &IntegrationPublisher{Writer: w, NewID: func() string { return "evt-fixed" }}
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

func TestIntegrationPublisherSatisfiesEncoderAndEventPublisher(t *testing.T) {
	var _ Encoder = (*IntegrationPublisher)(nil)
}

func TestIntegrationPublisherCloseWithFakeWriterIsANoOp(t *testing.T) {
	if err := (&IntegrationPublisher{Writer: &recordingWriter{}}).Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

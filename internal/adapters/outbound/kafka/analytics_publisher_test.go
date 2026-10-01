package kafka

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	ce "github.com/cloudevents/sdk-go/v2/event"
	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/labor-performance/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/labor-performance/internal/domain/shared"
)

// recordingWriter captures published messages instead of talking to a
// broker.
type recordingWriter struct {
	msgs     []kafkago.Message
	failWith error
}

func (w *recordingWriter) WriteMessages(_ context.Context, msgs ...kafkago.Message) error {
	if w.failWith != nil {
		return w.failWith
	}
	w.msgs = append(w.msgs, msgs...)
	return nil
}

// seqIDs mints predictable CloudEvents ids so assertions can name
// them.
func seqIDs() func() string {
	n := 0
	return func() string {
		n++
		return "evt-" + string(rune('0'+n))
	}
}

func newTestPublisher() (*AnalyticsPublisher, *recordingWriter) {
	w := &recordingWriter{}
	return &AnalyticsPublisher{Writer: w, NewID: seqIDs()}, w
}

func at(h int) time.Time { return time.Date(2026, 9, 5, h, 0, 0, 0, time.UTC) }

func decode(t *testing.T, msg kafkago.Message) (ce.Event, map[string]any) {
	t.Helper()
	return decodeCloudEvent(t, msg.Value)
}

func decodeCloudEvent(t *testing.T, value []byte) (ce.Event, map[string]any) {
	t.Helper()
	env, err := cloudevents.Decode(value)
	if err != nil {
		t.Fatalf("decode cloudevent: %v", err)
	}
	var data map[string]any
	if err := env.DataAs(&data); err != nil {
		t.Fatalf("decode data: %v", err)
	}
	return env, data
}

// publishCase is one row of TestAnalyticsPublisherPublishesEachDomainEvent's
// table: a domain event, the CloudEvent the analytics topic must carry,
// and the payload assertions for its data block.
type publishCase struct {
	name          string
	event         shared.DomainEvent
	wantEventType string
	wantKey       string
	assertData    func(t *testing.T, data map[string]any)
}

func TestAnalyticsPublisherPublishesEachDomainEvent(t *testing.T) {
	pct := 86.5

	tests := []publishCase{
		{
			name:          "LaborStandardDefined",
			event:         shared.NewLaborStandardDefined(at(9), "std-1", shared.Pick, 45, nil, at(9)),
			wantEventType: cloudevents.TypeLaborStandardDefined,
			wantKey:       "PICK",
			assertData:    assertStandardDefinedData,
		},
		{
			name: "LaborStandardDefined with a declared travel component",
			event: func() shared.DomainEvent {
				travel := int64(15)
				return shared.NewLaborStandardDefined(at(9), "std-1", shared.Pick, 45, &travel, at(9))
			}(),
			wantEventType: cloudevents.TypeLaborStandardDefined,
			wantKey:       "PICK",
			assertData:    assertStandardDefinedTravelData,
		},
		{
			name:          "LaborStandardRevised",
			event:         shared.NewLaborStandardRevised(at(10), "std-2", shared.Pick, 45, 40, nil, at(10)),
			wantEventType: cloudevents.TypeLaborStandardRevised,
			wantKey:       "PICK",
			assertData:    assertStandardRevisedData,
		},
		{
			name: "TaskPerformanceRecorded",
			event: shared.NewTaskPerformanceRecorded(
				at(11), "task-1", shared.AssociateId("assoc-1"), shared.Pack, 52, &pct, nil, at(9)),
			wantEventType: cloudevents.TypeTaskPerformanceRecorded,
			wantKey:       "PACK",
			assertData:    assertTaskPerformanceRecordedData,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) { assertPublishedEnvelope(t, tc) })
	}
}

// assertPublishedEnvelope publishes tc's event through a fresh publisher and
// pins the CloudEvent, partition key, and payload the analytics topic must
// carry for it.
func assertPublishedEnvelope(t *testing.T, tc publishCase) {
	t.Helper()
	p, w := newTestPublisher()

	if err := p.Publish(context.Background(), tc.event); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if len(w.msgs) != 1 {
		t.Fatalf("got %d messages, want 1", len(w.msgs))
	}

	env, data := decode(t, w.msgs[0])
	if env.Type() != tc.wantEventType {
		t.Errorf("type = %q, want %q", env.Type(), tc.wantEventType)
	}
	if env.Source() != cloudevents.Source {
		t.Errorf("source = %q, want %q", env.Source(), cloudevents.Source)
	}
	if env.SpecVersion() != "1.0" || env.DataContentType() != "application/json" {
		t.Errorf("specversion/datacontenttype = %q/%q", env.SpecVersion(), env.DataContentType())
	}
	if want := cloudevents.DataSchema(cloudevents.StreamAnalytics, eventNameOf(tc.wantEventType), 1); env.DataSchema() != want {
		t.Errorf("dataschema = %q, want %q", env.DataSchema(), want)
	}
	if env.Subject() == "" {
		t.Error("subject is empty")
	}
	assertContentTypeHeader(t, w.msgs[0].Headers)
	if env.ID() == "" {
		t.Error("id is empty; it is the projection's idempotency key")
	}
	if !env.Time().Equal(tc.event.OccurredAt()) {
		t.Errorf("time = %v, want %v", env.Time(), tc.event.OccurredAt())
	}
	if got := string(w.msgs[0].Key); got != tc.wantKey {
		t.Errorf("partition key = %q, want %q", got, tc.wantKey)
	}
	tc.assertData(t, data)
}

// assertStandardDefinedData pins a LaborStandardDefined payload without a
// travel component: the field must be OMITTED, not zero.
func assertStandardDefinedData(t *testing.T, data map[string]any) {
	t.Helper()
	if data["expected_seconds"] != float64(45) {
		t.Errorf("expected_seconds = %v, want 45", data["expected_seconds"])
	}
	if data["task_type"] != "PICK" {
		t.Errorf("task_type = %v, want PICK", data["task_type"])
	}
	if _, present := data["travel_component_seconds"]; present {
		t.Errorf("expected travel_component_seconds to be omitted when nil, got %v", data["travel_component_seconds"])
	}
}

// assertStandardDefinedTravelData pins a LaborStandardDefined payload that
// declares a travel component.
func assertStandardDefinedTravelData(t *testing.T, data map[string]any) {
	t.Helper()
	if data["travel_component_seconds"] != float64(15) {
		t.Errorf("travel_component_seconds = %v, want 15", data["travel_component_seconds"])
	}
}

// assertStandardRevisedData pins a LaborStandardRevised payload.
func assertStandardRevisedData(t *testing.T, data map[string]any) {
	t.Helper()
	if data["previous_expected_seconds"] != float64(45) {
		t.Errorf("previous_expected_seconds = %v, want 45", data["previous_expected_seconds"])
	}
	if data["expected_seconds"] != float64(40) {
		t.Errorf("expected_seconds = %v, want 40", data["expected_seconds"])
	}
	if _, present := data["travel_component_seconds"]; present {
		t.Errorf("expected travel_component_seconds to be omitted when nil, got %v", data["travel_component_seconds"])
	}
}

// assertTaskPerformanceRecordedData pins a TaskPerformanceRecorded payload.
func assertTaskPerformanceRecordedData(t *testing.T, data map[string]any) {
	t.Helper()
	if data["task_id"] != "task-1" {
		t.Errorf("task_id = %v, want task-1", data["task_id"])
	}
	if data["efficiency_pct"] != 86.5 {
		t.Errorf("efficiency_pct = %v, want 86.5", data["efficiency_pct"])
	}
	if data["actual_seconds"] != float64(52) {
		t.Errorf("actual_seconds = %v, want 52", data["actual_seconds"])
	}
	// completed_at is the business time and must travel
	// distinctly from the CloudEvents `time` attribute.
	if data["completed_at"] != at(9).Format(time.RFC3339) {
		t.Errorf("completed_at = %v, want %v", data["completed_at"], at(9).Format(time.RFC3339))
	}
}

func TestAnalyticsPublisherPreservesNilEfficiency(t *testing.T) {
	p, w := newTestPublisher()

	// An unscorable task. The nil must reach the wire as JSON null so
	// the projector can tell "unscorable" from "0% efficient".
	event := shared.NewTaskPerformanceRecorded(
		at(9), "task-1", shared.AssociateId(""), shared.TaskType(""), 0, nil, nil, at(9))

	if err := p.Publish(context.Background(), event); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	_, data := decode(t, w.msgs[0])
	v, present := data["efficiency_pct"]
	if !present {
		t.Fatal("efficiency_pct is absent from the payload; it must be present as an explicit null")
	}
	if v != nil {
		t.Errorf("efficiency_pct = %v, want null", v)
	}
	if data["associate_id"] != "" {
		t.Errorf("associate_id = %v, want the empty robot-station value", data["associate_id"])
	}
}

func TestAnalyticsPublisherMintsAUniqueEventIdPerMessage(t *testing.T) {
	p, w := newTestPublisher()

	// Two events for the SAME task type: their CloudEvents ids must
	// still differ, since the id — not task id or task type — is the
	// projection's dedup key.
	err := p.Publish(context.Background(),
		shared.NewLaborStandardDefined(at(9), "std-1", shared.Pick, 45, nil, at(9)),
		shared.NewLaborStandardRevised(at(10), "std-2", shared.Pick, 45, 40, nil, at(10)),
	)
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if len(w.msgs) != 2 {
		t.Fatalf("got %d messages, want 2", len(w.msgs))
	}

	first, _ := decode(t, w.msgs[0])
	second, _ := decode(t, w.msgs[1])
	if first.ID() == second.ID() {
		t.Errorf("both messages carry id %q; a shared id would make the projector drop one", first.ID())
	}
}

func TestAnalyticsPublisherSkipsEventsOutsideTheContract(t *testing.T) {
	p, w := newTestPublisher()

	if err := p.Publish(context.Background(), unknownEvent{}); err != nil {
		t.Fatalf("an event outside the analytics contract must be skipped, not an error: %v", err)
	}
	if len(w.msgs) != 0 {
		t.Errorf("got %d messages, want 0", len(w.msgs))
	}
}

func TestAnalyticsPublisherPropagatesWriteErrors(t *testing.T) {
	w := &recordingWriter{failWith: errors.New("broker down")}
	p := &AnalyticsPublisher{Writer: w, NewID: seqIDs()}

	err := p.Publish(context.Background(), shared.NewLaborStandardDefined(at(9), "std-1", shared.Pick, 45, nil, at(9)))
	if err == nil {
		t.Fatal("want the writer's error to surface")
	}
}

func TestAnalyticsPublisherInjectsTraceHeaders(t *testing.T) {
	p, w := newTestPublisher()

	if err := p.Publish(context.Background(), shared.NewLaborStandardDefined(at(9), "std-1", shared.Pick, 45, nil, at(9))); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	// With no global propagator configured in a unit test the header
	// slice is legitimately empty; what must hold is that the message
	// carries a non-nil header slice for the propagator to write into.
	if w.msgs[0].Headers == nil {
		t.Error("message headers are nil; the trace propagator has nowhere to inject")
	}
}

func TestFanOutPublisher(t *testing.T) {
	event := shared.NewLaborStandardDefined(at(9), "std-1", shared.Pick, 45, nil, at(9))

	t.Run("forwards to every publisher", func(t *testing.T) {
		a, b := &countingPublisher{}, &countingPublisher{}
		if err := NewFanOutPublisher(a, b).Publish(context.Background(), event); err != nil {
			t.Fatalf("Publish: %v", err)
		}
		if a.count != 1 || b.count != 1 {
			t.Errorf("publish counts = %d, %d; want 1, 1", a.count, b.count)
		}
	})

	t.Run("is fail-fast in publisher order", func(t *testing.T) {
		boom := errors.New("boom")
		failing := &countingPublisher{failWith: boom}
		after := &countingPublisher{}

		err := NewFanOutPublisher(failing, after).Publish(context.Background(), event)
		if !errors.Is(err, boom) {
			t.Fatalf("err = %v, want %v", err, boom)
		}
		if after.count != 0 {
			t.Error("a later publisher ran after an earlier one failed; the fan-out must stop")
		}
	})
}

type countingPublisher struct {
	count    int
	failWith error
}

func (p *countingPublisher) Publish(_ context.Context, events ...shared.DomainEvent) error {
	if p.failWith != nil {
		return p.failWith
	}
	p.count += len(events)
	return nil
}

// unknownEvent is a DomainEvent this adapter has no analytics mapping
// for, standing in for any future domain event.
type unknownEvent struct{}

func (unknownEvent) EventName() string     { return "SomethingElseHappened" }
func (unknownEvent) OccurredAt() time.Time { return at(9) }

// eventNameOf returns the PascalCase <EventName> segment of a full type.
func eventNameOf(fullType string) string {
	return fullType[strings.LastIndex(fullType, ".")+1:]
}

// assertContentTypeHeader pins the structured-mode Kafka header every
// produced message must carry.
func assertContentTypeHeader(t *testing.T, headers []kafkago.Header) {
	t.Helper()
	for _, h := range headers {
		if h.Key == "content-type" {
			if string(h.Value) != "application/cloudevents+json; charset=UTF-8" {
				t.Errorf("content-type header = %q", h.Value)
			}
			return
		}
	}
	t.Errorf("missing content-type header, got %v", headers)
}

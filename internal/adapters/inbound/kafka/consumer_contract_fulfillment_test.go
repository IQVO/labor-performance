package kafka

// Event consumer contract tests (E1-E4) for the OLTP edge: this repo's
// Consumer subscribes to fulfillment-execution's shared, fan-out topic
// warehouse.fulfillment.events and acts only on
// com.warehouse.wes.fulfillment-execution.task.TaskCompleted
// (cloudevents.TopicFulfillmentEvents / TypeFulfillmentTaskCompleted —
// ADR 0003, ADR 0021). The contract it validates against is the PINNED
// copy of fulfillment-execution's own apis/asyncapi.yaml
// (testdata/fulfillment-execution.asyncapi.yaml, registered in
// contracts/pinned.json; the pinned-contracts CI job fails when the copy
// drifts from IQVO/fulfillment-execution@develop).
//
// Golden fixtures come from the producer spec's OWN examples — every
// TaskCompleted example pinned here is a real wire document
// fulfillment-execution documents for this channel.

import (
	"context"
	"errors"
	"testing"
	"time"

	ce "github.com/cloudevents/sdk-go/v2/event"

	"github.com/claudioed/labor-performance/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/labor-performance/internal/adapters/outbound/events"
	"github.com/claudioed/labor-performance/internal/adapters/outbound/memory"
	"github.com/claudioed/labor-performance/internal/application/usecases"
	"github.com/claudioed/labor-performance/internal/domain/performance"
)

// countingPerformanceRepo counts the TaskPerformance rows the real
// RecordTaskPerformance use case persisted, so the tests can assert both
// routing (a golden event records exactly its task) and the dedupe
// contract (a replayed CloudEvents id records nothing more).
type countingPerformanceRepo struct {
	*memory.PerformanceRepo
	saved []*performance.TaskPerformance
}

func (r *countingPerformanceRepo) Save(ctx context.Context, p *performance.TaskPerformance) error {
	r.saved = append(r.saved, p)
	return r.PerformanceRepo.Save(ctx, p)
}

// newFulfillmentContractFixture mirrors consumer_test.go's newFixture
// (the REAL use case over in-memory repos) but with a save-counting repo.
func newFulfillmentContractFixture() (*Consumer, *countingPerformanceRepo) {
	repo := &countingPerformanceRepo{PerformanceRepo: memory.NewPerformanceRepo()}
	recordTaskPerformance := &usecases.RecordTaskPerformance{
		Performances: repo,
		Standards:    memory.NewStandardRepo(),
		Processed:    memory.NewProcessedEventRepo(),
		Events:       events.NewLogPublisher(nil),
		Clock:        memory.FixedClock{At: time.Date(2026, 8, 29, 8, 0, 0, 0, time.UTC)},
	}
	return &Consumer{recordTaskPerformance: recordTaskPerformance}, repo
}

// fulfillmentGoldenExamples returns the pinned producer spec's own
// TaskCompleted examples for the subscribed channel.
func fulfillmentGoldenExamples(t *testing.T) []pinnedExample {
	t.Helper()
	doc := loadPinnedAsyncAPI(t, pinnedFulfillmentSpec)
	msg := doc.message(t, "TaskCompleted")
	if len(msg.Examples) == 0 {
		t.Fatal("pinned fulfillment-execution spec documents TaskCompleted without examples")
	}
	return msg.Examples
}

// E1: the topic this repo's reader actually subscribes to (the
// cloudevents.TopicFulfillmentEvents constant NewConsumer wires into
// kafka.ReaderConfig) is documented by the producer's pinned spec as a
// 'subscribe:' channel — fleet channel-perspective: a producer's own
// topics live under 'subscribe:' — AND the message list on that channel
// documents the exact CloudEvents type this repo's decoder routes on.
func TestPinnedAsyncAPICoversSubscriptions_FulfillmentEvents(t *testing.T) {
	doc := loadPinnedAsyncAPI(t, pinnedFulfillmentSpec)

	names := doc.subscribeMessageRefs(t, cloudevents.TopicFulfillmentEvents)

	documented := map[string]bool{}
	for _, name := range names {
		msg := doc.message(t, name) // fails the test on a dangling $ref
		if c := doc.payloadConsts(t, msg.Payload)["type"]; c != "" {
			documented[c] = true
		}
	}

	// The one type handleFulfillmentEvent dispatches on must be documented.
	if !documented[cloudevents.TypeFulfillmentTaskCompleted] {
		t.Errorf("pinned fulfillment-execution spec does not document %s on the %s subscribe channel (documented: %v)",
			cloudevents.TypeFulfillmentTaskCompleted, cloudevents.TopicFulfillmentEvents, documented)
	}
	// Unknown types are skipped, not errors — so every OTHER documented
	// type on this shared fan-out topic is one this consumer must ignore.
	if len(documented) < 2 {
		t.Logf("note: pinned spec documents %d type(s) on the shared topic", len(documented))
	}
}

// E2: every TaskCompleted example the producer's pinned spec documents on
// the subscribed channel decodes through the REAL cloudevents.Decode and
// routes through the REAL handleFulfillmentEvent → RecordTaskPerformance
// path: exactly one TaskPerformance row per event, carrying the wire's
// task_id/associate/duration and keyed by the CloudEvents id (the
// dedupe key — a replayed id records nothing more).
func TestGoldenEventsDecode_FulfillmentTaskCompleted(t *testing.T) {
	for _, ex := range fulfillmentGoldenExamples(t) {
		t.Run(ex.Name, func(t *testing.T) {
			evt := decodeGoldenFulfillment(t, ex)
			consumer, repo := newFulfillmentContractFixture()
			handleGoldenFulfillment(t, consumer, evt)
			assertTaskCompletedRecorded(t, ex, evt, repo)

			// Dedupe contract: the CloudEvents id (not task_id) is the
			// ProcessedEvents gate's key — an at-least-once redelivery of
			// the same id records nothing more.
			handleGoldenFulfillment(t, consumer, evt)
			if len(repo.saved) != 1 {
				t.Errorf("replayed CloudEvents id %q recorded a second row (%d total)", evt.ID(), len(repo.saved))
			}
		})
	}
}

func decodeGoldenFulfillment(t *testing.T, ex pinnedExample) ce.Event {
	t.Helper()
	evt, err := cloudevents.Decode(goldenJSON(t, ex.Payload))
	if err != nil {
		t.Fatalf("decode golden example: %v", err)
	}
	if evt.Type() != cloudevents.TypeFulfillmentTaskCompleted {
		t.Fatalf("golden example type %q is not the routed type", evt.Type())
	}
	return evt
}

func handleGoldenFulfillment(t *testing.T, consumer *Consumer, evt ce.Event) {
	t.Helper()
	if err := consumer.handleFulfillmentEvent(context.Background(), evt); err != nil {
		t.Fatalf("handleFulfillmentEvent: %v", err)
	}
}

// assertTaskCompletedRecorded asserts the golden event produced exactly
// one TaskPerformance row carrying the wire's own facts.
func assertTaskCompletedRecorded(t *testing.T, ex pinnedExample, evt ce.Event, repo *countingPerformanceRepo) {
	t.Helper()
	if len(repo.saved) != 1 {
		t.Fatalf("golden event recorded %d TaskPerformance rows, want 1", len(repo.saved))
	}
	row := repo.saved[0]
	data := ex.Payload["data"].(map[string]any)
	if row.TaskId() != data["task_id"] {
		t.Errorf("recorded task id %q, wire says %q", row.TaskId(), data["task_id"])
	}
	if row.EventId() != ex.Payload["id"] {
		t.Errorf("recorded event id %q, wire CloudEvents id is %q", row.EventId(), ex.Payload["id"])
	}
	if want, _ := data["duration_seconds"].(int); row.ActualSeconds() != int64(want) {
		t.Errorf("recorded actual seconds %d, wire says %d", row.ActualSeconds(), want)
	}
	if !row.CompletedAt().Equal(evt.Time()) {
		t.Errorf("recorded completion %v, CloudEvents time is %v", row.CompletedAt(), evt.Time())
	}
}

// E3: every golden fixture carries the fleet-mandatory CloudEvents 1.0
// envelope (ADR 0021) — specversion 1.0, id, source, type, subject,
// time, datacontenttype application/json, dataschema
// urn:warehouse:fulfillment-execution:events:TaskCompleted:v1 — and the
// pinned spec's envelope schema requires exactly those attributes. The
// structured-mode Kafka content-type header the spec's examples carry
// must match this repo's cloudevents.MediaType constant.
func TestCloudEventsEnvelopeInvariants_Fulfillment(t *testing.T) {
	for _, ex := range fulfillmentGoldenExamples(t) {
		evt, err := cloudevents.Decode(goldenJSON(t, ex.Payload))
		if err != nil {
			t.Fatalf("decode golden example %s: %v", ex.Name, err)
		}
		if evt.SpecVersion() != cloudevents.SpecVersion {
			t.Errorf("%s: specversion %q, want %q", ex.Name, evt.SpecVersion(), cloudevents.SpecVersion)
		}
		if evt.ID() == "" || evt.Subject() == "" || evt.Source() == "" || evt.Time().IsZero() {
			t.Errorf("%s: id/source/subject/time must all be present (got %q/%q/%q/%v)",
				ex.Name, evt.ID(), evt.Source(), evt.Subject(), evt.Time())
		}
		if evt.Source() != "/warehouse/fulfillment-execution" {
			t.Errorf("%s: source %q, want /warehouse/fulfillment-execution", ex.Name, evt.Source())
		}
		if evt.DataMediaType() != cloudevents.DataContentType {
			t.Errorf("%s: datacontenttype %q, want %q", ex.Name, evt.DataMediaType(), cloudevents.DataContentType)
		}
		if evt.DataSchema() != "urn:warehouse:fulfillment-execution:events:TaskCompleted:v1" {
			t.Errorf("%s: dataschema %q", ex.Name, evt.DataSchema())
		}
		if got := ex.Headers["content-type"]; got != cloudevents.MediaType {
			t.Errorf("%s: Kafka content-type header %q, want this repo's constant %q", ex.Name, got, cloudevents.MediaType)
		}
	}

	doc := loadPinnedAsyncAPI(t, pinnedFulfillmentSpec)
	if doc.DefaultContentType != "application/cloudevents+json" {
		t.Errorf("pinned spec defaultContentType %q, want application/cloudevents+json", doc.DefaultContentType)
	}
	required := doc.envelopeRequired(t, doc.message(t, "TaskCompleted").Payload)
	for _, attr := range []string{"specversion", "id", "source", "type", "subject", "time", "datacontenttype", "dataschema", "data"} {
		if !required[attr] {
			t.Errorf("pinned spec's TaskCompleted envelope does not require %q (required: %v)", attr, required)
		}
	}
}

// E4: this consumer is a Conformist on a shared, fan-out topic — unknown
// ADDITIVE attributes in data must not fail the decode or the handling
// (the producer's own ADR-0047/0050 relocation fields are the live
// example), an unknown event TYPE is silently skipped (not an error, no
// DLQ, no row), and a message that is not a CloudEvent at all (the
// retired flat envelope) is a deterministic decode error — the exact
// signal handleMessage dead-letters once and never retries.
func TestUnknownAttributesTolerated_Fulfillment(t *testing.T) {
	golden := fulfillmentGoldenExamples(t)[0].Payload

	// Unknown additive attributes in data — scalar and nested.
	tolerant := clonePayload(golden)
	data := tolerant["data"].(map[string]any)
	data["future_additive_field"] = "unknown-to-this-consumer"
	data["nested_extension"] = map[string]any{"deep": []any{1, "two"}}
	evt, err := cloudevents.Decode(goldenJSON(t, tolerant))
	if err != nil {
		t.Fatalf("decode with unknown data attributes: %v", err)
	}
	consumer, repo := newFulfillmentContractFixture()
	if err := consumer.handleFulfillmentEvent(context.Background(), evt); err != nil {
		t.Fatalf("handle with unknown data attributes: %v", err)
	}
	if len(repo.saved) != 1 || repo.saved[0].TaskId() != data["task_id"] {
		t.Errorf("unknown data attributes changed the outcome: %d rows", len(repo.saved))
	}

	// Unknown event TYPE on the shared topic: silently skipped.
	otherType := clonePayload(golden)
	otherType["type"] = "com.warehouse.wes.fulfillment-execution.task.TaskClaimed"
	evt, err = cloudevents.Decode(goldenJSON(t, otherType))
	if err != nil {
		t.Fatalf("decode unknown-type example: %v", err)
	}
	if err := consumer.handleFulfillmentEvent(context.Background(), evt); err != nil {
		t.Fatalf("unknown event type must be skipped, not an error: %v", err)
	}
	if len(repo.saved) != 1 {
		t.Errorf("unknown event type was acted on (%d rows)", len(repo.saved))
	}

	// Not a CloudEvent (retired flat envelope): deterministic decode
	// error wrapping ErrNotCloudEvent — the DLQ-once trigger.
	_, err = cloudevents.Decode(goldenJSON(t, map[string]any{
		"task_id": "task-legacy", "station_id": "station-1", "work_unit_id": "wu-1",
	}))
	if !errors.Is(err, cloudevents.ErrNotCloudEvent) {
		t.Errorf("retired flat envelope decoded as %v, want ErrNotCloudEvent (the DLQ trigger)", err)
	}
}

// clonePayload deep-copies a golden example payload so a test can mutate
// it without touching the shared fixture.
func clonePayload(p map[string]any) map[string]any {
	return cloneMap(p).(map[string]any)
}

func cloneMap(v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, val := range t {
			out[k] = cloneMap(val)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, val := range t {
			out[i] = cloneMap(val)
		}
		return out
	default:
		return v
	}
}

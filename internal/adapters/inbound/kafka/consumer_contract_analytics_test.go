package kafka

// Event consumer contract tests (E1-E4) for the ANALYTICS edge: this
// repo's own cmd/labor-projector (AnalyticsConsumer) subscribes to the
// topic this service itself publishes through its transactional outbox
// relay — warehouse.labor-performance.analytics (ADR 0007) — and folds
// LaborStandardDefined, LaborStandardRevised and TaskPerformanceRecorded
// into the report projection, deduped on the CloudEvents id. The
// contract is validated against the PINNED copy of this repo's OWN
// apis/asyncapi.yaml (testdata/labor-performance.asyncapi.yaml,
// registered in contracts/pinned.json): a self-pin, so the projector's
// decode contract and the publisher's spec cannot drift apart silently.
//
// Golden fixtures come from that spec's OWN examples — the same
// documents the publisher documents for the channel.

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/claudioed/labor-performance/internal/adapters/kafka/cloudevents"
)

// analyticsMessages maps the message names the analytics channel's
// subscribe operation documents to the CloudEvents type the projector's
// isProjecting switch routes on.
var analyticsMessages = map[string]string{
	"LaborStandardDefined":    cloudevents.TypeLaborStandardDefined,
	"LaborStandardRevised":    cloudevents.TypeLaborStandardRevised,
	"TaskPerformanceRecorded": cloudevents.TypeTaskPerformanceRecorded,
}

func newAnalyticsContractConsumer() (*AnalyticsConsumer, *recordingProjection, *fakeProcessedEvents) {
	proj := &recordingProjection{}
	gate := newFakeProcessedEvents()
	return &AnalyticsConsumer{
		Projection: proj,
		Processed:  gate,
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	}, proj, gate
}

// analyticsGoldenExamples returns the pinned spec's own examples for one
// analytics message.
func analyticsGoldenExamples(t *testing.T, message string) []pinnedExample {
	t.Helper()
	doc := loadPinnedAsyncAPI(t, pinnedAnalyticsSpec)
	msg := doc.message(t, message)
	if len(msg.Examples) == 0 {
		t.Fatalf("pinned spec documents %s without examples", message)
	}
	return msg.Examples
}

// E1: the topic the projector's reader actually subscribes to (the
// cloudevents.TopicLaborPerformanceAnalytics constant
// NewAnalyticsConsumer wires into kafka.ReaderConfig) is documented by
// the pinned spec of its producer — this repo itself — and the messages
// on that channel cover every CloudEvents type the projector routes on.
func TestPinnedAsyncAPICoversSubscriptions_Analytics(t *testing.T) {
	doc := loadPinnedAsyncAPI(t, pinnedAnalyticsSpec)

	names := doc.subscribeMessageRefs(t, cloudevents.TopicLaborPerformanceAnalytics)

	documented := map[string]bool{}
	for _, name := range names {
		msg := doc.message(t, name) // fails the test on a dangling $ref
		if c := doc.payloadConsts(t, msg.Payload)["type"]; c != "" {
			documented[c] = true
		}
	}

	for message, want := range analyticsMessages {
		if !documented[want] {
			t.Errorf("pinned spec does not document %s (%s) on the %s channel (documented: %v)",
				message, want, cloudevents.TopicLaborPerformanceAnalytics, documented)
		}
	}
}

// E2: every example the pinned spec documents for each analytics message
// decodes through the REAL cloudevents.Decode and routes through the
// REAL AnalyticsConsumer.HandleMessage to the projection method for its
// type, deduped on the CloudEvents id (a replay applies nothing).
func TestGoldenEventsDecode_Analytics(t *testing.T) {
	ctx := context.Background()
	for message := range analyticsMessages {
		asserter := analyticsAsserters[message]
		for _, ex := range analyticsGoldenExamples(t, message) {
			t.Run(message+"/"+ex.Name, func(t *testing.T) {
				raw := goldenJSON(t, ex.Payload)
				consumer, proj, gate := newAnalyticsContractConsumer()

				if err := consumer.HandleMessage(ctx, raw); err != nil {
					t.Fatalf("HandleMessage: %v", err)
				}
				asserter(t, ex, proj)
				id, _ := ex.Payload["id"].(string)
				if _, ok := gate.seen[id]; !ok {
					t.Errorf("golden event %q was never claimed in the processed gate", id)
				}

				// Dedupe contract: the CloudEvents id is the ProcessedEvents
				// gate's key — an at-least-once redelivery applies nothing.
				if err := consumer.HandleMessage(ctx, raw); err != nil {
					t.Fatalf("replayed %s: %v", ex.Name, err)
				}
				if got := appliedFacts(proj); got != 1 {
					t.Errorf("replayed CloudEvents id %q applied a second fact (%d total)", id, got)
				}
			})
		}
	}
}

// analyticsAsserters maps a pinned message name to the assertion that a
// freshly-handled golden example of it produced exactly one applied fact
// of the right shape.
var analyticsAsserters = map[string]func(t *testing.T, ex pinnedExample, proj *recordingProjection){
	"LaborStandardDefined":    assertDefinedApplied,
	"LaborStandardRevised":    assertRevisedApplied,
	"TaskPerformanceRecorded": assertRecordedApplied,
}

func appliedFacts(p *recordingProjection) int {
	return len(p.defined) + len(p.revised) + len(p.tasks)
}

func assertDefinedApplied(t *testing.T, ex pinnedExample, proj *recordingProjection) {
	t.Helper()
	if len(proj.defined) != 1 {
		t.Fatalf("%s applied %d LaborStandardDefined facts, want 1", ex.Name, len(proj.defined))
	}
	assertStandardFact(t, ex, proj.defined[0], proj)
}

func assertRevisedApplied(t *testing.T, ex pinnedExample, proj *recordingProjection) {
	t.Helper()
	if len(proj.revised) != 1 {
		t.Fatalf("%s applied %d LaborStandardRevised facts, want 1", ex.Name, len(proj.revised))
	}
	assertStandardFact(t, ex, proj.revised[0], proj)
}

func assertStandardFact(t *testing.T, ex pinnedExample, applied appliedStandard, proj *recordingProjection) {
	t.Helper()
	data, _ := ex.Payload["data"].(map[string]any)
	id, _ := ex.Payload["id"].(string)
	if applied.eventId != id {
		t.Errorf("%s: applied event id %q, wire CloudEvents id is %q", ex.Name, applied.eventId, id)
	}
	if applied.fact.TaskType != data["task_type"] {
		t.Errorf("%s: applied task type %q, wire says %q", ex.Name, applied.fact.TaskType, data["task_type"])
	}
	if want, _ := data["expected_seconds"].(int); applied.fact.ExpectedSeconds != int64(want) {
		t.Errorf("%s: applied expected seconds %d, wire says %d", ex.Name, applied.fact.ExpectedSeconds, want)
	}
	assertBusinessTime(t, ex.Name, "effective_from", applied.fact.EffectiveFrom, data)
}

func assertRecordedApplied(t *testing.T, ex pinnedExample, proj *recordingProjection) {
	t.Helper()
	if len(proj.tasks) != 1 {
		t.Fatalf("%s applied %d TaskPerformanceRecorded facts, want 1", ex.Name, len(proj.tasks))
	}
	applied := proj.tasks[0]
	data, _ := ex.Payload["data"].(map[string]any)
	id, _ := ex.Payload["id"].(string)
	if applied.eventId != id {
		t.Errorf("applied event id %q, wire CloudEvents id is %q", applied.eventId, id)
	}
	if applied.fact.TaskType != data["task_type"] {
		t.Errorf("applied task type %q, wire says %q", applied.fact.TaskType, data["task_type"])
	}
	if want, _ := data["actual_seconds"].(int); applied.fact.ActualSeconds != int64(want) {
		t.Errorf("applied actual seconds %d, wire says %d", applied.fact.ActualSeconds, want)
	}
	assertEfficiencyPct(t, ex.Name, applied.fact.EfficiencyPct, data)
	assertBusinessTime(t, ex.Name, "completed_at", applied.fact.CompletedAt, data)
}

// assertEfficiencyPct checks the nullable score: null on the wire (a real,
// expected "unscorable" fact) must decode to nil, never coerce to 0.
func assertEfficiencyPct(t *testing.T, name string, got *float64, data map[string]any) {
	t.Helper()
	switch want := data["efficiency_pct"].(type) {
	case nil:
		if got != nil {
			t.Errorf("%s: null efficiency_pct decoded as %v, want nil", name, *got)
		}
	case float64:
		if got == nil || *got != want {
			t.Errorf("%s: efficiency_pct %v decoded as %v", name, want, got)
		}
	}
}

func assertBusinessTime(t *testing.T, name, field string, got time.Time, data map[string]any) {
	t.Helper()
	wantStr, _ := data[field].(string)
	want, err := time.Parse(time.RFC3339, wantStr)
	if err != nil {
		t.Fatalf("%s: golden example %s is not RFC3339: %v", name, field, err)
	}
	if !got.Equal(want) {
		t.Errorf("%s: applied %s %v, wire says %v", name, field, got, want)
	}
}

// E3: every golden fixture carries the fleet-mandatory CloudEvents 1.0
// envelope (ADR 0021) — specversion 1.0, id, source /warehouse/
// labor-performance, type com.warehouse.wes.labor-performance.*,
// subject, time, datacontenttype application/json, dataschema
// urn:warehouse:labor-performance:analytics:<EventName>:v1 — and the
// pinned spec's envelope schema requires those attributes.
func TestCloudEventsEnvelopeInvariants_Analytics(t *testing.T) {
	for message, wantType := range analyticsMessages {
		for _, ex := range analyticsGoldenExamples(t, message) {
			assertGoldenEnvelope(t, message, wantType, ex)
		}
	}

	doc := loadPinnedAsyncAPI(t, pinnedAnalyticsSpec)
	if doc.DefaultContentType != "application/cloudevents+json" {
		t.Errorf("pinned spec defaultContentType %q, want application/cloudevents+json", doc.DefaultContentType)
	}
	for message := range analyticsMessages {
		assertMessageContractDocumented(t, doc, message)
	}
}

// assertGoldenEnvelope decodes one golden example and asserts every
// fleet-mandatory CloudEvents 1.0 attribute on it.
func assertGoldenEnvelope(t *testing.T, message, wantType string, ex pinnedExample) {
	t.Helper()
	evt, err := cloudevents.Decode(goldenJSON(t, ex.Payload))
	if err != nil {
		t.Fatalf("decode %s/%s: %v", message, ex.Name, err)
	}
	if evt.SpecVersion() != cloudevents.SpecVersion {
		t.Errorf("%s: specversion %q, want %q", ex.Name, evt.SpecVersion(), cloudevents.SpecVersion)
	}
	if evt.ID() == "" || evt.Subject() == "" || evt.Time().IsZero() {
		t.Errorf("%s: id/subject/time must all be present", ex.Name)
	}
	if evt.Source() != cloudevents.Source {
		t.Errorf("%s: source %q, want this service's %q", ex.Name, evt.Source(), cloudevents.Source)
	}
	if evt.Type() != wantType {
		t.Errorf("%s: type %q, want %q", ex.Name, evt.Type(), wantType)
	}
	if evt.DataMediaType() != cloudevents.DataContentType {
		t.Errorf("%s: datacontenttype %q, want %q", ex.Name, evt.DataMediaType(), cloudevents.DataContentType)
	}
	if evt.DataSchema() != cloudevents.DataSchema(cloudevents.StreamAnalytics, message, 1) {
		t.Errorf("%s: dataschema %q, want %q", ex.Name, evt.DataSchema(),
			cloudevents.DataSchema(cloudevents.StreamAnalytics, message, 1))
	}
}

// assertMessageContractDocumented asserts the spec-side envelope contract
// for one analytics message: structured-mode contentType, agreement with
// this service's publisher header constant, and a required-attribute list
// covering the whole CloudEvents envelope.
func assertMessageContractDocumented(t *testing.T, doc *pinnedAsyncAPI, message string) {
	t.Helper()
	msg := doc.message(t, message)
	if msg.ContentType != "application/cloudevents+json" {
		t.Errorf("message %s contentType %q, want application/cloudevents+json", message, msg.ContentType)
	}
	// The structured-mode Kafka header this service's own publisher
	// stamps (cloudevents.MediaType) must agree with the spec's
	// message contentType — same media type, plus the charset param.
	if msg.ContentType != "" && !startsWith(cloudevents.MediaType, msg.ContentType) {
		t.Errorf("publisher header constant %q disagrees with spec contentType %q", cloudevents.MediaType, msg.ContentType)
	}
	for _, attr := range []string{"specversion", "id", "source", "type", "subject", "time", "datacontenttype", "dataschema", "data"} {
		if !doc.envelopeRequired(t, msg.Payload)[attr] {
			t.Errorf("message %s envelope does not require %q", message, attr)
		}
	}
}

func startsWith(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}

// E4: the projector is a Conformist on its own stream — unknown
// ADDITIVE attributes in data must not fail the decode or the apply; an
// unknown event TYPE is ignored and NOT marked processed (so widening
// the contract later can replay it); a message that is not a CloudEvent
// (or whose data cannot be decoded) is skipped with a WARN — nil error,
// no DLQ — never retried and never applied.
func TestUnknownAttributesTolerated_Analytics(t *testing.T) {
	ctx := context.Background()
	golden := analyticsGoldenExamples(t, "TaskPerformanceRecorded")[0].Payload

	tolerant := clonePayload(golden)
	data := tolerant["data"].(map[string]any)
	data["future_additive_field"] = "unknown-to-this-projector"
	data["nested_extension"] = map[string]any{"deep": true}
	consumer, proj, _ := newAnalyticsContractConsumer()
	if err := consumer.HandleMessage(ctx, goldenJSON(t, tolerant)); err != nil {
		t.Fatalf("HandleMessage with unknown data attributes: %v", err)
	}
	if len(proj.tasks) != 1 || proj.tasks[0].fact.TaskType != data["task_type"] {
		t.Fatalf("unknown data attributes changed the outcome (%d facts)", len(proj.tasks))
	}

	// Unknown event TYPE: ignored, and NOT marked processed.
	otherType := clonePayload(golden)
	otherType["type"] = "com.warehouse.wes.labor-performance.performance.TaskPerformanceRetired"
	consumer, proj, gate := newAnalyticsContractConsumer()
	if err := consumer.HandleMessage(ctx, goldenJSON(t, otherType)); err != nil {
		t.Fatalf("unknown event type must be ignored, not an error: %v", err)
	}
	if got := len(proj.defined) + len(proj.revised) + len(proj.tasks); got != 0 {
		t.Errorf("unknown event type was applied (%d facts)", got)
	}
	if len(gate.seen) != 0 {
		t.Errorf("unknown event type was marked processed (%v), blocking a future replay", gate.seen)
	}

	// Not a CloudEvent (retired flat envelope): skipped with a WARN
	// (nil error — this consumer has no DLQ), never applied.
	consumer, proj, gate = newAnalyticsContractConsumer()
	if err := consumer.HandleMessage(ctx, goldenJSON(t, map[string]any{
		"task_id": "task-legacy", "associate_id": "assoc-1", "task_type": "PICK",
	})); err != nil {
		t.Fatalf("invalid CloudEvent must be skipped, not an error: %v", err)
	}
	if got := len(proj.defined) + len(proj.revised) + len(proj.tasks); got != 0 {
		t.Errorf("invalid CloudEvent was applied (%d facts)", got)
	}
	if len(gate.seen) != 0 {
		t.Errorf("invalid CloudEvent was marked processed (%v)", gate.seen)
	}
}

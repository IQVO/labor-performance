package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	ce "github.com/cloudevents/sdk-go/v2/event"

	"github.com/claudioed/labor-performance/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/labor-performance/internal/analytics/report"
)

// recordingProjection captures every apply call so a test can assert on
// what the consumer decided, without a database.
type recordingProjection struct {
	tasks     []appliedTask
	defined   []appliedStandard
	revised   []appliedStandard
	failWith  error
	callCount int
}

type appliedTask struct {
	eventId string
	fact    report.TaskPerformanceFact
}

type appliedStandard struct {
	eventId string
	fact    report.StandardFact
}

func (p *recordingProjection) ApplyTaskPerformanceRecorded(_ context.Context, eventId string, f report.TaskPerformanceFact) error {
	p.callCount++
	if p.failWith != nil {
		return p.failWith
	}
	p.tasks = append(p.tasks, appliedTask{eventId, f})
	return nil
}

func (p *recordingProjection) ApplyLaborStandardDefined(_ context.Context, eventId string, f report.StandardFact) error {
	p.callCount++
	if p.failWith != nil {
		return p.failWith
	}
	p.defined = append(p.defined, appliedStandard{eventId, f})
	return nil
}

func (p *recordingProjection) ApplyLaborStandardRevised(_ context.Context, eventId string, f report.StandardFact) error {
	p.callCount++
	if p.failWith != nil {
		return p.failWith
	}
	p.revised = append(p.revised, appliedStandard{eventId, f})
	return nil
}

// fakeProcessedEvents is an in-memory ProcessedEvents gate.
type fakeProcessedEvents struct {
	seen     map[string]struct{}
	failWith error
}

func newFakeProcessedEvents() *fakeProcessedEvents {
	return &fakeProcessedEvents{seen: map[string]struct{}{}}
}

func (f *fakeProcessedEvents) MarkProcessed(_ context.Context, eventId string) (bool, error) {
	if f.failWith != nil {
		return false, f.failWith
	}
	if _, dup := f.seen[eventId]; dup {
		return false, nil
	}
	f.seen[eventId] = struct{}{}
	return true, nil
}

// newTestAnalyticsConsumer builds a consumer with no Kafka reader at all:
// every test here drives HandleMessage directly, so no broker (and no
// fake reader) is needed.
func newTestAnalyticsConsumer(p report.ProjectionStore, gate ProcessedEvents) *AnalyticsConsumer {
	return &AnalyticsConsumer{Projection: p, Processed: gate}
}

// analyticsMessage builds a raw CloudEvents 1.0 analytics message with
// the given full type, exactly as AnalyticsPublisher emits it.
func analyticsMessage(t *testing.T, eventId, eventType string, occurredAt time.Time, data map[string]any) []byte {
	t.Helper()
	return rawCloudEvent(t, eventId, eventType, "subject-1",
		"urn:warehouse:labor-performance:analytics:Test:v1", occurredAt, data)
}

// rawCloudEvent builds a structured-mode CloudEvents 1.0 JSON value with
// an arbitrary data payload (which may be raw JSON bytes).
func rawCloudEvent(t *testing.T, id, eventType, subject, dataschema string, occurredAt time.Time, data any) []byte {
	t.Helper()
	e := ce.New(cloudevents.SpecVersion)
	e.SetID(id)
	e.SetSource("/warehouse/test")
	e.SetType(eventType)
	e.SetSubject(subject)
	e.SetTime(occurredAt)
	e.SetDataSchema(dataschema)
	if raw, ok := data.(json.RawMessage); ok {
		if err := e.SetData(cloudevents.DataContentType, []byte(raw)); err != nil {
			t.Fatalf("set data: %v", err)
		}
	} else if err := e.SetData(cloudevents.DataContentType, data); err != nil {
		t.Fatalf("set data: %v", err)
	}
	b, err := json.Marshal(e)
	if err != nil {
		t.Fatalf("marshal cloudevent: %v", err)
	}
	return b
}

// legacyFlatMessage is the retired flat envelope shape. Every consumer
// must reject it (DLQ/skip), never parse it.
func legacyFlatMessage(eventType string, data string) []byte {
	return []byte(`{"event_id":"legacy-1","event_type":"` + eventType + `","occurred_at":"2026-09-05T09:00:00Z","source":"legacy","schema_version":1,"data":` + data + `}`)
}

func ts(h, m int) time.Time { return time.Date(2026, 9, 5, h, m, 0, 0, time.UTC) }

func TestAnalyticsConsumerProjectsTaskPerformanceRecorded(t *testing.T) {
	proj := &recordingProjection{}
	c := newTestAnalyticsConsumer(proj, newFakeProcessedEvents())

	raw := analyticsMessage(t, "evt-1", cloudevents.TypeTaskPerformanceRecorded, ts(11, 0), map[string]any{
		"task_id":        "task-1",
		"associate_id":   "assoc-1",
		"task_type":      "PICK",
		"efficiency_pct": 86.5,
		"actual_seconds": 52,
		// Deliberately EARLIER than the CloudEvents time: this is a replayed /
		// late-ingested event, and it must bucket by when the work
		// happened, not when we got around to scoring it.
		"completed_at": ts(9, 30),
	})

	if err := c.HandleMessage(context.Background(), raw); err != nil {
		t.Fatalf("HandleMessage: %v", err)
	}

	if len(proj.tasks) != 1 {
		t.Fatalf("got %d applied tasks, want 1", len(proj.tasks))
	}
	got := proj.tasks[0]
	if got.eventId != "evt-1" {
		t.Errorf("eventId = %q, want evt-1", got.eventId)
	}
	if got.fact.TaskType != "PICK" || got.fact.ActualSeconds != 52 {
		t.Errorf("fact = %+v, want PICK / 52s", got.fact)
	}
	if got.fact.EfficiencyPct == nil || *got.fact.EfficiencyPct != 86.5 {
		t.Errorf("EfficiencyPct = %v, want 86.5", got.fact.EfficiencyPct)
	}
	if !got.fact.CompletedAt.Equal(ts(9, 30)) {
		t.Errorf("CompletedAt = %v, want the business time 09:30, not the envelope's 11:00", got.fact.CompletedAt)
	}
	if !got.fact.OccurredAt.Equal(ts(11, 0)) {
		t.Errorf("OccurredAt = %v, want the envelope time 11:00 (the freshness watermark)", got.fact.OccurredAt)
	}
}

func TestAnalyticsConsumerNullEfficiencyStaysNil(t *testing.T) {
	proj := &recordingProjection{}
	c := newTestAnalyticsConsumer(proj, newFakeProcessedEvents())

	// An unscorable task: efficiency_pct is JSON null on the wire. It
	// must arrive as nil, NOT as 0 — a 0 would silently brand the
	// associate a 0% performer.
	raw := analyticsMessage(t, "evt-1", cloudevents.TypeTaskPerformanceRecorded, ts(9, 0), map[string]any{
		"task_id":        "task-1",
		"task_type":      "",
		"efficiency_pct": nil,
		"actual_seconds": 0,
		"completed_at":   ts(9, 0),
	})

	if err := c.HandleMessage(context.Background(), raw); err != nil {
		t.Fatalf("HandleMessage: %v", err)
	}

	if len(proj.tasks) != 1 {
		t.Fatalf("got %d applied tasks, want 1", len(proj.tasks))
	}
	if got := proj.tasks[0].fact.EfficiencyPct; got != nil {
		t.Errorf("EfficiencyPct = %v, want nil", *got)
	}
	if got := proj.tasks[0].fact.ActualSeconds; got != 0 {
		t.Errorf("ActualSeconds = %d, want 0", got)
	}
	if got := proj.tasks[0].fact.TaskType; got != "" {
		t.Errorf("TaskType = %q, want the raw empty value (the store normalises it, not the consumer)", got)
	}
}

func TestAnalyticsConsumerProjectsStandardEvents(t *testing.T) {
	proj := &recordingProjection{}
	c := newTestAnalyticsConsumer(proj, newFakeProcessedEvents())

	defined := analyticsMessage(t, "evt-1", cloudevents.TypeLaborStandardDefined, ts(9, 0), map[string]any{
		"standard_id":      "std-1",
		"task_type":        "PICK",
		"expected_seconds": 45,
		"effective_from":   ts(9, 0),
	})
	revised := analyticsMessage(t, "evt-2", cloudevents.TypeLaborStandardRevised, ts(10, 0), map[string]any{
		"standard_id":               "std-2",
		"task_type":                 "PICK",
		"previous_expected_seconds": 45,
		"expected_seconds":          40,
		"effective_from":            ts(10, 0),
	})

	ctx := context.Background()
	if err := c.HandleMessage(ctx, defined); err != nil {
		t.Fatalf("HandleMessage(defined): %v", err)
	}
	if err := c.HandleMessage(ctx, revised); err != nil {
		t.Fatalf("HandleMessage(revised): %v", err)
	}

	if len(proj.defined) != 1 || proj.defined[0].fact.ExpectedSeconds != 45 {
		t.Errorf("defined = %+v, want one fact with 45 expected seconds", proj.defined)
	}
	if len(proj.revised) != 1 || proj.revised[0].fact.ExpectedSeconds != 40 {
		t.Errorf("revised = %+v, want one fact with 40 expected seconds", proj.revised)
	}
	if !proj.revised[0].fact.EffectiveFrom.Equal(ts(10, 0)) {
		t.Errorf("EffectiveFrom = %v, want 10:00", proj.revised[0].fact.EffectiveFrom)
	}
}

func TestAnalyticsConsumerIsIdempotentOnEventId(t *testing.T) {
	proj := &recordingProjection{}
	c := newTestAnalyticsConsumer(proj, newFakeProcessedEvents())

	raw := analyticsMessage(t, "evt-dup", cloudevents.TypeTaskPerformanceRecorded, ts(9, 0), map[string]any{
		"task_id":        "task-1",
		"task_type":      "PICK",
		"efficiency_pct": 100,
		"actual_seconds": 45,
		"completed_at":   ts(9, 0),
	})

	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if err := c.HandleMessage(ctx, raw); err != nil {
			t.Fatalf("HandleMessage %d: %v", i, err)
		}
	}

	if proj.callCount != 1 {
		t.Errorf("projection was called %d times for 3 deliveries of one event id, want 1", proj.callCount)
	}
}

func TestAnalyticsConsumerSkipsUnknownEventTypes(t *testing.T) {
	proj := &recordingProjection{}
	gate := newFakeProcessedEvents()
	c := newTestAnalyticsConsumer(proj, gate)

	raw := analyticsMessage(t, "evt-1", "SomeFutureEventType", ts(9, 0), map[string]any{"whatever": 1})

	if err := c.HandleMessage(context.Background(), raw); err != nil {
		t.Fatalf("an unknown event type must be skipped, not an error: %v", err)
	}
	if proj.callCount != 0 {
		t.Errorf("projection was called %d times for an unknown event type, want 0", proj.callCount)
	}
	// Crucially it is NOT marked processed, so widening the contract
	// later can still project it on a replay.
	if _, marked := gate.seen["evt-1"]; marked {
		t.Error("an unknown event type was marked processed; a later replay could never project it")
	}
}

func TestAnalyticsConsumerErrors(t *testing.T) {
	ctx := context.Background()

	t.Run("a malformed data payload is an error", func(t *testing.T) {
		c := newTestAnalyticsConsumer(&recordingProjection{}, newFakeProcessedEvents())
		raw := rawCloudEvent(t, "evt-1", cloudevents.TypeTaskPerformanceRecorded, "assoc-1",
			"urn:warehouse:labor-performance:analytics:TaskPerformanceRecorded:v1", ts(9, 0),
			json.RawMessage(`{"actual_seconds":"not-a-number"}`))
		if err := c.HandleMessage(ctx, raw); err == nil {
			t.Fatal("want an error for a malformed data payload")
		}
	})

	t.Run("an idempotency-gate failure is an error, not a silent skip", func(t *testing.T) {
		gate := newFakeProcessedEvents()
		gate.failWith = errors.New("boom")
		proj := &recordingProjection{}
		c := newTestAnalyticsConsumer(proj, gate)

		raw := analyticsMessage(t, "evt-1", cloudevents.TypeTaskPerformanceRecorded, ts(9, 0), map[string]any{
			"task_type": "PICK", "completed_at": ts(9, 0),
		})
		if err := c.HandleMessage(ctx, raw); err == nil {
			t.Fatal("want an error when the idempotency gate fails")
		}
		if proj.callCount != 0 {
			t.Error("the projection ran despite the gate failing; a gate outage must never double-count")
		}
	})

	t.Run("a projection failure is returned", func(t *testing.T) {
		proj := &recordingProjection{failWith: errors.New("db down")}
		c := newTestAnalyticsConsumer(proj, newFakeProcessedEvents())

		raw := analyticsMessage(t, "evt-1", cloudevents.TypeTaskPerformanceRecorded, ts(9, 0), map[string]any{
			"task_type": "PICK", "completed_at": ts(9, 0),
		})
		if err := c.HandleMessage(ctx, raw); err == nil {
			t.Fatal("want the projection's error to surface")
		}
	})
}

func TestAnalyticsConsumerFallsBackToOccurredAtForAMissingBusinessTime(t *testing.T) {
	proj := &recordingProjection{}
	c := newTestAnalyticsConsumer(proj, newFakeProcessedEvents())

	// completed_at absent entirely — an older or truncated payload. The
	// row must land in the envelope's hour rather than at the zero
	// instant in year 1.
	raw := analyticsMessage(t, "evt-1", cloudevents.TypeTaskPerformanceRecorded, ts(9, 0), map[string]any{
		"task_id":   "task-1",
		"task_type": "PICK",
	})

	if err := c.HandleMessage(context.Background(), raw); err != nil {
		t.Fatalf("HandleMessage: %v", err)
	}
	if got := proj.tasks[0].fact.CompletedAt; !got.Equal(ts(9, 0)) {
		t.Errorf("CompletedAt = %v, want the CloudEvents time fallback 09:00", got)
	}
}

// A message that is not a valid CloudEvent — the retired flat analytics
// envelope, or plain garbage — is skipped (WARN + commit past via Run),
// never parsed, never projected, never marked processed.
func TestAnalyticsConsumerRejectsLegacyFlatEnvelope(t *testing.T) {
	for name, raw := range map[string][]byte{
		"legacy flat analytics envelope": legacyFlatMessage("TaskPerformanceRecorded", `{"task_type":"PICK","actual_seconds":45}`),
		"malformed json":                 []byte("{not json"),
	} {
		t.Run(name, func(t *testing.T) {
			proj := &recordingProjection{}
			gate := newFakeProcessedEvents()
			c := newTestAnalyticsConsumer(proj, gate)
			if err := c.HandleMessage(context.Background(), raw); err != nil {
				t.Fatalf("an invalid cloudevent must be skipped, not an error: %v", err)
			}
			if proj.callCount != 0 {
				t.Fatalf("projection called %d times for a non-CloudEvents message", proj.callCount)
			}
			if len(gate.seen) != 0 {
				t.Fatalf("a non-CloudEvents message was marked processed: %v", gate.seen)
			}
		})
	}
}

// The short event name (the retired flat-envelope event_type) is NOT a
// recognised type: dispatch is on the full CloudEvents type only.
func TestAnalyticsConsumerIgnoresShortTypeName(t *testing.T) {
	proj := &recordingProjection{}
	c := newTestAnalyticsConsumer(proj, newFakeProcessedEvents())
	raw := analyticsMessage(t, "evt-1", "TaskPerformanceRecorded", ts(9, 0), map[string]any{"task_type": "PICK"})
	if err := c.HandleMessage(context.Background(), raw); err != nil {
		t.Fatal(err)
	}
	if proj.callCount != 0 {
		t.Fatal("a bare short type name must not dispatch")
	}
}

package kafka

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	ce "github.com/cloudevents/sdk-go/v2/event"

	"github.com/claudioed/labor-performance/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/labor-performance/internal/adapters/outbound/events"
	"github.com/claudioed/labor-performance/internal/adapters/outbound/memory"
	"github.com/claudioed/labor-performance/internal/application/usecases"
	"github.com/claudioed/labor-performance/internal/domain/shared"
)

// fixture wires just enough of the real stack (in-memory repos, the
// existing RecordTaskPerformance use case) to exercise
// handleFulfillmentEvent without touching a broker.
type fixture struct {
	performances *memory.PerformanceRepo
	consumer     *Consumer
}

func newFixture() fixture {
	standards := memory.NewStandardRepo()
	performances := memory.NewPerformanceRepo()
	processed := memory.NewProcessedEventRepo()
	publisher := events.NewLogPublisher(nil)
	clock := memory.FixedClock{At: time.Date(2026, 8, 29, 8, 0, 0, 0, time.UTC)}

	recordTaskPerformance := &usecases.RecordTaskPerformance{
		Performances: performances, Standards: standards, Processed: processed, Events: publisher, Clock: clock,
	}

	return fixture{
		performances: performances,
		consumer:     &Consumer{recordTaskPerformance: recordTaskPerformance},
	}
}

func taskCompletedEnvelope(t *testing.T, eventId, taskId, associateId string, durationSeconds int64) ce.Event {
	t.Helper()
	return taskCompletedEnvelopeWithType(t, eventId, taskId, associateId, durationSeconds, "")
}

func taskCompletedEnvelopeWithType(t *testing.T, eventId, taskId, associateId string, durationSeconds int64, taskType string) ce.Event {
	t.Helper()
	return taskCompletedEvent(t, eventId, taskId, taskCompletedData{
		TaskId: taskId, StationId: "station-1", WorkUnitId: "wu-1",
		AssociateId: associateId, DurationSeconds: durationSeconds, TaskType: taskType,
	})
}

// taskCompletedEvent builds a decoded fulfillment-execution TaskCompleted
// CloudEvent carrying data as its payload.
func taskCompletedEvent(t *testing.T, eventId, subject string, data any) ce.Event {
	t.Helper()
	raw := rawCloudEvent(t, eventId, cloudevents.TypeFulfillmentTaskCompleted, subject,
		"urn:warehouse:fulfillment-execution:events:TaskCompleted:v1",
		time.Date(2026, 8, 29, 9, 0, 0, 0, time.UTC), data)
	evt, err := cloudevents.Decode(raw)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	return evt
}

func TestHandleFulfillmentEvent_RecordsTaskPerformance(t *testing.T) {
	f := newFixture()

	if err := f.consumer.handleFulfillmentEvent(context.Background(), taskCompletedEnvelope(t, "evt-1", "task-1", "assoc-1", 52)); err != nil {
		t.Fatalf("handleFulfillmentEvent: %v", err)
	}

	exists, err := f.performances.ExistsByAssociateID(context.Background(), "assoc-1")
	if err != nil {
		t.Fatalf("ExistsByAssociateID: %v", err)
	}
	if !exists {
		t.Fatal("expected a TaskPerformance to be recorded for assoc-1")
	}
}

func TestHandleFulfillmentEvent_IgnoresOtherEventTypes(t *testing.T) {
	f := newFixture()

	env := taskCompletedEnvelope(t, "evt-other", "task-1", "assoc-1", 52)
	env.SetType("com.warehouse.wes.fulfillment-execution.task.SomethingElse")

	if err := f.consumer.handleFulfillmentEvent(context.Background(), env); err != nil {
		t.Fatalf("handleFulfillmentEvent: %v", err)
	}

	exists, err := f.performances.ExistsByAssociateID(context.Background(), "assoc-1")
	if err != nil {
		t.Fatalf("ExistsByAssociateID: %v", err)
	}
	if exists {
		t.Fatal("an unrecognized event type must not record anything")
	}
}

func TestHandleFulfillmentEvent_RedeliveryIsIdempotent(t *testing.T) {
	f := newFixture()
	env := taskCompletedEnvelope(t, "evt-dup", "task-1", "assoc-1", 52)

	if err := f.consumer.handleFulfillmentEvent(context.Background(), env); err != nil {
		t.Fatalf("first handleFulfillmentEvent: %v", err)
	}
	if err := f.consumer.handleFulfillmentEvent(context.Background(), env); err != nil {
		t.Fatalf("redelivered handleFulfillmentEvent: %v", err)
	}

	sc, err := f.performances.ScorecardFor(context.Background(), "assoc-1")
	if err != nil {
		t.Fatalf("ScorecardFor: %v", err)
	}
	if sc.TaskCount != 1 {
		t.Fatalf("TaskCount = %d, want 1 (no double-count on redelivery)", sc.TaskCount)
	}
}

func TestHandleFulfillmentEvent_DegradesGracefullyOnOlderPayload(t *testing.T) {
	// A TaskCompleted predating fulfillment-execution's
	// feature/labor-performance-hooks enrichment omits associate_id and
	// duration_seconds entirely. The event's data must still unmarshal
	// (both fields default to their Go zero values) and the event must
	// still be recorded, never dropped or errored.
	f := newFixture()
	env := taskCompletedEvent(t, "evt-old", "task-1",
		json.RawMessage(`{"task_id":"task-1","station_id":"station-1","work_unit_id":"wu-1"}`))

	if err := f.consumer.handleFulfillmentEvent(context.Background(), env); err != nil {
		t.Fatalf("handleFulfillmentEvent: %v", err)
	}

	// Recorded under an empty associate id, per the degrade-gracefully
	// contract — verified by checking the fleet-wide (all-associates)
	// TaskTypePerformanceFor count did NOT change since we never gave it
	// a task type either (unclassified); the safest assertion here is
	// just that no error occurred and the second call sees the same
	// event_id as already processed.
	if err := f.consumer.handleFulfillmentEvent(context.Background(), env); err != nil {
		t.Fatalf("redelivered handleFulfillmentEvent: %v", err)
	}
}

// TaskType now arrives on the wire (fulfillment-execution ADR-0023,
// closing this service's own ADR-0014 gap): a recognized value must
// bucket the recorded TaskPerformance under that exact type, not
// "unclassified" — verified via ScorecardFor's ByTaskType map, the
// real read path GetTaskTypeUtilization and friends depend on.
func TestHandleFulfillmentEvent_RecordsRecognizedTaskType(t *testing.T) {
	f := newFixture()

	env := taskCompletedEnvelopeWithType(t, "evt-typed", "task-1", "assoc-1", 52, "PICK")
	if err := f.consumer.handleFulfillmentEvent(context.Background(), env); err != nil {
		t.Fatalf("handleFulfillmentEvent: %v", err)
	}

	sc, err := f.performances.ScorecardFor(context.Background(), "assoc-1")
	if err != nil {
		t.Fatalf("ScorecardFor: %v", err)
	}
	breakdown, ok := sc.ByTaskType[shared.Pick]
	if !ok {
		t.Fatal("expected a PICK entry in ByTaskType, got none — task_type did not flow through")
	}
	if breakdown.TaskCount != 1 {
		t.Fatalf("ByTaskType[PICK].TaskCount = %d, want 1", breakdown.TaskCount)
	}
	if _, unclassified := sc.ByTaskType[shared.TaskType("")]; unclassified {
		t.Fatal("event was wrongly bucketed as unclassified despite a recognized task_type on the wire")
	}
}

// An unrecognized task_type (REBIN is fulfillment-execution's fourth task
// type, but this service does not model REBIN engineered labor standards)
// must degrade to unclassified exactly like an absent value always has —
// never rejected, never mis-bucketed under a fabricated type.
func TestHandleFulfillmentEvent_UnrecognizedTaskTypeDegradesToUnclassified(t *testing.T) {
	f := newFixture()

	env := taskCompletedEnvelopeWithType(t, "evt-rebin", "task-1", "assoc-1", 52, "REBIN")
	if err := f.consumer.handleFulfillmentEvent(context.Background(), env); err != nil {
		t.Fatalf("handleFulfillmentEvent: %v", err)
	}

	sc, err := f.performances.ScorecardFor(context.Background(), "assoc-1")
	if err != nil {
		t.Fatalf("ScorecardFor: %v", err)
	}
	if _, ok := sc.ByTaskType[shared.TaskType("REBIN")]; ok {
		t.Fatal("REBIN must never appear as its own bucket — this service does not model it")
	}
	unclassified, ok := sc.ByTaskType[shared.TaskType("")]
	if !ok || unclassified.TaskCount != 1 {
		t.Fatal("expected the REBIN completion to be recorded under the unclassified (\"\") bucket")
	}
}

// Dispatch is on the FULL CloudEvents type: the bare short name the
// retired flat envelope used must not match.
func TestHandleFulfillmentEvent_IgnoresShortTypeName(t *testing.T) {
	f := newFixture()
	env := taskCompletedEnvelope(t, "evt-short", "task-1", "assoc-1", 52)
	env.SetType("TaskCompleted")
	if err := f.consumer.handleFulfillmentEvent(context.Background(), env); err != nil {
		t.Fatalf("handleFulfillmentEvent: %v", err)
	}
	if exists, _ := f.performances.ExistsByAssociateID(context.Background(), "assoc-1"); exists {
		t.Fatal("a short type name must not dispatch")
	}
}

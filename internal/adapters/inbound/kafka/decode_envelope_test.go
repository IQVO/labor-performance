package kafka

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/claudioed/labor-performance/internal/adapters/kafka/envelope"
)

// realTaskCompletedType is the exact reverse-DNS CloudEvents `type` string
// fulfillment-execution's apis/asyncapi.yaml specifies for TaskCompleted
// (TaskCompletedEvent schema's enum) — read from the spec, not guessed,
// per ADR-0027/0021 Phase 2 Task 2d.
const realTaskCompletedType = "com.warehouse.wes.fulfillment-execution.task.TaskCompleted"

func flatTaskCompletedJSON(t *testing.T, eventID, taskID, associateID string, durationSeconds int64, occurredAt time.Time) []byte {
	t.Helper()
	data, err := json.Marshal(taskCompletedData{
		TaskId: taskID, StationId: "station-1", WorkUnitId: "wu-1",
		AssociateId: associateID, DurationSeconds: durationSeconds, TaskType: "PICK",
	})
	if err != nil {
		t.Fatalf("marshal data: %v", err)
	}
	body, err := json.Marshal(envelope.Envelope{
		EventId: eventID, EventType: envelope.EventTypeTaskCompleted,
		OccurredAt: occurredAt, Source: "fulfillment-execution", Data: data,
	})
	if err != nil {
		t.Fatalf("marshal flat envelope: %v", err)
	}
	return body
}

func cloudEventsTaskCompletedJSON(t *testing.T, id, taskID, associateID string, durationSeconds int64, occurredAt time.Time) []byte {
	t.Helper()
	data, err := json.Marshal(taskCompletedData{
		TaskId: taskID, StationId: "station-1", WorkUnitId: "wu-1",
		AssociateId: associateID, DurationSeconds: durationSeconds, TaskType: "PICK",
	})
	if err != nil {
		t.Fatalf("marshal data: %v", err)
	}
	body, err := json.Marshal(cloudEvent{
		Specversion: "1.0", Id: id, Type: realTaskCompletedType,
		Source: "/warehouse/fulfillment-execution", Subject: taskID,
		Time: occurredAt, Datacontenttype: "application/json", Data: data,
	})
	if err != nil {
		t.Fatalf("marshal cloudevents envelope: %v", err)
	}
	return body
}

// TestDecodeEnvelope_FlatShape is the regression case: a flat-shaped
// TaskCompleted fixture must decode to exactly the same normalized result
// it always has.
func TestDecodeEnvelope_FlatShape(t *testing.T) {
	occurredAt := time.Date(2026, 8, 29, 9, 0, 0, 0, time.UTC)
	raw := flatTaskCompletedJSON(t, "evt-flat-1", "task-1", "assoc-1", 52, occurredAt)

	env, err := decodeEnvelope(raw)
	if err != nil {
		t.Fatalf("decodeEnvelope: %v", err)
	}
	if env.EventId != "evt-flat-1" {
		t.Errorf("EventId = %q, want evt-flat-1", env.EventId)
	}
	if env.EventType != envelope.EventTypeTaskCompleted {
		t.Errorf("EventType = %q, want %q", env.EventType, envelope.EventTypeTaskCompleted)
	}
	if !env.OccurredAt.Equal(occurredAt) {
		t.Errorf("OccurredAt = %v, want %v", env.OccurredAt, occurredAt)
	}
	if env.Source != "fulfillment-execution" {
		t.Errorf("Source = %q, want fulfillment-execution", env.Source)
	}

	var data taskCompletedData
	if err := json.Unmarshal(env.Data, &data); err != nil {
		t.Fatalf("unmarshal data: %v", err)
	}
	if data.TaskId != "task-1" || data.AssociateId != "assoc-1" || data.DurationSeconds != 52 {
		t.Errorf("data = %+v, unexpected", data)
	}
}

// TestDecodeEnvelope_CloudEventsShape decodes a CloudEvents-shaped
// TaskCompleted fixture, using the REAL type string from
// fulfillment-execution's apis/asyncapi.yaml, and asserts it normalizes to
// the same result the equivalent flat fixture produces.
func TestDecodeEnvelope_CloudEventsShape(t *testing.T) {
	occurredAt := time.Date(2026, 8, 29, 9, 0, 0, 0, time.UTC)
	flatRaw := flatTaskCompletedJSON(t, "evt-same-id", "task-1", "assoc-1", 52, occurredAt)
	ceRaw := cloudEventsTaskCompletedJSON(t, "evt-same-id", "task-1", "assoc-1", 52, occurredAt)

	flatEnv, err := decodeEnvelope(flatRaw)
	if err != nil {
		t.Fatalf("decodeEnvelope(flat): %v", err)
	}
	ceEnv, err := decodeEnvelope(ceRaw)
	if err != nil {
		t.Fatalf("decodeEnvelope(cloudevents): %v", err)
	}

	if ceEnv.EventId != flatEnv.EventId {
		t.Errorf("EventId = %q, want %q (matching flat)", ceEnv.EventId, flatEnv.EventId)
	}
	if ceEnv.EventType != flatEnv.EventType {
		t.Errorf("EventType = %q, want %q (matching flat, bare name stripped of reverse-DNS prefix)", ceEnv.EventType, flatEnv.EventType)
	}
	if ceEnv.EventType != envelope.EventTypeTaskCompleted {
		t.Errorf("EventType = %q, want %q", ceEnv.EventType, envelope.EventTypeTaskCompleted)
	}
	if !ceEnv.OccurredAt.Equal(flatEnv.OccurredAt) {
		t.Errorf("OccurredAt = %v, want %v (matching flat)", ceEnv.OccurredAt, flatEnv.OccurredAt)
	}
	if string(ceEnv.Data) != string(flatEnv.Data) {
		t.Errorf("Data = %s, want %s (byte-identical to flat)", ceEnv.Data, flatEnv.Data)
	}

	// End-to-end: handleFulfillmentEvent on the CloudEvents-normalized
	// envelope must produce the identical side effect the flat path does.
	f := newFixture()
	if err := f.consumer.handleFulfillmentEvent(context.Background(), ceEnv); err != nil {
		t.Fatalf("handleFulfillmentEvent(cloudevents-normalized): %v", err)
	}
	exists, err := f.performances.ExistsByAssociateID(context.Background(), "assoc-1")
	if err != nil {
		t.Fatalf("ExistsByAssociateID: %v", err)
	}
	if !exists {
		t.Fatal("expected a TaskPerformance to be recorded from the CloudEvents-shaped message")
	}
}

// TestDecodeEnvelope_MalformedSpecversionFailsSoft mirrors this consumer's
// existing malformed-message posture: decodeEnvelope returns an error
// (never panics, never silently invents a result), which handleMessage's
// caller already treats as skip + log + commit rather than crashing the
// consume loop.
func TestDecodeEnvelope_MalformedSpecversionFailsSoft(t *testing.T) {
	raw := []byte(`{"specversion":"2.0","id":"evt-bad","type":"` + realTaskCompletedType + `","data":{}}`)

	_, err := decodeEnvelope(raw)
	if err == nil {
		t.Fatal("expected an error for an unrecognized specversion, got nil")
	}
}

// TestDecodeEnvelope_UnparseableJSONFailsSoft is the pre-existing
// unparseable-message case, unchanged by dual-read: garbage bytes still
// return an error rather than a zero-value envelope silently accepted.
func TestDecodeEnvelope_UnparseableJSONFailsSoft(t *testing.T) {
	_, err := decodeEnvelope([]byte(`not json`))
	if err == nil {
		t.Fatal("expected an error for unparseable JSON, got nil")
	}
}

// TestBareEventType_StripsReverseDNSPrefix guards the exact stripping
// behavior decodeEnvelope depends on to key back into the existing
// switch/case logic.
func TestBareEventType_StripsReverseDNSPrefix(t *testing.T) {
	got := bareEventType(realTaskCompletedType)
	if got != "TaskCompleted" {
		t.Fatalf("bareEventType(%q) = %q, want TaskCompleted", realTaskCompletedType, got)
	}
}

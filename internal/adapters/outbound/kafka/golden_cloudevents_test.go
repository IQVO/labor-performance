package kafka

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/claudioed/labor-performance/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/labor-performance/internal/domain/shared"
)

// Golden exact-JSON tests (ADR 0021 / fleet STANDARD §6): one per
// published (stream, type), pinning every CloudEvents context attribute,
// the full type string, the byte-identical data payload, and the
// structured-mode content-type header.

func fixedID() string { return "0b6a3c1e-8f5d-4f8e-9c4b-2a7d1e5f6a90" }

func TestGolden_Analytics_LaborStandardDefined(t *testing.T) {
	travel := int64(15)
	p := &AnalyticsPublisher{NewID: fixedID}
	assertGolden(t, p, shared.NewLaborStandardDefined(at(9), "std-1", shared.Pick, 45, &travel, at(8)),
		cloudevents.TopicLaborPerformanceAnalytics, "PICK", `{
		"specversion":"1.0",
		"id":"0b6a3c1e-8f5d-4f8e-9c4b-2a7d1e5f6a90",
		"source":"/warehouse/labor-performance",
		"type":"com.warehouse.wes.labor-performance.standard.LaborStandardDefined",
		"subject":"std-1",
		"time":"2026-09-05T09:00:00Z",
		"datacontenttype":"application/json",
		"dataschema":"urn:warehouse:labor-performance:analytics:LaborStandardDefined:v1",
		"data":{"standard_id":"std-1","task_type":"PICK","expected_seconds":45,"effective_from":"2026-09-05T08:00:00Z","travel_component_seconds":15}
	}`)
}

func TestGolden_Analytics_LaborStandardRevised(t *testing.T) {
	p := &AnalyticsPublisher{NewID: fixedID}
	assertGolden(t, p, shared.NewLaborStandardRevised(at(10), "std-2", shared.Pack, 45, 40, nil, at(10)),
		cloudevents.TopicLaborPerformanceAnalytics, "PACK", `{
		"specversion":"1.0",
		"id":"0b6a3c1e-8f5d-4f8e-9c4b-2a7d1e5f6a90",
		"source":"/warehouse/labor-performance",
		"type":"com.warehouse.wes.labor-performance.standard.LaborStandardRevised",
		"subject":"std-2",
		"time":"2026-09-05T10:00:00Z",
		"datacontenttype":"application/json",
		"dataschema":"urn:warehouse:labor-performance:analytics:LaborStandardRevised:v1",
		"data":{"standard_id":"std-2","task_type":"PACK","previous_expected_seconds":45,"expected_seconds":40,"effective_from":"2026-09-05T10:00:00Z"}
	}`)
}

func TestGolden_Analytics_TaskPerformanceRecorded(t *testing.T) {
	pct := 86.5
	idle := int64(12)
	p := &AnalyticsPublisher{NewID: fixedID}
	assertGolden(t, p, shared.NewTaskPerformanceRecorded(at(11), "task-1", "assoc-1", shared.Pick, 52, &pct, &idle, at(9)),
		cloudevents.TopicLaborPerformanceAnalytics, "PICK", `{
		"specversion":"1.0",
		"id":"0b6a3c1e-8f5d-4f8e-9c4b-2a7d1e5f6a90",
		"source":"/warehouse/labor-performance",
		"type":"com.warehouse.wes.labor-performance.performance.TaskPerformanceRecorded",
		"subject":"assoc-1",
		"time":"2026-09-05T11:00:00Z",
		"datacontenttype":"application/json",
		"dataschema":"urn:warehouse:labor-performance:analytics:TaskPerformanceRecorded:v1",
		"data":{"task_id":"task-1","associate_id":"assoc-1","task_type":"PICK","efficiency_pct":86.5,"actual_seconds":52,"idle_seconds_before":12,"completed_at":"2026-09-05T09:00:00Z"}
	}`)
}

func TestGolden_Events_TaskPerformanceRecorded(t *testing.T) {
	p := &IntegrationPublisher{NewID: fixedID}
	assertGolden(t, p, shared.NewTaskPerformanceRecorded(at(11), "task-1", "assoc-1", shared.Pick, 52, nil, nil, at(9)),
		cloudevents.TopicLaborPerformanceEvents, "assoc-1", `{
		"specversion":"1.0",
		"id":"0b6a3c1e-8f5d-4f8e-9c4b-2a7d1e5f6a90",
		"source":"/warehouse/labor-performance",
		"type":"com.warehouse.wes.labor-performance.performance.TaskPerformanceRecorded",
		"subject":"assoc-1",
		"time":"2026-09-05T11:00:00Z",
		"datacontenttype":"application/json",
		"dataschema":"urn:warehouse:labor-performance:events:TaskPerformanceRecorded:v1",
		"data":{"task_id":"task-1","associate_id":"assoc-1","task_type":"PICK","efficiency_pct":null,"actual_seconds":52,"idle_seconds_before":null,"completed_at":"2026-09-05T09:00:00Z"}
	}`)
}

func assertGolden(t *testing.T, enc Encoder, event shared.DomainEvent, wantTopic, wantKey, wantJSON string) {
	t.Helper()
	msgs, err := enc.Encode(context.Background(), event)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("got %d messages, want 1", len(msgs))
	}
	m := msgs[0]
	if m.Topic != wantTopic {
		t.Errorf("topic = %q, want %q", m.Topic, wantTopic)
	}
	if string(m.Key) != wantKey {
		t.Errorf("key = %q, want %q", m.Key, wantKey)
	}
	if len(m.Headers) == 0 || m.Headers[0].Key != "content-type" ||
		string(m.Headers[0].Value) != "application/cloudevents+json; charset=UTF-8" {
		t.Errorf("headers = %v, want content-type: application/cloudevents+json; charset=UTF-8", m.Headers)
	}
	var got, want map[string]any
	if err := json.Unmarshal(m.Value, &got); err != nil {
		t.Fatalf("value is not JSON: %v", err)
	}
	if err := json.Unmarshal([]byte(wantJSON), &want); err != nil {
		t.Fatalf("golden is not JSON: %v", err)
	}
	gb, _ := json.Marshal(got)
	wb, _ := json.Marshal(want)
	if string(gb) != string(wb) {
		t.Errorf("wire JSON mismatch\n got: %s\nwant: %s", gb, wb)
	}
}

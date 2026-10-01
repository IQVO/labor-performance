package cloudevents

import (
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestTypeConstantsMatchBuilder(t *testing.T) {
	cases := map[string]string{
		TypeLaborStandardDefined:    Type(EntityStandard, EventLaborStandardDefined),
		TypeLaborStandardRevised:    Type(EntityStandard, EventLaborStandardRevised),
		TypeTaskPerformanceRecorded: Type(EntityPerformance, EventTaskPerformanceRecorded),
	}
	for want, got := range cases {
		if got != want {
			t.Errorf("Type() = %q, want %q", got, want)
		}
	}
	// The cross-service contract string workforce-management consumes.
	if TypeTaskPerformanceRecorded != "com.warehouse.wes.labor-performance.performance.TaskPerformanceRecorded" {
		t.Fatalf("TaskPerformanceRecorded type drifted: %s", TypeTaskPerformanceRecorded)
	}
	if TypeFulfillmentTaskCompleted != "com.warehouse.wes.fulfillment-execution.task.TaskCompleted" {
		t.Fatalf("TaskCompleted type drifted: %s", TypeFulfillmentTaskCompleted)
	}
}

func TestDataSchema(t *testing.T) {
	if got, want := DataSchema(StreamAnalytics, EventLaborStandardDefined, 1), "urn:warehouse:labor-performance:analytics:LaborStandardDefined:v1"; got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestNew_GoldenJSON(t *testing.T) {
	at := time.Date(2026, 9, 30, 12, 0, 0, 0, time.FixedZone("-03", -3*3600))
	b, err := New(Spec{
		ID: "11111111-1111-4111-8111-111111111111", Entity: EntityPerformance, EventName: EventTaskPerformanceRecorded,
		Subject: "assoc-1", Time: at, Stream: StreamEvents, Version: 1,
		Data: map[string]any{"task_id": "t-1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := `{"specversion":"1.0","id":"11111111-1111-4111-8111-111111111111","source":"/warehouse/labor-performance","type":"com.warehouse.wes.labor-performance.performance.TaskPerformanceRecorded","subject":"assoc-1","datacontenttype":"application/json","dataschema":"urn:warehouse:labor-performance:events:TaskPerformanceRecorded:v1","time":"2026-09-30T15:00:00Z","data":{"task_id":"t-1"}}`
	assertJSONEqual(t, b, want)
}

func TestNew_RejectsEmptySubjectAndID(t *testing.T) {
	if _, err := New(Spec{ID: "x", Entity: EntityStandard, EventName: EventLaborStandardDefined, Time: time.Now(), Stream: StreamEvents}); err == nil {
		t.Fatal("expected empty subject to be rejected")
	}
	if _, err := New(Spec{Entity: EntityStandard, EventName: EventLaborStandardDefined, Subject: "s", Time: time.Now(), Stream: StreamEvents}); err == nil {
		t.Fatal("expected empty id to be rejected")
	}
}

func TestNew_DefaultsVersionToOne(t *testing.T) {
	b, err := New(Spec{ID: "x", Entity: EntityStandard, EventName: EventLaborStandardDefined, Subject: "s", Time: time.Now(), Stream: StreamAnalytics})
	if err != nil {
		t.Fatal(err)
	}
	e, err := Decode(b)
	if err != nil {
		t.Fatal(err)
	}
	if e.DataSchema() != "urn:warehouse:labor-performance:analytics:LaborStandardDefined:v1" {
		t.Fatalf("dataschema %q", e.DataSchema())
	}
}

func TestDecode_RoundTrip(t *testing.T) {
	b, err := New(Spec{ID: "id-1", Entity: EntityStandard, EventName: EventLaborStandardRevised, Subject: "std-1", Time: time.Unix(0, 0), Stream: StreamAnalytics, Data: map[string]int{"expected_seconds": 30}})
	if err != nil {
		t.Fatal(err)
	}
	e, err := Decode(b)
	if err != nil {
		t.Fatal(err)
	}
	if e.ID() != "id-1" || e.Type() != TypeLaborStandardRevised || e.Subject() != "std-1" || e.Source() != Source {
		t.Fatalf("unexpected attributes: %+v", e.Context)
	}
	var d map[string]int
	if err := e.DataAs(&d); err != nil || d["expected_seconds"] != 30 {
		t.Fatalf("data: %v %v", d, err)
	}
}

func TestDecode_RejectsLegacyAndGarbage(t *testing.T) {
	for name, raw := range map[string]string{
		"legacy flat envelope": `{"event_id":"e1","event_type":"TaskCompleted","occurred_at":"2026-01-01T00:00:00Z","source":"fulfillment-execution","data":{}}`,
		"bad json":             `{not json`,
		"wrong specversion":    `{"specversion":"0.3","id":"1","source":"/x","type":"t"}`,
		"missing id":           `{"specversion":"1.0","source":"/x","type":"t"}`,
	} {
		if _, err := Decode([]byte(raw)); !errors.Is(err, ErrNotCloudEvent) {
			t.Errorf("%s: want ErrNotCloudEvent, got %v", name, err)
		}
	}
}

func TestContentTypeHeader(t *testing.T) {
	h := ContentTypeHeader()
	if h.Key != "content-type" || string(h.Value) != "application/cloudevents+json; charset=UTF-8" {
		t.Fatalf("header %+v", h)
	}
}

func assertJSONEqual(t *testing.T, got []byte, want string) {
	t.Helper()
	var g, w any
	if err := json.Unmarshal(got, &g); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(want), &w); err != nil {
		t.Fatal(err)
	}
	gb, _ := json.Marshal(g)
	wb, _ := json.Marshal(w)
	if string(gb) != string(wb) {
		t.Fatalf("JSON mismatch\n got: %s\nwant: %s", gb, wb)
	}
}

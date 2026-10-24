package kafka

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/claudioed/labor-performance/internal/adapters/outbound/events"
	"github.com/claudioed/labor-performance/internal/adapters/outbound/memory"
	"github.com/claudioed/labor-performance/internal/application/usecases"
	"github.com/claudioed/labor-performance/internal/domain/shared"
)

func TestHandlingUnitFrom(t *testing.T) {
	tests := map[string]shared.HandlingUnit{
		``:         "",
		`null`:     "",
		`"EACH"`:   shared.Each,
		`"CASE"`:   shared.Case,
		`"PALLET"`: shared.Pallet,
		`"INNER"`:  "",
		`"case"`:   "",
		`42`:       "",
		`{"a":1}`:  "",
		`"CASE`:    "",
		`["CASE"]`: "",
		`true`:     "",
		`""`:       "",
	}
	for in, want := range tests {
		if got := handlingUnitFrom(json.RawMessage(in)); got != want {
			t.Fatalf("handlingUnitFrom(%s) = %q, want %q", in, got, want)
		}
	}
}

func TestWeightGFrom(t *testing.T) {
	tests := map[string]*int64{
		``:        nil,
		`null`:    nil,
		`0`:       nil,
		`-5`:      nil,
		`1`:       ptr(1),
		`18500`:   ptr(18500),
		`18500.5`: nil,
		`"18500"`: nil,
		`true`:    nil,
		`[]`:      nil,
	}
	for in, want := range tests {
		got := weightGFrom(json.RawMessage(in))
		if (got == nil) != (want == nil) || (got != nil && *got != *want) {
			t.Fatalf("weightGFrom(%s) = %v, want %v", in, got, want)
		}
	}
}

func ptr(v int64) *int64 { return &v }

type huFixture struct {
	standards    *memory.StandardRepo
	performances *memory.PerformanceRepo
	consumer     *Consumer
	define       *usecases.DefineStandard
}

func newHUFixture(t *testing.T) huFixture {
	t.Helper()
	standards := memory.NewStandardRepo()
	performances := memory.NewPerformanceRepo()
	publisher := events.NewLogPublisher(nil)
	// Standards take effect before the TaskCompleted fixture time (09:00).
	clock := memory.FixedClock{At: time.Date(2026, 8, 29, 8, 0, 0, 0, time.UTC)}
	f := huFixture{
		standards:    standards,
		performances: performances,
		define:       &usecases.DefineStandard{Standards: standards, Events: publisher, Clock: clock},
		consumer: &Consumer{recordTaskPerformance: &usecases.RecordTaskPerformance{
			Performances: performances, Standards: standards, Processed: memory.NewProcessedEventRepo(), Events: publisher, Clock: clock,
		}},
	}
	ctx := context.Background()
	if _, err := f.define.Execute(ctx, shared.Pick, 45, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := f.define.ExecuteSpec(ctx, usecases.DefineStandardSpec{
		TaskType: shared.Pick, HandlingUnit: shared.Case, ExpectedSeconds: 30,
		TeamLift: &usecases.TeamLiftSpec{ThresholdG: 18000, ExpectedSeconds: 75},
	}); err != nil {
		t.Fatal(err)
	}
	return f
}

func (f huFixture) feed(t *testing.T, id, extra string) {
	t.Helper()
	body := `{"task_id":"` + id + `","station_id":"s","work_unit_id":"w","associate_id":"a1","duration_seconds":60,"task_type":"PICK"` + extra + `}`
	if err := f.consumer.handleFulfillmentEvent(context.Background(), taskCompletedEvent(t, id, id, json.RawMessage(body))); err != nil {
		t.Fatalf("handleFulfillmentEvent(%s): %v", id, err)
	}
}

func (f huFixture) latest(t *testing.T) (std int64, unit shared.HandlingUnit, lift bool) {
	t.Helper()
	rows, err := f.performances.RecentByAssociateID(context.Background(), "a1", 1)
	if err != nil || len(rows) != 1 {
		t.Fatalf("RecentByAssociateID: %v %d", err, len(rows))
	}
	return rows[0].StandardSecondsAtCompletion(), rows[0].HandlingUnit(), rows[0].TeamLift()
}

func TestHandleFulfillmentEvent_ScoresAgainstTheHandlingUnitStandard(t *testing.T) {
	tests := []struct {
		name     string
		extra    string
		wantStd  int64
		wantUnit shared.HandlingUnit
		wantLift bool
	}{
		{"no handling fields: today's behaviour", ``, 45, "", false},
		{"CASE", `,"handling_unit":"CASE"`, 30, shared.Case, false},
		{"CASE at the threshold", `,"handling_unit":"CASE","weight_g":18000`, 30, shared.Case, false},
		{"CASE over the threshold", `,"handling_unit":"CASE","weight_g":18001`, 75, shared.Case, true},
		{"EACH falls back to task-type-only", `,"handling_unit":"EACH"`, 45, shared.Each, false},
		{"weight without a unit is ignored", `,"weight_g":99999`, 45, "", false},
		{"unrecognised unit degrades to none", `,"handling_unit":"INNER","weight_g":99999`, 45, "", false},
		{"malformed unit degrades to none", `,"handling_unit":7`, 45, "", false},
		{"malformed weight degrades to unknown", `,"handling_unit":"CASE","weight_g":"heavy"`, 30, shared.Case, false},
		{"non-positive weight degrades to unknown", `,"handling_unit":"CASE","weight_g":-1`, 30, shared.Case, false},
	}
	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newHUFixture(t)
			f.feed(t, "evt-"+string(rune('a'+i)), tc.extra)
			std, unit, lift := f.latest(t)
			if std != tc.wantStd || unit != tc.wantUnit || lift != tc.wantLift {
				t.Fatalf("std=%d unit=%q lift=%v, want %d %q %v", std, unit, lift, tc.wantStd, tc.wantUnit, tc.wantLift)
			}
		})
	}
}

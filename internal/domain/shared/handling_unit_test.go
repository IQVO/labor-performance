package shared_test

import (
	"errors"
	"testing"
	"time"

	"github.com/claudioed/labor-performance/internal/domain/shared"
)

func TestNewHandlingUnit(t *testing.T) {
	tests := []struct {
		in      string
		want    shared.HandlingUnit
		wantErr error
	}{
		{"", "", nil},
		{"EACH", shared.Each, nil},
		{"CASE", shared.Case, nil},
		{"PALLET", shared.Pallet, nil},
		{"INNER", "", shared.ErrUnknownHandlingUnit},
		{"case", "", shared.ErrUnknownHandlingUnit},
		{" CASE", "", shared.ErrUnknownHandlingUnit},
	}
	for _, tc := range tests {
		got, err := shared.NewHandlingUnit(tc.in)
		if !errors.Is(err, tc.wantErr) {
			t.Fatalf("NewHandlingUnit(%q) err = %v, want %v", tc.in, err, tc.wantErr)
		}
		if got != tc.want {
			t.Fatalf("NewHandlingUnit(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestParseHandlingUnitLenient(t *testing.T) {
	tests := map[string]shared.HandlingUnit{
		"EACH":   shared.Each,
		"CASE":   shared.Case,
		"PALLET": shared.Pallet,
		"":       "",
		"INNER":  "",
		"carton": "",
		"case":   "",
	}
	for in, want := range tests {
		if got := shared.ParseHandlingUnitLenient(in); got != want {
			t.Fatalf("ParseHandlingUnitLenient(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestEventsWithHandling(t *testing.T) {
	baseTime := time.Date(2026, 10, 24, 9, 0, 0, 0, time.UTC)
	thr, secs := int64(18000), int64(80)
	d := shared.NewLaborStandardDefined(baseTime, "std-1", shared.Pick, 30, nil, baseTime).WithHandling(shared.Case, &thr, &secs)
	if d.HandlingUnit != shared.Case || *d.TeamLiftThresholdG != 18000 || *d.TeamLiftExpectedSeconds != 80 {
		t.Fatalf("defined = %+v", d)
	}
	plain := shared.NewLaborStandardDefined(baseTime, "std-1", shared.Pick, 30, nil, baseTime)
	if plain.HandlingUnit != "" || plain.TeamLiftThresholdG != nil || plain.TeamLiftExpectedSeconds != nil {
		t.Fatalf("constructor must leave handling fields zero: %+v", plain)
	}
	r := shared.NewLaborStandardRevised(baseTime, "std-2", shared.Pick, 30, 28, nil, baseTime).WithHandling(shared.Case, &thr, &secs)
	if r.HandlingUnit != shared.Case || *r.TeamLiftThresholdG != 18000 || *r.TeamLiftExpectedSeconds != 80 {
		t.Fatalf("revised = %+v", r)
	}
	p := shared.NewTaskPerformanceRecorded(baseTime, "t1", "a1", shared.Pick, 40, nil, nil, baseTime).WithHandling(shared.Case, true)
	if p.HandlingUnit != shared.Case || !p.TeamLift {
		t.Fatalf("recorded = %+v", p)
	}
	q := shared.NewTaskPerformanceRecorded(baseTime, "t1", "a1", shared.Pick, 40, nil, nil, baseTime)
	if q.HandlingUnit != "" || q.TeamLift {
		t.Fatalf("constructor must leave handling fields zero: %+v", q)
	}
}

package performance_test

import (
	"testing"
	"time"

	"github.com/claudioed/labor-performance/internal/domain/performance"
	"github.com/claudioed/labor-performance/internal/domain/shared"
)

func TestNewWithHandling_CarriesUnitAndTeamLift(t *testing.T) {
	at := time.Date(2026, 10, 24, 9, 0, 0, 0, time.UTC)
	p, err := performance.NewWithHandling("e1", "t1", "a1", shared.Pick, shared.Case, true, 80, 80, at)
	if err != nil {
		t.Fatal(err)
	}
	if p.HandlingUnit() != shared.Case || !p.TeamLift() {
		t.Fatalf("handling = %q teamLift = %v", p.HandlingUnit(), p.TeamLift())
	}
	if p.EfficiencyPct() == nil || *p.EfficiencyPct() != 100 {
		t.Fatalf("efficiency = %v, want 100", p.EfficiencyPct())
	}
}

func TestNewWithHandling_KeepsInvariants(t *testing.T) {
	at := time.Now()
	if _, err := performance.NewWithHandling("", "t1", "", shared.Pick, shared.Case, false, 1, 1, at); err != performance.ErrEmptyEventId {
		t.Fatalf("err = %v", err)
	}
	if _, err := performance.NewWithHandling("e", "", "", shared.Pick, shared.Case, false, 1, 1, at); err != performance.ErrEmptyTaskId {
		t.Fatalf("err = %v", err)
	}
	p, _ := performance.NewWithHandling("e", "t", "", shared.Pick, shared.Case, false, 10, 0, at)
	if p.EfficiencyPct() != nil {
		t.Fatalf("no standard must stay unscorable, got %v", *p.EfficiencyPct())
	}
}

func TestNew_HasNoHandlingUnit(t *testing.T) {
	p, _ := performance.New("e", "t", "a", shared.Pick, 10, 10, time.Now())
	if p.HandlingUnit() != "" || p.TeamLift() {
		t.Fatalf("New must carry no handling: %q %v", p.HandlingUnit(), p.TeamLift())
	}
	r := performance.Rehydrate("e", "t", "a", shared.Pick, 10, 10, nil, time.Now())
	if r.HandlingUnit() != "" || r.TeamLift() {
		t.Fatalf("Rehydrate must carry no handling")
	}
}

func TestRehydrateWithHandling(t *testing.T) {
	eff := 50.0
	r := performance.RehydrateWithHandling("e", "t", "a", shared.Pick, shared.Pallet, true, 10, 5, &eff, time.Now())
	if r.HandlingUnit() != shared.Pallet || !r.TeamLift() || *r.EfficiencyPct() != 50 || r.StandardSecondsAtCompletion() != 5 {
		t.Fatalf("RehydrateWithHandling = %+v", r)
	}
}

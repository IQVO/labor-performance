package standard_test

import (
	"errors"
	"testing"
	"time"

	"github.com/claudioed/labor-performance/internal/domain/shared"
	"github.com/claudioed/labor-performance/internal/domain/standard"
)

var from = time.Date(2026, 10, 24, 8, 0, 0, 0, time.UTC)

func mustTeamLift(t *testing.T, thresholdG, seconds int64) *standard.TeamLift {
	t.Helper()
	tl, err := standard.NewTeamLift(thresholdG, seconds)
	if err != nil {
		t.Fatalf("NewTeamLift: %v", err)
	}
	return &tl
}

func TestNewTeamLift_Boundaries(t *testing.T) {
	tests := []struct {
		name      string
		threshold int64
		seconds   int64
		wantErr   error
	}{
		{"smallest valid", 1, 1, nil},
		{"zero threshold", 0, 10, standard.ErrNonPositiveTeamLiftThreshold},
		{"negative threshold", -1, 10, standard.ErrNonPositiveTeamLiftThreshold},
		{"zero seconds", 100, 0, standard.ErrNonPositiveTeamLiftExpectedSeconds},
		{"negative seconds", 100, -1, standard.ErrNonPositiveTeamLiftExpectedSeconds},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tl, err := standard.NewTeamLift(tc.threshold, tc.seconds)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("err = %v, want %v", err, tc.wantErr)
			}
			if err == nil && (tl.ThresholdG() != tc.threshold || tl.ExpectedSeconds() != tc.seconds || tl.Operators() != 2) {
				t.Fatalf("team lift = %+v", tl)
			}
		})
	}
}

func TestTeamLift_AppliesIsStrictlyAboveThreshold(t *testing.T) {
	tl := mustTeamLift(t, 18000, 80)
	for w, want := range map[int64]bool{17999: false, 18000: false, 18001: true, 0: false, 40000: true} {
		if got := tl.Applies(w); got != want {
			t.Fatalf("Applies(%d) = %v, want %v", w, got, want)
		}
	}
}

func TestNewForUnit_Valid(t *testing.T) {
	tl := mustTeamLift(t, 18000, 80)
	s, err := standard.NewForUnit("std-c", shared.Pick, shared.Case, 30, nil, tl, from)
	if err != nil {
		t.Fatalf("NewForUnit: %v", err)
	}
	if s.HandlingUnit() != shared.Case || s.TeamLift() == nil || s.TeamLift().ThresholdG() != 18000 || s.ExpectedSeconds() != 30 || s.Version() != 1 {
		t.Fatalf("standard = %+v", s)
	}
	for _, u := range []shared.HandlingUnit{"", shared.Each, shared.Pallet} {
		s, err := standard.NewForUnit("std", shared.Pack, u, 12, nil, nil, from)
		if err != nil || s.HandlingUnit() != u || s.TeamLift() != nil {
			t.Fatalf("unit %q: s=%+v err=%v", u, s, err)
		}
	}
}

func TestNewForUnit_Invariants(t *testing.T) {
	tl := mustTeamLift(t, 18000, 80)
	travel := int64(31)
	if _, err := standard.NewForUnit("s", shared.Pick, shared.Each, 30, nil, tl, from); !errors.Is(err, standard.ErrTeamLiftRequiresCaseUnit) {
		t.Fatalf("EACH team lift err = %v", err)
	}
	if _, err := standard.NewForUnit("s", shared.Pick, shared.Pallet, 30, nil, tl, from); !errors.Is(err, standard.ErrTeamLiftRequiresCaseUnit) {
		t.Fatalf("PALLET team lift err = %v", err)
	}
	if _, err := standard.NewForUnit("s", shared.Pick, "", 30, nil, tl, from); !errors.Is(err, standard.ErrTeamLiftRequiresCaseUnit) {
		t.Fatalf("unit-less team lift err = %v", err)
	}
	if _, err := standard.NewForUnit("s", shared.Pick, shared.Case, 0, nil, nil, from); !errors.Is(err, standard.ErrNonPositiveExpectedSeconds) {
		t.Fatalf("zero expected err = %v", err)
	}
	if _, err := standard.NewForUnit("s", shared.Pick, shared.Case, 30, &travel, nil, from); !errors.Is(err, standard.ErrTravelComponentExceedsExpectedSeconds) {
		t.Fatalf("travel err = %v", err)
	}
}

func TestNew_IsTheUnitlessStandard(t *testing.T) {
	s, err := standard.New("s", shared.Pick, 45, nil, from)
	if err != nil {
		t.Fatal(err)
	}
	if s.HandlingUnit() != "" || s.TeamLift() != nil {
		t.Fatalf("New must produce a unit-less standard: %+v", s)
	}
	r := standard.Rehydrate("s", shared.Pick, 45, nil, from, nil, 3)
	if r.HandlingUnit() != "" || r.TeamLift() != nil || r.Version() != 3 {
		t.Fatalf("Rehydrate = %+v", r)
	}
}

func TestRehydrateForUnit(t *testing.T) {
	tl := mustTeamLift(t, 18000, 80)
	to := from.Add(time.Hour)
	r := standard.RehydrateForUnit("s", shared.Pick, shared.Case, 30, nil, tl, from, &to, 0)
	if r.HandlingUnit() != shared.Case || r.TeamLift() != tl || r.EffectiveTo() == nil || r.Version() != 1 {
		t.Fatalf("RehydrateForUnit = %+v", r)
	}
}

func TestSecondsFor(t *testing.T) {
	tl := mustTeamLift(t, 18000, 80)
	withLift, _ := standard.NewForUnit("a", shared.Pick, shared.Case, 30, nil, tl, from)
	noLift, _ := standard.NewForUnit("b", shared.Pick, shared.Case, 30, nil, nil, from)
	w := func(g int64) *int64 { return &g }

	tests := []struct {
		name     string
		s        *standard.LaborStandard
		weight   *int64
		wantSecs int64
		wantLift bool
	}{
		{"unknown weight never lifts", withLift, nil, 30, false},
		{"below threshold", withLift, w(17999), 30, false},
		{"exactly threshold", withLift, w(18000), 30, false},
		{"one gram above", withLift, w(18001), 80, true},
		{"no team lift declared, heavy", noLift, w(99999), 30, false},
		{"no team lift declared, no weight", noLift, nil, 30, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			secs, lift := tc.s.SecondsFor(tc.weight)
			if secs != tc.wantSecs || lift != tc.wantLift {
				t.Fatalf("SecondsFor = (%d, %v), want (%d, %v)", secs, lift, tc.wantSecs, tc.wantLift)
			}
		})
	}
}

package standard_test

import (
	"errors"
	"testing"
	"time"

	"github.com/claudioed/labor-performance/internal/domain/shared"
	"github.com/claudioed/labor-performance/internal/domain/standard"
)

const siteA, siteB shared.SiteCode = "SITE-A", "SITE-B"

func scoped(t *testing.T, id shared.StandardId, scope standard.Scope, seconds int64, at time.Time) *standard.LaborStandard {
	t.Helper()
	s, err := standard.NewForScope(id, scope, seconds, nil, nil, at)
	if err != nil {
		t.Fatalf("NewForScope(%+v): %v", scope, err)
	}
	return s
}

func TestNewForScope_CarriesTheSite(t *testing.T) {
	scope := standard.Scope{TaskType: shared.Pick, HandlingUnit: shared.Case, SiteCode: siteA}
	tl := mustTeamLift(t, 18000, 80)
	s, err := standard.NewForScope("std-s", scope, 30, nil, tl, from)
	if err != nil {
		t.Fatalf("NewForScope: %v", err)
	}
	if s.SiteCode() != siteA || s.HandlingUnit() != shared.Case || s.TaskType() != shared.Pick || s.Scope() != scope {
		t.Fatalf("standard = %+v, scope %+v", s, s.Scope())
	}
	if s.TeamLift() != tl || s.ExpectedSeconds() != 30 || s.Version() != 1 || !s.EffectiveFrom().Equal(from) || s.EffectiveTo() != nil {
		t.Fatalf("standard = %+v", s)
	}
}

// TestNewForUnit_IsSiteLess: every pre-ADR-0038 constructor still builds
// the site-less standard, so legacy behaviour is unchanged.
func TestNewForUnit_IsSiteLess(t *testing.T) {
	n, _ := standard.New("a", shared.Pick, 45, nil, from)
	u, _ := standard.NewForUnit("b", shared.Pick, shared.Case, 30, nil, nil, from)
	r := standard.Rehydrate("c", shared.Pick, 45, nil, from, nil, 1)
	ru := standard.RehydrateForUnit("d", shared.Pick, shared.Each, 12, nil, nil, from, nil, 1)
	for _, s := range []*standard.LaborStandard{n, u, r, ru} {
		if s.SiteCode() != "" || s.Scope().SiteCode != "" {
			t.Fatalf("%s must be site-less, got %q", s.ID(), s.SiteCode())
		}
	}
	if u.Scope() != (standard.Scope{TaskType: shared.Pick, HandlingUnit: shared.Case}) {
		t.Fatalf("NewForUnit scope = %+v", u.Scope())
	}
}

// TestNewForScope_KeepsEveryInvariant: a site does not relax any rule of a
// standard (ADR 0035 / ADR 0004 invariants apply unchanged).
func TestNewForScope_KeepsEveryInvariant(t *testing.T) {
	tl := mustTeamLift(t, 18000, 80)
	travel := int64(31)
	site := func(u shared.HandlingUnit) standard.Scope {
		return standard.Scope{TaskType: shared.Pick, HandlingUnit: u, SiteCode: siteA}
	}
	if _, err := standard.NewForScope("s", site(shared.Each), 30, nil, tl, from); !errors.Is(err, standard.ErrTeamLiftRequiresCaseUnit) {
		t.Fatalf("EACH team lift err = %v", err)
	}
	if _, err := standard.NewForScope("s", site(""), 30, nil, tl, from); !errors.Is(err, standard.ErrTeamLiftRequiresCaseUnit) {
		t.Fatalf("unit-less team lift err = %v", err)
	}
	if _, err := standard.NewForScope("s", site(shared.Case), 0, nil, nil, from); !errors.Is(err, standard.ErrNonPositiveExpectedSeconds) {
		t.Fatalf("zero expected err = %v", err)
	}
	if _, err := standard.NewForScope("s", site(shared.Case), 30, &travel, nil, from); !errors.Is(err, standard.ErrTravelComponentExceedsExpectedSeconds) {
		t.Fatalf("travel err = %v", err)
	}
}

func TestRehydrateForScope(t *testing.T) {
	to := from.Add(time.Hour)
	scope := standard.Scope{TaskType: shared.Pack, HandlingUnit: shared.Pallet, SiteCode: siteB}
	r := standard.RehydrateForScope("s", scope, 90, nil, nil, from, &to, 0)
	if r.Scope() != scope || r.SiteCode() != siteB || r.EffectiveTo() == nil || !r.EffectiveTo().Equal(to) || r.Version() != 1 {
		t.Fatalf("RehydrateForScope = %+v", r)
	}
	if v := standard.RehydrateForScope("s", scope, 90, nil, nil, from, nil, 4).Version(); v != 4 {
		t.Fatalf("version = %d, want 4", v)
	}
}

// TestSupersede_OnlyTheSameTriple pins "at most one open standard per
// (task type, handling unit, site); a revision closes only the same
// triple" (ADR 0038 decision 2).
func TestSupersede_OnlyTheSameTriple(t *testing.T) {
	later := from.Add(time.Hour)
	base := standard.Scope{TaskType: shared.Pick, HandlingUnit: shared.Case, SiteCode: siteA}
	tests := []struct {
		name  string
		prior standard.Scope
		ok    bool
	}{
		{"same triple", base, true},
		{"other site", standard.Scope{TaskType: shared.Pick, HandlingUnit: shared.Case, SiteCode: siteB}, false},
		{"site-less of the same pair", standard.Scope{TaskType: shared.Pick, HandlingUnit: shared.Case}, false},
		{"other handling unit, same site", standard.Scope{TaskType: shared.Pick, HandlingUnit: shared.Each, SiteCode: siteA}, false},
		{"task-type-only, same site", standard.Scope{TaskType: shared.Pick, SiteCode: siteA}, false},
		{"other task type, same site and unit", standard.Scope{TaskType: shared.Pack, HandlingUnit: shared.Case, SiteCode: siteA}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			prior := scoped(t, "old", tc.prior, 30, from)
			next := scoped(t, "new", base, 28, later)
			err := next.Supersede(prior)
			if tc.ok {
				if err != nil || prior.EffectiveTo() == nil || !prior.EffectiveTo().Equal(later) {
					t.Fatalf("Supersede: err=%v effectiveTo=%v, want closed at %v", err, prior.EffectiveTo(), later)
				}
				return
			}
			if !errors.Is(err, standard.ErrScopeMismatch) {
				t.Fatalf("err = %v, want ErrScopeMismatch", err)
			}
			if prior.EffectiveTo() != nil {
				t.Fatalf("a rejected supersede must leave prior open, got effectiveTo %v", prior.EffectiveTo())
			}
		})
	}
}

// TestSupersede_SiteLessLegacyPair: the site-less revision path is the
// pre-ADR-0038 one.
func TestSupersede_SiteLessLegacyPair(t *testing.T) {
	prior, _ := standard.New("old", shared.Pick, 45, nil, from)
	next, _ := standard.New("new", shared.Pick, 40, nil, from.Add(time.Minute))
	if err := next.Supersede(prior); err != nil || prior.EffectiveTo() == nil || !prior.EffectiveTo().Equal(from.Add(time.Minute)) {
		t.Fatalf("Supersede: err=%v prior=%+v", err, prior)
	}
	if next.EffectiveTo() != nil {
		t.Fatalf("the superseding standard must stay open")
	}
}

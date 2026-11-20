package usecases_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/labor-performance/internal/application/ports"
	"github.com/claudioed/labor-performance/internal/application/usecases"
	"github.com/claudioed/labor-performance/internal/domain/shared"
	"github.com/claudioed/labor-performance/internal/domain/standard"
)

// ADR 0038 decision 2: an optional site scope. At most one standard is open
// per (task type, handling unit, site) and a revision closes only the same
// triple.

const otherSite shared.SiteCode = "SITE-POA-02"

// scopeFailingRepo fails the exact-scope lookup only.
type scopeFailingRepo struct{ ports.StandardRepo }

func (scopeFailingRepo) FindCurrentlyActiveForScope(context.Context, standard.Scope) (*standard.LaborStandard, error) {
	return nil, errUnmapped
}

func TestDefineStandard_SiteStandardIsIndependentOfTheSiteLessOne(t *testing.T) {
	r := newHURig()
	ctx := context.Background()
	siteLess := r.def(t, usecases.DefineStandardSpec{TaskType: shared.Pick, ExpectedSeconds: 45})
	site := r.def(t, usecases.DefineStandardSpec{TaskType: shared.Pick, SiteCode: scoringSite, ExpectedSeconds: 40})
	if site.SiteCode() != scoringSite || site.HandlingUnit() != "" {
		t.Fatalf("site standard = %+v", site.Scope())
	}
	// Defining a site's standard must NOT close the site-less one...
	if cur, _ := r.standards.FindCurrentlyActive(ctx, shared.Pick); cur == nil || cur.ID() != siteLess.ID() || cur.EffectiveTo() != nil {
		t.Fatalf("site-less standard disturbed: %+v", cur)
	}
	// ...and is a Defined (the site had none), carrying the site.
	ev, ok := r.pub.got[len(r.pub.got)-1].(shared.LaborStandardDefined)
	if !ok || ev.SiteCode != scoringSite || ev.StandardId != site.ID() || ev.ExpectedSeconds != 40 {
		t.Fatalf("last event = %+v", r.pub.got[len(r.pub.got)-1])
	}
	// The site-less Defined carried no site.
	if first := r.pub.got[0].(shared.LaborStandardDefined); first.SiteCode != "" {
		t.Fatalf("site-less event carries a site: %+v", first)
	}
}

func TestDefineStandard_RevisingASiteStandardClosesOnlyThatTriple(t *testing.T) {
	r := newHURig()
	ctx := context.Background()
	siteLess := r.def(t, usecases.DefineStandardSpec{TaskType: shared.Pick, HandlingUnit: shared.Case, ExpectedSeconds: 30})
	first := r.def(t, usecases.DefineStandardSpec{TaskType: shared.Pick, HandlingUnit: shared.Case, SiteCode: scoringSite, ExpectedSeconds: 25})
	other := r.def(t, usecases.DefineStandardSpec{TaskType: shared.Pick, HandlingUnit: shared.Case, SiteCode: otherSite, ExpectedSeconds: 27})
	sameSiteOtherUnit := r.def(t, usecases.DefineStandardSpec{TaskType: shared.Pick, SiteCode: scoringSite, ExpectedSeconds: 41})
	r.clock.at = baseTime.Add(time.Hour)
	second := r.def(t, usecases.DefineStandardSpec{TaskType: shared.Pick, HandlingUnit: shared.Case, SiteCode: scoringSite, ExpectedSeconds: 22})

	rev, ok := r.pub.got[len(r.pub.got)-1].(shared.LaborStandardRevised)
	if !ok || rev.PreviousExpectedSeconds != 25 || rev.NewExpectedSeconds != 22 || rev.SiteCode != scoringSite || rev.HandlingUnit != shared.Case {
		t.Fatalf("last event = %+v, want a site Revised 25 -> 22", r.pub.got[len(r.pub.got)-1])
	}
	cur, _ := r.standards.FindCurrentlyActiveForScope(ctx, second.Scope())
	if cur == nil || cur.ID() != second.ID() {
		t.Fatalf("current site CASE = %+v, want %s", cur, second.ID())
	}
	untouched := map[string]*standard.LaborStandard{
		"site-less CASE":          siteLess,
		"other site's CASE":       other,
		"same site, no unit (41)": sameSiteOtherUnit,
	}
	for name, want := range untouched {
		got, _ := r.standards.FindCurrentlyActiveForScope(ctx, want.Scope())
		if got == nil || got.ID() != want.ID() || got.EffectiveTo() != nil {
			t.Fatalf("%s was disturbed by the site revision: %+v", name, got)
		}
	}
	if first.ID() == second.ID() {
		t.Fatalf("a revision must mint a new standard id")
	}
}

func TestDefineStandard_PropagatesScopeLookupError(t *testing.T) {
	r := newHURig()
	r.define.Standards = scopeFailingRepo{StandardRepo: r.standards}
	_, err := r.define.ExecuteSpec(context.Background(), usecases.DefineStandardSpec{TaskType: shared.Pick, SiteCode: scoringSite, ExpectedSeconds: 30})
	if !errors.Is(err, errUnmapped) {
		t.Fatalf("err = %v", err)
	}
	if len(r.pub.got) != 0 {
		t.Fatalf("a failed lookup must publish nothing")
	}
}

// TestDefineStandard_SiteLessPathNeverUsesTheScopeLookup: with no site the
// pre-ADR-0038 lookups answer, so a broken scope lookup cannot affect a
// site-less definition.
func TestDefineStandard_SiteLessPathNeverUsesTheScopeLookup(t *testing.T) {
	r := newHURig()
	r.define.Standards = scopeFailingRepo{StandardRepo: r.standards}
	for _, unit := range []shared.HandlingUnit{"", shared.Case} {
		if _, err := r.define.ExecuteSpec(context.Background(), usecases.DefineStandardSpec{TaskType: shared.Pick, HandlingUnit: unit, ExpectedSeconds: 30}); err != nil {
			t.Fatalf("unit %q: %v", unit, err)
		}
	}
}

func TestGetStandard_ExecuteForSiteReadsExactlyThatSite(t *testing.T) {
	r := newHURig()
	ctx := context.Background()
	siteLess := r.def(t, usecases.DefineStandardSpec{TaskType: shared.Pick, ExpectedSeconds: 45})
	caseStd := r.def(t, usecases.DefineStandardSpec{TaskType: shared.Pick, HandlingUnit: shared.Case, ExpectedSeconds: 30})

	// A site with no standard of its own: 404, never the site-less fallback.
	if _, err := r.get.ExecuteForSite(ctx, shared.Pick, "", scoringSite); !errors.Is(err, usecases.ErrStandardNotFound) {
		t.Fatalf("site without a standard: err = %v, want ErrStandardNotFound", err)
	}
	site := r.def(t, usecases.DefineStandardSpec{TaskType: shared.Pick, SiteCode: scoringSite, ExpectedSeconds: 40})
	got, err := r.get.ExecuteForSite(ctx, shared.Pick, "", scoringSite)
	if err != nil || got.ID() != site.ID() || got.SiteCode() != scoringSite {
		t.Fatalf("site read: %+v %v", got, err)
	}
	// The site's unit lookup does not fall back to the site's unit-less one.
	if _, err := r.get.ExecuteForSite(ctx, shared.Pick, shared.Case, scoringSite); !errors.Is(err, usecases.ErrStandardNotFound) {
		t.Fatalf("site CASE without a standard: err = %v", err)
	}
	if _, err := r.get.ExecuteForSite(ctx, shared.Pick, "", otherSite); !errors.Is(err, usecases.ErrStandardNotFound) {
		t.Fatalf("another site: err = %v", err)
	}
	// Without a site the answer is the site-less one, exactly as before.
	if got, err := r.get.ExecuteForSite(ctx, shared.Pick, "", ""); err != nil || got.ID() != siteLess.ID() {
		t.Fatalf("no site: %+v %v", got, err)
	}
	if got, err := r.get.ExecuteForSite(ctx, shared.Pick, shared.Case, ""); err != nil || got.ID() != caseStd.ID() {
		t.Fatalf("no site, CASE: %+v %v", got, err)
	}
	if got, err := r.get.Execute(ctx, shared.Pick); err != nil || got.ID() != siteLess.ID() {
		t.Fatalf("Execute must stay site-less: %+v %v", got, err)
	}
}

func TestGetStandard_ExecuteForSitePropagatesError(t *testing.T) {
	r := newHURig()
	uc := &usecases.GetStandard{Standards: scopeFailingRepo{StandardRepo: r.standards}}
	if _, err := uc.ExecuteForSite(context.Background(), shared.Pick, "", scoringSite); !errors.Is(err, errUnmapped) {
		t.Fatalf("err = %v", err)
	}
}

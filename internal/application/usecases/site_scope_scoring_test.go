package usecases_test

import (
	"testing"
	"time"

	"github.com/claudioed/labor-performance/internal/application/usecases"
	"github.com/claudioed/labor-performance/internal/domain/shared"
)

// ADR 0038 decision 3: scoring is unchanged. RecordTaskPerformance resolves
// SITE-LESS standards only, so declaring a site-scoped standard for the same
// task type (and handling unit) never changes a score.

const scoringSite shared.SiteCode = "SITE-GRU-01"

// TestRecord_SiteScopedStandardDoesNotChangeAScore scores the same
// completions on two rigs that differ ONLY by site-scoped standards for the
// same task type and units: every score must be identical, and equal to
// the site-less standard's.
func TestRecord_SiteScopedStandardDoesNotChangeAScore(t *testing.T) {
	plain, withSites := newHURig(), newHURig()
	for _, r := range []*huRig{plain, withSites} {
		r.def(t, usecases.DefineStandardSpec{TaskType: shared.Pick, ExpectedSeconds: 45})
		r.def(t, usecases.DefineStandardSpec{TaskType: shared.Pick, HandlingUnit: shared.Case, ExpectedSeconds: 30})
	}
	// Much faster site standards, for the task-type-only scope and for CASE:
	// if scoring ever resolved them every efficiency below would change.
	withSites.def(t, usecases.DefineStandardSpec{TaskType: shared.Pick, SiteCode: scoringSite, ExpectedSeconds: 10})
	withSites.def(t, usecases.DefineStandardSpec{TaskType: shared.Pick, HandlingUnit: shared.Case, SiteCode: scoringSite, ExpectedSeconds: 5})
	withSites.def(t, usecases.DefineStandardSpec{TaskType: shared.Pick, HandlingUnit: shared.Each, SiteCode: scoringSite, ExpectedSeconds: 3})

	at := baseTime.Add(time.Minute)
	tests := []struct {
		name    string
		unit    shared.HandlingUnit
		wantStd int64
	}{
		{"no unit uses the site-less task-type-only standard", "", 45},
		{"CASE uses the site-less CASE standard", shared.Case, 30},
		{"EACH with only a site EACH standard falls back to the site-less task-type-only one", shared.Each, 45},
	}
	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			id := "evt-site-" + string(rune('a'+i))
			want := plain.rec(t, id, tc.unit, nil, 60, at)
			got := withSites.rec(t, id, tc.unit, nil, 60, at)
			if got.std != tc.wantStd || want.std != tc.wantStd {
				t.Fatalf("StandardSecondsAtCompletion = %d (without site standards %d), want %d", got.std, want.std, tc.wantStd)
			}
			if got.eff == nil || want.eff == nil || *got.eff != *want.eff {
				t.Fatalf("EfficiencyPct = %v, want %v (a site standard changed a score)", got.eff, want.eff)
			}
		})
	}
}

// TestRecord_OnlyASiteScopedStandardIsUnscorable: with no site-less
// standard at all, a site's standard does not score anything — the
// completion stays unscorable rather than borrowing one site's number.
func TestRecord_OnlyASiteScopedStandardIsUnscorable(t *testing.T) {
	r := newHURig()
	r.def(t, usecases.DefineStandardSpec{TaskType: shared.Pick, SiteCode: scoringSite, ExpectedSeconds: 45})
	r.def(t, usecases.DefineStandardSpec{TaskType: shared.Pick, HandlingUnit: shared.Case, SiteCode: scoringSite, ExpectedSeconds: 30})
	for i, unit := range []shared.HandlingUnit{"", shared.Case} {
		got := r.rec(t, "evt-only-site-"+string(rune('a'+i)), unit, nil, 60, baseTime.Add(time.Minute))
		if got.std != 0 || got.eff != nil {
			t.Fatalf("unit %q: std=%d eff=%v, want 0 / nil (scoring never resolves a site standard)", unit, got.std, got.eff)
		}
	}
}

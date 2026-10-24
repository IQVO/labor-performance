package usecases_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/labor-performance/internal/adapters/outbound/memory"
	"github.com/claudioed/labor-performance/internal/application/ports"
	"github.com/claudioed/labor-performance/internal/application/usecases"
	"github.com/claudioed/labor-performance/internal/domain/shared"
	"github.com/claudioed/labor-performance/internal/domain/standard"
)

// huPublisher captures published domain events.
type huPublisher struct{ got []shared.DomainEvent }

func (p *huPublisher) Publish(_ context.Context, evts ...shared.DomainEvent) error {
	p.got = append(p.got, evts...)
	return nil
}

// huUnitFailingRepo fails the handling-unit lookups only.
type huUnitFailingRepo struct {
	ports.StandardRepo
	failAsOfForUnit, failCurrentForUnit bool
}

func (f *huUnitFailingRepo) FindActiveAsOfForUnit(ctx context.Context, tt shared.TaskType, u shared.HandlingUnit, at time.Time) (*standard.LaborStandard, error) {
	if f.failAsOfForUnit {
		return nil, errUnmapped
	}
	return f.StandardRepo.FindActiveAsOfForUnit(ctx, tt, u, at)
}

func (f *huUnitFailingRepo) FindCurrentlyActiveForUnit(ctx context.Context, tt shared.TaskType, u shared.HandlingUnit) (*standard.LaborStandard, error) {
	if f.failCurrentForUnit {
		return nil, errUnmapped
	}
	return f.StandardRepo.FindCurrentlyActiveForUnit(ctx, tt, u)
}

type huRig struct {
	standards *memory.StandardRepo
	pub       *huPublisher
	clock     *huClock
	define    *usecases.DefineStandard
	record    *usecases.RecordTaskPerformance
	get       *usecases.GetStandard
	perfs     *memory.PerformanceRepo
}

type huClock struct{ at time.Time }

func (c *huClock) Now() time.Time { return c.at }

func newHURig() *huRig {
	standards := memory.NewStandardRepo()
	perfs := memory.NewPerformanceRepo()
	pub := &huPublisher{}
	clock := &huClock{at: baseTime}
	return &huRig{
		standards: standards, pub: pub, clock: clock, perfs: perfs,
		define: &usecases.DefineStandard{Standards: standards, Events: pub, Clock: clock},
		get:    &usecases.GetStandard{Standards: standards},
		record: &usecases.RecordTaskPerformance{
			Performances: perfs, Standards: standards, Processed: memory.NewProcessedEventRepo(),
			Events: pub, Clock: clock,
		},
	}
}

func (r *huRig) def(t *testing.T, spec usecases.DefineStandardSpec) *standard.LaborStandard {
	t.Helper()
	s, err := r.define.ExecuteSpec(context.Background(), spec)
	if err != nil {
		t.Fatalf("ExecuteSpec(%+v): %v", spec, err)
	}
	return s
}

func i64(v int64) *int64 { return &v }

func (r *huRig) rec(t *testing.T, id string, unit shared.HandlingUnit, weight *int64, actual int64, at time.Time) float64OrNil {
	t.Helper()
	p, err := r.record.Execute(context.Background(), usecases.RecordTaskPerformanceRequest{
		KafkaEventId: id, TaskId: id, AssociateId: "a1", TaskType: shared.Pick,
		HandlingUnit: unit, WeightG: weight, ActualSeconds: actual, CompletedAt: at,
	})
	if err != nil || p == nil {
		t.Fatalf("record %s: p=%v err=%v", id, p, err)
	}
	return float64OrNil{std: p.StandardSecondsAtCompletion(), eff: p.EfficiencyPct(), unit: p.HandlingUnit(), lift: p.TeamLift()}
}

type float64OrNil struct {
	std  int64
	eff  *float64
	unit shared.HandlingUnit
	lift bool
}

func TestDefineStandard_UnitStandardsAreIndependentOfTheTaskTypeOnlyOne(t *testing.T) {
	r := newHURig()
	each := r.def(t, usecases.DefineStandardSpec{TaskType: shared.Pick, ExpectedSeconds: 45})
	cs := r.def(t, usecases.DefineStandardSpec{TaskType: shared.Pick, HandlingUnit: shared.Case, ExpectedSeconds: 30})
	if cs.HandlingUnit() != shared.Case {
		t.Fatalf("unit = %q", cs.HandlingUnit())
	}
	// Defining the CASE standard must NOT close the task-type-only one.
	still, err := r.standards.FindCurrentlyActive(context.Background(), shared.Pick)
	if err != nil || still == nil || still.ID() != each.ID() || still.EffectiveTo() != nil {
		t.Fatalf("task-type-only standard disturbed: %+v err=%v", still, err)
	}
	// ...and the first CASE standard is a Defined, not a Revised.
	if _, ok := r.pub.got[len(r.pub.got)-1].(shared.LaborStandardDefined); !ok {
		t.Fatalf("last event = %T, want LaborStandardDefined", r.pub.got[len(r.pub.got)-1])
	}
}

func TestDefineStandard_RevisingAUnitStandardClosesOnlyThatPair(t *testing.T) {
	r := newHURig()
	base := r.def(t, usecases.DefineStandardSpec{TaskType: shared.Pick, ExpectedSeconds: 45})
	first := r.def(t, usecases.DefineStandardSpec{TaskType: shared.Pick, HandlingUnit: shared.Case, ExpectedSeconds: 30})
	r.def(t, usecases.DefineStandardSpec{TaskType: shared.Pick, HandlingUnit: shared.Pallet, ExpectedSeconds: 90})
	r.clock.at = baseTime.Add(time.Hour)
	second := r.def(t, usecases.DefineStandardSpec{TaskType: shared.Pick, HandlingUnit: shared.Case, ExpectedSeconds: 28})

	rev, ok := r.pub.got[len(r.pub.got)-1].(shared.LaborStandardRevised)
	if !ok || rev.PreviousExpectedSeconds != 30 || rev.NewExpectedSeconds != 28 || rev.HandlingUnit != shared.Case {
		t.Fatalf("last event = %+v", r.pub.got[len(r.pub.got)-1])
	}
	cur, _ := r.standards.FindCurrentlyActiveForUnit(context.Background(), shared.Pick, shared.Case)
	if cur.ID() != second.ID() {
		t.Fatalf("current CASE = %s, want %s", cur.ID(), second.ID())
	}
	old, _ := r.standards.FindActiveAsOfForUnit(context.Background(), shared.Pick, shared.Case, baseTime)
	if old == nil || old.ID() != first.ID() || old.EffectiveTo() == nil {
		t.Fatalf("closed CASE standard must stay as-of resolvable: %+v", old)
	}
	if p, _ := r.standards.FindCurrentlyActive(context.Background(), shared.Pick); p.ID() != base.ID() || p.EffectiveTo() != nil {
		t.Fatalf("task-type-only standard was disturbed by a CASE revision")
	}
	if pl, _ := r.standards.FindCurrentlyActiveForUnit(context.Background(), shared.Pick, shared.Pallet); pl == nil || pl.EffectiveTo() != nil {
		t.Fatalf("PALLET standard was disturbed by a CASE revision")
	}
}

func TestDefineStandard_TeamLiftIsDeclaredAndPublished(t *testing.T) {
	r := newHURig()
	s := r.def(t, usecases.DefineStandardSpec{
		TaskType: shared.Pick, HandlingUnit: shared.Case, ExpectedSeconds: 30,
		TeamLift: &usecases.TeamLiftSpec{ThresholdG: 18000, ExpectedSeconds: 75},
	})
	if s.TeamLift() == nil || s.TeamLift().ThresholdG() != 18000 || s.TeamLift().ExpectedSeconds() != 75 {
		t.Fatalf("team lift = %+v", s.TeamLift())
	}
	ev := r.pub.got[0].(shared.LaborStandardDefined)
	if ev.HandlingUnit != shared.Case || *ev.TeamLiftThresholdG != 18000 || *ev.TeamLiftExpectedSeconds != 75 {
		t.Fatalf("event = %+v", ev)
	}
	r.clock.at = baseTime.Add(time.Minute)
	r.def(t, usecases.DefineStandardSpec{
		TaskType: shared.Pick, HandlingUnit: shared.Case, ExpectedSeconds: 31,
		TeamLift: &usecases.TeamLiftSpec{ThresholdG: 20000, ExpectedSeconds: 70},
	})
	rev := r.pub.got[len(r.pub.got)-1].(shared.LaborStandardRevised)
	if rev.HandlingUnit != shared.Case || *rev.TeamLiftThresholdG != 20000 || *rev.TeamLiftExpectedSeconds != 70 {
		t.Fatalf("revised event = %+v", rev)
	}
}

func TestDefineStandard_TeamLiftRejections(t *testing.T) {
	tests := []struct {
		name string
		spec usecases.DefineStandardSpec
		want error
	}{
		{"team lift on EACH", usecases.DefineStandardSpec{TaskType: shared.Pick, HandlingUnit: shared.Each, ExpectedSeconds: 10, TeamLift: &usecases.TeamLiftSpec{ThresholdG: 1, ExpectedSeconds: 1}}, standard.ErrTeamLiftRequiresCaseUnit},
		{"team lift without unit", usecases.DefineStandardSpec{TaskType: shared.Pick, ExpectedSeconds: 10, TeamLift: &usecases.TeamLiftSpec{ThresholdG: 1, ExpectedSeconds: 1}}, standard.ErrTeamLiftRequiresCaseUnit},
		{"zero threshold", usecases.DefineStandardSpec{TaskType: shared.Pick, HandlingUnit: shared.Case, ExpectedSeconds: 10, TeamLift: &usecases.TeamLiftSpec{ThresholdG: 0, ExpectedSeconds: 5}}, standard.ErrNonPositiveTeamLiftThreshold},
		{"zero team lift seconds", usecases.DefineStandardSpec{TaskType: shared.Pick, HandlingUnit: shared.Case, ExpectedSeconds: 10, TeamLift: &usecases.TeamLiftSpec{ThresholdG: 5, ExpectedSeconds: 0}}, standard.ErrNonPositiveTeamLiftExpectedSeconds},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := newHURig()
			metrics := &fakeStandardMetrics{}
			r.define.Metrics = metrics
			if _, err := r.define.ExecuteSpec(context.Background(), tc.spec); !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
			if metrics.rejected != 1 || metrics.accepted != 0 {
				t.Fatalf("metrics accepted=%d rejected=%d", metrics.accepted, metrics.rejected)
			}
			if len(r.pub.got) != 0 {
				t.Fatalf("a rejected definition must publish nothing")
			}
		})
	}
}

func TestDefineStandard_ExecuteIsTheTaskTypeOnlyPath(t *testing.T) {
	r := newHURig()
	s, err := r.define.Execute(context.Background(), shared.Pick, 45, nil)
	if err != nil || s.HandlingUnit() != "" || s.TeamLift() != nil {
		t.Fatalf("Execute = %+v, %v", s, err)
	}
	ev := r.pub.got[0].(shared.LaborStandardDefined)
	if ev.HandlingUnit != "" || ev.TeamLiftThresholdG != nil || ev.TeamLiftExpectedSeconds != nil {
		t.Fatalf("unit-less event must carry no handling fields: %+v", ev)
	}
}

func TestDefineStandard_PropagatesUnitLookupError(t *testing.T) {
	r := newHURig()
	r.define.Standards = &huUnitFailingRepo{StandardRepo: r.standards, failCurrentForUnit: true}
	_, err := r.define.ExecuteSpec(context.Background(), usecases.DefineStandardSpec{TaskType: shared.Pick, HandlingUnit: shared.Case, ExpectedSeconds: 30})
	if !errors.Is(err, errUnmapped) {
		t.Fatalf("err = %v", err)
	}
}

func TestRecord_ResolutionMostSpecificWins(t *testing.T) {
	r := newHURig()
	r.def(t, usecases.DefineStandardSpec{TaskType: shared.Pick, ExpectedSeconds: 45})
	r.def(t, usecases.DefineStandardSpec{TaskType: shared.Pick, HandlingUnit: shared.Case, ExpectedSeconds: 30})
	at := baseTime.Add(time.Minute)

	tests := []struct {
		name    string
		unit    shared.HandlingUnit
		wantStd int64
	}{
		{"no unit uses the task-type-only standard", "", 45},
		{"CASE uses the CASE standard", shared.Case, 30},
		{"EACH has no EACH standard and falls back to task-type-only", shared.Each, 45},
		{"PALLET has no PALLET standard and falls back to task-type-only", shared.Pallet, 45},
	}
	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := r.rec(t, "evt-res-"+string(rune('a'+i)), tc.unit, nil, 60, at)
			if got.std != tc.wantStd {
				t.Fatalf("StandardSecondsAtCompletion = %d, want %d", got.std, tc.wantStd)
			}
			wantEff := 100 * float64(tc.wantStd) / 60
			if got.eff == nil || *got.eff != wantEff {
				t.Fatalf("EfficiencyPct = %v, want %v", got.eff, wantEff)
			}
			if got.unit != tc.unit || got.lift {
				t.Fatalf("unit=%q lift=%v", got.unit, got.lift)
			}
		})
	}
}

func TestRecord_UnitWithNoStandardAtAllIsUnscorable(t *testing.T) {
	r := newHURig()
	r.def(t, usecases.DefineStandardSpec{TaskType: shared.Pick, HandlingUnit: shared.Pallet, ExpectedSeconds: 90})
	// A CASE completion must not be scored against the PALLET standard.
	got := r.rec(t, "evt-1", shared.Case, nil, 60, baseTime.Add(time.Minute))
	if got.std != 0 || got.eff != nil {
		t.Fatalf("std=%d eff=%v, want 0 / nil (never a fabricated number)", got.std, got.eff)
	}
	// ...and a completion without a unit must not be scored against it either.
	got = r.rec(t, "evt-2", "", nil, 60, baseTime.Add(time.Minute))
	if got.std != 0 || got.eff != nil {
		t.Fatalf("unit-less std=%d eff=%v, want 0 / nil", got.std, got.eff)
	}
}

func TestRecord_ResolutionIsAsOfCompletion(t *testing.T) {
	r := newHURig()
	r.def(t, usecases.DefineStandardSpec{TaskType: shared.Pick, ExpectedSeconds: 45})
	r.clock.at = baseTime.Add(time.Hour)
	r.def(t, usecases.DefineStandardSpec{TaskType: shared.Pick, HandlingUnit: shared.Case, ExpectedSeconds: 30})

	before := r.rec(t, "evt-before", shared.Case, nil, 60, baseTime.Add(30*time.Minute))
	after := r.rec(t, "evt-after", shared.Case, nil, 60, baseTime.Add(2*time.Hour))
	if before.std != 45 {
		t.Fatalf("a completion before the CASE standard existed must use the task-type-only one, got %d", before.std)
	}
	if after.std != 30 {
		t.Fatalf("a completion after must use the CASE one, got %d", after.std)
	}
}

func TestRecord_TeamLiftThresholdIsStrict(t *testing.T) {
	r := newHURig()
	r.def(t, usecases.DefineStandardSpec{
		TaskType: shared.Pick, HandlingUnit: shared.Case, ExpectedSeconds: 30,
		TeamLift: &usecases.TeamLiftSpec{ThresholdG: 18000, ExpectedSeconds: 75},
	})
	at := baseTime.Add(time.Minute)
	tests := []struct {
		name     string
		unit     shared.HandlingUnit
		weight   *int64
		wantStd  int64
		wantLift bool
	}{
		{"unknown weight is never a team lift", shared.Case, nil, 30, false},
		{"one gram under", shared.Case, i64(17999), 30, false},
		{"exactly at the threshold", shared.Case, i64(18000), 30, false},
		{"one gram over", shared.Case, i64(18001), 75, true},
		{"far over", shared.Case, i64(60000), 75, true},
		{"weight on an unrelated EACH completion is ignored", shared.Each, i64(60000), 0, false},
	}
	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := r.rec(t, "evt-tl-"+string(rune('a'+i)), tc.unit, tc.weight, 60, at)
			if got.std != tc.wantStd || got.lift != tc.wantLift {
				t.Fatalf("std=%d lift=%v, want %d %v", got.std, got.lift, tc.wantStd, tc.wantLift)
			}
		})
	}
}

func TestRecord_PublishesHandlingOnlyWhenTheEventCarriedAUnit(t *testing.T) {
	r := newHURig()
	r.def(t, usecases.DefineStandardSpec{
		TaskType: shared.Pick, HandlingUnit: shared.Case, ExpectedSeconds: 30,
		TeamLift: &usecases.TeamLiftSpec{ThresholdG: 18000, ExpectedSeconds: 75},
	})
	r.pub.got = nil
	r.rec(t, "evt-1", shared.Case, i64(20000), 60, baseTime.Add(time.Minute))
	r.rec(t, "evt-2", "", nil, 60, baseTime.Add(2*time.Minute))
	var recorded []shared.TaskPerformanceRecorded
	for _, e := range r.pub.got {
		if ev, ok := e.(shared.TaskPerformanceRecorded); ok {
			recorded = append(recorded, ev)
		}
	}
	if len(recorded) != 2 {
		t.Fatalf("recorded events = %d", len(recorded))
	}
	if recorded[0].HandlingUnit != shared.Case || !recorded[0].TeamLift {
		t.Fatalf("first = %+v", recorded[0])
	}
	if recorded[1].HandlingUnit != "" || recorded[1].TeamLift {
		t.Fatalf("second must carry no handling: %+v", recorded[1])
	}
}

func TestRecord_UnclassifiedTaskTypeIgnoresUnit(t *testing.T) {
	r := newHURig()
	r.def(t, usecases.DefineStandardSpec{TaskType: shared.Pick, HandlingUnit: shared.Case, ExpectedSeconds: 30})
	p, err := r.record.Execute(context.Background(), usecases.RecordTaskPerformanceRequest{
		KafkaEventId: "e", TaskId: "t", TaskType: "", HandlingUnit: shared.Case, ActualSeconds: 60, CompletedAt: baseTime.Add(time.Minute),
	})
	if err != nil || p.StandardSecondsAtCompletion() != 0 || p.EfficiencyPct() != nil {
		t.Fatalf("p=%+v err=%v", p, err)
	}
}

func TestRecord_PropagatesUnitLookupError(t *testing.T) {
	r := newHURig()
	r.record.Standards = &huUnitFailingRepo{StandardRepo: r.standards, failAsOfForUnit: true}
	_, err := r.record.Execute(context.Background(), usecases.RecordTaskPerformanceRequest{
		KafkaEventId: "e", TaskId: "t", TaskType: shared.Pick, HandlingUnit: shared.Case, ActualSeconds: 60, CompletedAt: baseTime,
	})
	if !errors.Is(err, errUnmapped) {
		t.Fatalf("err = %v", err)
	}
}

func TestRecord_PropagatesFallbackLookupError(t *testing.T) {
	r := newHURig()
	r.record.Standards = &failingStandardRepo{StandardRepo: r.standards, failFindActiveAsOf: true}
	_, err := r.record.Execute(context.Background(), usecases.RecordTaskPerformanceRequest{
		KafkaEventId: "e", TaskId: "t", TaskType: shared.Pick, HandlingUnit: shared.Case, ActualSeconds: 60, CompletedAt: baseTime,
	})
	if !errors.Is(err, errUnmapped) {
		t.Fatalf("err = %v", err)
	}
}

func TestGetStandard_ExecuteResolved(t *testing.T) {
	r := newHURig()
	ctx := context.Background()
	if _, err := r.get.ExecuteResolved(ctx, shared.Pick, shared.Case); !errors.Is(err, usecases.ErrStandardNotFound) {
		t.Fatalf("nothing declared: err = %v", err)
	}
	base := r.def(t, usecases.DefineStandardSpec{TaskType: shared.Pick, ExpectedSeconds: 45})
	got, err := r.get.ExecuteResolved(ctx, shared.Pick, shared.Case)
	if err != nil || got.ID() != base.ID() || got.HandlingUnit() != "" {
		t.Fatalf("fallback: %+v %v", got, err)
	}
	cs := r.def(t, usecases.DefineStandardSpec{TaskType: shared.Pick, HandlingUnit: shared.Case, ExpectedSeconds: 30})
	got, err = r.get.ExecuteResolved(ctx, shared.Pick, shared.Case)
	if err != nil || got.ID() != cs.ID() {
		t.Fatalf("specific: %+v %v", got, err)
	}
	got, err = r.get.ExecuteResolved(ctx, shared.Pick, "")
	if err != nil || got.ID() != base.ID() {
		t.Fatalf("no unit: %+v %v", got, err)
	}
	got, err = r.get.ExecuteResolved(ctx, shared.Pick, shared.Each)
	if err != nil || got.ID() != base.ID() {
		t.Fatalf("EACH falls back: %+v %v", got, err)
	}
}

func TestGetStandard_ExecuteResolvedPropagatesError(t *testing.T) {
	r := newHURig()
	uc := &usecases.GetStandard{Standards: &huUnitFailingRepo{StandardRepo: r.standards, failCurrentForUnit: true}}
	if _, err := uc.ExecuteResolved(context.Background(), shared.Pick, shared.Case); !errors.Is(err, errUnmapped) {
		t.Fatalf("err = %v", err)
	}
}

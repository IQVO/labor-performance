package usecases_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/claudioed/labor-performance/internal/adapters/outbound/events"
	"github.com/claudioed/labor-performance/internal/adapters/outbound/memory"
	"github.com/claudioed/labor-performance/internal/application/ports"
	"github.com/claudioed/labor-performance/internal/application/usecases"
	"github.com/claudioed/labor-performance/internal/domain/idleness"
	"github.com/claudioed/labor-performance/internal/domain/performance"
	"github.com/claudioed/labor-performance/internal/domain/shared"
	"github.com/claudioed/labor-performance/internal/domain/standard"
)

// fixture wires the real in-memory stack for the application-layer tests.
type fixture struct {
	standards    *memory.StandardRepo
	performances *memory.PerformanceRepo
	processed    *memory.ProcessedEventRepo
	idlePeriods  *memory.IdlePeriodRepo
	clock        memory.FixedClock

	defineStandard         *usecases.DefineStandard
	getStandard            *usecases.GetStandard
	recordTaskPerformance  *usecases.RecordTaskPerformance
	getAssociateScorecard  *usecases.GetAssociateScorecard
	getTaskTypePerformance *usecases.GetTaskTypePerformance
	getUtilization         *usecases.GetUtilization
}

func newFixture(now time.Time) *fixture {
	standards := memory.NewStandardRepo()
	performances := memory.NewPerformanceRepo()
	processed := memory.NewProcessedEventRepo()
	idlePeriods := memory.NewIdlePeriodRepo()
	publisher := events.NewLogPublisher(nil)
	clock := memory.FixedClock{At: now}

	return &fixture{
		standards:    standards,
		performances: performances,
		processed:    processed,
		idlePeriods:  idlePeriods,
		clock:        clock,

		defineStandard: &usecases.DefineStandard{Standards: standards, Events: publisher, Clock: clock},
		getStandard:    &usecases.GetStandard{Standards: standards},
		recordTaskPerformance: &usecases.RecordTaskPerformance{
			Performances: performances, Standards: standards, Processed: processed, Events: publisher, Clock: clock,
			IdlePeriods: idlePeriods,
		},
		getAssociateScorecard:  &usecases.GetAssociateScorecard{Performances: performances},
		getTaskTypePerformance: &usecases.GetTaskTypePerformance{Performances: performances},
		getUtilization:         &usecases.GetUtilization{Performances: performances, IdlePeriods: idlePeriods, Clock: clock},
	}
}

var baseTime = time.Date(2026, 8, 29, 8, 0, 0, 0, time.UTC)

func TestDefineStandard_Success_FirstDefinition(t *testing.T) {
	f := newFixture(baseTime)

	s, err := f.defineStandard.Execute(context.Background(), shared.Pick, 45, nil)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if s.TaskType() != shared.Pick || s.ExpectedSeconds() != 45 {
		t.Fatalf("unexpected standard: %+v", s)
	}
	if s.EffectiveTo() != nil {
		t.Fatal("a freshly defined standard must be open-ended")
	}
}

func TestDefineStandard_FailingPath_NonPositiveExpectedSeconds(t *testing.T) {
	f := newFixture(baseTime)
	_, err := f.defineStandard.Execute(context.Background(), shared.Pick, 0, nil)
	if !errors.Is(err, standard.ErrNonPositiveExpectedSeconds) {
		t.Fatalf("error = %v, want ErrNonPositiveExpectedSeconds", err)
	}
}

// TestDefineStandard_RecordsMetrics_OnAcceptedDefinition covers the
// fleet-standard-metrics ADR's Tier-2 business counter: a successful
// first-time definition (and, separately, a successful revision) both
// record labor_performance.standards.defined{outcome=accepted}, and
// neither records a rejection.
func TestDefineStandard_RecordsMetrics_OnAcceptedDefinition(t *testing.T) {
	standards := memory.NewStandardRepo()
	publisher := events.NewLogPublisher(nil)
	clock := memory.FixedClock{At: baseTime}
	metrics := &fakeStandardMetrics{}

	uc := &usecases.DefineStandard{Standards: standards, Events: publisher, Clock: clock, Metrics: metrics}
	if _, err := uc.Execute(context.Background(), shared.Pick, 45, nil); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if metrics.accepted != 1 {
		t.Fatalf("accepted = %d, want 1", metrics.accepted)
	}
	if metrics.rejected != 0 {
		t.Fatalf("rejected = %d, want 0", metrics.rejected)
	}
}

// TestDefineStandard_RecordsMetrics_OnRevision covers the revision branch
// specifically: it must also count as "accepted", not a separate outcome
// — the ADR treats first-definition and revision as the same business
// event (a caller's requested standard took effect).
func TestDefineStandard_RecordsMetrics_OnRevision(t *testing.T) {
	standards := memory.NewStandardRepo()
	publisher := events.NewLogPublisher(nil)
	metrics := &fakeStandardMetrics{}
	ctx := context.Background()

	uc1 := &usecases.DefineStandard{Standards: standards, Events: publisher, Clock: memory.FixedClock{At: baseTime}, Metrics: metrics}
	if _, err := uc1.Execute(ctx, shared.Pick, 45, nil); err != nil {
		t.Fatalf("first Execute: %v", err)
	}

	uc2 := &usecases.DefineStandard{Standards: standards, Events: publisher, Clock: memory.FixedClock{At: baseTime.Add(24 * time.Hour)}, Metrics: metrics}
	if _, err := uc2.Execute(ctx, shared.Pick, 50, nil); err != nil {
		t.Fatalf("second Execute: %v", err)
	}

	if metrics.accepted != 2 {
		t.Fatalf("accepted = %d, want 2 (one per successful Execute call)", metrics.accepted)
	}
	if metrics.rejected != 0 {
		t.Fatalf("rejected = %d, want 0", metrics.rejected)
	}
}

// TestDefineStandard_RecordsMetrics_OnRejectedDefinition covers the
// invariant-violation path: a non-positive ExpectedSeconds must record
// outcome=rejected and must NOT record outcome=accepted.
func TestDefineStandard_RecordsMetrics_OnRejectedDefinition(t *testing.T) {
	standards := memory.NewStandardRepo()
	publisher := events.NewLogPublisher(nil)
	clock := memory.FixedClock{At: baseTime}
	metrics := &fakeStandardMetrics{}

	uc := &usecases.DefineStandard{Standards: standards, Events: publisher, Clock: clock, Metrics: metrics}
	_, err := uc.Execute(context.Background(), shared.Pick, 0, nil)
	if !errors.Is(err, standard.ErrNonPositiveExpectedSeconds) {
		t.Fatalf("error = %v, want ErrNonPositiveExpectedSeconds", err)
	}

	if metrics.rejected != 1 {
		t.Fatalf("rejected = %d, want 1", metrics.rejected)
	}
	if metrics.accepted != 0 {
		t.Fatalf("accepted = %d, want 0", metrics.accepted)
	}
}

// TestDefineStandard_NilMetrics_DoesNotPanic covers the documented
// "nil is a valid not-instrumented value" contract on
// ports.StandardMetrics.
func TestDefineStandard_NilMetrics_DoesNotPanic(t *testing.T) {
	f := newFixture(baseTime)
	if _, err := f.defineStandard.Execute(context.Background(), shared.Pick, 45, nil); err != nil {
		t.Fatalf("Execute: %v", err)
	}
}

// TestDefineStandard_Revision covers the "DefineStandard called twice for
// the same TaskType closes the first" invariant end to end, including the
// "resolve active as of CompletedAt" resolution a subsequent
// RecordTaskPerformance depends on.
func TestDefineStandard_Revision(t *testing.T) {
	f := newFixture(baseTime)
	ctx := context.Background()

	first, err := f.defineStandard.Execute(ctx, shared.Pick, 45, nil)
	if err != nil {
		t.Fatalf("first Execute: %v", err)
	}

	revisedAt := baseTime.Add(24 * time.Hour)
	f2 := newFixtureAt(f, revisedAt)
	second, err := f2.defineStandard.Execute(ctx, shared.Pick, 50, nil)
	if err != nil {
		t.Fatalf("second Execute: %v", err)
	}
	if second.ExpectedSeconds() != 50 {
		t.Fatalf("second.ExpectedSeconds = %d, want 50", second.ExpectedSeconds())
	}

	// The first standard must now be closed at revisedAt, not deleted or
	// overwritten — its ExpectedSeconds must remain 45.
	closedFirst, err := f.standards.FindActiveAsOf(ctx, shared.Pick, baseTime)
	if err != nil {
		t.Fatalf("FindActiveAsOf(baseTime): %v", err)
	}
	if closedFirst == nil || closedFirst.ExpectedSeconds() != 45 {
		t.Fatalf("standard active at baseTime = %+v, want ExpectedSeconds=45", closedFirst)
	}
	if first.ID() != closedFirst.ID() {
		t.Fatalf("expected the same standard id to still resolve for baseTime")
	}

	// A TaskPerformance completed BEFORE the revision must freeze the OLD
	// standard's value even when recorded (replayed) AFTER the revision.
	backfilled, err := f.recordTaskPerformance.Execute(ctx, usecases.RecordTaskPerformanceRequest{
		KafkaEventId: "evt-backfill", TaskId: "task-1", TaskType: shared.Pick,
		ActualSeconds: 50, CompletedAt: baseTime.Add(time.Hour), // before the revision
	})
	if err != nil {
		t.Fatalf("RecordTaskPerformance (backfill): %v", err)
	}
	if backfilled.StandardSecondsAtCompletion() != 45 {
		t.Fatalf("backfilled StandardSecondsAtCompletion = %d, want 45 (the OLD standard)", backfilled.StandardSecondsAtCompletion())
	}

	// A TaskPerformance completed AFTER the revision must freeze the NEW
	// standard's value.
	fresh, err := f.recordTaskPerformance.Execute(ctx, usecases.RecordTaskPerformanceRequest{
		KafkaEventId: "evt-fresh", TaskId: "task-2", TaskType: shared.Pick,
		ActualSeconds: 50, CompletedAt: revisedAt.Add(time.Hour),
	})
	if err != nil {
		t.Fatalf("RecordTaskPerformance (fresh): %v", err)
	}
	if fresh.StandardSecondsAtCompletion() != 50 {
		t.Fatalf("fresh StandardSecondsAtCompletion = %d, want 50 (the NEW standard)", fresh.StandardSecondsAtCompletion())
	}
}

// newFixtureAt reuses f's repos/publisher but with a clock set to at —
// simulating "time has passed" for a use case under test without
// reconstructing the whole in-memory stack.
func newFixtureAt(f *fixture, at time.Time) *fixture {
	clock := memory.FixedClock{At: at}
	return &fixture{
		standards: f.standards, performances: f.performances, processed: f.processed, idlePeriods: f.idlePeriods, clock: clock,
		defineStandard: &usecases.DefineStandard{Standards: f.standards, Events: events.NewLogPublisher(nil), Clock: clock},
		getStandard:    &usecases.GetStandard{Standards: f.standards},
		recordTaskPerformance: &usecases.RecordTaskPerformance{
			Performances: f.performances, Standards: f.standards, Processed: f.processed, Events: events.NewLogPublisher(nil), Clock: clock,
			IdlePeriods: f.idlePeriods,
		},
		getAssociateScorecard:  &usecases.GetAssociateScorecard{Performances: f.performances},
		getTaskTypePerformance: &usecases.GetTaskTypePerformance{Performances: f.performances},
		getUtilization:         &usecases.GetUtilization{Performances: f.performances, IdlePeriods: f.idlePeriods, Clock: clock},
	}
}

func TestGetStandard_Success(t *testing.T) {
	f := newFixture(baseTime)
	if _, err := f.defineStandard.Execute(context.Background(), shared.Pack, 60, nil); err != nil {
		t.Fatalf("DefineStandard: %v", err)
	}

	s, err := f.getStandard.Execute(context.Background(), shared.Pack)
	if err != nil {
		t.Fatalf("GetStandard: %v", err)
	}
	if s.ExpectedSeconds() != 60 {
		t.Fatalf("ExpectedSeconds = %d, want 60", s.ExpectedSeconds())
	}
}

func TestGetStandard_FailingPath_NotFound(t *testing.T) {
	f := newFixture(baseTime)
	_, err := f.getStandard.Execute(context.Background(), shared.Slam)
	if !errors.Is(err, usecases.ErrStandardNotFound) {
		t.Fatalf("error = %v, want ErrStandardNotFound", err)
	}
}

func TestRecordTaskPerformance_Success_WithActiveStandard(t *testing.T) {
	f := newFixture(baseTime)
	ctx := context.Background()
	if _, err := f.defineStandard.Execute(ctx, shared.Pick, 45, nil); err != nil {
		t.Fatalf("DefineStandard: %v", err)
	}

	p, err := f.recordTaskPerformance.Execute(ctx, usecases.RecordTaskPerformanceRequest{
		KafkaEventId: "evt-1", TaskId: "task-1", AssociateId: "assoc-1", TaskType: shared.Pick,
		ActualSeconds: 52, CompletedAt: baseTime.Add(time.Hour),
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if p.StandardSecondsAtCompletion() != 45 {
		t.Fatalf("StandardSecondsAtCompletion = %d, want 45", p.StandardSecondsAtCompletion())
	}
	if p.EfficiencyPct() == nil {
		t.Fatal("EfficiencyPct must be set")
	}
}

// TestRecordTaskPerformance_NoActiveStandard covers the "TaskCompleted
// with no active standard for its TaskType yields EfficiencyPct=nil not
// an error" invariant.
func TestRecordTaskPerformance_NoActiveStandard(t *testing.T) {
	f := newFixture(baseTime)
	p, err := f.recordTaskPerformance.Execute(context.Background(), usecases.RecordTaskPerformanceRequest{
		KafkaEventId: "evt-1", TaskId: "task-1", TaskType: shared.Slam,
		ActualSeconds: 52, CompletedAt: baseTime,
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if p.StandardSecondsAtCompletion() != 0 {
		t.Fatalf("StandardSecondsAtCompletion = %d, want 0", p.StandardSecondsAtCompletion())
	}
	if p.EfficiencyPct() != nil {
		t.Fatalf("EfficiencyPct = %v, want nil", *p.EfficiencyPct())
	}
}

// TestRecordTaskPerformance_ZeroDurationSeconds covers "TaskCompleted with
// duration_seconds=0 yields ActualSeconds=0, EfficiencyPct=nil".
func TestRecordTaskPerformance_ZeroDurationSeconds(t *testing.T) {
	f := newFixture(baseTime)
	ctx := context.Background()
	if _, err := f.defineStandard.Execute(ctx, shared.Pick, 45, nil); err != nil {
		t.Fatalf("DefineStandard: %v", err)
	}

	p, err := f.recordTaskPerformance.Execute(ctx, usecases.RecordTaskPerformanceRequest{
		KafkaEventId: "evt-1", TaskId: "task-1", TaskType: shared.Pick,
		ActualSeconds: 0, CompletedAt: baseTime.Add(time.Hour),
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if p.ActualSeconds() != 0 {
		t.Fatalf("ActualSeconds = %d, want 0", p.ActualSeconds())
	}
	if p.EfficiencyPct() != nil {
		t.Fatalf("EfficiencyPct = %v, want nil", *p.EfficiencyPct())
	}
}

// TestRecordTaskPerformance_Idempotent covers "duplicate event_id
// consumed twice is idempotent (no double-count)".
func TestRecordTaskPerformance_Idempotent(t *testing.T) {
	f := newFixture(baseTime)
	ctx := context.Background()
	req := usecases.RecordTaskPerformanceRequest{
		KafkaEventId: "evt-dup", TaskId: "task-1", AssociateId: "assoc-1", TaskType: shared.Pick,
		ActualSeconds: 52, CompletedAt: baseTime,
	}

	first, err := f.recordTaskPerformance.Execute(ctx, req)
	if err != nil {
		t.Fatalf("first Execute: %v", err)
	}
	if first == nil {
		t.Fatal("first call must return the recorded performance")
	}

	second, err := f.recordTaskPerformance.Execute(ctx, req)
	if err != nil {
		t.Fatalf("second Execute: %v", err)
	}
	if second != nil {
		t.Fatalf("redelivery must be a no-op (nil, nil), got %+v", second)
	}

	sc, err := f.getAssociateScorecard.Execute(ctx, "assoc-1")
	if err != nil {
		t.Fatalf("GetAssociateScorecard: %v", err)
	}
	if sc.TaskCount != 1 {
		t.Fatalf("TaskCount = %d, want 1 (no double-count)", sc.TaskCount)
	}
}

// TestRecordTaskPerformance_EmptyAssociateId covers "TaskCompleted with
// empty associate_id is recorded and counted in GetTaskTypePerformance but
// excluded from any scorecard".
func TestRecordTaskPerformance_EmptyAssociateId(t *testing.T) {
	f := newFixture(baseTime)
	ctx := context.Background()

	if _, err := f.recordTaskPerformance.Execute(ctx, usecases.RecordTaskPerformanceRequest{
		KafkaEventId: "evt-robot", TaskId: "task-1", AssociateId: "", TaskType: shared.Pick,
		ActualSeconds: 40, CompletedAt: baseTime,
	}); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	tp, err := f.getTaskTypePerformance.Execute(ctx, shared.Pick)
	if err != nil {
		t.Fatalf("GetTaskTypePerformance: %v", err)
	}
	if tp.TaskCount != 1 {
		t.Fatalf("TaskCount = %d, want 1 — an empty-associate row must still count fleet-wide", tp.TaskCount)
	}

	if _, err := f.getAssociateScorecard.Execute(ctx, ""); !errors.Is(err, usecases.ErrAssociateNotFound) {
		t.Fatalf("GetAssociateScorecard(\"\") error = %v, want ErrAssociateNotFound", err)
	}
}

func TestGetAssociateScorecard_FailingPath_NeverSeen(t *testing.T) {
	f := newFixture(baseTime)
	_, err := f.getAssociateScorecard.Execute(context.Background(), "assoc-unknown")
	if !errors.Is(err, usecases.ErrAssociateNotFound) {
		t.Fatalf("error = %v, want ErrAssociateNotFound", err)
	}
}

// TestGetAssociateScorecard_KnownButAllNilEfficiency covers "an associate
// with 1+ rows but all-nil EfficiencyPct returns 200 with
// meanEfficiencyPct: null, not 404".
func TestGetAssociateScorecard_KnownButAllNilEfficiency(t *testing.T) {
	f := newFixture(baseTime)
	ctx := context.Background()
	// No standard defined, so EfficiencyPct will be nil for every row.
	if _, err := f.recordTaskPerformance.Execute(ctx, usecases.RecordTaskPerformanceRequest{
		KafkaEventId: "evt-1", TaskId: "task-1", AssociateId: "assoc-1", TaskType: shared.Pick,
		ActualSeconds: 52, CompletedAt: baseTime,
	}); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	sc, err := f.getAssociateScorecard.Execute(ctx, "assoc-1")
	if err != nil {
		t.Fatalf("GetAssociateScorecard must succeed for a known associate: %v", err)
	}
	if sc.TaskCount != 1 {
		t.Fatalf("TaskCount = %d, want 1", sc.TaskCount)
	}
	if sc.MeanEfficiencyPct != nil {
		t.Fatalf("MeanEfficiencyPct = %v, want nil", *sc.MeanEfficiencyPct)
	}
}

func TestGetTaskTypePerformance_NeverSeenReturnsZeroNotError(t *testing.T) {
	f := newFixture(baseTime)
	tp, err := f.getTaskTypePerformance.Execute(context.Background(), shared.Slam)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if tp.TaskCount != 0 || tp.MeanEfficiencyPct != nil {
		t.Fatalf("unexpected non-zero result for never-seen task type: %+v", tp)
	}
	if tp.MeanActualSeconds != nil {
		t.Fatalf("MeanActualSeconds = %v, want nil for never-seen task type", *tp.MeanActualSeconds)
	}
}

// TestGetTaskTypePerformance_MeanActualSeconds_IndependentOfStandard proves
// MeanActualSeconds is populated even when NO LaborStandard was ever
// defined (so MeanEfficiencyPct is nil for every row) -- the real-measured-
// rate field must not require a standard to exist, unlike EfficiencyPct.
func TestGetTaskTypePerformance_MeanActualSeconds_IndependentOfStandard(t *testing.T) {
	f := newFixture(baseTime)
	ctx := context.Background()
	// Deliberately no DefineStandard call.
	for i, secs := range []int64{40, 50, 60} {
		if _, err := f.recordTaskPerformance.Execute(ctx, usecases.RecordTaskPerformanceRequest{
			KafkaEventId: fmt.Sprintf("evt-%d", i), TaskId: fmt.Sprintf("task-%d", i), TaskType: shared.Pick,
			ActualSeconds: secs, CompletedAt: baseTime.Add(time.Duration(i) * time.Hour),
		}); err != nil {
			t.Fatalf("RecordTaskPerformance[%d]: %v", i, err)
		}
	}

	tp, err := f.getTaskTypePerformance.Execute(ctx, shared.Pick)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if tp.MeanEfficiencyPct != nil {
		t.Fatalf("MeanEfficiencyPct = %v, want nil (no standard was ever defined)", *tp.MeanEfficiencyPct)
	}
	if tp.MeanActualSeconds == nil {
		t.Fatal("MeanActualSeconds must be populated even with no standard defined")
	}
	if got, want := *tp.MeanActualSeconds, 50.0; got != want {
		t.Fatalf("MeanActualSeconds = %v, want %v (mean of 40,50,60)", got, want)
	}
}

// TestGetTaskTypePerformance_MeanActualSeconds_ExcludesUnmeasurableRows
// proves a row with ActualSeconds<=0 (unmeasurable) is excluded from the
// MeanActualSeconds average, exactly like it's excluded from
// MeanEfficiencyPct.
func TestGetTaskTypePerformance_MeanActualSeconds_ExcludesUnmeasurableRows(t *testing.T) {
	f := newFixture(baseTime)
	ctx := context.Background()
	if _, err := f.recordTaskPerformance.Execute(ctx, usecases.RecordTaskPerformanceRequest{
		KafkaEventId: "evt-measured", TaskId: "task-measured", TaskType: shared.Pack,
		ActualSeconds: 60, CompletedAt: baseTime,
	}); err != nil {
		t.Fatalf("RecordTaskPerformance(measured): %v", err)
	}
	if _, err := f.recordTaskPerformance.Execute(ctx, usecases.RecordTaskPerformanceRequest{
		KafkaEventId: "evt-unmeasurable", TaskId: "task-unmeasurable", TaskType: shared.Pack,
		ActualSeconds: 0, CompletedAt: baseTime.Add(time.Hour),
	}); err != nil {
		t.Fatalf("RecordTaskPerformance(unmeasurable): %v", err)
	}

	tp, err := f.getTaskTypePerformance.Execute(ctx, shared.Pack)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if tp.TaskCount != 2 {
		t.Fatalf("TaskCount = %d, want 2 (both rows count, even the unmeasurable one)", tp.TaskCount)
	}
	if tp.MeanActualSeconds == nil {
		t.Fatal("MeanActualSeconds must be populated from the one measurable row")
	}
	if got, want := *tp.MeanActualSeconds, 60.0; got != want {
		t.Fatalf("MeanActualSeconds = %v, want %v (the unmeasurable row must be excluded from the average, not counted as 0)", got, want)
	}
}

// TestGetAssociateScorecard_Trend_InsufficientDataBelowThreeScoredTasks
// covers the real end-to-end wiring: fewer than 3 scored tasks yields
// TrendInsufficientData and no coaching flag, through the real memory
// repo, not just the pure domain function in isolation.
func TestGetAssociateScorecard_Trend_InsufficientDataBelowThreeScoredTasks(t *testing.T) {
	f := newFixture(baseTime)
	ctx := context.Background()
	if _, err := f.defineStandard.Execute(ctx, shared.Pick, 45, nil); err != nil {
		t.Fatalf("DefineStandard: %v", err)
	}
	for i := range 2 {
		if _, err := f.recordTaskPerformance.Execute(ctx, usecases.RecordTaskPerformanceRequest{
			KafkaEventId: fmt.Sprintf("evt-%d", i), TaskId: fmt.Sprintf("task-%d", i), AssociateId: "assoc-1", TaskType: shared.Pick,
			ActualSeconds: 45, CompletedAt: baseTime.Add(time.Duration(i) * time.Hour),
		}); err != nil {
			t.Fatalf("RecordTaskPerformance[%d]: %v", i, err)
		}
	}

	sc, err := f.getAssociateScorecard.Execute(ctx, "assoc-1")
	if err != nil {
		t.Fatalf("GetAssociateScorecard: %v", err)
	}
	if sc.Trend != performance.TrendInsufficientData {
		t.Fatalf("Trend = %v, want TrendInsufficientData with only 2 scored tasks", sc.Trend)
	}
	if sc.CoachingFlag {
		t.Fatal("CoachingFlag must be false with only 2 scored tasks (below the 3-task threshold)")
	}
}

// TestGetAssociateScorecard_CoachingFlag_ThreeConsecutiveBelowFloor
// proves the real end-to-end wiring flags an associate whose 3 most
// recent scored tasks are all below the coaching floor, and that the
// window is chronological (RecentByAssociateID + the oldest-first
// reordering in trendAndCoachingFlag), not insertion order.
func TestGetAssociateScorecard_CoachingFlag_ThreeConsecutiveBelowFloor(t *testing.T) {
	f := newFixture(baseTime)
	ctx := context.Background()
	if _, err := f.defineStandard.Execute(ctx, shared.Pick, 100, nil); err != nil {
		t.Fatalf("DefineStandard: %v", err)
	}

	// 100/actualSeconds*100 efficiency: actualSeconds=200 -> 50% (well
	// below the 85% coaching floor), recorded across 3 distinct,
	// strictly increasing CompletedAt timestamps.
	for i := range 3 {
		if _, err := f.recordTaskPerformance.Execute(ctx, usecases.RecordTaskPerformanceRequest{
			KafkaEventId: fmt.Sprintf("evt-%d", i), TaskId: fmt.Sprintf("task-%d", i), AssociateId: "assoc-1", TaskType: shared.Pick,
			ActualSeconds: 200, CompletedAt: baseTime.Add(time.Duration(i) * time.Hour),
		}); err != nil {
			t.Fatalf("RecordTaskPerformance[%d]: %v", i, err)
		}
	}

	sc, err := f.getAssociateScorecard.Execute(ctx, "assoc-1")
	if err != nil {
		t.Fatalf("GetAssociateScorecard: %v", err)
	}
	if !sc.CoachingFlag {
		t.Fatal("CoachingFlag must be true: the last 3 scored tasks were all at 50% efficiency, well below the 85% floor")
	}
}

// TestGetAssociateScorecard_CoachingFlag_RecentGoodTaskClearsFlag proves
// a later on-standard task breaks a below-floor streak — the flag is
// about the CURRENT trailing window, not "ever had 3 bad in a row".
func TestGetAssociateScorecard_CoachingFlag_RecentGoodTaskClearsFlag(t *testing.T) {
	f := newFixture(baseTime)
	ctx := context.Background()
	if _, err := f.defineStandard.Execute(ctx, shared.Pick, 100, nil); err != nil {
		t.Fatalf("DefineStandard: %v", err)
	}

	// 3 bad tasks, then 1 good one (actualSeconds=90 -> ~111% efficiency).
	for i := range 3 {
		if _, err := f.recordTaskPerformance.Execute(ctx, usecases.RecordTaskPerformanceRequest{
			KafkaEventId: fmt.Sprintf("evt-bad-%d", i), TaskId: fmt.Sprintf("task-bad-%d", i), AssociateId: "assoc-1", TaskType: shared.Pick,
			ActualSeconds: 200, CompletedAt: baseTime.Add(time.Duration(i) * time.Hour),
		}); err != nil {
			t.Fatalf("RecordTaskPerformance(bad %d): %v", i, err)
		}
	}
	if _, err := f.recordTaskPerformance.Execute(ctx, usecases.RecordTaskPerformanceRequest{
		KafkaEventId: "evt-good", TaskId: "task-good", AssociateId: "assoc-1", TaskType: shared.Pick,
		ActualSeconds: 90, CompletedAt: baseTime.Add(4 * time.Hour),
	}); err != nil {
		t.Fatalf("RecordTaskPerformance(good): %v", err)
	}

	sc, err := f.getAssociateScorecard.Execute(ctx, "assoc-1")
	if err != nil {
		t.Fatalf("GetAssociateScorecard: %v", err)
	}
	if sc.CoachingFlag {
		t.Fatal("CoachingFlag must be false: the most recent task was on-standard, breaking the below-floor streak")
	}
}

func TestGetAssociateScorecard_PropagatesRecentByAssociateIDError(t *testing.T) {
	f := newFixture(baseTime)
	ctx := context.Background()
	if _, err := f.recordTaskPerformance.Execute(ctx, usecases.RecordTaskPerformanceRequest{
		KafkaEventId: "evt-1", TaskId: "task-1", AssociateId: "assoc-1", TaskType: shared.Pick,
		ActualSeconds: 45, CompletedAt: baseTime,
	}); err != nil {
		t.Fatalf("RecordTaskPerformance: %v", err)
	}

	wrapped := &failingPerformanceRepo{PerformanceRepo: f.performances, failRecent: true}
	uc := &usecases.GetAssociateScorecard{Performances: wrapped}
	if _, err := uc.Execute(ctx, "assoc-1"); !errors.Is(err, errUnmapped) {
		t.Fatalf("error = %v, want errUnmapped", err)
	}
}

var _ ports.Clock = memory.FixedClock{}

// The following tests exercise each use case's infrastructure-error
// propagation paths (repo/publisher failures the happy-path in-memory
// adapters never produce on their own), rounding out branch coverage
// beyond the domain-invariant-focused tests above.

func TestDefineStandard_PropagatesFindCurrentlyActiveError(t *testing.T) {
	f := newFixture(baseTime)
	wrapped := &failingStandardRepo{StandardRepo: f.standards, failFindCurrent: true}
	uc := &usecases.DefineStandard{Standards: wrapped, Events: events.NewLogPublisher(nil), Clock: f.clock}
	if _, err := uc.Execute(context.Background(), shared.Pick, 45, nil); !errors.Is(err, errUnmapped) {
		t.Fatalf("error = %v, want errUnmapped", err)
	}
}

func TestDefineStandard_PropagatesNextIDError(t *testing.T) {
	f := newFixture(baseTime)
	wrapped := &failingStandardRepo{StandardRepo: f.standards, failNextID: true}
	uc := &usecases.DefineStandard{Standards: wrapped, Events: events.NewLogPublisher(nil), Clock: f.clock}
	if _, err := uc.Execute(context.Background(), shared.Pick, 45, nil); !errors.Is(err, errUnmapped) {
		t.Fatalf("error = %v, want errUnmapped", err)
	}
}

func TestDefineStandard_PropagatesSaveError(t *testing.T) {
	f := newFixture(baseTime)
	wrapped := &failingStandardRepo{StandardRepo: f.standards, failSave: true}
	uc := &usecases.DefineStandard{Standards: wrapped, Events: events.NewLogPublisher(nil), Clock: f.clock}
	if _, err := uc.Execute(context.Background(), shared.Pick, 45, nil); !errors.Is(err, errUnmapped) {
		t.Fatalf("error = %v, want errUnmapped", err)
	}
}

func TestDefineStandard_PropagatesPublishError(t *testing.T) {
	f := newFixture(baseTime)
	uc := &usecases.DefineStandard{Standards: f.standards, Events: &failingPublisher{fail: true}, Clock: f.clock}
	if _, err := uc.Execute(context.Background(), shared.Pick, 45, nil); !errors.Is(err, errUnmapped) {
		t.Fatalf("error = %v, want errUnmapped", err)
	}
}

func TestGetStandard_PropagatesRepoError(t *testing.T) {
	f := newFixture(baseTime)
	wrapped := &failingStandardRepo{StandardRepo: f.standards, failFindCurrent: true}
	uc := &usecases.GetStandard{Standards: wrapped}
	if _, err := uc.Execute(context.Background(), shared.Pick); !errors.Is(err, errUnmapped) {
		t.Fatalf("error = %v, want errUnmapped", err)
	}
}

func TestRecordTaskPerformance_PropagatesProcessedEventsError(t *testing.T) {
	f := newFixture(baseTime)
	uc := &usecases.RecordTaskPerformance{
		Performances: f.performances, Standards: f.standards, Processed: &failingProcessedEvents{fail: true}, Events: events.NewLogPublisher(nil), Clock: f.clock,
	}
	_, err := uc.Execute(context.Background(), usecases.RecordTaskPerformanceRequest{KafkaEventId: "evt-1", TaskId: "task-1", TaskType: shared.Pick, CompletedAt: baseTime})
	if !errors.Is(err, errUnmapped) {
		t.Fatalf("error = %v, want errUnmapped", err)
	}
}

func TestRecordTaskPerformance_PropagatesStandardLookupError(t *testing.T) {
	f := newFixture(baseTime)
	wrapped := &failingStandardRepo{StandardRepo: f.standards, failFindActiveAsOf: true}
	uc := &usecases.RecordTaskPerformance{
		Performances: f.performances, Standards: wrapped, Processed: f.processed, Events: events.NewLogPublisher(nil), Clock: f.clock,
	}
	_, err := uc.Execute(context.Background(), usecases.RecordTaskPerformanceRequest{KafkaEventId: "evt-1", TaskId: "task-1", TaskType: shared.Pick, CompletedAt: baseTime})
	if !errors.Is(err, errUnmapped) {
		t.Fatalf("error = %v, want errUnmapped", err)
	}
}

func TestRecordTaskPerformance_PropagatesSaveError(t *testing.T) {
	f := newFixture(baseTime)
	wrapped := &failingPerformanceRepo{PerformanceRepo: f.performances, failSave: true}
	uc := &usecases.RecordTaskPerformance{
		Performances: wrapped, Standards: f.standards, Processed: f.processed, Events: events.NewLogPublisher(nil), Clock: f.clock,
	}
	_, err := uc.Execute(context.Background(), usecases.RecordTaskPerformanceRequest{KafkaEventId: "evt-1", TaskId: "task-1", TaskType: shared.Pick, CompletedAt: baseTime})
	if !errors.Is(err, errUnmapped) {
		t.Fatalf("error = %v, want errUnmapped", err)
	}
}

func TestRecordTaskPerformance_PropagatesPublishError(t *testing.T) {
	f := newFixture(baseTime)
	uc := &usecases.RecordTaskPerformance{
		Performances: f.performances, Standards: f.standards, Processed: f.processed, Events: &failingPublisher{fail: true}, Clock: f.clock,
	}
	_, err := uc.Execute(context.Background(), usecases.RecordTaskPerformanceRequest{KafkaEventId: "evt-1", TaskId: "task-1", TaskType: shared.Pick, CompletedAt: baseTime})
	if !errors.Is(err, errUnmapped) {
		t.Fatalf("error = %v, want errUnmapped", err)
	}
}

func TestRecordTaskPerformance_PropagatesConstructionError(t *testing.T) {
	// An empty TaskId fails performance.New's own validation; the use
	// case must surface that error rather than swallowing it.
	f := newFixture(baseTime)
	_, err := f.recordTaskPerformance.Execute(context.Background(), usecases.RecordTaskPerformanceRequest{
		KafkaEventId: "evt-1", TaskId: "", TaskType: shared.Pick, CompletedAt: baseTime,
	})
	if !errors.Is(err, performance.ErrEmptyTaskId) {
		t.Fatalf("error = %v, want ErrEmptyTaskId", err)
	}
}

// --- Idle-gap derivation (RecordTaskPerformance) ---------------------------

// TestRecordTaskPerformance_IdleGap_FirstObservation_NoGapRecorded covers
// the idleness ADR's "First-observation gap" limitation: an associate's
// first-ever completion has no prior completion to measure a gap from,
// so IdleSecondsBefore must be nil, never a fabricated "infinite" value.
func TestRecordTaskPerformance_IdleGap_FirstObservation_NoGapRecorded(t *testing.T) {
	f := newFixture(baseTime)
	ctx := context.Background()

	p, err := f.recordTaskPerformance.Execute(ctx, usecases.RecordTaskPerformanceRequest{
		KafkaEventId: "evt-1", TaskId: "task-1", AssociateId: "assoc-1", TaskType: shared.Pick,
		ActualSeconds: 50, CompletedAt: baseTime,
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if p == nil {
		t.Fatal("expected a recorded performance")
	}

	idleSeconds, count, err := f.idlePeriods.SumByAssociate(ctx, "assoc-1", baseTime.Add(-time.Hour))
	if err != nil {
		t.Fatalf("SumByAssociate: %v", err)
	}
	if count != 0 || idleSeconds != 0 {
		t.Fatalf("expected no idle period recorded for a first observation, got count=%d idleSeconds=%d", count, idleSeconds)
	}
}

// TestRecordTaskPerformance_IdleGap_RecordedOnSecondCompletion proves the
// happy path end to end: two completions for one associate with a real
// gap between them derive and persist an IdlePeriod, and the published
// event's IdleSecondsBefore matches the recorded gap seconds.
func TestRecordTaskPerformance_IdleGap_RecordedOnSecondCompletion(t *testing.T) {
	f := newFixture(baseTime)
	ctx := context.Background()

	// First task: claimed at baseTime, took 40s, completed at
	// baseTime+40s.
	if _, err := f.recordTaskPerformance.Execute(ctx, usecases.RecordTaskPerformanceRequest{
		KafkaEventId: "evt-1", TaskId: "task-1", AssociateId: "assoc-1", TaskType: shared.Pick,
		ActualSeconds: 40, CompletedAt: baseTime.Add(40 * time.Second),
	}); err != nil {
		t.Fatalf("first Execute: %v", err)
	}

	// Second task: claimed 100s after the first completion (idle gap =
	// 100s), took 30s, completed 130s after the first completion.
	firstCompletedAt := baseTime.Add(40 * time.Second)
	secondClaimedAt := firstCompletedAt.Add(100 * time.Second)
	secondCompletedAt := secondClaimedAt.Add(30 * time.Second)

	p, err := f.recordTaskPerformance.Execute(ctx, usecases.RecordTaskPerformanceRequest{
		KafkaEventId: "evt-2", TaskId: "task-2", AssociateId: "assoc-1", TaskType: shared.Pick,
		ActualSeconds: 30, CompletedAt: secondCompletedAt,
	})
	if err != nil {
		t.Fatalf("second Execute: %v", err)
	}
	if p == nil {
		t.Fatal("expected a recorded performance")
	}

	idleSeconds, count, err := f.idlePeriods.SumByAssociate(ctx, "assoc-1", baseTime.Add(-time.Hour))
	if err != nil {
		t.Fatalf("SumByAssociate: %v", err)
	}
	if count != 1 {
		t.Fatalf("count = %d, want 1", count)
	}
	if idleSeconds != 100 {
		t.Fatalf("idleSeconds = %d, want 100", idleSeconds)
	}
}

// TestRecordTaskPerformance_IdleGap_NegativeGapSkippedNotFailed proves
// the idleness ADR's "Kafka reordering makes negative gaps ROUTINE"
// discipline: a claim instant landing at or before the previous
// completion must skip idle-gap recording and log, WITHOUT failing the
// enclosing performance write.
func TestRecordTaskPerformance_IdleGap_NegativeGapSkippedNotFailed(t *testing.T) {
	f := newFixture(baseTime)
	ctx := context.Background()

	if _, err := f.recordTaskPerformance.Execute(ctx, usecases.RecordTaskPerformanceRequest{
		KafkaEventId: "evt-1", TaskId: "task-1", AssociateId: "assoc-1", TaskType: shared.Pick,
		ActualSeconds: 40, CompletedAt: baseTime.Add(time.Hour),
	}); err != nil {
		t.Fatalf("first Execute: %v", err)
	}

	// A "next" task whose derived claim instant (completedAt -
	// actualSeconds) lands BEFORE the first completion — out-of-order
	// Kafka delivery.
	p, err := f.recordTaskPerformance.Execute(ctx, usecases.RecordTaskPerformanceRequest{
		KafkaEventId: "evt-2", TaskId: "task-2", AssociateId: "assoc-1", TaskType: shared.Pick,
		ActualSeconds: 7200, CompletedAt: baseTime.Add(time.Hour).Add(30 * time.Minute),
	})
	if err != nil {
		t.Fatalf("second Execute must NOT fail on a negative idle gap: %v", err)
	}
	if p == nil {
		t.Fatal("expected the performance write to still succeed")
	}

	_, count, err := f.idlePeriods.SumByAssociate(ctx, "assoc-1", baseTime.Add(-time.Hour))
	if err != nil {
		t.Fatalf("SumByAssociate: %v", err)
	}
	if count != 0 {
		t.Fatalf("count = %d, want 0 (a negative gap must be skipped, not recorded)", count)
	}
}

// TestRecordTaskPerformance_IdleGap_EmptyAssociateSkipped covers the
// idleness ADR's "Robot stations" limitation: an empty AssociateId must
// never record an idle gap.
func TestRecordTaskPerformance_IdleGap_EmptyAssociateSkipped(t *testing.T) {
	f := newFixture(baseTime)
	ctx := context.Background()

	if _, err := f.recordTaskPerformance.Execute(ctx, usecases.RecordTaskPerformanceRequest{
		KafkaEventId: "evt-1", TaskId: "task-1", AssociateId: "", TaskType: shared.Pick,
		ActualSeconds: 40, CompletedAt: baseTime,
	}); err != nil {
		t.Fatalf("first Execute: %v", err)
	}
	if _, err := f.recordTaskPerformance.Execute(ctx, usecases.RecordTaskPerformanceRequest{
		KafkaEventId: "evt-2", TaskId: "task-2", AssociateId: "", TaskType: shared.Pick,
		ActualSeconds: 40, CompletedAt: baseTime.Add(time.Hour),
	}); err != nil {
		t.Fatalf("second Execute: %v", err)
	}

	_, count, err := f.idlePeriods.SumByAssociate(ctx, "", baseTime.Add(-time.Hour))
	if err != nil {
		t.Fatalf("SumByAssociate: %v", err)
	}
	if count != 0 {
		t.Fatalf("count = %d, want 0 (a robot-station task must never record an idle gap)", count)
	}
}

// TestRecordTaskPerformance_IdleGap_CappedAtConfiguredMax covers the
// idleness ADR's "Cross-shift gaps" limitation: a gap spanning a shift
// boundary is capped at IdleGapCapSeconds, not left to poison a running
// mean.
func TestRecordTaskPerformance_IdleGap_CappedAtConfiguredMax(t *testing.T) {
	standards := memory.NewStandardRepo()
	performances := memory.NewPerformanceRepo()
	processed := memory.NewProcessedEventRepo()
	idlePeriods := memory.NewIdlePeriodRepo()
	publisher := events.NewLogPublisher(nil)
	clock := memory.FixedClock{At: baseTime}
	uc := &usecases.RecordTaskPerformance{
		Performances: performances, Standards: standards, Processed: processed, Events: publisher, Clock: clock,
		IdlePeriods: idlePeriods, IdleGapCapSeconds: 60,
	}
	ctx := context.Background()

	if _, err := uc.Execute(ctx, usecases.RecordTaskPerformanceRequest{
		KafkaEventId: "evt-1", TaskId: "task-1", AssociateId: "assoc-1", TaskType: shared.Pick,
		ActualSeconds: 40, CompletedAt: baseTime,
	}); err != nil {
		t.Fatalf("first Execute: %v", err)
	}
	// A gap of 2 hours (7200s), well over the 60s cap.
	if _, err := uc.Execute(ctx, usecases.RecordTaskPerformanceRequest{
		KafkaEventId: "evt-2", TaskId: "task-2", AssociateId: "assoc-1", TaskType: shared.Pick,
		ActualSeconds: 40, CompletedAt: baseTime.Add(2 * time.Hour).Add(40 * time.Second),
	}); err != nil {
		t.Fatalf("second Execute: %v", err)
	}

	idleSeconds, count, err := idlePeriods.SumByAssociate(ctx, "assoc-1", baseTime.Add(-time.Hour))
	if err != nil {
		t.Fatalf("SumByAssociate: %v", err)
	}
	if count != 1 || idleSeconds != 60 {
		t.Fatalf("count=%d idleSeconds=%d, want count=1 idleSeconds=60 (capped)", count, idleSeconds)
	}
}

// TestRecordTaskPerformance_IdleGap_DefaultCapAppliedWhenUnset proves the
// documented default (3600s) applies when IdleGapCapSeconds is left at
// its zero value — the common case for every caller/test that predates
// idleness.
func TestRecordTaskPerformance_IdleGap_DefaultCapAppliedWhenUnset(t *testing.T) {
	f := newFixture(baseTime)
	ctx := context.Background()

	if _, err := f.recordTaskPerformance.Execute(ctx, usecases.RecordTaskPerformanceRequest{
		KafkaEventId: "evt-1", TaskId: "task-1", AssociateId: "assoc-1", TaskType: shared.Pick,
		ActualSeconds: 40, CompletedAt: baseTime,
	}); err != nil {
		t.Fatalf("first Execute: %v", err)
	}
	// 3 hours (10800s), well over the 3600s default.
	if _, err := f.recordTaskPerformance.Execute(ctx, usecases.RecordTaskPerformanceRequest{
		KafkaEventId: "evt-2", TaskId: "task-2", AssociateId: "assoc-1", TaskType: shared.Pick,
		ActualSeconds: 40, CompletedAt: baseTime.Add(3 * time.Hour).Add(40 * time.Second),
	}); err != nil {
		t.Fatalf("second Execute: %v", err)
	}

	idleSeconds, count, err := f.idlePeriods.SumByAssociate(ctx, "assoc-1", baseTime.Add(-time.Hour))
	if err != nil {
		t.Fatalf("SumByAssociate: %v", err)
	}
	if count != 1 || idleSeconds != 3600 {
		t.Fatalf("count=%d idleSeconds=%d, want count=1 idleSeconds=3600 (default cap)", count, idleSeconds)
	}
}

// TestRecordTaskPerformance_IdleGap_NilIdlePeriodsSkipsEntirely proves
// IdlePeriods is a genuinely optional dependency: a use case constructed
// without it must still succeed, recording no idle gap at all.
func TestRecordTaskPerformance_IdleGap_NilIdlePeriodsSkipsEntirely(t *testing.T) {
	standards := memory.NewStandardRepo()
	performances := memory.NewPerformanceRepo()
	processed := memory.NewProcessedEventRepo()
	publisher := events.NewLogPublisher(nil)
	clock := memory.FixedClock{At: baseTime}
	uc := &usecases.RecordTaskPerformance{
		Performances: performances, Standards: standards, Processed: processed, Events: publisher, Clock: clock,
	}
	ctx := context.Background()

	if _, err := uc.Execute(ctx, usecases.RecordTaskPerformanceRequest{
		KafkaEventId: "evt-1", TaskId: "task-1", AssociateId: "assoc-1", TaskType: shared.Pick,
		ActualSeconds: 40, CompletedAt: baseTime,
	}); err != nil {
		t.Fatalf("first Execute: %v", err)
	}
	p, err := uc.Execute(ctx, usecases.RecordTaskPerformanceRequest{
		KafkaEventId: "evt-2", TaskId: "task-2", AssociateId: "assoc-1", TaskType: shared.Pick,
		ActualSeconds: 40, CompletedAt: baseTime.Add(time.Hour),
	})
	if err != nil || p == nil {
		t.Fatalf("second Execute with nil IdlePeriods must still succeed: p=%v err=%v", p, err)
	}
}

func TestRecordTaskPerformance_PropagatesIdlePeriodSaveError(t *testing.T) {
	f := newFixture(baseTime)
	ctx := context.Background()
	if _, err := f.recordTaskPerformance.Execute(ctx, usecases.RecordTaskPerformanceRequest{
		KafkaEventId: "evt-1", TaskId: "task-1", AssociateId: "assoc-1", TaskType: shared.Pick,
		ActualSeconds: 40, CompletedAt: baseTime,
	}); err != nil {
		t.Fatalf("first Execute: %v", err)
	}

	wrapped := &failingIdlePeriodRepo{IdlePeriodRepo: f.idlePeriods, failSave: true}
	uc := &usecases.RecordTaskPerformance{
		Performances: f.performances, Standards: f.standards, Processed: f.processed, Events: events.NewLogPublisher(nil), Clock: f.clock,
		IdlePeriods: wrapped,
	}
	_, err := uc.Execute(ctx, usecases.RecordTaskPerformanceRequest{
		KafkaEventId: "evt-2", TaskId: "task-2", AssociateId: "assoc-1", TaskType: shared.Pick,
		ActualSeconds: 40, CompletedAt: baseTime.Add(time.Hour),
	})
	if !errors.Is(err, errUnmapped) {
		t.Fatalf("error = %v, want errUnmapped", err)
	}
}

// --- GetUtilization ----------------------------------------------------

func TestGetUtilization_ForTaskType_NeverObserved_ReturnsZeroNotError(t *testing.T) {
	f := newFixture(baseTime)
	result, err := f.getUtilization.ForTaskType(context.Background(), shared.Slam, time.Hour)
	if err != nil {
		t.Fatalf("ForTaskType: %v", err)
	}
	if result.TaskSeconds != 0 || result.IdleSeconds != 0 || result.Associates != 0 {
		t.Fatalf("unexpected non-zero result for never-observed task type: %+v", result)
	}
	if result.UtilizationPct != nil {
		t.Fatalf("UtilizationPct = %v, want nil for a never-observed task type", *result.UtilizationPct)
	}
}

// TestGetUtilization_ForTaskType_ComputesShareFromRealRecordedRows proves
// the real end-to-end wiring: two completions with a gap between them
// produce a task-type utilization result whose task/idle seconds and
// percentage match what was actually recorded, through the real
// RecordTaskPerformance write path, not hand-constructed fixtures.
func TestGetUtilization_ForTaskType_ComputesShareFromRealRecordedRows(t *testing.T) {
	f := newFixture(baseTime)
	ctx := context.Background()

	if _, err := f.recordTaskPerformance.Execute(ctx, usecases.RecordTaskPerformanceRequest{
		KafkaEventId: "evt-1", TaskId: "task-1", AssociateId: "assoc-1", TaskType: shared.Pick,
		ActualSeconds: 30, CompletedAt: baseTime.Add(30 * time.Second),
	}); err != nil {
		t.Fatalf("first Execute: %v", err)
	}
	// 70s idle gap, then a 30s task.
	if _, err := f.recordTaskPerformance.Execute(ctx, usecases.RecordTaskPerformanceRequest{
		KafkaEventId: "evt-2", TaskId: "task-2", AssociateId: "assoc-1", TaskType: shared.Pick,
		ActualSeconds: 30, CompletedAt: baseTime.Add(30 * time.Second).Add(70 * time.Second).Add(30 * time.Second),
	}); err != nil {
		t.Fatalf("second Execute: %v", err)
	}

	f2 := newFixtureAt(f, baseTime.Add(time.Hour))
	result, err := f2.getUtilization.ForTaskType(ctx, shared.Pick, 2*time.Hour)
	if err != nil {
		t.Fatalf("ForTaskType: %v", err)
	}
	if result.TaskSeconds != 60 {
		t.Fatalf("TaskSeconds = %d, want 60", result.TaskSeconds)
	}
	if result.IdleSeconds != 70 {
		t.Fatalf("IdleSeconds = %d, want 70", result.IdleSeconds)
	}
	if result.Associates != 1 {
		t.Fatalf("Associates = %d, want 1", result.Associates)
	}
	if result.UtilizationPct == nil {
		t.Fatal("UtilizationPct must be non-nil when both task and idle time were observed")
	}
	want := 100 * 60.0 / 130.0
	if *result.UtilizationPct != want {
		t.Fatalf("UtilizationPct = %v, want %v", *result.UtilizationPct, want)
	}
}

// TestGetUtilization_ForAssociate_OpenGap_IncludesStillRunningIdleTime
// covers the idleness ADR's "Trailing idleness" limitation: an associate
// idle RIGHT NOW (their last completion has no next TaskClaimed to close
// the gap) contributes a read-time OpenGapSeconds, computed but never
// persisted.
func TestGetUtilization_ForAssociate_OpenGap_IncludesStillRunningIdleTime(t *testing.T) {
	f := newFixture(baseTime)
	ctx := context.Background()

	if _, err := f.recordTaskPerformance.Execute(ctx, usecases.RecordTaskPerformanceRequest{
		KafkaEventId: "evt-1", TaskId: "task-1", AssociateId: "assoc-1", TaskType: shared.Pick,
		ActualSeconds: 40, CompletedAt: baseTime,
	}); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	// Read the utilization 90s later; the associate has no next
	// completion, so the gap since their last completion is "open".
	f2 := newFixtureAt(f, baseTime.Add(90*time.Second))
	result, err := f2.getUtilization.ForAssociate(ctx, "assoc-1", time.Hour)
	if err != nil {
		t.Fatalf("ForAssociate: %v", err)
	}
	if result.OpenGapSeconds != 90 {
		t.Fatalf("OpenGapSeconds = %d, want 90", result.OpenGapSeconds)
	}
	if result.IdleSeconds != 0 {
		t.Fatalf("IdleSeconds = %d, want 0 (no CLOSED gap recorded yet)", result.IdleSeconds)
	}
}

// TestGetUtilization_ForAssociate_NoOpenGapWhenNotCurrentlyIdle proves an
// associate with a closed (already-completed-and-followed) gap
// contributes zero OpenGapSeconds — the open-gap computation must not
// double-count time already captured by a recorded IdlePeriod.
func TestGetUtilization_ForAssociate_NoOpenGapWhenNotCurrentlyIdle(t *testing.T) {
	f := newFixture(baseTime)
	ctx := context.Background()

	if _, err := f.recordTaskPerformance.Execute(ctx, usecases.RecordTaskPerformanceRequest{
		KafkaEventId: "evt-1", TaskId: "task-1", AssociateId: "assoc-1", TaskType: shared.Pick,
		ActualSeconds: 40, CompletedAt: baseTime,
	}); err != nil {
		t.Fatalf("first Execute: %v", err)
	}
	// A second completion CLOSES the gap right up to "now".
	secondCompletedAt := baseTime.Add(90 * time.Second)
	if _, err := f.recordTaskPerformance.Execute(ctx, usecases.RecordTaskPerformanceRequest{
		KafkaEventId: "evt-2", TaskId: "task-2", AssociateId: "assoc-1", TaskType: shared.Pick,
		ActualSeconds: 0, CompletedAt: secondCompletedAt,
	}); err != nil {
		t.Fatalf("second Execute: %v", err)
	}

	f2 := newFixtureAt(f, secondCompletedAt)
	result, err := f2.getUtilization.ForAssociate(ctx, "assoc-1", time.Hour)
	if err != nil {
		t.Fatalf("ForAssociate: %v", err)
	}
	if result.OpenGapSeconds != 0 {
		t.Fatalf("OpenGapSeconds = %d, want 0 (the associate just completed a task, right at 'now')", result.OpenGapSeconds)
	}
}

func TestGetUtilization_DefaultsWindowWhenNonPositive(t *testing.T) {
	f := newFixture(baseTime)
	ctx := context.Background()
	if _, err := f.recordTaskPerformance.Execute(ctx, usecases.RecordTaskPerformanceRequest{
		KafkaEventId: "evt-1", TaskId: "task-1", AssociateId: "assoc-1", TaskType: shared.Pick,
		ActualSeconds: 40, CompletedAt: baseTime,
	}); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	result, err := f.getUtilization.ForTaskType(ctx, shared.Pick, 0)
	if err != nil {
		t.Fatalf("ForTaskType: %v", err)
	}
	if result.WindowSeconds != int64((time.Hour).Seconds()) {
		t.Fatalf("WindowSeconds = %d, want the 1h default", result.WindowSeconds)
	}
}

// TestGetUtilization_NilClock_FallsBackToWallClock proves a GetUtilization
// constructed without a Clock is a valid configuration (mirroring every
// other optional dependency in this layer): the read must fall back to the
// wall clock, not panic — the nil-Clock branch of now().
func TestGetUtilization_NilClock_FallsBackToWallClock(t *testing.T) {
	f := newFixture(baseTime)
	uc := &usecases.GetUtilization{Performances: f.performances, IdlePeriods: f.idlePeriods}
	ctx := context.Background()

	byType, err := uc.ForTaskType(ctx, shared.Pick, time.Hour)
	if err != nil {
		t.Fatalf("ForTaskType with nil Clock: %v", err)
	}
	if byType.TaskSeconds != 0 || byType.IdleSeconds != 0 || byType.UtilizationPct != nil {
		t.Fatalf("unexpected non-zero result with empty repos: %+v", byType)
	}

	byAssoc, err := uc.ForAssociate(ctx, "assoc-1", time.Hour)
	if err != nil {
		t.Fatalf("ForAssociate with nil Clock: %v", err)
	}
	if byAssoc.OpenGapSeconds != 0 || byAssoc.Associates != 0 || byAssoc.UtilizationPct != nil {
		t.Fatalf("unexpected non-zero result with empty repos: %+v", byAssoc)
	}
}

// TestGetUtilization_ForTaskType_PropagatesRepoErrors rounds out
// ForTaskType's three infrastructure-failure paths — one per repo call —
// none of which the happy-path in-memory adapters produce on their own.
func TestGetUtilization_ForTaskType_PropagatesRepoErrors(t *testing.T) {
	f := newFixture(baseTime)
	ctx := context.Background()
	cases := []struct {
		name         string
		performances ports.PerformanceRepo
		idlePeriods  ports.IdlePeriodRepo
	}{
		{
			name:         "SumActualSecondsByTaskType failure",
			performances: &failingPerformanceRepo{PerformanceRepo: f.performances, failSumTaskType: true},
			idlePeriods:  f.idlePeriods,
		},
		{
			name:         "SumByTaskType failure",
			performances: f.performances,
			idlePeriods:  &failingIdlePeriodRepo{IdlePeriodRepo: f.idlePeriods, failSumTaskType: true},
		},
		{
			name:         "DistinctAssociatesByTaskType failure",
			performances: f.performances,
			idlePeriods:  &failingIdlePeriodRepo{IdlePeriodRepo: f.idlePeriods, failDistinctAssoc: true},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			uc := &usecases.GetUtilization{Performances: tc.performances, IdlePeriods: tc.idlePeriods, Clock: f.clock}
			if _, err := uc.ForTaskType(ctx, shared.Pick, time.Hour); !errors.Is(err, errUnmapped) {
				t.Fatalf("error = %v, want errUnmapped", err)
			}
		})
	}
}

// TestGetUtilization_ForAssociate_PropagatesRepoErrors rounds out
// ForAssociate's four infrastructure-failure paths — one per repo call.
func TestGetUtilization_ForAssociate_PropagatesRepoErrors(t *testing.T) {
	f := newFixture(baseTime)
	ctx := context.Background()
	cases := []struct {
		name         string
		performances ports.PerformanceRepo
		idlePeriods  ports.IdlePeriodRepo
	}{
		{
			name:         "SumActualSecondsByAssociate failure",
			performances: &failingPerformanceRepo{PerformanceRepo: f.performances, failSumAssociate: true},
			idlePeriods:  f.idlePeriods,
		},
		{
			name:         "SumByAssociate failure",
			performances: f.performances,
			idlePeriods:  &failingIdlePeriodRepo{IdlePeriodRepo: f.idlePeriods, failSumAssociate: true},
		},
		{
			name:         "RecentByAssociateID failure",
			performances: &failingPerformanceRepo{PerformanceRepo: f.performances, failRecent: true},
			idlePeriods:  f.idlePeriods,
		},
		{
			name:         "LastEndedAtByAssociate failure",
			performances: f.performances,
			idlePeriods:  &failingIdlePeriodRepo{IdlePeriodRepo: f.idlePeriods, failLastEndedAt: true},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			uc := &usecases.GetUtilization{Performances: tc.performances, IdlePeriods: tc.idlePeriods, Clock: f.clock}
			if _, err := uc.ForAssociate(ctx, "assoc-1", time.Hour); !errors.Is(err, errUnmapped) {
				t.Fatalf("error = %v, want errUnmapped", err)
			}
		})
	}
}

// TestGetUtilization_ForTaskType_TaskWindow_LowerBoundInclusive pins the
// window's lower boundary through the real write and read paths: a
// completion falling EXACTLY on the window start (now - window) is
// counted, one a second earlier is excluded. Each row is its associate's
// first-ever completion so no idle gap muddies the task-time assertion.
func TestGetUtilization_ForTaskType_TaskWindow_LowerBoundInclusive(t *testing.T) {
	f := newFixture(baseTime)
	ctx := context.Background()
	readAt := baseTime.Add(2 * time.Hour)
	since := baseTime.Add(time.Hour)

	if _, err := f.recordTaskPerformance.Execute(ctx, usecases.RecordTaskPerformanceRequest{
		KafkaEventId: "evt-old", TaskId: "task-old", AssociateId: "assoc-old", TaskType: shared.Pick,
		ActualSeconds: 100, CompletedAt: since.Add(-1 * time.Second),
	}); err != nil {
		t.Fatalf("RecordTaskPerformance(old): %v", err)
	}
	if _, err := f.recordTaskPerformance.Execute(ctx, usecases.RecordTaskPerformanceRequest{
		KafkaEventId: "evt-edge", TaskId: "task-edge", AssociateId: "assoc-edge", TaskType: shared.Pick,
		ActualSeconds: 30, CompletedAt: since,
	}); err != nil {
		t.Fatalf("RecordTaskPerformance(edge): %v", err)
	}

	f2 := newFixtureAt(f, readAt)
	result, err := f2.getUtilization.ForTaskType(ctx, shared.Pick, time.Hour)
	if err != nil {
		t.Fatalf("ForTaskType: %v", err)
	}
	if result.TaskSeconds != 30 {
		t.Fatalf("TaskSeconds = %d, want 30 (only the completion exactly at the window start counts)", result.TaskSeconds)
	}
	if result.IdleSeconds != 0 {
		t.Fatalf("IdleSeconds = %d, want 0 (no gaps: both rows were first completions)", result.IdleSeconds)
	}
	if result.UtilizationPct == nil || *result.UtilizationPct != 100.0 {
		t.Fatalf("UtilizationPct = %v, want 100 (task time only)", result.UtilizationPct)
	}
}

// TestGetUtilization_ForTaskType_IdleWindow_LowerBoundInclusive pins the
// idle-side window boundary: a closed idle gap whose EndedAt falls EXACTLY
// on the window start is counted, one ending a second earlier is excluded
// — mirroring the task side's inclusive lower bound.
func TestGetUtilization_ForTaskType_IdleWindow_LowerBoundInclusive(t *testing.T) {
	f := newFixture(baseTime)
	ctx := context.Background()
	readAt := baseTime.Add(2 * time.Hour)
	since := baseTime.Add(time.Hour)

	// assoc-1: gap [baseTime, since) — ends EXACTLY on the boundary.
	if _, err := f.recordTaskPerformance.Execute(ctx, usecases.RecordTaskPerformanceRequest{
		KafkaEventId: "evt-1a", TaskId: "task-1a", AssociateId: "assoc-1", TaskType: shared.Pick,
		ActualSeconds: 40, CompletedAt: baseTime,
	}); err != nil {
		t.Fatalf("RecordTaskPerformance(1a): %v", err)
	}
	if _, err := f.recordTaskPerformance.Execute(ctx, usecases.RecordTaskPerformanceRequest{
		KafkaEventId: "evt-1b", TaskId: "task-1b", AssociateId: "assoc-1", TaskType: shared.Pick,
		ActualSeconds: 30, CompletedAt: since.Add(30 * time.Second),
	}); err != nil {
		t.Fatalf("RecordTaskPerformance(1b): %v", err)
	}

	// assoc-2: gap [baseTime, since-1s) — ends one second BEFORE the
	// boundary, so its 3599s must be excluded from the window sum.
	if _, err := f.recordTaskPerformance.Execute(ctx, usecases.RecordTaskPerformanceRequest{
		KafkaEventId: "evt-2a", TaskId: "task-2a", AssociateId: "assoc-2", TaskType: shared.Pick,
		ActualSeconds: 40, CompletedAt: baseTime,
	}); err != nil {
		t.Fatalf("RecordTaskPerformance(2a): %v", err)
	}
	if _, err := f.recordTaskPerformance.Execute(ctx, usecases.RecordTaskPerformanceRequest{
		KafkaEventId: "evt-2b", TaskId: "task-2b", AssociateId: "assoc-2", TaskType: shared.Pick,
		ActualSeconds: 30, CompletedAt: since.Add(29 * time.Second),
	}); err != nil {
		t.Fatalf("RecordTaskPerformance(2b): %v", err)
	}

	f2 := newFixtureAt(f, readAt)
	result, err := f2.getUtilization.ForTaskType(ctx, shared.Pick, time.Hour)
	if err != nil {
		t.Fatalf("ForTaskType: %v", err)
	}
	if result.IdleSeconds != 3600 {
		t.Fatalf("IdleSeconds = %d, want 3600 (only the gap ending exactly at the window start counts)", result.IdleSeconds)
	}
	if result.TaskSeconds != 60 {
		t.Fatalf("TaskSeconds = %d, want 60 (both boundary-side completions are in the window)", result.TaskSeconds)
	}
	if result.Associates != 1 {
		t.Fatalf("Associates = %d, want 1 (only assoc-1 has an idle gap inside the window)", result.Associates)
	}
}

// TestGetUtilization_ForAssociate_NeverObserved_AllZeroAndNilPct covers
// the open-gap computation's zero-time branch: an associate this service
// has never recorded anything for contributes NO open gap (no fabricated
// "idle since forever"), no associates count, and a nil — never 0 —
// utilization percentage.
func TestGetUtilization_ForAssociate_NeverObserved_AllZeroAndNilPct(t *testing.T) {
	f := newFixture(baseTime)
	result, err := f.getUtilization.ForAssociate(context.Background(), "assoc-never-seen", time.Hour)
	if err != nil {
		t.Fatalf("ForAssociate: %v", err)
	}
	if result.TaskSeconds != 0 || result.IdleSeconds != 0 || result.OpenGapSeconds != 0 {
		t.Fatalf("unexpected non-zero result for a never-observed associate: %+v", result)
	}
	if result.Associates != 0 {
		t.Fatalf("Associates = %d, want 0 for a never-observed associate", result.Associates)
	}
	if result.UtilizationPct != nil {
		t.Fatalf("UtilizationPct = %v, want nil (nothing was observed — never a fabricated number)", *result.UtilizationPct)
	}
}

// TestGetUtilization_ForAssociate_OpenGap_ClampedAtWindowStart proves the
// open gap can never exceed the window: an associate whose last activity
// predates the window start contributes exactly one window's worth of
// open-gap seconds, not the full time since their last activity. With no
// task time in the window either, utilization is a real measured 0.0% —
// not nil, because there IS a full window of idle time to share out.
func TestGetUtilization_ForAssociate_OpenGap_ClampedAtWindowStart(t *testing.T) {
	f := newFixture(baseTime)
	ctx := context.Background()
	if _, err := f.recordTaskPerformance.Execute(ctx, usecases.RecordTaskPerformanceRequest{
		KafkaEventId: "evt-1", TaskId: "task-1", AssociateId: "assoc-1", TaskType: shared.Pick,
		ActualSeconds: 40, CompletedAt: baseTime,
	}); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	// Read 2h later over a 1h window: the last activity (baseTime) is a
	// full hour before the window start, so the open gap clamps to
	// exactly the window length.
	f2 := newFixtureAt(f, baseTime.Add(2*time.Hour))
	result, err := f2.getUtilization.ForAssociate(ctx, "assoc-1", time.Hour)
	if err != nil {
		t.Fatalf("ForAssociate: %v", err)
	}
	if result.OpenGapSeconds != 3600 {
		t.Fatalf("OpenGapSeconds = %d, want 3600 (clamped to the window, never more)", result.OpenGapSeconds)
	}
	if result.TaskSeconds != 0 {
		t.Fatalf("TaskSeconds = %d, want 0 (the only completion predates the window)", result.TaskSeconds)
	}
	if result.Associates != 1 {
		t.Fatalf("Associates = %d, want 1 (the open gap alone marks the associate as observed)", result.Associates)
	}
	if result.UtilizationPct == nil || *result.UtilizationPct != 0 {
		t.Fatalf("UtilizationPct = %v, want a real measured 0.0 (idle for the whole window)", result.UtilizationPct)
	}
}

// TestGetUtilization_ForAssociate_OpenGap_MeasuredFromMostRecentIdleEnd
// pins lastActivity's "whichever is more recent" contract: when the last
// recorded idle-gap end is NEWER than the last completion, the open gap
// runs from the gap end, not the older completion. This repo state is not
// producible by the current write path (a recorded gap's EndedAt always
// precedes the completion that recorded it) — it is seeded directly, the
// way a backfill or a future write-path change could.
func TestGetUtilization_ForAssociate_OpenGap_MeasuredFromMostRecentIdleEnd(t *testing.T) {
	f := newFixture(baseTime)
	ctx := context.Background()
	if _, err := f.recordTaskPerformance.Execute(ctx, usecases.RecordTaskPerformanceRequest{
		KafkaEventId: "evt-1", TaskId: "task-1", AssociateId: "assoc-1", TaskType: shared.Pick,
		ActualSeconds: 40, CompletedAt: baseTime,
	}); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if err := f.idlePeriods.Save(ctx, idleness.Rehydrate(
		"assoc-1", shared.Pick, baseTime, baseTime.Add(60*time.Second), 60, false,
	)); err != nil {
		t.Fatalf("seed idle period: %v", err)
	}

	f2 := newFixtureAt(f, baseTime.Add(150*time.Second))
	result, err := f2.getUtilization.ForAssociate(ctx, "assoc-1", time.Hour)
	if err != nil {
		t.Fatalf("ForAssociate: %v", err)
	}
	if result.OpenGapSeconds != 90 {
		t.Fatalf("OpenGapSeconds = %d, want 90 (150s since read minus the 60s-newer idle-gap end, not 150 from the older completion)", result.OpenGapSeconds)
	}
	if result.IdleSeconds != 60 {
		t.Fatalf("IdleSeconds = %d, want 60 (the seeded closed gap still sums)", result.IdleSeconds)
	}
	if result.TaskSeconds != 40 {
		t.Fatalf("TaskSeconds = %d, want 40", result.TaskSeconds)
	}
}

// TestGetUtilization_ForAssociate_FutureLastActivity_ContributesNoOpenGap
// covers the open-gap floor: a last-known activity instant in the FUTURE
// (clock skew between the writer that recorded it and this reader)
// contributes zero open-gap seconds — floored, never negative.
func TestGetUtilization_ForAssociate_FutureLastActivity_ContributesNoOpenGap(t *testing.T) {
	f := newFixture(baseTime)
	ctx := context.Background()
	if _, err := f.recordTaskPerformance.Execute(ctx, usecases.RecordTaskPerformanceRequest{
		KafkaEventId: "evt-1", TaskId: "task-1", AssociateId: "assoc-1", TaskType: shared.Pick,
		ActualSeconds: 40, CompletedAt: baseTime,
	}); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	// A recorded idle-gap end 10 minutes in the future of the read time.
	if err := f.idlePeriods.Save(ctx, idleness.Rehydrate(
		"assoc-1", shared.Pick, baseTime.Add(9*time.Minute), baseTime.Add(10*time.Minute), 60, false,
	)); err != nil {
		t.Fatalf("seed idle period: %v", err)
	}

	f2 := newFixtureAt(f, baseTime.Add(5*time.Minute))
	result, err := f2.getUtilization.ForAssociate(ctx, "assoc-1", time.Hour)
	if err != nil {
		t.Fatalf("ForAssociate: %v", err)
	}
	if result.OpenGapSeconds != 0 {
		t.Fatalf("OpenGapSeconds = %d, want 0 (a future last activity is floored at zero, never negative)", result.OpenGapSeconds)
	}
	if result.UtilizationPct == nil || *result.UtilizationPct != 40 {
		t.Fatalf("UtilizationPct = %v, want 40 (100*40/(40+60): task time plus the closed gap only)", result.UtilizationPct)
	}
}

// --- Error propagation (scorecard existence/projection) -------------------

// TestGetAssociateScorecard_PropagatesExistsError covers the
// ExistsByAssociateID infrastructure-failure path.
func TestGetAssociateScorecard_PropagatesExistsError(t *testing.T) {
	f := newFixture(baseTime)
	wrapped := &failingPerformanceRepo{PerformanceRepo: f.performances, failExists: true}
	uc := &usecases.GetAssociateScorecard{Performances: wrapped}
	if _, err := uc.Execute(context.Background(), "assoc-1"); !errors.Is(err, errUnmapped) {
		t.Fatalf("error = %v, want errUnmapped", err)
	}
}

// TestGetAssociateScorecard_PropagatesScorecardForError covers the
// ScorecardFor infrastructure-failure path — reachable only once the
// associate is known to exist.
func TestGetAssociateScorecard_PropagatesScorecardForError(t *testing.T) {
	f := newFixture(baseTime)
	ctx := context.Background()
	if _, err := f.recordTaskPerformance.Execute(ctx, usecases.RecordTaskPerformanceRequest{
		KafkaEventId: "evt-1", TaskId: "task-1", AssociateId: "assoc-1", TaskType: shared.Pick,
		ActualSeconds: 40, CompletedAt: baseTime,
	}); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	wrapped := &failingPerformanceRepo{PerformanceRepo: f.performances, failScorecard: true}
	uc := &usecases.GetAssociateScorecard{Performances: wrapped}
	if _, err := uc.Execute(ctx, "assoc-1"); !errors.Is(err, errUnmapped) {
		t.Fatalf("error = %v, want errUnmapped", err)
	}
}

// TestRecordTaskPerformance_IdleGap_PropagatesPreviousCompletionLookupError
// covers previousCompletionFor's error path, reachable only when
// idle-period recording is wired AND the associate is non-empty — the
// failing RecentByAssociateID call that resolves the gap's start boundary.
func TestRecordTaskPerformance_IdleGap_PropagatesPreviousCompletionLookupError(t *testing.T) {
	f := newFixture(baseTime)
	ctx := context.Background()
	if _, err := f.recordTaskPerformance.Execute(ctx, usecases.RecordTaskPerformanceRequest{
		KafkaEventId: "evt-1", TaskId: "task-1", AssociateId: "assoc-1", TaskType: shared.Pick,
		ActualSeconds: 40, CompletedAt: baseTime,
	}); err != nil {
		t.Fatalf("first Execute: %v", err)
	}

	wrapped := &failingPerformanceRepo{PerformanceRepo: f.performances, failRecent: true}
	uc := &usecases.RecordTaskPerformance{
		Performances: wrapped, Standards: f.standards, Processed: f.processed, Events: events.NewLogPublisher(nil), Clock: f.clock,
		IdlePeriods: f.idlePeriods,
	}
	_, err := uc.Execute(ctx, usecases.RecordTaskPerformanceRequest{
		KafkaEventId: "evt-2", TaskId: "task-2", AssociateId: "assoc-1", TaskType: shared.Pick,
		ActualSeconds: 40, CompletedAt: baseTime.Add(time.Hour),
	})
	if !errors.Is(err, errUnmapped) {
		t.Fatalf("error = %v, want errUnmapped", err)
	}
}

// TestRecordTaskPerformance_IdleGap_NegativeGapLoggedWithLoggerWired
// re-runs the out-of-order-delivery scenario with a Logger wired, proving
// the skip-and-log discipline emits its structured warning without
// failing the enclosing performance write.
func TestRecordTaskPerformance_IdleGap_NegativeGapLoggedWithLoggerWired(t *testing.T) {
	standards := memory.NewStandardRepo()
	performances := memory.NewPerformanceRepo()
	processed := memory.NewProcessedEventRepo()
	idlePeriods := memory.NewIdlePeriodRepo()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	uc := &usecases.RecordTaskPerformance{
		Performances: performances, Standards: standards, Processed: processed, Events: events.NewLogPublisher(nil),
		Clock: memory.FixedClock{At: baseTime}, IdlePeriods: idlePeriods, Logger: logger,
	}
	ctx := context.Background()

	if _, err := uc.Execute(ctx, usecases.RecordTaskPerformanceRequest{
		KafkaEventId: "evt-1", TaskId: "task-1", AssociateId: "assoc-1", TaskType: shared.Pick,
		ActualSeconds: 40, CompletedAt: baseTime.Add(time.Hour),
	}); err != nil {
		t.Fatalf("first Execute: %v", err)
	}

	// Claim instant (completedAt - actualSeconds) lands strictly BEFORE
	// the previous completion — out-of-order Kafka delivery.
	p, err := uc.Execute(ctx, usecases.RecordTaskPerformanceRequest{
		KafkaEventId: "evt-2", TaskId: "task-2", AssociateId: "assoc-1", TaskType: shared.Pick,
		ActualSeconds: 7200, CompletedAt: baseTime.Add(time.Hour).Add(30 * time.Minute),
	})
	if err != nil || p == nil {
		t.Fatalf("second Execute must still succeed (p=%v, err=%v)", p, err)
	}

	_, count, err := idlePeriods.SumByAssociate(ctx, "assoc-1", baseTime.Add(-time.Hour))
	if err != nil {
		t.Fatalf("SumByAssociate: %v", err)
	}
	if count != 0 {
		t.Fatalf("count = %d, want 0 (a negative gap must be skipped, not recorded)", count)
	}
}

// TestRecordTaskPerformance_IdleGap_ZeroLengthGapSkipped pins the
// zero-length boundary of the same rule: a claim instant landing EXACTLY
// on the previous completion (a zero-length gap) is skipped like a
// negative one — idleness.New requires a strictly positive gap.
func TestRecordTaskPerformance_IdleGap_ZeroLengthGapSkipped(t *testing.T) {
	f := newFixture(baseTime)
	ctx := context.Background()

	firstCompletedAt := baseTime.Add(40 * time.Second)
	if _, err := f.recordTaskPerformance.Execute(ctx, usecases.RecordTaskPerformanceRequest{
		KafkaEventId: "evt-1", TaskId: "task-1", AssociateId: "assoc-1", TaskType: shared.Pick,
		ActualSeconds: 40, CompletedAt: firstCompletedAt,
	}); err != nil {
		t.Fatalf("first Execute: %v", err)
	}

	// claimedAt = completedAt - actualSeconds = firstCompletedAt exactly.
	p, err := f.recordTaskPerformance.Execute(ctx, usecases.RecordTaskPerformanceRequest{
		KafkaEventId: "evt-2", TaskId: "task-2", AssociateId: "assoc-1", TaskType: shared.Pick,
		ActualSeconds: 30, CompletedAt: firstCompletedAt.Add(30 * time.Second),
	})
	if err != nil || p == nil {
		t.Fatalf("second Execute must still succeed over a zero-length gap (p=%v, err=%v)", p, err)
	}

	_, count, err := f.idlePeriods.SumByAssociate(ctx, "assoc-1", baseTime.Add(-time.Hour))
	if err != nil {
		t.Fatalf("SumByAssociate: %v", err)
	}
	if count != 0 {
		t.Fatalf("count = %d, want 0 (a zero-length gap must be skipped, not recorded)", count)
	}
}

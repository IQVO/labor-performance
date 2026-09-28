package analyticsstore

import (
	"context"
	"testing"
	"time"

	"github.com/claudioed/labor-performance/internal/analytics/report"
)

func at(h, m int) time.Time {
	return time.Date(2026, 9, 5, h, m, 0, 0, time.UTC)
}

func f64(v float64) *float64 { return &v }

// queryAll runs the widest possible window so a test asserts on
// everything the store holds.
func queryAll(t *testing.T, s *MemoryStore) report.LaborPerformanceReport {
	t.Helper()
	rep, err := s.Query(context.Background(), report.ReportQuery{
		From:        at(0, 0).Add(-24 * time.Hour),
		To:          at(0, 0).Add(48 * time.Hour),
		Granularity: report.GranularityHour,
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	return rep
}

func TestMemoryStoreProjectsTaskPerformanceIntoHourBuckets(t *testing.T) {
	s := NewMemoryStore()

	// Two tasks in the 09:00 bucket, one in 10:00 — the minutes differ,
	// the buckets must not.
	mustApplyTask(t, s, "e1", report.TaskPerformanceFact{
		TaskType: "PICK", ActualSeconds: 40, EfficiencyPct: f64(112.5),
		CompletedAt: at(9, 5), OccurredAt: at(9, 5),
	})
	mustApplyTask(t, s, "e2", report.TaskPerformanceFact{
		TaskType: "PICK", ActualSeconds: 60, EfficiencyPct: f64(75),
		CompletedAt: at(9, 55), OccurredAt: at(9, 55),
	})
	mustApplyTask(t, s, "e3", report.TaskPerformanceFact{
		TaskType: "PICK", ActualSeconds: 50, EfficiencyPct: f64(90),
		CompletedAt: at(10, 1), OccurredAt: at(10, 1),
	})

	rep := queryAll(t, s)

	if len(rep.Rows) != 2 {
		t.Fatalf("got %d rows, want 2 (one per hour bucket): %+v", len(rep.Rows), rep.Rows)
	}
	if rep.Rows[0].TasksRecorded != 2 || rep.Rows[1].TasksRecorded != 1 {
		t.Errorf("bucket task counts = %d, %d; want 2, 1",
			rep.Rows[0].TasksRecorded, rep.Rows[1].TasksRecorded)
	}
	if got := rep.Rows[0].MeanEfficiencyPct(); got == nil || *got != 93.75 {
		t.Errorf("09:00 MeanEfficiencyPct = %v, want 93.75", got)
	}
	if rep.Totals.TasksRecorded != 3 || rep.Totals.TasksScored != 3 {
		t.Errorf("Totals = %+v, want 3 recorded / 3 scored", rep.Totals)
	}
}

func TestMemoryStoreIsIdempotentOnEventId(t *testing.T) {
	s := NewMemoryStore()

	fact := report.TaskPerformanceFact{
		TaskType: "PACK", ActualSeconds: 30, EfficiencyPct: f64(100),
		CompletedAt: at(9, 0), OccurredAt: at(9, 0),
	}

	// The same event delivered three times — Kafka's at-least-once
	// guarantee in miniature.
	for i := 0; i < 3; i++ {
		if err := s.ApplyTaskPerformanceRecorded(context.Background(), "dup-event", fact); err != nil {
			t.Fatalf("apply %d: %v", i, err)
		}
	}

	rep := queryAll(t, s)
	if len(rep.Rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(rep.Rows))
	}
	if rep.Rows[0].TasksRecorded != 1 {
		t.Errorf("TasksRecorded = %d after 3 deliveries of one event id, want 1 (no double-count)",
			rep.Rows[0].TasksRecorded)
	}
	if got := rep.Rows[0].MeanEfficiencyPct(); got == nil || *got != 100 {
		t.Errorf("MeanEfficiencyPct = %v, want 100 — a double-count would still average 100, "+
			"so the count assertion above is the real one", got)
	}
}

func TestMemoryStoreUnscorableAndUnmeasurableTasksStillCount(t *testing.T) {
	s := NewMemoryStore()

	// duration_seconds=0 on the wire: recorded, unmeasurable, unscorable.
	mustApplyTask(t, s, "e1", report.TaskPerformanceFact{
		TaskType: "PICK", ActualSeconds: 0, EfficiencyPct: nil,
		CompletedAt: at(9, 0), OccurredAt: at(9, 0),
	})
	// A real duration but no standard existed: measured, still unscored.
	mustApplyTask(t, s, "e2", report.TaskPerformanceFact{
		TaskType: "PICK", ActualSeconds: 45, EfficiencyPct: nil,
		CompletedAt: at(9, 10), OccurredAt: at(9, 10),
	})

	rep := queryAll(t, s)
	row := rep.Rows[0]

	if row.TasksRecorded != 2 {
		t.Errorf("TasksRecorded = %d, want 2 — both are real business facts", row.TasksRecorded)
	}
	if row.TasksScored != 0 || row.TasksUnscored() != 2 {
		t.Errorf("scored/unscored = %d/%d, want 0/2", row.TasksScored, row.TasksUnscored())
	}
	if got := row.MeanEfficiencyPct(); got != nil {
		t.Errorf("MeanEfficiencyPct = %v, want nil — never a fabricated 0", *got)
	}
	if got := row.MeanActualSeconds(); got == nil || *got != 45 {
		t.Errorf("MeanActualSeconds = %v, want 45 — the zero-duration row must not drag the mean down", got)
	}
}

func TestMemoryStoreNormalizesUnclassifiedTaskType(t *testing.T) {
	s := NewMemoryStore()

	mustApplyTask(t, s, "e1", report.TaskPerformanceFact{
		TaskType: "", ActualSeconds: 20, CompletedAt: at(9, 0), OccurredAt: at(9, 0),
	})

	rep := queryAll(t, s)
	if len(rep.Rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(rep.Rows))
	}
	if rep.Rows[0].Key.TaskType != report.UnclassifiedTaskType {
		t.Errorf("TaskType = %q, want %q — an empty task type must never become an empty key",
			rep.Rows[0].Key.TaskType, report.UnclassifiedTaskType)
	}
}

func TestMemoryStoreProjectsStandardEvents(t *testing.T) {
	s := NewMemoryStore()

	if err := s.ApplyLaborStandardDefined(context.Background(), "s1", report.StandardFact{
		TaskType: "PICK", ExpectedSeconds: 45,
		EffectiveFrom: at(9, 0), OccurredAt: at(9, 0),
	}); err != nil {
		t.Fatalf("ApplyLaborStandardDefined: %v", err)
	}
	if err := s.ApplyLaborStandardRevised(context.Background(), "s2", report.StandardFact{
		TaskType: "PICK", ExpectedSeconds: 40,
		EffectiveFrom: at(10, 0), OccurredAt: at(10, 0),
	}); err != nil {
		t.Fatalf("ApplyLaborStandardRevised: %v", err)
	}

	rep := queryAll(t, s)
	if len(rep.ByTaskType) != 1 {
		t.Fatalf("got %d bars, want 1", len(rep.ByTaskType))
	}
	bar := rep.ByTaskType[0]
	if bar.StandardsDefined != 1 || bar.StandardsRevised != 1 {
		t.Errorf("bar standards = %d defined / %d revised, want 1/1", bar.StandardsDefined, bar.StandardsRevised)
	}
	// A window containing only standard events has no tasks, so no mean.
	if bar.TasksRecorded != 0 || bar.MeanEfficiencyPct != nil {
		t.Errorf("bar = %+v, want zero tasks and a nil mean", bar)
	}
}

func TestMemoryStoreQueryFilters(t *testing.T) {
	s := NewMemoryStore()

	mustApplyTask(t, s, "e1", report.TaskPerformanceFact{
		TaskType: "PICK", ActualSeconds: 10, CompletedAt: at(9, 0), OccurredAt: at(9, 0),
	})
	mustApplyTask(t, s, "e2", report.TaskPerformanceFact{
		TaskType: "PACK", ActualSeconds: 10, CompletedAt: at(9, 0), OccurredAt: at(9, 0),
	})
	mustApplyTask(t, s, "e3", report.TaskPerformanceFact{
		TaskType: "PICK", ActualSeconds: 10, CompletedAt: at(11, 0), OccurredAt: at(11, 0),
	})

	t.Run("task type filter", func(t *testing.T) { assertQueryFiltersByTaskType(t, s) })
	t.Run("window is from-inclusive and to-exclusive", func(t *testing.T) { assertQueryWindowHalfOpen(t, s) })
	t.Run("an empty window yields an empty but non-nil report", func(t *testing.T) { assertQueryEmptyWindow(t, s) })
}

// assertQueryFiltersByTaskType pins the exact-match TaskType filter.
func assertQueryFiltersByTaskType(t *testing.T, s *MemoryStore) {
	t.Helper()
	rep, err := s.Query(context.Background(), report.ReportQuery{From: at(0, 0), To: at(23, 0), TaskType: "PICK"})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(rep.Rows) != 2 {
		t.Fatalf("got %d rows, want 2", len(rep.Rows))
	}
	for _, row := range rep.Rows {
		if row.Key.TaskType != "PICK" {
			t.Errorf("row leaked through the filter: %+v", row.Key)
		}
	}
}

// assertQueryWindowHalfOpen pins the half-open window: [from, to).
func assertQueryWindowHalfOpen(t *testing.T, s *MemoryStore) {
	t.Helper()
	// [09:00, 11:00) must include the 09:00 buckets and exclude the
	// 11:00 one.
	rep, err := s.Query(context.Background(), report.ReportQuery{From: at(9, 0), To: at(11, 0)})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(rep.Rows) != 2 {
		t.Fatalf("got %d rows, want 2 (both 09:00 buckets, not the 11:00 one): %+v", len(rep.Rows), rep.Rows)
	}
	for _, row := range rep.Rows {
		if !row.Key.HourBucket.Equal(at(9, 0)) {
			t.Errorf("row outside the half-open window: %+v", row.Key)
		}
	}
}

// assertQueryEmptyWindow pins an empty window's report shape: non-nil
// empty rows and nil means.
func assertQueryEmptyWindow(t *testing.T, s *MemoryStore) {
	t.Helper()
	rep, err := s.Query(context.Background(), report.ReportQuery{From: at(20, 0), To: at(22, 0)})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if rep.Rows == nil || len(rep.Rows) != 0 {
		t.Errorf("Rows = %+v, want an empty non-nil slice", rep.Rows)
	}
	if rep.Totals.MeanEfficiencyPct != nil || rep.Totals.MeanActualSeconds != nil {
		t.Errorf("Totals = %+v, want nil means for an empty window", rep.Totals)
	}
}

func TestMemoryStoreFreshnessLag(t *testing.T) {
	ctx := context.Background()

	t.Run("an empty store lags by zero", func(t *testing.T) {
		s := NewMemoryStore()
		lag, err := s.FreshnessLag(ctx)
		if err != nil {
			t.Fatalf("FreshnessLag: %v", err)
		}
		if lag != 0 {
			t.Errorf("lag = %v, want 0", lag)
		}
	})

	t.Run("lag is measured from the newest applied event", func(t *testing.T) {
		s := NewMemoryStore()
		s.Now = func() time.Time { return at(10, 30) }

		mustApplyTask(t, s, "old", report.TaskPerformanceFact{
			TaskType: "PICK", CompletedAt: at(8, 0), OccurredAt: at(8, 0),
		})
		mustApplyTask(t, s, "new", report.TaskPerformanceFact{
			TaskType: "PICK", CompletedAt: at(10, 0), OccurredAt: at(10, 0),
		})

		lag, err := s.FreshnessLag(ctx)
		if err != nil {
			t.Fatalf("FreshnessLag: %v", err)
		}
		if lag != 30*time.Minute {
			t.Errorf("lag = %v, want 30m (now 10:30 minus the NEWEST event at 10:00)", lag)
		}
	})

	t.Run("a future-dated event clamps the lag at zero", func(t *testing.T) {
		s := NewMemoryStore()
		s.Now = func() time.Time { return at(9, 0) }

		mustApplyTask(t, s, "future", report.TaskPerformanceFact{
			TaskType: "PICK", CompletedAt: at(11, 0), OccurredAt: at(11, 0),
		})

		lag, err := s.FreshnessLag(ctx)
		if err != nil {
			t.Fatalf("FreshnessLag: %v", err)
		}
		if lag != 0 {
			t.Errorf("lag = %v, want 0 — a negative lag is never reported", lag)
		}
	})
}

func TestMemoryStoreConsumerGateIsSeparateFromTheProjection(t *testing.T) {
	// The consumer gate and the projection must claim event ids in
	// SEPARATE sets — mirroring the two Postgres tables. If they shared
	// one, the gate admitting an event would make the projection treat
	// it as a duplicate and silently drop every single row.
	s := NewMemoryStore()

	isNew, err := s.MarkProcessed(context.Background(), "e1")
	if err != nil {
		t.Fatalf("MarkProcessed: %v", err)
	}
	if !isNew {
		t.Fatal("MarkProcessed returned false for a first-time event id")
	}

	mustApplyTask(t, s, "e1", report.TaskPerformanceFact{
		TaskType: "PICK", ActualSeconds: 30, EfficiencyPct: f64(100),
		CompletedAt: at(9, 0), OccurredAt: at(9, 0),
	})

	rep := queryAll(t, s)
	if len(rep.Rows) != 1 || rep.Rows[0].TasksRecorded != 1 {
		t.Fatalf("the projection dropped an event the consumer gate had admitted: %+v", rep.Rows)
	}

	// The gate itself still dedupes on a redelivery.
	if isNew, err := s.MarkProcessed(context.Background(), "e1"); err != nil || isNew {
		t.Errorf("MarkProcessed(dup) = (%v, %v), want (false, nil)", isNew, err)
	}
}

func mustApplyTask(t *testing.T, s *MemoryStore, eventId string, f report.TaskPerformanceFact) {
	t.Helper()
	if err := s.ApplyTaskPerformanceRecorded(context.Background(), eventId, f); err != nil {
		t.Fatalf("ApplyTaskPerformanceRecorded(%s): %v", eventId, err)
	}
}

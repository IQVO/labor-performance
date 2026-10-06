//go:build integration

package analyticsstore_test

import (
	"context"
	"errors"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/claudioed/labor-performance/internal/adapters/outbound/analyticsstore"
	"github.com/claudioed/labor-performance/internal/adapters/outbound/postgres"
	"github.com/claudioed/labor-performance/internal/analytics/report"
)

// analyticsDB boots a throwaway ANALYTICAL Postgres (testcontainers — the
// test owns its own database, never an external DSN) and runs the
// migrations/analytics set. This is the integration coverage ADR-0007
// deferred ("A Postgres integration test for the analytics store"); the
// in-memory implementation alone could never prove the projection's SQL
// (the ON CONFLICT claim, the commutative += upsert, the read model's
// window/filters) against a real database.
func analyticsDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("analytics"),
		tcpostgres.WithUsername("analytics"),
		tcpostgres.WithPassword("analytics"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(container) })

	url, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}
	_, thisFile, _, _ := runtime.Caller(0)
	migrations := filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "..", "migrations", "analytics")
	if err := postgres.RunMigrations(url, migrations); err != nil {
		t.Fatalf("run analytics migrations: %v", err)
	}
	pool, err := analyticsstore.NewPool(ctx, url)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func hour(h int) time.Time { return time.Date(2026, 9, 5, h, 0, 0, 0, time.UTC) }

// TestPostgresProjection_IdempotentFoldAndReadModel drives the writer
// (PostgresProjection) and the reader (PostgresReport) against one real
// analytical database: every Apply* is idempotent on eventId, the rollup
// buckets by BUSINESS time, an unclassified task type normalises to the
// explicit UNCLASSIFIED bucket, and the report serves raw counters with
// nullable means — including tasksMeasured, the denominator of
// meanActualSeconds (ADR-0007).
func TestPostgresProjection_IdempotentFoldAndReadModel(t *testing.T) {
	pool := analyticsDB(t)
	ctx := context.Background()
	projection := analyticsstore.NewPostgresProjection(pool)
	reader := analyticsstore.NewPostgresReport(pool)

	efficiency := 90.0
	facts := []struct {
		eventId string
		f       report.TaskPerformanceFact
	}{
		{"evt-1", report.TaskPerformanceFact{TaskType: "pick", ActualSeconds: 50, EfficiencyPct: &efficiency, CompletedAt: hour(9), OccurredAt: hour(9)}},
		{"evt-2", report.TaskPerformanceFact{TaskType: "PICK", ActualSeconds: 70, EfficiencyPct: &efficiency, CompletedAt: hour(9), OccurredAt: hour(9)}},
		{"evt-3", report.TaskPerformanceFact{TaskType: "", ActualSeconds: 0, EfficiencyPct: nil, CompletedAt: hour(9), OccurredAt: hour(9)}},
		{"evt-4", report.TaskPerformanceFact{TaskType: "PACK", ActualSeconds: 40, EfficiencyPct: nil, CompletedAt: hour(10), OccurredAt: hour(10)}},
	}
	for _, fact := range facts {
		if err := projection.ApplyTaskPerformanceRecorded(ctx, fact.eventId, fact.f); err != nil {
			t.Fatalf("apply %s: %v", fact.eventId, err)
		}
		// Kafka is at-least-once: every event is applied a SECOND time and
		// must fold exactly once.
		if err := projection.ApplyTaskPerformanceRecorded(ctx, fact.eventId, fact.f); err != nil {
			t.Fatalf("re-apply %s: %v", fact.eventId, err)
		}
	}

	if err := projection.ApplyLaborStandardDefined(ctx, "std-evt-1", report.StandardFact{TaskType: "pick", ExpectedSeconds: 45, EffectiveFrom: hour(9), OccurredAt: hour(9)}); err != nil {
		t.Fatalf("apply standard defined: %v", err)
	}
	if err := projection.ApplyLaborStandardDefined(ctx, "std-evt-1", report.StandardFact{TaskType: "pick", ExpectedSeconds: 45, EffectiveFrom: hour(9), OccurredAt: hour(9)}); err != nil {
		t.Fatalf("re-apply standard defined (must be a no-op): %v", err)
	}
	if err := projection.ApplyLaborStandardRevised(ctx, "std-evt-2", report.StandardFact{TaskType: "pick", ExpectedSeconds: 50, EffectiveFrom: hour(10), OccurredAt: hour(10)}); err != nil {
		t.Fatalf("apply standard revised: %v", err)
	}

	rep, err := reader.Query(ctx, report.ReportQuery{From: hour(0), To: hour(24)})
	if err != nil {
		t.Fatalf("query: %v", err)
	}

	// Row keys: (PICK, 09), (UNCLASSIFIED, 09), (PACK, 10), (PICK, 10 —
	// the revision bucket).
	rows := map[string]report.Row{}
	for _, row := range rep.Rows {
		rows[row.Key.TaskType+"@"+row.Key.HourBucket.Format("15")] = row
	}

	pick9, ok := rows["PICK@09"]
	if !ok {
		t.Fatalf("expected a (PICK, 09:00) row; got keys %v", keysOf(rows))
	}
	if pick9.TasksRecorded != 2 || pick9.TasksScored != 2 || pick9.TasksMeasured != 2 {
		t.Fatalf("PICK@9 counters = recorded %d / scored %d / measured %d, want 2/2/2",
			pick9.TasksRecorded, pick9.TasksScored, pick9.TasksMeasured)
	}
	if pick9.StandardsDefined != 1 {
		t.Fatalf("PICK@9 standardsDefined = %d, want 1", pick9.StandardsDefined)
	}
	if pick9.MeanActualSeconds() == nil || *pick9.MeanActualSeconds() != 60 {
		t.Fatalf("PICK@9 meanActualSeconds = %v, want 60 ((50+70)/2)", pick9.MeanActualSeconds())
	}
	if pick9.MeanEfficiencyPct() == nil || *pick9.MeanEfficiencyPct() != 90 {
		t.Fatalf("PICK@9 meanEfficiencyPct = %v, want 90", pick9.MeanEfficiencyPct())
	}

	unc, ok := rows["UNCLASSIFIED@09"]
	if !ok {
		t.Fatalf("expected an explicit (UNCLASSIFIED, 09:00) bucket; got keys %v", keysOf(rows))
	}
	if unc.TasksRecorded != 1 || unc.TasksScored != 0 || unc.TasksMeasured != 0 {
		t.Fatalf("UNCLASSIFIED@9 counters = %d/%d/%d, want 1/0/0", unc.TasksRecorded, unc.TasksScored, unc.TasksMeasured)
	}
	if unc.MeanActualSeconds() != nil || unc.MeanEfficiencyPct() != nil {
		t.Fatal("UNCLASSIFIED@9 means must be null — never fabricated")
	}

	pack10, ok := rows["PACK@10"]
	if !ok {
		t.Fatalf("expected a (PACK, 10:00) row; got keys %v", keysOf(rows))
	}
	// Measured but NOT scored — ADR-0006's distinction, on the wire.
	if pack10.TasksMeasured != 1 || pack10.TasksScored != 0 {
		t.Fatalf("PACK@10 measured/scored = %d/%d, want 1/0", pack10.TasksMeasured, pack10.TasksScored)
	}
	if pack10.MeanEfficiencyPct() != nil || pack10.MeanActualSeconds() == nil {
		t.Fatal("PACK@10 must have a meanActualSeconds but a null meanEfficiencyPct")
	}

	if rep.Totals.TasksRecorded != 4 || rep.Totals.TasksScored != 2 || rep.Totals.TasksMeasured != 3 {
		t.Fatalf("totals = recorded %d / scored %d / measured %d, want 4/2/3",
			rep.Totals.TasksRecorded, rep.Totals.TasksScored, rep.Totals.TasksMeasured)
	}

	// The taskType filter narrows to the PICK rows only.
	pickOnly, err := reader.Query(ctx, report.ReportQuery{From: hour(0), To: hour(24), TaskType: "PICK"})
	if err != nil {
		t.Fatalf("filtered query: %v", err)
	}
	if pickOnly.Totals.TasksRecorded != 2 || pickOnly.Totals.TasksMeasured != 2 {
		t.Fatalf("PICK-filtered totals = %d/%d, want 2/2", pickOnly.Totals.TasksRecorded, pickOnly.Totals.TasksMeasured)
	}

	// Freshness: the lag is anchored to the most recent applied event's
	// occurred_at (hour 10 on 2026-09-05), i.e. now minus that instant,
	// within a small margin for the test's own runtime.
	lag, err := reader.FreshnessLag(ctx)
	if err != nil {
		t.Fatalf("freshness lag: %v", err)
	}
	want := time.Since(hour(10))
	if lag < 0 {
		t.Fatalf("freshness lag must never be negative, got %v", lag)
	}
	if lag < want-time.Minute || lag > want+time.Minute {
		t.Fatalf("freshness lag = %v, want ~%v (anchored to the most recent event's occurred_at)", lag, want)
	}
}

func keysOf(m map[string]report.Row) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// TestConsumedEventsRepo_MarkProcessed proves the consumer gate's own
// table behaves as the in-memory fake always has: first claim true,
// duplicate false — against the real analytics schema.
func TestConsumedEventsRepo_MarkProcessed(t *testing.T) {
	pool := analyticsDB(t)
	repo := analyticsstore.NewConsumedEventsRepo(pool)
	ctx := context.Background()

	first, err := repo.MarkProcessed(ctx, "gate-evt-1")
	if err != nil || !first {
		t.Fatalf("first MarkProcessed = %v, %v; want true, nil", first, err)
	}
	second, err := repo.MarkProcessed(ctx, "gate-evt-1")
	if err != nil || second {
		t.Fatalf("duplicate MarkProcessed = %v, %v; want false, nil", second, err)
	}
}

// TestUnitOfWork_ClaimAndProjectionCommitOrRollBackTogether proves the
// consumer's claim (analytics_consumed_events), the projection's own claim
// (analytics_processed_events) and the rollup upsert are ONE transaction
// when run through the UnitOfWork: a failure after all three writes
// leaves none of them behind, and a success persists all three.
func TestUnitOfWork_ClaimAndProjectionCommitOrRollBackTogether(t *testing.T) {
	pool := analyticsDB(t)
	ctx := context.Background()
	uow := analyticsstore.NewUnitOfWork(pool)
	gate := analyticsstore.NewConsumedEventsRepo(pool)
	projection := analyticsstore.NewPostgresProjection(pool)
	fact := report.TaskPerformanceFact{TaskType: "PICK", ActualSeconds: 50, CompletedAt: hour(9), OccurredAt: hour(9)}

	count := func(query string) int64 {
		t.Helper()
		var n int64
		if err := pool.QueryRow(ctx, query).Scan(&n); err != nil {
			t.Fatalf("%s: %v", query, err)
		}
		return n
	}

	boom := errors.New("boom after all writes")
	err := uow.Execute(ctx, func(ctx context.Context) error {
		if isNew, err := gate.MarkProcessed(ctx, "uow-evt-1"); err != nil || !isNew {
			t.Fatalf("in-tx MarkProcessed = %v, %v; want true, nil", isNew, err)
		}
		if err := projection.ApplyTaskPerformanceRecorded(ctx, "uow-evt-1", fact); err != nil {
			t.Fatalf("in-tx apply: %v", err)
		}
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("Execute = %v, want the callback's error", err)
	}
	for _, q := range []string{
		"SELECT count(*) FROM analytics_consumed_events",
		"SELECT count(*) FROM analytics_processed_events",
		"SELECT count(*) FROM labor_performance_rollup",
	} {
		if n := count(q); n != 0 {
			t.Fatalf("%s = %d after rollback, want 0 — the writes were not one transaction", q, n)
		}
	}

	// The same event now applies cleanly (it was never claimed) and commits.
	if err := uow.Execute(ctx, func(ctx context.Context) error {
		if isNew, err := gate.MarkProcessed(ctx, "uow-evt-1"); err != nil || !isNew {
			t.Fatalf("retry MarkProcessed = %v, %v; want true, nil", isNew, err)
		}
		return projection.ApplyTaskPerformanceRecorded(ctx, "uow-evt-1", fact)
	}); err != nil {
		t.Fatalf("Execute (success): %v", err)
	}
	if n := count("SELECT COALESCE(SUM(tasks_recorded),0) FROM labor_performance_rollup"); n != 1 {
		t.Fatalf("tasks_recorded = %d after the committed retry, want 1", n)
	}
	if n := count("SELECT count(*) FROM analytics_consumed_events"); n != 1 {
		t.Fatalf("analytics_consumed_events = %d, want 1", n)
	}
}

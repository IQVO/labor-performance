//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/claudioed/labor-performance/internal/adapters/outbound/memory"
	"github.com/claudioed/labor-performance/internal/adapters/outbound/postgres"
	"github.com/claudioed/labor-performance/internal/application/ports"
	"github.com/claudioed/labor-performance/internal/application/usecases"
	"github.com/claudioed/labor-performance/internal/domain/performance"
	"github.com/claudioed/labor-performance/internal/domain/shared"
	"github.com/claudioed/labor-performance/internal/domain/standard"
)

func unitStandard(t *testing.T, id shared.StandardId, unit shared.HandlingUnit, seconds int64, lift *standard.TeamLift, from time.Time) *standard.LaborStandard {
	t.Helper()
	s, err := standard.NewForUnit(id, shared.Pick, unit, seconds, nil, lift, from)
	if err != nil {
		t.Fatalf("NewForUnit: %v", err)
	}
	return s
}

func TestPostgres_HandlingUnitStandardRoundTripAndIsolation(t *testing.T) {
	pool := testDB(t)
	repo := postgres.NewStandardRepo(pool)
	ctx := context.Background()
	from := time.Now().UTC().Truncate(time.Microsecond).Add(-time.Hour)

	tl, err := standard.NewTeamLift(18000, 75)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range []*standard.LaborStandard{
		unitStandard(t, "std-any", "", 45, nil, from),
		unitStandard(t, "std-case", shared.Case, 30, &tl, from),
		unitStandard(t, "std-each", shared.Each, 12, nil, from),
		unitStandard(t, "std-pallet", shared.Pallet, 90, nil, from),
	} {
		if err := repo.Save(ctx, s); err != nil {
			t.Fatalf("save %s: %v", s.ID(), err)
		}
	}

	// Each pair resolves to exactly its own row.
	for unit, want := range map[shared.HandlingUnit]shared.StandardId{
		"": "std-any", shared.Case: "std-case", shared.Each: "std-each", shared.Pallet: "std-pallet",
	} {
		got, err := repo.FindCurrentlyActiveForUnit(ctx, shared.Pick, unit)
		if err != nil || got == nil || got.ID() != want {
			t.Fatalf("FindCurrentlyActiveForUnit(%q) = %v, %v; want %s", unit, got, err, want)
		}
		asOf, err := repo.FindActiveAsOfForUnit(ctx, shared.Pick, unit, from.Add(time.Minute))
		if err != nil || asOf == nil || asOf.ID() != want {
			t.Fatalf("FindActiveAsOfForUnit(%q) = %v, %v; want %s", unit, asOf, err, want)
		}
	}

	// The pre-ADR-0035 lookups see ONLY the task-type-only standard.
	legacy, err := repo.FindCurrentlyActive(ctx, shared.Pick)
	if err != nil || legacy == nil || legacy.ID() != "std-any" || legacy.HandlingUnit() != "" || legacy.TeamLift() != nil {
		t.Fatalf("FindCurrentlyActive = %v, %v; want std-any", legacy, err)
	}
	legacyAsOf, err := repo.FindActiveAsOf(ctx, shared.Pick, from.Add(time.Minute))
	if err != nil || legacyAsOf == nil || legacyAsOf.ID() != "std-any" {
		t.Fatalf("FindActiveAsOf = %v, %v; want std-any", legacyAsOf, err)
	}

	// Team lift and unit survive the round trip.
	c, _ := repo.FindCurrentlyActiveForUnit(ctx, shared.Pick, shared.Case)
	if c.HandlingUnit() != shared.Case || c.TeamLift() == nil || c.TeamLift().ThresholdG() != 18000 || c.TeamLift().ExpectedSeconds() != 75 || c.ExpectedSeconds() != 30 {
		t.Fatalf("CASE round trip lost data: %+v", c)
	}

	// A unit with no declared standard resolves to nothing: no cross-unit leakage.
	if got, _ := repo.FindCurrentlyActiveForUnit(ctx, shared.Pack, shared.Case); got != nil {
		t.Fatalf("PACK/CASE must be empty, got %v", got.ID())
	}
}

func TestPostgres_OneOpenStandardPerTaskTypeAndHandlingUnit(t *testing.T) {
	pool := testDB(t)
	repo := postgres.NewStandardRepo(pool)
	ctx := context.Background()
	from := time.Now().UTC().Truncate(time.Microsecond).Add(-time.Hour)

	if err := repo.Save(ctx, unitStandard(t, "a", "", 45, nil, from)); err != nil {
		t.Fatal(err)
	}
	// A CASE standard coexists with the open task-type-only one.
	if err := repo.Save(ctx, unitStandard(t, "b", shared.Case, 30, nil, from)); err != nil {
		t.Fatalf("CASE alongside task-type-only must be allowed: %v", err)
	}
	// A second open CASE standard for the same task type conflicts...
	if err := repo.Save(ctx, unitStandard(t, "c", shared.Case, 31, nil, from.Add(time.Minute))); err != ports.ErrOpenStandardConflict {
		t.Fatalf("second open CASE standard: err = %v, want ErrOpenStandardConflict", err)
	}
	// ...and a second open task-type-only one still conflicts too.
	if err := repo.Save(ctx, unitStandard(t, "d", "", 46, nil, from.Add(time.Minute))); err != ports.ErrOpenStandardConflict {
		t.Fatalf("second open task-type-only standard: err = %v, want ErrOpenStandardConflict", err)
	}
	// Closing the CASE one frees only the CASE slot.
	b, _ := repo.FindCurrentlyActiveForUnit(ctx, shared.Pick, shared.Case)
	b.Close(from.Add(time.Hour))
	if err := repo.Save(ctx, b); err != nil {
		t.Fatalf("close CASE: %v", err)
	}
	if err := repo.Save(ctx, unitStandard(t, "e", shared.Case, 29, nil, from.Add(time.Hour))); err != nil {
		t.Fatalf("new CASE after close: %v", err)
	}
	if got, _ := repo.FindCurrentlyActive(ctx, shared.Pick); got == nil || got.ID() != "a" || got.EffectiveTo() != nil {
		t.Fatalf("task-type-only standard was disturbed: %v", got)
	}
}

func TestPostgres_HandlingUnitChecksRejectInvalidRows(t *testing.T) {
	pool := testDB(t)
	ctx := context.Background()
	cases := []struct{ name, sql string }{
		{"unknown unit", `INSERT INTO labor_standards (id, task_type, expected_seconds, effective_from, handling_unit) VALUES ('x1','PICK',10,now(),'CARTON')`},
		{"team lift on EACH", `INSERT INTO labor_standards (id, task_type, expected_seconds, effective_from, handling_unit, team_lift_threshold_g, team_lift_expected_seconds) VALUES ('x2','PICK',10,now(),'EACH',100,5)`},
		{"team lift without unit", `INSERT INTO labor_standards (id, task_type, expected_seconds, effective_from, team_lift_threshold_g, team_lift_expected_seconds) VALUES ('x3','PICK',10,now(),100,5)`},
		{"half a team lift (threshold only)", `INSERT INTO labor_standards (id, task_type, expected_seconds, effective_from, handling_unit, team_lift_threshold_g) VALUES ('x4','PICK',10,now(),'CASE',100)`},
		{"half a team lift (seconds only)", `INSERT INTO labor_standards (id, task_type, expected_seconds, effective_from, handling_unit, team_lift_expected_seconds) VALUES ('x5','PICK',10,now(),'CASE',5)`},
		{"zero threshold", `INSERT INTO labor_standards (id, task_type, expected_seconds, effective_from, handling_unit, team_lift_threshold_g, team_lift_expected_seconds) VALUES ('x6','PICK',10,now(),'CASE',0,5)`},
		{"zero team lift seconds", `INSERT INTO labor_standards (id, task_type, expected_seconds, effective_from, handling_unit, team_lift_threshold_g, team_lift_expected_seconds) VALUES ('x7','PICK',10,now(),'CASE',100,0)`},
	}
	for _, tc := range cases {
		if _, err := pool.Exec(ctx, tc.sql); err == nil {
			t.Fatalf("%s: the database accepted an invalid row", tc.name)
		}
	}
	ok := `INSERT INTO labor_standards (id, task_type, expected_seconds, effective_from, handling_unit, team_lift_threshold_g, team_lift_expected_seconds) VALUES ('ok','PICK',10,now(),'CASE',1,1)`
	if _, err := pool.Exec(ctx, ok); err != nil {
		t.Fatalf("valid CASE team lift (smallest values) rejected: %v", err)
	}
}

func TestPostgres_PerformanceRoundTripsHandlingUnitAndTeamLift(t *testing.T) {
	pool := testDB(t)
	repo := postgres.NewPerformanceRepo(pool)
	ctx := context.Background()
	at := time.Now().UTC().Truncate(time.Microsecond)

	lift, err := performance.NewWithHandling("e-lift", "t1", "a1", shared.Pick, shared.Case, true, 75, 75, at.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	plain, err := performance.New("e-plain", "t2", "a1", shared.Pick, 60, 45, at)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range []*performance.TaskPerformance{lift, plain} {
		if err := repo.Save(ctx, p); err != nil {
			t.Fatal(err)
		}
	}
	rows, err := repo.RecentByAssociateID(ctx, "a1", 10)
	if err != nil || len(rows) != 2 {
		t.Fatalf("rows = %d, err = %v", len(rows), err)
	}
	if rows[0].EventId() != "e-lift" || rows[0].HandlingUnit() != shared.Case || !rows[0].TeamLift() || rows[0].StandardSecondsAtCompletion() != 75 {
		t.Fatalf("handled row lost data: %+v", rows[0])
	}
	if rows[1].EventId() != "e-plain" || rows[1].HandlingUnit() != "" || rows[1].TeamLift() {
		t.Fatalf("plain row must default to no handling: %+v", rows[1])
	}
}

func TestOutbox_HandlingUnitFlowsEndToEndThroughTheRealStack(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	uow := postgres.NewUnitOfWork(pool)
	pub := postgres.NewOutboxPublisher(pool, analyticsEncoder(), integrationEncoder())
	standards := postgres.NewStandardRepo(pool)

	define := &usecases.DefineStandard{Standards: standards, Events: pub, Clock: memory.FixedClock{At: now.Add(-time.Hour)}, UnitOfWork: uow}
	if _, err := define.Execute(ctx, shared.Pick, 45, nil); err != nil {
		t.Fatalf("define task-type-only: %v", err)
	}
	if _, err := define.ExecuteSpec(ctx, usecases.DefineStandardSpec{
		TaskType: shared.Pick, HandlingUnit: shared.Case, ExpectedSeconds: 30,
		TeamLift: &usecases.TeamLiftSpec{ThresholdG: 18000, ExpectedSeconds: 75},
	}); err != nil {
		t.Fatalf("define CASE: %v", err)
	}
	// The CASE definition is a Defined (not a Revised) and carries the unit.
	if got := countOutbox(t, pool, "event_type LIKE '%LaborStandardDefined' AND convert_from(value,'UTF8')::jsonb->'data'->>'handling_unit' = 'CASE' AND (convert_from(value,'UTF8')::jsonb->'data'->>'team_lift_threshold_g')::bigint = 18000 AND (convert_from(value,'UTF8')::jsonb->'data'->>'team_lift_expected_seconds')::bigint = 75"); got != 1 {
		t.Fatalf("expected 1 LaborStandardDefined outbox row carrying the CASE declaration, got %d", got)
	}
	if got := countOutbox(t, pool, "event_type LIKE '%LaborStandardRevised'"); got != 0 {
		t.Fatalf("a CASE definition must not revise the task-type-only standard, got %d Revised rows", got)
	}

	record := &usecases.RecordTaskPerformance{
		Performances: postgres.NewPerformanceRepo(pool), Standards: standards, Processed: postgres.NewProcessedEventRepo(pool),
		Events: pub, Clock: memory.FixedClock{At: now}, UnitOfWork: uow,
	}
	w := int64(20000)
	p, err := record.Execute(ctx, usecases.RecordTaskPerformanceRequest{
		KafkaEventId: "evt-heavy", TaskId: "task-heavy", AssociateId: "a1", TaskType: shared.Pick,
		HandlingUnit: shared.Case, WeightG: &w, ActualSeconds: 75, CompletedAt: now,
	})
	if err != nil || p == nil {
		t.Fatalf("record: p=%v err=%v", p, err)
	}
	if p.StandardSecondsAtCompletion() != 75 || !p.TeamLift() || p.EfficiencyPct() == nil || *p.EfficiencyPct() != 100 {
		t.Fatalf("heavy case must be scored against the team-lift standard: %+v", p)
	}
	var unit string
	var lift bool
	if err := pool.QueryRow(ctx, "SELECT handling_unit, team_lift FROM task_performances WHERE event_id = 'evt-heavy'").Scan(&unit, &lift); err != nil || unit != "CASE" || !lift {
		t.Fatalf("persisted unit=%q lift=%v err=%v", unit, lift, err)
	}
	for _, topic := range []string{"warehouse.labor-performance.analytics", "warehouse.labor-performance.events"} {
		if got := countOutbox(t, pool, fmt.Sprintf("topic = '%s' AND event_type LIKE '%%TaskPerformanceRecorded' AND convert_from(value,'UTF8')::jsonb->'data'->>'handling_unit' = 'CASE' AND (convert_from(value,'UTF8')::jsonb->'data'->>'team_lift')::boolean", topic)); got != 1 {
			t.Fatalf("topic %s: expected the TaskPerformanceRecorded to carry handling_unit/team_lift, got %d", topic, got)
		}
	}

	// A completion without a unit keeps the old payload: no handling keys.
	if _, err := record.Execute(ctx, usecases.RecordTaskPerformanceRequest{
		KafkaEventId: "evt-plain", TaskId: "task-plain", AssociateId: "a2", TaskType: shared.Pick, ActualSeconds: 45, CompletedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	if got := countOutbox(t, pool, "event_type LIKE '%TaskPerformanceRecorded' AND convert_from(value,'UTF8')::jsonb->'data'->>'task_id' = 'task-plain' AND NOT (convert_from(value,'UTF8')::jsonb->'data' ? 'handling_unit') AND NOT (convert_from(value,'UTF8')::jsonb->'data' ? 'team_lift')"); got != 2 {
		t.Fatalf("a unit-less completion must keep the pre-ADR payload on both topics, got %d", got)
	}
}

// TestMigration0007_UpgradesAndRollsBackWithData proves the migration is
// safe on a database that already holds standards and performances: legacy
// rows survive with the defaults (” / false), the widened index lets a CASE
// standard coexist, and the down migration removes the handling-unit rows
// and restores the one-open-per-task_type index.
func TestMigration0007_UpgradesAndRollsBackWithData(t *testing.T) {
	ctx := context.Background()
	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("labor"), tcpostgres.WithUsername("labor"), tcpostgres.WithPassword("labor"),
		tcpostgres.BasicWaitStrategies())
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(container) })
	url, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}

	m, err := migrate.New("file://"+migrationsDir(t), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = m.Close() })
	if err := m.Migrate(6); err != nil {
		t.Fatalf("migrate to 6: %v", err)
	}

	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	mustExec := func(sql string) {
		t.Helper()
		if _, err := pool.Exec(ctx, sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	mustExec(`INSERT INTO labor_standards (id, task_type, expected_seconds, effective_from) VALUES ('legacy','PICK',45,now())`)
	mustExec(`INSERT INTO task_performances (event_id, task_id, associate_id, task_type, actual_seconds, standard_seconds_at_completion, efficiency_pct, completed_at) VALUES ('e0','t0','a','PICK',60,45,75,now())`)

	if err := m.Migrate(7); err != nil {
		t.Fatalf("migrate to 7: %v", err)
	}
	var unit string
	var lift *int64
	if err := pool.QueryRow(ctx, `SELECT handling_unit, team_lift_threshold_g FROM labor_standards WHERE id='legacy'`).Scan(&unit, &lift); err != nil || unit != "" || lift != nil {
		t.Fatalf("legacy standard after up: unit=%q lift=%v err=%v", unit, lift, err)
	}
	var pu string
	var pl bool
	if err := pool.QueryRow(ctx, `SELECT handling_unit, team_lift FROM task_performances WHERE event_id='e0'`).Scan(&pu, &pl); err != nil || pu != "" || pl {
		t.Fatalf("legacy performance after up: unit=%q lift=%v err=%v", pu, pl, err)
	}
	mustExec(`INSERT INTO labor_standards (id, task_type, expected_seconds, effective_from, handling_unit) VALUES ('case','PICK',30,now(),'CASE')`)
	if _, err := pool.Exec(ctx, `INSERT INTO labor_standards (id, task_type, expected_seconds, effective_from) VALUES ('dup','PICK',46,now())`); err == nil || !strings.Contains(err.Error(), "idx_labor_standards_one_open_per_task_type") {
		t.Fatalf("a second open task-type-only standard must still violate the index, got %v", err)
	}

	if err := m.Migrate(6); err != nil {
		t.Fatalf("migrate back to 6: %v", err)
	}
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM labor_standards`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("down must drop the handling-unit standard and keep the legacy one, got %d rows (err %v)", n, err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO labor_standards (id, task_type, expected_seconds, effective_from) VALUES ('dup2','PICK',46,now())`); err == nil {
		t.Fatal("the original one-open-per-task_type index must be restored by the down migration")
	}
	if err := m.Migrate(7); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		t.Fatalf("re-apply 0007 after a rollback: %v", err)
	}
}

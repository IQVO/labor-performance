//go:build integration

// ADR 0038 decision 2 against a real PostgreSQL (testcontainers): migration
// 0008 adds labor_standards.site_code (default: the empty code) and widens the
// one-open-standard index to (task_type, handling_unit, site_code); the
// repository round-trips the site and keeps every pre-ADR-0038 lookup on
// the site-less standards (decision 3).
package postgres_test

import (
	"context"
	"errors"
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
	"github.com/claudioed/labor-performance/internal/domain/shared"
	"github.com/claudioed/labor-performance/internal/domain/standard"
)

const itSite, itOtherSite shared.SiteCode = "SITE-GRU-01", "SITE-POA-02"

func siteStandard(t *testing.T, id shared.StandardId, unit shared.HandlingUnit, site shared.SiteCode, seconds int64, from time.Time) *standard.LaborStandard {
	t.Helper()
	s, err := standard.NewForScope(id, standard.Scope{TaskType: shared.Pick, HandlingUnit: unit, SiteCode: site}, seconds, nil, nil, from)
	if err != nil {
		t.Fatalf("NewForScope: %v", err)
	}
	return s
}

func TestPostgres_SiteStandardRoundTripAndIsolation(t *testing.T) {
	pool := testDB(t)
	repo := postgres.NewStandardRepo(pool)
	ctx := context.Background()
	from := time.Now().UTC().Truncate(time.Microsecond).Add(-time.Hour)

	for _, s := range []*standard.LaborStandard{
		siteStandard(t, "std-any", "", "", 45, from),
		siteStandard(t, "std-case", shared.Case, "", 30, from),
		siteStandard(t, "std-site", "", itSite, 20, from),
		siteStandard(t, "std-site-case", shared.Case, itSite, 10, from),
		siteStandard(t, "std-other-site", "", itOtherSite, 25, from),
	} {
		if err := repo.Save(ctx, s); err != nil {
			t.Fatalf("save %s: %v", s.ID(), err)
		}
	}

	// Each exact scope resolves to exactly its own row, site included.
	for scope, want := range map[standard.Scope]shared.StandardId{
		{TaskType: shared.Pick}:                                              "std-any",
		{TaskType: shared.Pick, HandlingUnit: shared.Case}:                   "std-case",
		{TaskType: shared.Pick, SiteCode: itSite}:                            "std-site",
		{TaskType: shared.Pick, HandlingUnit: shared.Case, SiteCode: itSite}: "std-site-case",
		{TaskType: shared.Pick, SiteCode: itOtherSite}:                       "std-other-site",
	} {
		got, err := repo.FindCurrentlyActiveForScope(ctx, scope)
		if err != nil || got == nil || got.ID() != want || got.Scope() != scope {
			t.Fatalf("FindCurrentlyActiveForScope(%+v) = %v, %v; want %s", scope, got, err, want)
		}
	}
	// No fallback: a site/unit pair nobody declared is empty.
	if got, _ := repo.FindCurrentlyActiveForScope(ctx, standard.Scope{TaskType: shared.Pick, HandlingUnit: shared.Each, SiteCode: itSite}); got != nil {
		t.Fatalf("PICK/EACH/%s must be empty, got %v", itSite, got.ID())
	}

	// Every pre-ADR-0038 lookup (the scoring path) sees ONLY site-less rows.
	at := from.Add(time.Minute)
	for unit, want := range map[shared.HandlingUnit]shared.StandardId{"": "std-any", shared.Case: "std-case"} {
		cur, err := repo.FindCurrentlyActiveForUnit(ctx, shared.Pick, unit)
		if err != nil || cur == nil || cur.ID() != want || cur.SiteCode() != "" {
			t.Fatalf("FindCurrentlyActiveForUnit(%q) = %v, %v; want %s", unit, cur, err, want)
		}
		asOf, err := repo.FindActiveAsOfForUnit(ctx, shared.Pick, unit, at)
		if err != nil || asOf == nil || asOf.ID() != want {
			t.Fatalf("FindActiveAsOfForUnit(%q) = %v, %v; want %s", unit, asOf, err, want)
		}
	}
	if legacy, err := repo.FindActiveAsOf(ctx, shared.Pick, at); err != nil || legacy == nil || legacy.ID() != "std-any" {
		t.Fatalf("FindActiveAsOf = %v, %v; want std-any", legacy, err)
	}
}

// TestPostgres_SiteOnlyStandardIsNeverScored: a task type whose ONLY
// standard is a site's resolves to nothing on the scoring lookups.
func TestPostgres_SiteOnlyStandardIsNeverScored(t *testing.T) {
	pool := testDB(t)
	repo := postgres.NewStandardRepo(pool)
	ctx := context.Background()
	from := time.Now().UTC().Truncate(time.Microsecond).Add(-time.Hour)
	if err := repo.Save(ctx, siteStandard(t, "only-site", "", itSite, 20, from)); err != nil {
		t.Fatal(err)
	}
	if got, err := repo.FindActiveAsOf(ctx, shared.Pick, from.Add(time.Minute)); err != nil || got != nil {
		t.Fatalf("FindActiveAsOf = %v, %v; want nil (scoring never resolves a site standard)", got, err)
	}
	if got, err := repo.FindCurrentlyActive(ctx, shared.Pick); err != nil || got != nil {
		t.Fatalf("FindCurrentlyActive = %v, %v; want nil", got, err)
	}
}

func TestPostgres_OneOpenStandardPerTriple(t *testing.T) {
	pool := testDB(t)
	repo := postgres.NewStandardRepo(pool)
	ctx := context.Background()
	from := time.Now().UTC().Truncate(time.Microsecond).Add(-time.Hour)

	for _, s := range []*standard.LaborStandard{
		siteStandard(t, "a", "", "", 45, from),
		siteStandard(t, "b", "", itSite, 40, from),
		siteStandard(t, "c", "", itOtherSite, 41, from),
		siteStandard(t, "d", shared.Case, itSite, 30, from),
	} {
		if err := repo.Save(ctx, s); err != nil {
			t.Fatalf("%s must coexist with the other scopes: %v", s.ID(), err)
		}
	}
	// A second open standard of the same triple conflicts.
	if err := repo.Save(ctx, siteStandard(t, "e", "", itSite, 39, from.Add(time.Minute))); !errors.Is(err, ports.ErrOpenStandardConflict) {
		t.Fatalf("second open %s standard: err = %v, want ErrOpenStandardConflict", itSite, err)
	}
	// Closing the site's standard frees only that triple.
	b, _ := repo.FindCurrentlyActiveForScope(ctx, standard.Scope{TaskType: shared.Pick, SiteCode: itSite})
	b.Close(from.Add(time.Hour))
	if err := repo.Save(ctx, b); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := repo.Save(ctx, siteStandard(t, "f", "", itSite, 38, from.Add(time.Hour))); err != nil {
		t.Fatalf("new site standard after close: %v", err)
	}
	for scope, want := range map[standard.Scope]shared.StandardId{
		{TaskType: shared.Pick}:                                              "a",
		{TaskType: shared.Pick, SiteCode: itOtherSite}:                       "c",
		{TaskType: shared.Pick, HandlingUnit: shared.Case, SiteCode: itSite}: "d",
	} {
		if got, _ := repo.FindCurrentlyActiveForScope(ctx, scope); got == nil || got.ID() != want || got.EffectiveTo() != nil {
			t.Fatalf("%+v was disturbed: %v", scope, got)
		}
	}
}

// TestDefineStandard_SiteRevisionThroughPostgres drives the use case on the
// real adapter and unit of work: the site revision closes only its triple.
func TestDefineStandard_SiteRevisionThroughPostgres(t *testing.T) {
	pool := testDB(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	repo := postgres.NewStandardRepo(pool)
	define := &usecases.DefineStandard{Standards: repo, Events: &capturePublisher{}, Clock: memory.FixedClock{At: now}, UnitOfWork: postgres.NewUnitOfWork(pool)}
	siteLess, err := define.ExecuteSpec(ctx, usecases.DefineStandardSpec{TaskType: shared.Pick, ExpectedSeconds: 45})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := define.ExecuteSpec(ctx, usecases.DefineStandardSpec{TaskType: shared.Pick, SiteCode: itSite, ExpectedSeconds: 40}); err != nil {
		t.Fatal(err)
	}
	define.Clock = memory.FixedClock{At: now.Add(time.Minute)}
	revised, err := define.ExecuteSpec(ctx, usecases.DefineStandardSpec{TaskType: shared.Pick, SiteCode: itSite, ExpectedSeconds: 35})
	if err != nil {
		t.Fatalf("site revision: %v", err)
	}
	if got, _ := repo.FindCurrentlyActiveForScope(ctx, standard.Scope{TaskType: shared.Pick, SiteCode: itSite}); got == nil || got.ID() != revised.ID() || got.ExpectedSeconds() != 35 {
		t.Fatalf("current site standard = %v", got)
	}
	if got, _ := repo.FindCurrentlyActive(ctx, shared.Pick); got == nil || got.ID() != siteLess.ID() || got.EffectiveTo() != nil {
		t.Fatalf("site-less standard disturbed: %v", got)
	}
	var closed int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM labor_standards WHERE site_code = $1 AND effective_to IS NOT NULL`, string(itSite)).Scan(&closed); err != nil || closed != 1 {
		t.Fatalf("closed site rows = %d (err %v), want 1", closed, err)
	}
}

// capturePublisher records published events (no outbox needed here).
type capturePublisher struct{ got []shared.DomainEvent }

func (p *capturePublisher) Publish(_ context.Context, evts ...shared.DomainEvent) error {
	p.got = append(p.got, evts...)
	return nil
}

// TestMigration0008_UpgradesAndRollsBackWithData: 0008 is safe on a
// database that already holds standards (they become site-less: the empty code),
// rejects an invalid site code at the column, lets a site standard coexist
// with the site-less one, and the down migration removes the site rows and
// restores the (task_type, handling_unit) index.
func TestMigration0008_UpgradesAndRollsBackWithData(t *testing.T) {
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
	if err := m.Migrate(7); err != nil {
		t.Fatalf("migrate to 7: %v", err)
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
	mustExec(`INSERT INTO labor_standards (id, task_type, expected_seconds, effective_from, handling_unit) VALUES ('legacy-case','PICK',30,now(),'CASE')`)

	if err := m.Migrate(8); err != nil {
		t.Fatalf("migrate to 8: %v", err)
	}
	var site string
	if err := pool.QueryRow(ctx, `SELECT site_code FROM labor_standards WHERE id='legacy'`).Scan(&site); err != nil || site != "" {
		t.Fatalf("legacy standard after up: site=%q err=%v", site, err)
	}
	repo := postgres.NewStandardRepo(pool)
	if got, err := repo.FindCurrentlyActive(ctx, shared.Pick); err != nil || got == nil || got.ID() != "legacy" || got.SiteCode() != "" {
		t.Fatalf("legacy standard through the repo: %v %v", got, err)
	}
	mustExec(`INSERT INTO labor_standards (id, task_type, expected_seconds, effective_from, site_code) VALUES ('site','PICK',40,now(),'SITE-GRU-01')`)
	if _, err := pool.Exec(ctx, `INSERT INTO labor_standards (id, task_type, expected_seconds, effective_from) VALUES ('dup','PICK',46,now())`); err == nil || !strings.Contains(err.Error(), "idx_labor_standards_one_open_per_task_type") {
		t.Fatalf("a second open site-less standard must still violate the index, got %v", err)
	}
	for name, code := range map[string]string{"whitespace": "SITE GRU", "33 characters": strings.Repeat("S", 33), "tab": "SITE\tGRU"} {
		if _, err := pool.Exec(ctx, `INSERT INTO labor_standards (id, task_type, expected_seconds, effective_from, site_code) VALUES ('bad','PACK',10,now(),$1)`, code); err == nil || !strings.Contains(err.Error(), "labor_standards_site_code_shape") {
			t.Fatalf("%s site code must violate the CHECK, got %v", name, err)
		}
	}
	mustExec(`INSERT INTO labor_standards (id, task_type, expected_seconds, effective_from, site_code) VALUES ('site-32','PACK',10,now(),'` + strings.Repeat("S", 32) + `')`)

	if err := m.Migrate(7); err != nil {
		t.Fatalf("migrate back to 7: %v", err)
	}
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM labor_standards`).Scan(&n); err != nil || n != 2 {
		t.Fatalf("down must drop the site standards and keep the 2 legacy ones, got %d rows (err %v)", n, err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO labor_standards (id, task_type, expected_seconds, effective_from, handling_unit) VALUES ('dup2','PICK',31,now(),'CASE')`); err == nil {
		t.Fatal("the (task_type, handling_unit) one-open index must be restored by the down migration")
	}
	if err := m.Migrate(8); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		t.Fatalf("re-apply 0008 after a rollback: %v", err)
	}
}

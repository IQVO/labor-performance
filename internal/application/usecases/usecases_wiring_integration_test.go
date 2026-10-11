//go:build integration

// Package usecases_test proves the two write use cases — DefineStandard and
// RecordTaskPerformance — against a REAL Postgres (testcontainers): the real
// postgres repos, the real UnitOfWork, and a buffering publisher, wired
// exactly like the composition roots in cmd/labor and cmd/mcp. These are
// integration tests in the fleet's sense: they execute the real
// cross-component contracts (the one-open-standard revision bracket that
// keeps historical scoring frozen, Kafka-event idempotency, idle-gap
// derivation on the same unit of work, the atomic Publish-inside-UoW
// bracket, and the read models those writes project) against real
// infrastructure, with no in-memory repo fakes anywhere in the path.
//
// The package boots its own throwaway Postgres via testcontainers in
// TestMain (this package's first integration suite): one container for the
// whole package, migrated once into a template database, one private
// database per test (CREATE DATABASE ... WITH TEMPLATE, a file-level copy:
// milliseconds). Never an external DATABASE_URL, never t.Skip.
package usecases_test

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/claudioed/labor-performance/internal/adapters/outbound/memory"
	"github.com/claudioed/labor-performance/internal/adapters/outbound/postgres"
	"github.com/claudioed/labor-performance/internal/application/usecases"
	"github.com/claudioed/labor-performance/internal/domain/performance"
	"github.com/claudioed/labor-performance/internal/domain/shared"
)

// One Postgres container serves the whole package (containers are slow to
// boot). TestMain starts it, applies every migration ONCE into a template
// database, and each test then gets its own database cloned from that
// template (CREATE DATABASE ... TEMPLATE, a file-level copy: milliseconds).
// Isolation is therefore total — no TRUNCATE bookkeeping, no dependence on
// test order. Never an external DATABASE_URL, never t.Skip.
const templateDB = "usecases_migrated_template"

var (
	sharedBaseURL string // connection URL of the container's default database
	dbSeq         atomic.Uint64
)

// TestMain owns the package-wide container lifecycle.
func TestMain(m *testing.M) {
	os.Exit(runTests(m))
}

func runTests(m *testing.M) int {
	ctx := context.Background()
	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("labor_usecases_it"),
		tcpostgres.WithUsername("labor"),
		tcpostgres.WithPassword("labor"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(90*time.Second),
		),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "start postgres container: %v\n", err)
		return 1
	}
	defer func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			fmt.Fprintf(os.Stderr, "terminate postgres container: %v\n", err)
		}
	}()

	var err2 error
	sharedBaseURL, err2 = container.ConnectionString(ctx, "sslmode=disable")
	if err2 != nil {
		fmt.Fprintf(os.Stderr, "postgres connection string: %v\n", err2)
		return 1
	}

	// Migrate a template database once; every test clones it.
	if err := createDatabase(ctx, templateDB); err != nil {
		fmt.Fprintf(os.Stderr, "create template database: %v\n", err)
		return 1
	}
	if err := postgres.RunMigrations(withDB(sharedBaseURL, templateDB), migrationsDir()); err != nil {
		fmt.Fprintf(os.Stderr, "migrate template: %v\n", err)
		return 1
	}
	return m.Run()
}

// migrationsDir resolves the repo's migrations directory relative to this
// test file, so the suite does not depend on the working directory go test
// was invoked from.
func migrationsDir() string {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		panic("unable to resolve test file path")
	}
	return filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "migrations")
}

// withDB rewrites the path of a connection URL to the named database.
func withDB(baseURL, name string) string {
	u, err := url.Parse(baseURL)
	if err != nil {
		panic(err)
	}
	u.Path = "/" + name
	return u.String()
}

// createDatabase creates an empty database inside the shared container.
func createDatabase(ctx context.Context, name string) error {
	conn, err := pgx.Connect(ctx, sharedBaseURL)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, fmt.Sprintf("CREATE DATABASE %q", name)); err != nil {
		return fmt.Errorf("create database %s: %w", name, err)
	}
	return nil
}

// migratedDB hands the test a connection URL to its own private database,
// cloned from the migrated template. Cloning is a file-level copy, so it
// costs milliseconds and the test's writes never leak into another test.
func migratedDB(t *testing.T) string {
	t.Helper()
	name := fmt.Sprintf("usecases_it_%d", dbSeq.Add(1))
	conn, err := pgx.Connect(context.Background(), sharedBaseURL)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(context.Background())
	if _, err := conn.Exec(context.Background(), fmt.Sprintf(
		"CREATE DATABASE %q WITH TEMPLATE %q", name, templateDB)); err != nil {
		t.Fatalf("clone database: %v", err)
	}
	return withDB(sharedBaseURL, name)
}

// wiringEpoch is the fixed instant the wired clocks freeze at: revisions
// happen at epoch-1h, the first standard at epoch-2h, recorded completions
// inside the last hour.
var wiringEpoch = time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)

// bufferingPublisher accumulates published domain events for assertions,
// standing in for the composition root's publisher.
type bufferingPublisher struct {
	mu     sync.Mutex
	events []shared.DomainEvent
}

// Publish records every event, in order.
func (p *bufferingPublisher) Publish(_ context.Context, evts ...shared.DomainEvent) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = append(p.events, evts...)
	return nil
}

// Published returns a copy of everything published so far, in order.
func (p *bufferingPublisher) Published() []shared.DomainEvent {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]shared.DomainEvent(nil), p.events...)
}

// countOf reports how many published events carry the given name.
func (p *bufferingPublisher) countOf(name string) int {
	n := 0
	for _, e := range p.Published() {
		if e.EventName() == name {
			n++
		}
	}
	return n
}

// idleSecondsBefore returns the IdleSecondsBefore of the Nth (0-based)
// TaskPerformanceRecorded event, failing the test when there are fewer.
func (p *bufferingPublisher) idleSecondsBefore(t *testing.T, nth int) *int64 {
	t.Helper()
	seen := 0
	for _, e := range p.Published() {
		evt, ok := e.(shared.TaskPerformanceRecorded)
		if !ok {
			continue
		}
		if seen == nth {
			return evt.IdleSecondsBefore
		}
		seen++
	}
	t.Fatalf("wanted TaskPerformanceRecorded event #%d, saw only %d", nth+1, seen)
	return nil
}

// wiredStack is the real adapter stack over a private migrated database:
// the postgres repos, the real UnitOfWork, and a buffering publisher the
// tests assert on — wired exactly like cmd/labor's composition root for the
// two write use cases this context owns. No in-memory repo fakes.
type wiredStack struct {
	standards    *postgres.StandardRepo
	performances *postgres.PerformanceRepo
	processed    *postgres.ProcessedEventRepo
	idlePeriods  *postgres.IdlePeriodRepo
	publisher    *bufferingPublisher
	uow          *postgres.UnitOfWork
	define       *usecases.DefineStandard
	record       *usecases.RecordTaskPerformance
	scorecard    *usecases.GetAssociateScorecard
	taskTypePerf *usecases.GetTaskTypePerformance
	utilization  *usecases.GetUtilization
}

// newWiredUsecases builds the stack over a fresh private database. The
// define clock freezes at epoch-2h (the first standard's effective instant)
// and advances per call site through the use case's own Clock dependency;
// the record/read clocks freeze at the epoch itself.
func newWiredUsecases(t *testing.T) *wiredStack {
	t.Helper()
	ctx := context.Background()
	pool, err := postgres.NewPool(ctx, migratedDB(t))
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)

	w := &wiredStack{
		standards:    postgres.NewStandardRepo(pool),
		performances: postgres.NewPerformanceRepo(pool),
		processed:    postgres.NewProcessedEventRepo(pool),
		idlePeriods:  postgres.NewIdlePeriodRepo(pool),
		publisher:    &bufferingPublisher{},
	}
	w.uow = postgres.NewUnitOfWork(pool)
	w.define = &usecases.DefineStandard{
		Standards: w.standards, Events: w.publisher,
		Clock: memory.FixedClock{At: wiringEpoch.Add(-2 * time.Hour)}, UnitOfWork: w.uow,
	}
	w.record = &usecases.RecordTaskPerformance{
		Performances: w.performances, Standards: w.standards, Processed: w.processed,
		Events: w.publisher, Clock: memory.FixedClock{At: wiringEpoch},
		UnitOfWork: w.uow, IdlePeriods: w.idlePeriods,
	}
	w.scorecard = &usecases.GetAssociateScorecard{Performances: w.performances}
	w.taskTypePerf = &usecases.GetTaskTypePerformance{Performances: w.performances}
	w.utilization = &usecases.GetUtilization{
		Performances: w.performances, IdlePeriods: w.idlePeriods,
		Clock: memory.FixedClock{At: wiringEpoch},
	}
	return w
}

// defineAt executes DefineStandard with the clock advanced to at, so a
// revision's effective instant is deterministic.
func (w *wiredStack) defineAt(t *testing.T, at time.Time, expectedSeconds int64) {
	t.Helper()
	w.define.Clock = memory.FixedClock{At: at}
	if _, err := w.define.Execute(context.Background(), shared.Pick, expectedSeconds, nil); err != nil {
		t.Fatalf("define standard %ds at %s: %v", expectedSeconds, at.Format(time.RFC3339), err)
	}
}

// recordOne records one completion through the real RecordTaskPerformance.
func (w *wiredStack) recordOne(t *testing.T, eventId, taskId string, actualSeconds int64, completedAt time.Time) {
	t.Helper()
	if _, err := w.record.Execute(context.Background(), usecases.RecordTaskPerformanceRequest{
		KafkaEventId: eventId, TaskId: taskId, AssociateId: "assoc-itcov",
		TaskType: shared.Pick, ActualSeconds: actualSeconds, CompletedAt: completedAt,
	}); err != nil {
		t.Fatalf("record %s: %v", taskId, err)
	}
}

// TestUsecases_StandardLifecycleKeepsHistoricalScoringFrozen drives the
// DefineStandard lifecycle — first definition, then revision — against the
// real repos and UnitOfWork, asserting both the persisted effective ranges
// and the published events, then proves the fleet's frozen-scoring contract
// (ADR 0004): a completion that PREDATES the revision is scored against the
// standard that was active AS OF its completion instant, not the
// currently-active one.
func TestUsecases_StandardLifecycleKeepsHistoricalScoringFrozen(t *testing.T) {
	w := newWiredUsecases(t)
	ctx := context.Background()

	// First definition: no prior, one open standard, Defined published.
	w.defineAt(t, wiringEpoch.Add(-2*time.Hour), 60)
	active, err := w.standards.FindCurrentlyActive(ctx, shared.Pick)
	if err != nil || active == nil {
		t.Fatalf("currently active after first define: %v, %v", active, err)
	}
	if active.ExpectedSeconds() != 60 {
		t.Fatalf("first standard expected 60s, got %d", active.ExpectedSeconds())
	}
	if active.EffectiveTo() != nil {
		t.Fatalf("first standard must be open (nil effective_to), got %v", active.EffectiveTo())
	}
	if n := w.publisher.countOf("LaborStandardDefined"); n != 1 {
		t.Fatalf("expected 1 LaborStandardDefined, got %d", n)
	}

	// Revision at epoch-1h: the prior closes AT the revision instant and
	// the new one opens there — no gap, no overlap.
	w.defineAt(t, wiringEpoch.Add(-1*time.Hour), 90)
	active, err = w.standards.FindCurrentlyActive(ctx, shared.Pick)
	if err != nil || active == nil || active.ExpectedSeconds() != 90 {
		t.Fatalf("currently active after revision: %+v (%v), want 90s", active, err)
	}
	asOfBefore, err := w.standards.FindActiveAsOf(ctx, shared.Pick, wiringEpoch.Add(-90*time.Minute))
	if err != nil || asOfBefore == nil || asOfBefore.ExpectedSeconds() != 60 {
		t.Fatalf("as-of epoch-90m: %+v (%v), want the closed 60s standard", asOfBefore, err)
	}
	if asOfBefore.EffectiveTo() == nil || !asOfBefore.EffectiveTo().Equal(wiringEpoch.Add(-1*time.Hour)) {
		t.Fatalf("closed standard must end at the revision instant, got %v", asOfBefore.EffectiveTo())
	}
	asOfAfter, err := w.standards.FindActiveAsOf(ctx, shared.Pick, wiringEpoch.Add(-30*time.Minute))
	if err != nil || asOfAfter == nil || asOfAfter.ExpectedSeconds() != 90 {
		t.Fatalf("as-of epoch-30m: %+v (%v), want the revised 90s standard", asOfAfter, err)
	}
	if n := w.publisher.countOf("LaborStandardRevised"); n != 1 {
		t.Fatalf("expected 1 LaborStandardRevised, got %d", n)
	}
	if n := w.publisher.countOf("LaborStandardDefined"); n != 1 {
		t.Fatalf("revision must not republish Defined, got %d", n)
	}

	// Frozen scoring: a completion at epoch-90m (inside the 60s range)
	// with actual 60s scores 100% against the OLD standard. If the use
	// case resolved "active right now" (90s) instead, this would read
	// 150% — the assertion pins the as-of contract.
	w.recordOne(t, "itcov-std-evt-1", "ITCOV-STD-1", 60, wiringEpoch.Add(-90*time.Minute))
	recent, err := w.performances.RecentByAssociateID(ctx, "assoc-itcov", 1)
	if err != nil || len(recent) != 1 {
		t.Fatalf("recent by associate: %v (%v)", recent, err)
	}
	if pct := recent[0].EfficiencyPct(); pct == nil || *pct != 100 {
		t.Fatalf("pre-revision completion must score 100%% against the 60s standard, got %v", pct)
	}
	if s := recent[0].StandardSecondsAtCompletion(); s != 60 {
		t.Fatalf("frozen StandardSecondsAtCompletion = %d, want 60", s)
	}
}

// TestUsecases_RecordTaskPerformanceIsIdempotentAndDerivesIdle drives the
// Kafka-consumer write path end-to-end: two completions two minutes apart
// (the first with no prior, the second preceded by a real measured gap)
// persist one TaskPerformance and one derived IdlePeriod each on the same
// unit of work, publish TaskPerformanceRecorded carrying the gap, and a
// redelivered event id is a benign no-op — no second row, no second event.
// The read models (scorecard, task-type performance, utilization) then
// answer from exactly those rows.
func TestUsecases_RecordTaskPerformanceIsIdempotentAndDerivesIdle(t *testing.T) {
	w := newWiredUsecases(t)
	ctx := context.Background()
	w.defineAt(t, wiringEpoch.Add(-2*time.Hour), 60)

	// First observation: completed epoch-30m, actual 60s (claimed
	// epoch-31m) — no prior completion, so no idle gap exists to derive.
	w.recordOne(t, "itcov-rec-evt-1", "ITCOV-REC-1", 60, wiringEpoch.Add(-30*time.Minute))
	// Second: completed epoch-26m, actual 60s (claimed epoch-27m) —
	// gap from the prior completion (epoch-30m) to the claim is 180s.
	w.recordOne(t, "itcov-rec-evt-2", "ITCOV-REC-2", 60, wiringEpoch.Add(-26*time.Minute))

	if n := w.publisher.countOf("TaskPerformanceRecorded"); n != 2 {
		t.Fatalf("expected 2 TaskPerformanceRecorded, got %d", n)
	}
	// The event payloads carry the derivation: nil for the first
	// observation, 180 for the measured gap before the second.
	if got := w.publisher.idleSecondsBefore(t, 0); got != nil {
		t.Fatalf("first observation must publish nil IdleSecondsBefore, got %v", *got)
	}
	if got := w.publisher.idleSecondsBefore(t, 1); got == nil || *got != 180 {
		t.Fatalf("second completion must publish IdleSecondsBefore=180, got %v", got)
	}

	// The idle gap really persisted through the real repo.
	idleSeconds, count, err := w.idlePeriods.SumByTaskType(ctx, shared.Pick, wiringEpoch.Add(-time.Hour))
	if err != nil || count != 1 || idleSeconds != 180 {
		t.Fatalf("idle periods = %ds/%d rows (%v), want 180s/1", idleSeconds, count, err)
	}

	// Redelivery of the SAME Kafka event id: a benign no-op (nil, nil),
	// not an error and not a double-count.
	dup, err := w.record.Execute(ctx, usecases.RecordTaskPerformanceRequest{
		KafkaEventId: "itcov-rec-evt-2", TaskId: "ITCOV-REC-2", AssociateId: "assoc-itcov",
		TaskType: shared.Pick, ActualSeconds: 60, CompletedAt: wiringEpoch.Add(-26 * time.Minute),
	})
	if err != nil || dup != nil {
		t.Fatalf("redelivered event must be a (nil, nil) no-op, got (%v, %v)", dup, err)
	}
	if n := w.publisher.countOf("TaskPerformanceRecorded"); n != 2 {
		t.Fatalf("redelivery must not republish, saw %d TaskPerformanceRecorded", n)
	}

	// The read models answer from exactly those two rows: scorecard with
	// the recent-window signal, fleet-wide performance, and windowed
	// utilization composing BOTH repos the write path just wrote through.
	sc, err := w.scorecard.Execute(ctx, "assoc-itcov")
	if err != nil {
		t.Fatalf("scorecard: %v", err)
	}
	if sc.TaskCount != 2 || sc.MeanEfficiencyPct == nil || *sc.MeanEfficiencyPct != 100 {
		t.Fatalf("scorecard = %+v, want 2 tasks at 100%%", sc)
	}
	if sc.Trend != performance.TrendInsufficientData || sc.CoachingFlag {
		t.Fatalf("2 scored tasks is below the trend minimum: trend=%s coaching=%v", sc.Trend, sc.CoachingFlag)
	}

	ttp, err := w.taskTypePerf.Execute(ctx, shared.Pick)
	if err != nil {
		t.Fatalf("task-type performance: %v", err)
	}
	if ttp.TaskCount != 2 || ttp.MeanActualSeconds == nil || *ttp.MeanActualSeconds != 60 {
		t.Fatalf("task-type performance = %+v, want 2 tasks / 60s mean actual", ttp)
	}

	util, err := w.utilization.ForTaskType(ctx, shared.Pick, time.Hour)
	if err != nil {
		t.Fatalf("utilization: %v", err)
	}
	if util.TaskSeconds != 120 || util.IdleSeconds != 180 || util.Associates != 1 {
		t.Fatalf("utilization inputs = task %d / idle %d / associates %d, want 120/180/1",
			util.TaskSeconds, util.IdleSeconds, util.Associates)
	}
	if util.UtilizationPct == nil || *util.UtilizationPct != 40 {
		t.Fatalf("utilizationPct = %v, want 40 (120 of 120+180)", util.UtilizationPct)
	}
}

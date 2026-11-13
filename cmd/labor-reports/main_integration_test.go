//go:build integration

package main

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/claudioed/labor-performance/internal/adapters/outbound/analyticsstore"
	"github.com/claudioed/labor-performance/internal/adapters/outbound/postgres"
	"github.com/claudioed/labor-performance/internal/analytics/report"
	"github.com/claudioed/labor-performance/internal/testing/pgtest"
)

func TestMain(m *testing.M) {
	code := m.Run()
	pgtest.Shutdown()
	os.Exit(code)
}

// The real entry point the way the fleet's reports template proves it: the
// analytical schema applied by the TEST (the reader never migrates — the
// projector owns the schema, ADR-0007), facts seeded with the repo's own
// analytics writer, then the real run() serving the documented probes and
// reports off exactly those facts, and a SIGTERM draining cleanly.
//
// The seeded facts exercise the read model's documented discipline: one
// scored completion (120s actual, 50% against its frozen standard) plus one
// measured-but-unscored completion (90s, no EfficiencyPct), in the same
// hour bucket, with the standard effective in an earlier bucket — so the
// report must answer with a nullable mean that is 50 over the scored subset
// (never 25 over the recorded subset, ADR-0004) and a mean actual seconds
// of 105 over the measured subset (the ADR-0006 measured-without-scored
// distinction).
func TestReportsServeTheProjectionAndStopCleanly(t *testing.T) {
	url := pgtest.EmptyURL(t)
	if err := postgres.RunMigrations(url, analyticsMigrationsDir(t)); err != nil {
		t.Fatalf("run analytics migrations: %v", err)
	}

	ctx := context.Background()
	pool, err := analyticsstore.NewPool(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	projection := analyticsstore.NewPostgresProjection(pool)

	// Business times (the bucket dimension) are fixed and whole-minute, in
	// two different buckets; the occurred-at freshness watermark is near
	// now so the documented lag answer is bounded by the test's own clock.
	effectiveFrom := time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC)
	completedScored := time.Date(2026, 11, 13, 11, 5, 0, 0, time.UTC)
	completedUnscored := time.Date(2026, 11, 13, 11, 47, 0, 0, time.UTC)
	seededAt := time.Now().Truncate(time.Microsecond)
	scored := 50.0
	if err := projection.ApplyLaborStandardDefined(ctx, "reports-itest-std-1", report.StandardFact{
		TaskType: "PICK", ExpectedSeconds: 60, EffectiveFrom: effectiveFrom,
		OccurredAt: seededAt.Add(-3 * time.Minute),
	}); err != nil {
		t.Fatal(err)
	}
	if err := projection.ApplyTaskPerformanceRecorded(ctx, "reports-itest-perf-1", report.TaskPerformanceFact{
		TaskType: "PICK", ActualSeconds: 120, EfficiencyPct: &scored,
		CompletedAt: completedScored, OccurredAt: seededAt.Add(-2 * time.Minute),
	}); err != nil {
		t.Fatal(err)
	}
	if err := projection.ApplyTaskPerformanceRecorded(ctx, "reports-itest-perf-2", report.TaskPerformanceFact{
		TaskType: "PICK", ActualSeconds: 90, // measured but unscored: EfficiencyPct stays nil
		CompletedAt: completedUnscored, OccurredAt: seededAt.Add(-90 * time.Second),
	}); err != nil {
		t.Fatal(err)
	}

	addr := freeAddr(t)
	t.Setenv("ANALYTICS_DATABASE_URL", url)
	t.Setenv("HTTP_ADDR", addr)
	t.Setenv("LOG_LEVEL", "error")

	done := launchRun(t)
	waitUntil(t, "the /healthz probe to answer 200", func() bool {
		code, _ := getJSON(t, "http://"+addr+"/healthz")
		return code == http.StatusOK
	})

	// The documented report: half-open [from, to) over the two seeded
	// buckets, rows keyed by the events' business time.
	code, body := getJSON(t, "http://"+addr+"/reports/performance?from=2026-11-13T10:00:00Z&to=2026-11-13T12:00:00Z")
	if code != http.StatusOK {
		t.Fatalf("/reports/performance = %d %v", code, body)
	}
	rows, _ := body["rows"].([]any)
	if len(rows) != 2 {
		t.Fatalf("rows = %v, want the 10:00 standard bucket and the 11:00 task bucket", body["rows"])
	}
	byHour := map[string]map[string]any{}
	for _, r := range rows {
		row, _ := r.(map[string]any)
		byHour[row["hourBucket"].(string)] = row
	}
	stdRow := byHour["2026-11-13T10:00:00Z"]
	if stdRow["standardsDefined"] != 1.0 || stdRow["tasksRecorded"] != 0.0 ||
		stdRow["meanEfficiencyPct"] != nil || stdRow["meanActualSeconds"] != nil {
		t.Fatalf("standard bucket = %v, want standardsDefined 1 and nil means (never 0, ADR-0004)", stdRow)
	}
	taskRow := byHour["2026-11-13T11:00:00Z"]
	if taskRow["tasksRecorded"] != 2.0 || taskRow["tasksScored"] != 1.0 || taskRow["tasksUnscored"] != 1.0 ||
		taskRow["tasksMeasured"] != 2.0 || taskRow["meanEfficiencyPct"] != 50.0 || taskRow["meanActualSeconds"] != 105.0 {
		t.Fatalf("task bucket = %v, want 2 recorded / 1 scored / 2 measured, means 50%% and 105s", taskRow)
	}
	bars, _ := body["byTaskType"].([]any)
	if len(bars) != 1 {
		t.Fatalf("byTaskType = %v, want exactly the PICK bar", body["byTaskType"])
	}
	bar, _ := bars[0].(map[string]any)
	if bar["taskType"] != "PICK" || bar["standardsDefined"] != 1.0 || bar["meanEfficiencyPct"] != 50.0 {
		t.Fatalf("PICK bar = %v, want the standard and the 50%% mean folded in", bar)
	}
	totals, _ := body["totals"].(map[string]any)
	if totals["tasksRecorded"] != 2.0 || totals["tasksScored"] != 1.0 || totals["tasksUnscored"] != 1.0 ||
		totals["meanEfficiencyPct"] != 50.0 || totals["meanActualSeconds"] != 105.0 {
		t.Fatalf("totals = %v, want 2/1/1 with means 50%% (not 25) and 105s", totals)
	}

	// The documented freshness signal: now minus the most recently applied
	// event's occurred-at — at least the 90s behind the seed, bounded
	// above by the test's own runtime.
	code, body = getJSON(t, "http://"+addr+"/reports/performance/freshness")
	if code != http.StatusOK {
		t.Fatalf("/reports/performance/freshness = %d %v", code, body)
	}
	lag, ok := body["lagSeconds"].(float64)
	if !ok || lag < 89 || lag > 300 {
		t.Fatalf("lagSeconds = %v, want the seeded ~90s watermark within the test's runtime", body["lagSeconds"])
	}

	// The documented RFC 7807 rejection (AGENTS.md: same problem shape on
	// the reports API): a missing required query parameter.
	resp, err := http.Get("http://" + addr + "/reports/performance")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest ||
		!strings.HasPrefix(resp.Header.Get("Content-Type"), "application/problem+json") {
		t.Fatalf("missing 'from' = %d %q, want a 400 problem+json", resp.StatusCode, resp.Header.Get("Content-Type"))
	}

	stopBySIGTERM(t, done)
}

// launchRun starts the real signal-driven run() and registers THIS test's
// SIGTERM handler before the first signal can ever fly, so stopping the
// binary later can never fall back to Go's default disposition.
func launchRun(t *testing.T) <-chan error {
	t.Helper()
	sig := make(chan os.Signal, 4)
	signal.Notify(sig, syscall.SIGTERM)
	done := make(chan error, 1)
	go func() { done <- run() }()
	return done
}

// stopBySIGTERM stops run() the only way its contract allows: SIGTERM,
// retried on a timer until run() returns, then requiring a clean nil
// return (the reader's documented graceful drain). The handler registered
// in launchRun is NEVER unregistered: run()'s own signal.NotifyContext
// stop has already run by the time run() returns, so removing the last
// handler would restore the default disposition while a just-sent retry
// can still be in flight and kill the test binary itself.
func stopBySIGTERM(t *testing.T, done <-chan error) {
	t.Helper()
	_ = syscall.Kill(os.Getpid(), syscall.SIGTERM)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("a SIGTERM'd reports server must drain cleanly, got %v", err)
			}
			return
		case <-ticker.C:
			_ = syscall.Kill(os.Getpid(), syscall.SIGTERM)
		}
	}
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	return l.Addr().String()
}

func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(90 * time.Second); time.Now().Before(deadline); time.Sleep(200 * time.Millisecond) {
		if cond() {
			return
		}
	}
	t.Fatalf("timed out waiting for %s", what)
}

func getJSON(t *testing.T, url string) (int, map[string]any) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		return 0, nil
	}
	defer func() { _ = resp.Body.Close() }()
	var body map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&body)
	return resp.StatusCode, body
}

// analyticsMigrationsDir resolves the repo's analytics migrations directory
// relative to this file, so the suite does not depend on the working
// directory go test was invoked from.
func analyticsMigrationsDir(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("unable to resolve test source path")
	}
	return filepath.Join(filepath.Dir(thisFile), "..", "..", "migrations", "analytics")
}

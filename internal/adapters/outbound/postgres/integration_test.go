//go:build integration

package postgres_test

import (
	"context"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/claudioed/labor-performance/internal/adapters/outbound/postgres"
)

// migrationsDir resolves /migrations relative to this test file, so the
// test works regardless of the working directory `go test` is invoked
// from.
func migrationsDir(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("unable to resolve test file path")
	}
	return filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "..", "migrations")
}

// The package shares ONE testcontainers Postgres across every test that
// needs the OLTP schema (the fleet rule: the tests own their own
// database — never an external DATABASE_URL, never a t.Skip gate, never a
// hardcoded localhost). The container boots lazily on first use (so a
// `-run` filter that selects no OLTP test pays nothing), migrates once,
// and every test's first act is to truncate the OLTP tables so tests
// cannot observe each other's rows.
var (
	sharedDBOnce sync.Once
	sharedPool   *pgxpool.Pool
	sharedErr    error
)

// oltpTables is every OLTP table, in no dependency order — TRUNCATE ...
// CASCADE makes order irrelevant.
var oltpTables = []string{
	"idempotency_keys",
	"outbox_events",
	"idle_periods",
	"processed_events",
	"task_performances",
	"labor_standards",
}

func bootSharedDB() {
	ctx := context.Background()
	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("labor"),
		tcpostgres.WithUsername("labor"),
		tcpostgres.WithPassword("labor"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		sharedErr = err
		return
	}
	// The container lives for the whole test-binary run: testcontainers'
	// ryuk reaper terminates it after the process exits, so no explicit
	// t.Cleanup (which would tear it down after the FIRST test) is
	// registered — the pool is closed by TestMain below.
	url, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		sharedErr = err
		return
	}

	_, thisFile, _, _ := runtime.Caller(0)
	migrations := filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "..", "migrations")
	if err := postgres.RunMigrations(url, migrations); err != nil {
		sharedErr = err
		return
	}

	pool, err := postgres.NewPool(ctx, url)
	if err != nil {
		sharedErr = err
		return
	}
	sharedPool = pool
}

// testDB returns the package's shared, migrated testcontainers pool with
// every OLTP table truncated, so the calling test starts from a clean
// database and cannot observe another test's rows.
func testDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	sharedDBOnce.Do(bootSharedDB)
	if sharedErr != nil {
		t.Fatalf("boot shared postgres container: %v", sharedErr)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := sharedPool.Ping(ctx); err != nil {
		t.Fatalf("ping shared postgres: %v", err)
	}
	for _, table := range oltpTables {
		if _, err := sharedPool.Exec(context.Background(), "TRUNCATE TABLE "+table+" CASCADE"); err != nil {
			t.Fatalf("truncate %s: %v", table, err)
		}
	}
	t.Cleanup(func() {
		for _, table := range oltpTables {
			_, _ = sharedPool.Exec(context.Background(), "TRUNCATE TABLE "+table+" CASCADE")
		}
	})
	return sharedPool
}

package main

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// migrationsDirForTest resolves the repo's migrations directory, so the
// retry under test fails on the DIAL rather than on a missing directory
// (which would return before any retry and make the test vacuous).
func migrationsDirForTest(t *testing.T) string {
	t.Helper()
	// cmd/labor -> repo root.
	dir := "../../migrations"
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("migrations directory not found at %s: %v", dir, err)
	}
	return dir
}

// TestBuildPersistence_RetriesTheDatabaseNotJustOnce is the test that
// would have caught the original defect this repo's boot path had: a
// helper that works in isolation (bootretry's own tests) but isn't
// actually wired into the real boot path.
//
// It drives buildPersistence — the ACTUAL composition-root function
// cmd/labor/main.go calls at startup — against an unreachable address and
// asserts on the ELAPSED time: a single attempt returns fast (a refused
// connection is near-instant), whereas the retry budget cannot be paid in
// less than the sum of its backoffs.
func TestBuildPersistence_RetriesTheDatabaseNotJustOnce(t *testing.T) {
	// Port 1 on loopback refuses immediately, so each attempt fails fast
	// and the only thing that can make this slow is the backoff itself.
	databaseURL := "postgres://u:***@127.0.0.1:1/labor_performance?sslmode=disable&connect_timeout=1"

	start := time.Now()
	_, err := buildPersistence(context.Background(), databaseURL, databaseURL, migrationsDirForTest(t), quietLogger())
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("an unreachable database must fail boot, never fall back to the in-memory repo")
	}
	if elapsed < 3*time.Second {
		t.Fatalf("buildPersistence gave up in %v — it is not retrying, so a single "+
			"first-dial reset would crash-loop the pod (err: %v)", elapsed, err)
	}
	if !strings.Contains(err.Error(), "attempts") {
		t.Fatalf("err = %v, want it to report how many attempts were made", err)
	}
}

// With no DATABASE_URL the in-memory repo is correct and must cost
// nothing: no dial, no backoff, no delay to a local run.
func TestBuildPersistence_NoDatabaseURLUsesMemoryImmediately(t *testing.T) {
	start := time.Now()
	p, err := buildPersistence(context.Background(), "", "", migrationsDirForTest(t), quietLogger())
	if err != nil {
		t.Fatalf("buildPersistence: %v", err)
	}
	defer p.close()

	if p.standards == nil {
		t.Fatal("no repository returned")
	}
	if p.pool != nil {
		t.Fatal("no DATABASE_URL must not open a pool")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("the in-memory path took %v; it must not touch the network", elapsed)
	}
}

// TestMigrationsDatabaseURLFallback proves the fallback wiring
// cmd/labor/main.go's run() applies before calling buildPersistence:
// getenv("MIGRATIONS_DATABASE_URL", databaseURL) must return
// MIGRATIONS_DATABASE_URL's own value when it is set, and databaseURL
// itself (DATABASE_URL) when it is unset. This is the exact env-lookup
// line the fix for the PgBouncer/pg_advisory_lock incompatibility (ADR
// 0020-migrations-direct-postgres-connection.md, porting order-management's
// ADR-0029) depends on: any environment that doesn't provision the split
// (local dev, CI integration tests, a cluster whose Terraform predates
// this fix) must keep working exactly as before, using DATABASE_URL for
// everything including migrations.
func TestMigrationsDatabaseURLFallback(t *testing.T) {
	const databaseURL = "postgres://u:***@pgbouncer.example:6432/labor_performance?sslmode=disable"

	t.Run("falls back to DATABASE_URL when MIGRATIONS_DATABASE_URL is unset", func(t *testing.T) {
		t.Setenv("MIGRATIONS_DATABASE_URL", "")
		os.Unsetenv("MIGRATIONS_DATABASE_URL")

		got := getenv("MIGRATIONS_DATABASE_URL", databaseURL)
		if got != databaseURL {
			t.Fatalf("getenv fallback = %q, want the DATABASE_URL value %q", got, databaseURL)
		}
	})

	t.Run("uses MIGRATIONS_DATABASE_URL when set, not DATABASE_URL", func(t *testing.T) {
		const direct = "postgres://u:***@postgres-postgresql.example:5432/labor_performance?sslmode=disable"
		t.Setenv("MIGRATIONS_DATABASE_URL", direct)

		got := getenv("MIGRATIONS_DATABASE_URL", databaseURL)
		if got != direct {
			t.Fatalf("getenv = %q, want the direct MIGRATIONS_DATABASE_URL value %q (must NOT silently keep using DATABASE_URL/PgBouncer)", got, direct)
		}
		if got == databaseURL {
			t.Fatal("MIGRATIONS_DATABASE_URL and DATABASE_URL collapsed to the same value — the whole point of this env var is that it differs")
		}
	})
}

// TestBuildPersistence_UsesMigrationsDatabaseURLNotDatabaseURLForMigrations
// proves buildPersistence itself — not just the env-var read above —
// actually threads migrationsDatabaseURL into the migration step and
// databaseURL into the pgxpool, rather than the two ever being
// conflated. Gives DATABASE_URL an address nothing listens on (so
// opening the pgxpool, which happens AFTER migrations succeed, would
// hang/fail loudly if ever reached) and MIGRATIONS_DATABASE_URL a
// schemeless string that migrate.New rejects immediately with a
// distinctive parse error ("failed to parse scheme from database URL")
// — if buildPersistence ignored migrationsDatabaseURL and ran migrations
// against databaseURL instead, this test would see a dial/"connection
// refused" error, not the parse error.
func TestBuildPersistence_UsesMigrationsDatabaseURLNotDatabaseURLForMigrations(t *testing.T) {
	const (
		bogusMigrationsURL = "not-a-valid-connection-string"
		unreachableAppURL  = "postgres://u:***@127.0.0.1:1/labor_performance?sslmode=disable&connect_timeout=1"
	)

	_, err := buildPersistence(context.Background(), unreachableAppURL, bogusMigrationsURL, migrationsDirForTest(t), quietLogger())
	if err == nil {
		t.Fatal("a malformed MIGRATIONS_DATABASE_URL must fail boot")
	}
	if !strings.Contains(err.Error(), "parse scheme") {
		t.Fatalf("err = %v — expected the bogus-URL parse error from migrate.New; a \"connection refused\"/dial error here would mean migrations ran against databaseURL/unreachableAppURL instead of migrationsDatabaseURL", err)
	}
}

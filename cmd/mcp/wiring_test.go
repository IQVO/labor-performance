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

func quietLoggerMCP() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// migrationsDirForTestMCP resolves the repo's migrations directory, so the
// retry under test fails on the DIAL rather than on a missing directory
// (which would return before any retry and make the test vacuous).
func migrationsDirForTestMCP(t *testing.T) string {
	t.Helper()
	// cmd/mcp -> repo root.
	dir := "../../migrations"
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("migrations directory not found at %s: %v", dir, err)
	}
	return dir
}

// TestBuildAdapters_RetriesTheDatabaseNotJustOnce mirrors
// cmd/labor/wiring_test.go's identical test for this binary's own
// composition-root function, buildAdapters.
func TestBuildAdapters_RetriesTheDatabaseNotJustOnce(t *testing.T) {
	databaseURL := "postgres://u:***@127.0.0.1:1/labor_performance?sslmode=disable&connect_timeout=1"

	start := time.Now()
	_, cleanup, err := buildAdapters(context.Background(), databaseURL, databaseURL, migrationsDirForTestMCP(t), quietLoggerMCP())
	elapsed := time.Since(start)
	if cleanup != nil {
		cleanup()
	}

	if err == nil {
		t.Fatal("an unreachable database must fail boot, never fall back to the in-memory repo")
	}
	if elapsed < 3*time.Second {
		t.Fatalf("buildAdapters gave up in %v — it is not retrying, so a single "+
			"first-dial reset would crash-loop the pod (err: %v)", elapsed, err)
	}
	if !strings.Contains(err.Error(), "attempts") {
		t.Fatalf("err = %v, want it to report how many attempts were made", err)
	}
}

// With no DATABASE_URL the in-memory repo is correct and must cost
// nothing: no dial, no backoff, no delay to a local run.
func TestBuildAdapters_NoDatabaseURLUsesMemoryImmediately(t *testing.T) {
	start := time.Now()
	adapters, cleanup, err := buildAdapters(context.Background(), "", "", migrationsDirForTestMCP(t), quietLoggerMCP())
	if err != nil {
		t.Fatalf("buildAdapters: %v", err)
	}
	defer cleanup()

	if adapters.standards == nil {
		t.Fatal("no repository returned")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("the in-memory path took %v; it must not touch the network", elapsed)
	}
}

// TestMigrationsDatabaseURLFallback_MCP proves the fallback wiring
// cmd/mcp/main.go's run() applies before calling buildAdapters:
// getenv("MIGRATIONS_DATABASE_URL", databaseURL) must return
// MIGRATIONS_DATABASE_URL's own value when it is set, and databaseURL
// itself (DATABASE_URL) when it is unset. Same fix as cmd/labor
// (ADR 0020-migrations-direct-postgres-connection.md, porting
// order-management's ADR-0029): this binary also runs migrations on
// start, so it needs the identical split.
func TestMigrationsDatabaseURLFallback_MCP(t *testing.T) {
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

// TestBuildAdapters_UsesMigrationsDatabaseURLNotDatabaseURLForMigrations
// proves buildAdapters itself — not just the env-var read above —
// actually threads migrationsDatabaseURL into the migration step and
// databaseURL into the pgxpool, rather than the two ever being
// conflated. Mirrors cmd/labor/wiring_test.go's identical test.
func TestBuildAdapters_UsesMigrationsDatabaseURLNotDatabaseURLForMigrations(t *testing.T) {
	const (
		bogusMigrationsURL = "not-a-valid-connection-string"
		unreachableAppURL  = "postgres://u:***@127.0.0.1:1/labor_performance?sslmode=disable&connect_timeout=1"
	)

	_, cleanup, err := buildAdapters(context.Background(), unreachableAppURL, bogusMigrationsURL, migrationsDirForTestMCP(t), quietLoggerMCP())
	if cleanup != nil {
		cleanup()
	}
	if err == nil {
		t.Fatal("a malformed MIGRATIONS_DATABASE_URL must fail boot")
	}
	if !strings.Contains(err.Error(), "parse scheme") {
		t.Fatalf("err = %v — expected the bogus-URL parse error from migrate.New; a \"connection refused\"/dial error here would mean migrations ran against databaseURL/unreachableAppURL instead of migrationsDatabaseURL", err)
	}
}

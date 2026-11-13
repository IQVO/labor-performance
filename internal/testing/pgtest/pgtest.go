// Package pgtest boots ONE real Postgres through testcontainers per test
// binary and hands every test its own database, cloned from a template that
// already has this repo's migrations applied. It never reads DATABASE_URL and
// never skips: a missing Docker is a test failure (fleet rule,
// TestPostgresIntegrationTestsUseTestcontainers in internal/architecture).
//
// It ports the fleet's shared helper shape (customer-ordering's
// internal/testing/pgtest) so this repo's cmd-level integration suites get
// the same URL(t)/Shutdown() contract. The migrations run TWICE into the
// template on purpose: every binary that boots against a pgtest database
// runs them again itself in buildAdapters, and that second run must be a
// no-op.
package pgtest

import (
	"context"
	"fmt"
	"net/url"
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

	"github.com/claudioed/labor-performance/internal/adapters/outbound/postgres"
)

const (
	user         = "labor_test"
	password     = "labor_test"
	adminDB      = "postgres"
	templateName = "labor_it_template"
)

var (
	once      sync.Once
	container *tcpostgres.PostgresContainer
	errStart  error
	seq       atomic.Uint64
)

func start() {
	ctx := context.Background()
	container, errStart = tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase(adminDB),
		tcpostgres.WithUsername(user),
		tcpostgres.WithPassword(password),
		testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(90*time.Second)),
	)
	if errStart != nil {
		return
	}
	errStart = buildTemplate(ctx)
}

// buildTemplate creates the template database and applies the migrations to
// it twice: boot runs them on every start, so a second run must be a no-op.
func buildTemplate(ctx context.Context) error {
	if err := adminExec(ctx, `CREATE DATABASE `+templateName); err != nil {
		return err
	}
	dsn, err := databaseURL(ctx, templateName)
	if err != nil {
		return err
	}
	for range 2 {
		if err := postgres.RunMigrations(dsn, migrationsDir()); err != nil {
			return fmt.Errorf("migrations: %w", err)
		}
	}
	return nil
}

// migrationsDir resolves the repo's migrations directory relative to this
// file, so the suite does not depend on the working directory go test was
// invoked from.
func migrationsDir() string {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		panic("unable to resolve pgtest source path")
	}
	return filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "migrations")
}

// databaseURL rewrites the container's connection URL to the named database.
func databaseURL(ctx context.Context, db string) (string, error) {
	base, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return "", err
	}
	u, err := url.Parse(base)
	if err != nil {
		return "", err
	}
	u.Path = "/" + db
	return u.String(), nil
}

// adminExec runs one statement against the container's admin database.
func adminExec(ctx context.Context, sql string) error {
	dsn, err := databaseURL(ctx, adminDB)
	if err != nil {
		return err
	}
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(ctx) }()
	_, err = conn.Exec(ctx, sql)
	return err
}

// URL returns the DSN of a FRESH database (migrations applied) on the shared
// container, cloned from the migrated template in milliseconds.
func URL(t *testing.T) string {
	t.Helper()
	once.Do(start)
	if errStart != nil {
		t.Fatalf("start postgres container: %v", errStart)
	}
	ctx := context.Background()
	name := fmt.Sprintf("t_%d", seq.Add(1))
	if err := adminExec(ctx, `CREATE DATABASE `+name+` TEMPLATE `+templateName); err != nil {
		t.Fatalf("create database %s: %v", name, err)
	}
	dsn, err := databaseURL(ctx, name)
	if err != nil {
		t.Fatalf("database url: %v", err)
	}
	return dsn
}

// Shutdown terminates the shared container; call it from TestMain.
func Shutdown() {
	if container != nil {
		_ = testcontainers.TerminateContainer(container)
	}
}

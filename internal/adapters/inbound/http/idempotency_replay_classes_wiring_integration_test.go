//go:build integration

package http_test

import (
	"context"
	"errors"
	"net/http"
	"os"
	"testing"

	inboundhttp "github.com/claudioed/labor-performance/internal/adapters/inbound/http"
	"github.com/claudioed/labor-performance/internal/adapters/outbound/postgres"
	"github.com/claudioed/labor-performance/internal/pgtx"
	"github.com/claudioed/labor-performance/internal/testing/pgtest"
)

// TestMain owns the shared pgtest container this file's wiring boots (one container for the whole
// binary, one cloned, migrated database per newReplayOps call). The package's other integration
// tests boot their own container through idempotencyDB and are unaffected.
func TestMain(m *testing.M) {
	code := m.Run()
	pgtest.Shutdown()
	os.Exit(code)
}

// newReplayOps is the repo's half of the replay-class test (the harness template renders the other
// half verbatim): one clean Postgres per call, the REAL middleware over it, and the six closures.
// No driver type is named here: the pool comes from the package's helpers by type inference.
func newReplayOps(t *testing.T) replayOps {
	t.Helper()
	ctx := context.Background()
	pool, err := postgres.NewPool(ctx, pgtest.URL(t))
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)
	if _, err := pool.Exec(ctx, `CREATE TABLE replay_probe (id bigserial PRIMARY KEY, tag text NOT NULL)`); err != nil {
		t.Fatalf("create replay_probe: %v", err)
	}
	count := func(t *testing.T, query string, args ...any) int {
		t.Helper()
		var n int
		if err := pool.QueryRow(ctx, query, args...).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	return replayOps{
		Catalogue: func() []problemEntry {
			var out []problemEntry
			for _, p := range inboundhttp.ProblemTypesForTest() {
				out = append(out, problemEntry{Slug: p.Slug, Status: p.Status, Class: p.Class})
			}
			return out
		},
		Middleware: func(_ *testing.T, next http.Handler) http.Handler {
			return inboundhttp.RequireIdempotencyKey(pool)(next)
		},
		Refuse: func(w http.ResponseWriter, r *http.Request, e problemEntry) {
			inboundhttp.RefuseForTest(w, r, e.Slug, e.Status)
		},
		// Write goes through the transaction the middleware bound into the request context: the
		// same unit of work a use case joins (postgres.UnitOfWork.Execute).
		Write: func(ctx context.Context, tag string) error {
			tx, ok := pgtx.TxFrom(ctx)
			if !ok {
				return errNoTx
			}
			_, err := tx.Exec(ctx, `INSERT INTO replay_probe (tag) VALUES ($1)`, tag)
			return err
		},
		// Committed counts from the pool, a different connection: only committed rows count.
		Committed: func(t *testing.T, tag string) int {
			return count(t, `SELECT count(*) FROM replay_probe WHERE tag = $1`, tag)
		},
		KeyRows: func(t *testing.T, key string) int {
			return count(t, `SELECT count(*) FROM idempotency_keys WHERE key = $1`, key)
		},
	}
}

var errNoTx = errors.New("the middleware did not bind a transaction into the request context")

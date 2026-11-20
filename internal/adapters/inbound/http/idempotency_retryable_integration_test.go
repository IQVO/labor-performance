//go:build integration

// A refusal that depends on state that can change is RETRYABLE (ADR 0039): it
// is not stored, the key is released, and a retry under the same key executes
// again. These tests run through the REAL router and a real Postgres, for the
// two refusals POST /standards can answer for that reason:
// concurrent-modification (a stale read of the open standard) and
// standard-conflict (a lost race on the one-open-standard index).
package http_test

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	inboundhttp "github.com/claudioed/labor-performance/internal/adapters/inbound/http"
	"github.com/claudioed/labor-performance/internal/adapters/outbound/events"
	"github.com/claudioed/labor-performance/internal/adapters/outbound/postgres"
	"github.com/claudioed/labor-performance/internal/application/ports"
	"github.com/claudioed/labor-performance/internal/application/usecases"
	"github.com/claudioed/labor-performance/internal/domain/shared"
	"github.com/claudioed/labor-performance/internal/domain/standard"
)

const retryBody = `{"taskType":"PICK","expectedSeconds":50}`

func keyRows(t *testing.T, pool *pgxpool.Pool, key string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), "SELECT count(*) FROM idempotency_keys WHERE key = $1", key).Scan(&n); err != nil {
		t.Fatalf("count idempotency_keys: %v", err)
	}
	return n
}

// staleOpenStandard serves the open standard as it was, then (once armed)
// moves its version on out-of-band, exactly what a request that committed
// first does to a second one that already loaded the same row.
type staleOpenStandard struct {
	ports.StandardRepo
	pool  *pgxpool.Pool
	mu    sync.Mutex
	armed bool
}

func (s *staleOpenStandard) arm(on bool) { s.mu.Lock(); s.armed = on; s.mu.Unlock() }

func (s *staleOpenStandard) FindCurrentlyActive(ctx context.Context, tt shared.TaskType) (*standard.LaborStandard, error) {
	prior, err := s.StandardRepo.FindCurrentlyActive(ctx, tt)
	s.mu.Lock()
	armed := s.armed
	s.mu.Unlock()
	if armed && prior != nil {
		if _, err := s.pool.Exec(context.Background(), "UPDATE labor_standards SET version = version + 1 WHERE effective_to IS NULL"); err != nil {
			return nil, err
		}
	}
	return prior, err
}

func routerOver(pool *pgxpool.Pool, repo ports.StandardRepo) http.Handler {
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	server := &inboundhttp.Server{
		DefineStandard: &usecases.DefineStandard{
			Standards: repo, Events: events.NewLogPublisher(quiet),
			Clock:      fixedClockAt(time.Date(2026, 9, 27, 9, 0, 0, 0, time.UTC)),
			UnitOfWork: postgres.NewUnitOfWork(pool),
		},
		GetStandard:     &usecases.GetStandard{Standards: repo},
		IdempotencyPool: pool,
	}
	return inboundhttp.NewRouter(server, quiet, "")
}

// concurrent-modification: the 409 is not stored, its key row is absent, and
// the retry under the SAME key executes, succeeds, and is then stored and
// replayed.
func TestIdempotency_ConcurrentModificationIsNotStoredAndTheRetryExecutes(t *testing.T) {
	pool := idempotencyDB(t)
	stale := &staleOpenStandard{StandardRepo: postgres.NewStandardRepo(pool), pool: pool}
	router := routerOver(pool, stale)

	if rec := postStandard(router, "key-setup", validStandardBody); rec.Code != http.StatusCreated {
		t.Fatalf("setup status = %d, want 201 (%s)", rec.Code, rec.Body.String())
	}

	stale.arm(true)
	refused := postStandard(router, "key-cm", retryBody)
	stale.arm(false)
	if refused.Code != http.StatusConflict || !strings.Contains(refused.Body.String(), "concurrent-modification") {
		t.Fatalf("first = %d %s, want 409 concurrent-modification", refused.Code, refused.Body.String())
	}
	if n := keyRows(t, pool, "key-cm"); n != 0 {
		t.Fatalf("key rows after a RETRYABLE refusal = %d, want 0: the key must be released", n)
	}
	// Nothing committed: the setup standard is still the only row and still open.
	if got := countStandardRows(t, pool); got != 1 {
		t.Fatalf("labor_standards rows after the refusal = %d, want 1: a refusal commits nothing", got)
	}

	retry := postStandard(router, "key-cm", retryBody)
	if retry.Code != http.StatusCreated || retry.Header().Get(inboundhttp.IdempotencyReplayedHeader) != "" {
		t.Fatalf("retry = %d replayed=%q, want a fresh 201 (%s)", retry.Code, retry.Header().Get(inboundhttp.IdempotencyReplayedHeader), retry.Body.String())
	}
	if n := keyRows(t, pool, "key-cm"); n != 1 {
		t.Fatalf("key rows after the success = %d, want 1: a 2xx is stored", n)
	}
	again := postStandard(router, "key-cm", retryBody)
	if again.Code != http.StatusCreated || again.Header().Get(inboundhttp.IdempotencyReplayedHeader) != "true" || again.Body.String() != retry.Body.String() {
		t.Fatalf("third = %d replayed=%q, want the stored 201 replayed", again.Code, again.Header().Get(inboundhttp.IdempotencyReplayedHeader))
	}
	if got := countStandardRows(t, pool); got != 2 {
		t.Fatalf("labor_standards rows = %d, want 2 (the setup standard, closed, and the one the retry defined)", got)
	}
	// Released means a different body under the same key is a new request, not a reuse conflict.
	if other := postStandard(router, "key-cm-2", retryBody); other.Code != http.StatusCreated {
		t.Fatalf("a second key = %d, want 201", other.Code)
	}
}

// standard-conflict: two requests race to open the first standard; the loser's
// 409 is not stored, and its retry under the same key executes (it now finds the
// winner's standard open and supersedes it).
func TestIdempotency_StandardConflictIsNotStoredAndTheRetryExecutes(t *testing.T) {
	pool := idempotencyDB(t)
	racing := &staleReadStandards{StandardRepo: postgres.NewStandardRepo(pool)}
	racing.readers.Add(2)
	raced := routerOver(pool, racing)

	keys := []string{"key-conflict-a", "key-conflict-b"}
	results := make([]*httptest.ResponseRecorder, len(keys))
	var wg sync.WaitGroup
	for i, key := range keys {
		wg.Add(1)
		go func(i int, key string) {
			defer wg.Done()
			results[i] = postStandard(raced, key, validStandardBody)
		}(i, key)
	}
	wg.Wait()

	loser := ""
	for i, rec := range results {
		if rec.Code == http.StatusConflict {
			loser = keys[i]
		}
	}
	if loser == "" {
		t.Fatalf("no request answered 409: %d / %d", results[0].Code, results[1].Code)
	}
	if n := keyRows(t, pool, loser); n != 0 {
		t.Fatalf("key rows of the loser = %d, want 0: standard-conflict is RETRYABLE and its key is released", n)
	}

	plain := routerOver(pool, postgres.NewStandardRepo(pool))
	retry := postStandard(plain, loser, validStandardBody)
	if retry.Code != http.StatusCreated || retry.Header().Get(inboundhttp.IdempotencyReplayedHeader) != "" {
		t.Fatalf("retry = %d replayed=%q, want a fresh 201 (%s)", retry.Code, retry.Header().Get(inboundhttp.IdempotencyReplayedHeader), retry.Body.String())
	}
	if again := postStandard(plain, loser, validStandardBody); again.Code != http.StatusCreated || again.Header().Get(inboundhttp.IdempotencyReplayedHeader) != "true" {
		t.Fatalf("third = %d replayed=%q, want the stored 201 replayed", again.Code, again.Header().Get(inboundhttp.IdempotencyReplayedHeader))
	}
	if got := countStandardRows(t, pool); got != 2 {
		t.Fatalf("labor_standards rows = %d, want 2 (the winner's, closed by the retry, and the retry's)", got)
	}
}

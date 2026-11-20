//go:build integration

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

	inboundhttp "github.com/claudioed/labor-performance/internal/adapters/inbound/http"
	"github.com/claudioed/labor-performance/internal/adapters/outbound/events"
	"github.com/claudioed/labor-performance/internal/adapters/outbound/postgres"
	"github.com/claudioed/labor-performance/internal/application/ports"
	"github.com/claudioed/labor-performance/internal/application/usecases"
	"github.com/claudioed/labor-performance/internal/domain/shared"
	"github.com/claudioed/labor-performance/internal/domain/standard"
)

// staleReadStandards makes N concurrent DefineStandard calls all observe
// "no standard is open yet" before any of them writes — exactly what two
// real racing requests see under READ COMMITTED — so the race is
// deterministic instead of timing-dependent.
type staleReadStandards struct {
	ports.StandardRepo
	readers sync.WaitGroup
}

func (s *staleReadStandards) FindCurrentlyActive(ctx context.Context, tt shared.TaskType) (*standard.LaborStandard, error) {
	prior, err := s.StandardRepo.FindCurrentlyActive(ctx, tt)
	s.readers.Done()
	waited := make(chan struct{})
	go func() { s.readers.Wait(); close(waited) }()
	select {
	case <-waited:
	case <-time.After(10 * time.Second):
	}
	return prior, err
}

// Under RequireIdempotencyKey the use case's UnitOfWork JOINS the
// middleware's transaction. A 23505 on the one-open-standard partial index
// used to abort that shared transaction, so the middleware's own
// `UPDATE idempotency_keys` failed and the loser got 500 instead of the
// mapped 409 standard-conflict. Two requests with DIFFERENT keys race to
// open the first standard for a task type: exactly one wins (201), the
// other must get 409 standard-conflict (not a 500). standard-conflict is
// RETRYABLE (ADR 0039): its key is released, not recorded, so a retry
// executes (idempotency_retryable_integration_test.go proves that half).
func TestIdempotency_UniqueViolationInSharedTx_Returns409NotInternalError(t *testing.T) {
	pool := idempotencyDB(t)

	base := postgres.NewStandardRepo(pool)
	racing := &staleReadStandards{StandardRepo: base}
	racing.readers.Add(2)

	publisher := events.NewLogPublisher(slog.New(slog.NewTextHandler(io.Discard, nil)))
	server := &inboundhttp.Server{
		DefineStandard: &usecases.DefineStandard{
			Standards: racing, Events: publisher,
			Clock:      fixedClockAt(time.Date(2026, 9, 27, 9, 0, 0, 0, time.UTC)),
			UnitOfWork: postgres.NewUnitOfWork(pool),
		},
		GetStandard:     &usecases.GetStandard{Standards: base},
		IdempotencyPool: pool,
	}
	router := inboundhttp.NewRouter(server, slog.New(slog.NewTextHandler(io.Discard, nil)), "")

	keys := []string{"key-race-a", "key-race-b"}
	results := make([]*httptest.ResponseRecorder, len(keys))
	var wg sync.WaitGroup
	for i, key := range keys {
		wg.Add(1)
		go func(i int, key string) {
			defer wg.Done()
			req := httptest.NewRequest(http.MethodPost, "/standards", strings.NewReader(validStandardBody))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set(inboundhttp.IdempotencyKeyHeader, key)
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			results[i] = rec
		}(i, key)
	}
	wg.Wait()

	created, conflicted := 0, 0
	loserKey := ""
	for i, rec := range results {
		switch rec.Code {
		case http.StatusCreated:
			created++
		case http.StatusConflict:
			conflicted++
			loserKey = keys[i]
			if !strings.Contains(rec.Body.String(), "standard-conflict") {
				t.Errorf("409 body = %s, want the standard-conflict problem category", rec.Body.String())
			}
		default:
			t.Errorf("request %d status = %d, want 201 or 409 (body: %s)", i, rec.Code, rec.Body.String())
		}
	}
	if created != 1 || conflicted != 1 {
		t.Fatalf("created=%d conflicted=%d, want exactly one 201 and one 409", created, conflicted)
	}
	if got := countStandardRows(t, pool); got != 1 {
		t.Fatalf("labor_standards rows = %d, want exactly 1", got)
	}

	// The 409 is RETRYABLE: nothing was stored under the loser's key, so a
	// retry runs the request again instead of replaying the refusal.
	if n := keyRows(t, pool, loserKey); n != 0 {
		t.Fatalf("key rows for %s = %d, want 0 (a RETRYABLE refusal releases its key)", loserKey, n)
	}
}

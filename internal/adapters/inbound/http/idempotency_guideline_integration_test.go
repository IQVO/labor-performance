//go:build integration

// REST API guideline section 2 (docs/docs/adr/0037-adopt-fleet-rest-api-guideline.md)
// against a real Postgres 16 (testcontainers, never skip-gated): the key is
// validated (1-128 printable ASCII), global (found by the key alone, compared
// by method + path + body), and a 5xx is never stored. The cases that need a
// second route or a failing handler wrap throwaway stubs in the REAL
// RequireIdempotencyKey over a real chi router; POST /standards itself is
// covered through the real router in idempotency_integration_test.go.
package http_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	inboundhttp "github.com/claudioed/labor-performance/internal/adapters/inbound/http"
)

// problemTypeOf decodes the RFC 9457 problem `type` of a recorded response.
func problemTypeOf(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	var problem struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &problem); err != nil {
		t.Fatalf("decode problem: %v (body: %s)", err, rec.Body.String())
	}
	return problem.Type
}

func postStandard(router http.Handler, key, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/standards", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(inboundhttp.IdempotencyKeyHeader, key)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec
}

// A key that is not 1-128 printable ASCII characters is 400
// idempotency-key-invalid and never reaches the use case; 128 characters
// is the longest accepted key.
func TestIdempotency_MalformedKey_Returns400Invalid(t *testing.T) {
	pool := idempotencyDB(t)
	router := newIdempotencyRouter(t, pool)

	for name, key := range map[string]string{
		"129 characters":    strings.Repeat("k", 129),
		"control character": "key\x01one",
		"DEL":               "key\x7fone",
		"non-ASCII":         "clé-1",
	} {
		rec := postStandard(router, key, validStandardBody)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("%s: status = %d, want 400 (body: %s)", name, rec.Code, rec.Body.String())
		}
		if typ := problemTypeOf(t, rec); !strings.HasSuffix(typ, "idempotency-key-invalid") {
			t.Fatalf("%s: problem.type = %q, want suffix idempotency-key-invalid", name, typ)
		}
	}
	if got := countStandardRows(t, pool); got != 0 {
		t.Fatalf("labor_standards rows = %d, want 0 (a malformed key must not execute anything)", got)
	}

	longest := strings.Repeat("k", 128)
	if rec := postStandard(router, longest, validStandardBody); rec.Code != http.StatusCreated {
		t.Fatalf("128-character key: status = %d, want 201 (body: %s)", rec.Code, rec.Body.String())
	}
}

// stubRouter wraps two POST routes and one PATCH route in the real
// middleware; every handler counts its executions.
func stubRouter(pool *pgxpool.Pool, runs *atomic.Int32, status func(n int32) int) http.Handler {
	handler := func(w http.ResponseWriter, _ *http.Request) {
		n := runs.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status(n))
		_, _ = w.Write([]byte(`{"run":` + strconv.Itoa(int(n)) + `}`))
	}
	r := chi.NewRouter()
	keyed := r.With(inboundhttp.RequireIdempotencyKey(pool))
	keyed.Post("/a", handler)
	keyed.Post("/b", handler)
	keyed.Patch("/a", handler)
	return r
}

func doStub(router http.Handler, method, path, key, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set(inboundhttp.IdempotencyKeyHeader, key)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec
}

// The key is GLOBAL: the stored record is found by the key alone and compared
// by method + path + body, so the same key on another route or with another
// method is 409 idempotency-key-reused (never a fresh key), and it does not
// execute the handler.
func TestIdempotency_KeyIsGlobal_OtherRouteOrMethodIs409(t *testing.T) {
	pool := idempotencyDB(t)
	var runs atomic.Int32
	router := stubRouter(pool, &runs, func(int32) int { return http.StatusCreated })

	if rec := doStub(router, http.MethodPost, "/a", "key-global-1", `{}`); rec.Code != http.StatusCreated {
		t.Fatalf("first call status = %d, want 201", rec.Code)
	}
	for name, c := range map[string]struct{ method, path string }{
		"other route":  {http.MethodPost, "/b"},
		"other method": {http.MethodPatch, "/a"},
	} {
		rec := doStub(router, c.method, c.path, "key-global-1", `{}`)
		if rec.Code != http.StatusConflict {
			t.Fatalf("%s: status = %d, want 409 (body: %s)", name, rec.Code, rec.Body.String())
		}
		if typ := problemTypeOf(t, rec); !strings.HasSuffix(typ, "idempotency-key-reused") {
			t.Fatalf("%s: problem.type = %q, want suffix idempotency-key-reused", name, typ)
		}
		if got := rec.Header().Get(inboundhttp.IdempotencyReplayedHeader); got != "" {
			t.Fatalf("%s: a conflict carries %s: %q", name, inboundhttp.IdempotencyReplayedHeader, got)
		}
	}
	if got := runs.Load(); got != 1 {
		t.Fatalf("handler executions = %d, want 1 (reuse must never execute)", got)
	}
	// The same fingerprint is still a replay.
	if rec := doStub(router, http.MethodPost, "/a", "key-global-1", `{}`); rec.Header().Get(inboundhttp.IdempotencyReplayedHeader) != "true" {
		t.Fatalf("an identical retry must replay with %s: true", inboundhttp.IdempotencyReplayedHeader)
	}
}

// A 5xx is not a deterministic answer: it is never stored, so the retry with
// the same key executes again; a 4xx is stored and replayed.
func TestIdempotency_ServerErrorIsNotStored_ClientErrorIs(t *testing.T) {
	pool := idempotencyDB(t)
	var runs atomic.Int32
	router := stubRouter(pool, &runs, func(n int32) int {
		switch n {
		case 1:
			return http.StatusInternalServerError
		case 2:
			return http.StatusUnprocessableEntity
		default:
			return http.StatusCreated
		}
	})

	if rec := doStub(router, http.MethodPost, "/a", "key-5xx-1", `{}`); rec.Code != http.StatusInternalServerError {
		t.Fatalf("first call status = %d, want 500", rec.Code)
	}
	var stored int
	if err := pool.QueryRow(t.Context(), "SELECT count(*) FROM idempotency_keys WHERE key = $1", "key-5xx-1").Scan(&stored); err != nil {
		t.Fatalf("count idempotency_keys: %v", err)
	}
	if stored != 0 {
		t.Fatalf("a 5xx stored %d key rows, want 0", stored)
	}

	retry := doStub(router, http.MethodPost, "/a", "key-5xx-1", `{}`)
	if retry.Code != http.StatusUnprocessableEntity || retry.Header().Get(inboundhttp.IdempotencyReplayedHeader) != "" {
		t.Fatalf("retry after a 5xx: status = %d replayed = %q, want a fresh execution (422)", retry.Code, retry.Header().Get(inboundhttp.IdempotencyReplayedHeader))
	}
	again := doStub(router, http.MethodPost, "/a", "key-5xx-1", `{}`)
	if again.Code != http.StatusUnprocessableEntity || again.Header().Get(inboundhttp.IdempotencyReplayedHeader) != "true" {
		t.Fatalf("second retry: status = %d replayed = %q, want the cached 422 replayed", again.Code, again.Header().Get(inboundhttp.IdempotencyReplayedHeader))
	}
	if got := runs.Load(); got != 2 {
		t.Fatalf("handler executions = %d, want 2 (the 5xx retry ran, the 4xx replay did not)", got)
	}
}

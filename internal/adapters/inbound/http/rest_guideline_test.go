package http_test

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	inboundhttp "github.com/claudioed/labor-performance/internal/adapters/inbound/http"
)

// verbSegments is the closed vocabulary of command verbs the REST API
// guideline (section 1.2) forbids as a path segment. A new route that needs
// one of these actions is a transition noun (section 1.4), not a verb.
var verbSegments = map[string]bool{
	"cancel": true, "confirm": true, "close": true, "dispatch": true, "receive": true,
	"retire": true, "activate": true, "deactivate": true, "suspend": true, "reinstate": true,
	"approve": true, "reject": true, "publish": true, "revise": true, "void": true,
	"assign": true, "resolve": true, "complete": true, "release": true, "stow": true,
	"inspect": true, "override": true, "withdraw": true, "discontinue": true,
	"decommission": true, "reoptimise": true, "evaluate": true, "merge": true, "start": true,
	"end": true, "claim": true, "define": true, "record": true, "recompute": true,
}

var pathParam = regexp.MustCompile(`\{[^/}]+\}`)

// guidelineRouter is the real router with a Postgres pool that is never
// dialled: a request without an Idempotency-Key is answered 400 before the
// middleware touches it.
func guidelineRouter(t *testing.T) chi.Routes {
	t.Helper()
	pool, err := pgxpool.New(t.Context(), "postgres://labor:labor@127.0.0.1:1/labor?sslmode=disable")
	if err != nil {
		t.Fatalf("lazy pool: %v", err)
	}
	t.Cleanup(pool.Close)
	server := &inboundhttp.Server{IdempotencyPool: pool}
	router, ok := inboundhttp.NewRouter(server, slog.New(slog.NewTextHandler(io.Discard, nil)), "").(chi.Routes)
	if !ok {
		t.Fatal("NewRouter does not return a chi.Routes")
	}
	return router
}

// REST API guideline R1/R2: every route is a noun (no `:custom-method`, no
// verb segment). This service has no retired route to rename, so the guard
// is the closed vocabulary rather than a table of old paths.
func TestRoutesAreNounsNotVerbs(t *testing.T) {
	err := chi.Walk(guidelineRouter(t), func(method, route string, _ http.Handler, _ ...func(http.Handler) http.Handler) error {
		if strings.Contains(route, ":") {
			t.Errorf("%s %s uses a :custom-method suffix", method, route)
		}
		for _, seg := range strings.Split(strings.Trim(route, "/"), "/") {
			if verbSegments[seg] {
				t.Errorf("%s %s has the verb segment %q", method, route, seg)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk routes: %v", err)
	}
}

// REST API guideline R3: EVERY POST and PATCH route sits behind the
// idempotency middleware, so a request without the header is 400
// idempotency-key-required and never reaches the handler.
func TestEveryPostAndPatchRouteRequiresAnIdempotencyKey(t *testing.T) {
	router := guidelineRouter(t)
	checked := 0
	err := chi.Walk(router, func(method, route string, _ http.Handler, _ ...func(http.Handler) http.Handler) error {
		if method != http.MethodPost && method != http.MethodPatch {
			return nil
		}
		checked++
		path := pathParam.ReplaceAllString(route, "x")
		rec := httptest.NewRecorder()
		router.(http.Handler).ServeHTTP(rec, httptest.NewRequest(method, path, strings.NewReader(`{}`)))
		if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "idempotency-key-required") {
			t.Errorf("%s %s without a key: status = %d body = %s, want 400 idempotency-key-required", method, route, rec.Code, rec.Body.String())
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk routes: %v", err)
	}
	if checked == 0 {
		t.Fatal("no POST/PATCH route found: the walk is not seeing the router")
	}
}

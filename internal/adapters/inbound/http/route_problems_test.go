package http

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
)

// TestUnknownRouteAndWrongMethodAreProblems pins that chi's text/plain 404 and
// bare 405 defaults never reach a client: both routers answer RFC 7807.
func TestUnknownRouteAndWrongMethodAreProblems(t *testing.T) {
	routers := map[string]http.Handler{
		"oltp":    NewRouter(&Server{}, nil, ""),
		"reports": NewReportsRouter(&ReportsHandlers{Store: &stubReportStore{}}, nil, ""),
	}
	cases := []struct {
		name, method, path string
		wantStatus         int
		wantSlug           string
	}{
		{"unknown route", http.MethodGet, "/no/such/route", http.StatusNotFound, "resource-not-found"},
		{"unknown route, other method", http.MethodPost, "/no/such/route", http.StatusNotFound, "resource-not-found"},
		{"wrong method on /healthz", http.MethodDelete, "/healthz", http.StatusMethodNotAllowed, "method-not-allowed"},
	}
	for router, h := range routers {
		for _, tc := range cases {
			t.Run(router+"/"+tc.name, func(t *testing.T) {
				rec := httptest.NewRecorder()
				h.ServeHTTP(rec, httptest.NewRequest(tc.method, tc.path, http.NoBody))
				assertRouteProblem(t, rec, tc.wantStatus, tc.wantSlug, tc.path)
			})
		}
	}
}

func assertRouteProblem(t *testing.T, rec *httptest.ResponseRecorder, wantStatus int, wantSlug, wantInstance string) {
	t.Helper()
	if rec.Code != wantStatus {
		t.Fatalf("status = %d, want %d", rec.Code, wantStatus)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/problem+json" {
		t.Fatalf("Content-Type = %q, want application/problem+json", ct)
	}
	var p struct {
		Type     string `json:"type"`
		Status   int    `json:"status"`
		Instance string `json:"instance"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &p); err != nil {
		t.Fatalf("body is not JSON: %v (%s)", err, rec.Body.String())
	}
	if want := problemBaseURI + wantSlug; p.Type != want {
		t.Errorf("type = %q, want %q", p.Type, want)
	}
	if p.Status != wantStatus {
		t.Errorf("problem.status = %d, want %d", p.Status, wantStatus)
	}
	if p.Instance != wantInstance {
		t.Errorf("instance = %q, want %q", p.Instance, wantInstance)
	}
}

// TestWrongMethodCarriesAllowHeader pins RFC 9110 §15.5.6: a 405 MUST list the
// methods the resource does support. chi sets Allow only in its DEFAULT 405
// handler; once a custom handler is registered (the RFC 7807 one) it is called
// as-is, so the handler derives the list from the router itself.
func TestWrongMethodCarriesAllowHeader(t *testing.T) {
	routers := map[string]http.Handler{
		"oltp":    NewRouter(&Server{}, nil, ""),
		"reports": NewReportsRouter(&ReportsHandlers{Store: &stubReportStore{}}, nil, ""),
	}
	cases := []struct {
		router, method, path, wantAllow string
	}{
		{"oltp", http.MethodDelete, "/healthz", "GET"},
		{"oltp", http.MethodPost, "/readyz", "GET"},
		{"reports", http.MethodPost, "/healthz", "GET"},
		{"reports", http.MethodDelete, "/reports/performance", "GET"},
		{"reports", http.MethodPut, "/reports/performance/freshness", "GET"},
	}
	for _, tc := range cases {
		t.Run(tc.router+" "+tc.method+" "+tc.path, func(t *testing.T) {
			rec := httptest.NewRecorder()
			routers[tc.router].ServeHTTP(rec, httptest.NewRequest(tc.method, tc.path, http.NoBody))
			if rec.Code != http.StatusMethodNotAllowed {
				t.Fatalf("status = %d, want 405", rec.Code)
			}
			if got := rec.Header().Get("Allow"); got != tc.wantAllow {
				t.Errorf("Allow = %q, want %q", got, tc.wantAllow)
			}
		})
	}
}

// TestAllowedMethodsListsEveryRegisteredMethod covers a path with several
// methods, so the list is derived from the router and not hard-coded to GET.
func TestAllowedMethodsListsEveryRegisteredMethod(t *testing.T) {
	r := chi.NewRouter()
	noop := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
	r.Get("/x", noop)
	r.Post("/x", noop)
	r.Delete("/x", noop)
	r.Get("/y", noop)
	if got, want := allowedMethods(r, "/x"), "GET, POST, DELETE"; got != want {
		t.Errorf("allowedMethods(/x) = %q, want %q", got, want)
	}
	if got, want := allowedMethods(r, "/y"), "GET"; got != want {
		t.Errorf("allowedMethods(/y) = %q, want %q", got, want)
	}
	if got := allowedMethods(r, "/nope"); got != "" {
		t.Errorf("allowedMethods(/nope) = %q, want empty", got)
	}
}

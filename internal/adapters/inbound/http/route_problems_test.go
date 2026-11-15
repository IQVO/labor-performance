package http

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
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

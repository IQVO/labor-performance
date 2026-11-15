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
				if rec.Code != tc.wantStatus {
					t.Fatalf("status = %d, want %d", rec.Code, tc.wantStatus)
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
				if want := problemBaseURI + tc.wantSlug; p.Type != want {
					t.Errorf("type = %q, want %q", p.Type, want)
				}
				if p.Status != tc.wantStatus {
					t.Errorf("problem.status = %d, want %d", p.Status, tc.wantStatus)
				}
				if p.Instance != tc.path {
					t.Errorf("instance = %q, want %q", p.Instance, tc.path)
				}
			})
		}
	}
}

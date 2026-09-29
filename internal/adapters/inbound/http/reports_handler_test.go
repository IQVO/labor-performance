package http

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/claudioed/labor-performance/internal/analytics/report"
)

// stubReportStore is a ReportStore whose answers a test dictates.
type stubReportStore struct {
	rep       report.LaborPerformanceReport
	lag       time.Duration
	queryErr  error
	freshErr  error
	lastQuery report.ReportQuery
}

func (s *stubReportStore) Query(_ context.Context, q report.ReportQuery) (report.LaborPerformanceReport, error) {
	s.lastQuery = q
	if s.queryErr != nil {
		return report.LaborPerformanceReport{}, s.queryErr
	}
	return s.rep, nil
}

func (s *stubReportStore) FreshnessLag(context.Context) (time.Duration, error) {
	if s.freshErr != nil {
		return 0, s.freshErr
	}
	return s.lag, nil
}

func reportsRequest(t *testing.T, store report.ReportStore, target string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	NewReportsRouter(&ReportsHandlers{Store: store}, nil).
		ServeHTTP(rec, httptest.NewRequest(http.MethodGet, target, nil))
	return rec
}

func decodeBody(t *testing.T, rec *httptest.ResponseRecorder, dest any) {
	t.Helper()
	if err := json.NewDecoder(rec.Body).Decode(dest); err != nil {
		t.Fatalf("decode body %q: %v", rec.Body.String(), err)
	}
}

func hour(h int) time.Time { return time.Date(2026, 9, 5, h, 0, 0, 0, time.UTC) }

// performanceReportRow mirrors one rows[] element of the GET
// /reports/performance response body.
type performanceReportRow struct {
	TaskType          string   `json:"taskType"`
	HourBucket        string   `json:"hourBucket"`
	TasksRecorded     int      `json:"tasksRecorded"`
	TasksUnscored     int      `json:"tasksUnscored"`
	MeanEfficiencyPct *float64 `json:"meanEfficiencyPct"`
	MeanActualSeconds *float64 `json:"meanActualSeconds"`
	StandardsDefined  int      `json:"standardsDefined"`
}

// performanceReportBar mirrors one byTaskType[] element of the same body.
type performanceReportBar struct {
	TaskType          string   `json:"taskType"`
	TasksRecorded     int      `json:"tasksRecorded"`
	MeanEfficiencyPct *float64 `json:"meanEfficiencyPct"`
}

// performanceReportTotals mirrors the totals object of the same body.
type performanceReportTotals struct {
	TasksRecorded     int      `json:"tasksRecorded"`
	TasksScored       int      `json:"tasksScored"`
	MeanEfficiencyPct *float64 `json:"meanEfficiencyPct"`
}

// performanceReportBody mirrors the GET /reports/performance response body.
type performanceReportBody struct {
	From       string                  `json:"from"`
	To         string                  `json:"to"`
	Rows       []performanceReportRow  `json:"rows"`
	ByTaskType []performanceReportBar  `json:"byTaskType"`
	Totals     performanceReportTotals `json:"totals"`
}

func TestGetPerformanceReportSuccess(t *testing.T) {
	store := &stubReportStore{rep: report.Build([]report.Row{
		{
			Key:           report.RowKey{TaskType: "PICK", HourBucket: hour(9)},
			TasksRecorded: 3, TasksScored: 2, EfficiencyPctSum: 180,
			TasksMeasured: 3, ActualSecondsSum: 150,
			StandardsDefined: 1,
		},
		{
			Key:           report.RowKey{TaskType: report.UnclassifiedTaskType, HourBucket: hour(9)},
			TasksRecorded: 5,
		},
	})}

	rec := reportsRequest(t, store, "/reports/performance?from=2026-09-05T00:00:00Z&to=2026-09-06T00:00:00Z")

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}

	var body performanceReportBody
	decodeBody(t, rec, &body)

	if body.From != "2026-09-05T00:00:00Z" || body.To != "2026-09-06T00:00:00Z" {
		t.Errorf("window echoed as %s..%s, want the requested bounds", body.From, body.To)
	}
	if len(body.Rows) != 2 {
		t.Fatalf("got %d rows, want 2", len(body.Rows))
	}
	// Sorted by (hour, taskType): PICK before UNCLASSIFIED.
	assertReportPickRow(t, body.Rows[0])
	assertReportUnclassifiedRow(t, body.Rows[1])
	assertReportBreakdownAndTotals(t, body)
}

// assertReportPickRow pins the scored PICK row: real means, one standard
// defined.
func assertReportPickRow(t *testing.T, row performanceReportRow) {
	t.Helper()
	if row.TaskType != "PICK" || row.HourBucket != "2026-09-05T09:00:00Z" {
		t.Errorf("row[0] = %s @ %s, want PICK @ 09:00Z", row.TaskType, row.HourBucket)
	}
	if row.MeanEfficiencyPct == nil || *row.MeanEfficiencyPct != 90 {
		t.Errorf("PICK meanEfficiencyPct = %v, want 90", row.MeanEfficiencyPct)
	}
	if row.StandardsDefined != 1 {
		t.Errorf("PICK standardsDefined = %d, want 1", row.StandardsDefined)
	}
}

// assertReportUnclassifiedRow pins the unclassified bucket: real tasks, NO
// fabricated numbers. This is the wire-level assertion for ADR-0004's
// discipline — the JSON must carry null, not 0.
func assertReportUnclassifiedRow(t *testing.T, unclassified performanceReportRow) {
	t.Helper()
	if unclassified.TaskType != report.UnclassifiedTaskType {
		t.Fatalf("row[1] = %s, want %s", unclassified.TaskType, report.UnclassifiedTaskType)
	}
	if unclassified.TasksRecorded != 5 || unclassified.TasksUnscored != 5 {
		t.Errorf("unclassified counts = %d recorded / %d unscored, want 5/5",
			unclassified.TasksRecorded, unclassified.TasksUnscored)
	}
	if unclassified.MeanEfficiencyPct != nil {
		t.Errorf("unclassified meanEfficiencyPct = %v, want null", *unclassified.MeanEfficiencyPct)
	}
	if unclassified.MeanActualSeconds != nil {
		t.Errorf("unclassified meanActualSeconds = %v, want null", *unclassified.MeanActualSeconds)
	}
}

// assertReportBreakdownAndTotals pins the per-task-type bars and the window
// headline: every task counted, means averaged over the scored subset only.
func assertReportBreakdownAndTotals(t *testing.T, body performanceReportBody) {
	t.Helper()
	if len(body.ByTaskType) != 2 {
		t.Errorf("got %d bars, want 2 (one per task type)", len(body.ByTaskType))
	}
	if body.Totals.TasksRecorded != 8 || body.Totals.TasksScored != 2 {
		t.Errorf("totals = %d recorded / %d scored, want 8/2", body.Totals.TasksRecorded, body.Totals.TasksScored)
	}
	if body.Totals.MeanEfficiencyPct == nil || *body.Totals.MeanEfficiencyPct != 90 {
		t.Errorf("totals meanEfficiencyPct = %v, want 90 — the 5 unscorable tasks must not drag it toward 0",
			body.Totals.MeanEfficiencyPct)
	}
}

func TestGetPerformanceReportEmptyWindowServesEmptyArrays(t *testing.T) {
	store := &stubReportStore{rep: report.Build(nil)}

	rec := reportsRequest(t, store, "/reports/performance?from=2026-09-05T00:00:00Z&to=2026-09-06T00:00:00Z")

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 — an empty window is not an error", rec.Code)
	}
	// Asserting on the raw JSON: a chart binding to null crashes, a
	// chart binding to [] renders an honest empty chart.
	var raw map[string]json.RawMessage
	decodeBody(t, rec, &raw)
	if string(raw["rows"]) != "[]" {
		t.Errorf("rows = %s, want []", raw["rows"])
	}
	if string(raw["byTaskType"]) != "[]" {
		t.Errorf("byTaskType = %s, want []", raw["byTaskType"])
	}
}

func TestGetPerformanceReportPassesFiltersThrough(t *testing.T) {
	store := &stubReportStore{rep: report.Build(nil)}

	rec := reportsRequest(t, store,
		"/reports/performance?from=2026-09-05T00:00:00Z&to=2026-09-06T00:00:00Z&taskType=PICK&granularity=hour")

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", rec.Code, rec.Body.String())
	}
	if store.lastQuery.TaskType != "PICK" {
		t.Errorf("query TaskType = %q, want PICK", store.lastQuery.TaskType)
	}
	if store.lastQuery.Granularity != report.GranularityHour {
		t.Errorf("query Granularity = %q, want hour", store.lastQuery.Granularity)
	}
	if !store.lastQuery.From.Equal(time.Date(2026, 9, 5, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("query From = %v, want 2026-09-05T00:00:00Z", store.lastQuery.From)
	}
}

func TestGetPerformanceReportErrors(t *testing.T) {
	tests := []struct {
		name       string
		target     string
		store      report.ReportStore
		wantStatus int
		wantSlug   string
	}{
		{
			name:       "missing from",
			target:     "/reports/performance?to=2026-09-06T00:00:00Z",
			store:      &stubReportStore{},
			wantStatus: http.StatusBadRequest,
			wantSlug:   "invalid-report-query",
		},
		{
			name:       "missing to",
			target:     "/reports/performance?from=2026-09-05T00:00:00Z",
			store:      &stubReportStore{},
			wantStatus: http.StatusBadRequest,
			wantSlug:   "invalid-report-query",
		},
		{
			name:       "malformed from",
			target:     "/reports/performance?from=yesterday&to=2026-09-06T00:00:00Z",
			store:      &stubReportStore{},
			wantStatus: http.StatusBadRequest,
			wantSlug:   "invalid-report-query",
		},
		{
			name:       "inverted window",
			target:     "/reports/performance?from=2026-09-06T00:00:00Z&to=2026-09-05T00:00:00Z",
			store:      &stubReportStore{},
			wantStatus: http.StatusBadRequest,
			wantSlug:   "invalid-report-query",
		},
		{
			name:       "unsupported granularity",
			target:     "/reports/performance?from=2026-09-05T00:00:00Z&to=2026-09-06T00:00:00Z&granularity=day",
			store:      &stubReportStore{},
			wantStatus: http.StatusBadRequest,
			wantSlug:   "invalid-report-query",
		},
		{
			name:       "store failure",
			target:     "/reports/performance?from=2026-09-05T00:00:00Z&to=2026-09-06T00:00:00Z",
			store:      &stubReportStore{queryErr: errors.New("connection refused")},
			wantStatus: http.StatusInternalServerError,
			wantSlug:   "report-store-error",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := reportsRequest(t, tc.store, tc.target)

			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d: %s", rec.Code, tc.wantStatus, rec.Body.String())
			}
			if ct := rec.Header().Get("Content-Type"); ct != "application/problem+json" {
				t.Errorf("Content-Type = %q, want application/problem+json (RFC 7807)", ct)
			}

			var problem struct {
				Type   string `json:"type"`
				Title  string `json:"title"`
				Status int    `json:"status"`
				Detail string `json:"detail"`
			}
			decodeBody(t, rec, &problem)

			if want := problemBaseURI + tc.wantSlug; problem.Type != want {
				t.Errorf("type = %q, want %q", problem.Type, want)
			}
			if problem.Status != tc.wantStatus {
				t.Errorf("body status = %d, want %d", problem.Status, tc.wantStatus)
			}
			if problem.Title == "" || problem.Detail == "" {
				t.Errorf("problem is missing title/detail: %+v", problem)
			}
		})
	}
}

func TestGetPerformanceFreshness(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		rec := reportsRequest(t, &stubReportStore{lag: 90 * time.Second}, "/reports/performance/freshness")

		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200: %s", rec.Code, rec.Body.String())
		}
		var body struct {
			LagSeconds float64 `json:"lagSeconds"`
		}
		decodeBody(t, rec, &body)
		if body.LagSeconds != 90 {
			t.Errorf("lagSeconds = %v, want 90", body.LagSeconds)
		}
	})

	t.Run("store failure", func(t *testing.T) {
		rec := reportsRequest(t, &stubReportStore{freshErr: errors.New("db down")}, "/reports/performance/freshness")

		if rec.Code != http.StatusInternalServerError {
			t.Fatalf("status = %d, want 500", rec.Code)
		}
		if ct := rec.Header().Get("Content-Type"); ct != "application/problem+json" {
			t.Errorf("Content-Type = %q, want application/problem+json", ct)
		}
	})
}

func TestReportsHealthz(t *testing.T) {
	rec := reportsRequest(t, &stubReportStore{}, "/healthz")

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var body map[string]string
	decodeBody(t, rec, &body)
	if body["status"] != "ok" {
		t.Errorf("body = %v, want status ok", body)
	}
}

func TestReportsRouterAllowsConsoleOriginsButNotWrites(t *testing.T) {
	t.Setenv("CORS_ALLOWED_ORIGINS", "http://localhost:5187")

	router := NewReportsRouter(&ReportsHandlers{Store: &stubReportStore{}}, nil)

	req := httptest.NewRequest(http.MethodOptions, "/reports/performance", nil)
	req.Header.Set("Origin", "http://localhost:5187")
	req.Header.Set("Access-Control-Request-Method", http.MethodGet)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "http://localhost:5187" {
		t.Errorf("Access-Control-Allow-Origin = %q, want the configured console origin", got)
	}
	// The reports server has no write surface, so it must not advertise
	// one.
	if got := rec.Header().Get("Access-Control-Allow-Methods"); got != http.MethodGet {
		t.Errorf("Access-Control-Allow-Methods = %q, want GET only", got)
	}
}

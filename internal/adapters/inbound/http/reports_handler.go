package http

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/riandyrn/otelchi"
	otelchimetric "github.com/riandyrn/otelchi/metric"

	"github.com/claudioed/labor-performance/internal/analytics/report"
)

// ReportsHandlers is the inbound HTTP adapter for the labor-performance
// data product's READER (cmd/labor-reports). It depends only on the
// read-model port (report.ReportStore) — never on the OLTP use cases, and
// never on the writer (ADR-0007).
type ReportsHandlers struct {
	Store report.ReportStore
}

// reportRowDTO is the wire shape of one (taskType, hour) report row. It
// is a dedicated DTO so the read-model struct never leaks onto the API,
// matching the OLTP adapter's own rule.
//
// The mean fields are POINTERS so a bucket with no scored/measured tasks
// serialises as JSON null rather than 0 — the wire-level expression of
// ADR-0004's never-fabricate-a-number discipline. A client charting these
// must distinguish "no data" from "0% efficient".
type reportRowDTO struct {
	TaskType          string   `json:"taskType"`
	HourBucket        string   `json:"hourBucket"`
	TasksRecorded     int      `json:"tasksRecorded"`
	TasksScored       int      `json:"tasksScored"`
	TasksUnscored     int      `json:"tasksUnscored"`
	TasksMeasured     int      `json:"tasksMeasured"`
	MeanEfficiencyPct *float64 `json:"meanEfficiencyPct"`
	MeanActualSeconds *float64 `json:"meanActualSeconds"`
	StandardsDefined  int      `json:"standardsDefined"`
	StandardsRevised  int      `json:"standardsRevised"`
}

// taskTypeBarDTO is one bar of the per-TaskType breakdown — the shape the
// console's WES Dashboard panel binds a bar chart to.
type taskTypeBarDTO struct {
	TaskType          string   `json:"taskType"`
	TasksRecorded     int      `json:"tasksRecorded"`
	TasksScored       int      `json:"tasksScored"`
	TasksUnscored     int      `json:"tasksUnscored"`
	TasksMeasured     int      `json:"tasksMeasured"`
	MeanEfficiencyPct *float64 `json:"meanEfficiencyPct"`
	MeanActualSeconds *float64 `json:"meanActualSeconds"`
	StandardsDefined  int      `json:"standardsDefined"`
	StandardsRevised  int      `json:"standardsRevised"`
}

// totalsDTO is the whole queried window collapsed to one headline.
type totalsDTO struct {
	TasksRecorded     int      `json:"tasksRecorded"`
	TasksScored       int      `json:"tasksScored"`
	TasksUnscored     int      `json:"tasksUnscored"`
	TasksMeasured     int      `json:"tasksMeasured"`
	MeanEfficiencyPct *float64 `json:"meanEfficiencyPct"`
	MeanActualSeconds *float64 `json:"meanActualSeconds"`
}

// laborPerformanceReportDTO is the wire shape of a report response.
type laborPerformanceReportDTO struct {
	From       string           `json:"from"`
	To         string           `json:"to"`
	Rows       []reportRowDTO   `json:"rows"`
	ByTaskType []taskTypeBarDTO `json:"byTaskType"`
	Totals     totalsDTO        `json:"totals"`
}

// freshnessDTO is the wire shape of the freshness-lag response.
type freshnessDTO struct {
	LagSeconds float64 `json:"lagSeconds"`
}

// GetPerformanceReport serves GET /reports/performance. from and to
// (RFC3339) are required; taskType and granularity are optional
// (granularity defaults to hour).
func (h *ReportsHandlers) GetPerformanceReport(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	from, ok := parseRequiredTime(w, r, q.Get("from"), "from")
	if !ok {
		return
	}
	to, ok := parseRequiredTime(w, r, q.Get("to"), "to")
	if !ok {
		return
	}
	if !to.After(from) {
		writeReportBadRequest(w, r, "query parameter 'to' must be strictly after 'from'")
		return
	}

	granularity := report.GranularityHour
	if g := q.Get("granularity"); g != "" {
		if g != string(report.GranularityHour) {
			writeReportBadRequest(w, r, "granularity must be 'hour'")
			return
		}
		granularity = report.Granularity(g)
	}

	rep, err := h.Store.Query(r.Context(), report.ReportQuery{
		From:        from,
		To:          to,
		TaskType:    q.Get("taskType"),
		Granularity: granularity,
	})
	if err != nil {
		writeReportInternal(w, r, err)
		return
	}

	writeJSON(w, http.StatusOK, toReportDTO(from, to, rep))
}

// GetPerformanceFreshness serves GET /reports/performance/freshness — how
// far the projection trails the event stream, the data product's
// published freshness signal.
func (h *ReportsHandlers) GetPerformanceFreshness(w http.ResponseWriter, r *http.Request) {
	lag, err := h.Store.FreshnessLag(r.Context())
	if err != nil {
		writeReportInternal(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, freshnessDTO{LagSeconds: lag.Seconds()})
}

// GetReportsHealthz serves GET /healthz for the reports service.
func (h *ReportsHandlers) GetReportsHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// toReportDTO maps the read model onto the wire shape. Slices are built
// with make(..., 0, n) so an empty window marshals as [] rather than
// null — a chart binding to null is a client-side crash, a chart binding
// to [] is an honest empty chart.
func toReportDTO(from, to time.Time, rep report.LaborPerformanceReport) laborPerformanceReportDTO {
	rows := make([]reportRowDTO, 0, len(rep.Rows))
	for _, row := range rep.Rows {
		rows = append(rows, reportRowDTO{
			TaskType:          row.Key.TaskType,
			HourBucket:        row.Key.HourBucket.UTC().Format(timeFormat),
			TasksRecorded:     row.TasksRecorded,
			TasksScored:       row.TasksScored,
			TasksUnscored:     row.TasksUnscored(),
			TasksMeasured:     row.TasksMeasured,
			MeanEfficiencyPct: row.MeanEfficiencyPct(),
			MeanActualSeconds: row.MeanActualSeconds(),
			StandardsDefined:  row.StandardsDefined,
			StandardsRevised:  row.StandardsRevised,
		})
	}

	bars := make([]taskTypeBarDTO, 0, len(rep.ByTaskType))
	for _, b := range rep.ByTaskType {
		bars = append(bars, taskTypeBarDTO{
			TaskType:          b.TaskType,
			TasksRecorded:     b.TasksRecorded,
			TasksScored:       b.TasksScored,
			TasksUnscored:     b.TasksUnscored,
			TasksMeasured:     b.TasksMeasured,
			MeanEfficiencyPct: b.MeanEfficiencyPct,
			MeanActualSeconds: b.MeanActualSeconds,
			StandardsDefined:  b.StandardsDefined,
			StandardsRevised:  b.StandardsRevised,
		})
	}

	return laborPerformanceReportDTO{
		From:       from.UTC().Format(timeFormat),
		To:         to.UTC().Format(timeFormat),
		Rows:       rows,
		ByTaskType: bars,
		Totals: totalsDTO{
			TasksRecorded:     rep.Totals.TasksRecorded,
			TasksScored:       rep.Totals.TasksScored,
			TasksUnscored:     rep.Totals.TasksUnscored,
			TasksMeasured:     rep.Totals.TasksMeasured,
			MeanEfficiencyPct: rep.Totals.MeanEfficiencyPct,
			MeanActualSeconds: rep.Totals.MeanActualSeconds,
		},
	}
}

// parseRequiredTime parses an RFC3339 timestamp, writing an RFC 7807 400
// and returning ok=false when it is missing or malformed.
func parseRequiredTime(w http.ResponseWriter, r *http.Request, raw, name string) (time.Time, bool) {
	if raw == "" {
		writeReportBadRequest(w, r, "query parameter '"+name+"' is required (RFC3339)")
		return time.Time{}, false
	}
	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		writeReportBadRequest(w, r, "query parameter '"+name+"' must be an RFC3339 timestamp")
		return time.Time{}, false
	}
	return t, true
}

// writeReportBadRequest writes the reports service's RFC 7807 400, using
// the same problemDetails struct and problemBaseURI namespace as the OLTP
// adapter so both surfaces speak one error format.
func writeReportBadRequest(w http.ResponseWriter, r *http.Request, detail string) {
	writeProblem(w, http.StatusBadRequest,
		problemInfo{"invalid-report-query", "The report query is malformed or missing a required parameter"},
		detail, r.URL.Path)
}

// writeReportInternal writes the reports service's RFC 7807 500.
func writeReportInternal(w http.ResponseWriter, r *http.Request, err error) {
	writeProblem(w, http.StatusInternalServerError,
		problemInfo{"report-store-error", "The report could not be served"},
		err.Error(), r.URL.Path)
}

// NewReportsRouter builds the chi router for the labor-reports reader
// service. A nil logger defaults to slog.Default(); an empty serviceName
// defaults to DefaultReportsServiceName.
//
// Middleware order mirrors the OLTP router's (ADR-0008 Tier 1 item 2):
// otelchi + the request-duration histogram run ahead of the request
// logger so a log line already carries the request's span context, and
// WithChiRoutes labels metrics by route pattern rather than raw path.
//
// CORS is applied here too, from the same CORS_ALLOWED_ORIGINS convention
// the OLTP router uses: the reports API is the console-facing surface the
// WES Dashboard's labor panel actually calls from the browser, so it
// needs CORS at least as much as the OLTP API does. Only GET is allowed —
// this server has no write surface at all.
func NewReportsRouter(h *ReportsHandlers, logger *slog.Logger, serviceName string) http.Handler {
	if logger == nil {
		logger = slog.Default()
	}
	if serviceName == "" {
		serviceName = DefaultReportsServiceName
	}

	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(otelchi.Middleware(serviceName, otelchi.WithChiRoutes(r)))
	// Emits http.server.request.duration (seconds) per OTel HTTP semantic
	// conventions — the same RED instrumentation the OLTP router carries.
	r.Use(otelchimetric.NewServerRequestDuration(otelchimetric.NewBaseConfig(serviceName)))
	r.Use(RequestLogger(logger))
	r.Use(middleware.Recoverer)
	r.Use(readOnlyCORSMiddleware())

	r.Get("/healthz", h.GetReportsHealthz)

	r.Get("/reports/performance", h.GetPerformanceReport)
	r.Get("/reports/performance/freshness", h.GetPerformanceFreshness)

	return r
}

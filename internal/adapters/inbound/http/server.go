package http

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/cors"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riandyrn/otelchi"
	otelchimetric "github.com/riandyrn/otelchi/metric"

	"github.com/claudioed/labor-performance/internal/application/ports"
	"github.com/claudioed/labor-performance/internal/application/usecases"
	"github.com/claudioed/labor-performance/internal/domain/shared"
)

// DefaultServiceName labels this service in logs, spans and metrics when
// the caller does not supply one. It matches the OTel resource's
// service.name.
const DefaultServiceName = "labor-performance"

// DefaultReportsServiceName is the reports reader's equivalent default
// (cmd/labor-reports), matching that binary's OTEL_SERVICE_NAME fallback.
const DefaultReportsServiceName = "labor-performance-reports"

// Server holds every use case the HTTP adapter depends on.
type Server struct {
	DefineStandard         *usecases.DefineStandard
	GetStandard            *usecases.GetStandard
	GetAssociateScorecard  *usecases.GetAssociateScorecard
	GetTaskTypePerformance *usecases.GetTaskTypePerformance
	GetUtilization         *usecases.GetUtilization
	// IdempotencyPool, when non-nil, wires RequireIdempotencyKey onto
	// POST /standards (see idempotency.go). A nil pool means "no
	// transactional Postgres backing wired" (in-memory dev/test
	// configuration) — the idempotency middleware needs a real
	// pgxpool.Pool to begin its own transaction, so it is simply not
	// applied in that case, mirroring this codebase's existing
	// convention for every other optional Postgres-backed capability
	// (UnitOfWork, the outbox relay) and order-management's identical
	// IdempotencyPool field (PR #105 / ADR 0023).
	IdempotencyPool *pgxpool.Pool
	// Readiness backs GET /readyz (ADR-0017 §graceful shutdown): flipped
	// to not-ready as the FIRST step of shutdown, distinct from
	// /healthz (liveness, never flipped). Optional: a nil Readiness
	// (every existing caller/test that predates this field) always
	// reports ready, mirroring order-management's identical Readiness
	// field (ADR-0025).
	Readiness *Readiness
}

// NewRouter builds the chi router for every endpoint in CLAUDE.md's REST
// API. A nil logger defaults to slog.Default(); an empty serviceName
// defaults to DefaultServiceName.
//
// Middleware order matters here (fleet-standard-metrics ADR, Tier 1 item
// 2): otelchi runs before RequestLogger so the request context already
// carries a span by the time a line is logged, which is what lets the
// telemetry slog handler stamp trace_id/span_id onto it. WithChiRoutes
// resolves the route pattern up front, so spans/metrics are labeled
// "/standards/{taskType}" rather than one distinct name per task type.
func NewRouter(s *Server, logger *slog.Logger, serviceName string) http.Handler {
	if logger == nil {
		logger = slog.Default()
	}
	if serviceName == "" {
		serviceName = DefaultServiceName
	}

	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(otelchi.Middleware(serviceName, otelchi.WithChiRoutes(r)))
	// Emits http.server.request.duration (seconds) per OTel HTTP semantic
	// conventions; no hand-rolled histogram needed.
	r.Use(otelchimetric.NewServerRequestDuration(otelchimetric.NewBaseConfig(serviceName)))
	r.Use(RequestLogger(logger))
	r.Use(middleware.Recoverer)
	r.Use(corsMiddleware())

	// /healthz stays outside any route group; it never needed a
	// credential and still doesn't now that REST auth is gone.
	r.Get("/healthz", s.handleHealthz)
	// /readyz is distinct from /healthz (ADR-0017 §graceful shutdown):
	// liveness never flips, readiness flips to not-ready as the FIRST
	// step of shutdown so a Kubernetes readinessProbe pointed here
	// stops routing new traffic during the drain window that follows.
	r.Get("/readyz", s.handleReadyz)

	// POST /standards is route-scoped (r.With, not r.Use) behind
	// RequireIdempotencyKey — it is this service's one true
	// resource-creation endpoint (DefineStandard mints a fresh
	// StandardId server-side; a lost 201 response and a client retry
	// would otherwise silently close-and-redefine the standard a
	// second time). IdempotencyPool nil (in-memory dev/test
	// configuration, no transactional Postgres backing) skips the
	// middleware entirely, mirroring every other optional
	// Postgres-backed capability's nil convention in this service. See
	// docs/docs/adr/0016-idempotency-key-middleware.md.
	if s.IdempotencyPool != nil {
		r.With(RequireIdempotencyKey(s.IdempotencyPool)).Post("/standards", s.handleDefineStandard)
	} else {
		r.Post("/standards", s.handleDefineStandard)
	}
	r.Get("/standards/{taskType}", s.handleGetStandard)
	r.Get("/associates/{associateId}/scorecard", s.handleGetAssociateScorecard)
	r.Get("/task-types/{taskType}/performance", s.handleGetTaskTypePerformance)
	r.Get("/task-types/{taskType}/utilization", s.handleGetTaskTypeUtilization)
	r.Get("/associates/{associateId}/utilization", s.handleGetAssociateUtilization)

	return r
}

func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleDefineStandard(w http.ResponseWriter, r *http.Request) {
	var req defineStandardRequest
	if !decodeJSON(w, r, &req) {
		return
	}

	taskType, err := shared.NewTaskType(req.TaskType)
	if err != nil {
		writeError(w, r, err)
		return
	}

	st, err := s.DefineStandard.Execute(r.Context(), taskType, req.ExpectedSeconds, req.TravelComponentSeconds)
	if err != nil {
		writeError(w, r, err)
		return
	}

	writeJSON(w, http.StatusCreated, toStandardResponse(st.TaskType(), st.ExpectedSeconds(), st.TravelComponentSeconds(), st.EffectiveFrom(), st.EffectiveTo()))
}

func (s *Server) handleGetStandard(w http.ResponseWriter, r *http.Request) {
	taskType, err := shared.NewTaskType(chi.URLParam(r, "taskType"))
	if err != nil {
		writeError(w, r, err)
		return
	}

	st, err := s.GetStandard.Execute(r.Context(), taskType)
	if err != nil {
		writeError(w, r, err)
		return
	}

	writeJSON(w, http.StatusOK, toStandardResponse(st.TaskType(), st.ExpectedSeconds(), st.TravelComponentSeconds(), st.EffectiveFrom(), st.EffectiveTo()))
}

func (s *Server) handleGetAssociateScorecard(w http.ResponseWriter, r *http.Request) {
	associateId := shared.AssociateId(chi.URLParam(r, "associateId"))

	sc, err := s.GetAssociateScorecard.Execute(r.Context(), associateId)
	if err != nil {
		writeError(w, r, err)
		return
	}

	writeJSON(w, http.StatusOK, toScorecardResponse(sc))
}

func (s *Server) handleGetTaskTypePerformance(w http.ResponseWriter, r *http.Request) {
	taskType, err := shared.NewTaskType(chi.URLParam(r, "taskType"))
	if err != nil {
		writeError(w, r, err)
		return
	}

	tp, err := s.GetTaskTypePerformance.Execute(r.Context(), taskType)
	if err != nil {
		writeError(w, r, err)
		return
	}

	writeJSON(w, http.StatusOK, taskTypePerformanceResponse{
		TaskType:          string(tp.TaskType),
		TaskCount:         tp.TaskCount,
		MeanEfficiencyPct: tp.MeanEfficiencyPct,
		MeanActualSeconds: tp.MeanActualSeconds,
	})
}

func (s *Server) handleGetTaskTypeUtilization(w http.ResponseWriter, r *http.Request) {
	taskType, err := shared.NewTaskType(chi.URLParam(r, "taskType"))
	if err != nil {
		writeError(w, r, err)
		return
	}

	result, err := s.GetUtilization.ForTaskType(r.Context(), taskType, windowParam(r))
	if err != nil {
		writeError(w, r, err)
		return
	}

	writeJSON(w, http.StatusOK, toUtilizationResponse(result))
}

func (s *Server) handleGetAssociateUtilization(w http.ResponseWriter, r *http.Request) {
	associateId := shared.AssociateId(chi.URLParam(r, "associateId"))

	result, err := s.GetUtilization.ForAssociate(r.Context(), associateId, windowParam(r))
	if err != nil {
		writeError(w, r, err)
		return
	}

	writeJSON(w, http.StatusOK, toUtilizationResponse(result))
}

// windowParam parses the "window" query param as a Go duration string
// (e.g. "1h", "30m"). An absent or malformed value resolves to zero,
// which GetUtilization's callers treat as "apply the default" — a typo
// here must degrade gracefully, not 400, mirroring every other tuning
// knob in this fleet.
func windowParam(r *http.Request) time.Duration {
	raw := r.URL.Query().Get("window")
	if raw == "" {
		return 0
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return 0
	}
	return d
}

func toUtilizationResponse(result usecases.UtilizationResult) utilizationResponse {
	return utilizationResponse{
		TaskType:       string(result.TaskType),
		AssociateId:    string(result.AssociateId),
		Associates:     result.Associates,
		WindowSeconds:  result.WindowSeconds,
		TaskSeconds:    result.TaskSeconds,
		IdleSeconds:    result.IdleSeconds,
		OpenGapSeconds: result.OpenGapSeconds,
		UtilizationPct: result.UtilizationPct,
	}
}

const timeFormat = time.RFC3339

func toStandardResponse(taskType shared.TaskType, expectedSeconds int64, travelComponentSeconds *int64, effectiveFrom time.Time, effectiveTo *time.Time) standardResponse {
	var to *string
	if effectiveTo != nil {
		formatted := effectiveTo.UTC().Format(timeFormat)
		to = &formatted
	}
	return standardResponse{
		TaskType:               string(taskType),
		ExpectedSeconds:        expectedSeconds,
		TravelComponentSeconds: travelComponentSeconds,
		EffectiveFrom:          effectiveFrom.UTC().Format(timeFormat),
		EffectiveTo:            to,
	}
}

func toScorecardResponse(sc ports.Scorecard) scorecardResponse {
	byType := make(map[string]taskTypeBreakdownResponse, len(sc.ByTaskType))
	for tt, b := range sc.ByTaskType {
		byType[string(tt)] = taskTypeBreakdownResponse{
			TaskCount:         b.TaskCount,
			MeanEfficiencyPct: b.MeanEfficiencyPct,
		}
	}
	return scorecardResponse{
		AssociateId:       string(sc.AssociateId),
		TaskCount:         sc.TaskCount,
		MeanEfficiencyPct: sc.MeanEfficiencyPct,
		ByTaskType:        byType,
		Trend:             string(sc.Trend),
		CoachingFlag:      sc.CoachingFlag,
	}
}

func decodeJSON(w http.ResponseWriter, r *http.Request, dest any) bool {
	if err := json.NewDecoder(r.Body).Decode(dest); err != nil {
		writeProblem(w, http.StatusBadRequest, problemInfo{"malformed-request-body", "The request body is not valid JSON"}, err.Error(), r.URL.Path)
		return false
	}
	return true
}

// writeError writes a domain/application error as an RFC 7807
// (application/problem+json) response.
func writeError(w http.ResponseWriter, r *http.Request, err error) {
	writeProblem(w, statusFor(err), problemFor(err), err.Error(), r.URL.Path)
}

func writeProblem(w http.ResponseWriter, status int, info problemInfo, detail, instance string) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(problemDetails{
		Type:     problemBaseURI + info.slug,
		Title:    info.title,
		Status:   status,
		Detail:   detail,
		Instance: instance,
	})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

// corsMiddleware allows the warehouse-console browser SPA (and this
// service's own future labor-mfe remote dev origin) to call this API
// directly from the browser. CORS_ALLOWED_ORIGINS overrides the local-dev
// default (comma-separated) for staging/prod deployments. No labor-mfe
// screen exists yet in this round — see the README's "Deferred (v1)"
// section — but CORS ships now, matching the fleet's convention that CORS
// is added alongside a service's first console-facing REST surface.
func corsMiddleware() func(http.Handler) http.Handler {
	return cors.Handler(cors.Options{
		AllowedOrigins: allowedOrigins(),
		AllowedMethods: []string{http.MethodGet, http.MethodPost},
		// Idempotency-Key is listed alongside Authorization (kept from the
		// pre-ADR-0012 auth layer; the architecture fitness test tolerates
		// it) because POST /standards's idempotency middleware (ADR-0016)
		// requires the header: a browser (the labor-mfe remote) sending it
		// on a preflighted POST would be rejected by the browser itself
		// without an explicit allow entry.
		AllowedHeaders:   []string{"Content-Type", "Authorization", "Idempotency-Key"},
		AllowCredentials: false,
		MaxAge:           300,
	})
}

// readOnlyCORSMiddleware is corsMiddleware for the reports reader
// (cmd/labor-reports): same origin policy, but GET only. The reports
// server has no write surface, so advertising POST in
// Access-Control-Allow-Methods would be a claim that is simply untrue.
func readOnlyCORSMiddleware() func(http.Handler) http.Handler {
	return cors.Handler(cors.Options{
		AllowedOrigins:   allowedOrigins(),
		AllowedMethods:   []string{http.MethodGet},
		AllowedHeaders:   []string{"Content-Type", "Authorization"},
		AllowCredentials: false,
		MaxAge:           300,
	})
}

// allowedOrigins resolves the browser origins permitted to call this
// service, from CORS_ALLOWED_ORIGINS (comma-separated) or the local-dev
// default. Shared by the OLTP and reports routers so the two servers can
// never drift onto different origin policies.
func allowedOrigins() []string {
	if v := os.Getenv("CORS_ALLOWED_ORIGINS"); v != "" {
		return strings.Split(v, ",")
	}
	return []string{"http://localhost:5173", "http://localhost:5187"}
}

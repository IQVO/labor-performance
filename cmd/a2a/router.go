package main

import (
	"net/http"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	"github.com/go-chi/chi/v5"
	"github.com/riandyrn/otelchi"
	otelchimetric "github.com/riandyrn/otelchi/metric"
)

// newRouter wraps the A2A handler in a small chi router so the binary is
// deployable behind Kubernetes probes and carries the same HTTP RED
// instrumentation as every other surface of this service (ADR-0008 Tier 1
// item 2):
//
//   - otelchi.Middleware starts the request span (named after the route
//     pattern, via WithChiRoutes) and otelchimetric records
//     http.server.request.duration, in that order.
//   - GET /healthz -> 200 {"status":"ok"}. Liveness and readiness probes hit
//     this route. A2A is unauthenticated by decision (ADR 0012), so probes
//     need no credentials, and the route leaks no skill or state.
//   - The Agent Card is served at the well-known path
//     /.well-known/agent-card.json (the discovery contract).
//   - The JSON-RPC endpoint is mounted at BOTH "/" (the canonical root) and
//     "/a2a" (the fleet's <CTX>_A2A_ENDPOINT convention, mirroring cmd/mcp's
//     dual "/" and "/mcp" mount).
//
// serviceName is the OTEL_SERVICE_NAME resolved by the composition root.
func newRouter(a2aHandler http.Handler, card *a2a.AgentCard, serviceName string) http.Handler {
	r := chi.NewRouter()
	r.Use(otelchi.Middleware(
		serviceName,
		otelchi.WithChiRoutes(r),
		otelchi.WithRequestMethodInSpanName(true),
	))
	r.Use(otelchimetric.NewServerRequestDuration(otelchimetric.NewBaseConfig(serviceName)))
	r.Get("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	r.Handle(a2asrv.WellKnownAgentCardPath, a2asrv.NewStaticAgentCardHandler(card))
	r.Handle("/a2a", a2aHandler)
	r.Handle("/", a2aHandler)
	return r
}

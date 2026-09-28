// Command labor-reports is the READER composition root of the
// labor-performance "Labor Performance Report" data product. It opens the
// analytical Postgres database over a read-only pool and serves the
// report and its freshness over REST:
//
//	GET /reports/performance
//	GET /reports/performance/freshness
//	GET /healthz
//
// It writes nothing and migrates nothing: the writer
// (cmd/labor-projector) is a separate deployable and owns the schema
// (ADR-0007). It also never opens the OLTP DATABASE_URL, so report query
// load cannot contend with the transactional write path.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	inboundhttp "github.com/claudioed/labor-performance/internal/adapters/inbound/http"
	"github.com/claudioed/labor-performance/internal/adapters/outbound/analyticsstore"
	"github.com/claudioed/labor-performance/internal/adapters/outbound/bootretry"
	"github.com/claudioed/labor-performance/internal/adapters/outbound/telemetry"
)

// errMissingAnalyticsURL is returned when ANALYTICS_DATABASE_URL is
// unset.
var errMissingAnalyticsURL = errors.New("ANALYTICS_DATABASE_URL is required")

// version is the service version reported as the OTel service.version
// resource attribute, overridable at build time
// (-ldflags "-X main.version=1.2.3"), else SERVICE_VERSION, else "dev".
var version = ""

func main() {
	if err := run(); err != nil {
		slog.Error("labor-reports exited with error", "error", err)
		os.Exit(1)
	}
}

func run() error {
	logger := newLogger(getenv("LOG_LEVEL", "info"))
	slog.SetDefault(logger)

	rootCtx := context.Background()

	serviceName := getenv("OTEL_SERVICE_NAME", "labor-performance-reports")
	shutdownTelemetry, err := telemetry.Setup(rootCtx, serviceName, serviceVersion(),
		getenv("OTEL_EXPORTER_OTLP_ENDPOINT", telemetry.DefaultOTLPEndpoint))
	if err != nil {
		return err
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := shutdownTelemetry(shutdownCtx); err != nil {
			logger.Warn("telemetry shutdown did not flush cleanly", "error", err)
		}
	}()

	httpAddr := getenv("HTTP_ADDR", ":8092")
	analyticsURL := os.Getenv("ANALYTICS_DATABASE_URL")
	if analyticsURL == "" {
		return errMissingAnalyticsURL
	}

	// Read-only pool: even a bug in the reader cannot mutate the read
	// model, on top of the read-only database role
	// ANALYTICS_DATABASE_URL should itself use.
	pool, err := analyticsstore.NewReadOnlyPool(rootCtx, analyticsURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	// Retried: this fleet's Istio native sidecars reset EVERY pod's
	// first outbound TCP dial ~10s after the app starts
	// (holdApplicationUntilProxyStarts is a no-op for native sidecars).
	// ParseConfig/NewWithConfig do not themselves establish a
	// connection, so without this the first-dial reset would surface
	// inside the first served request instead of at boot, and a single
	// attempt would turn that transient condition into CrashLoopBackOff.
	if err := bootretry.Retry(rootCtx, logger, "ping analytics database", func() error {
		return pool.Ping(rootCtx)
	}); err != nil {
		return err
	}

	handlers := &inboundhttp.ReportsHandlers{
		Store: analyticsstore.NewPostgresReport(pool),
	}
	srv := &http.Server{
		Addr:              httpAddr,
		Handler:           inboundhttp.NewReportsRouter(handlers, logger),
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		logger.Info("reports server listening", "addr", httpAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	stopCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	select {
	case err := <-errCh:
		return err
	case <-stopCtx.Done():
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return srv.Shutdown(shutdownCtx)
}

// newLogger builds the process-wide structured logger, wrapped so any
// *Context log call made while a span is active carries trace_id/span_id.
func newLogger(level string) *slog.Logger {
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn", "warning":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	return slog.New(telemetry.NewTraceHandler(
		slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl}),
	))
}

// serviceVersion resolves service.version: build-time ldflags first, then
// SERVICE_VERSION, then "dev".
func serviceVersion() string {
	if version != "" {
		return version
	}
	return getenv("SERVICE_VERSION", "dev")
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

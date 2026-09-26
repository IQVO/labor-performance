// Command labor-projector is the WRITER composition root of the
// labor-performance "Labor Performance Report" data product. It consumes
// warehouse.labor-performance.analytics from the earliest offset,
// projects each event into the analytical Postgres database via the
// idempotent PostgresProjection, and serves only a health endpoint on an
// admin port.
//
// It is the SINGLE writer of the analytical database and serves no
// reports; the reader (cmd/labor-reports) is a separate deployable with a
// read-only pool. It also never opens the OLTP DATABASE_URL — the two
// databases share no connection (ADR-0007).
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

	inboundkafka "github.com/claudioed/labor-performance/internal/adapters/inbound/kafka"
	"github.com/claudioed/labor-performance/internal/adapters/kafka/envelope"
	"github.com/claudioed/labor-performance/internal/adapters/outbound/analyticsstore"
	"github.com/claudioed/labor-performance/internal/adapters/outbound/bootretry"
	"github.com/claudioed/labor-performance/internal/adapters/outbound/postgres"
	"github.com/claudioed/labor-performance/internal/adapters/outbound/telemetry"
)

// errMissingAnalyticsURL is returned when ANALYTICS_DATABASE_URL is
// unset: the projector is the writer of the analytical database and
// cannot start without it.
var errMissingAnalyticsURL = errors.New("ANALYTICS_DATABASE_URL is required")

// version is the service version reported as the OTel service.version
// resource attribute, overridable at build time
// (-ldflags "-X main.version=1.2.3"), else SERVICE_VERSION, else "dev".
var version = ""

func main() {
	if err := run(); err != nil {
		slog.Error("labor-projector exited with error", "error", err)
		os.Exit(1)
	}
}

func run() error {
	logger := newLogger(getenv("LOG_LEVEL", "info"))
	slog.SetDefault(logger)

	rootCtx := context.Background()

	serviceName := getenv("OTEL_SERVICE_NAME", "labor-performance-projector")
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

	adminAddr := getenv("ADMIN_ADDR", ":8091")
	analyticsURL := os.Getenv("ANALYTICS_DATABASE_URL")
	if analyticsURL == "" {
		return errMissingAnalyticsURL
	}
	kafkaBrokers := strings.Split(getenv("KAFKA_BROKERS", "localhost:9092"), ",")
	migrationsPath := getenv("ANALYTICS_MIGRATIONS_PATH", "migrations/analytics")

	// The projector owns the analytical schema, so it — and only it —
	// runs those migrations on start. The reader never migrates.
	//
	// Retried: this fleet's Istio native sidecars reset EVERY pod's
	// first outbound TCP dial ~10s after the app starts
	// (holdApplicationUntilProxyStarts is a no-op for native sidecars).
	// A single attempt turns that transient condition into
	// CrashLoopBackOff; the retry still fails closed once its budget is
	// exhausted.
	if err := bootretry.Retry(rootCtx, logger, "run analytics migrations", func() error {
		return postgres.RunMigrations(analyticsURL, migrationsPath)
	}); err != nil {
		return err
	}

	pool, err := analyticsstore.NewPool(rootCtx, analyticsURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	// ParseConfig/NewWithConfig do not themselves establish a
	// connection, so without this the first-dial reset would surface
	// inside the first consumed message instead of at boot.
	if err := bootretry.Retry(rootCtx, logger, "ping analytics database", func() error {
		return pool.Ping(rootCtx)
	}); err != nil {
		return err
	}

	consumer := inboundkafka.NewAnalyticsConsumer(
		kafkaBrokers,
		analyticsstore.NewPostgresProjection(pool),
		analyticsstore.NewConsumedEventsRepo(pool),
		logger,
	)
	defer func() {
		if err := consumer.Close(); err != nil {
			logger.Error("error closing analytics kafka consumer", "error", err)
		}
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	srv := &http.Server{Addr: adminAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	errCh := make(chan error, 2)
	go func() {
		logger.Info("projector admin server listening", "addr", adminAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	consumerCtx, cancelConsumer := context.WithCancel(rootCtx)
	defer cancelConsumer()
	go func() {
		logger.Info("analytics consumer starting",
			"topic", envelope.TopicLaborPerformanceAnalytics,
			"group", inboundkafka.AnalyticsConsumerGroup,
			"brokers", kafkaBrokers)
		if err := consumer.Run(consumerCtx); err != nil {
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

	cancelConsumer()
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

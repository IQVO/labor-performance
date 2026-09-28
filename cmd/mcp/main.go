// Command mcp is the composition root for the Labor Performance MCP
// server: it wires env config to outbound adapters, adapters to the read
// use cases, and those to the inbound MCP adapter, then serves MCP over
// Streamable HTTP. It is a second, independent deployable alongside
// cmd/labor (the HTTP + Kafka-consumer service), per ADR-0009.
//
// labor-performance exposes no write use case over MCP: DefineStandard is
// driven by an operator over chi HTTP and RecordTaskPerformance is driven
// by fulfillment-execution's TaskCompleted event over Kafka -- neither is
// a decision an MCP-calling agent should make on this context's behalf.
// This server therefore wires only the three read use cases
// (GetAssociateScorecard, GetTaskTypePerformance, GetStandard) and exposes
// only read tools.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	inboundmcp "github.com/claudioed/labor-performance/internal/adapters/inbound/mcp"
	"github.com/claudioed/labor-performance/internal/adapters/outbound/bootretry"
	"github.com/claudioed/labor-performance/internal/adapters/outbound/memory"
	"github.com/claudioed/labor-performance/internal/adapters/outbound/postgres"
	"github.com/claudioed/labor-performance/internal/adapters/outbound/telemetry"
	"github.com/claudioed/labor-performance/internal/application/ports"
	"github.com/claudioed/labor-performance/internal/application/usecases"
)

func main() {
	if err := run(); err != nil {
		slog.Error("mcp server exited with error", "error", err)
		os.Exit(1)
	}
}

func run() error {
	serviceName := getenv("OTEL_SERVICE_NAME", "labor-performance-mcp")

	logger := slog.New(telemetry.NewTraceHandler(
		newJSONHandler(getenv("LOG_LEVEL", "info")),
	))
	slog.SetDefault(logger)

	// Same non-blocking telemetry setup as the HTTP service: an unreachable
	// Collector degrades to dropped telemetry, never a server that won't start.
	shutdownTelemetry, err := telemetry.Setup(
		context.Background(),
		serviceName,
		getenv("SERVICE_VERSION", "dev"),
		getenv("OTEL_EXPORTER_OTLP_ENDPOINT", telemetry.DefaultOTLPEndpoint),
	)
	if err != nil {
		return err
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := shutdownTelemetry(shutdownCtx); err != nil {
			logger.Warn("telemetry shutdown reported an error", "error", err)
		}
	}()

	httpAddr := getenv("MCP_ADDR", ":8090")
	databaseURL := os.Getenv("DATABASE_URL")
	migrationsPath := getenv("MIGRATIONS_PATH", "migrations")

	adapters, closeAdapters, err := buildAdapters(context.Background(), databaseURL, migrationsPath, logger)
	if err != nil {
		return err
	}
	defer closeAdapters()

	// The MCP adapter reuses the SAME read use cases the HTTP adapter uses:
	// GetAssociateScorecard, GetTaskTypePerformance and GetStandard, each
	// over the same repos. No write use case is wired -- see this file's
	// own package doc comment.
	deps := inboundmcp.Deps{
		GetAssociateScorecard:  &usecases.GetAssociateScorecard{Performances: adapters.performances},
		GetTaskTypePerformance: &usecases.GetTaskTypePerformance{Performances: adapters.performances},
		GetStandard:            &usecases.GetStandard{Standards: adapters.standards},
		GetUtilization:         &usecases.GetUtilization{Performances: adapters.performances, IdlePeriods: adapters.idlePeriods, Clock: memory.SystemClock{}},
	}
	server := inboundmcp.NewServer(deps)
	handler := inboundmcp.Handler(server)

	srv := &http.Server{Addr: httpAddr, Handler: handler, ReadHeaderTimeout: 5 * time.Second}

	go func() {
		logger.Info("mcp server listening (Streamable HTTP)", "addr", httpAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("mcp server failed", "error", err)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return srv.Shutdown(shutdownCtx)
}

// adapterSet is the read-side outbound repos the MCP server needs, chosen
// at startup: Postgres when DATABASE_URL is set, in-memory otherwise. It
// mirrors cmd/labor's selection so both binaries read the same store.
type adapterSet struct {
	standards    ports.StandardRepo
	performances ports.PerformanceRepo
	idlePeriods  ports.IdlePeriodRepo
}

// buildAdapters wires the Postgres repos when DATABASE_URL is set, or falls
// back to the in-memory repos for local development without a database --
// exactly as cmd/labor/main.go's buildRepoAdapters does (minus the
// ProcessedEvents repo, which only the Kafka-consuming OLTP binary needs).
func buildAdapters(ctx context.Context, databaseURL, migrationsPath string, logger *slog.Logger) (adapterSet, func(), error) {
	noop := func() {}

	if databaseURL == "" {
		logger.Info("database url not configured; using in-memory adapters")
		return adapterSet{
			standards:    memory.NewStandardRepo(),
			performances: memory.NewPerformanceRepo(),
			idlePeriods:  memory.NewIdlePeriodRepo(),
		}, noop, nil
	}

	if err := bootretry.Retry(ctx, logger, "run migrations", func() error {
		return postgres.RunMigrations(databaseURL, migrationsPath)
	}); err != nil {
		return adapterSet{}, noop, err
	}

	pool, err := postgres.NewPool(ctx, databaseURL)
	if err != nil {
		return adapterSet{}, noop, err
	}
	// ParseConfig/NewWithConfig do not themselves establish a
	// connection, so without this the first-dial reset would surface
	// inside the first served request instead of at boot.
	if err := bootretry.Retry(ctx, logger, "ping database", func() error {
		return pool.Ping(ctx)
	}); err != nil {
		pool.Close()
		return adapterSet{}, noop, err
	}

	return adapterSet{
		standards:    postgres.NewStandardRepo(pool),
		performances: postgres.NewPerformanceRepo(pool),
		idlePeriods:  postgres.NewIdlePeriodRepo(pool),
	}, pool.Close, nil
}

// newJSONHandler mirrors cmd/labor/main.go's newLogger level parsing, kept
// local to this binary so both composition roots stay free-standing.
func newJSONHandler(level string) slog.Handler {
	var lvl slog.Level
	switch level {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	return slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl})
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

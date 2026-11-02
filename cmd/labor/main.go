// Command labor is the composition root for the Labor Performance
// service: it wires config from the environment to adapters, use cases,
// the HTTP router, and the inbound Kafka consumer, then serves both.
//
// Postgres and Kafka are the ONLY runtime (ADR 0034): there is no
// in-memory mode, no log sink, no optional relay and no consumer that can
// be switched off. A missing mandatory variable is a boot error naming it,
// raised before any connection is opened and before the listener binds.
//
// Environment:
//
//	DATABASE_URL                  REQUIRED. Postgres DSN
//	MIGRATIONS_DATABASE_URL       direct DSN for the boot migrations (default DATABASE_URL)
//	KAFKA_BROKERS                 REQUIRED. Comma-separated brokers
//	KAFKA_CONSUMER_GROUP          REQUIRED. Consumer group of the fulfillment-events consumer
//	EVENT_PUBLISHER               kafka (default). log is REJECTED at boot
//	HTTP_ADDR                     listen address (default :8080)
//	OUTBOX_RELAY_INTERVAL         relay poll interval (default 1s)
//	OUTBOX_MAX_ATTEMPTS           failures before the relay excludes a row (default 20, 0 = unlimited)
//	HOUSEKEEPING_INTERVAL, IDEMPOTENCY_KEY_TTL, OUTBOX_RETENTION   sweeper knobs (ADR-0023)
//	OTEL_EXPORTER_OTLP_ENDPOINT, LOG_LEVEL, SERVICE_VERSION
//
// /readyz is the drain gate AND a Postgres ping AND a Kafka metadata check
// (a broker answers a request that names no topic: reachability only, never
// topics), cached for a few seconds. /healthz is liveness only. The service
// never creates topics through an admin API: the cluster broker auto-creates
// them on first write.
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
	inboundkafka "github.com/claudioed/labor-performance/internal/adapters/inbound/kafka"
	"github.com/claudioed/labor-performance/internal/adapters/kafka/cloudevents"
	outboundkafka "github.com/claudioed/labor-performance/internal/adapters/outbound/kafka"
	"github.com/claudioed/labor-performance/internal/adapters/outbound/postgres"
	"github.com/claudioed/labor-performance/internal/adapters/outbound/system"
	"github.com/claudioed/labor-performance/internal/adapters/outbound/telemetry"
	"github.com/claudioed/labor-performance/internal/application/ports"
	"github.com/claudioed/labor-performance/internal/application/usecases"
)

// version is the service version reported as the OTel `service.version`
// resource attribute. Overridable at build time
// (-ldflags "-X main.version=1.2.3"), else SERVICE_VERSION, else "dev".
var version = ""

func main() {
	if err := run(context.Background()); err != nil {
		slog.Error("service exited with error", "error", err)
		os.Exit(1)
	}
}

// run boots the service and serves until SIGINT/SIGTERM or until ctx is
// cancelled (the in-process integration test stops it that way), then shuts
// down in the fleet order. It reads the whole configuration from the
// environment.
func run(ctx context.Context) error {
	logger := newLogger(getenv("LOG_LEVEL", "info"))
	slog.SetDefault(logger)

	// Fail fast, before telemetry, any connection and the listener: a
	// missing mandatory variable is a boot error that names it.
	cfg, err := loadBootConfig()
	if err != nil {
		return err
	}

	serviceName, shutdownTelemetry, err := setupTelemetry(ctx, logger)
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

	httpAddr := getenv("HTTP_ADDR", ":8080")
	// cfg.migrationsDatabaseURL is MIGRATIONS_DATABASE_URL, a DIRECT
	// (non-pooled, session-mode) Postgres connection string used ONLY for
	// the golang-migrate startup step below — everything else (the pgxpool
	// this process serves requests through) keeps using cfg.databaseURL
	// unchanged. See buildPersistence's doc comment for the full "why":
	// golang-migrate's postgres driver takes a session-scoped
	// `SELECT pg_advisory_lock($1)` to serialize concurrent migration
	// runs, which PgBouncer's transaction-pooling mode does not support
	// (warehouse-infra's PgBouncer rollout, PR #43; this fallback closes
	// the fleet-wide bug that rollout introduced — see ADR
	// 0020-migrations-direct-postgres-connection.md, porting
	// order-management's ADR-0029). It falls back to DATABASE_URL when
	// unset.
	persistence, err := buildPersistence(ctx, cfg.databaseURL, cfg.migrationsDatabaseURL, cfg.migrationsPath, logger)
	if err != nil {
		return err
	}
	defer persistence.close()
	standards, performances, processed := persistence.standards, persistence.performances, persistence.processed

	publisher, relay, closePublisher := buildEventPublisher(persistence, cfg.kafkaBrokers, logger)
	defer closePublisher()
	clock := system.Clock{}

	// Housekeeping sweeper (ADR-0023) + the outbox-lag gauge (ADR-0010's
	// deferred follow-up). The returned stop func is deferred so it runs
	// BEFORE the deferred persistence.close() closes the pool.
	stopHousekeeping := startHousekeeping(persistence.pool, logger)
	defer stopHousekeeping()

	standardMetrics, err := telemetry.NewStandardMetrics()
	if err != nil {
		return err
	}

	recordTaskPerformance := &usecases.RecordTaskPerformance{
		Performances:      performances,
		Standards:         standards,
		Processed:         processed,
		Events:            publisher,
		Clock:             clock,
		UnitOfWork:        persistence.uow,
		IdlePeriods:       persistence.idlePeriods,
		IdleGapCapSeconds: idleGapCapSecondsEnv(),
		Logger:            logger,
	}

	// readiness backs GET /readyz (ADR-0017 §graceful shutdown, ADR 0034):
	// the drain gate, flipped to not-ready as the FIRST step of the shutdown
	// sequence, AND a Postgres ping AND a Kafka metadata check, so a
	// Kubernetes readinessProbe only routes traffic to a pod whose
	// dependencies answer.
	readiness := inboundhttp.NewReadiness(0, 0,
		inboundhttp.Dependency{Name: "postgres", Check: persistence.pool.Ping},
		inboundhttp.Dependency{Name: "kafka", Check: outboundkafka.NewBrokerCheck(cfg.kafkaBrokers).Check},
	)
	server := buildHTTPServer(persistence, standards, performances, publisher, clock, standardMetrics, readiness)

	httpServer := &http.Server{
		Addr:              httpAddr,
		Handler:           inboundhttp.NewRouter(server, logger, serviceName),
		ReadHeaderTimeout: 5 * time.Second,
	}

	consumer := inboundkafka.NewConsumer(cfg.kafkaBrokers, cfg.consumerGroup, recordTaskPerformance, logger)

	return serve(ctx, httpServer, consumer, relay, cfg.kafkaBrokers, cfg.consumerGroup, logger, readiness)
}

// buildHTTPServer assembles the inbound HTTP adapter's dependency set:
// every use case the router serves plus the Postgres-backed collaborators.
// readiness gates GET /readyz (ADR-0017 §graceful shutdown): SetNotReady is
// called as the FIRST step of shutdown, before the HTTP server stops
// accepting, so a Kubernetes readinessProbe has a window to observe the
// flip.
func buildHTTPServer(p *persistence, standards ports.StandardRepo, performances ports.PerformanceRepo, publisher ports.EventPublisher, clock ports.Clock, standardMetrics *telemetry.StandardMetrics, readiness *inboundhttp.Readiness) *inboundhttp.Server {
	return &inboundhttp.Server{
		DefineStandard:         &usecases.DefineStandard{Standards: standards, Events: publisher, Clock: clock, UnitOfWork: p.uow, Metrics: standardMetrics},
		GetStandard:            &usecases.GetStandard{Standards: standards},
		GetAssociateScorecard:  &usecases.GetAssociateScorecard{Performances: performances},
		GetTaskTypePerformance: &usecases.GetTaskTypePerformance{Performances: performances},
		GetUtilization: &usecases.GetUtilization{
			Performances: performances,
			IdlePeriods:  p.idlePeriods,
			Clock:        clock,
		},
		// IdempotencyPool is always the Postgres pool: the Idempotency-Key
		// check on POST /standards is always on (ADR 0034).
		IdempotencyPool: p.pool,
		Readiness:       readiness,
	}
}

// setupTelemetry configures OTel once at boot and reports the resolved
// service name (the HTTP router needs it for its metrics label) plus the
// shutdown flush, which the caller defers until after every component has
// drained.
func setupTelemetry(ctx context.Context, logger *slog.Logger) (string, func(context.Context) error, error) {
	serviceName := getenv("OTEL_SERVICE_NAME", inboundhttp.DefaultServiceName)
	otlpEndpoint := getenv("OTEL_EXPORTER_OTLP_ENDPOINT", telemetry.DefaultOTLPEndpoint)
	shutdown, err := telemetry.Setup(ctx, serviceName, serviceVersion(), otlpEndpoint)
	if err != nil {
		return "", nil, err
	}
	logger.Info("telemetry configured",
		"service_name", serviceName,
		"service_version", serviceVersion(),
		"environment", telemetry.Environment(),
		"otlp_endpoint", otlpEndpoint,
	)
	return serviceName, shutdown, nil
}

// serve runs the wired components until a signal arrives (or ctx is
// cancelled) or one of them fails: the HTTP server, the fulfillment-events
// consumer, and the Postgres-backed outbox relay draining committed events
// onto Kafka. On shutdown it drains them in order — readiness flip, HTTP
// server, relay, consumer — and returns the first component error, if any.
func serve(ctx context.Context, httpServer *http.Server, consumer *inboundkafka.Consumer, relay *postgres.OutboxRelay, kafkaBrokers []string, kafkaGroupID string, logger *slog.Logger, readiness *inboundhttp.Readiness) error {
	defer func() {
		if err := consumer.Close(); err != nil {
			logger.Error("error closing kafka consumer", "error", err)
		}
	}()

	consumerCtx, cancelConsumer := context.WithCancel(ctx)
	defer cancelConsumer()
	// consumerDone closes once the Kafka consumer's Run goroutine has
	// actually returned -- including having committed the offset for
	// whatever message it was mid-handling when cancelConsumer was
	// called (see handleMessage's own commit-before-return shape) --
	// so graceful shutdown can wait for a REAL stop, not just
	// fire-and-forget the cancel (ADR-0017 §graceful shutdown,
	// mirroring order-management's ADR-0025 exactly).
	consumerDone := make(chan struct{})

	errCh := make(chan error, 3)
	go func() {
		logger.Info("http server listening", "addr", httpServer.Addr)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()
	go func() {
		defer close(consumerDone)
		logger.Info("kafka consumer starting", "brokers", kafkaBrokers, "group_id", kafkaGroupID, "topic", "warehouse.fulfillment.events")
		if err := consumer.Run(consumerCtx); err != nil {
			errCh <- err
		}
	}()

	stopCtx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// The outbox relay (ADR 0010) runs alongside the HTTP server and the
	// consumer in the same process, draining outbox_events onto Kafka. It is
	// always on: the service has no other way to publish.
	relayDone := make(chan struct{})
	relayCtx, stopRelay := context.WithCancel(context.Background())
	defer stopRelay()
	go func() {
		defer close(relayDone)
		logger.Info("outbox relay running", "topics", []string{cloudevents.TopicLaborPerformanceAnalytics, cloudevents.TopicLaborPerformanceEvents})
		if err := relay.Run(relayCtx); err != nil && !errors.Is(err, context.Canceled) {
			errCh <- err
		}
	}()

	select {
	case err := <-errCh:
		return err
	case <-stopCtx.Done():
	}

	// Graceful shutdown (ADR-0017 §graceful shutdown, mirroring
	// order-management's ADR-0025 sequencing exactly), in order:
	//
	//  1. Flip readiness to not-ready FIRST, before anything else
	//     stops -- a Kubernetes readinessProbe polling /readyz needs a
	//     window to observe this and stop routing NEW traffic to this
	//     pod before step 2 below ever closes the listener, so a
	//     request racing the SIGTERM is far less likely to be routed
	//     here only to hit a closing connection.
	//  2. Stop accepting new HTTP connections and drain in-flight
	//     requests, bounded by shutdownCtx.
	//  3. Stop the outbox relay and the Kafka consumer's loop cleanly:
	//     cancel their contexts (no new message is fetched/handled
	//     after this) and wait, bounded by the SAME shutdownCtx, for
	//     their goroutines to actually finish in-flight work (a
	//     message already being handled commits its offset before Run
	//     returns -- see Consumer.Run/handleMessage) rather than
	//     merely asking them to stop and moving on. This is the
	//     "final offset commit" guarantee: no message is left
	//     processed-but-uncommitted by an abrupt stop.
	//  4. Only THEN do the deferred consumer.Close()/closePublisher()/
	//     persistence.close() calls (registered earlier in this
	//     function, so by defer's LIFO order they run AFTER this
	//     function returns) -- persistence.close(), which closes the
	//     pgx pool, is registered FIRST and so by LIFO runs LAST of
	//     all, after every consumer/relay goroutine has already
	//     stopped touching it.
	readiness.SetNotReady()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := httpServer.Shutdown(shutdownCtx)

	// Let the relay finish its in-flight pass so an event committed by a
	// request (or a consumed message) that completed just before shutdown
	// is not stranded until the next pod boots.
	stopRelay()
	select {
	case <-relayDone:
	case <-shutdownCtx.Done():
		logger.Warn("outbox relay did not stop before the shutdown deadline")
	}

	// Stop the Kafka consumer's loop cleanly: cancel so no NEW message
	// is fetched, then wait (bounded) for any message already being
	// handled to finish -- including its offset commit -- before this
	// function returns and the deferred consumer.Close()/
	// persistence.close() calls run.
	cancelConsumer()
	select {
	case <-consumerDone:
	case <-shutdownCtx.Done():
		logger.Warn("kafka consumer did not stop before the shutdown deadline")
	}

	return err
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

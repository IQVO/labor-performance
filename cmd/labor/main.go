// Command labor is the composition root for the Labor Performance
// service: it wires config from the environment to adapters, use cases,
// the HTTP router, and the inbound Kafka consumer, then serves both.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	inboundhttp "github.com/claudioed/labor-performance/internal/adapters/inbound/http"
	inboundkafka "github.com/claudioed/labor-performance/internal/adapters/inbound/kafka"
	"github.com/claudioed/labor-performance/internal/adapters/kafka/envelope"
	"github.com/claudioed/labor-performance/internal/adapters/outbound/bootretry"
	"github.com/claudioed/labor-performance/internal/adapters/outbound/events"
	outboundkafka "github.com/claudioed/labor-performance/internal/adapters/outbound/kafka"
	"github.com/claudioed/labor-performance/internal/adapters/outbound/memory"
	"github.com/claudioed/labor-performance/internal/adapters/outbound/postgres"
	"github.com/claudioed/labor-performance/internal/adapters/outbound/telemetry"
	"github.com/claudioed/labor-performance/internal/application/ports"
	"github.com/claudioed/labor-performance/internal/application/usecases"
)

// version is the service version reported as the OTel `service.version`
// resource attribute. Overridable at build time
// (-ldflags "-X main.version=1.2.3"), else SERVICE_VERSION, else "dev".
var version = ""

func main() {
	if err := run(); err != nil {
		slog.Error("service exited with error", "error", err)
		os.Exit(1)
	}
}

func run() error {
	logger := newLogger(getenv("LOG_LEVEL", "info"))
	slog.SetDefault(logger)

	ctx := context.Background()

	serviceName := getenv("OTEL_SERVICE_NAME", inboundhttp.DefaultServiceName)
	otlpEndpoint := getenv("OTEL_EXPORTER_OTLP_ENDPOINT", telemetry.DefaultOTLPEndpoint)
	shutdownTelemetry, err := telemetry.Setup(ctx, serviceName, serviceVersion(), otlpEndpoint)
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
	logger.Info("telemetry configured",
		"service_name", serviceName,
		"service_version", serviceVersion(),
		"environment", telemetry.Environment(),
		"otlp_endpoint", otlpEndpoint,
	)

	httpAddr := getenv("HTTP_ADDR", ":8080")
	databaseURL := os.Getenv("DATABASE_URL")
	migrationsPath := getenv("MIGRATIONS_PATH", "migrations")

	persistence, err := buildPersistence(ctx, databaseURL, migrationsPath, logger)
	if err != nil {
		return err
	}
	defer persistence.close()
	standards, performances, processed := persistence.standards, persistence.performances, persistence.processed

	publisher, relay, closePublisher := buildEventPublisher(persistence, logger)
	defer closePublisher()
	clock := memory.SystemClock{}

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

	getUtilization := &usecases.GetUtilization{
		Performances: performances,
		IdlePeriods:  persistence.idlePeriods,
		Clock:        clock,
	}

	server := &inboundhttp.Server{
		DefineStandard:         &usecases.DefineStandard{Standards: standards, Events: publisher, Clock: clock, UnitOfWork: persistence.uow, Metrics: standardMetrics},
		GetStandard:            &usecases.GetStandard{Standards: standards},
		GetAssociateScorecard:  &usecases.GetAssociateScorecard{Performances: performances},
		GetTaskTypePerformance: &usecases.GetTaskTypePerformance{Performances: performances},
		GetUtilization:         getUtilization,
	}

	httpServer := &http.Server{
		Addr:              httpAddr,
		Handler:           inboundhttp.NewRouter(server, logger, serviceName),
		ReadHeaderTimeout: 5 * time.Second,
	}

	kafkaBrokers := strings.Split(getenv("KAFKA_BROKERS", "localhost:9092"), ",")
	kafkaGroupID := getenv("KAFKA_CONSUMER_GROUP", "labor-performance")
	consumer := inboundkafka.NewConsumer(kafkaBrokers, kafkaGroupID, recordTaskPerformance, logger)
	defer func() {
		if err := consumer.Close(); err != nil {
			logger.Error("error closing kafka consumer", "error", err)
		}
	}()

	consumerCtx, cancelConsumer := context.WithCancel(ctx)
	defer cancelConsumer()

	errCh := make(chan error, 3)
	go func() {
		logger.Info("http server listening", "addr", httpAddr)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()
	go func() {
		logger.Info("kafka consumer starting", "brokers", kafkaBrokers, "group_id", kafkaGroupID, "topic", "warehouse.fulfillment.events")
		if err := consumer.Run(consumerCtx); err != nil {
			errCh <- err
		}
	}()

	stopCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// The outbox relay (ADR 0010) runs alongside the HTTP server and the
	// consumer in the same process, draining outbox_events onto Kafka. It
	// is only wired when both Postgres and the kafka publisher are
	// configured.
	relayDone := make(chan struct{})
	relayCtx, stopRelay := context.WithCancel(context.Background())
	defer stopRelay()
	if relay != nil {
		go func() {
			defer close(relayDone)
			logger.Info("outbox relay running", "topics", []string{envelope.TopicLaborPerformanceAnalytics, envelope.TopicLaborPerformanceEvents})
			if err := relay.Run(relayCtx); err != nil && !errors.Is(err, context.Canceled) {
				errCh <- err
			}
		}()
	} else {
		close(relayDone)
	}

	select {
	case err := <-errCh:
		return err
	case <-stopCtx.Done():
	}

	cancelConsumer()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = httpServer.Shutdown(shutdownCtx)
	// Let the relay finish its in-flight pass so an event committed by a
	// request (or a consumed message) that completed just before shutdown
	// is not stranded until the next pod boots.
	stopRelay()
	select {
	case <-relayDone:
	case <-shutdownCtx.Done():
		logger.Warn("outbox relay did not stop before the shutdown deadline")
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
	case "warn":
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

// buildEventPublisher wires the outbound event publisher, returning it,
// the outbox relay to run alongside the HTTP server (nil when there is
// none), and a close function.
//
// The default is the log publisher this service has always used, so
// nothing about the existing OLTP behaviour changes unless it is opted
// into. Setting EVENT_PUBLISHER=kafka additionally fans every domain
// event onto TWO Kafka topics: warehouse.labor-performance.analytics,
// which feeds the analytical data product's projector (ADR-0007), and
// warehouse.labor-performance.events, this service's integration topic
// (ADR-0013) carrying TaskPerformanceRecorded for other bounded contexts
// to consume. The log publisher stays FIRST in the fan-out so a broker
// outage still leaves the event visible in the logs before the publish
// error surfaces.
//
// With BOTH Postgres and kafka configured the use cases publish into the
// transactional outbox (ADR 0010) and the relay forwards rows to both
// topics; the store and the topics can no longer diverge. With kafka but
// no Postgres (in-memory dev runs) events go straight to the broker as
// before — there is no transaction to bind them to.
//
// None of this touches the domain or application layers: they still see
// one ports.EventPublisher.
func buildEventPublisher(p *persistence, logger *slog.Logger) (ports.EventPublisher, *postgres.OutboxRelay, func()) {
	logPublisher := events.NewLogPublisher(logger)
	if !strings.EqualFold(getenv("EVENT_PUBLISHER", "log"), "kafka") {
		logger.Info("event publisher configured", "publisher", "log", "mode", "direct")
		return logPublisher, nil, func() {}
	}

	brokers := strings.Split(getenv("KAFKA_BROKERS", "localhost:9092"), ",")
	analytics := outboundkafka.NewAnalyticsPublisher(brokers, uuid.NewString)
	integration := outboundkafka.NewIntegrationPublisher(brokers, uuid.NewString)
	closePublishers := func() {
		if err := analytics.Close(); err != nil {
			logger.Error("error closing analytics kafka publisher", "error", err)
		}
		if err := integration.Close(); err != nil {
			logger.Error("error closing integration kafka publisher", "error", err)
		}
	}

	if p.pool == nil {
		logger.Info("event publisher configured", "publisher", "kafka", "mode", "direct",
			"topics", []string{envelope.TopicLaborPerformanceAnalytics, envelope.TopicLaborPerformanceEvents}, "brokers", brokers)
		return outboundkafka.NewFanOutPublisher(logPublisher, analytics, integration), nil, closePublishers
	}

	sink := outboundkafka.NewRelaySink(brokers)
	relay := postgres.NewOutboxRelay(p.pool, sink, logger,
		postgres.WithInterval(durationEnv("OUTBOX_RELAY_INTERVAL", time.Second)))
	logger.Info("event publisher configured", "publisher", "kafka", "mode", "outbox",
		"topics", []string{envelope.TopicLaborPerformanceAnalytics, envelope.TopicLaborPerformanceEvents}, "brokers", brokers)
	outbox := postgres.NewOutboxPublisher(p.pool, analytics, integration)
	return outboundkafka.NewFanOutPublisher(logPublisher, outbox), relay, func() {
		if err := sink.Close(); err != nil {
			logger.Error("error closing outbox relay sink", "error", err)
		}
		closePublishers()
	}
}

// persistence is what buildPersistence wires: the repos the use cases
// read/write through, the Postgres pool (nil when running in-memory),
// and the UnitOfWork that brackets Save + Publish (nil when in-memory,
// which the use cases treat as "run them back to back").
type persistence struct {
	standards    ports.StandardRepo
	performances ports.PerformanceRepo
	processed    ports.ProcessedEvents
	idlePeriods  ports.IdlePeriodRepo
	pool         *pgxpool.Pool
	uow          ports.UnitOfWork
	close        func()
}

// buildPersistence wires the Postgres adapters when DATABASE_URL is set,
// or falls back to the in-memory adapters for local development without a
// database.
func buildPersistence(ctx context.Context, databaseURL, migrationsPath string, logger *slog.Logger) (*persistence, error) {
	if databaseURL == "" {
		logger.Info("database url not configured; using in-memory adapters")
		return &persistence{
			standards:    memory.NewStandardRepo(),
			performances: memory.NewPerformanceRepo(),
			processed:    memory.NewProcessedEventRepo(),
			idlePeriods:  memory.NewIdlePeriodRepo(),
			close:        func() {},
		}, nil
	}

	// Retried: this fleet's Istio native sidecars reset EVERY pod's first
	// outbound TCP dial ~10s after the app starts
	// (holdApplicationUntilProxyStarts is a no-op for native sidecars). A
	// single attempt turns that transient condition into CrashLoopBackOff;
	// the retry still fails closed once its budget is exhausted.
	if err := bootretry.Retry(ctx, logger, "run migrations", func() error {
		return postgres.RunMigrations(databaseURL, migrationsPath)
	}); err != nil {
		return nil, err
	}
	pool, err := postgres.NewPool(ctx, databaseURL)
	if err != nil {
		return nil, err
	}
	// ParseConfig/NewWithConfig do not themselves establish a connection,
	// so without this the first-dial reset would surface inside the first
	// real request instead of at boot.
	if err := bootretry.Retry(ctx, logger, "ping database", func() error {
		return pool.Ping(ctx)
	}); err != nil {
		pool.Close()
		return nil, err
	}
	return &persistence{
		standards:    postgres.NewStandardRepo(pool),
		performances: postgres.NewPerformanceRepo(pool),
		processed:    postgres.NewProcessedEventRepo(pool),
		idlePeriods:  postgres.NewIdlePeriodRepo(pool),
		pool:         pool,
		uow:          postgres.NewUnitOfWork(pool),
		close:        pool.Close,
	}, nil
}

// durationEnv parses key as a time.Duration, falling back on absence or a
// malformed value (the relay interval is a tuning knob, not a contract, so
// a typo must not fail the boot).
func durationEnv(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return fallback
	}
	return d
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// idleGapCapSecondsEnv resolves IDLE_GAP_CAP_SECONDS, falling back to
// RecordTaskPerformance's own defaultIdleGapCapSeconds (3600) on absence
// or a malformed/non-positive value — a typo here must degrade
// gracefully, not crash the boot, mirroring durationEnv's discipline for
// OUTBOX_RELAY_INTERVAL.
func idleGapCapSecondsEnv() int64 {
	v := os.Getenv("IDLE_GAP_CAP_SECONDS")
	if v == "" {
		return 0
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n <= 0 {
		return 0
	}
	return n
}

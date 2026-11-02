package main

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/labor-performance/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/labor-performance/internal/adapters/outbound/bootretry"
	outboundkafka "github.com/claudioed/labor-performance/internal/adapters/outbound/kafka"
	"github.com/claudioed/labor-performance/internal/adapters/outbound/postgres"
	"github.com/claudioed/labor-performance/internal/application/ports"
)

// persistence is what buildPersistence wires: the Postgres repos the use
// cases read/write through, the pool, and the UnitOfWork that brackets
// Save + Publish.
type persistence struct {
	standards    ports.StandardRepo
	performances ports.PerformanceRepo
	processed    ports.ProcessedEvents
	idlePeriods  ports.IdlePeriodRepo
	pool         *pgxpool.Pool
	uow          ports.UnitOfWork
	close        func()
}

// buildPersistence wires the Postgres adapters. An empty databaseURL is a
// boot error (errNoDatabase): there is no in-memory fallback (ADR 0034).
//
// migrationsDatabaseURL is used ONLY for the golang-migrate step below —
// the pgxpool opened just after it (and used for every subsequent
// request) always uses databaseURL. They are deliberately different
// connection strings in a PgBouncer-fronted environment: golang-migrate's
// postgres driver takes a session-scoped `SELECT pg_advisory_lock($1)` to
// serialize concurrent migration runs across replicas starting at the
// same time, and PgBouncer's transaction-pooling mode (this fleet's
// pool_mode for every OLTP DATABASE_URL, warehouse-infra PR #43) does not
// support session-scoped state — each statement in one logical client
// session can land on a different physical backend connection, so the
// advisory lock never behaves as a real mutex. Losing replicas crash-loop
// with `pq: unnamed prepared statement does not exist` / `pq: canceling
// statement due to statement timeout` until one wins the race. See ADR
// 0020-migrations-direct-postgres-connection.md (porting order-management's
// ADR-0029) for the full incident and fix. Callers pass
// MIGRATIONS_DATABASE_URL when set (warehouse-infra provisions it as a
// direct, non-pooled DSN alongside DATABASE_URL, PR #44) or fall back to
// databaseURL itself for any environment that doesn't provision the split
// (local dev, CI integration tests).
func buildPersistence(ctx context.Context, databaseURL, migrationsDatabaseURL, migrationsPath string, logger *slog.Logger) (*persistence, error) {
	if databaseURL == "" {
		return nil, errNoDatabase
	}

	// Retried: this fleet's Istio native sidecars reset EVERY pod's first
	// outbound TCP dial ~10s after the app starts
	// (holdApplicationUntilProxyStarts is a no-op for native sidecars). A
	// single attempt turns that transient condition into CrashLoopBackOff;
	// the retry still fails closed once its budget is exhausted.
	if err := bootretry.Retry(ctx, logger, "run migrations", func() error {
		return postgres.RunMigrations(migrationsDatabaseURL, migrationsPath)
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

// buildEventPublisher wires the outbound event publisher, returning it,
// the outbox relay to run alongside the HTTP server, and a close function.
//
// The only publisher is the Kafka-backed transactional outbox (ADR 0010,
// ADR 0034): every domain event is enqueued onto TWO Kafka topics in the
// same transaction as the aggregate write — warehouse.labor-performance.
// analytics, which feeds the analytical data product's projector (ADR-0007),
// and warehouse.labor-performance.events, this service's integration topic
// (ADR-0013) carrying TaskPerformanceRecorded for other bounded contexts to
// consume — and the relay forwards the rows. The store and the topics can
// never diverge. There is no log sink and no direct-to-broker mode.
//
// None of this touches the domain or application layers: they still see
// one ports.EventPublisher.
func buildEventPublisher(p *persistence, brokers []string, logger *slog.Logger) (ports.EventPublisher, *postgres.OutboxRelay, func()) {
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

	sink := outboundkafka.NewRelaySink(brokers)
	// OUTBOX_MAX_ATTEMPTS (ADR 0032, gap fix D12): after this many failed
	// sends a row is no longer claimed, so one poison row cannot block the
	// relay forever. 0 = unlimited.
	maxAttempts := nonNegativeIntEnv(outboxMaxAttemptsEnv, defaultOutboxMaxAttempts, logger)
	relay := postgres.NewOutboxRelay(p.pool, sink, logger,
		postgres.WithInterval(durationEnv("OUTBOX_RELAY_INTERVAL", time.Second)),
		postgres.WithMaxAttempts(maxAttempts))
	logger.Info("event publisher configured", "publisher", "kafka", "mode", "outbox",
		"topics", []string{cloudevents.TopicLaborPerformanceAnalytics, cloudevents.TopicLaborPerformanceEvents}, "brokers", brokers,
		"outbox_max_attempts", maxAttempts)
	return postgres.NewOutboxPublisher(p.pool, analytics, integration), relay, func() {
		if err := sink.Close(); err != nil {
			logger.Error("error closing outbox relay sink", "error", err)
		}
		closePublishers()
	}
}

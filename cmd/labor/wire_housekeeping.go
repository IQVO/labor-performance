package main

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/labor-performance/internal/adapters/outbound/postgres"
)

// startHousekeeping wires the two background jobs that need the Postgres
// backing and returns the function that stops them cleanly (called via
// defer from run, so by LIFO order it runs BEFORE the deferred
// persistence.close() closes the pool):
//
//   - the housekeeping sweeper (ADR-0023): bounds idempotency_keys
//     (IDEMPOTENCY_KEY_TTL, default 24h) and PUBLISHED outbox_events
//     (OUTBOX_RETENTION, default 7d), sweeping every HOUSEKEEPING_INTERVAL
//     (default 1h; 0 disables).
//   - labor_performance.outbox.lag_seconds (ADR-0010's deferred outbox-lag
//     metric): an async gauge sampling the age of the oldest unpublished
//     outbox row.
func startHousekeeping(pool *pgxpool.Pool, logger *slog.Logger) func() {
	if reg, err := postgres.RegisterOutboxLagGauge(pool); err != nil {
		logger.Warn("outbox lag gauge unavailable", "error", err)
	} else {
		// Unregister now rather than in the returned stop func: the gauge
		// must stop sampling before the pool closes, and the sweeper's
		// own bounded stop below is the only thing that needs to WAIT.
		defer func() { _ = reg.Unregister() }()
	}

	done := make(chan struct{})
	ctx, stop := context.WithCancel(context.Background())
	sweeper := buildSweeper(pool, logger)
	go func() {
		defer close(done)
		if err := sweeper.Run(ctx); err != nil {
			logger.Error("housekeeping sweeper exited", "error", err)
		}
	}()
	return func() {
		stop()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			logger.Warn("housekeeping sweeper did not stop before the shutdown deadline")
		}
	}
}

// buildSweeper wires the housekeeping sweeper (ADR-0023) from the
// environment. Env vars are read HERE, in the composition root, never in
// the adapter. A 0/negative HOUSEKEEPING_INTERVAL disables the job; a
// 0/negative TTL disables the corresponding half. The values in effect are
// logged once at boot.
func buildSweeper(pool *pgxpool.Pool, logger *slog.Logger) *postgres.Sweeper {
	interval := housekeepingDurationEnv("HOUSEKEEPING_INTERVAL", postgres.DefaultSweepInterval)
	idempotencyTTL := housekeepingDurationEnv("IDEMPOTENCY_KEY_TTL", postgres.DefaultIdempotencyKeyTTL)
	outboxRetention := housekeepingDurationEnv("OUTBOX_RETENTION", postgres.DefaultOutboxRetention)
	logger.Info("housekeeping sweeper configured",
		"interval", interval.String(), "enabled", interval > 0,
		"idempotency_key_ttl", idempotencyTTL.String(), "outbox_retention", outboxRetention.String())
	return postgres.NewSweeper(pool,
		postgres.WithSweepInterval(interval),
		postgres.WithIdempotencyKeyTTL(idempotencyTTL),
		postgres.WithOutboxRetention(outboxRetention),
		postgres.WithSweeperLogger(logger),
	)
}

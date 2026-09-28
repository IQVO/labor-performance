// Package postgres provides pgxpool-backed implementations of the outbound
// ports, plus a golang-migrate runner for the SQL migrations in
// /migrations.
package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// MaxConns is the OLTP pool's per-process connection ceiling, shared by
// both cmd/labor (the api Deployment, HPA-scalable up to
// charts/labor-performance values.yaml's autoscaling.api.maxReplicas, 4)
// and cmd/mcp (the mcp Deployment, fixed replicaCount -- see that chart
// value's own doc comment for why it does not get an HPA).
//
// Sized against the fleet's shared Postgres instance's REAL
// max_connections (100, the Bitnami chart's own unmodified default --
// warehouse-infra's terraform/postgres.tf does not override it; verified
// live per order-management's ADR-0026, the reference this pool mirrors):
// at the api Deployment's HPA ceiling of 4 replicas, 4 * 10 = 40
// connections, ~40% of the instance-wide ceiling for this ONE of up to
// 10 fleet backend services' OLTP path alone -- deliberately leaving
// headroom for the other 9 services (and this service's own
// mcp/projector/reports processes) sharing the SAME Postgres instance.
// See docs/docs/adr/0019-horizontal-autoscaling-and-pgxpool-tuning.md for
// the full connection-budget accounting and the documented residual risk
// if every sibling service scales to its own ceiling at once.
//
// PgBouncer sits in front of this Postgres instance in transaction-
// pooling mode (warehouse-infra PR #43): every service's OLTP
// DATABASE_URL Secret is already re-pointed at PgBouncer, so this
// per-process MaxConns is this process's own logical ceiling, not a
// direct claim on a server-side backend connection per pgxpool
// connection -- PgBouncer is what absorbs the real server-side
// multiplexing across every fleet service's replicas. The number stays
// deliberately generous (matching order-management's OLTP MaxConns)
// rather than being shrunk just because a pooler now sits in front of
// it.
const MaxConns = 10

// StatementTimeout bounds how long a single query may hold a connection
// on the OLTP database before Postgres cancels it. labor-performance's
// OLTP queries (DefineStandard, GetStandard, RecordTaskPerformance, the
// scorecard/utilization reads) are all single-aggregate or small
// bounded-fan-out reads/writes keyed by id, normally low-single-digit
// milliseconds. 5s is generous headroom for real transient contention (a
// lock wait behind a concurrent writer) without ever being a normal-path
// concern, while bounding the absolute worst case tightly since this is
// the interactive, latency-sensitive path and also the pool with the
// most connections (40 at max HPA scale) to protect. Matches
// order-management's OLTP StatementTimeout exactly.
const StatementTimeout = "5s"

// NewPool opens a connection pool against databaseURL, with MaxConns and
// StatementTimeout applied to every connection.
func NewPool(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	return NewPoolWithLimits(ctx, databaseURL, MaxConns, StatementTimeout)
}

// NewPoolWithLimits is NewPool's shared implementation, taking maxConns
// and statementTimeout explicitly so an integration test can drive a
// much shorter timeout directly -- proving the AfterConnect hook really
// applies the setting to every new connection, by triggering an actual
// cancellation -- without waiting out the real production value.
// Production callers should use NewPool.
func NewPoolWithLimits(ctx context.Context, databaseURL string, maxConns int32, statementTimeout string) (*pgxpool.Pool, error) {
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, err
	}
	config.MaxConns = maxConns
	config.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		_, err := conn.Exec(ctx, fmt.Sprintf("SET statement_timeout = '%s'", statementTimeout))
		return err
	}
	return pgxpool.NewWithConfig(ctx, config)
}

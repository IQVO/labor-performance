package analyticsstore

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/labor-performance/internal/analytics/report"
	"github.com/claudioed/labor-performance/internal/pgtx"
)

// PostgresProjection is the WRITER implementation of
// report.ProjectionStore, backed by a pgxpool over the analytical database.
// Every Apply* runs in a transaction that first claims the event id in
// analytics_processed_events (ON CONFLICT DO NOTHING); it only mutates the
// rollup when the claim is new, making each apply idempotent per eventId
// under Kafka's at-least-once delivery. It is the only writer of the
// analytical database (ADR-0007).
type PostgresProjection struct {
	pool *pgxpool.Pool
}

// NewPostgresProjection constructs a PostgresProjection over pool.
func NewPostgresProjection(pool *pgxpool.Pool) *PostgresProjection {
	return &PostgresProjection{pool: pool}
}

// delta is the set of counter increments one applied event contributes to
// its rollup row. Every field is added to the existing row (or inserted as
// the row's initial value), so an apply is always a commutative +=: the
// order two different events arrive in cannot change the result.
type delta struct {
	tasksRecorded    int
	tasksScored      int
	efficiencyPctSum float64
	tasksMeasured    int
	actualSecondsSum int64
	standardsDefined int
	standardsRevised int
}

// claim inserts eventId into analytics_processed_events, returning true iff
// this call newly recorded it (so the caller should apply the effect). It
// runs inside tx so the claim and the effect commit atomically — a crash
// between them can only roll both back, never leave the counter moved with
// the event unclaimed (or vice versa).
func claim(ctx context.Context, tx pgx.Tx, eventId string, occurredAt time.Time) (bool, error) {
	tag, err := tx.Exec(ctx,
		`INSERT INTO analytics_processed_events (event_id, occurred_at)
		 VALUES ($1, $2) ON CONFLICT (event_id) DO NOTHING`,
		eventId, occurredAt)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}

// apply is the whole of every Apply* method: in one transaction, claim the
// eventId and — only if the claim is new — add d's increments to the
// (task_type, hour_bucket) row. bucketAt is the event's BUSINESS time (it
// chooses the bucket); occurredAt is its emission time (it feeds the
// freshness watermark).
//
// When ctx already carries a UnitOfWork transaction (the analytics
// consumer's atomic claim+apply) apply JOINS it and leaves commit/rollback
// to the owner; otherwise it opens and commits its own transaction.
func (p *PostgresProjection) apply(ctx context.Context, eventId, taskType string, bucketAt, occurredAt time.Time, d delta) error {
	if tx, ok := pgtx.TxFrom(ctx); ok {
		return applyOn(ctx, tx, eventId, taskType, bucketAt, occurredAt, d)
	}

	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
		}
	}()

	if err := applyOn(ctx, tx, eventId, taskType, bucketAt, occurredAt, d); err != nil {
		return err
	}

	committed = true
	return tx.Commit(ctx)
}

// applyOn claims eventId and, if new, upserts d into the rollup, all on tx.
func applyOn(ctx context.Context, tx pgx.Tx, eventId, taskType string, bucketAt, occurredAt time.Time, d delta) error {
	isNew, err := claim(ctx, tx, eventId, occurredAt)
	if err != nil {
		return fmt.Errorf("analyticsstore: claim event: %w", err)
	}
	if !isNew {
		return nil
	}

	if _, err := tx.Exec(ctx,
		`INSERT INTO labor_performance_rollup (
			task_type, hour_bucket,
			tasks_recorded, tasks_scored, efficiency_pct_sum,
			tasks_measured, actual_seconds_sum,
			standards_defined, standards_revised)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		 ON CONFLICT (task_type, hour_bucket) DO UPDATE SET
			tasks_recorded     = labor_performance_rollup.tasks_recorded     + EXCLUDED.tasks_recorded,
			tasks_scored       = labor_performance_rollup.tasks_scored       + EXCLUDED.tasks_scored,
			efficiency_pct_sum = labor_performance_rollup.efficiency_pct_sum + EXCLUDED.efficiency_pct_sum,
			tasks_measured     = labor_performance_rollup.tasks_measured     + EXCLUDED.tasks_measured,
			actual_seconds_sum = labor_performance_rollup.actual_seconds_sum + EXCLUDED.actual_seconds_sum,
			standards_defined  = labor_performance_rollup.standards_defined  + EXCLUDED.standards_defined,
			standards_revised  = labor_performance_rollup.standards_revised  + EXCLUDED.standards_revised`,
		report.NormalizeTaskType(taskType), report.HourBucket(bucketAt),
		d.tasksRecorded, d.tasksScored, d.efficiencyPctSum,
		d.tasksMeasured, d.actualSecondsSum,
		d.standardsDefined, d.standardsRevised,
	); err != nil {
		return fmt.Errorf("analyticsstore: upsert rollup: %w", err)
	}

	return nil
}

// ApplyTaskPerformanceRecorded folds one completed task into its
// (taskType, hour-of-CompletedAt) row. Idempotent on eventId.
//
// A task with no EfficiencyPct still increments tasks_recorded but not
// tasks_scored, and a task with a non-positive ActualSeconds still
// increments tasks_recorded but not tasks_measured — so an unscorable or
// unmeasurable task is counted as the real business fact it is without ever
// contributing to a mean.
func (p *PostgresProjection) ApplyTaskPerformanceRecorded(ctx context.Context, eventId string, f report.TaskPerformanceFact) error {
	d := delta{tasksRecorded: 1}
	if f.EfficiencyPct != nil {
		d.tasksScored = 1
		d.efficiencyPctSum = *f.EfficiencyPct
	}
	if f.ActualSeconds > 0 {
		d.tasksMeasured = 1
		d.actualSecondsSum = f.ActualSeconds
	}
	return p.apply(ctx, eventId, f.TaskType, f.CompletedAt, f.OccurredAt, d)
}

// ApplyLaborStandardDefined counts one first-ever standard for a TaskType,
// bucketed by when it took effect. Idempotent on eventId.
func (p *PostgresProjection) ApplyLaborStandardDefined(ctx context.Context, eventId string, f report.StandardFact) error {
	return p.apply(ctx, eventId, f.TaskType, f.EffectiveFrom, f.OccurredAt, delta{standardsDefined: 1})
}

// ApplyLaborStandardRevised counts one standard revision, which marks the
// bucket where efficiency numbers stop being comparable across the
// boundary. Idempotent on eventId.
func (p *PostgresProjection) ApplyLaborStandardRevised(ctx context.Context, eventId string, f report.StandardFact) error {
	return p.apply(ctx, eventId, f.TaskType, f.EffectiveFrom, f.OccurredAt, delta{standardsRevised: 1})
}

// Compile-time assertion that PostgresProjection satisfies the write port.
var _ report.ProjectionStore = (*PostgresProjection)(nil)

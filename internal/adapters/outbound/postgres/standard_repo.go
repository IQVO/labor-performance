package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/labor-performance/internal/application/ports"
	"github.com/claudioed/labor-performance/internal/domain/shared"
	"github.com/claudioed/labor-performance/internal/domain/standard"
)

// oneOpenStandardIndex is the partial unique index (migration 0006) that
// enforces "at most one open (effective_to IS NULL) standard per
// task_type" at the database level. A 23505 violation naming it is mapped
// to ports.ErrOpenStandardConflict.
const oneOpenStandardIndex = "idx_labor_standards_one_open_per_task_type"

// StandardRepo is a pgxpool-backed implementation of ports.StandardRepo.
type StandardRepo struct {
	pool *pgxpool.Pool
}

// NewStandardRepo constructs a StandardRepo over pool.
func NewStandardRepo(pool *pgxpool.Pool) *StandardRepo {
	return &StandardRepo{pool: pool}
}

// Save upserts s, version-guarded against a concurrent writer (ADR 0022):
// on an existing row the update only applies while the row's version still
// matches s.Version(), and always advances the version by exactly one.
// ports.ErrConcurrentModification is returned when the row exists but its
// version moved on — the caller must re-load, not blindly re-Save. A
// unique violation on the one-open-standard partial index maps to
// ports.ErrOpenStandardConflict.
//
// The guarded-update arm updates ONLY effective_to and version. A close
// (the one mutation DefineStandard performs on an existing record, ADR
// 0004) never rewrites ExpectedSeconds/EffectiveFrom — that is what keeps
// historically-frozen TaskPerformance rows accurate — and a Save of a
// still-open record is a no-op that must not disturb the frozen columns
// either.
func (r *StandardRepo) Save(ctx context.Context, s *standard.LaborStandard) error {
	tag, err := execGuarded(ctx, r.pool, `
		INSERT INTO labor_standards (id, task_type, expected_seconds, travel_component_seconds, effective_from, effective_to, version)
		VALUES ($1, $2, $3, $4, $5, $6, 1)
		ON CONFLICT (id) DO UPDATE
		  SET effective_to = EXCLUDED.effective_to,
		      version = labor_standards.version + 1
		WHERE labor_standards.version = $7
	`, string(s.ID()), string(s.TaskType()), s.ExpectedSeconds(), s.TravelComponentSeconds(), s.EffectiveFrom(), s.EffectiveTo(), s.Version())
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" && pgErr.ConstraintName == oneOpenStandardIndex {
			return ports.ErrOpenStandardConflict
		}
		return err
	}
	if tag.RowsAffected() == 0 {
		// The row exists (this is the ON CONFLICT arm; a plain INSERT of a
		// fresh id always affects exactly 1 row) but its version no longer
		// matches what s was loaded at.
		return ports.ErrConcurrentModification
	}
	return nil
}

// execGuarded runs one write statement. When ctx carries a transaction
// (the idempotency middleware's, or a UnitOfWork's) the statement runs
// inside a SAVEPOINT: a failure — notably the 23505 on the one-open-standard
// index — rolls back to the savepoint only, leaving the surrounding
// transaction usable. Without it Postgres aborts the whole transaction
// (SQLSTATE 25P02), so the caller's later statements (the middleware's
// UPDATE idempotency_keys recording the 409) fail and the request becomes
// a 500. Outside a transaction the statement runs on the pool as before.
func execGuarded(ctx context.Context, pool *pgxpool.Pool, sql string, args ...any) (pgconn.CommandTag, error) {
	tx, ok := txFrom(ctx)
	if !ok {
		return pool.Exec(ctx, sql, args...)
	}
	sp, err := tx.Begin(ctx) // pgx nests a transaction as a savepoint
	if err != nil {
		return pgconn.CommandTag{}, err
	}
	tag, err := sp.Exec(ctx, sql, args...)
	if err != nil {
		if rbErr := sp.Rollback(ctx); rbErr != nil {
			err = errors.Join(err, rbErr)
		}
		return pgconn.CommandTag{}, err
	}
	return tag, sp.Commit(ctx)
}

func (r *StandardRepo) FindActiveAsOf(ctx context.Context, taskType shared.TaskType, t time.Time) (*standard.LaborStandard, error) {
	row := querierFrom(ctx, r.pool).QueryRow(ctx, `
		SELECT id, task_type, expected_seconds, travel_component_seconds, effective_from, effective_to, version
		FROM labor_standards
		WHERE task_type = $1
		  AND effective_from <= $2
		  AND (effective_to IS NULL OR effective_to > $2)
	`, string(taskType), t)
	return scanStandard(row)
}

func (r *StandardRepo) FindCurrentlyActive(ctx context.Context, taskType shared.TaskType) (*standard.LaborStandard, error) {
	row := querierFrom(ctx, r.pool).QueryRow(ctx, `
		SELECT id, task_type, expected_seconds, travel_component_seconds, effective_from, effective_to, version
		FROM labor_standards
		WHERE task_type = $1 AND effective_to IS NULL
	`, string(taskType))
	return scanStandard(row)
}

func scanStandard(row pgx.Row) (*standard.LaborStandard, error) {
	var (
		id                     string
		taskType               string
		expectedSeconds        int64
		travelComponentSeconds *int64
		effectiveFrom          time.Time
		effectiveTo            *time.Time
		version                int
	)
	err := row.Scan(&id, &taskType, &expectedSeconds, &travelComponentSeconds, &effectiveFrom, &effectiveTo, &version)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return standard.Rehydrate(shared.StandardId(id), shared.TaskType(taskType), expectedSeconds, travelComponentSeconds, effectiveFrom, effectiveTo, version), nil
}

// NextID mints a standard id.
func (r *StandardRepo) NextID(_ context.Context) (shared.StandardId, error) {
	return shared.StandardId("std-" + newUUID()), nil
}

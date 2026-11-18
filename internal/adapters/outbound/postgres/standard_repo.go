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

// oneOpenStandardIndex is the partial unique index (migration 0006,
// widened to (task_type, handling_unit) by migration 0007 and to
// (task_type, handling_unit, site_code) by migration 0008) that enforces
// "at most one open (effective_to IS NULL) standard per scope" at the
// database level. A 23505 violation naming it is mapped
// to ports.ErrOpenStandardConflict.
const oneOpenStandardIndex = "idx_labor_standards_one_open_per_task_type"

// standardColumns is the column list every standard read selects, in the
// order scanStandard scans it.
const standardColumns = `id, task_type, expected_seconds, travel_component_seconds, effective_from, effective_to, version,
		       handling_unit, team_lift_threshold_g, team_lift_expected_seconds, site_code`

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
		INSERT INTO labor_standards (id, task_type, expected_seconds, travel_component_seconds, effective_from, effective_to, version, handling_unit, team_lift_threshold_g, team_lift_expected_seconds, site_code)
		VALUES ($1, $2, $3, $4, $5, $6, 1, $8, $9, $10, $11)
		ON CONFLICT (id) DO UPDATE
		  SET effective_to = EXCLUDED.effective_to,
		      version = labor_standards.version + 1
		WHERE labor_standards.version = $7
	`, string(s.ID()), string(s.TaskType()), s.ExpectedSeconds(), s.TravelComponentSeconds(), s.EffectiveFrom(), s.EffectiveTo(), s.Version(),
		string(s.HandlingUnit()), teamLiftThreshold(s), teamLiftSeconds(s), string(s.SiteCode()))
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

// teamLiftThreshold and teamLiftSeconds flatten the optional team lift into
// the two nullable columns that persist it.
func teamLiftThreshold(s *standard.LaborStandard) *int64 {
	if tl := s.TeamLift(); tl != nil {
		v := tl.ThresholdG()
		return &v
	}
	return nil
}

func teamLiftSeconds(s *standard.LaborStandard) *int64 {
	if tl := s.TeamLift(); tl != nil {
		v := tl.ExpectedSeconds()
		return &v
	}
	return nil
}

// FindActiveAsOf resolves the task-type-only, site-less standard
// (handling_unit = ”, site_code = ”).
func (r *StandardRepo) FindActiveAsOf(ctx context.Context, taskType shared.TaskType, t time.Time) (*standard.LaborStandard, error) {
	return r.FindActiveAsOfForUnit(ctx, taskType, "", t)
}

// FindActiveAsOfForUnit resolves the SITE-LESS standard of (taskType, unit)
// active at t. A site's standard is never returned: scoring, the only
// caller, stays on the site-less standards (ADR 0038 decision 3).
func (r *StandardRepo) FindActiveAsOfForUnit(ctx context.Context, taskType shared.TaskType, unit shared.HandlingUnit, t time.Time) (*standard.LaborStandard, error) {
	row := querierFrom(ctx, r.pool).QueryRow(ctx, `
		SELECT `+standardColumns+`
		FROM labor_standards
		WHERE task_type = $1
		  AND handling_unit = $3
		  AND site_code = ''
		  AND effective_from <= $2
		  AND (effective_to IS NULL OR effective_to > $2)
	`, string(taskType), t, string(unit))
	return scanStandard(row)
}

// FindCurrentlyActive resolves the task-type-only, site-less open standard.
func (r *StandardRepo) FindCurrentlyActive(ctx context.Context, taskType shared.TaskType) (*standard.LaborStandard, error) {
	return r.FindCurrentlyActiveForUnit(ctx, taskType, "")
}

// FindCurrentlyActiveForUnit resolves the site-less open standard of
// exactly (taskType, unit).
func (r *StandardRepo) FindCurrentlyActiveForUnit(ctx context.Context, taskType shared.TaskType, unit shared.HandlingUnit) (*standard.LaborStandard, error) {
	return r.FindCurrentlyActiveForScope(ctx, standard.Scope{TaskType: taskType, HandlingUnit: unit})
}

// FindCurrentlyActiveForScope resolves the open standard of exactly scope
// (task type, handling unit, site code); no fallback to another scope.
func (r *StandardRepo) FindCurrentlyActiveForScope(ctx context.Context, scope standard.Scope) (*standard.LaborStandard, error) {
	row := querierFrom(ctx, r.pool).QueryRow(ctx, `
		SELECT `+standardColumns+`
		FROM labor_standards
		WHERE task_type = $1 AND handling_unit = $2 AND site_code = $3 AND effective_to IS NULL
	`, string(scope.TaskType), string(scope.HandlingUnit), string(scope.SiteCode))
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
		handlingUnit           string
		thresholdG             *int64
		teamLiftSeconds        *int64
		siteCode               string
	)
	err := row.Scan(&id, &taskType, &expectedSeconds, &travelComponentSeconds, &effectiveFrom, &effectiveTo, &version,
		&handlingUnit, &thresholdG, &teamLiftSeconds, &siteCode)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var teamLift *standard.TeamLift
	if thresholdG != nil && teamLiftSeconds != nil {
		// The CHECK constraints of migration 0007 guarantee both are
		// set together and positive, so NewTeamLift cannot fail here.
		tl, err := standard.NewTeamLift(*thresholdG, *teamLiftSeconds)
		if err != nil {
			return nil, err
		}
		teamLift = &tl
	}
	scope := standard.Scope{TaskType: shared.TaskType(taskType), HandlingUnit: shared.HandlingUnit(handlingUnit), SiteCode: shared.SiteCode(siteCode)}
	return standard.RehydrateForScope(shared.StandardId(id), scope, expectedSeconds, travelComponentSeconds, teamLift, effectiveFrom, effectiveTo, version), nil
}

// NextID mints a standard id.
func (r *StandardRepo) NextID(_ context.Context) (shared.StandardId, error) {
	return shared.StandardId("std-" + newUUID()), nil
}

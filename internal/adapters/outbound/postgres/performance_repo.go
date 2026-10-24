package postgres

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/labor-performance/internal/application/ports"
	"github.com/claudioed/labor-performance/internal/domain/performance"
	"github.com/claudioed/labor-performance/internal/domain/shared"
)

// PerformanceRepo is a pgxpool-backed implementation of
// ports.PerformanceRepo.
type PerformanceRepo struct {
	pool *pgxpool.Pool
}

// NewPerformanceRepo constructs a PerformanceRepo over pool.
func NewPerformanceRepo(pool *pgxpool.Pool) *PerformanceRepo {
	return &PerformanceRepo{pool: pool}
}

func (r *PerformanceRepo) Save(ctx context.Context, p *performance.TaskPerformance) error {
	_, err := querierFrom(ctx, r.pool).Exec(ctx, `
		INSERT INTO task_performances
			(event_id, task_id, associate_id, task_type, actual_seconds, standard_seconds_at_completion, efficiency_pct, completed_at, handling_unit, team_lift)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (event_id) DO NOTHING
	`, p.EventId(), p.TaskId(), string(p.AssociateId()), string(p.TaskType()), p.ActualSeconds(), p.StandardSecondsAtCompletion(), p.EfficiencyPct(), p.CompletedAt(), string(p.HandlingUnit()), p.TeamLift())
	return err
}

func (r *PerformanceRepo) ExistsByAssociateID(ctx context.Context, associateId shared.AssociateId) (bool, error) {
	var exists bool
	err := querierFrom(ctx, r.pool).QueryRow(ctx, `
		SELECT EXISTS(SELECT 1 FROM task_performances WHERE associate_id = $1)
	`, string(associateId)).Scan(&exists)
	return exists, err
}

func (r *PerformanceRepo) ScorecardFor(ctx context.Context, associateId shared.AssociateId) (ports.Scorecard, error) {
	sc := ports.Scorecard{AssociateId: associateId, ByTaskType: make(map[shared.TaskType]ports.TaskTypeBreakdown)}

	row := querierFrom(ctx, r.pool).QueryRow(ctx, `
		SELECT COUNT(*), AVG(efficiency_pct)
		FROM task_performances
		WHERE associate_id = $1
	`, string(associateId))
	var mean *float64
	if err := row.Scan(&sc.TaskCount, &mean); err != nil {
		return ports.Scorecard{}, err
	}
	sc.MeanEfficiencyPct = mean

	rows, err := querierFrom(ctx, r.pool).Query(ctx, `
		SELECT task_type, COUNT(*), AVG(efficiency_pct)
		FROM task_performances
		WHERE associate_id = $1
		GROUP BY task_type
	`, string(associateId))
	if err != nil {
		return ports.Scorecard{}, err
	}
	defer rows.Close()

	for rows.Next() {
		var taskType string
		var breakdown ports.TaskTypeBreakdown
		if err := rows.Scan(&taskType, &breakdown.TaskCount, &breakdown.MeanEfficiencyPct); err != nil {
			return ports.Scorecard{}, err
		}
		sc.ByTaskType[shared.TaskType(taskType)] = breakdown
	}
	if err := rows.Err(); err != nil {
		return ports.Scorecard{}, err
	}

	return sc, nil
}

func (r *PerformanceRepo) TaskTypePerformanceFor(ctx context.Context, taskType shared.TaskType) (ports.TaskTypePerformance, error) {
	out := ports.TaskTypePerformance{TaskType: taskType}
	err := querierFrom(ctx, r.pool).QueryRow(ctx, `
		SELECT COUNT(*), AVG(efficiency_pct), AVG(actual_seconds) FILTER (WHERE actual_seconds > 0)
		FROM task_performances
		WHERE task_type = $1
	`, string(taskType)).Scan(&out.TaskCount, &out.MeanEfficiencyPct, &out.MeanActualSeconds)
	return out, err
}

// RecentByAssociateID returns associateId's most recent rows, newest
// first (ORDER BY completed_at DESC), capped at limit.
func (r *PerformanceRepo) RecentByAssociateID(ctx context.Context, associateId shared.AssociateId, limit int) ([]*performance.TaskPerformance, error) {
	rows, err := querierFrom(ctx, r.pool).Query(ctx, `
		SELECT event_id, task_id, associate_id, task_type, actual_seconds, standard_seconds_at_completion, efficiency_pct, completed_at, handling_unit, team_lift
		FROM task_performances
		WHERE associate_id = $1
		ORDER BY completed_at DESC
		LIMIT $2
	`, string(associateId), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]*performance.TaskPerformance, 0, limit)
	for rows.Next() {
		var eventId, taskId, assocId, taskType, handlingUnit string
		var teamLift bool
		var actualSeconds, standardSeconds int64
		var efficiencyPct *float64
		var completedAt time.Time
		if err := rows.Scan(&eventId, &taskId, &assocId, &taskType, &actualSeconds, &standardSeconds, &efficiencyPct, &completedAt, &handlingUnit, &teamLift); err != nil {
			return nil, err
		}
		out = append(out, performance.RehydrateWithHandling(
			eventId, taskId, shared.AssociateId(assocId), shared.TaskType(taskType),
			shared.HandlingUnit(handlingUnit), teamLift,
			actualSeconds, standardSeconds, efficiencyPct, completedAt,
		))
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// SumActualSecondsByTaskType returns the total ActualSeconds across rows
// for taskType whose CompletedAt is on or after since.
func (r *PerformanceRepo) SumActualSecondsByTaskType(ctx context.Context, taskType shared.TaskType, since time.Time) (int64, error) {
	var sum int64
	err := querierFrom(ctx, r.pool).QueryRow(ctx, `
		SELECT COALESCE(SUM(actual_seconds), 0)
		FROM task_performances
		WHERE task_type = $1 AND completed_at >= $2
	`, string(taskType), since).Scan(&sum)
	return sum, err
}

// SumActualSecondsByAssociate is SumActualSecondsByTaskType's
// per-associate counterpart.
func (r *PerformanceRepo) SumActualSecondsByAssociate(ctx context.Context, associateId shared.AssociateId, since time.Time) (int64, error) {
	var sum int64
	err := querierFrom(ctx, r.pool).QueryRow(ctx, `
		SELECT COALESCE(SUM(actual_seconds), 0)
		FROM task_performances
		WHERE associate_id = $1 AND completed_at >= $2
	`, string(associateId), since).Scan(&sum)
	return sum, err
}

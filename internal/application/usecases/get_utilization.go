package usecases

import (
	"context"
	"time"

	"github.com/claudioed/labor-performance/internal/application/ports"
	"github.com/claudioed/labor-performance/internal/domain/idleness"
	"github.com/claudioed/labor-performance/internal/domain/performance"
	"github.com/claudioed/labor-performance/internal/domain/shared"
)

// defaultUtilizationWindow is the window GetUtilization applies when the
// caller does not supply one (REST/MCP both default `?window=` to 1h).
const defaultUtilizationWindow = time.Hour

// UtilizationResult is the read model returned by GetUtilization: task
// time vs. idle time for a subject (either a TaskType, fleet-wide, or one
// associate) over a trailing window, plus an OPEN GAP contribution for
// whichever associate(s) are idle RIGHT NOW (see the idleness ADR's
// "Trailing idleness" limitation — an open gap is a read-time
// computation, never persisted).
type UtilizationResult struct {
	// TaskType is set only for a task-type-scoped result; empty for a
	// per-associate result.
	TaskType shared.TaskType
	// AssociateId is set only for a per-associate-scoped result; empty
	// for a task-type-scoped result.
	AssociateId shared.AssociateId
	// Associates is the distinct associate count contributing to this
	// result. Always 1 for a per-associate result; for a task-type
	// result it is the distinct-associate count over the window (0 for
	// a never-observed task type).
	Associates int
	// WindowSeconds is the requested window's length in seconds.
	WindowSeconds int64
	// TaskSeconds is the summed ActualSeconds over the window.
	TaskSeconds int64
	// IdleSeconds is the summed recorded (closed) idle-gap seconds over
	// the window — never includes the OpenGapSeconds contribution.
	IdleSeconds int64
	// OpenGapSeconds is the still-running idle time for a subject idle
	// right now: now - lastCompletedAt (or last idle-gap EndedAt),
	// clamped to the window. Zero when the subject is not currently
	// idle, or the last-known activity predates the window. Only
	// ForAssociate computes it; ForTaskType always leaves it 0 (an open
	// gap has no task type until the associate's next claim ends it, and
	// ADR 0014 defines it per associate).
	OpenGapSeconds int64
	// UtilizationPct is 100 * TaskSeconds / (TaskSeconds + IdleSeconds +
	// OpenGapSeconds), nil when there was nothing to compute a share of
	// — never a fabricated number.
	UtilizationPct *float64
}

// GetUtilization answers "how idle vs. busy has this subject been over
// the last window" — per TaskType (fleet-wide) or per associate. It
// composes the existing PerformanceRepo (task time) with the new
// IdlePeriodRepo (idle time), the SAME two repos RecordTaskPerformance
// already writes through, so the read side never diverges from what was
// actually recorded.
type GetUtilization struct {
	Performances ports.PerformanceRepo
	IdlePeriods  ports.IdlePeriodRepo
	Clock        ports.Clock
}

// ForTaskType computes UtilizationResult for taskType over window
// (defaultUtilizationWindow when window<=0). It reports recorded (closed)
// idle gaps only; OpenGapSeconds is always 0 — see UtilizationResult.
func (uc *GetUtilization) ForTaskType(ctx context.Context, taskType shared.TaskType, window time.Duration) (UtilizationResult, error) {
	window = resolveWindow(window)
	now := uc.now()
	since := now.Add(-window)

	taskSeconds, err := uc.Performances.SumActualSecondsByTaskType(ctx, taskType, since)
	if err != nil {
		return UtilizationResult{}, err
	}
	idleSeconds, _, err := uc.IdlePeriods.SumByTaskType(ctx, taskType, since)
	if err != nil {
		return UtilizationResult{}, err
	}
	associates, err := uc.IdlePeriods.DistinctAssociatesByTaskType(ctx, taskType, since)
	if err != nil {
		return UtilizationResult{}, err
	}

	return UtilizationResult{
		TaskType:       taskType,
		Associates:     associates,
		WindowSeconds:  int64(window.Seconds()),
		TaskSeconds:    taskSeconds,
		IdleSeconds:    idleSeconds,
		UtilizationPct: idleness.UtilizationPct(taskSeconds, idleSeconds),
	}, nil
}

// ForAssociate computes UtilizationResult for one associate over window
// (defaultUtilizationWindow when window<=0), including that associate's
// OPEN GAP contribution when they are idle right now (see the idleness
// ADR's "Trailing idleness" limitation): the read-time gap from their
// last recorded activity (whichever of last completion or last idle-gap
// end is more recent) to now, clamped to the window and never persisted.
func (uc *GetUtilization) ForAssociate(ctx context.Context, associateId shared.AssociateId, window time.Duration) (UtilizationResult, error) {
	window = resolveWindow(window)
	now := uc.now()
	since := now.Add(-window)

	taskSeconds, err := uc.Performances.SumActualSecondsByAssociate(ctx, associateId, since)
	if err != nil {
		return UtilizationResult{}, err
	}
	idleSeconds, _, err := uc.IdlePeriods.SumByAssociate(ctx, associateId, since)
	if err != nil {
		return UtilizationResult{}, err
	}

	lastCompleted, err := uc.Performances.RecentByAssociateID(ctx, associateId, 1)
	if err != nil {
		return UtilizationResult{}, err
	}
	lastIdleEnd, err := uc.IdlePeriods.LastEndedAtByAssociate(ctx, associateId)
	if err != nil {
		return UtilizationResult{}, err
	}

	openGapSeconds := openGapSeconds(lastActivity(lastCompleted, lastIdleEnd), now, since)

	return UtilizationResult{
		AssociateId:    associateId,
		Associates:     associateCountFor(taskSeconds, idleSeconds, openGapSeconds),
		WindowSeconds:  int64(window.Seconds()),
		TaskSeconds:    taskSeconds,
		IdleSeconds:    idleSeconds,
		OpenGapSeconds: openGapSeconds,
		UtilizationPct: idleness.UtilizationPct(taskSeconds, idleSeconds+openGapSeconds),
	}, nil
}

// resolveWindow applies the default when window is non-positive.
func resolveWindow(window time.Duration) time.Duration {
	if window <= 0 {
		return defaultUtilizationWindow
	}
	return window
}

func (uc *GetUtilization) now() time.Time {
	if uc.Clock == nil {
		return time.Now()
	}
	return uc.Clock.Now()
}

// lastActivity resolves the most recent of a last-completion row (if
// any) and a last-idle-gap end instant, or the zero time if neither
// exists.
func lastActivity(lastCompleted []*performance.TaskPerformance, lastIdleEnd time.Time) time.Time {
	var last time.Time
	if len(lastCompleted) > 0 {
		last = lastCompleted[0].CompletedAt()
	}
	if lastIdleEnd.After(last) {
		last = lastIdleEnd
	}
	return last
}

// openGapSeconds computes the still-running idle contribution: now minus
// lastActivity, clamped to the window (never before since) and floored
// at zero (a lastActivity in the future, or no lastActivity at all,
// contributes nothing).
func openGapSeconds(lastActivity, now, since time.Time) int64 {
	if lastActivity.IsZero() || !lastActivity.Before(now) {
		return 0
	}
	if lastActivity.Before(since) {
		lastActivity = since
	}
	return int64(now.Sub(lastActivity).Seconds())
}

// associateCountFor reports 1 whenever this associate contributed
// anything observable to the window, 0 for a never-observed associate —
// mirrors GetTaskTypePerformance's "no fabricated non-zero result"
// discipline for an unseen subject.
func associateCountFor(taskSeconds, idleSeconds, openGapSeconds int64) int {
	if taskSeconds > 0 || idleSeconds > 0 || openGapSeconds > 0 {
		return 1
	}
	return 0
}

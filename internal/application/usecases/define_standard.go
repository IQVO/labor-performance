package usecases

import (
	"context"
	"time"

	"github.com/claudioed/labor-performance/internal/application/ports"
	"github.com/claudioed/labor-performance/internal/domain/shared"
	"github.com/claudioed/labor-performance/internal/domain/standard"
)

// DefineStandard defines (or revises) the engineered labor standard for a
// TaskType. If a standard is already active for that TaskType, this use
// case closes it (ends its effective range at "now" — the same instant
// the new one begins) rather than overwriting it in place, so a past
// TaskPerformance's frozen StandardSecondsAtCompletion stays historically
// accurate. See ADR 0004-standard-frozen-at-completion-time-not-recomputed.md.
type DefineStandard struct {
	Standards ports.StandardRepo
	Events    ports.EventPublisher
	Clock     ports.Clock
	// UnitOfWork brackets the Save(s) + Publish atomically (ADR 0010).
	// Optional: a nil value means "no transactional backing" and the
	// calls run back to back, which is exactly the in-memory /
	// log-publisher dev configuration.
	UnitOfWork ports.UnitOfWork
	// Metrics records the labor_performance.standards.defined business
	// counter (fleet-standard-metrics ADR, Tier 2), split by outcome
	// (accepted/rejected). Nil is a valid "not instrumented" value.
	Metrics ports.StandardMetrics
}

func (uc *DefineStandard) Execute(ctx context.Context, taskType shared.TaskType, expectedSeconds int64, travelComponentSeconds *int64) (*standard.LaborStandard, error) {
	return uc.ExecuteSpec(ctx, DefineStandardSpec{TaskType: taskType, ExpectedSeconds: expectedSeconds, TravelComponentSeconds: travelComponentSeconds})
}

// TeamLiftSpec is the declared heavy-case rule of a CASE standard: a case
// heavier than ThresholdG grams is a team lift scored against
// ExpectedSeconds instead of the standard's own (ADR 0035).
type TeamLiftSpec struct {
	ThresholdG      int64
	ExpectedSeconds int64
}

// DefineStandardSpec is everything DefineStandard can declare. HandlingUnit
// "" declares the task-type-only standard (what Execute always did); EACH,
// CASE or PALLET declares the more specific standard for that unit, which
// wins over the task-type-only one when a completed task carries that unit.
// TeamLift is optional and only valid for CASE.
type DefineStandardSpec struct {
	TaskType               shared.TaskType
	HandlingUnit           shared.HandlingUnit
	ExpectedSeconds        int64
	TravelComponentSeconds *int64
	TeamLift               *TeamLiftSpec
}

// ExecuteSpec defines (or revises) the standard for (spec.TaskType,
// spec.HandlingUnit): the prior open standard of the SAME pair, if any, is
// closed; standards of other handling units are untouched.
func (uc *DefineStandard) ExecuteSpec(ctx context.Context, spec DefineStandardSpec) (*standard.LaborStandard, error) {
	taskType, expectedSeconds, travelComponentSeconds := spec.TaskType, spec.ExpectedSeconds, spec.TravelComponentSeconds
	now := uc.Clock.Now()

	prior, err := uc.currentlyActive(ctx, taskType, spec.HandlingUnit)
	if err != nil {
		return nil, err
	}

	id, err := uc.Standards.NextID(ctx)
	if err != nil {
		return nil, err
	}

	teamLift, err := uc.buildTeamLift(ctx, spec)
	if err != nil {
		return nil, err
	}

	next, err := standard.NewForUnit(id, taskType, spec.HandlingUnit, expectedSeconds, travelComponentSeconds, teamLift, now)
	if err != nil {
		uc.rejected(ctx)
		return nil, err
	}

	// Closing the prior standard, saving the new one and publishing the
	// event are one business fact ("the standard changed") and must
	// commit together or not at all.
	err = atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
		if prior != nil {
			prior.Close(now)
			if err := uc.Standards.Save(ctx, prior); err != nil {
				return err
			}
		}

		if err := uc.Standards.Save(ctx, next); err != nil {
			return err
		}

		return uc.publish(ctx, spec, id, prior, teamLift, now)
	})
	if err != nil {
		return nil, err
	}

	if uc.Metrics != nil {
		uc.Metrics.StandardDefinitionAccepted(ctx)
	}

	return next, nil
}

// currentlyActive loads the open standard of exactly (taskType, unit); the
// task-type-only case goes through the original FindCurrentlyActive so the
// pre-ADR-0035 path is unchanged.
func (uc *DefineStandard) currentlyActive(ctx context.Context, taskType shared.TaskType, unit shared.HandlingUnit) (*standard.LaborStandard, error) {
	if unit == "" {
		return uc.Standards.FindCurrentlyActive(ctx, taskType)
	}
	return uc.Standards.FindCurrentlyActiveForUnit(ctx, taskType, unit)
}

// atomically runs fn inside uow when one is wired, or directly otherwise.
// Keeping this in one place means every use case treats a nil UnitOfWork
// identically instead of each re-deciding the fallback.
func atomically(ctx context.Context, uow ports.UnitOfWork, fn func(ctx context.Context) error) error {
	if uow == nil {
		return fn(ctx)
	}
	return uow.Execute(ctx, fn)
}

// rejected counts a refused definition when metrics are wired.
func (uc *DefineStandard) rejected(ctx context.Context) {
	if uc.Metrics != nil {
		uc.Metrics.StandardDefinitionRejected(ctx)
	}
}

// buildTeamLift validates the optional team-lift declaration of a spec.
func (uc *DefineStandard) buildTeamLift(ctx context.Context, spec DefineStandardSpec) (*standard.TeamLift, error) {
	if spec.TeamLift == nil {
		return nil, nil
	}
	tl, err := standard.NewTeamLift(spec.TeamLift.ThresholdG, spec.TeamLift.ExpectedSeconds)
	if err != nil {
		uc.rejected(ctx)
		return nil, err
	}
	return &tl, nil
}

// publish emits Revised when a prior standard of the same pair was closed,
// Defined otherwise, both carrying the handling unit and team lift.
func (uc *DefineStandard) publish(ctx context.Context, spec DefineStandardSpec, id shared.StandardId, prior *standard.LaborStandard, teamLift *standard.TeamLift, now time.Time) error {
	var thresholdG, teamLiftSeconds *int64
	if teamLift != nil {
		t, s := teamLift.ThresholdG(), teamLift.ExpectedSeconds()
		thresholdG, teamLiftSeconds = &t, &s
	}
	if prior != nil {
		return uc.Events.Publish(ctx, shared.NewLaborStandardRevised(now, id, spec.TaskType, prior.ExpectedSeconds(), spec.ExpectedSeconds, spec.TravelComponentSeconds, now).
			WithHandling(spec.HandlingUnit, thresholdG, teamLiftSeconds))
	}
	return uc.Events.Publish(ctx, shared.NewLaborStandardDefined(now, id, spec.TaskType, spec.ExpectedSeconds, spec.TravelComponentSeconds, now).
		WithHandling(spec.HandlingUnit, thresholdG, teamLiftSeconds))
}

package usecases

import (
	"context"

	"github.com/claudioed/labor-performance/internal/application/ports"
	"github.com/claudioed/labor-performance/internal/domain/shared"
	"github.com/claudioed/labor-performance/internal/domain/standard"
)

// GetStandard returns the currently-active LaborStandard for taskType, or
// ErrStandardNotFound if none is active.
type GetStandard struct {
	Standards ports.StandardRepo
}

func (uc *GetStandard) Execute(ctx context.Context, taskType shared.TaskType) (*standard.LaborStandard, error) {
	s, err := uc.Standards.FindCurrentlyActive(ctx, taskType)
	if err != nil {
		return nil, err
	}
	if s == nil {
		return nil, ErrStandardNotFound
	}
	return s, nil
}

// ExecuteResolved returns the standard that APPLIES to a (taskType, unit)
// handle: the standard declared for exactly that handling unit when one is
// active, otherwise the task-type-only standard (most specific wins, ADR
// 0035). unit "" is Execute. The returned standard's own HandlingUnit()
// tells the caller which of the two answered. ErrStandardNotFound only when
// neither exists.
func (uc *GetStandard) ExecuteResolved(ctx context.Context, taskType shared.TaskType, unit shared.HandlingUnit) (*standard.LaborStandard, error) {
	if unit != "" {
		specific, err := uc.Standards.FindCurrentlyActiveForUnit(ctx, taskType, unit)
		if err != nil {
			return nil, err
		}
		if specific != nil {
			return specific, nil
		}
	}
	return uc.Execute(ctx, taskType)
}

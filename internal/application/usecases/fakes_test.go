package usecases_test

import (
	"context"
	"errors"
	"time"

	"github.com/claudioed/labor-performance/internal/application/ports"
	"github.com/claudioed/labor-performance/internal/domain/idleness"
	"github.com/claudioed/labor-performance/internal/domain/performance"
	"github.com/claudioed/labor-performance/internal/domain/shared"
	"github.com/claudioed/labor-performance/internal/domain/standard"
)

// errUnmapped is a generic infrastructure failure used to exercise a use
// case's error-propagation paths (repo/publisher failures), which the
// happy-path in-memory adapters never produce on their own.
var errUnmapped = errors.New("unmapped: simulated infrastructure failure")

// failingStandardRepo wraps a real ports.StandardRepo and can be told to
// fail on specific calls, to exercise DefineStandard/GetStandard/
// RecordTaskPerformance error-propagation paths.
type failingStandardRepo struct {
	ports.StandardRepo
	failSave           bool
	failFindActiveAsOf bool
	failFindCurrent    bool
	failNextID         bool
}

func (f *failingStandardRepo) Save(ctx context.Context, s *standard.LaborStandard) error {
	if f.failSave {
		return errUnmapped
	}
	return f.StandardRepo.Save(ctx, s)
}

func (f *failingStandardRepo) FindActiveAsOf(ctx context.Context, taskType shared.TaskType, t time.Time) (*standard.LaborStandard, error) {
	if f.failFindActiveAsOf {
		return nil, errUnmapped
	}
	return f.StandardRepo.FindActiveAsOf(ctx, taskType, t)
}

func (f *failingStandardRepo) FindCurrentlyActive(ctx context.Context, taskType shared.TaskType) (*standard.LaborStandard, error) {
	if f.failFindCurrent {
		return nil, errUnmapped
	}
	return f.StandardRepo.FindCurrentlyActive(ctx, taskType)
}

func (f *failingStandardRepo) NextID(ctx context.Context) (shared.StandardId, error) {
	if f.failNextID {
		return "", errUnmapped
	}
	return f.StandardRepo.NextID(ctx)
}

// failingPerformanceRepo wraps a real ports.PerformanceRepo and can be
// told to fail Save, RecentByAssociateID, ExistsByAssociateID,
// ScorecardFor, SumActualSecondsByTaskType or SumActualSecondsByAssociate,
// to exercise RecordTaskPerformance's, GetAssociateScorecard's and
// GetUtilization's error paths.
type failingPerformanceRepo struct {
	ports.PerformanceRepo
	failSave         bool
	failRecent       bool
	failExists       bool
	failScorecard    bool
	failSumTaskType  bool
	failSumAssociate bool
}

func (f *failingPerformanceRepo) Save(ctx context.Context, p *performance.TaskPerformance) error {
	if f.failSave {
		return errUnmapped
	}
	return f.PerformanceRepo.Save(ctx, p)
}

func (f *failingPerformanceRepo) RecentByAssociateID(ctx context.Context, associateId shared.AssociateId, limit int) ([]*performance.TaskPerformance, error) {
	if f.failRecent {
		return nil, errUnmapped
	}
	return f.PerformanceRepo.RecentByAssociateID(ctx, associateId, limit)
}

func (f *failingPerformanceRepo) ExistsByAssociateID(ctx context.Context, associateId shared.AssociateId) (bool, error) {
	if f.failExists {
		return false, errUnmapped
	}
	return f.PerformanceRepo.ExistsByAssociateID(ctx, associateId)
}

func (f *failingPerformanceRepo) ScorecardFor(ctx context.Context, associateId shared.AssociateId) (ports.Scorecard, error) {
	if f.failScorecard {
		return ports.Scorecard{}, errUnmapped
	}
	return f.PerformanceRepo.ScorecardFor(ctx, associateId)
}

func (f *failingPerformanceRepo) SumActualSecondsByTaskType(ctx context.Context, taskType shared.TaskType, since time.Time) (int64, error) {
	if f.failSumTaskType {
		return 0, errUnmapped
	}
	return f.PerformanceRepo.SumActualSecondsByTaskType(ctx, taskType, since)
}

func (f *failingPerformanceRepo) SumActualSecondsByAssociate(ctx context.Context, associateId shared.AssociateId, since time.Time) (int64, error) {
	if f.failSumAssociate {
		return 0, errUnmapped
	}
	return f.PerformanceRepo.SumActualSecondsByAssociate(ctx, associateId, since)
}

// failingPublisher can be told to fail Publish, to exercise a use case's
// event-publish error path.
type failingPublisher struct {
	fail bool
}

func (p *failingPublisher) Publish(ctx context.Context, events ...shared.DomainEvent) error {
	if p.fail {
		return errUnmapped
	}
	return nil
}

// failingProcessedEvents can be told to fail MarkProcessed, to exercise
// RecordTaskPerformance's idempotency-gate error path.
type failingProcessedEvents struct {
	fail bool
}

func (p *failingProcessedEvents) MarkProcessed(ctx context.Context, eventId string) (bool, error) {
	if p.fail {
		return false, errUnmapped
	}
	return true, nil
}

// fakeStandardMetrics records how many times each ports.StandardMetrics
// method was called, so DefineStandard's tests can assert the metrics
// port is invoked on exactly the right outcome without depending on a
// real OTel MeterProvider.
type fakeStandardMetrics struct {
	accepted int
	rejected int
}

func (f *fakeStandardMetrics) StandardDefinitionAccepted(ctx context.Context) {
	f.accepted++
}

func (f *fakeStandardMetrics) StandardDefinitionRejected(ctx context.Context) {
	f.rejected++
}

// failingIdlePeriodRepo wraps a real ports.IdlePeriodRepo and can be told
// to fail Save, SumByTaskType, SumByAssociate, LastEndedAtByAssociate or
// DistinctAssociatesByTaskType, to exercise RecordTaskPerformance's and
// GetUtilization's error paths.
type failingIdlePeriodRepo struct {
	ports.IdlePeriodRepo
	failSave          bool
	failSumTaskType   bool
	failSumAssociate  bool
	failLastEndedAt   bool
	failDistinctAssoc bool
}

func (f *failingIdlePeriodRepo) Save(ctx context.Context, p *idleness.IdlePeriod) error {
	if f.failSave {
		return errUnmapped
	}
	return f.IdlePeriodRepo.Save(ctx, p)
}

func (f *failingIdlePeriodRepo) SumByTaskType(ctx context.Context, taskType shared.TaskType, since time.Time) (int64, int, error) {
	if f.failSumTaskType {
		return 0, 0, errUnmapped
	}
	return f.IdlePeriodRepo.SumByTaskType(ctx, taskType, since)
}

func (f *failingIdlePeriodRepo) SumByAssociate(ctx context.Context, associateId shared.AssociateId, since time.Time) (int64, int, error) {
	if f.failSumAssociate {
		return 0, 0, errUnmapped
	}
	return f.IdlePeriodRepo.SumByAssociate(ctx, associateId, since)
}

func (f *failingIdlePeriodRepo) LastEndedAtByAssociate(ctx context.Context, associateId shared.AssociateId) (time.Time, error) {
	if f.failLastEndedAt {
		return time.Time{}, errUnmapped
	}
	return f.IdlePeriodRepo.LastEndedAtByAssociate(ctx, associateId)
}

func (f *failingIdlePeriodRepo) DistinctAssociatesByTaskType(ctx context.Context, taskType shared.TaskType, since time.Time) (int, error) {
	if f.failDistinctAssoc {
		return 0, errUnmapped
	}
	return f.IdlePeriodRepo.DistinctAssociatesByTaskType(ctx, taskType, since)
}

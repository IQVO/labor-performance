package memory

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/claudioed/labor-performance/internal/domain/shared"
	"github.com/claudioed/labor-performance/internal/domain/standard"
)

// StandardRepo is an in-memory implementation of ports.StandardRepo.
type StandardRepo struct {
	mu        sync.RWMutex
	standards map[shared.StandardId]*standard.LaborStandard
	nextID    int

	// FailNextSaveWith, when non-nil, is returned by the NEXT Save call
	// and cleared — a test seam for scripting the Postgres adapter's
	// optimistic-concurrency / one-open-standard failures (ADR 0022)
	// through the same use case the HTTP adapter drives.
	FailNextSaveWith error
}

// NewStandardRepo constructs an empty StandardRepo.
func NewStandardRepo() *StandardRepo {
	return &StandardRepo{standards: make(map[shared.StandardId]*standard.LaborStandard)}
}

func (r *StandardRepo) Save(_ context.Context, s *standard.LaborStandard) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.FailNextSaveWith != nil {
		err := r.FailNextSaveWith
		r.FailNextSaveWith = nil
		return err
	}
	// Store a copy so a later in-memory mutation of the caller's aggregate
	// (e.g. Close) cannot silently change what was already persisted —
	// mirroring the Postgres adapter's persisted-row semantics.
	stored := *s
	r.standards[s.ID()] = &stored
	return nil
}

func (r *StandardRepo) FindActiveAsOf(ctx context.Context, taskType shared.TaskType, t time.Time) (*standard.LaborStandard, error) {
	return r.FindActiveAsOfForUnit(ctx, taskType, "", t)
}

// FindActiveAsOfForUnit resolves the SITE-LESS standard of (taskType,
// unit) active at t: a site's standard is never returned (ADR 0038).
func (r *StandardRepo) FindActiveAsOfForUnit(_ context.Context, taskType shared.TaskType, unit shared.HandlingUnit, t time.Time) (*standard.LaborStandard, error) {
	return r.findActiveAt(standard.Scope{TaskType: taskType, HandlingUnit: unit}, t), nil
}

// findActiveAt returns a copy of the standard of exactly scope active at t.
func (r *StandardRepo) findActiveAt(scope standard.Scope, t time.Time) *standard.LaborStandard {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, s := range r.standards {
		if s.Scope() == scope && s.IsActiveAt(t) {
			cp := *s
			return &cp
		}
	}
	return nil
}

func (r *StandardRepo) FindCurrentlyActive(ctx context.Context, taskType shared.TaskType) (*standard.LaborStandard, error) {
	return r.FindActiveAsOf(ctx, taskType, time.Now())
}

func (r *StandardRepo) FindCurrentlyActiveForUnit(ctx context.Context, taskType shared.TaskType, unit shared.HandlingUnit) (*standard.LaborStandard, error) {
	return r.FindActiveAsOfForUnit(ctx, taskType, unit, time.Now())
}

// FindCurrentlyActiveForScope resolves the standard of exactly scope (task
// type, handling unit, site code) active now; no fallback to another scope.
func (r *StandardRepo) FindCurrentlyActiveForScope(_ context.Context, scope standard.Scope) (*standard.LaborStandard, error) {
	return r.findActiveAt(scope, time.Now()), nil
}

func (r *StandardRepo) NextID(_ context.Context) (shared.StandardId, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nextID++
	return shared.StandardId(fmt.Sprintf("std-%d", r.nextID)), nil
}

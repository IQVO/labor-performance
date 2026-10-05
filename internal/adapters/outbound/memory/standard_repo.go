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

func (r *StandardRepo) FindActiveAsOf(_ context.Context, taskType shared.TaskType, t time.Time) (*standard.LaborStandard, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, s := range r.standards {
		if s.TaskType() == taskType && s.IsActiveAt(t) {
			cp := *s
			return &cp, nil
		}
	}
	return nil, nil
}

func (r *StandardRepo) FindCurrentlyActive(ctx context.Context, taskType shared.TaskType) (*standard.LaborStandard, error) {
	return r.FindActiveAsOf(ctx, taskType, time.Now())
}

func (r *StandardRepo) NextID(_ context.Context) (shared.StandardId, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nextID++
	return shared.StandardId(fmt.Sprintf("std-%d", r.nextID)), nil
}

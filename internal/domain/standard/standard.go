// Package standard implements the LaborStandard aggregate: how long a
// TaskType should take. A standard can be revised, but revising it never
// overwrites the prior record in place — it closes the prior record's
// effective range and starts a new one, so already-recorded
// TaskPerformance rows' frozen StandardSecondsAtCompletion values remain
// historically accurate even after a later revision (see ADR
// 0004-standard-frozen-at-completion-time-not-recomputed.md).
package standard

import (
	"errors"
	"time"

	"github.com/claudioed/labor-performance/internal/domain/shared"
)

// ErrNonPositiveExpectedSeconds enforces the one aggregate invariant on
// LaborStandard: ExpectedSeconds must be > 0. A standard that says a task
// should take zero or negative seconds is not a business fact.
var ErrNonPositiveExpectedSeconds = errors.New("expected seconds must be greater than zero")

// ErrNegativeTravelComponentSeconds is returned when a non-nil
// TravelComponentSeconds is negative — a negative travel allowance is
// not a business fact.
var ErrNegativeTravelComponentSeconds = errors.New("travel component seconds must not be negative")

// ErrTravelComponentExceedsExpectedSeconds is returned when a non-nil
// TravelComponentSeconds is greater than ExpectedSeconds — the travel
// portion of a standard can be as large as the WHOLE standard (a task
// type that is effectively all travel, e.g. a water-spider replenishment
// run) but can never exceed it.
var ErrTravelComponentExceedsExpectedSeconds = errors.New("travel component seconds must not exceed expected seconds")

// LaborStandard is the aggregate root for "how long a task TYPE should
// take". EffectiveTo is nil while the standard is the currently active one
// for its TaskType; Close sets it once a revision supersedes this record.
//
// TravelComponentSeconds is an OPTIONAL breakdown of how much of
// ExpectedSeconds is attributable to travel between locations for this
// TaskType — nil means "not broken out" (the default, and the only state
// that existed before this field). This service never computes it: it is
// supplied by the caller, grounded in a real distance estimate performed
// elsewhere (e.g. an operator or tooling using facility-layout's
// estimate_travel_distance endpoint). This service has no REST dependency
// in either direction with any sibling context (see AGENTS.md's
// non-negotiables and the domain-model rule's "v1 scope decisions still
// in force") — it never resolves or validates this value against a live
// distance lookup itself. See ADR 0015.
type LaborStandard struct {
	id                     shared.StandardId
	taskType               shared.TaskType
	expectedSeconds        int64
	travelComponentSeconds *int64
	effectiveFrom          time.Time
	effectiveTo            *time.Time
	// version is the optimistic-concurrency token (ADR 0022): 1 for a
	// freshly constructed aggregate, advanced by exactly one on every
	// persisted Save. Inert infrastructure metadata, exactly like id —
	// never read by domain business logic.
	version int
}

// New constructs a LaborStandard, freshly active (EffectiveTo nil) from
// effectiveFrom onward. Returns ErrNonPositiveExpectedSeconds if
// expectedSeconds <= 0. travelComponentSeconds is optional (nil means "not
// broken out"); when non-nil it must be in [0, expectedSeconds] —
// ErrNegativeTravelComponentSeconds or
// ErrTravelComponentExceedsExpectedSeconds otherwise.
func New(id shared.StandardId, taskType shared.TaskType, expectedSeconds int64, travelComponentSeconds *int64, effectiveFrom time.Time) (*LaborStandard, error) {
	if expectedSeconds <= 0 {
		return nil, ErrNonPositiveExpectedSeconds
	}
	if err := validateTravelComponentSeconds(travelComponentSeconds, expectedSeconds); err != nil {
		return nil, err
	}
	return &LaborStandard{
		id:                     id,
		taskType:               taskType,
		expectedSeconds:        expectedSeconds,
		travelComponentSeconds: travelComponentSeconds,
		effectiveFrom:          effectiveFrom,
		version:                1,
	}, nil
}

// Rehydrate reconstructs a LaborStandard from persisted state without
// re-validating construction invariants (used by repository adapters).
// version is the row's optimistic-concurrency token as read; 0 is treated
// as 1 for callers predating the column (the in-memory adapter).
func Rehydrate(id shared.StandardId, taskType shared.TaskType, expectedSeconds int64, travelComponentSeconds *int64, effectiveFrom time.Time, effectiveTo *time.Time, version int) *LaborStandard {
	if version <= 0 {
		version = 1
	}
	return &LaborStandard{
		id:                     id,
		taskType:               taskType,
		expectedSeconds:        expectedSeconds,
		travelComponentSeconds: travelComponentSeconds,
		effectiveFrom:          effectiveFrom,
		effectiveTo:            effectiveTo,
		version:                version,
	}
}

// Close ends this standard's effective range at at, marking it superseded
// by a revision. It does NOT overwrite ExpectedSeconds or EffectiveFrom —
// this record remains exactly as it was while it was active, which is what
// keeps historically-frozen TaskPerformance rows accurate.
func (s *LaborStandard) Close(at time.Time) {
	s.effectiveTo = &at
}

// IsActiveAt reports whether this standard was the active one for its
// TaskType at instant t: t is on or after EffectiveFrom, and strictly
// before EffectiveTo (or EffectiveTo is nil, meaning still active).
func (s *LaborStandard) IsActiveAt(t time.Time) bool {
	if t.Before(s.effectiveFrom) {
		return false
	}
	if s.effectiveTo != nil && !t.Before(*s.effectiveTo) {
		return false
	}
	return true
}

func (s *LaborStandard) ID() shared.StandardId     { return s.id }
func (s *LaborStandard) TaskType() shared.TaskType { return s.taskType }
func (s *LaborStandard) ExpectedSeconds() int64    { return s.expectedSeconds }

// TravelComponentSeconds is the optional travel-time breakdown of
// ExpectedSeconds — nil when not declared for this standard.
func (s *LaborStandard) TravelComponentSeconds() *int64 { return s.travelComponentSeconds }
func (s *LaborStandard) EffectiveFrom() time.Time       { return s.effectiveFrom }
func (s *LaborStandard) EffectiveTo() *time.Time        { return s.effectiveTo }

// Version returns the optimistic-concurrency token (ADR 0022): the row
// version this in-memory aggregate was loaded at (1 for a fresh New).
// Repositories guard Save with it; domain logic never reads it.
func (s *LaborStandard) Version() int { return s.version }

// validateTravelComponentSeconds enforces the one invariant on the
// optional travel breakdown: nil is always valid ("not declared"); a
// non-nil value must be in [0, expectedSeconds].
func validateTravelComponentSeconds(travelComponentSeconds *int64, expectedSeconds int64) error {
	if travelComponentSeconds == nil {
		return nil
	}
	if *travelComponentSeconds < 0 {
		return ErrNegativeTravelComponentSeconds
	}
	if *travelComponentSeconds > expectedSeconds {
		return ErrTravelComponentExceedsExpectedSeconds
	}
	return nil
}

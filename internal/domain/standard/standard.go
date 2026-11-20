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

// ErrTeamLiftRequiresCaseUnit is returned when a team-lift declaration is
// supplied for a standard whose handling unit is not CASE. A team lift is a
// property of a heavy sealed case; an EACH has no such threshold and a
// PALLET is moved with equipment, not lifted by two operators (ADR 0035).
var ErrTeamLiftRequiresCaseUnit = errors.New("a team lift can only be declared on a CASE handling-unit standard")

// ErrNonPositiveTeamLiftThreshold is returned when the team-lift weight
// threshold is zero or negative: "heavier than 0 g" is every case, not a
// threshold.
var ErrNonPositiveTeamLiftThreshold = errors.New("team lift threshold grams must be greater than zero")

// ErrNonPositiveTeamLiftExpectedSeconds is returned when the team-lift
// standard itself is zero or negative, for the same reason as
// ErrNonPositiveExpectedSeconds.
var ErrNonPositiveTeamLiftExpectedSeconds = errors.New("team lift expected seconds must be greater than zero")

// ErrScopeMismatch is returned when a standard is asked to supersede a
// standard of another scope. A revision closes only the record of the same
// (task type, handling unit, site): a site's standard never closes the
// site-less one or another site's, and the reverse (ADR 0038).
var ErrScopeMismatch = errors.New("a standard can only supersede a standard of the same task type, handling unit and site")

// Scope is what a standard is ABOUT: the task type, optionally one handling
// unit (ADR 0035) and optionally one site (ADR 0038). At most one standard
// is open per Scope. The zero HandlingUnit and SiteCode are the task-type-
// only, site-less standard: exactly what a standard was before both ADRs.
type Scope struct {
	TaskType     shared.TaskType
	HandlingUnit shared.HandlingUnit
	SiteCode     shared.SiteCode
}

// TeamLiftOperators is the number of operators a team lift needs. It is a
// constant, not a declared parameter: the business fact declared per
// standard is the weight threshold and the standard time, and two is what
// "team lift" means (ADR 0035).
const TeamLiftOperators = 2

// TeamLift is the declared heavy-case rule of a CASE standard: a case
// heavier than ThresholdG grams needs TeamLiftOperators operators, and its
// engineered elapsed time is ExpectedSeconds instead of the standard's own.
// It is declarative input: this context never learns a case weight from a
// sibling over REST or MCP (AGENTS.md non-negotiable 1).
type TeamLift struct {
	thresholdG      int64
	expectedSeconds int64
}

// NewTeamLift validates and constructs a TeamLift declaration.
func NewTeamLift(thresholdG, expectedSeconds int64) (TeamLift, error) {
	if thresholdG <= 0 {
		return TeamLift{}, ErrNonPositiveTeamLiftThreshold
	}
	if expectedSeconds <= 0 {
		return TeamLift{}, ErrNonPositiveTeamLiftExpectedSeconds
	}
	return TeamLift{thresholdG: thresholdG, expectedSeconds: expectedSeconds}, nil
}

// ThresholdG is the weight in grams ABOVE which a case needs a team lift.
func (t TeamLift) ThresholdG() int64 { return t.thresholdG }

// ExpectedSeconds is the engineered elapsed time of a team lift.
func (t TeamLift) ExpectedSeconds() int64 { return t.expectedSeconds }

// Operators is the number of operators a team lift needs.
func (t TeamLift) Operators() int { return TeamLiftOperators }

// Applies reports whether a case of weightG grams needs a team lift. The
// threshold is exclusive: a case weighing EXACTLY the threshold is still a
// one-operator lift; only strictly heavier cases are team lifts.
func (t TeamLift) Applies(weightG int64) bool { return weightG > t.thresholdG }

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
//
// HandlingUnit is OPTIONAL: "" (the default, and the only state that
// existed before ADR 0035) means the standard is about the task type alone;
// EACH, CASE or PALLET makes it the more specific standard for that
// handling unit, which wins over the task-type-only one when a completed
// task carries a handling unit. At most one standard is open per
// (TaskType, HandlingUnit) of a site. TeamLift is the optional heavy-case
// rule and is only valid on a CASE standard.
//
// SiteCode is OPTIONAL (ADR 0038): "" (the default, and the only state
// that existed before) is the site-less standard that applies to every
// site; a site code scopes the standard to one facility. At most one
// standard is open per (TaskType, HandlingUnit, SiteCode) — see Scope.
// Scoring still resolves site-less standards only (ADR 0038 decision 3).
type LaborStandard struct {
	id                     shared.StandardId
	taskType               shared.TaskType
	handlingUnit           shared.HandlingUnit
	siteCode               shared.SiteCode
	teamLift               *TeamLift
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
	return NewForUnit(id, taskType, "", expectedSeconds, travelComponentSeconds, nil, effectiveFrom)
}

// NewForUnit constructs a LaborStandard for a (TaskType, HandlingUnit)
// pair. handlingUnit "" is the task-type-only standard (identical to New).
// teamLift is optional and only valid when handlingUnit is CASE
// (ErrTeamLiftRequiresCaseUnit otherwise). Every New invariant applies.
// The standard is site-less (every site).
func NewForUnit(id shared.StandardId, taskType shared.TaskType, handlingUnit shared.HandlingUnit, expectedSeconds int64, travelComponentSeconds *int64, teamLift *TeamLift, effectiveFrom time.Time) (*LaborStandard, error) {
	return NewForScope(id, Scope{TaskType: taskType, HandlingUnit: handlingUnit}, expectedSeconds, travelComponentSeconds, teamLift, effectiveFrom)
}

// NewForScope constructs a LaborStandard for a full Scope (task type,
// optional handling unit, optional site). A Scope with no site code is
// NewForUnit. Every NewForUnit invariant applies; the site code itself is
// a validated value object (shared.NewSiteCode) by the time it gets here.
func NewForScope(id shared.StandardId, scope Scope, expectedSeconds int64, travelComponentSeconds *int64, teamLift *TeamLift, effectiveFrom time.Time) (*LaborStandard, error) {
	if expectedSeconds <= 0 {
		return nil, ErrNonPositiveExpectedSeconds
	}
	if err := validateTravelComponentSeconds(travelComponentSeconds, expectedSeconds); err != nil {
		return nil, err
	}
	if teamLift != nil && scope.HandlingUnit != shared.Case {
		return nil, ErrTeamLiftRequiresCaseUnit
	}
	return &LaborStandard{
		id:                     id,
		taskType:               scope.TaskType,
		handlingUnit:           scope.HandlingUnit,
		siteCode:               scope.SiteCode,
		teamLift:               teamLift,
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
	return RehydrateForUnit(id, taskType, "", expectedSeconds, travelComponentSeconds, nil, effectiveFrom, effectiveTo, version)
}

// RehydrateForUnit is Rehydrate for a handling-unit standard: the same
// silent reconstruction, carrying the handling unit and the optional team
// lift as persisted. The standard is site-less.
func RehydrateForUnit(id shared.StandardId, taskType shared.TaskType, handlingUnit shared.HandlingUnit, expectedSeconds int64, travelComponentSeconds *int64, teamLift *TeamLift, effectiveFrom time.Time, effectiveTo *time.Time, version int) *LaborStandard {
	return RehydrateForScope(id, Scope{TaskType: taskType, HandlingUnit: handlingUnit}, expectedSeconds, travelComponentSeconds, teamLift, effectiveFrom, effectiveTo, version)
}

// RehydrateForScope is Rehydrate for a full Scope (ADR 0038): the same
// silent reconstruction, carrying the site code as persisted.
func RehydrateForScope(id shared.StandardId, scope Scope, expectedSeconds int64, travelComponentSeconds *int64, teamLift *TeamLift, effectiveFrom time.Time, effectiveTo *time.Time, version int) *LaborStandard {
	if version <= 0 {
		version = 1
	}
	return &LaborStandard{
		id:                     id,
		taskType:               scope.TaskType,
		handlingUnit:           scope.HandlingUnit,
		siteCode:               scope.SiteCode,
		teamLift:               teamLift,
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

// Supersede makes s the revision of prior: prior is closed at the instant s
// becomes effective (append-only history, ADR 0004). Only a standard of
// exactly the same Scope can be superseded (ErrScopeMismatch otherwise,
// and prior is left untouched): a revision closes only the same (task
// type, handling unit, site), never a sibling scope (ADR 0035, ADR 0038).
func (s *LaborStandard) Supersede(prior *LaborStandard) error {
	if prior.Scope() != s.Scope() {
		return ErrScopeMismatch
	}
	prior.Close(s.effectiveFrom)
	return nil
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

// HandlingUnit is the handling unit this standard is specific to, or ""
// when it is about the task type alone.
func (s *LaborStandard) HandlingUnit() shared.HandlingUnit { return s.handlingUnit }

// SiteCode is the site this standard is scoped to, or "" for the site-less
// standard that applies to every site (ADR 0038).
func (s *LaborStandard) SiteCode() shared.SiteCode { return s.siteCode }

// Scope is what this standard is about: (TaskType, HandlingUnit, SiteCode).
func (s *LaborStandard) Scope() Scope {
	return Scope{TaskType: s.taskType, HandlingUnit: s.handlingUnit, SiteCode: s.siteCode}
}

// TeamLift is the optional heavy-case rule (CASE standards only), nil when
// none was declared.
func (s *LaborStandard) TeamLift() *TeamLift { return s.teamLift }

// SecondsFor returns the engineered expected seconds to score a completion
// against and whether the team-lift standard was the one used. weightG is
// the declared weight in grams of the handled case; nil means "unknown" and
// never triggers a team lift (a weight is never guessed). Without a
// declared team lift, or when weightG does not exceed its threshold, the
// answer is the standard's own ExpectedSeconds.
func (s *LaborStandard) SecondsFor(weightG *int64) (seconds int64, teamLift bool) {
	if s.teamLift != nil && weightG != nil && s.teamLift.Applies(*weightG) {
		return s.teamLift.ExpectedSeconds(), true
	}
	return s.expectedSeconds, false
}

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

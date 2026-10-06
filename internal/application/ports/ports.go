// Package ports declares the outbound interfaces the application layer
// depends on: repositories for each aggregate, an event publisher, and a
// clock. Adapters implement these; the application layer never imports an
// adapter package.
package ports

import (
	"context"
	"time"

	"github.com/claudioed/labor-performance/internal/domain/idleness"
	"github.com/claudioed/labor-performance/internal/domain/performance"
	"github.com/claudioed/labor-performance/internal/domain/shared"
	"github.com/claudioed/labor-performance/internal/domain/standard"
)

// StandardRepo persists and loads LaborStandard aggregates. History is
// append-only: Save upserts one record by id, and FindActiveAsOf resolves
// which record (if any) was active for a TaskType at a given instant —
// "active as of t", not "active right now" — since a possibly-replayed
// Kafka message must be scored against the standard that was genuinely
// active when the task completed, not whatever is active at ingestion
// time.
type StandardRepo interface {
	Save(ctx context.Context, s *standard.LaborStandard) error
	// FindActiveAsOf returns the LaborStandard for taskType whose
	// effective range covers t ([EffectiveFrom, EffectiveTo)), or
	// (nil, nil) if none does.
	FindActiveAsOf(ctx context.Context, taskType shared.TaskType, t time.Time) (*standard.LaborStandard, error)
	// FindCurrentlyActive returns the LaborStandard for taskType that is
	// active right now (EffectiveTo nil), or (nil, nil) if none is.
	FindCurrentlyActive(ctx context.Context, taskType shared.TaskType) (*standard.LaborStandard, error)
	NextID(ctx context.Context) (shared.StandardId, error)
}

// PerformanceRepo persists and loads TaskPerformance aggregates, and
// answers the read-model queries GetAssociateScorecard/
// GetTaskTypePerformance are built on.
type PerformanceRepo interface {
	// Save persists p. Idempotency on eventId is enforced by the
	// application layer via ProcessedEvents before Save is ever called —
	// Save itself does not need to re-check.
	Save(ctx context.Context, p *performance.TaskPerformance) error
	// ExistsByAssociateID reports whether at least one TaskPerformance
	// row has ever been recorded for associateId — the "have we ever
	// seen this associate" check GetAssociateScorecard's 404 depends on,
	// distinct from "do we have a numeric score for them".
	ExistsByAssociateID(ctx context.Context, associateId shared.AssociateId) (bool, error)
	// ScorecardFor returns the per-associate read model: task count,
	// mean EfficiencyPct across rows that have one (nil if none do), and
	// a per-TaskType breakdown. Rows with an empty AssociateId are never
	// included in any scorecard.
	ScorecardFor(ctx context.Context, associateId shared.AssociateId) (Scorecard, error)
	// TaskTypePerformanceFor returns the fleet-wide read model across
	// ALL associates for one TaskType: task count and mean EfficiencyPct
	// across rows that have one. Includes rows with an empty
	// AssociateId (e.g. robot-station completions), unlike ScorecardFor.
	TaskTypePerformanceFor(ctx context.Context, taskType shared.TaskType) (TaskTypePerformance, error)
	// RecentByAssociateID returns associateId's most recent TaskPerformance
	// rows, ordered NEWEST-FIRST (descending CompletedAt), capped at
	// limit. Used by GetAssociateScorecard to compute a trend/coaching
	// signal over a bounded recent window without loading every row this
	// associate has ever had scored — an associate could accumulate
	// thousands of rows over a long tenure, and the trend/coaching
	// signal only ever needs a small recent slice, never the full
	// history.
	RecentByAssociateID(ctx context.Context, associateId shared.AssociateId, limit int) ([]*performance.TaskPerformance, error)
	// SumActualSecondsByTaskType returns the total ActualSeconds across
	// rows for taskType whose CompletedAt falls within [since, now) —
	// the "task time" half of GetUtilization's windowed utilization
	// computation.
	SumActualSecondsByTaskType(ctx context.Context, taskType shared.TaskType, since time.Time) (int64, error)
	// SumActualSecondsByAssociate is SumActualSecondsByTaskType's
	// per-associate counterpart.
	SumActualSecondsByAssociate(ctx context.Context, associateId shared.AssociateId, since time.Time) (int64, error)
}

// Scorecard is the per-associate read model — a projection over
// TaskPerformance rows, not a stored aggregate.
type Scorecard struct {
	AssociateId       shared.AssociateId
	TaskCount         int
	MeanEfficiencyPct *float64
	ByTaskType        map[shared.TaskType]TaskTypeBreakdown
	// Trend classifies this associate's recent scored performance
	// against their all-time baseline (see
	// performance.ClassifyTrend). Always present, never nil —
	// TrendInsufficientData is itself a real value, not an absence.
	Trend performance.TrendDirection
	// CoachingFlag is true iff this associate's most recent 3 SCORED
	// tasks were all below the coaching floor (see
	// performance.DetectCoachingFlag) — a "this is worth a
	// conversation" signal, never itself an automated action. Mirrors
	// this context's "visibility, not enforcement" discipline (ADR
	// 0002).
	CoachingFlag bool
}

// TaskTypeBreakdown is one TaskType's slice of a Scorecard.
type TaskTypeBreakdown struct {
	TaskCount         int
	MeanEfficiencyPct *float64
}

// TaskTypePerformance is the fleet-wide (all-associates) read model for
// one TaskType.
type TaskTypePerformance struct {
	TaskType          shared.TaskType
	TaskCount         int
	MeanEfficiencyPct *float64
	// MeanActualSeconds is the mean ActualSeconds across recorded rows
	// for this TaskType whose ActualSeconds is > 0 (rows with
	// ActualSeconds<=0 — unmeasurable, e.g. a pre-migration task with
	// no ClaimedAt — are excluded, exactly like EfficiencyPct excludes
	// them). This is a REAL measured rate, independent of whether an
	// engineered standard exists to compare it against — distinct from
	// MeanEfficiencyPct, which additionally requires an active standard
	// to have existed at completion time. Consumers wanting an actual
	// observed pace (e.g. workforce-management's ProposePathPlan
	// closing the loop on a previously hand-guessed plannedRate) want
	// THIS field, not MeanEfficiencyPct. Nil iff no row with a positive
	// ActualSeconds was ever recorded for this TaskType — never a
	// fabricated number.
	MeanActualSeconds *float64
}

// IdlePeriodRepo persists IdlePeriod aggregates — the between-task idle
// gap this service derives, per associate, on the SAME Kafka-consume
// path RecordTaskPerformance already runs on (see
// internal/domain/idleness). "Last completion" (the gap's start
// boundary) is deliberately NOT a separate query on this port: it is
// answered by the existing PerformanceRepo.RecentByAssociateID(ctx, id,
// 1), which already indexes associate+completedAt for exactly this
// purpose — duplicating that query here would violate DRY for no
// benefit.
type IdlePeriodRepo interface {
	// Save persists p. Not itself idempotency-gated: RecordTaskPerformance's
	// existing KafkaEventId dedup, on the same unit of work, already
	// prevents a redelivered TaskCompleted from writing a second gap.
	Save(ctx context.Context, p *idleness.IdlePeriod) error
	// SumByTaskType returns the total recorded idle seconds and count
	// of idle-gap rows whose TaskType matches taskType and whose
	// EndedAt falls within [since, now) — the raw inputs
	// GetUtilization sums against PerformanceRepo's matching ActualSeconds
	// query to compute a windowed utilization percentage.
	SumByTaskType(ctx context.Context, taskType shared.TaskType, since time.Time) (idleSeconds int64, count int, err error)
	// SumByAssociate is SumByTaskType's per-associate counterpart.
	SumByAssociate(ctx context.Context, associateId shared.AssociateId, since time.Time) (idleSeconds int64, count int, err error)
	// LastEndedAtByAssociate returns the EndedAt of associateId's most
	// recently recorded idle gap, or the zero time if none exists.
	// GetUtilization's OPEN GAP computation (see the idleness ADR's
	// "Trailing idleness" limitation) needs this to decide whether an
	// associate is idle RIGHT NOW, distinct from the windowed closed
	// gaps SumByAssociate already totals.
	LastEndedAtByAssociate(ctx context.Context, associateId shared.AssociateId) (time.Time, error)
	// DistinctAssociatesByTaskType returns the count of distinct
	// associates with at least one recorded idle-gap row for taskType
	// whose EndedAt falls within [since, now) — surfaced on
	// GetUtilization's task-type-scoped result so a caller can see how
	// many people a fleet-wide aggregate actually spans.
	DistinctAssociatesByTaskType(ctx context.Context, taskType shared.TaskType, since time.Time) (int, error)
}

// EventPublisher publishes domain events raised by aggregates. The default
// adapter is a log publisher; with EVENT_PUBLISHER=kafka events are fanned
// onto the analytics and integration topics — through the transactional
// outbox when Postgres is configured (ADR 0010, ADR 0013), otherwise
// straight to the broker. The integration topic already has a consumer
// (workforce-management's labor-performance cache).
type EventPublisher interface {
	Publish(ctx context.Context, events ...shared.DomainEvent) error
}

// UnitOfWork brackets a use case's state change and the domain events it
// raises so both commit or neither does (ADR 0010, transactional outbox).
//
// Execute runs fn inside one atomic scope. Every Repo.Save,
// ProcessedEvents.MarkProcessed and EventPublisher.Publish made with the
// ctx handed to fn is bound to that same scope: if fn returns an error
// the scope is rolled back and nothing — neither the aggregate row, nor
// the idempotency marker, nor the outbox row — is visible afterwards.
//
// Adapters that have no transactional backing (the in-memory repos, the
// log publisher) satisfy this with a pass-through that simply calls fn;
// the use cases stay adapter-agnostic either way and treat a nil
// UnitOfWork as exactly that pass-through.
type UnitOfWork interface {
	Execute(ctx context.Context, fn func(ctx context.Context) error) error
}

// ProcessedEvents is the idempotency gate for at-least-once Kafka
// consumption of TaskCompleted, keyed on the message's event_id rather
// than TaskId (which could in principle be reused after a very long
// time). Unlike workforce-management's identically-shaped port — which
// gates only its analytics side-projection — this service's OLTP write
// path depends on it directly, since consuming TaskCompleted IS this
// service's whole job (see ADR
// 0003-kafka-choreography-consumer-of-fulfillment-execution.md).
type ProcessedEvents interface {
	// MarkProcessed records eventId if absent, returning true iff this
	// call newly recorded it (so the caller should process the event)
	// and false if it was already seen (a duplicate to skip).
	MarkProcessed(ctx context.Context, eventId string) (bool, error)
}

// Clock supplies the current time so domain/application logic never calls
// time.Now() directly.
type Clock interface {
	Now() time.Time
}

// StandardMetrics records DefineStandard outcomes (fleet-standard-metrics
// ADR, Tier 2) so the business signal — how often a caller's attempt to
// set an engineered labor standard actually takes effect versus gets
// rejected for violating the one aggregate invariant (ExpectedSeconds must
// be > 0) — is observable independently of HTTP traffic. Use cases treat a
// nil value as "not instrumented", so wiring it is optional, mirroring
// inventory-storage's ports.ReservationMetrics.
type StandardMetrics interface {
	// StandardDefinitionAccepted records a DefineStandard call that
	// persisted successfully (first definition or revision — both are
	// the same business event: a caller's requested standard took
	// effect).
	StandardDefinitionAccepted(ctx context.Context)
	// StandardDefinitionRejected records a DefineStandard call rejected
	// for a non-positive ExpectedSeconds — a caller submitted a standard
	// that is not a business fact (see standard.ErrNonPositiveExpectedSeconds).
	StandardDefinitionRejected(ctx context.Context)
}

package shared

import "time"

// DomainEvent is a past-tense fact produced by an aggregate in this
// domain. The outbound event publisher serializes these (as CloudEvents 1.0,
// ADR 0021) and the domain never depends on the publishing mechanism. With
// EVENT_PUBLISHER=kafka the events are fanned onto the analytics and
// integration topics, through the transactional outbox when Postgres is
// configured (ADR 0010, ADR 0013); the default log publisher only logs them.
// TaskPerformanceRecorded on the integration topic is consumed by
// workforce-management's event-fed labor-performance cache.
type DomainEvent interface {
	EventName() string
	OccurredAt() time.Time
}

type base struct {
	Name string    `json:"eventName"`
	At   time.Time `json:"occurredAt"`
}

func (b base) EventName() string     { return b.Name }
func (b base) OccurredAt() time.Time { return b.At }

func newBase(name string, occurredAt time.Time) base {
	return base{Name: name, At: occurredAt}
}

// LaborStandardDefined: a TaskType had no active standard, and one was
// just defined.
type LaborStandardDefined struct {
	base
	StandardId      StandardId
	TaskType        TaskType
	ExpectedSeconds int64
	// TravelComponentSeconds is the optional travel-time breakdown of
	// ExpectedSeconds this standard declared — nil when not declared
	// (the common case). See standard.LaborStandard's own doc comment
	// and ADR 0015.
	TravelComponentSeconds *int64
	EffectiveFrom          time.Time
}

func NewLaborStandardDefined(occurredAt time.Time, id StandardId, taskType TaskType, expectedSeconds int64, travelComponentSeconds *int64, effectiveFrom time.Time) LaborStandardDefined {
	return LaborStandardDefined{
		base:                   newBase("LaborStandardDefined", occurredAt),
		StandardId:             id,
		TaskType:               taskType,
		ExpectedSeconds:        expectedSeconds,
		TravelComponentSeconds: travelComponentSeconds,
		EffectiveFrom:          effectiveFrom,
	}
}

// LaborStandardRevised: a TaskType already had an active standard, which
// was closed, and a new one started in its place (append-only history —
// the prior standard's row is never overwritten, only closed).
type LaborStandardRevised struct {
	base
	StandardId              StandardId
	TaskType                TaskType
	PreviousExpectedSeconds int64
	NewExpectedSeconds      int64
	// NewTravelComponentSeconds is the optional travel-time breakdown
	// declared on the NEW (revised) standard — nil when not declared.
	// The previous standard's own travel component, if any, is not
	// carried on this event: it remains queryable on that closed
	// LaborStandard row exactly as it was.
	NewTravelComponentSeconds *int64
	EffectiveFrom             time.Time
}

func NewLaborStandardRevised(occurredAt time.Time, id StandardId, taskType TaskType, previousExpectedSeconds, newExpectedSeconds int64, newTravelComponentSeconds *int64, effectiveFrom time.Time) LaborStandardRevised {
	return LaborStandardRevised{
		base:                      newBase("LaborStandardRevised", occurredAt),
		StandardId:                id,
		TaskType:                  taskType,
		PreviousExpectedSeconds:   previousExpectedSeconds,
		NewExpectedSeconds:        newExpectedSeconds,
		NewTravelComponentSeconds: newTravelComponentSeconds,
		EffectiveFrom:             effectiveFrom,
	}
}

// TaskPerformanceRecorded: one completed task was scored against whatever
// standard was active at completion time (or no standard, if none was).
type TaskPerformanceRecorded struct {
	base
	TaskId        string
	AssociateId   AssociateId
	TaskType      TaskType
	ActualSeconds int64
	EfficiencyPct *float64
	// IdleSecondsBefore is the measured idle gap immediately preceding
	// this task's claim (previous completion -> this claim instant),
	// mirroring EfficiencyPct's existing nullable-pointer pattern: nil
	// -- never a fabricated number -- when there was no prior
	// completion to measure from (this associate's first-ever
	// observation), the gap was negative/zero (Kafka redelivered or
	// reordered the underlying TaskCompleted events), or AssociateId is
	// empty (no checked-in occupant, e.g. a robot station). ADDITIVE:
	// existing consumers unmarshaling unknown-field-tolerant JSON are
	// unaffected by this field's presence. See the idleness package and
	// its ADR for the full "Idle Gap" vocabulary.
	IdleSecondsBefore *int64
	CompletedAt       time.Time
}

func NewTaskPerformanceRecorded(occurredAt time.Time, taskId string, associateId AssociateId, taskType TaskType, actualSeconds int64, efficiencyPct *float64, idleSecondsBefore *int64, completedAt time.Time) TaskPerformanceRecorded {
	return TaskPerformanceRecorded{
		base:              newBase("TaskPerformanceRecorded", occurredAt),
		TaskId:            taskId,
		AssociateId:       associateId,
		TaskType:          taskType,
		ActualSeconds:     actualSeconds,
		EfficiencyPct:     efficiencyPct,
		IdleSecondsBefore: idleSecondsBefore,
		CompletedAt:       completedAt,
	}
}

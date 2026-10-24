// Package performance implements the TaskPerformance aggregate: one
// scored, already-completed task, frozen at ingestion time from a
// fulfillment-execution TaskCompleted event. It is immutable once recorded
// — this is an event-sourced fact, not something a human edits, so there
// is no update/delete use case anywhere in the application layer.
package performance

import (
	"errors"
	"time"

	"github.com/claudioed/labor-performance/internal/domain/shared"
)

var (
	// ErrEmptyEventId is returned when a TaskPerformance is constructed
	// without a Kafka event id — the id this aggregate is keyed and
	// deduplicated by.
	ErrEmptyEventId = errors.New("kafka event id must not be empty")

	// ErrEmptyTaskId is returned when a TaskPerformance is constructed
	// without a task id. TaskId is fulfillment-execution's, treated as an
	// opaque foreign reference this context does not validate further.
	ErrEmptyTaskId = errors.New("task id must not be empty")
)

// TaskPerformance is the aggregate root for one scored completed task.
type TaskPerformance struct {
	eventId                     string
	taskId                      string
	associateId                 shared.AssociateId
	taskType                    shared.TaskType
	actualSeconds               int64
	standardSecondsAtCompletion int64
	efficiencyPct               *float64
	completedAt                 time.Time
	// handlingUnit is the handling unit the completed task carried ("" when
	// its event carried none) and teamLift whether the team-lift standard
	// was the one StandardSecondsAtCompletion was resolved from. Both are
	// frozen at ingestion like the standard itself (ADR 0004, ADR 0035).
	handlingUnit shared.HandlingUnit
	teamLift     bool
}

// New constructs a TaskPerformance, computing EfficiencyPct from
// actualSeconds and standardSecondsAtCompletion. eventId is the Kafka
// message's event_id — the dedup key this aggregate is recorded under (not
// taskId, which fulfillment-execution could in principle reuse after a
// very long time). taskType may be "" ("unclassified") when the source
// event did not carry a recognizable task type — see
// shared.ParseTaskTypeLenient's doc comment for why that is a legitimate,
// expected v1 case rather than an error. associateId may likewise be ""
// when the completing station had no checked-in occupant.
func New(eventId, taskId string, associateId shared.AssociateId, taskType shared.TaskType, actualSeconds, standardSecondsAtCompletion int64, completedAt time.Time) (*TaskPerformance, error) {
	return NewWithHandling(eventId, taskId, associateId, taskType, "", false, actualSeconds, standardSecondsAtCompletion, completedAt)
}

// NewWithHandling is New for a completion that carried a handling unit:
// handlingUnit is the unit the event carried ("" for none) and teamLift
// whether standardSecondsAtCompletion is the team-lift standard (ADR 0035).
// EfficiencyPct is computed exactly as New does.
func NewWithHandling(eventId, taskId string, associateId shared.AssociateId, taskType shared.TaskType, handlingUnit shared.HandlingUnit, teamLift bool, actualSeconds, standardSecondsAtCompletion int64, completedAt time.Time) (*TaskPerformance, error) {
	if eventId == "" {
		return nil, ErrEmptyEventId
	}
	if taskId == "" {
		return nil, ErrEmptyTaskId
	}
	return &TaskPerformance{
		eventId:                     eventId,
		taskId:                      taskId,
		associateId:                 associateId,
		taskType:                    taskType,
		actualSeconds:               actualSeconds,
		standardSecondsAtCompletion: standardSecondsAtCompletion,
		efficiencyPct:               computeEfficiencyPct(actualSeconds, standardSecondsAtCompletion),
		completedAt:                 completedAt,
		handlingUnit:                handlingUnit,
		teamLift:                    teamLift,
	}, nil
}

// Rehydrate reconstructs a TaskPerformance from persisted state without
// recomputing EfficiencyPct — the persisted value is trusted as-is so a
// repository round-trip never silently changes a frozen historical fact.
func Rehydrate(eventId, taskId string, associateId shared.AssociateId, taskType shared.TaskType, actualSeconds, standardSecondsAtCompletion int64, efficiencyPct *float64, completedAt time.Time) *TaskPerformance {
	return RehydrateWithHandling(eventId, taskId, associateId, taskType, "", false, actualSeconds, standardSecondsAtCompletion, efficiencyPct, completedAt)
}

// RehydrateWithHandling is Rehydrate carrying the persisted handling unit
// and team-lift flag.
func RehydrateWithHandling(eventId, taskId string, associateId shared.AssociateId, taskType shared.TaskType, handlingUnit shared.HandlingUnit, teamLift bool, actualSeconds, standardSecondsAtCompletion int64, efficiencyPct *float64, completedAt time.Time) *TaskPerformance {
	return &TaskPerformance{
		handlingUnit:                handlingUnit,
		teamLift:                    teamLift,
		eventId:                     eventId,
		taskId:                      taskId,
		associateId:                 associateId,
		taskType:                    taskType,
		actualSeconds:               actualSeconds,
		standardSecondsAtCompletion: standardSecondsAtCompletion,
		efficiencyPct:               efficiencyPct,
		completedAt:                 completedAt,
	}
}

// computeEfficiencyPct implements the EfficiencyPct invariant: NEVER
// divide by zero. actualSeconds<=0 (an unmeasurable completion — e.g. a
// TaskCompleted whose duration_seconds is 0 because no claim-timestamp
// existed) or standardSecondsAtCompletion<=0 (no active standard existed
// for that TaskType at completion time) both yield nil, not an error and
// not a fabricated number.
func computeEfficiencyPct(actualSeconds, standardSecondsAtCompletion int64) *float64 {
	if actualSeconds <= 0 || standardSecondsAtCompletion <= 0 {
		return nil
	}
	pct := 100 * float64(standardSecondsAtCompletion) / float64(actualSeconds)
	return &pct
}

func (p *TaskPerformance) EventId() string                    { return p.eventId }
func (p *TaskPerformance) TaskId() string                     { return p.taskId }
func (p *TaskPerformance) AssociateId() shared.AssociateId    { return p.associateId }
func (p *TaskPerformance) TaskType() shared.TaskType          { return p.taskType }
func (p *TaskPerformance) ActualSeconds() int64               { return p.actualSeconds }
func (p *TaskPerformance) StandardSecondsAtCompletion() int64 { return p.standardSecondsAtCompletion }
func (p *TaskPerformance) EfficiencyPct() *float64            { return p.efficiencyPct }
func (p *TaskPerformance) CompletedAt() time.Time             { return p.completedAt }

// HandlingUnit is the handling unit the completed task carried, or "" when
// its event carried none.
func (p *TaskPerformance) HandlingUnit() shared.HandlingUnit { return p.handlingUnit }

// TeamLift reports whether the task was scored against a team-lift
// standard (a case heavier than the declared threshold).
func (p *TaskPerformance) TeamLift() bool { return p.teamLift }

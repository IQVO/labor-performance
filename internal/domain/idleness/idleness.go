// Package idleness implements the idle-gap and utilization vocabulary
// this service measures on top of the TaskPerformance aggregate: an
// associate's IDLE GAP is the wait between completing one task and
// claiming the next; UTILIZATION is 1 minus the idle share of a window;
// an OPEN GAP is the still-running idle time for an associate who is
// idle right now (computed at read time, never persisted -- see
// UtilizationPct and the application layer's GetUtilization use case).
//
// Idleness is a DERIVED labor concern, not a new bounded context: it
// sits next to actual-vs-standard performance scoring in every real LMS
// product (see the ADR this package ships with). It is measured
// entirely from facts this service already has on the wire --
// fulfillment-execution's TaskCompleted carries duration_seconds, so the
// previous task's claim instant is recoverable as
// time - duration_seconds (time = the CloudEvents attribute, ADR 0021)
// with no upstream change required.
package idleness

import (
	"errors"
	"time"

	"github.com/claudioed/labor-performance/internal/domain/shared"
)

var (
	// ErrEmptyAssociateId is returned when an IdlePeriod is constructed
	// for an empty associate id. An empty AssociateId means the
	// completing station had no checked-in occupant (e.g. a robot
	// station) -- CLAUDE.md's "Robot stations" known limitation:
	// utilization of robots is a WCS/equipment concern, out of this
	// context's scope, so no idle gap is ever recorded for one.
	ErrEmptyAssociateId = errors.New("idleness: associate id must not be empty")

	// ErrNegativeGap is returned when the gap is not strictly positive:
	// endedAt is before, or equal to, startedAt. Kafka delivery is
	// UNORDERED, so a TaskCompleted whose claim instant (derived as
	// completedAt - duration_seconds) lands before or at the
	// associate's previously-recorded completion is a ROUTINE
	// occurrence, not an exceptional one -- the caller skips-and-logs
	// this, it never fails the enclosing RecordTaskPerformance write.
	ErrNegativeGap = errors.New("idleness: idle gap ended before or at its start")
)

// IdlePeriod is one measured idle gap: the wait between an associate's
// previous task completion (StartedAt) and the next task's claim instant
// (EndedAt). TaskType is the type of the task that ENDED the gap (the
// one just claimed), not the one that started it -- so per-task-type
// utilization answers "how much idle time preceded a PICK", which is the
// operationally useful framing (a PICK station starved of work, not a
// PACK station that happened to finish first).
//
// Seconds is capped at construction time (the domain owns the capping
// rule, per CLAUDE.md's guidance for every derived business number) so a
// gap spanning a shift boundary cannot silently poison a running mean --
// see the ADR's "Cross-shift gaps" known limitation. Capped is true iff
// the cap was actually applied, so a consumer of the resulting
// TaskPerformanceRecorded.IdleSecondsBefore can tell a genuinely short
// gap from one that was clipped.
type IdlePeriod struct {
	associateId shared.AssociateId
	taskType    shared.TaskType
	startedAt   time.Time
	endedAt     time.Time
	seconds     int64
	capped      bool
}

// New constructs an IdlePeriod from the raw gap boundaries, applying the
// capSeconds rule (capSeconds <= 0 means "no cap"). Returns
// ErrEmptyAssociateId for an empty associateId and ErrNegativeGap when
// endedAt is not strictly after startedAt -- both are real, EXPECTED
// outcomes callers must skip-and-log, not treat as failures.
func New(associateId shared.AssociateId, taskType shared.TaskType, startedAt, endedAt time.Time, capSeconds int64) (*IdlePeriod, error) {
	if associateId == "" {
		return nil, ErrEmptyAssociateId
	}
	if !endedAt.After(startedAt) {
		return nil, ErrNegativeGap
	}

	rawSeconds := int64(endedAt.Sub(startedAt).Seconds())
	if capSeconds > 0 && rawSeconds > capSeconds {
		return &IdlePeriod{
			associateId: associateId,
			taskType:    taskType,
			startedAt:   startedAt,
			endedAt:     endedAt,
			seconds:     capSeconds,
			capped:      true,
		}, nil
	}
	return &IdlePeriod{
		associateId: associateId,
		taskType:    taskType,
		startedAt:   startedAt,
		endedAt:     endedAt,
		seconds:     rawSeconds,
		capped:      false,
	}, nil
}

// Rehydrate reconstructs an IdlePeriod from persisted state without
// re-validating or re-applying the capping rule -- the persisted values
// are trusted as-is, mirroring performance.Rehydrate's contract.
func Rehydrate(associateId shared.AssociateId, taskType shared.TaskType, startedAt, endedAt time.Time, seconds int64, capped bool) *IdlePeriod {
	return &IdlePeriod{
		associateId: associateId,
		taskType:    taskType,
		startedAt:   startedAt,
		endedAt:     endedAt,
		seconds:     seconds,
		capped:      capped,
	}
}

func (p *IdlePeriod) AssociateId() shared.AssociateId { return p.associateId }
func (p *IdlePeriod) TaskType() shared.TaskType       { return p.taskType }
func (p *IdlePeriod) StartedAt() time.Time            { return p.startedAt }
func (p *IdlePeriod) EndedAt() time.Time              { return p.endedAt }
func (p *IdlePeriod) Seconds() int64                  { return p.seconds }
func (p *IdlePeriod) Capped() bool                    { return p.capped }

// UtilizationPct computes 1 minus the idle share of a window, expressed
// as a percentage: 100 * taskSeconds / (taskSeconds + idleSeconds). Nil
// -- never a fabricated number -- when there is nothing to compute a
// share of (both inputs zero, e.g. a window with no observed activity at
// all for this subject).
func UtilizationPct(taskSeconds, idleSeconds int64) *float64 {
	total := taskSeconds + idleSeconds
	if total <= 0 {
		return nil
	}
	pct := 100 * float64(taskSeconds) / float64(total)
	return &pct
}

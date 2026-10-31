// Package shared holds the value objects, domain events, and error types
// common to both aggregates in the Labor Performance domain: LaborStandard
// and TaskPerformance.
package shared

import "errors"

// TaskType is the task type an engineered labor standard applies to, and
// (when known) the type a scored TaskPerformance belongs to. It mirrors
// fulfillment-execution's task.Type enum exactly (PICK, PACK, SLAM) — this
// context does not invent new task types.
type TaskType string

const (
	Pick TaskType = "PICK"
	Pack TaskType = "PACK"
	Slam TaskType = "SLAM"
)

// ErrUnknownTaskType is returned when a caller-supplied (REST path/body)
// task type string is not one of the three known values.
var ErrUnknownTaskType = errors.New("unknown task type: must be PICK, PACK, or SLAM")

// NewTaskType validates a caller-supplied task type string. Used at the
// REST boundary (DefineStandard, GetStandard, GetTaskTypePerformance),
// where an unrecognized value is a caller mistake worth rejecting with a
// 400. Contrast with ParseTaskTypeLenient, used only when consuming a
// TaskCompleted Kafka event, where an unrecognized/absent value must never
// block recording real performance data.
func NewTaskType(value string) (TaskType, error) {
	switch TaskType(value) {
	case Pick, Pack, Slam:
		return TaskType(value), nil
	default:
		return "", ErrUnknownTaskType
	}
}

// ParseTaskTypeLenient coerces raw into a known TaskType, or "" ("
// unclassified") when raw is empty or not one of the three known values.
//
// The consumer (internal/adapters/inbound/kafka/consumer.go) always
// routes fulfillment-execution's TaskCompleted.task_type through here
// rather than trusting it directly: since this service MUST NOT call
// fulfillment-execution synchronously to resolve a task's type, and a
// TaskCompleted event is a real, must-be-recorded business fact
// regardless, an absent/unrecognized task_type degrades to ""
// (unclassified) exactly like an absent associate_id degrades to "" — it
// never blocks recording the row. This still matters even now that
// fulfillment-execution's ADR-0023 puts a real task_type on the wire for
// every findable task: an older payload predating that ADR, a REBIN
// completion (a fourth task type this service does not model an
// engineered labor standard for), or the lookup-miss degrade case ADR-0023
// itself documents all still need to land here safely, not panic or
// reject the completion. A "" TaskType simply never has a LaborStandard
// to compare against (no lookup is possible without a known type) and
// never appears under any GetTaskTypePerformance query (those require one
// of the three known values).
func ParseTaskTypeLenient(raw string) TaskType {
	switch TaskType(raw) {
	case Pick, Pack, Slam:
		return TaskType(raw)
	default:
		return ""
	}
}

// AssociateId identifies the associate who completed a task. Empty is a
// legitimate, expected value — fulfillment-execution's TaskCompleted
// enrichment supplies "" when the completing station had no checked-in
// occupant (e.g. a robot station).
type AssociateId string

// StandardId identifies one LaborStandard record (one row in its
// append-only history — a revision mints a new StandardId rather than
// reusing the prior one).
type StandardId string

// HandlingUnit is the packaging level a unit of work handles: one EACH
// (a single sellable unit), one CASE (a sealed master case) or one PALLET.
// The three values are the handling-unit subset of the fleet's pack-level
// vocabulary (product-master's EACH|INNER|CASE|PALLET; INNER is not
// implemented by any flow in v1 and is deliberately not modelled here).
// The zero value ("") means "no handling unit": the standard or the
// completed task is about the task type alone, which is exactly this
// context's behaviour before ADR 0035. See ADR 0035.
type HandlingUnit string

const (
	Each   HandlingUnit = "EACH"
	Case   HandlingUnit = "CASE"
	Pallet HandlingUnit = "PALLET"
)

// ErrUnknownHandlingUnit is returned when a caller-supplied (REST) handling
// unit string is not one of EACH, CASE or PALLET.
var ErrUnknownHandlingUnit = errors.New("unknown handling unit: must be EACH, CASE, or PALLET")

// NewHandlingUnit validates a caller-supplied handling unit string. The
// empty string is valid and means "no handling unit" (the task type alone);
// any other value must be EACH, CASE or PALLET. Used at the REST boundary,
// where a mistake is worth a 400. Contrast with ParseHandlingUnitLenient.
func NewHandlingUnit(value string) (HandlingUnit, error) {
	switch HandlingUnit(value) {
	case "", Each, Case, Pallet:
		return HandlingUnit(value), nil
	default:
		return "", ErrUnknownHandlingUnit
	}
}

// ParseHandlingUnitLenient coerces raw into a known HandlingUnit, or ""
// (no handling unit) when raw is empty or not one of the three values. Used
// only when consuming a TaskCompleted Kafka event: a handling unit this
// context does not recognise (INNER, a future level) must never block
// recording a real completion, it degrades to the task-type-only standard
// exactly like an absent field does.
func ParseHandlingUnitLenient(raw string) HandlingUnit {
	switch HandlingUnit(raw) {
	case Each, Case, Pallet:
		return HandlingUnit(raw)
	default:
		return ""
	}
}

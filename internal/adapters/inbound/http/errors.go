package http

import (
	"errors"
	"net/http"

	"github.com/claudioed/labor-performance/internal/application/ports"
	"github.com/claudioed/labor-performance/internal/application/usecases"
	"github.com/claudioed/labor-performance/internal/domain/performance"
	"github.com/claudioed/labor-performance/internal/domain/shared"
	"github.com/claudioed/labor-performance/internal/domain/standard"
)

// statusFor maps a typed domain/application error to an HTTP status code.
func statusFor(err error) int {
	switch {
	case errors.Is(err, usecases.ErrStandardNotFound),
		errors.Is(err, usecases.ErrAssociateNotFound):
		return http.StatusNotFound

	case errors.Is(err, ports.ErrConcurrentModification),
		errors.Is(err, ports.ErrOpenStandardConflict):
		return http.StatusConflict

	case errors.Is(err, shared.ErrUnknownTaskType),
		errors.Is(err, shared.ErrUnknownHandlingUnit),
		errors.Is(err, performance.ErrEmptyEventId),
		errors.Is(err, performance.ErrEmptyTaskId):
		return http.StatusBadRequest

	case errors.Is(err, standard.ErrNonPositiveExpectedSeconds),
		errors.Is(err, standard.ErrNegativeTravelComponentSeconds),
		errors.Is(err, standard.ErrTravelComponentExceedsExpectedSeconds),
		errors.Is(err, standard.ErrTeamLiftRequiresCaseUnit),
		errors.Is(err, standard.ErrNonPositiveTeamLiftThreshold),
		errors.Is(err, standard.ErrNonPositiveTeamLiftExpectedSeconds):
		return http.StatusUnprocessableEntity

	default:
		return http.StatusInternalServerError
	}
}

// problemBaseURI is the namespace for this service's RFC 7807 "type" URIs.
// It does not need to resolve to a real page — it's an identifier, unique
// per distinct error category in this service.
const problemBaseURI = "https://errors.labor-performance.warehouse-systems.dev/"

// problemInfo is the fixed, category-level (type, title) pair for an RFC
// 7807 problem response. slug becomes the last path segment of "type";
// title is a fixed human string for the category (the dynamic detail comes
// from err.Error() at write time, not from this table).
type problemInfo struct {
	slug  string
	title string
}

// problemFor maps a typed domain/application error to its RFC 7807
// (type, title) pair. Mirrors statusFor's error groupings one-for-one —
// statusFor itself is untouched; this only decides what goes in the body.
var problemTable = []struct {
	sentinel error
	info     problemInfo
}{
	{usecases.ErrStandardNotFound, problemInfo{"standard-not-found", "No active labor standard for this task type"}},
	{usecases.ErrAssociateNotFound, problemInfo{"associate-not-found", "No task performance recorded for this associate"}},
	{ports.ErrConcurrentModification, problemInfo{"concurrent-modification", "The standard was concurrently modified; reload and retry"}},
	{ports.ErrOpenStandardConflict, problemInfo{"standard-conflict", "Another standard is already open for this task type"}},
	{shared.ErrUnknownTaskType, problemInfo{"unknown-task-type", "Task type must be PICK, PACK, or SLAM"}},
	{shared.ErrUnknownHandlingUnit, problemInfo{"unknown-handling-unit", "Handling unit must be EACH, CASE, or PALLET"}},
	{performance.ErrEmptyEventId, problemInfo{"empty-event-id", "Kafka event id must not be empty"}},
	{performance.ErrEmptyTaskId, problemInfo{"empty-task-id", "Task id must not be empty"}},
	{standard.ErrNonPositiveExpectedSeconds, problemInfo{"non-positive-expected-seconds", "Expected seconds must be greater than zero"}},
	{standard.ErrNegativeTravelComponentSeconds, problemInfo{"negative-travel-component-seconds", "Travel component seconds must not be negative"}},
	{standard.ErrTravelComponentExceedsExpectedSeconds, problemInfo{"travel-component-exceeds-expected-seconds", "Travel component seconds must not exceed expected seconds"}},
	{standard.ErrTeamLiftRequiresCaseUnit, problemInfo{"team-lift-requires-case-unit", "A team lift can only be declared on a CASE handling-unit standard"}},
	{standard.ErrNonPositiveTeamLiftThreshold, problemInfo{"non-positive-team-lift-threshold", "Team lift threshold grams must be greater than zero"}},
	{standard.ErrNonPositiveTeamLiftExpectedSeconds, problemInfo{"non-positive-team-lift-expected-seconds", "Team lift expected seconds must be greater than zero"}},
}

func problemFor(err error) problemInfo {
	for _, row := range problemTable {
		if errors.Is(err, row.sentinel) {
			return row.info
		}
	}
	return problemInfo{"internal-error", "An unexpected internal error occurred"}
}

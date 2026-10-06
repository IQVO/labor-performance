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
		errors.Is(err, performance.ErrEmptyEventId),
		errors.Is(err, performance.ErrEmptyTaskId):
		return http.StatusBadRequest

	case errors.Is(err, standard.ErrNonPositiveExpectedSeconds),
		errors.Is(err, standard.ErrNegativeTravelComponentSeconds),
		errors.Is(err, standard.ErrTravelComponentExceedsExpectedSeconds):
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
func problemFor(err error) problemInfo {
	switch {
	case errors.Is(err, usecases.ErrStandardNotFound):
		return problemInfo{"standard-not-found", "No active labor standard for this task type"}
	case errors.Is(err, usecases.ErrAssociateNotFound):
		return problemInfo{"associate-not-found", "No task performance recorded for this associate"}

	case errors.Is(err, ports.ErrConcurrentModification):
		return problemInfo{"concurrent-modification", "The standard was concurrently modified; reload and retry"}
	case errors.Is(err, ports.ErrOpenStandardConflict):
		return problemInfo{"standard-conflict", "Another standard is already open for this task type"}

	case errors.Is(err, shared.ErrUnknownTaskType):
		return problemInfo{"unknown-task-type", "Task type must be PICK, PACK, or SLAM"}
	case errors.Is(err, performance.ErrEmptyEventId):
		return problemInfo{"empty-event-id", "Kafka event id must not be empty"}
	case errors.Is(err, performance.ErrEmptyTaskId):
		return problemInfo{"empty-task-id", "Task id must not be empty"}

	case errors.Is(err, standard.ErrNonPositiveExpectedSeconds):
		return problemInfo{"non-positive-expected-seconds", "Expected seconds must be greater than zero"}
	case errors.Is(err, standard.ErrNegativeTravelComponentSeconds):
		return problemInfo{"negative-travel-component-seconds", "Travel component seconds must not be negative"}
	case errors.Is(err, standard.ErrTravelComponentExceedsExpectedSeconds):
		return problemInfo{"travel-component-exceeds-expected-seconds", "Travel component seconds must not exceed expected seconds"}

	default:
		return problemInfo{"internal-error", "An unexpected internal error occurred"}
	}
}

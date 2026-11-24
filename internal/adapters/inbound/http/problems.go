package http

import "net/http"

// registered collects the problem types declared with register (problems.go). The typed domain
// errors are not here: they are the rows of problemTable (errors.go), their status is statusFor's.
var registered []problemType

// register declares a problem type that is not a typed domain error: it records the entry for
// allProblemTypes and returns the problemInfo to answer it with. The status is the one the call
// sites answer; allProblemTypes' tests use it to drive the real error writer.
func register(slug, title string, status int, class replayClass) problemInfo {
	info := problemInfo{slug, title, class}
	registered = append(registered, problemType{Slug: slug, Status: status, Class: class, info: info})
	return info
}

// The problem types that are not a typed domain error (those are the rows of problemTable in
// errors.go). Each is declared with register, which takes the replay class, so none can exist
// without one (replay_class_test.go scans for a problemInfo literal anywhere else).
//
// A refusal that is a function of the request alone is FINAL; one that depends on state, a
// dependency or the server itself is RETRYABLE. This service has no OUTCOME (ADR 0039).
var (
	problemMalformedJSON  = register(problemMalformedRequestBody, "The request body is not valid JSON", http.StatusBadRequest, classFinal)
	problemBodyUnreadable = register(problemMalformedRequestBody, "The request body could not be read", http.StatusBadRequest, classFinal)
	problemBodyHasNUL     = register(problemMalformedRequestBody, "The request body contains a NUL character", http.StatusBadRequest, classFinal)

	// The three Idempotency-Key answers are given by the middleware itself, before a key row
	// exists or instead of running the handler: none is ever stored, FINAL records that each is a
	// function of the request (and of the stored fingerprint) alone.
	problemIdempotencyKeyRequired = register("idempotency-key-required", "Idempotency-Key header is required", http.StatusBadRequest, classFinal)
	problemIdempotencyKeyInvalid  = register("idempotency-key-invalid", "Idempotency-Key header is invalid", http.StatusBadRequest, classFinal)
	problemIdempotencyKeyReused   = register("idempotency-key-reused", "Idempotency-Key was already used with a different request", http.StatusConflict, classFinal)

	problemResourceNotFound   = register("resource-not-found", "Resource not found", http.StatusNotFound, classFinal)
	problemMethodNotAllowed   = register("method-not-allowed", "Method not allowed", http.StatusMethodNotAllowed, classFinal)
	problemInvalidReportQuery = register("invalid-report-query", "The report query is malformed or missing a required parameter", http.StatusBadRequest, classFinal)

	// A 5xx is never stored, so it is RETRYABLE by definition.
	problemInternalError         = register("internal-error", "An unexpected internal error occurred", http.StatusInternalServerError, classRetryable)
	problemDependencyUnavailable = register("dependency-unavailable", "A runtime dependency is unavailable", http.StatusServiceUnavailable, classRetryable)
	problemReportStoreError      = register("report-store-error", "The report could not be served", http.StatusInternalServerError, classRetryable)
)

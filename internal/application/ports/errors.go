package ports

import "errors"

var (
	// ErrConcurrentModification is returned by a repository Save whose
	// optimistic-concurrency guard (the aggregate's version column,
	// ADR 0022) did not match the row's current version — another writer
	// persisted a change after this caller loaded the aggregate. The
	// caller must re-load and retry (or surface the conflict), never
	// blindly re-Save the same in-memory copy. Mirrors the fleet's
	// identically-named sentinel (workforce-management ADR 0021,
	// inventory-storage ADR 0019).
	ErrConcurrentModification = errors.New("aggregate was concurrently modified; reload and retry")

	// ErrOpenStandardConflict is returned when a write would leave two
	// open (effective_to IS NULL) standards for the same task_type —
	// a violation of the "one open standard per task type" invariant the
	// partial unique index idx_labor_standards_one_open_per_task_type
	// enforces at the database level (ADR 0022).
	ErrOpenStandardConflict = errors.New("another standard is already open for this task type")
)

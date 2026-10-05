-- Optimistic concurrency + "one open standard per task_type"
-- (see docs/docs/adr/0022-optimistic-concurrency-one-open-standard.md).
--
-- version backs the optimistic-concurrency guard on StandardRepo.Save: an
-- ON CONFLICT (id) DO UPDATE only applies while the row's version still
-- matches the version the caller loaded the aggregate at, so two
-- concurrent DefineStandard calls can never both close the same prior
-- standard (the fleet's ADR-0021 pattern, ported from
-- workforce-management).
ALTER TABLE labor_standards ADD COLUMN version INTEGER NOT NULL DEFAULT 1;

-- The database-level backstop for the "one open standard per task_type"
-- invariant: at most ONE row per task_type may have a NULL effective_to
-- (the currently-active standard). DefineStandard closes the prior record
-- and inserts the new one in the SAME transaction, so the application
-- never intends to violate this -- the index turns any bug or race that
-- would into a constraint error (23505) instead of silent data corruption.
CREATE UNIQUE INDEX idx_labor_standards_one_open_per_task_type
    ON labor_standards (task_type)
    WHERE effective_to IS NULL;

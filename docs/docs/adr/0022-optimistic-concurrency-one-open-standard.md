---
id: 0022-optimistic-concurrency-one-open-standard
slug: /adr/0022-optimistic-concurrency-one-open-standard
title: 0022. Optimistic concurrency and one open standard per task type on labor_standards
sidebar_label: 0022. OCC + one open standard
description: "ADR 0022 — a version column with a guarded upsert on StandardRepo.Save closes the two-writers DefineStandard race (two concurrent POST /standards both closing the same prior standard and leaving two open standards), and a partial unique index (task_type WHERE effective_to IS NULL) makes \"one open standard per task type\" a database-level invariant. Both surface as distinct 409 categories."
---

# 0022. Optimistic concurrency and one open standard per task type on labor_standards

## Status

Accepted — implemented in the same change that introduced this record.
Mirrors the fleet's reference pattern (workforce-management ADR 0021,
inventory-storage ADR 0019), adapted to this service's single mutable
aggregate and its one-open-standard invariant.

## Context

`DefineStandard` is a read-modify-write over `labor_standards`:

```go
prior, _ := uc.Standards.FindCurrentlyActive(ctx, taskType)   // read
// ... inside one unit of work:
prior.Close(now); uc.Standards.Save(ctx, prior)                // write 1
next := standard.New(id, ...); uc.Standards.Save(ctx, next)    // write 2
```

The unit of work makes each REQUEST atomic, but nothing serializes two
concurrent requests against each other. Two `POST /standards` calls for
the same TaskType with different `Idempotency-Key`s (a genuine
double-writer: two operators, or a client that minted a fresh key for a
retry) both read the SAME currently-active standard, both close it, and
both insert their own new open standard:

- the two `Save(prior)` calls both write `effective_to = now` to the same
  row — the second silently overwrites the first (a lost update, though a
  benign one here since both close "now");
- the two `Save(next)` calls insert TWO rows with
  `effective_to IS NULL` for the same `task_type`.

Afterwards `FindCurrentlyActive` returns whichever row the planner
happens to scan first — the "one open standard per task type" invariant
the whole frozen-history model (ADR 0004) rests on is silently broken,
and nothing in the schema or the code detects it.

The same applies to any future caller that loads a standard, mutates it
in memory, and saves: nothing detects that the row moved underneath it.

## Decision

**Add a `version` column to `labor_standards` and a partial unique index
enforcing one open standard per `task_type`.** Both are migration
`0006_standard_version_and_one_open`.

1. **Domain**: `LaborStandard` gains an unexported `version int` (1 from
   `New`, populated by `Rehydrate` from the row read) and a `Version()`
   accessor — inert infrastructure metadata exactly like the fleet's
   reference; no domain business logic reads it.

2. **Repo `Save`** becomes a single version-guarded upsert:

   ```sql
   INSERT INTO labor_standards (..., version) VALUES (..., 1)
   ON CONFLICT (id) DO UPDATE
     SET effective_to = EXCLUDED.effective_to,
         version = labor_standards.version + 1
   WHERE labor_standards.version = $loaded_version
   ```

   `RowsAffected() == 0` means the row exists but its version moved on:
   return `ports.ErrConcurrentModification` (new sentinel, the fleet's
   name and meaning). The update arm deliberately touches ONLY
   `effective_to` and `version` — `Close` is the one mutation the use
   case performs on an existing record, and ADR 0004's frozen columns
   (`expected_seconds`, `effective_from`, ...) must be unpersistable-by-
   accident through this path.

3. **Partial unique index**:

   ```sql
   CREATE UNIQUE INDEX idx_labor_standards_one_open_per_task_type
       ON labor_standards (task_type)
       WHERE effective_to IS NULL;
   ```

   The use case never intends to violate it (it closes the prior record
   in the SAME transaction that opens the new one), so in normal
   operation it never fires. It is the database-level backstop that turns
   any bug, race, or manual write that WOULD leave two open standards
   into a constraint error instead of silent invariant corruption. A
   `23505` on this index maps to a new `ports.ErrOpenStandardConflict`.

4. **HTTP mapping** (`errors.go`): both sentinels map to **409 Conflict**
   with their own RFC 7807 categories — `concurrent-modification`
   ("reload and retry") and `standard-conflict` ("another standard is
   already open for this task type") — distinct from the domain-rule 4xx
   rejections, so a caller can tell a retryable race apart from a
   rejection on the merits.

5. **Use cases are unchanged.** `DefineStandard` already loads → mutates
   → saves inside `atomically`; the version flows through `Rehydrate` →
   aggregate → `Save` transparently. This mirrors the fleet reference's
   finding that no use-case file needed a code change.

## Consequences

**Positive**

- The two-writers race is closed at the row level and proven closed by a
  real two-goroutine concurrent test against testcontainers Postgres
  (exactly one winner, exactly one version increment, the loser gets
  `ErrConcurrentModification`), not a sequential simulation.
- "One open standard per task type" is now enforced by the database
  itself — a property ADR 0004's frozen-history guarantee can actually
  lean on, rather than an assumption about use-case discipline.
- The 409 contract is additive: neither sentinel can fire on any
  existing single-writer flow, so no existing client sees a new status
  code on any path that worked before.

**Negative / accepted**

- A caller receiving `409 concurrent-modification` must re-fetch and
  retry; this repo implements no automatic retry (matches the fleet
  reference and keeps the idempotency-key middleware's replay contract —
  a retried request with the same key gets the cached 409, which is the
  correct answer for that logical request).
- One more column, one more index, one more migration. The partial index
  adds a write-time uniqueness check over the (small) set of open
  standards per task type — three task types today.
- `Save` of a still-open standard (no conflict, `effective_to` NULL →
  NULL) is now an in-place no-op that still advances the version. No
  current caller does this; the fleet reference's guarded-upsert shape
  was kept verbatim rather than special-cased.

## Alternatives considered

- **`SELECT ... FOR UPDATE` across the use case.** Would require holding
  a transaction open across the read AND the caller's in-memory mutation
  between two separate repo calls — does not fit this codebase's
  load-then-save use-case shape, and pessimistic locking buys nothing
  over the version guard at this contention level.
- **Retry-on-conflict inside the use case instead of a 409.** Rejected:
  a silent retry loop hides a genuine double-define from the caller, and
  the idempotency-key middleware already defines what a "same logical
  request" retry means. A DIFFERENT logical request that races must be
  told it raced.
- **Relying on the unique index alone (no version column).** The index
  catches the two-open-standards outcome but not the lost-update on the
  shared prior row's close (both transactions close it "now"; neither
  inserts a second open row for the PRIOR standard, so the index never
  fires on that half of the race). The version guard closes that half.

## Verification

- Unit (`internal/domain/standard/standard_test.go`): `Rehydrate`
  preserves the passed version; a non-positive version normalizes to 1;
  `New` starts at 1.
- Unit (`internal/adapters/inbound/http/server_test.go`): both new
  sentinels map to 409 with their own problem categories through the
  real router.
- Integration (`-tags=integration`, testcontainers Postgres,
  `internal/adapters/outbound/postgres/standard_repo_integration_test.go`):
  - `TestStandardRepo_Save_StaleVersionFails` — two sequential loads,
    first Save wins, stale second Save returns `ErrConcurrentModification`
    and its close is verifiably absent on reload.
  - `TestStandardRepo_ConcurrentDefine_ExactlyOneRevision` — the real
    race: two goroutines, synchronized start, exactly one success + one
    conflict, exactly one version increment.
  - `TestStandardRepo_OneOpenStandardPerTaskType` — a second open
    standard of the same task type is rejected with
    `ErrOpenStandardConflict`; a different task type is fine; a new one
    is fine after the prior closes.
  - `TestPostgres_StandardRoundTrip` — version round-trips 1 → 2 across
    a close, and the frozen-history as-of lookups still hold.

---
id: 0014-labor-utilization-idleness
title: 14. Measuring idleness and utilization
sidebar_label: 14. Idleness / utilization
sidebar_position: 14
description: "Why labor-performance now derives per-associate idle gaps from the existing TaskCompleted wire contract, publishes an additive IdleSecondsBefore on TaskPerformanceRecorded, and exposes windowed utilization read models -- without any fulfillment-execution wire change."
---

# 14. Measuring idleness and utilization

## Status

**Accepted** — implemented in the same change that introduced this
record.

## Context

Competitor research (Manhattan Active Labor Management, Blue Yonder
Workforce & Labor Management) treats idle time / utilization as a
first-class labor signal sitting right next to actual-vs-standard
performance scoring — not a separate product. This context already owns
that vocabulary (`TaskPerformance`, `EfficiencyPct`), but has never
measured the gaps BETWEEN tasks: an associate who finishes a PICK and
waits four minutes before claiming the next one is invisible today. That
gap is exactly the signal `workforce-management` needs for a staffing
decision and `warehouse-ops-agent` needs for a flow-balance advisory
(both are later, separate phases — see the fleet-wide idleness plan).

The critical enabling fact, verified against `fulfillment-execution`'s
actual wire contract (ADR-0014 there) before writing any code: its
`TaskCompleted` payload already carries `duration_seconds` (`completedAt
− claimedAt`) and `associate_id`. That means the PREVIOUS task's claim
instant is recoverable, with no upstream change, as
`time − duration_seconds` (the TaskCompleted CloudEvent `time`; ADR 0021). An associate's idle gap is then simply
`claimedAt(task N+1) − completedAt(task N)` — fully computable from data
this service already consumes. Phase 1 of the idleness plan is therefore
scoped to `labor-performance` alone.

`task_type` was not on `fulfillment-execution`'s wire when this ADR was
written (`shared.ParseTaskTypeLenient("")` always resolved `""`), so
per-task-type utilization buckets started out empty. That gap is now
**closed**: `fulfillment-execution`'s own ADR-0023 added `task_type` to
the wire payload, and this service's consumer reads it directly (see
ADR-0003's updated note and README.md's "Known gaps" section).
Per-task-type utilization is populated as of that change; associate-level
utilization was unaffected throughout.

## Decision

### New domain package: `internal/domain/idleness`

A new `IdlePeriod` aggregate, sibling to `performance` and `standard`,
carrying `AssociateId`, `TaskType` (of the task that ENDED the gap — the
one just claimed, not the one that finished before it), `StartedAt`
(previous completion), `EndedAt` (next claim instant), `Seconds`, and
`Capped`. `New` rejects an empty `AssociateId`
(`ErrEmptyAssociateId`) and a non-positive gap (`ErrNegativeGap` —
Kafka is unordered, so a "next" claim landing at or before the previous
completion is ROUTINE, not exceptional; callers skip-and-log rather than
fail). Capping is applied INSIDE the constructor (the domain owns the
rule), never by a caller after the fact.

Ubiquitous language, added to this context's vocabulary:

- **Idle Gap** — the between-task wait for one associate.
- **Utilization** — `1 − idle share` over a window, expressed as a
  percent.
- **Open Gap** — the still-running idle time for an associate idle RIGHT
  NOW (their last completion has no next claim to close the gap yet).
  Computed at READ time as `now − lastCompletedAt`, flagged in the
  response, and NEVER persisted — this context does not invent a
  completion that has not happened.

### Application: derive the gap on the existing consume path

`RecordTaskPerformance.Execute` gained an optional `IdlePeriods
ports.IdlePeriodRepo` dependency. After saving the `TaskPerformance` row
(idempotency already gated on `KafkaEventId`), it resolves the
associate's PRIOR completion — via the existing
`PerformanceRepo.RecentByAssociateID(ctx, id, 1)`, queried BEFORE this
call's own row is saved (a subtle but load-bearing ordering: querying
after Save would see the just-saved row as its own "previous"
completion) — derives `claimedAt = CompletedAt −
ActualSeconds`, and builds an `IdlePeriod`. The write rides the SAME unit
of work as the performance write and the outbox insert (ADR-0010), so a
Kafka redelivery can never double-record either fact.

Every business-rule non-result (no prior completion — a real,
first-observation case, never "infinite idleness"; a negative gap; an
empty `AssociateId` — a robot station) is a nil, not an error, and never
fails the enclosing `RecordTaskPerformance` write. Only a genuine
infrastructure failure (a failing repo call) propagates.

`IDLE_GAP_CAP_SECONDS` (default 3600) bounds a recorded gap's reported
duration so a gap spanning a shift boundary cannot silently poison a
running mean, without this context modeling shifts itself — that stays
workforce-management's domain. A capped gap is stored with `capped:
true`, distinguishing a genuinely short gap from a clipped one.

New use case `GetUtilization`, mirroring `GetAssociateScorecard`'s and
`GetTaskTypePerformance`'s shape: `ForTaskType` and `ForAssociate`, each
composing `PerformanceRepo` (task time) with `IdlePeriodRepo` (idle
time) over a trailing window (default 1h), plus the read-time Open Gap
computation for `ForAssociate`.

### Published Language: additive `IdleSecondsBefore`

`TaskPerformanceRecorded` (`internal/domain/shared/events.go`) gained
`IdleSecondsBefore *int64` — mirroring `EfficiencyPct`'s existing
nullable-pointer pattern exactly. Nil means "not observed" (same three
cases as above), never a fabricated `0`. This is ADDITIVE: the existing
Conformist consumer, `workforce-management`'s `laborperformancecache`
(unmarshaling unknown-field-tolerant JSON), is unaffected until it opts
in to read the new field (Phase 2 of the idleness plan). Both outbound
encoders (`AnalyticsPublisher`, `IntegrationPublisher`) were updated to
marshal the field as `idle_seconds_before`, and `apis/asyncapi.yaml`
documents it on both the analytics and integration envelope schemas. No
new event type, no new topic — the existing `TaskPerformanceRecorded`
message on both `warehouse.labor-performance.analytics` and
`warehouse.labor-performance.events` (ADR-0013) simply carries one more
optional field.

A dedicated `UtilizationObserved` event was considered and rejected as
premature (YAGNI): no consumer today needs a separate message type, and
splitting later — if a genuine reason emerges — is a strictly additive
change from here.

### Inbound surfaces

- **REST**: `GET /task-types/{taskType}/utilization?window=1h` and
  `GET /associates/{associateId}/utilization?window=1h`, RFC 7807 errors,
  new `Utilization` schema and `WindowQueryParam` in `apis/openapi.yaml`
  (Spectral-clean).
- **MCP**: a 4th tool, `get_task_type_utilization`, calling the SAME
  `GetUtilization` use case the REST handler calls — never a parallel
  code path. This keeps the tool surface at 4 of the fleet's 8-tool
  governance cap.
- **Postgres**: new `idle_periods` table
  (`migrations/0003_idle_periods.up.sql`), indexed on
  `(associate_id, ended_at)` and `(task_type, ended_at)` for the two
  windowed-sum queries `GetUtilization` issues. `PerformanceRepo` gained
  two matching windowed-sum methods
  (`SumActualSecondsByTaskType`/`SumActualSecondsByAssociate`) so the
  "task time" half of a utilization computation reuses the existing
  table rather than duplicating it. A memory adapter mirrors both new
  repos for tests and no-DB local runs. Kafka/Postgres integration
  coverage uses testcontainers exclusively (fleet rule) — no
  `DATABASE_URL`/`KAFKA_BROKERS` skip-gate, no hardcoded broker/DB port.

## Consequences

**Positive**

- The idle-gap signal is derivable from data this service already has —
  zero new upstream dependency, zero fulfillment-execution change.
- Additive-only on the wire: every existing consumer of
  `TaskPerformanceRecorded` (today, only this repo's own projector and
  the integration topic's not-yet-live subscriber) is unaffected until it
  chooses to read the new field.
- The MCP tool surface stays well under the fleet's governance cap
  (4/8), leaving room for Phase 3's ops-agent-facing additions elsewhere
  in the fleet without touching this repo again.
- `GetUtilization` reuses `PerformanceRepo` rather than duplicating a
  "task time" query in a new table — one less thing that can drift.

**Negative / accepted**

- `IDLE_GAP_CAP_SECONDS`'s default (3600s) is a judgment call, not a
  measured shift-length. An operator with unusually long or short shifts
  should override it; the code default is a reasonable v1 starting
  point, not a claim about any real facility's schedule.
- Per-task-type utilization is populated: `fulfillment-execution`'s own
  ADR-0023 added `task_type` to the wire payload (a pre-existing,
  separately tracked gap that has since closed — see ADR-0003's updated
  note). Associate-level utilization was unaffected throughout.
- `Open Gap` is a read-time estimate, not a stored fact — a caller
  polling `GetUtilization.ForAssociate` in a tight loop recomputes it
  every time rather than reading a cached value. Acceptable: the whole
  point of "never persist an assumption about ongoing idleness" is that
  it must always reflect `now`, not a stale snapshot.

## Alternatives considered

- **A dedicated `UtilizationObserved` event**, kept strictly separate
  from `TaskPerformanceRecorded`. Rejected for now (see above) as
  speculative scope with no current consumer — the additive field is
  non-breaking, ships in the existing outbox pipeline with zero new
  moving parts, and keeps one event per observed fact. Revisit if a
  genuine "performance vs. utilization" separation need emerges.
- **Have `fulfillment-execution` publish a dedicated `TaskClaimed`
  integration event** instead of deriving the claim instant from
  `duration_seconds`. Rejected: `TaskClaimed`/`LeaseExpired` are
  deliberately in-process only on that side (ADR-0014 there), and
  `duration_seconds` already makes the derivation possible without
  reopening that boundary decision.
- **Query the previous completion AFTER saving the new row**, filtering
  it out by `TaskId`/`KafkaEventId`. Rejected as fragile and
  harder to reason about than simply resolving "previous" BEFORE the
  write — the chosen ordering has no special-casing at all.

## Verification

- Unit: `internal/domain/idleness/idleness_test.go` — every constructor
  rejection branch (empty associate, negative/zero gap), the capping
  rule (including the exact-boundary case), `Rehydrate`'s
  trust-persisted-state contract, and `UtilizationPct`'s
  never-divide-by-zero invariant. `gremlins unleash
  ./internal/domain/idleness`: 100% mutator coverage, 100% test efficacy.
- Unit: `internal/application/usecases/usecases_test.go` — idle-gap
  derivation end to end through the real in-memory stack (first
  observation, second-completion happy path, negative-gap
  skip-not-fail, empty-associate skip, capping at a configured max and
  at the default, the optional-nil-`IdlePeriods` path, and the
  infrastructure-error propagation path), plus `GetUtilization` (never-
  observed subject, real recorded rows, the Open Gap computation both
  present and absent, and the window default).
- Unit: `internal/adapters/inbound/http` and
  `internal/adapters/inbound/mcp` — the two new REST endpoints and the
  new MCP tool, exercised against the real use case over the real
  in-memory repos.
- Integration (`-tags=integration`, testcontainers Postgres
  `postgres:16-alpine`, never a `DATABASE_URL` skip-gate):
  `internal/adapters/outbound/postgres/idle_period_repo_integration_test.go`
  — a round trip through the real `idle_periods` table (save, both
  windowed sums, `LastEndedAtByAssociate`, distinct-associate count) and
  an end-to-end `RecordTaskPerformance` execution against the real
  outbox-backed unit of work that derives and persists a real
  `idle_periods` row. Run and passed locally against a live
  testcontainers Postgres.
- `make check-all` (fmt-check, vet, build, lint, test -race, 90%
  coverage gate) run locally: 94.6% coverage on
  `./internal/domain/...,./internal/application/...,./internal/analytics/...`.
- `spectral lint` clean on all three specs (`apis/openapi.yaml`,
  `apis/openapi-reports.yaml`, `apis/asyncapi.yaml`).

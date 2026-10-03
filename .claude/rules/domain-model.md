# Domain model: ubiquitous language, aggregates, invariants, use cases

## Why this context exists

Competitor research (Manhattan Active Labor Management, Blue Yonder
Workforce & Labor Management) converges on ONE capability neither
`workforce-management` nor `fulfillment-execution` has today: engineered
labor standards (expected time-per-task-type) and actual-vs-standard
performance scoring. Both vendors treat it as a distinct product capability,
not a sub-feature of shift/schedule planning.

`workforce-management` owns "who is on shift, on which PATH, at what rate"
and explicitly stops at the path boundary, never linking an associate to a
task. `fulfillment-execution` owns Task/Station lifecycle and has zero
concept of a "standard" to measure a completion against — adding one there
would be scope creep into a domain neither aggregate has any business
modeling. A new bounded context is the correct home because it inherits
neither sibling's boundary promise. See ADR 0002.

It is a **Supporting** subdomain (not Core, not Generic): valuable and
real, but it measures execution quality against a standard someone else
configures — it does not define the fulfillment work itself.

## Bounded-context boundary (non-negotiable)

- Pure Kafka **consumer** of `fulfillment-execution`'s `TaskCompleted`
  event on `warehouse.fulfillment.events` (shared/fan-out topic,
  `wes-work-planning` also consumes it under its own consumer group). Only
  the full CloudEvents type
  `com.warehouse.wes.fulfillment-execution.task.TaskCompleted` is acted on;
  every other type is silently skipped, not an error.
- Separate Go module/repo: no Go import from and no write access to
  `fulfillment-execution` or `workforce-management`'s aggregates. No
  outbound REST or MCP call to ANY sibling context — everything needed
  (`AssociateId`, `TaskType`, `DurationSeconds`) already travels on the
  Kafka event.
- This context DOES expose its own Open Host Services: the OLTP REST API,
  the reports API (`cmd/labor-reports`) and the MCP server (`cmd/mcp`).
  The `labor_mfe` remote in `web/` calls the OLTP API; `warehouse-ops-agent`
  reads the MCP server and the reports API.
- `workforce-management` is a downstream Customer: it consumes this
  context's `TaskPerformanceRecorded` integration event on
  `warehouse.labor-performance.events` (ADR 0013). This context has zero
  Go-import, REST or Kafka dependency on `workforce-management`.

See ADR 0002, ADR 0003, ADR 0013, ADR 0015.

## Ubiquitous language

| Term | Meaning |
| --- | --- |
| **LaborStandard** | Aggregate root: expected duration for a `TaskType` (PICK/PACK/SLAM — mirrors `fulfillment-execution`'s `task.Type` exactly), with append-only revision history. `ExpectedSeconds` must be > 0. ONE active standard per TaskType at any time. Optional caller-supplied `TravelComponentSeconds` (ADR 0015). |
| **TaskPerformance** | Aggregate root: one scored, already-completed task. Immutable once recorded (event-sourced fact from Kafka, no update/delete use case). |
| **Scorecard** | Read model (NOT a stored aggregate) — projection over `TaskPerformance` rows per associate: task count, mean `EfficiencyPct`, per-`TaskType` breakdown, `trend`, `coachingFlag`. |
| **TaskTypePerformance** | Fleet-wide (all-associates) read model per TaskType — the "labor monitoring" view, independent of any one associate. |
| **EfficiencyPct** | `100 * StandardSecondsAtCompletion / ActualSeconds`, nullable, never computed by dividing by zero. |
| **IdlePeriod** | Aggregate root (`internal/domain/idleness`): one associate's between-task wait, from the previous completion to the next claim (`CompletedAt − ActualSeconds`). Capped at `IDLE_GAP_CAP_SECONDS` (default 3600, `Capped` flag). ADR 0014. |
| **Utilization** | `1 − idle share` over a trailing window (default 1h), as a nullable percent. An associate's still-running **open gap** is computed at read time and never persisted. |

## LaborStandard invariants

- `ExpectedSeconds` must be > 0.
- **Append-only history.** `DefineStandard` for a TaskType that already has
  an active standard does NOT overwrite in place — it closes the prior
  standard's effective range (`EffectiveTo`) and starts a new one, so
  already-recorded `TaskPerformance` rows' frozen
  `StandardSecondsAtCompletion` stay historically accurate. Mirrors
  `workforce-management`'s "AssignLabor ends the prior assignment rather
  than rejecting" pattern. See ADR 0004.
- `TravelComponentSeconds`, when supplied, must satisfy
  `0 <= t <= ExpectedSeconds`. It is never computed or validated against a
  live lookup (ADR 0015).

## TaskPerformance invariants

- **Immutable once recorded.**
- **Idempotent on the CloudEvents `id`** (stored in the `event_id` column), not `TaskId` (which
  could in principle be reused after a very long time). Recording the same
  `event_id` twice is a no-op, mirroring the `ProcessedEvents`
  idempotency-gate pattern every analytics projector in this fleet uses.
- **`EfficiencyPct` never divides by zero.** `ActualSeconds<=0` (e.g. a
  `TaskCompleted` whose `duration_seconds` is 0 because no claim-timestamp
  existed) or `StandardSecondsAtCompletion<=0` (no active standard existed
  for that TaskType at completion time) both yield `EfficiencyPct=nil` —
  a real, expected business fact, never an error and never a fabricated
  number.
- **`StandardSecondsAtCompletion` is frozen at ingestion time** — resolved
  once as "the standard active AS OF `CompletedAt`," not "active right
  now," and never recomputed later from a since-changed standard. This
  means a possibly out-of-order or replayed Kafka message is scored
  against the standard genuinely in force when the task completed.
- **An empty `AssociateId` is legitimate**, not an error — a `TaskCompleted`
  from a station with no checked-in occupant (e.g. a robot station) is
  still recorded and counted in `GetTaskTypePerformance`, just excluded
  from any per-associate scorecard.

## Domain events (past tense)

`LaborStandardDefined`, `LaborStandardRevised`, `TaskPerformanceRecorded`.
Default publisher is a log publisher; `EVENT_PUBLISHER=kafka` fans all
three onto `warehouse.labor-performance.analytics` (the dedicated analytics
topic feeding `cmd/labor-projector`, ADR 0007), and `TaskPerformanceRecorded`
also onto the integration topic `warehouse.labor-performance.events`
(ADR 0013, consumed by `workforce-management`). Both are routed through the
transactional outbox when `DATABASE_URL` is set (ADR 0010).
`TaskPerformanceRecorded` carries the additive nullable
`idle_seconds_before` (ADR 0014).

## Use cases (application layer)

1. `DefineStandard(taskType, expectedSeconds, travelComponentSeconds?) ->
   LaborStandard` — closes any prior active standard for that TaskType,
   starts a new one.
2. `GetStandard(taskType) -> LaborStandard | 404`
3. `RecordTaskPerformance(taskId, associateId, taskType, actualSeconds,
   completedAt, kafkaEventId) -> TaskPerformance` — the Kafka-consumer-
   driven use case, called from the inbound Kafka adapter only, never
   from HTTP. Idempotent on `kafkaEventId`. Also derives and saves the
   associate's `IdlePeriod` in the same unit of work (ADR 0014).
4. `GetAssociateScorecard(associateId) -> Scorecard | 404` — 404 only when
   this service has NEVER recorded a TaskPerformance row for this
   associate; an associate with 1+ rows but all-nil efficiency returns 200
   with `meanEfficiencyPct: null`.
5. `GetTaskTypePerformance(taskType) -> TaskTypePerformance` — always 200,
   including a zero-count result for a TaskType never recorded.
6. `GetUtilization.ForTaskType / ForAssociate(subject, window)` — task time
   plus idle time over a trailing window (default 1h); `ForAssociate` adds
   the read-time open gap (ADR 0014).

## REST API (OLTP, `apis/openapi.yaml`)

| Method | Path | Use case |
| --- | --- | --- |
| `POST` | `/standards` | DefineStandard |
| `GET` | `/standards/{taskType}` | GetStandard |
| `GET` | `/associates/{associateId}/scorecard` | GetAssociateScorecard |
| `GET` | `/task-types/{taskType}/performance` | GetTaskTypePerformance |
| `GET` | `/task-types/{taskType}/utilization` | GetUtilization.ForTaskType |
| `GET` | `/associates/{associateId}/utilization` | GetUtilization.ForAssociate |
| `GET` | `/healthz` | Liveness probe |

There is deliberately **no** REST endpoint for `RecordTaskPerformance` — it
is exclusively Kafka-consumer-driven. Every error is RFC 7807
`application/problem+json`, identical shape across every fleet service.

MCP (`cmd/mcp`): read-only tools `get_associate_scorecard`,
`get_task_type_performance`, `get_labor_standard`,
`get_task_type_utilization`; resource template
`scorecard://labor/{associateId}`; prompt `review_associate_performance`.

## Inbound Kafka contract (`apis/asyncapi.yaml`, inbound section)

Subscribes to `warehouse.fulfillment.events`. Every message is a
CloudEvents 1.0 event, structured mode (ADR 0021) — the only envelope:

```json
{
  "specversion": "1.0",
  "id": "4f1c2a7e-9d31-4a6b-8f0e-6b2c1d5e7a90",
  "source": "/warehouse/fulfillment-execution",
  "type": "com.warehouse.wes.fulfillment-execution.task.TaskCompleted",
  "subject": "task-1",
  "time": "2026-08-29T22:00:00Z",
  "datacontenttype": "application/json",
  "dataschema": "urn:warehouse:fulfillment-execution:events:TaskCompleted:v1",
  "data": {
    "task_id": "...", "station_id": "...", "work_unit_id": "...",
    "associate_id": "...", "duration_seconds": 52, "task_type": "PICK"
  }
}
```

`associate_id`, `duration_seconds` and `task_type` are OPTIONAL on the
wire — an older payload predating an enrichment omits them, and this
service's JSON unmarshaling degrades them to `""`/`0` (the same "no
occupant"/"unmeasurable"/"unclassified" business facts already modeled,
not an error).

`task_type` is on the wire since `fulfillment-execution` ADR-0023 and goes
through `shared.ParseTaskTypeLenient`. An unrecognized value (e.g. `REBIN`)
or an absent field resolves to `""` (unclassified) — still recorded and
counted, but never resolves a `LaborStandard` and never appears under
`GetTaskTypePerformance` (which requires PICK/PACK/SLAM). The analytics
side reports it as `UNCLASSIFIED` (ADR 0007).

## v1 scope decisions still in force

- **No automatic pay-for-performance / bonus calculation** (a real
  Manhattan feature, "Pay for Performance") — this context surfaces the
  number, a human/other system decides what to do with it.
- **No gamification or automated coaching workflows.** v1 (ADR 0005) ships
  the passive `Trend`/`CoachingFlag` signal only, a human reads it.
- **No outbound REST or MCP dependency on any sibling context.** Siblings
  may read this context's own surfaces; this context never calls theirs.
- **No gating/blocking of `fulfillment-execution`** — a below-standard
  associate is still allowed to claim tasks. Visibility, not enforcement.

## Now built (fleet-parity pass, no longer deferred from the original v1)

Helm chart, godog/BDD acceptance tests, Postgres integration tests
(testcontainers), Gremlins mutation-testing gate, arch-go fitness tests,
Spectral linting (all 3 specs), CodeQL/Scorecard/Trivy, Docker publish +
release automation, full Docusaurus site, the `labor_mfe` remote (`web/`),
the analytics data product (ADR 0007), the MCP server (ADR 0009), the
transactional outbox (ADR 0010), the integration topic (ADR 0013),
idleness/utilization (ADR 0014), the travel component (ADR 0015), and (per
ADR 0012) the REST/MCP auth layer was added then explicitly removed again — see
`.claude/rules/adrs-and-decisions.md`.

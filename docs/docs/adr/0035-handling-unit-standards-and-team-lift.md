---
id: 0035-handling-unit-standards-and-team-lift
slug: /adr/0035-handling-unit-standards-and-team-lift
title: "35. Engineered standards per handling unit and a declared heavy-case team lift"
sidebar_label: "35. Handling-unit standards, team lift"
description: "ADR 0035 — a LaborStandard can be declared for (taskType, handlingUnit in EACH|CASE|PALLET) so a case handle has its own expected time distinct from an each pick; the most specific standard wins; a CASE standard may declare a team-lift weight threshold and standard time. Declarative through this service's own REST; TaskCompleted carries no handling unit today, so the resolution input is an explicit optional field, never a guess."
---

# 35. Engineered standards per handling unit and a declared heavy-case team lift

## Status

**Proposed** (2026-10-24). Additive: a completion that carries no handling unit
is scored exactly as before. Contract P11-5 of the mastercase / GTIN adoption
(`.hermes/plans/mastercase-briefs/contracts.md`, sections 0, 3, 7 and 11 of the
warehouse-systems workspace). Builds on [ADR 0004](./0004-standard-frozen-at-completion-time-not-recomputed.md)
(the standard is frozen as of the completion instant), [ADR 0015](./0015-optional-travel-component-on-labor-standard.md)
(declarative, caller-supplied standard parameters), [ADR 0021](./0021-cloudevents-mandatory-event-envelope.md)
and [ADR 0022](./0022-optimistic-concurrency-one-open-standard.md) (one open
standard, which this ADR widens).

## Context

An engineered standard answers "how long should a PICK take?" and is keyed by
`TaskType` alone (`PICK`, `PACK`, `SLAM`). The mastercase program makes a second
question real: a pick of one sealed case (a case handle) is a different motion
from a pick of one each, and scoring it against the each-pick yardstick makes
every case picker look wrong in one direction or the other. A heavy case adds a
third: above some weight one operator cannot lift it, so the work needs two
operators and the honest yardstick is the time of a team lift.

What the boundary allows ([AGENTS.md](https://github.com/IQVO/labor-performance/blob/develop/AGENTS.md)
non-negotiable 1, ADR 0002, ADR 0003, ADR 0015):

- This service is a **pure Kafka consumer of `fulfillment-execution`'s
  `TaskCompleted`, nothing else**. It never calls any sibling context over REST
  or MCP, and cross-context data must be local or declarative.
- It therefore does **not** consume product-master's `ProductPackLevelDeclared` /
  `ProductPackLevelRetired` events, although contract section 11 allows an
  event-fed pack projection where a repo's AGENTS.md permits one. Here it does
  not: the only topic this service subscribes to is the fulfillment topic. Pack
  facts are not needed in any case. A standard is per handling unit, not per
  SKU, so no `unitsPerPack`, cube or case weight is looked up. What the standard
  needs from the pack world (the unit, and optionally the case weight) is
  **declared**: the standard itself through this service's own REST
  (`POST /standards`), the per-completion unit and weight on the completed task
  event, which is the one input this service already consumes.

What `TaskCompleted` carries today (read from `fulfillment-execution`'s
`origin/develop` `apis/asyncapi.yaml`): `task_id`, `station_id`, `work_unit_id`,
`associate_id`, `duration_seconds`, `task_type` (`PICK`, `PACK`, `SLAM`, `REBIN`,
`DISPATCH`, `ARRIVAL`, `RELOCATE`, `REPLENISH`), `order_ref`, `line_no`, and for
RELOCATE/REPLENISH only `sku`, `from_bin`, `to_bin`, `quantity`,
`relocation_ref`, `request_ref`. **It carries no handling unit, no case marker
and no weight.** Its `quantity` is eaches and exists only on the two stock-move
task types, so it is not a marker. `fulfillment-execution` models the case pick
as a `PICK` task with a pick plan unit and a `BREAK_CASE` task (its ADR 0054,
`internal/domain/task/break.go`), carried on `WorkReleased` (`pick`,
`case_break`), not on `TaskCompleted`. Guessing a unit from `quantity`, from
`sku` or from the work unit id would fabricate a fact, so this ADR does not.

## Decision

1. **A standard is declared per `(taskType, handlingUnit)`.** `handlingUnit` is
   optional: `EACH`, `CASE` or `PALLET` (the handling-unit subset of the fleet's
   pack-level vocabulary; `INNER` has no flow in v1 and is rejected). Absent or
   empty is the task-type-only standard and is exactly what a standard has been
   until now. At most one standard is open per `(taskType, handlingUnit)`:
   defining a `CASE` standard never closes the task-type-only one, and revising
   it closes only the previous `CASE` standard (append-only history, ADR 0004,
   unchanged). The partial unique index of ADR 0022 is widened to
   `(task_type, handling_unit)` and keeps its name, so the conflict mapping is
   unchanged. Migration 0007.
2. **Most specific wins.** Scoring a completion that carries a handling unit
   uses the standard active at `CompletedAt` for exactly `(taskType, unit)`;
   when none exists it falls back to the task-type-only standard; when neither
   exists the completion is recorded unscorable (`EfficiencyPct` null, never a
   fabricated number). A completion with **no** handling unit consults only the
   task-type-only standard: today's behaviour, byte for byte, including the
   existing tests, the REST bodies and the event payloads. A `CASE` completion
   is never scored against a `PALLET` standard, or the reverse.
3. **A heavy-case team lift is a declared parameter of a `CASE` standard.**
   `teamLift: {thresholdG, expectedSeconds}` on `POST /standards`: a case
   **strictly heavier** than `thresholdG` grams needs two operators
   (`TeamLiftOperators = 2`, a constant) and is scored against
   `expectedSeconds`, the engineered elapsed time of a team lift, instead of the
   standard's own `expectedSeconds`. A case weighing exactly the threshold is
   still a one-operator lift. Invariants: only valid with `handlingUnit` `CASE`
   (`team-lift-requires-case-unit`, 422), `thresholdG > 0` and
   `expectedSeconds > 0` (422), both or neither. The columns carry the same
   checks in Postgres.
4. **The resolution input is explicit.** Because `TaskCompleted` v1 carries
   neither, the consumer reads two **optional, additive** fields from its data:
   `handling_unit` (`EACH|CASE|PALLET`) and `weight_g` (integer grams of the
   handled case). They are parsed leniently: absent, null, of the wrong type, an
   unrecognised unit (for example `INNER`) or a non-positive weight degrade to
   "absent" and never fail the decode or dead-letter a real completion. An
   unknown weight is never a team lift. `fulfillment-execution` does not publish
   these fields today, so **on all traffic published so far the behaviour is
   unchanged**; the feature starts to take effect when the producer adds them
   (a follow-up in that repo, an additive field on `TaskCompleted` v1, same
   rule as every other optional field there). Nothing is looked up to fill the
   gap.
5. **What is recorded and published.** The unit the completion carried and
   whether the team-lift time scored it are frozen on the `TaskPerformance` row
   (`handling_unit`, `team_lift`; defaults `''` and `false`) next to
   `StandardSecondsAtCompletion`. `TaskPerformanceRecorded` (analytics and
   integration topics) gains the additive `handling_unit` and `team_lift`,
   **only when the completion carried a unit**; `LaborStandardDefined` /
   `LaborStandardRevised` gain the additive `handling_unit`,
   `team_lift_threshold_g` and `team_lift_expected_seconds`, only when set.
   Omitted, never null, so a completion or standard without them keeps the
   previous payload. Same event types, same `dataschema` versions: v1 stays v1
   ([ADR 0021](./0021-cloudevents-mandatory-event-envelope.md)).
6. **REST (additive).** `POST /standards` accepts `handlingUnit` and `teamLift`;
   a standard response echoes them (`teamLift` with `operators: 2`) and omits
   both for a task-type-only standard. `GET /standards/{taskType}` accepts an
   optional `handlingUnit` query and answers "which standard applies to this
   handle": the unit's own standard, else the task-type-only one, 404 when
   neither exists; the body's `handlingUnit` says which answered. An unknown
   unit is 400 (`unknown-handling-unit`). Without the query the answer is the
   task-type-only standard, as before.
7. **Infra values.** None. No new env var, consumer group, topic, Kafka reader
   or chart value; the Helm chart is unchanged. `INFRA VALUES REQUIRED`: none.

## Consequences

### Easier

- A case pick, a pallet move and an each pick each get their own yardstick, and a
  heavy case is judged by the team-lift time instead of making a two-person job
  look like a slow one-person job.
- No sibling dependency and no new consumer: the boundary of ADR 0002 and 0003
  holds, and the change is additive on every surface (REST, both event payloads,
  the database).

### Harder / limits

- **Until `fulfillment-execution` emits `handling_unit` (and `weight_g`), nothing
  changes for live traffic.** The declared standards exist and are queryable, and
  the scoring path is covered end to end by tests with a real Kafka broker and
  Postgres, but production completions fall back to the task-type-only standard.
  The producer follow-up is not part of this change.
- **`BREAK_CASE` is not modelled.** It is a task type in `fulfillment-execution`
  but not one of this service's three (`PICK`, `PACK`, `SLAM`); a `BREAK_CASE`
  completion is recorded unclassified, as `REBIN` and the others are today.
  Giving it its own standard means a fourth task type through the REST path
  enums, the report dimension and the web screen, which is a separate ADR. A case
  pick is `PICK` with unit `CASE`.
- **Team lift is a time yardstick, not a labour-hours rule.** The completed task
  names one associate, so each is scored on elapsed time against the team-lift
  standard; the second operator's effort is not attributed. `operators: 2` is
  reported, not multiplied.
- **The weight is as fresh as the producer's declaration.** This service cannot
  check a case weight, same trade as the travel component in ADR 0015.
- **Analytics and the web screen do not yet split by handling unit.** The report
  rollup still buckets by task type; a handling-unit standard counts into its
  task type's `standards defined` figure. The MCP read tools and the `labor_mfe`
  screen are unchanged (they read the task-type-only standard). Splitting the
  report and the screens by unit is a follow-up.
- A rollback of migration 0007 deletes the handling-unit standards (the
  one-open-per-task-type index cannot hold them); task performance rows keep
  their score and lose only the unit columns.

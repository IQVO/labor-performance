---
title: Subdomain Classification
sidebar_label: Subdomain Classification
description: Why Labor Performance is a Supporting subdomain, with the aggregates and invariants that follow from it.
---

# Subdomain Classification

## Verdict: **Supporting subdomain**

Labor Performance does not define the fulfillment work itself — it only
measures how well it is executed against a standard someone else configures.
It is genuinely useful (both Manhattan and Blue Yonder ship it as a
first-class capability), but it is not what differentiates a fulfillment
operation the way `fulfillment-execution`'s Pick/Pack/SLAM task lifecycle or
`inventory-storage`'s chaotic-storage inventory truth do.

| Context | Classification | Reasoning |
| --- | --- | --- |
| `fulfillment-execution` | Core | The Pick/Pack/SLAM task lifecycle; throughput and accuracy at scale. |
| `inventory-storage` | Core | Owns inventory truth: bin-accurate location + usable inventory. |
| `wes-work-planning` | Core | The conductor — waveless release and flow balance. |
| `workforce-management` | Supporting | Labor & workforce allocation: important, industry-common, not the differentiator. |
| **`labor-performance`** | **Supporting** | **Measures execution quality against a configured standard — valuable, but not the differentiator itself.** |
| `facility-layout` | Generic | Physical warehouse structure, extracted once rather than duplicated. |

## What "Supporting" buys and obliges

Classifying a subdomain Supporting (not Core, not Generic) means:

- **It is genuinely part of the business**, unlike a Generic subdomain —
  engineered labor standards are a real competitive-adjacent capability
  (Manhattan and Blue Yonder both ship it), not a commodity integration
  surface.
- **It does not carry Core-level investment**, but it is not skipped
  either — this service still carries the full fleet-standard rigor (BDD,
  mutation testing, arch-fitness tests, 90%+ domain/application coverage)
  because "Supporting" is a business-differentiation classification, not a
  quality bar.
- **It is a pure Customer of a Core context's Open Host Service** — it never
  gets write access to `fulfillment-execution`'s Task aggregate, and it
  never influences that context's decisions (no gating, no blocking). It
  is in turn a Supplier to `workforce-management`, which consumes its
  `TaskPerformanceRecorded` integration event (ADR 0013).

## Aggregates & invariants

### LaborStandard

The aggregate root for "how long a task TYPE should take."

- `ExpectedSeconds` must be > 0 — a standard that says a task should take
  zero or negative seconds is not a business fact.
- An optional, caller-supplied `TravelComponentSeconds` breakdown must be
  `>= 0` and `<= ExpectedSeconds` when present. This service never
  computes or validates it against a live distance lookup — see
  [ADR 0015](/docs/adr/0015-optional-travel-component-on-labor-standard).
- **Append-only history.** `DefineStandard` for a TaskType that already has
  an active standard does NOT overwrite it in place — it closes the prior
  standard's effective range (`EffectiveTo`) and starts a new one. This
  keeps already-recorded `TaskPerformance` rows' frozen
  `StandardSecondsAtCompletion` values historically accurate even after a
  later revision. Mirrors `workforce-management`'s own "AssignLabor ends
  the prior assignment rather than rejecting" pattern — a revision closes
  the old record rather than erroring. See
  [ADR 0004](/docs/adr/0004-standard-frozen-at-completion-time-not-recomputed).
- ONE ACTIVE standard per TaskType at any given time.

### TaskPerformance

The aggregate root for one scored, already-completed task — an
event-sourced fact from Kafka, not something a human edits.

- **Immutable once recorded.** No update/delete use case anywhere in the
  application layer.
- **Idempotent on the CloudEvents `id`** (ADR 0021), not `TaskId` (which
  could in principle be reused after a very long time). Recording the same
  event `id` twice is a no-op, never a double-count — mirrors the
  `ProcessedEvents` idempotency-gate pattern every analytics projector in
  this fleet already uses.
- **`EfficiencyPct` never divides by zero.** `ActualSeconds<=0` (an
  unmeasurable completion — e.g. a `TaskCompleted` whose `duration_seconds`
  is 0 because no claim-timestamp existed to compute it from) or
  `StandardSecondsAtCompletion<=0` (no active standard existed for that
  TaskType at completion time) both yield `null`, not an error and not a
  fabricated number.
- **`StandardSecondsAtCompletion` is frozen at ingestion time.** Resolved
  once — the standard active "as of `CompletedAt`," not "active right
  now" — and never recomputed later from a since-changed standard.
- **An empty `AssociateId` is legitimate**, not an error — a `TaskCompleted`
  from a station with no checked-in occupant (e.g. a robot station) is
  still recorded and counted in `GetTaskTypePerformance`, just excluded
  from any per-associate scorecard.

### Scorecard (read model, not a stored aggregate)

A projection over `TaskPerformance` rows, per associate: task count, mean
`EfficiencyPct` across tasks that have one, breakdown by `TaskType`. A 404
means "this service has never recorded a row for this associate" — distinct
from "the associate has rows but no numeric score yet" (which is a 200 with
`meanEfficiencyPct: null`). It also carries a `trend`
(`IMPROVING`/`DECLINING`/`STABLE`/`INSUFFICIENT_DATA`) and a `coachingFlag`
computed from the associate's most recent (up to 10) scored tasks — see
[ADR 0005](/docs/adr/0005-associate-trend-and-coaching-flag).

### IdlePeriod

The aggregate root for one associate's between-task wait
([ADR 0014](/docs/adr/0014-labor-utilization-idleness)), derived when a
`TaskCompleted` arrives: from the associate's previous completion to this
task's claim instant (`CompletedAt − ActualSeconds`).

- Rejects an empty `AssociateId` and a non-positive gap. Both are routine
  on an unordered stream, so the use case skips them rather than failing
  the enclosing `RecordTaskPerformance`.
- Capped at `IDLE_GAP_CAP_SECONDS` (default 3600) inside the constructor;
  a clipped gap is stored with `Capped: true`.
- A still-running **open gap** (an associate idle right now) is computed
  at read time only and never persisted. Decided 2026-10-06: it is
  reported per associate only; at task-type scope `openGapSeconds` is
  always 0 by design.

## Ubiquitous language

The full glossary — every term mapped to its code identifier, with the
terms whose code name differs flagged — lives on the
[Ubiquitous Language](./ubiquitous-language.md) page. The core terms are
**LaborStandard**, **TaskPerformance**, **StandardSecondsAtCompletion**,
**EfficiencyPct**, **TaskType** (PICK, PACK, SLAM — mirrors
`fulfillment-execution`'s `task.Type` exactly), **Scorecard**,
**TaskTypePerformance**, **Idle Gap** (`IdlePeriod`) and **Utilization**.

## Domain events (past tense)

`LaborStandardDefined`, `LaborStandardRevised`, `TaskPerformanceRecorded`.
The default publisher only logs them. With `EVENT_PUBLISHER=kafka` all three
go to `warehouse.labor-performance.analytics`, which feeds this service's
own analytical projector (ADR 0007). `TaskPerformanceRecorded` also goes to
the integration topic `warehouse.labor-performance.events` (ADR 0013), which
`workforce-management` consumes. It carries an additive, nullable
`idle_seconds_before` field (ADR 0014). With Postgres configured, both
topics are fed through the transactional outbox (ADR 0010). Full type
strings, keys and payloads: [Domain Events](./domain-events.md). Where
this classification sits on a ddd-crew Core Domain Chart:
[Core Domain Chart](./core-domain-chart.md).

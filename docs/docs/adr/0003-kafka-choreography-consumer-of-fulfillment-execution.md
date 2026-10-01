---
id: 0003-kafka-choreography-consumer-of-fulfillment-execution
slug: /adr/0003-kafka-choreography-consumer-of-fulfillment-execution
title: 0003. Kafka choreography consumer of fulfillment-execution, no REST dependency
sidebar_label: 0003. Kafka-only consumer of fulfillment-execution
description: ADR 0003 — this service is a pure Kafka consumer of TaskCompleted, never a synchronous caller of fulfillment-execution or workforce-management.
---

# 0003. Kafka choreography consumer of fulfillment-execution, no REST dependency

## Status

Accepted. Established with the initial implementation of this bounded context.

> **Envelope superseded by [ADR-0021](./0021-cloudevents-mandatory-event-envelope.md).** The flat envelope (`event_id`/`event_type`/`occurred_at`) described below is retired: every message is now a CloudEvents 1.0 event. The rest of this decision stands.

## Context

This service's core job — scoring a completed task against a standard —
needs three facts about that completion: `AssociateId`, `TaskType`, and
`DurationSeconds`. All three conceptually belong to `fulfillment-execution`
(Task lifecycle) or `workforce-management` (associate identity). Two
integration shapes were available:

1. **Synchronous HTTP calls** to `fulfillment-execution` and/or
   `workforce-management` at scoring time — the shape `order-management`
   originally used for `inventory-storage`/`wes-work-planning` before its
   own choreographed-release redesign (see that repo's ADR 0005).
2. **Asynchronous Kafka consumption** of an event `fulfillment-execution`
   already publishes, matching the proven
   `wes-work-planning` ← `fulfillment-execution`/`order-management`
   consumer pattern (`wes-work-planning`'s own `RecordCompletion`/
   `EnqueueWorkUnit` consumers).

Forces favoring option 2:

- **This service's entire job IS reacting to a completed task — a fact
  that already happened.** There is no synchronous request/response this
  service needs to make; it never asks fulfillment-execution to do
  anything, it only observes what already occurred. That is the textbook
  shape for choreography over orchestration.
- **`fulfillment-execution` already publishes `TaskCompleted`** on
  `warehouse.fulfillment.events`, and the sibling
  `feature/labor-performance-hooks` PR in that repo (verified this session
  against its actual `internal/adapters/outbound/kafka/publisher.go`)
  enriches that SAME event with `AssociateId` and `DurationSeconds` — no
  new topic, no new publisher, no synchronous endpoint had to be built on
  that side at all. This build is 100% additive from
  `fulfillment-execution`'s point of view.
- **A synchronous HTTP dependency would invert the coupling direction this
  fleet's DDD reference docs establish.** `fulfillment-execution` is Core;
  this context is Supporting. Making Core block on a Supporting-context
  HTTP call (or, worse, having Supporting poll/call Core synchronously at
  scoring time, adding latency and a new failure mode to every task
  completion) is exactly the anti-pattern `wes-work-planning`'s own
  choreographed-release migration eliminated for
  `order-management` → `wes-work-planning`.
- **`workforce-management`'s own boundary already forbids the join this
  service might otherwise be tempted to make.** That context "never links
  an associate to a specific task" (see
  [ADR 0002](./0002-new-bounded-context-not-extension-of-workforce-or-fulfillment.md)),
  so there is no synchronous endpoint on that side to call in the first
  place — `AssociateId` must come from wherever it is already resolved,
  which is `fulfillment-execution`'s station-occupant lookup at publish
  time.

## Decision

**This service is a pure Kafka consumer.** It:

- Subscribes to `warehouse.fulfillment.events` — the SAME shared,
  fan-out topic `wes-work-planning` already consumes from — under its own
  consumer group (`labor-performance` by default).
- Acts only on `event_type == "TaskCompleted"`; every other event type on
  that shared topic is silently skipped, mirroring
  `wes-work-planning`'s own consumer's skip-unrecognized-event-type
  behavior.
- **MUST NOT** import any Go package from `fulfillment-execution` or
  `workforce-management` — it is a separate Go module in a separate
  repository, and the inbound Kafka adapter's `taskCompletedData` struct
  is this context's own private mirror of the wire shape, not a shared
  type.
- **MUST NOT** call either sibling context synchronously over HTTP, ever
  — it has zero REST dependency on either, matching the "everything it
  needs is already on the Kafka event" design.
- Exposes its own REST API (`POST /standards`, `GET /standards/{taskType}`,
  `GET /associates/{associateId}/scorecard`,
  `GET /task-types/{taskType}/performance`) as its OWN Open Host Service,
  symmetric with every other context in the fleet — this is this
  context's outbound surface, not an inbound dependency on anyone else.

**Idempotency is keyed on the Kafka message's `event_id`**, not `TaskId` —
a `TaskId` could in principle be reused after a very long time, so the
`ProcessedEvents` gate (mirroring `workforce-management`'s
`internal/application/ports.ProcessedEvents` idempotency-gate pattern)
uses the envelope's own de-duplication key. Unlike `workforce-management`'s
use of that pattern — which gates only an *additive analytics
side-projection* — here it gates the **entire OLTP write path**, since
consuming `TaskCompleted` IS this service's whole job, not a side effect
of it.

**A known, accepted wire-contract gap:** `fulfillment-execution`'s
`TaskCompletedData` (verified this session against its actual
`feature/labor-performance-hooks` publisher) does not carry a `task_type`
field at all. This service resolves `TaskType` as `""` (unclassified) for
every event as a result — see `shared.ParseTaskTypeLenient`'s doc comment.
This is documented, not silently absorbed: CLAUDE.md's explicit instruction
to "degrade gracefully" rather than block the build on that PR's timing
extends to this field too. A `""`-typed `TaskPerformance` is still
recorded and counted in a hypothetical "all types" view, but never
resolves a `LaborStandard` (no lookup is possible without a known type)
and never appears under any `GetTaskTypePerformance` query (those require
one of the three known enum values).

## Consequences

### Easier

- **No new failure mode on the fulfillment-execution hot path.** A task
  completion never blocks, retries, or slows down waiting on this
  service; if this service is down, messages queue in Kafka and are
  processed on recovery, at-least-once.
- **This service can be deployed, scaled, and even go down independently**
  without affecting `fulfillment-execution`'s or `workforce-management`'s
  own availability — the defining benefit of choreography over
  orchestration.
- **Testing is a fake-reader unit test, not a live-broker integration
  test**, for the common case (see `consumer_test.go`), with a real
  broker reserved for the separate, build-tagged
  `consumer_integration_test.go`.

### Harder

- **This service cannot backfill/repair TaskType for already-recorded,
  unclassified rows** without either a fulfillment-execution enrichment
  landing (adding `task_type` to the wire) or a manual reprocessing pass —
  there is no synchronous fallback lookup to fill the gap, by design.
- **A redelivered-but-slightly-different message (e.g. a genuine data
  correction re-published under a NEW `event_id` for the same
  `TaskId`) would be recorded as a second row**, since `TaskId` is treated
  purely as an opaque foreign reference, not a repository key. This is
  accepted as consistent with `TaskPerformance` being described as
  "immutable once recorded" — a real correction from the source of truth
  is a new fact, not an edit of the old one.

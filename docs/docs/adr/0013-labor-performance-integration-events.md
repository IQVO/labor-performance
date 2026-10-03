---
id: 0013-labor-performance-integration-events
title: 13. Labor performance publishes an integration event
sidebar_label: 13. Integration events
sidebar_position: 13
description: "Why labor-performance — the fleet's only pure event sink — now publishes TaskPerformanceRecorded onto a new integration topic, warehouse.labor-performance.events, using the same transactional-outbox fan-out already in place for the analytics topic."
---

# 13. Labor performance publishes an integration event

## Status

**Accepted** — implemented in the same change that introduced this
record.

> **Envelope superseded by [ADR-0021](./0021-cloudevents-mandatory-event-envelope.md).** The flat envelope (`event_id`/`event_type`/`occurred_at`) described below is retired: every message is now a CloudEvents 1.0 event. The rest of this decision stands.

## Context

Every other bounded context in the `warehouse-systems` fleet both
consumes AND publishes at least one integration event: order-management,
inventory-storage, wes-work-planning, fulfillment-execution,
workforce-management and facility-layout all own at least one topic named
`warehouse.<context>.events` that a sibling service subscribes to.

`labor-performance` has never been one of them. It consumes
`fulfillment-execution`'s `TaskCompleted` from the shared
`warehouse.fulfillment.events` topic (ADR 0002/0003 — choreography, not
orchestration) and, since ADR 0007/0010, publishes its own domain events
onto `warehouse.labor-performance.analytics` — but that topic is a
dedicated **analytics** stream, consumed today only by this repo's own
`cmd/labor-projector`. No other bounded context has ever been able to
subscribe to anything this service knows. It is the fleet's only pure
event sink: input flows in, nothing flows back out for anyone else.

That gap has a concrete cost. `workforce-management` needs
per-associate labor-performance facts (to answer "how has this associate
been performing lately" without recomputing it itself) and today can only
get them by calling this service synchronously over REST — a runtime
coupling this fleet otherwise avoids between bounded contexts, and the
kind of dependency a Kafka-based choreography architecture exists to
remove. A parallel change on the workforce-management side replaces that
HTTP call with an event-fed local cache, but only once labor-performance
actually publishes something to feed it.

## Decision

Publish the existing `TaskPerformanceRecorded` domain event (raised by
`RecordTaskPerformance` for every consumed `TaskCompleted` — see
`internal/application/usecases/record_task_performance.go`) onto a
**new, dedicated integration topic**:

```
warehouse.labor-performance.events
```

named with the same `warehouse.<context>.events` convention every
sibling service's own integration topic uses (e.g.
`fulfillment-execution`'s `warehouse.fulfillment.events`), and kept
strictly separate from `warehouse.labor-performance.analytics` so the two
streams — one an Open-Host-Service Published Language for other bounded
contexts, the other an internal feed for this repo's own projector —
evolve independently, exactly as ADR 0007 already reasons for the
analytics topic's own separation from the inbound contract.

No new domain event is introduced. This is additive wiring around a fact
this service already raises; the domain and application layers are
untouched.

### Scope: TaskPerformanceRecorded only, for now

Only `TaskPerformanceRecorded` is published on the integration topic.
`LaborStandardDefined`/`LaborStandardRevised` remain analytics-only. The
first intended consumer, workforce-management's per-associate cache, only
needs task performance facts; publishing the standard-lifecycle events
too would be speculative. Widening the contract later is a purely
additive change (a new case in `integrationData`, a new outbox row per
event) that needs no change on this service's write path.

### Wire format

> Superseded by [ADR-0021](./0021-cloudevents-mandatory-event-envelope.md):
> the integration topic now carries a CloudEvents 1.0 event (the original
> flat-envelope text is no longer the wire format). Current shape:

```json
{
  "specversion": "1.0",
  "id": "7a2d5e91-3f04-4c8b-9e12-8b4a6d1c5f30",
  "source": "/warehouse/labor-performance",
  "type": "com.warehouse.wes.labor-performance.performance.TaskPerformanceRecorded",
  "subject": "assoc-4471",
  "time": "2026-09-05T11:00:00Z",
  "datacontenttype": "application/json",
  "dataschema": "urn:warehouse:labor-performance:events:TaskPerformanceRecorded:v1",
  "data": {
    "task_id": "task-10231",
    "associate_id": "assoc-4471",
    "task_type": "PICK",
    "efficiency_pct": 91.2,
    "actual_seconds": 41,
    "completed_at": "2026-09-05T09:30:00Z"
  }
}
```

`data` carries the identical fields the analytics stream's
`TaskPerformanceRecorded` message does — `efficiency_pct` is nullable and
null is a real, expected "unmeasurable" fact a consumer must not coerce
to 0, exactly as documented for the analytics contract.

### Partition key: AssociateId, not TaskType

The analytics publisher keys every message on `TaskType`, because its
consumer (the projector) rolls facts up by `(task_type, hour)`. The
integration publisher keys on **`AssociateId`** instead, because its
intended consumer (workforce-management's per-associate cache) needs
every event for one associate applied in publish order on a single
partition. A task with no checked-in associate (a robot station) keys on
the empty string — every such task lands on one shared partition,
still ordered relative to each other.

### Same transactional-outbox pattern, extended

No new persistence or delivery mechanism. `postgres.OutboxPublisher` and
`postgres.OutboxRelay` (ADR 0010) were already written to be
**topic-agnostic**: `OutboxPublisher` takes `encoders ...kafka.Encoder`
and stores one row per `(event, encoder)` pair; `OutboxRelay` reads
`topic` per stored row and forwards it to whichever topic that row names.
Adding the integration topic required zero changes to either file. The
only wiring change is in `cmd/labor/main.go`'s `buildEventPublisher`:
when `EVENT_PUBLISHER=kafka`, it now constructs a second encoder,
`outboundkafka.NewIntegrationPublisher`, and passes it alongside the
existing analytics encoder into both
`NewOutboxPublisher(pool, analytics, integration)` (the DB-backed outbox
path) and `NewFanOutPublisher(logPublisher, analytics, integration)` (the
in-memory, no-DB direct path). The log-only default (`EVENT_PUBLISHER`
unset) is unchanged.

The result: one `RecordTaskPerformance` execution now produces up to two
outbox rows — one per topic — committed in the SAME transaction as the
`TaskPerformance` row and the `processed_events` idempotency marker.
Either both rows land or neither does; the relay then drains each to its
own topic, preserving per-key ordering independently per topic (a
partition-key collision on one topic has no bearing on the other, since
they are different Kafka topics entirely).

### `kafka.Encoder` implementation

`internal/adapters/outbound/kafka/integration_publisher.go` mirrors
`analytics_publisher.go`'s structure exactly: an `Encode(ctx,
events...) ([]Encoded, error)` that builds the envelope and partition key
without touching the broker, a `Publish` built from `Encode` +
`WriteMessages`, and a `Close`. It satisfies both `ports.EventPublisher`
and the outbox's `kafka.Encoder` interface, so the outbox and the
direct-publish path can never disagree on wire format, exactly as the
analytics publisher already guarantees.

## Consequences

**Positive**

- labor-performance is no longer the fleet's only pure event sink. It now
  has a real Open-Host-Service Published Language other bounded contexts
  can build against, closing a named gap in the fleet's wiring plan.
- workforce-management's parallel change can replace a synchronous HTTP
  dependency on this service with an event-fed local cache — removing a
  runtime coupling between two bounded contexts.
- Zero new infrastructure: no new table, no new relay, no new delivery
  semantics to document beyond what ADR 0010 already established. The
  atomicity, at-least-once, and per-key-ordering guarantees from ADR 0010
  apply identically to the new topic.
- The scope is intentionally narrow (`TaskPerformanceRecorded` only),
  keeping the first cut of this Published Language small and reviewable;
  widening it is additive.

**Negative / accepted**

- Two Kafka writes per `RecordTaskPerformance` execution instead of one
  (one per topic, via the outbox). Negligible relative to the write's
  existing `MarkProcessed` + `Save` + `Publish` cost, and both still
  commit atomically with the aggregate.
- The integration contract's stability requirements are now real: unlike
  the analytics topic (consumed only by this repo's own projector, so a
  breaking change is a one-repo problem), a breaking change to
  `warehouse.labor-performance.events`' wire shape now affects another
  bounded context's build. Widening additively (new optional fields, new
  event types) is safe; renaming or removing an existing field is not,
  and must be coordinated the way any other fleet integration topic
  change is.
- `LaborStandardDefined`/`LaborStandardRevised` are not on this topic.
  A future consumer needing standard changes (not scoring facts) would
  need a follow-up ADR to widen the contract — deliberately deferred
  rather than speculatively included now.

## Alternatives considered

- **Reuse the analytics topic for external consumers too.** Rejected for
  the same reason ADR 0007 gives for keeping them separate in the first
  place: an analytics stream and an integration contract have different
  stability expectations and different consumers, and coupling them means
  a schema change made for the projector's convenience could silently
  break an external consumer, or vice versa.
- **Publish all three domain events (including the standard-lifecycle
  ones) on the integration topic from day one.** Rejected as speculative
  scope: no current consumer needs them, and adding them later is a
  strictly additive change with no migration cost.
- **A synchronous REST endpoint on labor-performance instead of a Kafka
  event.** This is the status quo workforce-management is trying to move
  away from — a direct runtime dependency between two bounded contexts,
  contrary to the fleet's choreography-over-orchestration default (ADR
  0002/0003). Rejected.
- **A brand-new outbox/relay pair dedicated to the integration topic.**
  Unnecessary: `OutboxPublisher`/`OutboxRelay` were already
  topic-agnostic by design (ADR 0010's fan-out variant explicitly says
  "a future integration topic is a one-line wiring change"). Building a
  second one would duplicate proven, already-tested machinery for no
  benefit.

## Verification

- Unit: `internal/adapters/outbound/kafka/integration_publisher_test.go`
  — `Encode`/`Publish` produce the correct envelope, event_type,
  AssociateId partition key (including the empty-associate case),
  preserve a nil `efficiency_pct` as JSON `null`, skip every event type
  outside the contract, mint a unique `event_id` per message, inject
  trace headers, agree byte-for-byte with what `Publish` writes, and
  propagate writer errors — mirroring `analytics_publisher_test.go`'s
  test shape exactly.
- Unit (extended): `TestFanOutPublisher`-style coverage already exercises
  fanning out to N publishers; `buildEventPublisher`'s wiring change adds
  no new branch logic beyond one more encoder in two existing
  constructor calls.
- Integration (`-tags=integration`, testcontainers Kafka
  `confluentinc/confluent-local:7.6.1`, no external broker):
  `internal/adapters/outbound/kafka/integration_publisher_integration_test.go`
  — publishes a real `TaskPerformanceRecorded` through
  `IntegrationPublisher` against a live broker and reads it back off the
  topic, asserting the exact envelope/data shape and the AssociateId
  partition key. Run and passed locally: `--- PASS:
  TestIntegrationPublisher_PublishesTaskPerformanceRecordedToRealBroker
  (23.81s)`.
- Integration (`-tags=integration`, testcontainers Postgres
  `postgres:16-alpine`):
  `internal/adapters/outbound/postgres/outbox_integration_test.go`'s new
  `TestOutbox_RecordTaskPerformance_FansOutToBothAnalyticsAndIntegrationTopics`
  — one `RecordTaskPerformance` execution with both encoders configured
  produces exactly one outbox row per topic, and `OutboxRelay.RelayOnce`
  forwards both to the sink. Run and passed locally alongside the
  pre-existing outbox suite (6/6 tests passing, ~12s).
- `make check` (fmt-check, vet, build, lint, test -race) run locally —
  see the PR description for the verbatim output.

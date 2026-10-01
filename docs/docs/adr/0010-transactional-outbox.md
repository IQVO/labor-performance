---
id: 0010-transactional-outbox
title: 10. Transactional outbox for the analytics topic
sidebar_label: 10. Transactional outbox
sidebar_position: 10
description: "Why this service stopped publishing to Kafka from inside its use cases and now commits every domain event, already encoded, to an outbox table in the same Postgres transaction as the aggregate change (and, for the consumer-driven path, the processed_events idempotency marker), with an in-process relay draining the table onto warehouse.labor-performance.analytics."
---

# 10. Transactional outbox for the analytics topic

## Status

**Accepted** — implemented in the same change that introduced this record.
The fleet's reference implementation is `process-path-management` (its
ADR 0003); this record is `labor-performance` adopting the same pattern,
adapted to a service whose only outbound topic is the analytics data
product's and whose main write path is itself a Kafka consumer.

> **Envelope superseded by [ADR-0021](./0021-cloudevents-mandatory-event-envelope.md).** The flat envelope (`event_id`/`event_type`/`occurred_at`) described below is retired: every message is now a CloudEvents 1.0 event. The rest of this decision stands.

## Context

Both of this service's writing use cases ended with the same shape:

```go
if err := uc.Standards.Save(ctx, next); err != nil { return nil, err }
if err := uc.Events.Publish(ctx, evt); err != nil { return nil, err }
```

Two independent writes to two independent systems, no compensation. A
crash, a broker timeout, or a pod eviction between them leaves a row in
the OLTP store whose event never reached
`warehouse.labor-performance.analytics` — and the analytical data product
(ADR 0007) is built **exclusively** from that topic. A standard defined
but never projected means every scorecard the reports API serves is
computed against a standard the report side does not know exists; a
`TaskPerformance` recorded but never projected is a task the operator's
console simply never counts.

`RecordTaskPerformance` had a second, worse variant of the problem. It is
driven by the inbound consumer of `warehouse.fulfillment.events` and its
first write is the `processed_events` idempotency marker:

```go
isNew, _ := uc.Processed.MarkProcessed(ctx, eventId)   // write 1 (committed)
...
uc.Performances.Save(ctx, p)                             // write 2
uc.Events.Publish(ctx, evt)                              // write 3
```

If write 2 or 3 failed, the message was NACKed and redelivered — and the
redelivery was then **dropped as a duplicate** because write 1 had already
committed. The idempotency gate that exists to make at-least-once delivery
safe turned a transient failure into a permanent data loss.

The same Save-then-Publish shape existed in four sibling services; the
fleet is rolling out the pattern below to all of them from one brief, so
the design is deliberately identical to `process-path-management`'s where
it can be and differs only where this service's shape forces it.

## Decision

Adopt the **transactional outbox** pattern:

1. **`outbox_events` table** (migration `0002_outbox`). Each row is one
   **already-encoded Kafka message**: `topic`, `event_type`, `key` (the
   partition key — the TaskType), `value` (the JSON
   `envelope.AnalyticsEnvelope`, byte-for-byte what the topic will
   carry), `headers` (a JSON array of `{key,value}` — the W3C
   `traceparent` captured from the originating request), plus
   `published_at`, `attempts`, `last_error` for the relay. A partial
   index over `published_at IS NULL` keeps the relay's scan tiny.

2. **`ports.UnitOfWork`** — a new driven port:
   `Execute(ctx, fn func(ctx) error) error`. The application layer wraps
   **every write** of a use case in one call: `DefineStandard` wraps
   closing the prior standard + saving the new one + `Publish`;
   `RecordTaskPerformance` wraps `MarkProcessed` + `Save` + `Publish`.
   Reads that only decide whether to act (`FindCurrentlyActive`,
   `NextID`, `FindActiveAsOf`) may stay where they are. The port is
   optional (`nil` = "run them back to back"), which is exactly the
   in-memory / log-publisher dev configuration. The domain and
   application layers gain no knowledge of transactions, Postgres, or
   Kafka — the arch-go fitness test is unchanged and still passes.

3. **`kafka.Encoder`** — the `AnalyticsPublisher` is split in two halves:
   `Encode(ctx, events...) ([]Encoded, error)` builds the envelope,
   partition key and trace headers without sending; `Publish` is now
   `Encode` + `WriteMessages`. Trace headers are injected at **encode**
   time because in outbox mode the send happens later, on the relay
   goroutine, which has no request span of its own. Events outside the
   analytics contract produce no `Encoded` (and therefore no outbox
   row), exactly as they were skipped before.

4. **`postgres.UnitOfWork`** opens a `pgx.Tx`, binds it to the context,
   and commits or rolls back around `fn`. `StandardRepo`,
   `PerformanceRepo`, `ProcessedEventRepo` and the new `OutboxPublisher`
   resolve their querier from the context: the pool when standalone, the
   bound transaction when inside a unit of work. Nested `Execute` calls
   join the outer transaction rather than opening a second one.

5. **`postgres.OutboxPublisher`** implements `ports.EventPublisher`
   (variadic) by running every event through each configured
   `kafka.Encoder` **inside the transaction** and `INSERT`ing one row per
   encoded message. It never touches the broker. Because it uses the very
   same `Encode` the direct publisher uses, the outbox and the direct
   path can never disagree on wire format. Today there is exactly one
   encoder (analytics); the constructor is variadic so a future
   integration topic is a one-line wiring change.

6. **`kafka.RelaySink`** wraps a kafka-go `Writer` with **no** fixed
   `Topic` and sets `Message.Topic` from `Encoded.Topic` per message.
   (kafka-go rejects a per-message Topic when the Writer also has one, and
   vice versa — which is why `AnalyticsPublisher`'s writer keeps its
   pinned topic and `Encode` leaves `Message.Topic` empty.)

7. **`postgres.OutboxRelay`** runs as a goroutine inside the `labor`
   process, next to the HTTP server and the inbound consumer. Each pass
   claims up to 100 pending rows with `SELECT … FOR UPDATE SKIP LOCKED
   ORDER BY id`, sends them to the sink **one at a time in id order**,
   and marks each `published_at`. On a send failure it stops the pass at
   that row (so a later event for the same key can never overtake a
   failed earlier one), records `attempts+1`/`last_error` on the row,
   commits what was already sent, and retries on the next tick. Sleep
   between empty passes is `OUTBOX_RELAY_INTERVAL` (default `1s`); a
   full batch is followed immediately by another pass.

8. **Composition root** (`cmd/labor/main.go` only; `labor-projector`,
   `labor-reports` and `mcp` are untouched) picks the mode. The Postgres
   pool is now built **before** the publisher so the outbox can be wired
   over it:

   | `DATABASE_URL` | `EVENT_PUBLISHER` | Publisher wired                          | Relay   |
   |----------------|-------------------|------------------------------------------|---------|
   | unset          | `log` (default)   | log                                      | none    |
   | unset          | `kafka`           | FanOut(log, analytics) — direct, no outbox | none  |
   | set            | `log`             | log                                      | none    |
   | set            | `kafka`           | **FanOut(log, OutboxPublisher(analytics))** | **yes** |

   The mode is logged once at startup:
   `"event publisher configured" publisher=kafka mode=outbox|direct`.
   The cluster runs the last row. Graceful shutdown cancels the consumer,
   stops the HTTP server, then cancels the relay and waits for its
   in-flight pass (bounded by the 10s shutdown deadline), so an event
   committed by a request or a consumed message that completed a moment
   before SIGTERM is not stranded until the next pod boots.

### Delivery semantics (what the analytics projector may now rely on)

- **Atomicity**: an aggregate change, its idempotency marker and its
  event are committed together or not at all. Verified by an integration
  test that forces the outbox encode to fail and asserts the
  `labor_standards` / `task_performances` / `processed_events` rows are
  all absent — and that the same message is then accepted on redelivery.
- **At-least-once**: a crash between a successful `Send` and the row's
  `UPDATE` republishes that row on the next pass. The projector already
  dedupes on the envelope's `event_id`, and that id is part of the stored
  `value`, so a republished message carries the **same `event_id`** and
  is a no-op for it.
- **Per-key ordering**: preserved. Rows are drained in insertion order
  within one relay, keyed by TaskType onto one partition, and a failed
  row blocks everything behind it rather than being skipped.
- **Latency**: events reach the topic within one relay interval (≤1s in
  the cluster) of the HTTP response / consumer ack, versus "before the
  response" under the old direct publish. For a reporting data product
  that is already eventually consistent by design this is invisible.

## Consequences

**Positive**
- There is no longer any code path that persists a standard or a task
  performance without also persisting its analytics event, and no code
  path that commits a `processed_events` marker for a message whose row
  did not commit. The "redelivery dropped as duplicate" data-loss mode is
  gone.
- No broker dependency on the request path. `POST /standards` succeeds
  when Kafka is down; the event is published once it returns. Previously
  the request failed with a 500 after the row was already committed.
- The inbound consumer's at-least-once contract is now genuinely safe:
  any failure inside `RecordTaskPerformance` rolls back the marker, so
  the redelivery is scored.
- Use cases are simpler to reason about: one atomic scope, one error.

**Negative / accepted**
- One more table, one more goroutine, one more failure mode (the relay)
  to observe. The relay logs every failed pass at ERROR with the row id
  and the broker error; `outbox_events.attempts`/`last_error` are
  queryable for operators. Metrics for outbox lag are a follow-up.
- Events are no longer synchronous with the HTTP response / consumer
  ack. Documented above; acceptable for this domain.
- With two pods overlapping during a rolling deploy, `SKIP LOCKED`
  guarantees no double-claim within one pass but does **not** guarantee
  global order across the two relays for different keys. Per-key order
  is what the projector depends on, and that is preserved because all
  events for one key in one request are inserted contiguously and drained
  by whichever relay claims that range first.
- `EVENT_PUBLISHER=kafka` without `DATABASE_URL` still publishes
  directly. That mode exists only for in-memory local runs and is logged
  as such at startup (`mode=direct`).
- The log publisher stays first in the fan-out in every mode, so a
  human reading the pod logs still sees every event at the moment it was
  raised, not at the moment the relay shipped it.

## Alternatives considered

- **Keep Save-then-Publish and add retry around Publish.** Does not fix
  a crash between the two writes, and — for the consumer path — does
  nothing about the already-committed idempotency marker. Rejected.
- **Publish-then-Save.** Inverts the failure: an event on the topic for a
  row that never persisted, which the projector would happily count.
  Rejected.
- **Move `MarkProcessed` to the end of `RecordTaskPerformance`.** Fixes
  the marker-first data loss but leaves Save-then-Publish divergence in
  place, and turns a redelivery during the window into a double-count.
  The unit of work subsumes this: with the marker inside the same
  transaction the ordering no longer matters.
- **Change-data-capture (Debezium) on the OLTP tables.** Correct, but adds
  a Kafka Connect deployment to a kind cluster that already runs Istio,
  Kong, Kafka, Postgres and the observability stack, and moves envelope
  encoding out of the service's own code. Deferred until more than one
  service needs it.
- **A separate relay binary** (like `labor-projector`). Cleaner
  isolation, but the relay's work is a single `SELECT`/`UPDATE` loop and
  this process already hosts a consumer goroutine; in-process, with
  graceful-shutdown handling, is proportionate. If the relay ever needs
  independent scaling it can be lifted into `cmd/labor-relay` without
  touching the adapters.

## Verification

- Unit: `internal/application/usecases/unit_of_work_test.go` — each
  writing use case runs every write and its Publish inside exactly one
  scope (both Saves of a revision; `MarkProcessed` + `Save` + `Publish`
  of a recording), a publish failure rolls the scope back and is not
  counted as an accepted definition, a begin failure leaves the
  idempotency marker unwritten, a rejected input opens no scope, a
  duplicate delivery commits an empty scope, nil UnitOfWork still works.
- Unit: `internal/adapters/outbound/kafka/encode_test.go` — `Encode`
  yields one message per contract event with topic/key/envelope, writes
  nothing, agrees byte-for-byte with what `Publish` writes, captures a
  real `traceparent` from an active span; `RelaySink` sets the topic per
  message and writes one batch.
- Integration (`-tags=integration`, **testcontainers** Postgres
  `postgres:16-alpine` — the test owns its own database, never an
  external `DATABASE_URL`, never `t.Skip`):
  `internal/adapters/outbound/postgres/outbox_integration_test.go` —
  commit-together for both use cases, rollback of aggregate + marker on
  encode failure and acceptance on redelivery, relay publishes in id
  order and marks rows, relay stops at a failed row (attempts/last_error)
  and recovers in order, `Run` drains until cancelled.
- Cluster: not yet performed as part of this change. After deploy the
  check is `POST /standards` then `SELECT event_type, published_at FROM
  outbox_events ORDER BY id DESC LIMIT 1` showing the row published within
  one interval, and `GET /reports/performance` on `labor-reports`
  reflecting the new standard once the projector has applied it.

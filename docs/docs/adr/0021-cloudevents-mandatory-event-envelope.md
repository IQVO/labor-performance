---
id: 0021-cloudevents-mandatory-event-envelope
slug: /adr/0021-cloudevents-mandatory-event-envelope
title: 21. CloudEvents 1.0 as the mandatory event envelope
sidebar_label: 21. CloudEvents 1.0 mandatory envelope
description: "ADR 0021 -- fleet-wide standard: every Kafka message labor-performance produces or consumes (integration warehouse.labor-performance.events, analytics warehouse.labor-performance.analytics, and fulfillment-execution's warehouse.fulfillment.events) is a CloudEvents 1.0 event in structured content mode, built and validated with sdk-go v2's event package. The flat event_id/event_type/occurred_at envelope, the analytics schema_version field, and the dual-read decoder are removed. No coexistence."
---

# 21. CloudEvents 1.0 as the mandatory event envelope

## Status

**Accepted** — implemented in the same change that introduces this
record, as labor-performance's part of the fleet-wide CloudEvents cutover
(Accepted fleet-wide 2026-09-30). It supersedes the envelope sections of
[ADR 0003](./0003-kafka-choreography-consumer-of-fulfillment-execution.md)
(the flat inbound envelope), [ADR 0007](./0007-analytical-data-product.md)
(the analytics "Envelope v1" with `schema_version`),
[ADR 0010](./0010-transactional-outbox.md) (the outbox stored the flat
envelope) and [ADR 0013](./0013-labor-performance-integration-events.md)
(the flat integration envelope), plus the dual-read decoder this service
ran for fulfillment-execution's ADR-0027 / wes-work-planning's ADR-0021
migration. Everything else in those ADRs stands.

## Context

The fleet carried a home-grown "CloudEvents-like" flat envelope
(`event_id`, `event_type`, `occurred_at`, `source`, `data`), a second
analytics variant with `schema_version`, and — during a dual-envelope
migration — a consumer here that sniffed `specversion` to decide which
shape to parse, then stripped the CloudEvents `type` back to a short name
(`TaskCompleted`) for dispatch. Three shapes, a discriminator and a
suffix-match are three ways for producers and consumers to drift. The
fleet decided to stop migrating and make CloudEvents 1.0 the only envelope,
cut over in one coordinated deploy.

## Decision

### 1. Scope

EVERY message this service writes to or reads from Kafka is a CloudEvents
1.0 event:

| direction | topic | types |
|---|---|---|
| publish (integration) | `warehouse.labor-performance.events` | `com.warehouse.wes.labor-performance.performance.TaskPerformanceRecorded` |
| publish (analytics) | `warehouse.labor-performance.analytics` | `com.warehouse.wes.labor-performance.standard.LaborStandardDefined`, `com.warehouse.wes.labor-performance.standard.LaborStandardRevised`, `com.warehouse.wes.labor-performance.performance.TaskPerformanceRecorded` |
| consume (OLTP) | `warehouse.fulfillment.events` | `com.warehouse.wes.fulfillment-execution.task.TaskCompleted` |
| consume (projector) | `warehouse.labor-performance.analytics` | the three analytics types above |

There is NO flat envelope, NO dual-write, NO dual-read and NO envelope
toggle.

### 2. Encoding

- Kafka protocol binding, **structured content mode**: the message value
  is the JSON event format; every produced message carries the Kafka
  header `content-type: application/cloudevents+json; charset=UTF-8`.
- Message key and the `kafkago.Hash{}` balancer are unchanged (ADR 0018):
  AssociateId on the integration topic, TaskType on the analytics topic.
- W3C trace context stays in `traceparent`/`tracestate` Kafka headers; it
  is not duplicated into extension attributes.
- Events are built, validated and (un)marshalled only through
  `github.com/cloudevents/sdk-go/v2/event` (v2.16.2), wrapped by the single
  helper package `internal/adapters/kafka/cloudevents` (`New`, `Decode`,
  `ContentTypeHeader`, the topic names and the full type constants).
  Transport stays `segmentio/kafka-go`; the sdk-go protocol/client packages
  are not used.

### 3. Context attributes (all required)

| attribute | value |
|---|---|
| `specversion` | `1.0` |
| `id` | UUID v4 minted once at `Encode` time and stored inside the outbox row's value, so a relay redelivery carries the same id. `(source, id)` is the consumer idempotency key. |
| `source` | `/warehouse/labor-performance` (both topics) |
| `type` | `com.warehouse.wes.labor-performance.<entity>.<EventName>`; entity `standard` for LaborStandardDefined/Revised, `performance` for TaskPerformanceRecorded |
| `subject` | aggregate id: StandardId for the standard events; AssociateId for TaskPerformanceRecorded (the integration key) — or the TaskId when the task had no associate (robot station), because `subject` is never empty |
| `time` | the domain event's occurred-at, UTC |
| `datacontenttype` | `application/json` |
| `dataschema` | `urn:warehouse:labor-performance:<events\|analytics>:<EventName>:v1` |

`data` is byte-for-byte the payload published before this change. The
analytics `schema_version` field is removed — `dataschema` replaces it.
The same `type` names an occurrence on both topics; `dataschema` names the
payload shape. A breaking payload change requires a new `dataschema`
version and a new `.v2` type, never a mutation.

### 4. Consumers

1. `cloudevents.Decode` parses and `Validate()`s. A message that is not a
   valid CloudEvents 1.0 event (legacy flat shape, bad JSON, missing
   attribute) is a deterministic poison message:
   - the fulfillment consumer (`internal/adapters/inbound/kafka/consumer.go`)
     sends it straight to its existing DLQ (`<topic>.dlq`, ADR 0017) with
     no retries, and commits past it;
   - the analytics projector (`analytics_consumer.go`, no DLQ) logs it at
     WARN and commits past it.
   Neither ever crashes, blocks the partition, or falls back to a flat
   parser.
2. Dispatch on the FULL `type` string. Unknown types are ignored. A bare
   short name (`TaskCompleted`) is an unknown type.
3. Dedupe on the CloudEvents `id` (the existing `processed_events` /
   `consumed_events` columns, now populated from `id`).
4. `time` and `subject` come from the context attributes, the payload
   from `DataAs`. TaskCompleted's `time` is the completion instant handed
   to `RecordTaskPerformance`; the projector's freshness watermark is the
   analytics event's `time`.

### 5. Cross-service contract

This service's exact strings consumed elsewhere:
`com.warehouse.wes.labor-performance.performance.TaskPerformanceRecorded`
→ workforce-management (and this service's own projector). Consumed from
elsewhere: `com.warehouse.wes.fulfillment-execution.task.TaskCompleted`
(fulfillment-execution). The fleet-wide table lives in each repo's
CloudEvents ADR and in warehouse-docs.

## Consequences

- One envelope, one decoder, one dispatch rule; the dual-read code and its
  tests are deleted.
- **Breaking wire change.** This service must deploy together with the
  rest of the fleet cutover. Before deploying: drain the outbox (existing
  rows hold flat-encoded values), then delete and recreate
  `warehouse.labor-performance.events` / `.analytics` so a `FirstOffset`
  replay (the projector) never meets a flat message — see warehouse-infra
  `docs/cloudevents-cutover.md`. Any stray flat message that does arrive
  is DLQ'd/skipped, never mis-parsed.
- The outbox `event_type` column now stores the full CloudEvents type
  (informational only — the relay forwards the stored value verbatim).
- Tests: golden exact-JSON test per published (stream, type) including the
  content-type header; a legacy-flat-rejected test for both consumers
  (unit, and a Testcontainers DLQ test for the fulfillment consumer);
  existing Testcontainers integration tests use the CloudEvents wire
  format.

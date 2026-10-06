---
id: domain-events
title: Domain events
sidebar_label: Domain events
sidebar_position: 11
description: Every event Labor Performance publishes and consumes — full CloudEvents type, topic, partition key, dataschema, payload fields, producing use case and known consumers.
---

# Domain events

Every Kafka message this service produces or consumes is a **CloudEvents
1.0 event in structured content mode** (ADR 0021): the whole event —
attributes and `data` — is the JSON message value, and the Kafka header
`content-type` is `application/cloudevents+json; charset=UTF-8`. Events
are built and decoded only by `internal/adapters/kafka/cloudevents`
(sdk-go v2 `event`); transport is kafka-go. There is no flat envelope and
no binary mode.

Attributes set on every published event (`cloudevents.New`):

| Attribute | Value |
|---|---|
| `specversion` | `1.0` |
| `id` | a fresh UUID per encoded message — the consumer's dedupe key |
| `source` | `/warehouse/labor-performance` |
| `type` | `com.warehouse.wes.labor-performance.<entity>.<EventName>` |
| `subject` | the aggregate instance (see each event) — never empty |
| `time` | the domain event's `OccurredAt`, UTC |
| `datacontenttype` | `application/json` |
| `dataschema` | `urn:warehouse:labor-performance:<stream>:<EventName>:v1`, stream = `analytics` or `events` |

The domain event is raised inside the use case's unit of work. With
`EVENT_PUBLISHER=kafka` and `DATABASE_URL` set it is encoded into
`outbox_events` in the same transaction and published by the outbox
relay (ADR 0010); with `EVENT_PUBLISHER=kafka` and no database it is
written straight to Kafka; by default (`EVENT_PUBLISHER=log`) it is only
logged.

## Published

### LaborStandardDefined

| | |
|---|---|
| CE type | `com.warehouse.wes.labor-performance.standard.LaborStandardDefined` |
| Topic | `warehouse.labor-performance.analytics` only |
| Partition key | `task_type` |
| `subject` | the new `standard_id` |
| `dataschema` | `urn:warehouse:labor-performance:analytics:LaborStandardDefined:v1` |
| Producer | `DefineStandard` when no standard was open for the task type |
| Consumers | this repo's `cmd/labor-projector` (counts `standards_defined`) |

| Field | Type | Notes |
|---|---|---|
| `standard_id` | string | `shared.StandardId` |
| `task_type` | string | `PICK`, `PACK` or `SLAM` |
| `expected_seconds` | integer | > 0 |
| `effective_from` | RFC 3339 timestamp | |
| `travel_component_seconds` | integer | **omitted** when the standard declares none (ADR 0015) |

### LaborStandardRevised

| | |
|---|---|
| CE type | `com.warehouse.wes.labor-performance.standard.LaborStandardRevised` |
| Topic | `warehouse.labor-performance.analytics` only |
| Partition key | `task_type` |
| `subject` | the new `standard_id` |
| `dataschema` | `urn:warehouse:labor-performance:analytics:LaborStandardRevised:v1` |
| Producer | `DefineStandard` when an open standard was closed |
| Consumers | this repo's `cmd/labor-projector` (counts `standards_revised`) |

| Field | Type | Notes |
|---|---|---|
| `standard_id` | string | the **new** standard's id |
| `task_type` | string | |
| `previous_expected_seconds` | integer | from the closed standard |
| `expected_seconds` | integer | the new value (`NewExpectedSeconds` in Go) |
| `effective_from` | RFC 3339 timestamp | also the instant the prior standard closed |
| `travel_component_seconds` | integer | the new standard's value; omitted when none |

### TaskPerformanceRecorded (analytics)

| | |
|---|---|
| CE type | `com.warehouse.wes.labor-performance.performance.TaskPerformanceRecorded` |
| Topic | `warehouse.labor-performance.analytics` |
| Partition key | `task_type` (empty string when unclassified) |
| `subject` | `associate_id`, or `task_id` when there is no associate |
| `dataschema` | `urn:warehouse:labor-performance:analytics:TaskPerformanceRecorded:v1` |
| Producer | `RecordTaskPerformance` (Kafka-driven only) |
| Consumers | this repo's `cmd/labor-projector` |

### TaskPerformanceRecorded (integration)

| | |
|---|---|
| CE type | `com.warehouse.wes.labor-performance.performance.TaskPerformanceRecorded` (same type) |
| Topic | `warehouse.labor-performance.events` (ADR 0013) |
| Partition key | `associate_id` (empty string for a robot station) |
| `subject` | `associate_id`, or `task_id` when there is no associate |
| `dataschema` | `urn:warehouse:labor-performance:events:TaskPerformanceRecorded:v1` |
| Producer | `RecordTaskPerformance` |
| Consumers | `workforce-management` (`internal/adapters/outbound/laborperformancecache/consumer.go`, `LABOR_PERFORMANCE_MODE=kafka-cache`) |

Both `TaskPerformanceRecorded` messages carry the same `data`, but each is
a separate CloudEvent with its own `id`:

| Field | Type | Notes |
|---|---|---|
| `task_id` | string | `fulfillment-execution`'s task id |
| `associate_id` | string | empty when the station had no checked-in occupant |
| `task_type` | string | `PICK`, `PACK`, `SLAM` or empty (unclassified) |
| `efficiency_pct` | number or null | null when unscorable — never 0 |
| `actual_seconds` | integer | the upstream `duration_seconds`; 0 when unmeasurable |
| `idle_seconds_before` | integer or null | additive (ADR 0014); null on first observation, empty associate or out-of-order gap |
| `completed_at` | RFC 3339 timestamp | the consumed event's `time` |

## Consumed

### TaskCompleted (from fulfillment-execution)

| | |
|---|---|
| CE type | `com.warehouse.wes.fulfillment-execution.task.TaskCompleted` |
| Topic | `warehouse.fulfillment.events` (shared, fan-out with `wes-work-planning`) |
| Consumer group | `labor-performance` (`KAFKA_CONSUMER_GROUP`) |
| Dedupe | CloudEvents `id`, via `processed_events` |
| Handler | `inbound/kafka.Consumer.handleFulfillmentEvent` → `RecordTaskPerformance` |
| Failure | invalid CloudEvent, or 3 failed attempts → `warehouse.fulfillment.events.dlq` |

| Field read | Maps to |
|---|---|
| `id` (attribute) | `KafkaEventId` |
| `time` (attribute) | `CompletedAt` |
| `data.task_id` | `TaskId` |
| `data.associate_id` | `AssociateId` (empty allowed) |
| `data.duration_seconds` | `ActualSeconds` (0 allowed) |
| `data.task_type` | `TaskType` via `shared.ParseTaskTypeLenient` |

`data.station_id` and `data.work_unit_id` are decoded but unused. Every
other CloudEvents type on the shared topic is committed and ignored.

### Own analytics events (projector)

`cmd/labor-projector` consumes `warehouse.labor-performance.analytics`
with group `labor-performance-analytics` from the earliest offset, and
folds the three types above into `labor_performance_rollup`. It reads
`task_type`, `expected_seconds`, `effective_from`, `actual_seconds`,
`efficiency_pct` and `completed_at`; an empty `task_type` becomes
`UNCLASSIFIED`.

## Dead-letter topic

`warehouse.fulfillment.events.dlq` receives the **original** message
bytes and headers plus `x-dlq-source-topic`, `x-dlq-error` and
`x-dlq-failed-at` (ADR 0017). It is not a CloudEvent of its own and has
no consumer in the fleet.

Source: `internal/domain/shared/events.go`,
`internal/adapters/kafka/cloudevents/cloudevents.go`,
`internal/adapters/outbound/kafka/analytics_publisher.go`,
`internal/adapters/outbound/kafka/integration_publisher.go`,
`internal/adapters/inbound/kafka/consumer.go`,
`internal/adapters/inbound/kafka/analytics_consumer.go`,
`apis/asyncapi.yaml`, `cmd/labor/main.go`.

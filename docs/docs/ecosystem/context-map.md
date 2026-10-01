---
title: Context Map
sidebar_label: Context Map
description: Labor Performance's relationships — a Kafka Customer of fulfillment-execution, a Kafka supplier to workforce-management, and a read-only Open Host Service for the console and the ops agent. It makes no outbound calls.
---

# Context Map

Labor Performance has exactly **one** input: it is a pure Kafka
**Customer** of `fulfillment-execution`'s already-published
`TaskCompleted` integration event. Everything else points the other way —
it publishes its own integration event and exposes its own REST, reports
and MCP surfaces that other contexts read. It has **no outbound REST or
MCP client** to any sibling context (ADR 0003, restated for facility-layout
by ADR 0015); there is no `internal/adapters/outbound/` package that
targets another service.

```mermaid
flowchart LR
    subgraph WES["WES tier — when &amp; in what order"]
        FE["<b>fulfillment-execution</b><br/>Core subdomain<br/>Pick/Pack/SLAM task lifecycle"]
    end

    subgraph SUPP["Supporting subdomains"]
        LP["<b>labor-performance</b><br/>engineered standards · performance scoring · utilization"]
        WFM["<b>workforce-management</b><br/>shift &amp; path staffing"]
    end

    subgraph READERS["Readers of this context's own surfaces"]
        CONSOLE["warehouse-console<br/>(labor_mfe remote)"]
        AGENT["warehouse-ops-agent"]
    end

    FE ==>|"<b>warehouse.fulfillment.events</b><br/>TaskCompleted (shared, fan-out topic)"| LP
    LP ==>|"<b>warehouse.labor-performance.events</b><br/>TaskPerformanceRecorded"| WFM
    CONSOLE -->|"REST: /standards, /associates/{id}/scorecard,<br/>/task-types/{taskType}/performance"| LP
    AGENT -->|"MCP: get_associate_scorecard,<br/>get_task_type_utilization<br/>REST: /reports/performance[/freshness]"| LP

    classDef core fill:#1e3a8a,stroke:#1e293b,color:#fff;
    classDef this fill:#0f766e,stroke:#134e4a,color:#fff,stroke-width:4px;
    class FE core;
    class LP this;
```

**Bold edges are live Kafka topics with a real publisher and a real
consumer on each end.** Thin edges are calls another context makes INTO
this service's own Open Host Service — this service never initiates them.
This service's third topic, `warehouse.labor-performance.analytics`, is
internal to its own analytical data product (consumed only by
`cmd/labor-projector`, ADR 0007) and is not an integration contract.

## → `fulfillment-execution` (live, inbound Kafka only)

**Strategically: Customer/Supplier, with this context as a Conformist
downstream.** `fulfillment-execution` is the Open Host Service; this context
subscribes to its Published Language (the `TaskCompleted` event shape) and
never gets write access to a `Task` or `Station` aggregate.

This service subscribes to **`warehouse.fulfillment.events`** — the SAME
shared, fan-out topic `wes-work-planning` already consumes from — under its
own consumer group id (`KAFKA_CONSUMER_GROUP`, `labor-performance` by
default). Only the CloudEvents type
`com.warehouse.wes.fulfillment-execution.task.TaskCompleted` is acted on; every other
event type on this shared topic is silently skipped, not an error,
mirroring `wes-work-planning`'s own consumer's skip-unrecognized-event-type
behavior.

Every message is a CloudEvents 1.0 event in structured mode (ADR 0021;
Kafka header `content-type: application/cloudevents+json; charset=UTF-8`).
A message that is not a valid CloudEvent is dead-lettered, never parsed:

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
    "task_id": "...",
    "station_id": "...",
    "work_unit_id": "...",
    "associate_id": "...",
    "duration_seconds": 52,
    "task_type": "PICK"
  }
}
```

`associate_id`, `duration_seconds` and `task_type` are all optional on the
wire: an older payload that predates an enrichment omits the field, and
this service's JSON unmarshaling degrades it to its Go zero value (`""` /
`0`) — exactly the "no checked-in occupant" / "unmeasurable duration" /
"unclassified" business facts this service's own aggregate invariants
already model, not an error.

### `task_type` on the wire (gap closed)

`task_type` was once a known wire-contract gap: every consumed event was
bucketed as `""` (unclassified). `fulfillment-execution` ADR-0023 added it
to the `TaskCompleted` payload, and this service's consumer now passes it
to `shared.ParseTaskTypeLenient`. A recognized `PICK`/`PACK`/`SLAM` passes
through; an unrecognized value (e.g. `REBIN`, which this context does not
model as an engineered-labor-standard task type) or an absent field still
resolves to `""` — recorded and counted, but never scored against a
`LaborStandard` and never listed under `GetTaskTypePerformance`.

The same event also drives idleness (ADR 0014): the gap between an
associate's previous completion and this task's claim instant
(the CloudEvents `time` − `duration_seconds`) is recorded as an `IdlePeriod`, with no
additional upstream field required.

## → `workforce-management` (live, outbound Kafka)

**Strategically: this context is the upstream Supplier; `workforce-management`
is a Conformist Customer.** Since ADR 0013 this service publishes
`TaskPerformanceRecorded` onto its integration topic
**`warehouse.labor-performance.events`**, through the same transactional
outbox as the analytics topic (ADR 0010). `workforce-management`'s
`laborperformancecache` consumer (its ADR 0019, `LABOR_PERFORMANCE_MODE=kafka-cache`)
builds a local measured-rate read model from it for `ProposePathPlan`, and
reads the additive `idle_seconds_before` field (ADR 0014) as a staffing
signal (its ADR 0020).

`workforce-management` also keeps an older `LABOR_PERFORMANCE_MODE=http`
option that calls this service's `GET /task-types/{taskType}/performance`.
Either way the dependency points from `workforce-management` to this
context — this service has no Go import from, REST call to, or Kafka
subscription on `workforce-management`.

## ← `warehouse-console` (the `labor_mfe` remote)

`web/` is this context's Module Federation remote (`labor_mfe`), mounted
by the console shell at `/labor`. It calls only this service's own OLTP
REST API: `POST /standards`, `GET /associates/{associateId}/scorecard` and
`GET /task-types/{taskType}/performance`. CORS is configured through
`CORS_ALLOWED_ORIGINS` on both the OLTP and reports routers.

## ← `warehouse-ops-agent` (reads over MCP and the reports API)

`warehouse-ops-agent` reads this context through two of its own surfaces:

- the MCP server (`cmd/mcp`) — `get_associate_scorecard` and
  `get_task_type_utilization`, the latter feeding its flow-balance
  advisory (its ADR 0008);
- the reports API (`cmd/labor-reports`) — `GET /reports/performance` and
  `GET /reports/performance/freshness`, for the console's WES dashboard.

Both are unauthenticated reads (ADR 0012). This service knows nothing
about the agent.

## Why this is not one bounded context with `fulfillment-execution`

`fulfillment-execution` follows a strict "Task/Station only" design
discipline — adding a labor-standard concept there would be scope creep into
a domain neither Task nor Station has any business modeling. Splitting this
context out means a standard revision, an efficiency computation, or a
scorecard projection never needs to touch `fulfillment-execution`'s own
release cadence or test suite, and vice versa — `fulfillment-execution` can
evolve its Task/Station model without this context's scoring logic ever
being a blocker. See
[ADR 0002](/docs/adr/0002-new-bounded-context-not-extension-of-workforce-or-fulfillment)
and
[ADR 0003](/docs/adr/0003-kafka-choreography-consumer-of-fulfillment-execution)
for the full reasoning.

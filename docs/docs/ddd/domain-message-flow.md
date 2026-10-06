---
id: domain-message-flow
title: Domain Message Flow
sidebar_label: Domain Message Flow
sidebar_position: 5
description: ddd-crew Domain Message Flow diagrams for Labor Performance's key business scenarios — scoring a completed task, revising a standard, the ops agent reading performance, and the hourly report.
---

# Domain Message Flow

Following [ddd-crew Domain Message Flow Modelling](https://github.com/ddd-crew/domain-message-flow-modelling).
Participants are contexts, people and external systems; every arrow is
numbered and prefixed `cmd:` (command), `evt:` (event) or `qry:` (query),
and names a real REST route, MCP tool or CloudEvents type. Internal
classes are on the [Sequence diagrams](./sequence-diagrams.md) page
instead.

## 1. A completed task is scored and reaches workforce planning

```mermaid
sequenceDiagram
    autonumber
    actor Associate
    participant FE as fulfillment-execution
    participant LP as labor-performance
    participant WFM as workforce-management
    participant PRJ as labor-performance projector
    Associate->>FE: cmd: complete task at station
    FE-)LP: evt: com.warehouse.wes.fulfillment-execution.task.TaskCompleted on warehouse.fulfillment.events
    Note over LP: RecordTaskPerformance - score vs standard as of completion, derive idle gap
    LP-)WFM: evt: com.warehouse.wes.labor-performance.performance.TaskPerformanceRecorded on warehouse.labor-performance.events
    LP-)PRJ: evt: com.warehouse.wes.labor-performance.performance.TaskPerformanceRecorded on warehouse.labor-performance.analytics
    Note over WFM: laborperformancecache updates measured rate for ProposePathPlan
```

Source: `internal/adapters/inbound/kafka/consumer.go`,
`internal/application/usecases/record_task_performance.go`,
`internal/adapters/outbound/kafka/integration_publisher.go`,
`internal/adapters/outbound/kafka/analytics_publisher.go`; WFM's
`internal/adapters/outbound/laborperformancecache/consumer.go`. Omitted:
the outbox hop (see [Sequence diagrams](./sequence-diagrams.md)), the DLQ
branch, and how `fulfillment-execution` itself records the completion.

## 2. An industrial engineer revises a labor standard

```mermaid
sequenceDiagram
    autonumber
    actor IE as Industrial engineer
    participant UI as warehouse-console labor_mfe
    participant LP as labor-performance
    participant PRJ as labor-performance projector
    IE->>UI: enter PICK expectedSeconds 40
    UI->>LP: cmd: POST /standards with Idempotency-Key
    Note over LP: DefineStandard - close open PICK standard, open new one
    LP-->>UI: 201 standard with effectiveFrom
    LP-)PRJ: evt: com.warehouse.wes.labor-performance.standard.LaborStandardRevised on warehouse.labor-performance.analytics
    Note over LP: later TaskCompleted events with time after effectiveFrom are scored against 40 s
```

Source: `web/src/screens/LaborPerformanceScreen.tsx`,
`internal/adapters/inbound/http/server.go`,
`internal/adapters/inbound/http/idempotency.go`,
`internal/application/usecases/define_standard.go`. Omitted: the
first-definition variant (`LaborStandardDefined`), the 409/422 error
branches. Standard events are **not** published on the integration
topic — no sibling receives them.

## 3. The ops agent explains a slow task type

```mermaid
sequenceDiagram
    autonumber
    actor Sup as Shift supervisor
    participant AG as warehouse-ops-agent
    participant MCP as labor-performance MCP server
    Sup->>AG: why is PICK slow right now
    AG->>MCP: qry: get_task_type_utilization taskType PICK windowSeconds 3600
    MCP-->>AG: taskSeconds, idleSeconds, openGapSeconds always 0 at task-type scope, utilizationPct
    AG->>MCP: qry: get_task_type_performance taskType PICK
    MCP-->>AG: taskCount, meanEfficiencyPct, meanActualSeconds
    AG->>MCP: qry: get_labor_standard taskType PICK
    MCP-->>AG: expectedSeconds, effectiveFrom
    AG-->>Sup: advisory - utilization vs efficiency vs target pace
```

Source: `internal/adapters/inbound/mcp/tools.go`,
`internal/adapters/inbound/mcp/mapping.go`; the agent's
`internal/adapters/outbound/mcpclient/labor_performance.go` and
`internal/application/usecases/flow_balance_advisory.go`. Omitted: the
agent's calls to other contexts, `get_associate_scorecard`, and the MCP
session handshake.

## 4. The hourly Labor Performance Report

```mermaid
sequenceDiagram
    autonumber
    participant LP as labor-performance
    participant PRJ as labor-performance projector
    participant RPT as labor-performance reports API
    participant CON as warehouse-console reports page
    LP-)PRJ: evt: com.warehouse.wes.labor-performance.performance.TaskPerformanceRecorded on warehouse.labor-performance.analytics
    LP-)PRJ: evt: com.warehouse.wes.labor-performance.standard.LaborStandardDefined on warehouse.labor-performance.analytics
    Note over PRJ: fold into labor_performance_rollup per task_type and hour
    CON->>RPT: qry: GET /reports/performance from to granularity hour
    RPT-->>CON: rows, byTaskType, totals - means null when nothing scored
    CON->>RPT: qry: GET /reports/performance/freshness
    RPT-->>CON: lagSeconds
```

Source: `internal/adapters/inbound/kafka/analytics_consumer.go`,
`internal/adapters/outbound/analyticsstore/postgres_projection.go`,
`internal/adapters/inbound/http/reports_handler.go`,
`internal/analytics/report/labor_performance.go`; console's
`src/features/context-reports/laborPerformance.config.tsx`. Omitted:
`LaborStandardRevised` (folded the same way) and the analytical DB, which
the projector writes and the reports API reads read-only (ADR 0007).

---
id: eventstorming
title: EventStorming (design level)
sidebar_label: EventStorming
sidebar_position: 6
description: Design-level EventStorming boards for Labor Performance — scoring a completed task, defining or revising a standard, and building the analytical report — in ddd-crew cheat-sheet notation, with code evidence for every sticky.
---

# EventStorming (design level)

Notation follows the [ddd-crew EventStorming glossary & cheat sheet](https://github.com/ddd-crew/eventstorming-glossary-cheat-sheet).
Each board reads left to right. Every sticky is backed by code in the
table under it; hotspots are known gaps recorded in ADRs or visible in
the code, not invented ones.

## Legend

```mermaid
flowchart LR
    A["Actor"]:::actor
    C["Command"]:::command
    AG["Aggregate"]:::aggregate
    E["Domain Event"]:::event
    P["Policy"]:::policy
    R["Read Model"]:::readmodel
    X["External System"]:::external
    H["Hotspot"]:::hotspot

    classDef actor fill:#fff59d,stroke:#b59f00,color:#000,font-size:11px;
    classDef command fill:#4aa3df,stroke:#1f6f9f,color:#000;
    classDef aggregate fill:#f7d84a,stroke:#a68b00,color:#000;
    classDef event fill:#f6a04d,stroke:#a85d12,color:#000;
    classDef policy fill:#c39bd3,stroke:#7d4f91,color:#000;
    classDef readmodel fill:#7dcea0,stroke:#2e8b57,color:#000;
    classDef external fill:#f1948a,stroke:#a93226,color:#000;
    classDef hotspot fill:#e74c3c,stroke:#922b21,color:#fff;
```

Source: ddd-crew cheat-sheet colours as specified for this fleet.
Omitted: nothing — this is the key only.

## Board 1 — Score a completed task

```mermaid
flowchart LR
    FE["fulfillment-execution"]:::external
    E0["TaskCompleted"]:::event
    P1["Whenever TaskCompleted arrives, record its performance once per event id"]:::policy
    C1["RecordTaskPerformance"]:::command
    RM0["LaborStandard active as of completion"]:::readmodel
    AG1["TaskPerformance"]:::aggregate
    AG2["IdlePeriod"]:::aggregate
    E1["TaskPerformanceRecorded"]:::event
    WFM["workforce-management"]:::external
    PRJ["labor-performance projector"]:::external
    RM1["Scorecard"]:::readmodel
    RM2["TaskTypePerformance"]:::readmodel
    RM3["Utilization"]:::readmodel
    H1["Hotspot: REBIN and other unknown task types are never scored"]:::hotspot
    H2["Hotspot: idle cap 3600 s is a judgment call, not a shift length"]:::hotspot
    H3["Hotspot: DLQ has no consumer or replay tooling"]:::hotspot

    FE --> E0 --> P1 --> C1
    RM0 --> C1
    C1 --> AG1
    C1 --> AG2
    AG1 --> E1
    E1 --> WFM
    E1 --> PRJ
    AG1 --> RM1
    AG1 --> RM2
    AG2 --> RM3
    H1 -.- C1
    H2 -.- AG2
    H3 -.- P1

    classDef actor fill:#fff59d,stroke:#b59f00,color:#000,font-size:11px;
    classDef command fill:#4aa3df,stroke:#1f6f9f,color:#000;
    classDef aggregate fill:#f7d84a,stroke:#a68b00,color:#000;
    classDef event fill:#f6a04d,stroke:#a85d12,color:#000;
    classDef policy fill:#c39bd3,stroke:#7d4f91,color:#000;
    classDef readmodel fill:#7dcea0,stroke:#2e8b57,color:#000;
    classDef external fill:#f1948a,stroke:#a93226,color:#000;
    classDef hotspot fill:#e74c3c,stroke:#922b21,color:#fff;
```

Source: `internal/adapters/inbound/kafka/consumer.go`,
`internal/application/usecases/record_task_performance.go`,
`internal/domain/performance/performance.go`,
`internal/domain/idleness/idleness.go`, `internal/domain/shared/events.go`,
`internal/application/ports/ports.go`. Omitted: the retry/backoff loop and
the outbox relay (infrastructure, not domain).

| Sticky | Kind | Code evidence |
|---|---|---|
| fulfillment-execution | External System | `cloudevents.TypeFulfillmentTaskCompleted`, topic `cloudevents.TopicFulfillmentEvents` |
| TaskCompleted | Domain Event (upstream) | `consumer.handleFulfillmentEvent`, `taskCompletedData` |
| Record once per event id | Policy | `ports.ProcessedEvents.MarkProcessed` inside `RecordTaskPerformance.Execute` |
| RecordTaskPerformance | Command | `usecases.RecordTaskPerformanceRequest` / `RecordTaskPerformance.Execute` |
| LaborStandard active as of completion | Read Model | `StandardRepo.FindActiveAsOf` in `standardSecondsAtCompletion` |
| TaskPerformance | Aggregate | `performance.New` |
| IdlePeriod | Aggregate | `idleness.New` in `recordIdleGap` |
| TaskPerformanceRecorded | Domain Event | `shared.NewTaskPerformanceRecorded`; CE type `com.warehouse.wes.labor-performance.performance.TaskPerformanceRecorded` |
| workforce-management | External System | integration topic `warehouse.labor-performance.events` (`integration_publisher.go`) |
| labor-performance projector | External System (own process) | analytics topic `warehouse.labor-performance.analytics` (`analytics_publisher.go`) |
| Scorecard | Read Model | `ports.Scorecard`, `GetAssociateScorecard` |
| TaskTypePerformance | Read Model | `ports.TaskTypePerformance`, `GetTaskTypePerformance` |
| Utilization | Read Model | `usecases.UtilizationResult`, `GetUtilization` |
| Unknown task types never scored | Hotspot | `shared.ParseTaskTypeLenient` doc comment (REBIN) |
| Idle cap is a judgment call | Hotspot | ADR 0014 "Negative / accepted"; `defaultIdleGapCapSeconds = 3600` |
| DLQ has no consumer | Hotspot | `dlqTopicSuffix = ".dlq"` in `consumer.go`; no reader of that topic exists in the repo |

## Board 2 — Define or revise a labor standard

```mermaid
flowchart LR
    A1["Industrial engineer"]:::actor
    RM0["Current standard"]:::readmodel
    C1["DefineStandard"]:::command
    AG1["LaborStandard"]:::aggregate
    E1["LaborStandardDefined"]:::event
    E2["LaborStandardRevised"]:::event
    P1["When a standard is open for the TaskType, close it at the same instant"]:::policy
    PRJ["labor-performance projector"]:::external
    H1["Hotspot: anyone can POST /standards - no auth since ADR 0012"]:::hotspot

    A1 --> RM0
    A1 --> C1
    C1 --> P1
    P1 --> AG1
    AG1 --> E1
    AG1 --> E2
    E1 --> PRJ
    E2 --> PRJ
    H1 -.- C1

    classDef actor fill:#fff59d,stroke:#b59f00,color:#000,font-size:11px;
    classDef command fill:#4aa3df,stroke:#1f6f9f,color:#000;
    classDef aggregate fill:#f7d84a,stroke:#a68b00,color:#000;
    classDef event fill:#f6a04d,stroke:#a85d12,color:#000;
    classDef policy fill:#c39bd3,stroke:#7d4f91,color:#000;
    classDef readmodel fill:#7dcea0,stroke:#2e8b57,color:#000;
    classDef external fill:#f1948a,stroke:#a93226,color:#000;
    classDef hotspot fill:#e74c3c,stroke:#922b21,color:#fff;
```

Source: `internal/adapters/inbound/http/server.go`,
`internal/application/usecases/define_standard.go`,
`internal/domain/standard/standard.go`, `internal/domain/shared/events.go`.
Omitted: the idempotency-key replay and the 409 concurrency branch (see
[Sequence diagrams](./sequence-diagrams.md)).

| Sticky | Kind | Code evidence |
|---|---|---|
| Industrial engineer | Actor | `web/src/screens/LaborPerformanceScreen.tsx` (`apiPost("/standards")`) |
| Current standard | Read Model | `GET /standards/{taskType}` → `GetStandard` → `StandardRepo.FindCurrentlyActive` |
| DefineStandard | Command | `POST /standards` → `usecases.DefineStandard.Execute` |
| Close the open standard | Policy | `prior.Close(now)` in `DefineStandard.Execute` |
| LaborStandard | Aggregate | `standard.New`, `LaborStandard.Close` |
| LaborStandardDefined | Domain Event | `shared.NewLaborStandardDefined`; CE type `com.warehouse.wes.labor-performance.standard.LaborStandardDefined` |
| LaborStandardRevised | Domain Event | `shared.NewLaborStandardRevised`; CE type `com.warehouse.wes.labor-performance.standard.LaborStandardRevised` |
| labor-performance projector | External System (own process) | `analytics_consumer.go` (`isProjecting`) |
| No auth on POST /standards | Hotspot | ADR 0012; `NewRouter` has no auth middleware |

## Board 3 — Build the Labor Performance Report

```mermaid
flowchart LR
    E1["TaskPerformanceRecorded"]:::event
    E2["LaborStandardDefined or Revised"]:::event
    P1["Whenever an analytics event arrives, fold it into its task type and hour bucket once"]:::policy
    RM1["labor_performance_rollup"]:::readmodel
    A1["Supervisor or ops agent"]:::actor
    RM2["Labor Performance Report"]:::readmodel
    RM3["Report freshness"]:::readmodel
    H1["Hotspot: a failed fold is logged and skipped, not retried"]:::hotspot

    E1 --> P1
    E2 --> P1
    P1 --> RM1
    RM1 --> RM2
    RM1 --> RM3
    A1 --> RM2
    A1 --> RM3
    H1 -.- P1

    classDef actor fill:#fff59d,stroke:#b59f00,color:#000,font-size:11px;
    classDef command fill:#4aa3df,stroke:#1f6f9f,color:#000;
    classDef aggregate fill:#f7d84a,stroke:#a68b00,color:#000;
    classDef event fill:#f6a04d,stroke:#a85d12,color:#000;
    classDef policy fill:#c39bd3,stroke:#7d4f91,color:#000;
    classDef readmodel fill:#7dcea0,stroke:#2e8b57,color:#000;
    classDef external fill:#f1948a,stroke:#a93226,color:#000;
    classDef hotspot fill:#e74c3c,stroke:#922b21,color:#fff;
```

Source: `internal/adapters/inbound/kafka/analytics_consumer.go`,
`internal/adapters/outbound/analyticsstore/postgres_projection.go`,
`internal/analytics/report/labor_performance.go`,
`internal/adapters/inbound/http/reports_handler.go`. Omitted: there is no
command and no aggregate on this board — the analytical side is a pure
projection (ADR 0007).

| Sticky | Kind | Code evidence |
|---|---|---|
| TaskPerformanceRecorded, LaborStandardDefined/Revised | Domain Event | `isProjecting` in `analytics_consumer.go` |
| Fold once per event | Policy | `analytics_consumed_events` gate + `claim` into `analytics_processed_events` in `postgres_projection.go` |
| labor_performance_rollup | Read Model | `migrations/analytics/0001_report.up.sql` |
| Supervisor or ops agent | Actor | console `laborPerformance.config.tsx`; agent `restclient/reports_clients.go` |
| Labor Performance Report | Read Model | `report.Build`, `GET /reports/performance` |
| Report freshness | Read Model | `ReportStore.FreshnessLag`, `GET /reports/performance/freshness` |
| Failed fold is skipped | Hotspot | `AnalyticsConsumer.Run` logs a `Handle` error and keeps reading (`ReadMessage` auto-commits) |

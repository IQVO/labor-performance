---
id: class-diagram
title: Class diagrams
sidebar_label: Class diagrams
sidebar_position: 8
description: UML class diagrams of Labor Performance's domain model, application ports and read models, plus the hexagonal ports-and-adapters view — real type names, fields and state-changing methods from internal/domain and internal/application.
---

# Class diagrams

Real type names from `internal/domain/**` and `internal/application/**`.
Go pointer fields (`*int64`, `*float64`, `*time.Time`) are shown as
`nullable`; unexported fields keep their Go names with a `-` prefix.
Getters that only return a field are omitted; constructors, mutators and
pure domain functions are shown.

## 1. Domain model

```mermaid
classDiagram
    direction LR

    class LaborStandard {
        <<AggregateRoot>>
        -id StandardId
        -taskType TaskType
        -expectedSeconds int64
        -travelComponentSeconds int64 nullable
        -effectiveFrom time.Time
        -effectiveTo time.Time nullable
        -version int
        +New(id, taskType, expectedSeconds, travelComponentSeconds, effectiveFrom) LaborStandard
        +Close(at time.Time)
        +IsActiveAt(t time.Time) bool
        +Version() int
    }

    class TaskPerformance {
        <<AggregateRoot>>
        -eventId string
        -taskId string
        -associateId AssociateId
        -taskType TaskType
        -actualSeconds int64
        -standardSecondsAtCompletion int64
        -efficiencyPct float64 nullable
        -completedAt time.Time
        +New(eventId, taskId, associateId, taskType, actualSeconds, standardSecondsAtCompletion, completedAt) TaskPerformance
    }

    class IdlePeriod {
        <<AggregateRoot>>
        -associateId AssociateId
        -taskType TaskType
        -startedAt time.Time
        -endedAt time.Time
        -seconds int64
        -capped bool
        +New(associateId, taskType, startedAt, endedAt, capSeconds) IdlePeriod
    }

    class TaskType {
        <<Enumeration>>
        PICK
        PACK
        SLAM
        +NewTaskType(value) TaskType
        +ParseTaskTypeLenient(raw) TaskType
    }

    class TrendDirection {
        <<Enumeration>>
        IMPROVING
        DECLINING
        STABLE
        INSUFFICIENT_DATA
        +ClassifyTrend(recentMean, baselineMean, recentScoredCount) TrendDirection
        +DetectCoachingFlag(recentScoredEfficiencyPctsChronological) bool
    }

    class StandardId {
        <<ValueObject>>
        string
    }

    class AssociateId {
        <<ValueObject>>
        string
    }

    class DomainEvent {
        <<interface>>
        +EventName() string
        +OccurredAt() time.Time
    }

    class LaborStandardDefined {
        <<DomainEvent>>
        +StandardId StandardId
        +TaskType TaskType
        +ExpectedSeconds int64
        +TravelComponentSeconds int64 nullable
        +EffectiveFrom time.Time
    }

    class LaborStandardRevised {
        <<DomainEvent>>
        +StandardId StandardId
        +TaskType TaskType
        +PreviousExpectedSeconds int64
        +NewExpectedSeconds int64
        +NewTravelComponentSeconds int64 nullable
        +EffectiveFrom time.Time
    }

    class TaskPerformanceRecorded {
        <<DomainEvent>>
        +TaskId string
        +AssociateId AssociateId
        +TaskType TaskType
        +ActualSeconds int64
        +EfficiencyPct float64 nullable
        +IdleSecondsBefore int64 nullable
        +CompletedAt time.Time
    }

    LaborStandard *-- StandardId
    LaborStandard *-- TaskType
    TaskPerformance *-- AssociateId
    TaskPerformance *-- TaskType
    IdlePeriod *-- AssociateId
    IdlePeriod *-- TaskType
    DomainEvent <|.. LaborStandardDefined
    DomainEvent <|.. LaborStandardRevised
    DomainEvent <|.. TaskPerformanceRecorded
    LaborStandard ..> LaborStandardDefined : raised by DefineStandard
    LaborStandard ..> LaborStandardRevised : raised by DefineStandard
    TaskPerformance ..> TaskPerformanceRecorded : raised by RecordTaskPerformance
    IdlePeriod ..> TaskPerformanceRecorded : seconds as IdleSecondsBefore
    TaskPerformance ..> TrendDirection : scored rows feed
```

Source: `internal/domain/standard/standard.go`,
`internal/domain/performance/performance.go`,
`internal/domain/performance/trend.go`,
`internal/domain/idleness/idleness.go`, `internal/domain/shared/shared.go`,
`internal/domain/shared/events.go`. Omitted: `Rehydrate` constructors,
plain getters, the embedded `base` struct of every event (`Name`, `At`),
domain error variables (listed on the
[Aggregate Design Canvas](./aggregate-design-canvas.md)), and
`idleness.UtilizationPct` (a pure function, no type). `TrendDirection`'s
two functions are package-level functions in `performance`, drawn on the
enum for compactness. No aggregate holds a reference to another
aggregate — `TaskPerformance` copies the standard's seconds instead of
pointing at a `LaborStandard`.

## 2. Application ports and read models

```mermaid
classDiagram
    direction LR

    class StandardRepo {
        <<Repository>>
        +Save(ctx, s LaborStandard) error
        +FindActiveAsOf(ctx, taskType, t) LaborStandard
        +FindCurrentlyActive(ctx, taskType) LaborStandard
        +NextID(ctx) StandardId
    }

    class PerformanceRepo {
        <<Repository>>
        +Save(ctx, p TaskPerformance) error
        +ExistsByAssociateID(ctx, associateId) bool
        +ScorecardFor(ctx, associateId) Scorecard
        +TaskTypePerformanceFor(ctx, taskType) TaskTypePerformance
        +RecentByAssociateID(ctx, associateId, limit) list
        +SumActualSecondsByTaskType(ctx, taskType, since) int64
        +SumActualSecondsByAssociate(ctx, associateId, since) int64
    }

    class IdlePeriodRepo {
        <<Repository>>
        +Save(ctx, p IdlePeriod) error
        +SumByTaskType(ctx, taskType, since) int64, int
        +SumByAssociate(ctx, associateId, since) int64, int
        +LastEndedAtByAssociate(ctx, associateId) time.Time
        +DistinctAssociatesByTaskType(ctx, taskType, since) int
    }

    class ProcessedEvents {
        <<interface>>
        +MarkProcessed(ctx, eventId) bool
    }

    class EventPublisher {
        <<interface>>
        +Publish(ctx, events) error
    }

    class UnitOfWork {
        <<interface>>
        +Execute(ctx, fn) error
    }

    class Clock {
        <<interface>>
        +Now() time.Time
    }

    class StandardMetrics {
        <<interface>>
        +StandardDefinitionAccepted(ctx)
        +StandardDefinitionRejected(ctx)
    }

    class Scorecard {
        <<ReadModel>>
        +AssociateId AssociateId
        +TaskCount int
        +MeanEfficiencyPct float64 nullable
        +ByTaskType map of TaskTypeBreakdown
        +Trend TrendDirection
        +CoachingFlag bool
    }

    class TaskTypeBreakdown {
        <<ReadModel>>
        +TaskCount int
        +MeanEfficiencyPct float64 nullable
    }

    class TaskTypePerformance {
        <<ReadModel>>
        +TaskType TaskType
        +TaskCount int
        +MeanEfficiencyPct float64 nullable
        +MeanActualSeconds float64 nullable
    }

    class UtilizationResult {
        <<ReadModel>>
        +TaskType TaskType
        +AssociateId AssociateId
        +Associates int
        +WindowSeconds int64
        +TaskSeconds int64
        +IdleSeconds int64
        +OpenGapSeconds int64
        +UtilizationPct float64 nullable
    }

    Scorecard *-- TaskTypeBreakdown
    PerformanceRepo ..> Scorecard : builds
    PerformanceRepo ..> TaskTypePerformance : builds
```

Source: `internal/application/ports/ports.go`,
`internal/application/usecases/get_utilization.go`. Omitted: the two
sentinel errors in `ports/errors.go` (`ErrConcurrentModification`,
`ErrOpenStandardConflict`), and the analytical side's own ports
(`report.ReportStore`, `report.ProjectionStore` in
`internal/analytics/report/ports.go`), which depend on nothing in this
diagram by design (ADR 0007, arch-go enforced).

## 3. Use cases

```mermaid
classDiagram
    direction LR
    class DefineStandard {
        +Standards StandardRepo
        +Events EventPublisher
        +Clock Clock
        +UnitOfWork UnitOfWork
        +Metrics StandardMetrics
        +Execute(ctx, taskType, expectedSeconds, travelComponentSeconds) LaborStandard
    }
    class RecordTaskPerformance {
        +Performances PerformanceRepo
        +Standards StandardRepo
        +Processed ProcessedEvents
        +Events EventPublisher
        +Clock Clock
        +UnitOfWork UnitOfWork
        +IdlePeriods IdlePeriodRepo
        +IdleGapCapSeconds int64
        +Execute(ctx, req) TaskPerformance
    }
    class GetStandard {
        +Standards StandardRepo
        +Execute(ctx, taskType) LaborStandard
    }
    class GetAssociateScorecard {
        +Performances PerformanceRepo
        +Execute(ctx, associateId) Scorecard
    }
    class GetTaskTypePerformance {
        +Performances PerformanceRepo
        +Execute(ctx, taskType) TaskTypePerformance
    }
    class GetUtilization {
        +Performances PerformanceRepo
        +IdlePeriods IdlePeriodRepo
        +Clock Clock
        +ForTaskType(ctx, taskType, window) UtilizationResult
        +ForAssociate(ctx, associateId, window) UtilizationResult
    }
```

Source: `internal/application/usecases/*.go`. Omitted: the optional
`Logger` field on `RecordTaskPerformance` and the private helper
functions (`atomically`, `trendAndCoachingFlag`, `openGapSeconds`).

## 4. Hexagonal view: ports and adapters

```mermaid
flowchart LR
    subgraph IN["Inbound adapters"]
        HTTP["inbound/http NewRouter<br/>cmd/labor :8080"]
        RPT["inbound/http NewReportsRouter<br/>cmd/labor-reports :8092"]
        MCP["inbound/mcp<br/>cmd/mcp :8090"]
        KC["inbound/kafka Consumer<br/>warehouse.fulfillment.events"]
        KA["inbound/kafka AnalyticsConsumer<br/>cmd/labor-projector"]
    end

    subgraph APP["Application - usecases"]
        UC1["DefineStandard"]
        UC2["RecordTaskPerformance"]
        UC3["GetStandard, GetAssociateScorecard,<br/>GetTaskTypePerformance, GetUtilization"]
    end

    subgraph DOM["Domain"]
        D1["standard, performance,<br/>idleness, shared"]
    end

    subgraph ANA["internal/analytics/report"]
        R1["Build, ReportStore, ProjectionStore"]
    end

    subgraph OUT["Outbound adapters"]
        PG["outbound/postgres<br/>repos, UnitOfWork, OutboxPublisher,<br/>OutboxRelay, Sweeper"]
        MEM["outbound/memory<br/>repos, SystemClock"]
        KP["outbound/kafka<br/>Analytics, Integration, FanOut publishers, RelaySink"]
        LOG["outbound/events LogPublisher"]
        AS["outbound/analyticsstore<br/>PostgresProjection, PostgresReport"]
        TEL["outbound/telemetry StandardMetrics"]
    end

    HTTP --> UC1
    HTTP --> UC3
    MCP --> UC3
    KC --> UC2
    UC1 --> D1
    UC2 --> D1
    UC3 --> D1
    UC1 -.->|StandardRepo, EventPublisher,<br/>UnitOfWork, StandardMetrics| PG
    UC2 -.->|PerformanceRepo, IdlePeriodRepo,<br/>ProcessedEvents| PG
    UC2 -.-> MEM
    PG -->|outbox rows| KP
    UC2 -.->|EventPublisher| LOG
    UC1 -.-> TEL
    KA --> R1
    RPT --> R1
    R1 -.->|ProjectionStore, ReportStore| AS
```

Source: `cmd/labor/main.go`, `cmd/mcp/main.go`,
`cmd/labor-projector/main.go`, `cmd/labor-reports/main.go`,
`internal/adapters/**`, `internal/architecture/architecture_test.go`.
Omitted: `adapters/kafka/cloudevents` and `adapters/kafka/otelkafka`
(shared helpers), `outbound/bootretry`, and the per-port wiring of the
in-memory adapters (`DATABASE_URL` unset swaps every Postgres adapter for
its `memory` twin and drops the unit of work). Dotted arrows are "port
implemented by adapter"; the domain depends on nothing and the analytics
package depends on nothing outside itself (arch-go).

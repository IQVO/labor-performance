---
id: sequence-diagrams
title: Sequence diagrams
sidebar_label: Sequence diagrams
sidebar_position: 10
description: UML sequence diagrams for Labor Performance's use cases — DefineStandard over REST, RecordTaskPerformance from Kafka, the outbox relay, the scorecard and utilization queries, and the analytics projector — following the real function bodies.
---

# Sequence diagrams

One diagram per use case or group of use cases, drawn from the function
bodies. Participants are the inbound adapter, the use case, the domain
aggregate, the repositories (ports with their Postgres adapters), the
outbox and the downstream. Error branches are `alt` blocks. When
`DATABASE_URL` is unset the same use cases run on the in-memory adapters
with no unit of work and the log publisher; these diagrams show the
Postgres + `EVENT_PUBLISHER=kafka` configuration.

## 1. DefineStandard — `POST /standards`

```mermaid
sequenceDiagram
    autonumber
    actor Client
    participant MW as RequireIdempotencyKey
    participant H as http handleDefineStandard
    participant UC as usecases.DefineStandard
    participant SR as StandardRepo postgres
    participant AG as standard.LaborStandard
    participant OB as OutboxPublisher
    participant DB as Postgres

    Client->>MW: POST /standards with Idempotency-Key and body
    alt header missing
        MW-->>Client: 400 idempotency-key-required
    end
    MW->>DB: BEGIN, INSERT idempotency_keys ON CONFLICT DO NOTHING
    alt key already stored
        MW->>DB: ROLLBACK, SELECT stored response
        alt same request hash
            MW-->>Client: replay stored status and body
        else different body
            MW-->>Client: 422 idempotency-key-reused
        end
    end
    MW->>H: request with tx in context
    H->>H: shared.NewTaskType
    alt unknown task type
        H-->>MW: 400 unknown-task-type
    end
    H->>UC: Execute taskType, expectedSeconds, travelComponentSeconds
    UC->>SR: FindCurrentlyActive taskType
    SR-->>UC: prior or nil
    UC->>SR: NextID
    UC->>AG: standard.New id, taskType, seconds, travel, now
    alt invariant violated
        AG-->>UC: ErrNonPositiveExpectedSeconds or travel error
        UC-->>H: error, metrics rejected
        H-->>MW: 422 problem
    end
    Note over UC,DB: UnitOfWork.Execute reuses the middleware tx
    opt prior exists
        UC->>AG: prior.Close now
        UC->>SR: Save prior with version guard
        alt version moved
            SR-->>UC: ErrConcurrentModification
            UC-->>H: error
            H-->>MW: 409 concurrent-modification
        end
    end
    UC->>SR: Save next
    UC->>OB: Publish LaborStandardRevised or LaborStandardDefined
    OB->>DB: INSERT outbox_events for analytics topic
    UC-->>H: new standard, metrics accepted
    H-->>MW: 201 standard
    MW->>DB: UPDATE idempotency_keys with response, COMMIT
    MW-->>Client: 201 standard
```

Source: `internal/adapters/inbound/http/idempotency.go`,
`internal/adapters/inbound/http/server.go` (`handleDefineStandard`),
`internal/adapters/inbound/http/errors.go`,
`internal/application/usecases/define_standard.go`,
`internal/adapters/outbound/postgres/standard_repo.go`,
`internal/adapters/outbound/postgres/unit_of_work.go`,
`internal/adapters/outbound/postgres/outbox_publisher.go`. Omitted: the
log publisher that runs alongside the outbox in the fan-out, body
decoding errors, and the `ErrOpenStandardConflict` branch (partial unique
index violation on `Save next`, the database backstop of ADR 0022).

## 2. RecordTaskPerformance — Kafka `TaskCompleted`

```mermaid
sequenceDiagram
    autonumber
    participant K as Kafka warehouse.fulfillment.events
    participant C as inbound/kafka Consumer
    participant UC as usecases.RecordTaskPerformance
    participant PE as ProcessedEvents
    participant SR as StandardRepo
    participant PR as PerformanceRepo
    participant AG as performance.TaskPerformance
    participant IR as IdlePeriodRepo
    participant OB as OutboxPublisher
    participant DLQ as Kafka warehouse.fulfillment.events.dlq

    K->>C: FetchMessage
    C->>C: cloudevents.Decode
    alt not a valid CloudEvent
        C->>DLQ: original message plus x-dlq headers
        C->>K: CommitMessages
    end
    alt type is not fulfillment-execution TaskCompleted
        C->>K: CommitMessages - skipped
    end
    C->>UC: Execute KafkaEventId, TaskId, AssociateId, TaskType lenient, ActualSeconds, CompletedAt
    Note over UC,OB: one UnitOfWork transaction
    UC->>PE: MarkProcessed event id
    alt already processed
        PE-->>UC: false
        UC-->>C: nil - duplicate, no-op
    end
    opt task type known
        UC->>SR: FindActiveAsOf taskType, completedAt
        SR-->>UC: standard or nil
    end
    UC->>AG: performance.New with standardSecondsAtCompletion
    AG-->>UC: TaskPerformance, efficiencyPct or nil
    opt associate present
        UC->>PR: RecentByAssociateID limit 1
        PR-->>UC: previous completion or none
    end
    UC->>PR: Save TaskPerformance
    opt previous completion exists
        UC->>UC: idleness.New previous completion to claim instant, capped
        alt gap not positive
            UC->>UC: log and skip, idle_seconds_before nil
        else gap recorded
            UC->>IR: Save IdlePeriod
        end
    end
    UC->>OB: Publish TaskPerformanceRecorded
    OB->>OB: INSERT outbox_events for analytics and integration topics
    UC-->>C: TaskPerformance
    alt handler error after 3 attempts with backoff
        C->>DLQ: original message plus x-dlq-error
    end
    C->>K: CommitMessages
```

Source: `internal/adapters/inbound/kafka/consumer.go`,
`internal/application/usecases/record_task_performance.go`,
`internal/domain/performance/performance.go`,
`internal/domain/idleness/idleness.go`,
`internal/adapters/outbound/postgres/processed_event_repo.go`,
`internal/adapters/outbound/postgres/outbox_publisher.go`. Omitted: OTel
consume spans, and the transaction rollback on any write error (the
whole unit of work, including the `processed_events` marker, rolls back,
so the retry re-scores the event).

## 3. Outbox relay — publishing committed events

```mermaid
sequenceDiagram
    autonumber
    participant R as postgres.OutboxRelay
    participant DB as Postgres outbox_events
    participant S as kafka.RelaySink
    participant T1 as warehouse.labor-performance.analytics
    participant T2 as warehouse.labor-performance.events
    participant W as postgres.Sweeper

    loop every OUTBOX_RELAY_INTERVAL default 1s
        R->>DB: BEGIN, SELECT up to 100 unpublished rows ORDER BY id FOR UPDATE SKIP LOCKED
        loop each row in id order
            R->>S: Send topic, key, CloudEvent bytes, headers
            alt send fails
                R->>DB: UPDATE attempts and last_error, COMMIT
                Note over R: stop the pass, row retried next tick
            else topic is analytics
                S->>T1: write message key task_type
                R->>DB: UPDATE published_at now
            else topic is events
                S->>T2: write message key associate_id
                R->>DB: UPDATE published_at now
            end
        end
        R->>DB: COMMIT
    end
    loop every HOUSEKEEPING_INTERVAL default 1h
        W->>DB: DELETE published rows older than OUTBOX_RETENTION default 7d in batches
    end
```

Source: `internal/adapters/outbound/postgres/outbox_relay.go`,
`internal/adapters/outbound/kafka/relay_sink.go`,
`internal/adapters/outbound/postgres/sweeper.go`, `cmd/labor/main.go`.
Omitted: the sweeper's `idempotency_keys` deletion (TTL 24 h), the outbox
lag gauge, and the immediate follow-up pass when a full batch was sent.

## 4. Queries — scorecard, task-type performance, standard

```mermaid
sequenceDiagram
    autonumber
    actor Caller as Console, REST client or ops agent
    participant IN as http handler or MCP tool
    participant UC as usecases.GetAssociateScorecard
    participant PR as PerformanceRepo
    participant TF as performance trend functions

    Caller->>IN: GET /associates/assoc-42/scorecard or get_associate_scorecard
    IN->>UC: Execute assoc-42
    alt empty associate id
        UC-->>IN: ErrAssociateNotFound
        IN-->>Caller: 404 associate-not-found
    end
    UC->>PR: ExistsByAssociateID
    alt never seen
        UC-->>IN: ErrAssociateNotFound
        IN-->>Caller: 404 associate-not-found
    end
    UC->>PR: ScorecardFor
    PR-->>UC: taskCount, meanEfficiencyPct nullable, byTaskType
    UC->>PR: RecentByAssociateID limit 10
    UC->>TF: ClassifyTrend recent mean vs baseline
    UC->>TF: DetectCoachingFlag last 3 scored
    UC-->>IN: Scorecard
    IN-->>Caller: 200 scorecard with trend and coachingFlag
    Note over Caller,PR: GetTaskTypePerformance and GetStandard are single repo reads - TaskTypePerformanceFor and FindCurrentlyActive, 404 standard-not-found when no standard is open
```

Source: `internal/application/usecases/get_associate_scorecard.go`,
`internal/application/usecases/get_task_type_performance.go`,
`internal/application/usecases/get_standard.go`,
`internal/adapters/inbound/http/server.go`,
`internal/adapters/inbound/mcp/tools.go`. Omitted: the MCP resource
`scorecard://labor/{associateId}`, which calls the same use case; the
400 for an unknown task type on the task-type routes.

## 5. GetUtilization — per associate

```mermaid
sequenceDiagram
    autonumber
    actor Caller
    participant H as http handleGetAssociateUtilization
    participant UC as usecases.GetUtilization
    participant PR as PerformanceRepo
    participant IR as IdlePeriodRepo

    Caller->>H: GET /associates/assoc-42/utilization?window=1h
    H->>UC: ForAssociate assoc-42, window or default 1h
    UC->>PR: SumActualSecondsByAssociate since
    UC->>IR: SumByAssociate since
    UC->>PR: RecentByAssociateID limit 1
    UC->>IR: LastEndedAtByAssociate
    UC->>UC: openGapSeconds from last activity to now, clipped to window
    UC->>UC: idleness.UtilizationPct task vs idle plus open gap
    UC-->>H: UtilizationResult, utilizationPct nil when nothing observed
    H-->>Caller: 200 utilization
```

Source: `internal/application/usecases/get_utilization.go`,
`internal/domain/idleness/idleness.go`,
`internal/adapters/inbound/http/server.go` (`windowParam`). Omitted:
`ForTaskType` (REST `GET /task-types/{taskType}/utilization` and MCP
`get_task_type_utilization`), which sums the same two repos per task type,
adds `DistinctAssociatesByTaskType`, and computes no open gap.

## 6. Analytics projector and reports API

```mermaid
sequenceDiagram
    autonumber
    participant T as Kafka warehouse.labor-performance.analytics
    participant AC as inbound/kafka AnalyticsConsumer
    participant UW as analyticsstore.UnitOfWork
    participant CE as ConsumedEventsRepo
    participant PJ as analyticsstore.PostgresProjection
    participant ADB as Analytics Postgres
    actor Client
    participant RH as http ReportsHandlers
    participant RS as analyticsstore.PostgresReport

    T->>AC: FetchMessage group labor-performance-analytics
    AC->>AC: cloudevents.Decode
    alt invalid, not a projected type or undecodable data
        AC->>AC: log and skip, commit offset
    end
    AC->>UW: Execute claim and apply
    UW->>ADB: BEGIN
    UW->>CE: MarkProcessed id, same tx
    alt already consumed
        CE-->>UW: false - nothing to apply
    end
    UW->>PJ: Apply TaskPerformanceRecorded, LaborStandardDefined or Revised
    PJ->>ADB: INSERT analytics_processed_events ON CONFLICT DO NOTHING
    PJ->>ADB: UPSERT labor_performance_rollup task_type, hour_bucket, counters
    alt every step succeeded
        UW->>ADB: COMMIT
        AC->>T: CommitMessages offset
    else a step failed
        UW->>ADB: ROLLBACK, claim undone
        AC->>AC: backoff 200 ms to 5 s, retry same message, offset not committed
    end
    Client->>RH: GET /reports/performance?from and to
    RH->>RS: Query
    RS->>ADB: SELECT rollup rows in window, read-only pool
    RS-->>RH: report.Build rows, byTaskType, totals
    RH-->>Client: 200 report
```

Source: `internal/adapters/inbound/kafka/analytics_consumer.go`,
`internal/adapters/outbound/analyticsstore/unit_of_work.go`,
`internal/adapters/outbound/analyticsstore/consumed_events_repo.go`,
`internal/adapters/outbound/analyticsstore/postgres_projection.go`,
`internal/adapters/outbound/analyticsstore/postgres_report.go`,
`internal/adapters/inbound/http/reports_handler.go`,
`internal/analytics/report/labor_performance.go`. Omitted: the
`/reports/performance/freshness` read (max `occurred_at` lag from
`analytics_processed_events`) and the 400 branches for missing or
malformed `from`/`to`.

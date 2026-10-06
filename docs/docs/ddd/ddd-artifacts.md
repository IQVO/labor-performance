---
id: ddd-artifacts
title: DDD artifact pack (ddd-crew)
sidebar_label: Artifact pack index
sidebar_position: 1
description: Index of the ddd-crew DDD artifacts and UML/ER/sequence diagrams for the Labor Performance bounded context, all derived from the code on develop.
---

# DDD artifact pack (ddd-crew)

This pack describes the **Labor Performance** bounded context with the
[ddd-crew](https://github.com/ddd-crew) modelling tools plus UML, ER and
sequence diagrams. Every diagram is Mermaid, every page lists the source
files it was derived from, and every page states what it leaves out.

| Artifact | Page | ddd-crew tool / notation it follows |
|---|---|---|
| Core Domain Chart | [Core Domain Chart](./core-domain-chart.md) | [Core Domain Charts](https://github.com/ddd-crew/core-domain-charts) |
| Bounded Context Canvas | [Bounded Context Canvas](./bounded-context-canvas.md) | [Bounded Context Canvas v5](https://github.com/ddd-crew/bounded-context-canvas) |
| Context Map | [Context map](../ecosystem/context-map.md) (kept under Ecosystem, not duplicated here) | [Context Mapping](https://github.com/ddd-crew/context-mapping) |
| Aggregate Design Canvas | [Aggregate Design Canvas](./aggregate-design-canvas.md) | [Aggregate Design Canvas v1.1](https://github.com/ddd-crew/aggregate-design-canvas) |
| Domain Message Flow | [Domain Message Flow](./domain-message-flow.md) | [Domain Message Flow Modelling](https://github.com/ddd-crew/domain-message-flow-modelling) |
| EventStorming (design level) | [EventStorming](./eventstorming.md) | [EventStorming glossary & cheat sheet](https://github.com/ddd-crew/eventstorming-glossary-cheat-sheet) |
| Ubiquitous Language | [Ubiquitous language](./ubiquitous-language.md) | Glossary mapped to code identifiers |
| UML class diagrams | [Class diagrams](./class-diagram.md) | UML class diagram + hexagonal ports/adapters view |
| ER diagram | [Entity-relationship](./entity-relationship.md) | Crow's-foot ER of the final migrated schema (OLTP and analytics) |
| UML sequence diagrams | [Sequence diagrams](./sequence-diagrams.md) | UML sequence diagrams per command use case |
| Domain events | [Domain events](./domain-events.md) | Published Language catalogue (CloudEvents 1.0) |

Related narrative pages: [Subdomain classification](./subdomain-classification.md)
and [Domain vision](../business-context/domain-vision.md).

## The context in one paragraph

Labor Performance is a **Supporting** subdomain and a **downstream
observer**. Its only input is `fulfillment-execution`'s
`com.warehouse.wes.fulfillment-execution.task.TaskCompleted` CloudEvent on
`warehouse.fulfillment.events`; its only human-driven write is
`POST /standards`. It scores each completed task against the engineered
labor standard that was active when the task finished, derives the idle
gap before it, and publishes `TaskPerformanceRecorded` for
`workforce-management`. It **never calls a sibling context** over REST or
MCP — every relationship on the [context map](../ecosystem/context-map.md)
is either an event it consumes, an event it publishes, or a read another
context makes into its own surfaces.

## Sources of truth

The code on `develop` wins over every page in this pack. The pages were
derived from:

- `internal/domain/**` — the three aggregates (`standard.LaborStandard`,
  `performance.TaskPerformance`, `idleness.IdlePeriod`), the `shared`
  value objects, domain errors and the three domain events
  (`internal/domain/shared/events.go`), plus the pure trend/coaching
  functions in `internal/domain/performance/trend.go`.
- `internal/application/usecases/**` and
  `internal/application/ports/{ports,errors}.go` — the six use cases and
  the eight outbound ports.
- `internal/adapters/**` — the REST routers (`inbound/http/server.go`,
  `inbound/http/reports_handler.go`), the idempotency middleware, the MCP
  tools/resources/prompts (`inbound/mcp/`), the Kafka consumers
  (`inbound/kafka/consumer.go`, `inbound/kafka/analytics_consumer.go`),
  the CloudEvents helper (`kafka/cloudevents/cloudevents.go`), the
  publishers, the outbox relay and the sweeper.
- `internal/analytics/report/**` — the analytical read model.
- `migrations/*.up.sql` and `migrations/analytics/*.up.sql` — the schema.
- `apis/openapi.yaml`, `apis/openapi-reports.yaml` and `apis/asyncapi.yaml`
  — the published contracts.
- `docs/docs/adr/` — the decisions behind the shape (ADR 0001–0030).
- For the relationships, each collaborating repository's own `develop`
  branch (paths cited on the [Context map](../ecosystem/context-map.md)).

When a page and the code disagree, the code is right and the page is a bug.

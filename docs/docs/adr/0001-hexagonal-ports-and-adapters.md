---
id: 0001-hexagonal-ports-and-adapters
slug: /adr/0001-hexagonal-ports-and-adapters
title: 0001. Hexagonal (ports & adapters) architecture
sidebar_label: 0001. Hexagonal architecture
description: ADR 0001 — adopt ports & adapters with a strict inward-only dependency rule.
---

# 0001. Hexagonal (ports & adapters) architecture

## Status

Accepted. Established with the initial implementation of this bounded context.

## Context

Labor Performance is a **Supporting subdomain**, not a Core one: the value
here is not a clever algorithm, it is that "actual vs standard" is *correct
and legible* — that a standard revision never rewrites the historical
record it was compared against, that a duplicate Kafka message never
double-counts a completed task, and that an unmeasurable completion
(`duration_seconds=0`, or no standard yet defined for a task type) yields a
clean `null` score rather than a fabricated number. The rules are simple to
state and easy to get quietly wrong, especially under the pressure of an
at-least-once Kafka delivery guarantee.

Several forces push against keeping them clean:

- **This context's whole job is consuming an event it does not control.**
  `RecordTaskPerformance` is triggered by a `TaskCompleted` message from
  `fulfillment-execution` — a different bounded context's wire format,
  arriving out of order or redelivered. Written naively, `kafka-go` types
  end up inside the business logic and the invariants become untestable
  without a live broker.
- **The rules must be testable without infrastructure.** "A duplicate
  `event_id` is a no-op, not a double-count" and "EfficiencyPct never
  divides by zero" each deserve a dedicated failing-path test. That
  investment only pays off if tests run in milliseconds and need no
  Postgres and no broker.
- **The frozen-history invariant only survives in one place.** Once
  `StandardSecondsAtCompletion` is resolved at ingestion time (see
  [ADR 0004](./0004-standard-frozen-at-completion-time-not-recomputed.md)),
  nothing may ever recompute it from a since-revised standard — that
  guarantee only holds if the resolution logic lives in exactly one place
  the domain/application layers control, not scattered across a SQL
  projection or an ad hoc adapter query.
- **The service must run without Postgres or Kafka for local development
  and CI.** `go test ./...` and a developer poking at the REST API need a
  fully functional service with in-memory adapters only.
- **The rest of the fleet already does this.** All six sibling services
  (`order-management`, `inventory-storage`, `wes-work-planning`,
  `workforce-management`, `fulfillment-execution`, `facility-layout`) are
  hexagonal. A seventh shaped differently would be a tax on every reader
  who moves between them.

## Decision

**We will structure the service as a hexagon (ports & adapters), with a
strict inward-only dependency rule:**

> **domain depends on nothing; application depends on domain; adapters
> depend on application/domain.**

Concretely:

- `internal/domain/` is **pure Go**. No `chi`, no `pgx`, no `kafka-go`, no
  `net/http`. The `LaborStandard` aggregate enforces
  `ExpectedSeconds > 0` and its own append-only-history `Close` semantics;
  the `TaskPerformance` aggregate enforces the never-divide-by-zero
  `EfficiencyPct` computation on construction, not as an adapter-side
  afterthought.
- `internal/application/ports/` declares the **outbound interfaces** the
  application needs: `StandardRepo`, `PerformanceRepo`, `ProcessedEvents`,
  `EventPublisher`, `Clock`. They are owned by the application and
  expressed in *this* context's types — never in fulfillment-execution's
  wire types (see
  [ADR 0003](./0003-kafka-choreography-consumer-of-fulfillment-execution.md)).
- `internal/application/usecases/` holds **one struct per use case**
  (`DefineStandard`, `GetStandard`, `RecordTaskPerformance`,
  `GetAssociateScorecard`, `GetTaskTypePerformance`, `GetUtilization`),
  with collaborators as plain fields. No use case imports an adapter
  package.
- `internal/adapters/` implements the ports: `inbound/http` (chi, DTOs,
  RFC 7807 error mapping), `inbound/kafka` (the `TaskCompleted` consumer),
  `inbound/mcp` (the MCP read adapter, [ADR
  0009](./0009-mcp-inbound-adapter.md)), `outbound/postgres`,
  `outbound/memory`, `outbound/events` (log publisher),
  `outbound/kafka` (the outbox relay sink).
- Four composition roots now exist, each the only file in its binary
  that reads environment variables for its own concerns: `cmd/labor`
  (the OLTP HTTP + Kafka-consumer service — also the only one with
  `StandardRepo`/`PerformanceRepo` writers), `cmd/mcp` (the MCP server,
  ADR 0009), `cmd/labor-reports` (the analytics reader, ADR 0007), and
  `cmd/labor-projector` (the analytics projector admin server). Every
  root reads its OWN env vars for its own concerns (e.g. `cmd/labor`
  reads `CORS_ALLOWED_ORIGINS` in `server.go`'s router builder and every
  root reads `ENVIRONMENT` via `telemetry.Environment`) — the rule is
  "no env var read outside a composition root," not "only one root may
  exist."

`Clock` is a port for the same reason the repositories are: "as of
`CompletedAt`" resolution and standard-revision timestamps are domain
outputs computed from "now", so time is injected rather than read from
`time.Now()` inside a use case. That is what makes the replay/backfill
tests in [ADR 0004](./0004-standard-frozen-at-completion-time-not-recomputed.md)
exact instead of tolerance-based.

## Consequences

### Easier

- **Invariants are unit-testable in microseconds.** The domain has no I/O,
  so every failing path (`ErrNonPositiveExpectedSeconds`,
  `ErrEmptyEventId`, `ErrEmptyTaskId`, the `EfficiencyPct` zero-guard) is a
  table-driven test. This is what made the >90% domain+application
  coverage gate affordable.
- **The Kafka consumer is a fake-reader unit test, not an integration
  test.** `handleFulfillmentEvent` is exercised directly against
  in-memory repos in `consumer_test.go`, with a real broker reserved for
  the separate, build-tagged `consumer_integration_test.go` (mirroring
  `wes-work-planning`'s own precedent).
- **Two storage backends, no domain change.** `memory` and `postgres`
  implement the same ports. `go run ./cmd/labor` with no `DATABASE_URL`
  starts a working service.
- **The wire contract and the domain model evolve independently.** DTOs
  live in the HTTP adapter; a `standardResponse` is not a
  `standard.LaborStandard`. The Kafka envelope's `taskCompletedData`
  struct lives in the inbound Kafka adapter, isolated from
  fulfillment-execution's own Go types entirely.

### Harder

- **More files and more indirection.** Adding a field end-to-end touches
  the aggregate, the port, both adapters, and the DTO/wire schema. For a
  CRUD service this would be over-engineering; here the mapping is the
  boundary that keeps fulfillment-execution's wire shape (and any future
  change to it) out of the domain model.
- **Cross-cutting timing invariants are an explicit use-case concern.**
  `RecordTaskPerformance` must mark the event processed *before* it looks
  up the standard and saves the row, so a crash mid-flight never
  double-processes on redelivery. Nothing in the compiler enforces that
  ordering; the tests do.
- **The rule is easy to violate under deadline pressure.** Nothing stops a
  use case importing `net/http` or `kafka-go` directly. The sibling
  services closed that gap with arch-go fitness tests; this context has
  adopted the same tooling (`internal/architecture/fitness_test.go`, run
  by the `arch-test` CI job), so the rule is now enforced by the build,
  not only by review.

---
id: 0007-analytical-data-product
slug: /adr/0007-analytical-data-product
title: 0007. Per-service analytical data product (the Labor Performance Report) via a separate analytics topic
sidebar_label: 0007. Analytical data product
description: ADR 0007 — an analytical read model (the "Labor Performance Report") built from this service's own domain events on a dedicated warehouse.labor-performance.analytics topic, projected into a separate analytical database and served by a read-only reports binary over REST — fleet parity with the six sibling contexts' data products, with no central data platform.
---

# 0007. Per-service analytical data product (the "Labor Performance Report")

## Status

Accepted.

> **Envelope superseded by [ADR-0021](./0021-cloudevents-mandatory-event-envelope.md).** The flat envelope (`event_id`/`event_type`/`occurred_at`/`schema_version`) described below is retired: every message is now a CloudEvents 1.0 event. The rest of this decision stands.

## Context

Every other bounded context in the warehouse-systems fleet ships a
per-service **analytical data product**: a `cmd/<name>-projector` writer, a
`cmd/<name>-reports` reader, an `internal/analytics/report` read model, a
separate analytical database under `migrations/analytics/`, and a
`GET /reports/...` + `GET /reports/.../freshness` REST surface.
`labor-performance` was the only context without one — its v1 CLAUDE.md
explicitly deferred the "per-service analytics data-mesh" as out of scope
for the first pass, to be added "in a fast-follow once the core domain is
proven". The core domain is now proven (six merged PRs, ADRs 0001–0006),
and the fleet console's upcoming **WES Dashboard** needs a
labor-performance panel alongside the other six contexts.

The forces are the same ones order-management's ADR-0006 records, plus one
that is specific to this context:

- **Follow data-mesh principles without standing up a data platform.** No
  central warehouse, no lake, no shared ETL team. Reuse what the estate
  already runs: Kafka, Postgres, chi, the Helm chart.
- **Analytics must never contend with OLTP.** Report query load, a long
  aggregation, or a projection rebuild must not touch the transactional
  database that serves standard definition and TaskCompleted ingestion.
- **The service owns its data as a product.** The read side lives in this
  repo, owned by the same team, with a contract and a published freshness
  signal — not shipped off to a central team.
- **The OLTP write path must be untouched.** This service's whole job is
  consuming `TaskCompleted` and scoring it. That path is the thing most
  worth not breaking, and it had no reason to change to serve a report.
- **Specific to this context: this service already refuses to fabricate
  numbers.** ADR-0004 froze `StandardSecondsAtCompletion` at completion
  time rather than recomputing it; ADR-0006 added `MeanActualSeconds`
  precisely because an efficiency ratio was not the same measurement as an
  observed pace; `EfficiencyPct` is nullable throughout because a task with
  no standard, or no duration, genuinely has no score. An analytical read
  model that averaged nulls into zeros — or that reported `0%` for an hour
  in which nothing was scorable — would quietly undo all three. Whatever
  shape the report takes, that discipline has to survive the projection.

## Decision

**Labor Performance owns an analytical data product built solely from its
own domain events, delivered on a dedicated analytics topic, projected into
a separate analytical database, and served read-only over REST. Three
processes; one writer.** This mirrors order-management's ADR-0006 shape
deliberately and closely — the point is fleet parity, not novelty.

### 1. Separate analytics topic

A new outbound adapter
(`internal/adapters/outbound/kafka/analytics_publisher.go`) publishes this
service's three past-tense domain events — `LaborStandardDefined`,
`LaborStandardRevised`, `TaskPerformanceRecorded` — to
**`warehouse.labor-performance.analytics`**, wrapped in the shared
Envelope v1 shape (`event_id`, `event_type`, `occurred_at`, `source`,
`schema_version`, `data`) with a snake_case per-`event_type` payload, keyed
by TaskType.

This is **strictly additive**. Before this change the service published its
domain events to a log publisher only (v1 shipped no Kafka publisher at
all, since no other repo consumed these events). That log publisher is
still the default and still runs; setting `EVENT_PUBLISHER=kafka` wraps it
in a `FanOutPublisher` that additionally emits to the analytics topic. The
inbound integration contract (`warehouse.fulfillment.events`, consumed from
`fulfillment-execution`) is untouched, and no existing consumer anywhere in
the fleet is affected.

Unlike order-management's equivalent publisher, this one performs **no
repository enrichment**: every field the report is keyed or aggregated by
already travels on the domain events themselves, so the publisher is a pure
serializer with no read-back into the OLTP store.

### 2. Separate analytical database

Projections land in a **separate analytical database** with its own
credentials (`ANALYTICS_DATABASE_URL`), its own golang-migrate migration
set (`migrations/analytics/`), and a **read-only role** for the reader. The
OLTP `DATABASE_URL` database is never opened by the analytical side.

### 3. Three processes, one writer

- **`cmd/labor`** — the OLTP binary. Unchanged except that its composition
  root can fan domain events to the analytics topic
  (`EVENT_PUBLISHER=kafka`). The domain and application layers are not
  modified at all: they still see one `ports.EventPublisher`.
- **`cmd/labor-projector`** — the analytics **writer**. Consumes
  `warehouse.labor-performance.analytics` under consumer group
  `labor-performance-analytics`, reading from `FirstOffset`, applies
  idempotent projections, and is the **only** writer of the analytical
  database. Runs the analytical migrations on start. Serves a health
  endpoint only.
- **`cmd/labor-reports`** — the **read-only reader**. Opens the analytical
  database over a pool pinned to `default_transaction_read_only=on` (on top
  of the read-only role the URL should use), serves the report, and never
  writes or migrates.

The analytics consumer declares **its own** `ProcessedEvents` port rather
than reusing `internal/application/ports`, so the OLTP application layer
gains no dependency on the analytics side. The consumer gate
(`analytics_consumed_events`) and the projection's own event-id claim
(`analytics_processed_events`) are separate tables so the two idempotency
layers never race to claim the same key.

### 4. The report

A **Labor Performance Report**, keyed per **TaskType × UTC hour bucket**:

| Counter | Meaning |
| --- | --- |
| `tasksRecorded` | every `TaskPerformanceRecorded`, including unscorable and unmeasurable ones |
| `tasksScored` | the subset with a non-nil `EfficiencyPct` — the denominator of mean efficiency |
| `tasksMeasured` | the subset with `ActualSeconds > 0` — the denominator of mean actual seconds |
| `standardsDefined` / `standardsRevised` | standard-lifecycle events in the bucket |

Served with a per-TaskType breakdown (one bar per TaskType — the shape the
WES Dashboard's chart binds to) and window totals. `tasksMeasured` is
served on every layer of the response (rows, per-TaskType bars, window
totals) alongside `tasksScored` — a bucket can be measured but not scored,
and hiding that denominator would make `meanActualSeconds` unauditable
from the wire alone.

Two design choices follow directly from this context's existing
discipline:

**Raw counters, never stored means.** The fact table stores counts and
sums only; every mean is derived at read time by one function
(`report.mean`) that returns `nil` for a zero denominator. That makes
"an hour with tasks but nothing scorable has NO efficiency number, not
0.0" a property of a single place, rather than something every writer and
every reader has to remember. It is ADR-0004's never-fabricate-a-number
rule restated on the analytical side, and it holds all the way to the
wire: the JSON carries `null`, not `0`. `tasksScored` and `tasksMeasured`
are tracked as **separate** denominators for the same reason ADR-0006
separated `MeanEfficiencyPct` from `MeanActualSeconds` — a task can be
measured without being scored (a real duration with no engineered
standard to compare it against), and collapsing the two would silently
mis-weight both.

**Bucketed by business time, not ingestion time.** A `TaskPerformance`
row buckets on its `CompletedAt`, a standard on its `EffectiveFrom` —
never on the envelope's `occurred_at`, which is only used as the freshness
watermark. A replayed or late-ingested event therefore lands in the hour
the work actually happened. This is the analytical twin of ADR-0004's
"resolve the standard active *as of* CompletedAt, not as of now".

One consequence is worth stating plainly rather than discovering later: as
of `fulfillment-execution`'s ADR-0023, `task_type` IS on the
`TaskCompleted` wire (this service's own long-standing gap, tracked in
ADR-0003/0014 and since closed — the consumer reads it at
`consumer.go`'s `ParseTaskTypeLenient` call). Completions from publishers
that predate that change — and the lookup-miss degrade case for task
types this service does not model (e.g. REBIN) — still arrive with an
empty task type; the report labels that bucket `UNCLASSIFIED`, an
explicit, chartable category and a non-empty primary-key column, rather
than dropping the rows, inventing a task type for them, or keying a fact
table on an empty string. The per-TaskType breakdown therefore shows real
bars today, with UNCLASSIFIED as the residual rather than the whole
picture.

### 5. Served over REST

`cmd/labor-reports` serves:

- `GET /reports/performance?from=&to=&taskType=&granularity=`
- `GET /reports/performance/freshness`
- `GET /healthz`

RFC 7807 `application/problem+json` for every error, using the same
`problemDetails` struct and `problemBaseURI` namespace as the OLTP adapter.
CORS is applied from the same `CORS_ALLOWED_ORIGINS` convention, but
**GET-only** — the reports server has no write surface and must not
advertise one.

The read model lives in a new `internal/analytics/report` region that
depends on nothing; the consumer and store adapters depend on it. Two new
arch-go fitness rules in `internal/architecture/architecture_test.go`
enforce that isolation in both directions.

## Consequences

### Easier

- **Fleet parity.** The WES Dashboard can render a labor panel using the
  same three-process shape, the same `/reports/.../freshness` convention,
  and the same envelope as the other six contexts.
- **The OLTP write path is untouched** — no dual-write, no new failure mode
  on the path that ingests `TaskCompleted`.
- **Analytics cannot contend with OLTP** — separate database, separate
  connection, read-only reader pool on top of a read-only role.
- **The read model is rebuildable from scratch** by replaying the topic
  from its earliest offset, because the projection is idempotent per
  `event_id` and every apply is a commutative `+=`.
- **The nullability discipline is now enforced in one place** and covered
  by tests that assert `null` rather than `0` at the read model, the store,
  and the HTTP wire.

### Harder

- **Three processes to deploy, not one.** The projector and reports
  binaries need their own manifests, their own `ANALYTICS_DATABASE_URL`,
  and (for the reader) a genuinely read-only role to be worth the defence
  in depth.
- **The report is eventually consistent.** It is a projection, not a live
  query; `GET /reports/performance/freshness` exists precisely so a
  consumer can tell how far behind it is. Target: p95 event-to-report lag
  under 30s, matching the sibling contexts.
- **Two idempotency layers to reason about** (consumer gate and projection
  claim). They are deliberately separate tables; conflating them would
  cause the gate to starve the projection, which is why the in-memory store
  keeps two sets too, and why a test asserts exactly that.
- **`EVENT_PUBLISHER=kafka` must actually be set** for the pipeline to
  carry anything. The default stays `log`, so a deployment that forgets it
  gets an empty report rather than an error. The freshness endpoint is the
  intended way to notice.

### Deferred

- **The `labor-mfe` remote's console screen content.** CORS is in place on
  both servers and the remote itself has since shipped (`web/` + the
  chart's frontend component, disabled by default) — further screen work
  is ordinary feature work, not an open ADR item.

> **Deferred items since shipped:** the Helm chart wiring for the
> projector and reports binaries (this chart's `analytics:` block), the
> `labor-mfe` remote (`web/` + `frontend:` chart component), and the MCP
> adapter with a read tool over the same use cases (ADR-0009) are all
> merged on `develop`. The one genuinely outstanding item is below.

- **~~A Postgres integration test for the analytics store.~~ Shipped:**
  `internal/adapters/outbound/analyticsstore/postgres_integration_test.go`
  drives the real projection and reader against a throwaway testcontainers
  Postgres under `migrations/analytics` — the projection's idempotent
  claim, the commutative `+=` upsert, UNCLASSIFIED normalisation, window
  and task-type filters, and the freshness watermark are all proven
  against real SQL, closing this ADR's original fast-follow.

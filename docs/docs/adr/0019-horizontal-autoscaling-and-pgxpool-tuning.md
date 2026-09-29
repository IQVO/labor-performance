---
id: 0019-horizontal-autoscaling-and-pgxpool-tuning
slug: /adr/0019-horizontal-autoscaling-and-pgxpool-tuning
title: 19. Per-workload HorizontalPodAutoscaler and pgxpool MaxConns/statement_timeout tuning
sidebar_label: 19. HPA + pgxpool tuning
description: "ADR 0019 -- Phase 3 (scalability) for labor-performance: an autoscaling/v2 HorizontalPodAutoscaler per independently-assessed workload (api max 4, analytics-projector max 2, analytics-reports max 3; mcp explicitly excluded for the same in-memory-session-state reason order-management excluded its own cmd/mcp; frontend has no chart-rendered Deployment in this service, so it is out of scope), all default-disabled via values.yaml so this PR changes nothing on merge; plus explicit pgxpool.Config MaxConns caps and per-pool statement_timeout values, sized against the shared Postgres instance's real max_connections=100 ceiling and PgBouncer's transaction-pooling front end (warehouse-infra PR #43)."
---

# 19. Per-workload HorizontalPodAutoscaler and pgxpool MaxConns/statement_timeout tuning

## Status

Accepted — implemented in the same change that introduces this record.
This is labor-performance's instance of the fleet's Phase 3 (scalability)
production-readiness wave, directly porting
[order-management PR #110](https://github.com/claudioed/order-management/pull/110)
(ADR-0026), the reference the other ~9 fleet repos in this phase copy —
the same role order-management's ADR-0025 played for Phase 2's resilience
wave (labor-performance's own ADR-0017 ported that one).

## Context

Before this change, `labor-performance`'s Helm chart had exactly one
`HorizontalPodAutoscaler` template, unconditionally targeting the `api`
Deployment only, wired to a flat `autoscaling.enabled/minReplicas/
maxReplicas/targetCPUUtilizationPercentage` block — untested against the
other three Deployments this chart renders (`analytics-projector`,
`analytics-reports`, `mcp`), and never assessed for whether scaling
`analytics-projector` past 1 replica was even safe given its Kafka
consumer-group membership. `replicaCount` was hardcoded fleet-wide with
no per-workload HPA anywhere in this chart.

Separately, no pool in this codebase set an explicit `pgxpool.Config.
MaxConns`, so every pool — the OLTP pool (`internal/adapters/outbound/
postgres/pool.go`, used by `cmd/labor` and `cmd/mcp`) and the two
analytics pools (`internal/adapters/outbound/analyticsstore/pool.go`'s
`NewPool` used by `cmd/labor-projector`, and `NewReadOnlyPool` used by
`cmd/labor-reports`) — ran on pgx's library default, `max(4,
runtime.NumCPU())` connections per process. No pool set a
`statement_timeout` either, so a single runaway query could hold a
pooled connection indefinitely, with nothing to cancel it.

This matters together, not separately, for the exact reason
order-management's ADR-0026 gives: turning on HPA for a Postgres-backed
workload without an explicit, bounded `MaxConns` means the service's
real connection ceiling becomes an unbounded, indirect function of
cluster autoscaling and CPU load, not a number anyone chose.

### Fleet facts reused, not re-derived

Per the Phase 3 fan-out plan (already verified and not re-derived here):

- All 10 fleet backend services share ONE Postgres server instance, each
  with its own logical database/role, `max_connections=100` — the
  unmodified Bitnami chart default, never deliberately sized for the
  fleet's real demand.
- PgBouncer (`warehouse-infra` PR #43, merged) sits in front of that
  Postgres instance in transaction-pooling mode. Every service's OLTP
  `DATABASE_URL` Secret — including this service's — is already
  re-pointed at PgBouncer; no code change was needed for that. Analytics
  DSNs stay DIRECT per PgBouncer PR #43's own reasoning (the projector's
  batched projection writes and the reader's wide time-range aggregations
  do not fit PgBouncer's transaction-pooling mode as cleanly as the
  OLTP path's short, single-aggregate transactions do) — this PR mirrors
  that OLTP/analytics split exactly, it does not re-decide it.
- `pgxpool.MaxConns` can stay generous per service (this PR matches
  order-management's numbers: 10 OLTP, 5 projector, 5 reports) because
  PgBouncer absorbs the real server-side connection multiplexing across
  every fleet service's replicas — the per-process `MaxConns` bounds this
  process's own logical pool, not a direct 1:1 claim on a Postgres
  backend connection.

### Repo-specific fact verified before designing HPA per workload

`cmd/` in this repo currently has exactly four binaries — verified live,
not assumed unchanged from CLAUDE.md's prose:

```
$ ls cmd/
labor  labor-projector  labor-reports  mcp
```

There is no `cmd/labor-frontend` binary and no `frontend-deployment.yaml`
Deployment producing a distinct workload beyond what
`charts/labor-performance/templates/frontend-deployment.yaml` already
renders when `frontend.enabled=true` (the `web/` nginx-unprivileged SPA).
That frontend Deployment DOES get an HPA in this PR (see below) — the
`cmd/` binary count is what determines the Go-side workload assessment
(api/projector/reports/mcp), not the frontend, which is a separate
static-asset Deployment with its own already-established chart shape.

labor-performance also confirms the same stricter-isolation fact
process-path-management already established for the fleet: **this
service makes no synchronous outbound HTTP call to any sibling
context** (`CLAUDE.md`'s non-negotiable #1 — pure Kafka consumer of
`fulfillment-execution`, choreography not orchestration). This has no
direct bearing on the HPA/pgxpool decisions below (none of them route
through a sibling), but it is confirmed here because it rules out an
entire class of Phase 2-style circuit-breaker work this PR does NOT also
need to add (ADR-0017 already recorded the same finding for the DLQ/
graceful-shutdown port).

## Decision

### 1. HorizontalPodAutoscaler — one per independently-assessed workload

Assessed each of the four Deployments this chart renders on its own
merits — statefulness, and (for Kafka consumers) consumer-group-id
convention — rather than blanket-enabling HPA everywhere:

| Deployment | HPA? | min | max | target CPU | Why |
|---|---|---|---|---|---|
| `api` (`cmd/labor`) | Yes | 1 | 4 | 70% | Stateless OLTP HTTP + Kafka consumer of `warehouse.fulfillment.events`. Its consumer group (`config.kafkaConsumerGroup`, chart default `"labor-performance"`) is a **stable, shared** group with no per-instance uniqueness — the fleet's normal horizontally-scalable pattern, N replicas share partitions via ordinary Kafka group rebalancing (`internal/adapters/inbound/kafka/consumer.go`). Its outbox relay (`internal/adapters/outbound/postgres/outbox_relay.go`) drains `outbox_events` with `FOR UPDATE SKIP LOCKED`, explicitly designed so two relays (a rolling deploy's overlapping old/new pod, or N HPA replicas) never claim the same row — verified directly in that file's own doc comment. Nothing here breaks at N>1. |
| `analytics-projector` (`cmd/labor-projector`) | Yes, capped at **2**, not api's 4 | 1 | 2 | 70% | The ONLY writer of the analytical database (ADR-0007). Its Kafka consumer group (`kafka.AnalyticsConsumerGroup == "labor-performance-analytics"`, `internal/adapters/inbound/kafka/analytics_consumer.go`) is also a **stable, shared** group with no per-instance uniqueness — the same safe shape as `api`'s consumer group. Every write is idempotent on `event_id` via `ProcessedEvents.MarkProcessed` before being applied, so correctness does not regress at N>1. Capped at 2, not left at api's 4, for the same reasons order-management's projector was: the analytics topic is keyed by `TaskType` (`internal/adapters/outbound/kafka/analytics_publisher.go`'s `marshalData`), so ordering is only guaranteed per-partition, and a wider fan-out buys little extra throughput for what is a lightweight idempotent-upsert workload. |
| `analytics-reports` (`cmd/labor-reports`) | Yes | 1 | 3 | 70% | Stateless read-only REST reader over its own read-only pgxpool (`analyticsstore.NewReadOnlyPool`) — no in-memory state, no Kafka consumption. Same treatment as `api`. |
| `frontend` (nginx-unprivileged serving the built `labor_mfe` SPA bundle) | Yes | 1 | 3 | 70% | Pure static-asset serving. No server-side session, no per-request state. The most trivially horizontally-scalable workload in this chart. |
| `mcp` (`cmd/mcp`) | **No — deliberately excluded, not just disabled** | — | — | — | `internal/adapters/inbound/mcp/server.go` wraps the `github.com/modelcontextprotocol/go-sdk` `StreamableHTTPHandler` (`mcp.NewStreamableHTTPHandler`), which keeps **per-process, in-memory session state** keyed by the MCP protocol's own `Mcp-Session-Id` header — a real multi-request session, not just a TCP/HTTP connection. `charts/labor-performance/templates/mcp-service.yaml` is a plain `ClusterIP` Service with no `sessionAffinity` configured, so under >1 replica a second request carrying the same `Mcp-Session-Id` (e.g. a `tools/call` following an earlier `initialize`) could land on a different pod than the one that created the session, which has never heard of it and would reject or silently start a new one. Fixing it for real needs either `sessionAffinity: ClientIP` (a partial mitigation only) or wiring the SDK's `StreamableHTTPOptions.EventStore` to a shared/external session store — a real code change, out of scope for this chart-and-pool-tuning PR. `mcp.replicaCount` stays a plain, manually-set value; no `autoscaling.mcp` block exists in `values.yaml` at all. Ported verbatim from order-management's ADR-0026 reasoning, which excluded its own `cmd/mcp` for the identical reason. Revisit if/when `cmd/mcp` adopts an external session store or the SDK's stateless mode. |

Every enabled block is namespaced under a single top-level `autoscaling:`
key in `values.yaml` (`autoscaling.<api|projector|reports|frontend>.
{enabled,minReplicas,maxReplicas,targetCPUUtilizationPercentage}`),
**every `enabled` value defaults to `false`**. This PR makes per-workload
HPA possible and verified-correct; it deliberately does not turn any of
it on — the fleet enables each workload's HPA later, once, as a
conscious rollout decision, the same "ship the mechanism, not the
behavior change" shape Phase 1's outbox/idempotency work used.

**No replicas-vs-HPA fight.** Each guarded Deployment template's
`spec.replicas` field is now wrapped in `{{- if not .Values.autoscaling.
<x>.enabled }}` — when a workload's HPA is enabled, its Deployment
renders with NO `replicas` field at all (a hardcoded `replicas:` next to
an active HPA would otherwise fight it on every reconcile, most visibly
right after a `helm upgrade` resets it back to the chart's static value).
`mcp`'s Deployment keeps its unconditional `replicas:` field, unchanged
— it has no HPA to fight.

Verified directly with `helm template`:

- Default values → 0 `HorizontalPodAutoscaler` resources render, every
  Deployment keeps its static `replicas:` field.
- All four `autoscaling.*.enabled=true` (with `analytics.enabled=true`,
  `frontend.enabled=true`, `mcp.enabled=true`) → exactly 4
  `HorizontalPodAutoscaler` resources render (one per scalable workload,
  `mcp` has none by design), and none of those four Deployments has a
  `replicas:` field — `mcp`'s Deployment still does.

`helm lint` passes. `go build`/`go vet`/`gofmt` all pass unchanged (no
Go code path is affected by the chart changes).

### 2. pgxpool MaxConns

All three pools now set an explicit `pgxpool.Config.MaxConns` instead of
inheriting the CPU-derived library default (`max(4, runtime.NumCPU())`),
matching order-management's numbers exactly (see the Context section for
why PgBouncer being in front of the OLTP path is what makes staying this
generous safe):

| Pool | Used by | `MaxConns` | Reasoning |
|---|---|---|---|
| OLTP (`postgres.NewPool`) | `cmd/labor` (`api`), `cmd/mcp` (`mcp`) | **10** | `api`'s HPA ceiling of 4 replicas × 10 = 40 connections, ~40% of the shared instance's `max_connections=100` for this ONE of up to 10 fleet services' OLTP path alone — deliberately leaving the remaining ~60% for the other 9 services (and this service's own mcp/projector/reports processes). This pool's DSN is served through PgBouncer's transaction pool, so the real server-side connection multiplexing is PgBouncer's job, not this per-process ceiling's. |
| Analytics writer (`analyticsstore.NewPool`) | `cmd/labor-projector` | **5** | The projector has no HPA (fixed `replicaCount`) and does single-row `ON CONFLICT` upserts against one projection row at a time; a small, flat pool is enough. Connects DIRECT to Postgres, not through PgBouncer. |
| Analytics reader (`analyticsstore.NewReadOnlyPool`) | `cmd/labor-reports` | **5** (`ReportsMaxConns`) | `reports` IS HPA-scalable (max 3); at that ceiling, 3 × 5 = 15 connections against the analytical database — comfortably inside the shared ceiling alongside the OLTP path's 40. Connects DIRECT, not through PgBouncer. |

Worst case across every workload simultaneously at its proposed HPA
maximum (`api` 4 × 10 = 40, `projector` fixed at 1 × 5 = 5, `reports` 3 ×
5 = 15; `mcp` has no HPA, assume 2 manually-set replicas × 10 = 20): 40 +
5 + 15 + 20 = **80** of the shared instance's 100 connections for this
ONE service alone — even at every proposed HPA ceiling simultaneously,
with **HPA still disabled by default today** (at `replicaCount: 1`
everywhere and no HPA enabled, this service's actual usage is 10 (api) +
10 (mcp, if deployed) + 5 (projector) + 5 (reports) = at most 30
connections, 30% of the ceiling). That 80-at-max-scale number is
presented to be honest about the ceiling, not to claim it's comfortable
— it depends on what `MaxConns` values the other 9 services' own Phase 3
PRs choose (order-management's own PR #110 already committed to the
identical 80/100-for-one-service accounting, so the two services'
worst-case numbers are not additive beyond what each PR already disclosed
in isolation — a genuine fleet-wide follow-up once more of these land, not
solved here).

### 3. statement_timeout

All three pools set `statement_timeout` via `pgxpool.Config.AfterConnect`,
running `SET statement_timeout = '<value>'` on every new physical
connection as it's established (not per-query, so it survives connection
reuse across pooled acquisitions). Values match order-management's
exactly, for the same query-shape reasoning:

| Pool | `statement_timeout` | Reasoning |
|---|---|---|
| OLTP (`postgres.StatementTimeout`) | **5s** | Every OLTP query (`DefineStandard`, `GetStandard`, `RecordTaskPerformance`, the scorecard/utilization reads) is a single-aggregate or small bounded-fan-out read/write keyed by id, normally low-single-digit milliseconds. 5s is roughly 1000x that — generous headroom for real transient contention without ever being a normal-path concern, while bounding the absolute worst case tightly since this is the pool with the most connections (40 at max HPA scale) to protect. |
| Analytics writer (`analyticsstore.StatementTimeout`) | **10s** | A Kafka consumer replaying a backlog after a redeploy issues upserts in a tight loop; a transient lock wait here doesn't need to be as tight as an interactive OLTP request. Still bounded — the projector has no replica to fail over to (fixed at 1), so an unbounded query here would stall the entire analytics pipeline. |
| Analytics reader (`analyticsstore.ReportsStatementTimeout`) | **15s** | The performance report aggregates rows across a caller-chosen time range (`postgres_report.go`) — wider than the OLTP side's always-single-aggregate-by-id shape — so it gets more headroom, but still a hard ceiling: a caller-supplied wide date range must not be able to hold a reports connection forever. |

Verified with a real Postgres via testcontainers
(`internal/adapters/outbound/postgres/pool_limits_integration_test.go`,
`-tags=integration`), not a mock and not just reading `pg_settings` —
ported line-for-line from order-management's reference test:

- `TestNewPool_AppliesStatementTimeoutToNewConnections` — opens a pool
  against a real `postgres:16-alpine` container with a short test-only
  timeout (200ms, via the shared `NewPoolWithLimits` the production
  `NewPool` wraps), confirms `SHOW statement_timeout` reads back `200ms`
  on a freshly acquired connection, then runs `SELECT pg_sleep(2)` and
  asserts Postgres itself cancels it (SQLSTATE 57014, "canceling
  statement due to statement timeout") rather than letting it run the
  full 2s, and finally confirms the pool is still usable afterward.
- `TestNewPool_AppliesMaxConns` — acquires exactly `maxConns` connections
  from a pool configured with `MaxConns=2`, then asserts a further
  `Acquire` blocks until `context.DeadlineExceeded`, proving `MaxConns`
  is the pool's real, enforced ceiling rather than advisory.

Both tests pass locally against a real Postgres container
(`go test -tags=integration ./internal/adapters/outbound/postgres/...
-run TestNewPool_ -v`).

## Consequences

- HPA is now possible, correct, and independently verified per workload
  for four of this chart's four scalable Deployments — but **off by
  default everywhere**. Merging this PR changes nothing about production
  replica counts; `MaxConns`/`statement_timeout` are the only behavior
  change that takes effect on deploy, and both are conservative relative
  to today's unbounded defaults (they can only reduce, never increase,
  worst-case connection usage and hung-query duration).
- `mcp` remains explicitly un-autoscaled, with the exact reason
  (in-memory session state, no sticky routing) recorded here and in
  `values.yaml`'s comments, so a future contributor doesn't mechanically
  copy `api`'s HPA block onto it without re-solving the session-affinity
  problem first.
- The worst-case 80-of-100-connections number for this service alone, at
  every proposed HPA ceiling simultaneously, leaves only 20 connections
  (20%) for the other 9 fleet services if they are all maxed at the same
  moment. That is an honest, documented residual risk, not a solved one
  — it depends on what `MaxConns` numbers the other services' own Phase 3
  PRs choose, and is worth a fleet-wide follow-up (outside this PR's
  scope) once more of those PRs land.
- `max_connections=100` itself is an unexamined Bitnami chart default,
  not a value anyone has deliberately sized for this fleet's real
  demand. This ADR treats it as a hard external constraint to work
  within, exactly as order-management's ADR-0026 did, not something in
  scope to change.
- PgBouncer's transaction-pooling front end (`warehouse-infra` PR #43) is
  what makes the OLTP `MaxConns=10` figure safe to keep generous even as
  HPA scales `api` up to 4 replicas — this ADR relies on that existing
  infrastructure decision rather than re-deriving it; the analytics
  pools stay direct per that same PR's reasoning, mirrored here without
  re-litigating it.

# ADR index (0001–0018)

Full records live in `docs/docs/adr/` (Nygard format, Docusaurus-rendered
at `/docs/adr`). This is a summary index — read the actual ADR before
relying on a detail not captured here.

| # | Title | Status | What it settles |
| --- | --- | --- | --- |
| 0001 | Hexagonal (ports & adapters) architecture | Accepted | The layering rule enforced by arch-go. |
| 0002 | A new bounded context, not an extension of workforce-management or fulfillment-execution | Accepted | Why this is its own repo (competitor research + both siblings' boundary discipline). |
| 0003 | Kafka choreography consumer of fulfillment-execution, no REST dependency | Accepted | The bounded-context boundary: pure consumer, no write access, no sync calls. |
| 0004 | StandardSecondsAtCompletion is frozen at ingestion time, never recomputed | Accepted | The "resolve as of CompletedAt, not as of now" invariant. |
| 0005 | Associate Trend and CoachingFlag on the Scorecard read model | Accepted | The passive visibility signal computed from the last (up to 10) scored tasks. |
| 0006 | MeanActualSeconds on TaskTypePerformance, independent of any standard | Accepted | Why a real measured rate is tracked separately from EfficiencyPct. |
| 0007 | Analytical data product | Accepted | The `cmd/labor-projector` + `cmd/labor-reports` split, the separate analytics topic/DB, the Labor Performance Report shape. See `.claude/rules/docs-and-api-drift.md` for the resulting two-OpenAPI-spec setup. |
| 0008 | Standard metrics convention across the fleet | Accepted | Fleet-wide naming/shape conventions this service's metrics follow. |
| 0009 | Model Context Protocol as an inbound adapter, not a new service | Accepted | MCP server lives in this repo (`internal/adapters/inbound/mcp/`, `cmd/mcp/`), not a separate service. |
| 0010 | Transactional outbox for the analytics topic | Accepted | Domain event + `processed_events` marker + analytics event commit in one Postgres transaction; an in-process relay drains `outbox_events` onto `warehouse.labor-performance.analytics`. |
| 0011 | REST identity — fleet-standard static bearer keys with read/read-write scopes | **Superseded by 0012** | Do not re-implement this pattern; it was deliberately removed. |
| 0012 | Remove the REST/MCP identity layer | Accepted | Current state: no auth layer on REST or MCP. Read this before assuming any endpoint requires a bearer token. |
| 0013 | Labor performance publishes an integration event | Accepted | `TaskPerformanceRecorded` also goes to the integration topic `warehouse.labor-performance.events` (via the same outbox), consumed by `workforce-management`. The analytics topic stays internal. |
| 0014 | Measuring idleness and utilization | Accepted | `IdlePeriod` aggregate derived from consecutive `TaskCompleted` events (`IDLE_GAP_CAP_SECONDS`), additive `idle_seconds_before` on `TaskPerformanceRecorded`, the two `/utilization` endpoints and the `get_task_type_utilization` MCP tool. |
| 0015 | Optional travel-time component on a LaborStandard | Accepted | Caller-supplied `TravelComponentSeconds` (`0 <= t <= ExpectedSeconds`); this service never calls facility-layout or any sibling to compute or validate it. |
| 0016 | Transactional Idempotency-Key middleware for POST /standards | Accepted | Route-scoped, transactional Idempotency-Key HTTP middleware for `POST /standards`, this service's one true resource-creation endpoint; reuses the outbox's tx-in-context mechanism (`internal/pgtx`). Ported from order-management's reference (PR #105, ADR 0023). |
| 0017 | Kafka consumer dead-letter queue and graceful shutdown hardening | Accepted | `Consumer.handleMessage` retries `handleFulfillmentEvent` in-process (cenkalti/backoff/v4, up to 3 attempts) then dead-letters an exhausted/poisoned message to `warehouse.fulfillment.events.dlq`, committing the offset so one poison message never blocks the partition. Graceful shutdown gains a readiness-flip-first sequence backing a new `GET /readyz` distinct from `GET /healthz`. Ported from order-management's DLQ/shutdown design (PR #107, ADR 0025) — this service has no sibling-context outbound calls, so no circuit breaker work applies here. |
| 0018 | Key-aware Hash balancer on every outbound Kafka writer | Accepted | Every writer in `internal/adapters/outbound/kafka` (`IntegrationPublisher`, `AnalyticsPublisher`, `RelaySink`) used `&kafkago.LeastBytes{}`, which ignores `Message.Key` for partition routing entirely — `Message.Key` (AssociateId / TaskType) was always set correctly but had no effect on partition placement. Switched every writer's `Balancer` to `&kafkago.Hash{}`. Same fleet-wide fix as order-management PR #111, closing the ordering gap warehouse-infra PR #42's 1→8 partition scaleup exposed. Proven via a real-broker Testcontainers test on an 8-partition topic. |

## Reading order for a newcomer

1. 0001 (architecture) → 0002 (why this repo exists) → 0003 (the boundary).
2. 0004 (the core scoring invariant) → 0005/0006 (what got added on top of
   the v1 scoring model).
3. 0007 (analytics data product) → 0010 (how writes get to the analytics
   topic reliably) — read together, 0010 depends on 0007's shape existing.
4. 0009 (MCP) if working on the MCP inbound adapter.
5. 0011 then 0012 together — 0011 is superseded, but reading it first
   makes 0012's reasoning ("why we undid this") legible.
6. 0013 → 0014 (the integration topic, then the idleness field it carries)
   and 0015 (how the no-outbound-call boundary held when facility-layout
   distance data looked tempting).
7. 0016 (idempotency-key middleware) → 0017 (Kafka DLQ and graceful
   shutdown) — both are fleet-wide production-readiness ports from
   order-management's reference implementations, read independently of
   the rest.
8. 0018 (Kafka writer Hash balancer) — a fleet-wide producer-routing
   correctness fix, read whenever touching any `kafkago.Writer`
   construction in `internal/adapters/outbound/kafka`.

## Proposing a new ADR

Copy the Nygard template from `docs/docs/adr/about.md`, next free number,
`Status: Proposed`, add to both the site's `docs/docs/adr/about.md` table
and `docs/sidebars.ts`, PR it, flip to `Accepted` on merge. An accepted
ADR is never edited to change its decision — a reversal gets a new ADR
that supersedes it (see 0011 → 0012 for the pattern).

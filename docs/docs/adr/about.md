---
slug: /adr
title: Architecture Decision Records
sidebar_label: About ADRs
description: Why this service keeps ADRs, the template it uses, and how to propose a new one.
---

# Architecture Decision Records

## What these are

An **Architecture Decision Record (ADR)** captures a single architecturally
significant decision, its context, and its consequences — at the moment it was
made, in the words that were true then.

The value is not the decision; it is the **context**. Six months later, the
code shows *what* was chosen and can never show *why*, or what the alternatives
were, or what was known at the time. A reader who does not know the why has two
bad options: assume the decision was arbitrary and change it, or assume it was
sacred and work around it. An ADR gives them a third.

## The format

These records use **Michael Nygard's template** — the de facto standard:

```markdown
# NNNN. Title (a short noun phrase)

## Status
Accepted | Proposed | Deprecated | Superseded by ADR-XXXX

## Context
The forces at play: technical, business, operational. What is true that makes
this a decision rather than an obvious default? Written in full sentences and
in the present tense, describing the situation as it is.

## Decision
The response to those forces, stated actively: "We will…"

## Consequences
What becomes easier and what becomes harder — both. A record with only
positive consequences has not been thought about.
```

One decision per file, numbered `0001-`, `0002-`, … Numbers are never reused
and files are never deleted.

## Immutability

**An accepted ADR is never edited to change its decision.** If a decision is
reversed, write a new ADR that supersedes it, and update the old one's
*Status* line to point at the successor. The historical record of what was
believed at the time is the entire asset.

Typos, broken links and formatting are of course fair game.

## The records

| # | Title | Status |
| --- | --- | --- |
| [0001](./0001-hexagonal-ports-and-adapters.md) | Hexagonal (ports & adapters) architecture | Accepted |
| [0002](./0002-new-bounded-context-not-extension-of-workforce-or-fulfillment.md) | A new bounded context, not an extension of workforce-management or fulfillment-execution | Accepted |
| [0003](./0003-kafka-choreography-consumer-of-fulfillment-execution.md) | Kafka choreography consumer of fulfillment-execution, no REST dependency | Accepted; envelope superseded by 0021 |
| [0004](./0004-standard-frozen-at-completion-time-not-recomputed.md) | StandardSecondsAtCompletion is frozen at ingestion time, never recomputed | Accepted |
| [0005](./0005-associate-trend-and-coaching-flag.md) | Associate Trend and CoachingFlag on the Scorecard read model | Accepted |
| [0006](./0006-mean-actual-seconds-independent-of-standard.md) | MeanActualSeconds on TaskTypePerformance, independent of any standard | Accepted |
| [0007](./0007-analytical-data-product.md) | Analytical data product | Accepted; envelope superseded by 0021 |
| [0008](./0008-standard-metrics-convention.md) | Standard metrics convention across the fleet | Accepted |
| [0009](./0009-mcp-inbound-adapter.md) | Model Context Protocol as an inbound adapter, not a new service | Accepted |
| [0010](./0010-transactional-outbox.md) | Transactional outbox for the analytics topic | Accepted; envelope superseded by 0021 |
| [0011](./0011-rest-auth-static-bearer-scopes.md) | REST identity — fleet-standard static bearer keys with read/read-write scopes | Superseded by 0012 |
| [0012](./0012-remove-rest-auth-layer.md) | Remove the REST/MCP identity layer | Accepted |
| [0013](./0013-labor-performance-integration-events.md) | Labor performance publishes an integration event | Accepted; envelope superseded by 0021 |
| [0014](./0014-labor-utilization-idleness.md) | Measuring idleness and utilization | Accepted |
| [0015](./0015-optional-travel-component-on-labor-standard.md) | Optional travel-time component on a LaborStandard | Accepted |
| [0016](./0016-idempotency-key-middleware.md) | Transactional Idempotency-Key middleware for POST /standards | Accepted |
| [0017](./0017-kafka-dlq-and-graceful-shutdown.md) | Kafka consumer dead-letter queue and graceful shutdown hardening | Accepted |
| [0018](./0018-kafka-writer-hash-balancer.md) | Key-aware Hash balancer on every outbound Kafka writer | Accepted |
| [0019](./0019-horizontal-autoscaling-and-pgxpool-tuning.md) | Per-workload HorizontalPodAutoscaler and pgxpool MaxConns/statement_timeout tuning | Accepted |
| [0020](./0020-migrations-direct-postgres-connection.md) | Run golang-migrate against a direct Postgres connection, not PgBouncer | Accepted |
| [0021](./0021-cloudevents-mandatory-event-envelope.md) | CloudEvents 1.0 as the mandatory event envelope | Accepted |
| [0022](./0022-optimistic-concurrency-one-open-standard.md) | Optimistic concurrency (version column) and one open standard per task type on labor_standards | Accepted |

Each of these reconstructs a decision that is actually visible in this
repository's history, `CLAUDE.md` or code — none is a generic placeholder.

## Proposing a new one

1. Copy the template above into `docs/docs/adr/NNNN-short-kebab-title.md`,
   taking the next free number.
2. Open it with **Status: Proposed**.
3. Write the *Context* before the *Decision*. If the context does not make the
   decision feel inevitable, the context is incomplete — or the decision is
   wrong.
4. Fill in *Consequences* honestly, including the ones you dislike.
5. Add it to the table above and to `sidebars.ts`.
6. Raise it as a pull request; the discussion belongs on the PR, not inside the
   record.
7. On merge, flip the status to **Accepted**.

## What deserves an ADR

Something is architecturally significant if reversing it later would be
expensive — it constrains the structure of the code, pins a contract other
teams depend on, or is costly to undo.

**Yes:** the layering rule; the bounded-context boundary decision; the
integration transport and envelope; a quality gate that blocks merges.

**No:** which linter rules are enabled; a library upgrade; a refactor with no
external consequence. Padding the log with trivia makes the significant records
harder to find.

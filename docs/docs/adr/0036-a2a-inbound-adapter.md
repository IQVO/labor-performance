---
id: 0036-a2a-inbound-adapter
slug: /adr/0036-a2a-inbound-adapter
title: "36. A2A inbound adapter (third driving adapter, read-only)"
sidebar_label: "36. A2A inbound adapter"
description: "ADR 0036 — labor-performance is a read-only Agent2Agent (A2A) agent through a third driving adapter over the same read use cases as REST and MCP. Two curated goal-shaped read skills, associate_scorecard and task_type_status, in a message-only agent served by cmd/a2a on port 8093. A2A carries delegation only and exposes no write: this context's MCP charter offers none, so the agent changes no state."
---

# 36. A2A inbound adapter (third driving adapter, read-only)

## Status

Accepted (2026-10-29). Mirrors the reference rollout in
[wes-work-planning ADR-0046](https://iqvo.github.io/wes-work-planning/docs/adr/0046-a2a-inbound-adapter)
(read-heavy) and workforce-management's A2A adapter; follows
[ADR 0009](./0009-mcp-inbound-adapter.md) (the MCP adapter this one is modelled
on), [ADR 0012](./0012-remove-rest-auth-layer.md) (no authenticated surface),
[ADR 0021](./0021-cloudevents-mandatory-event-envelope.md) (CloudEvents on every
Kafka message) and
[ADR 0034](./0034-postgres-and-kafka-are-the-only-runtime.md) (Postgres and
Kafka are the only runtime).

## Context

The fleet is adopting the Agent2Agent protocol so that a bounded context can
**delegate** a goal to another context's agent. Labor Performance already
exposes its read use cases over REST and MCP. Three transports now carry three
different things and must not overlap:

| Transport | Carries |
|---|---|
| Kafka + CloudEvents | **facts** between contexts (ADR 0021) |
| MCP | an agent's **tools** |
| A2A | **delegation** of a goal to another context's agent |

This context has two hard boundaries that shape the agent:

- It is a **pure read-side reporter** to the rest of the fleet. A
  `TaskPerformance` is recorded only by the Kafka consumer reacting to
  `TaskCompleted` (idempotent on the CloudEvents `id`); a `LaborStandard` is
  defined by an operator over this service's own REST. The MCP governance
  charter ([§7 rule 3](../mcp/governance-charter.md)) records that the MCP
  server "exposes read tools only"; [ADR 0009](./0009-mcp-inbound-adapter.md)
  registers no write tool.
- It **never calls any sibling context over REST or MCP** (`CLAUDE.md`
  non-negotiable 1). That rule is about outbound clients.

## Decision

Add `internal/adapters/inbound/a2a`, a third driving adapter over the **same**
read use cases (`GetAssociateScorecard`, `GetTaskTypePerformance`,
`GetStandard`, `GetUtilization`) as the HTTP and MCP adapters, built on the
official `a2aproject/a2a-go/v2` SDK (pinned v2.7.0) and served by a new
`cmd/a2a` binary (JSON-RPC 2.0 over HTTP, port **8093**, the Agent Card at
`/.well-known/agent-card.json`, the endpoint mounted at `/` and `/a2a`,
`GET /healthz`).

### The agent is read-only

The write gate is: a skill may change state only if the context's MCP surface
already offers that write. It offers none, so **no skill changes state** and
`Deps` carries no write use case (a test pins the field set, so adding one
fails loudly and forces an ADR). Consequently `cmd/a2a` has no Kafka producer,
no outbox publisher and no relay, and the chart's a2a Deployment carries no
`KAFKA_*` / `EVENT_PUBLISHER` env (a chart test asserts that). Nothing here
widens what the context exposes: both skills are compositions of reads the MCP
server already offers.

### Two curated skills, not one per endpoint

| Skill | Kind | Derived from | What it does |
|---|---|---|---|
| `associate_scorecard` | read | MCP `get_associate_scorecard` | task count, mean efficiency, per-task-type breakdown, trend and a coaching flag for one associate. |
| `task_type_status` | read | MCP `get_task_type_performance` + `get_labor_standard` + `get_task_type_utilization` | for one task type (PICK, PACK, SLAM): fleet-wide task count, mean efficiency and measured mean duration, the active engineered standard, and idle-vs-busy utilization over a trailing `windowSeconds` (default 3600). |

The same curation discipline as ADR 0009: goal-shaped, never one skill per
endpoint. The three task-type reads are composed into one skill because an
agent asking "how is PICK doing" needs them together (a pace without its
standard, or a utilization without its task count, is not interpretable).

Two choices in the composition:

- **A task type with no active standard is a valid answer.** `standard` is
  `null` (and so is `meanEfficiencyPct`, because nothing can be scored) instead
  of failing the whole read with the `404` the standalone standard lookup gives.
- **`windowSeconds` is validated, not silently defaulted.** Omitted or `0`
  applies the 1 h default (as MCP does); a negative or non-integer value is a
  `400` rather than a quiet fallback.

Nullable figures stay `null`, never `0` (`meanEfficiencyPct`,
`meanActualSeconds`, `utilizationPct`), the same "never fabricate a number"
rule as REST, MCP and the analytics report. The coaching flag is a visibility
signal for a human, never a trigger to act; the card says so.

### Message-only, answered in-band

Both skills are request/response reads, so the agent is message-only (no Task
lifecycle). Domain rejections and malformed requests are **answered** as RFC
7807-shaped `problem`s inside the message envelope, never raised as JSON-RPC
transport errors, with the **same classification as the REST adapter**
(`statusFor`) for the errors a read can raise: `404` unknown associate (no
performance recorded) or no active standard, `400` unknown task type or a bad
argument, `500` anything else (answered opaquely, detail only in the logs).
The `409` and `422` classes REST has belong to standard-definition writes, which
this surface does not offer.

### Strict input

Unknown argument names and trailing data are **rejected**, not ignored (a
misspelled `windowSecond` must not silently apply the default window). Go's
`encoding/json` matches names case-insensitively, so only genuinely unknown
names are refused. The skill request is a Data part, or a Text part holding the
same JSON.

This context has no deprecated alias on the surface the skills touch, so the
alias-leak concern some siblings had (`siteCode` vs `buildingId`) does not
arise.

### Facts still travel on Kafka, and nothing flows outward

A2A never becomes a data-sync channel; this agent publishes nothing at all.
`internal/adapters/inbound/a2a` imports only the application and domain layers
(arch-go enforced), and **no outbound A2A client is added** to this context —
the no-outbound-sibling-call rule stands. Delegating *to* this agent is an
inbound request from another context's agent; this context still never reaches
out.

### Unauthenticated, like REST and MCP

The card declares no security schemes: this surface is unauthenticated by the
same fleet-wide decision as REST and MCP ([ADR 0012](./0012-remove-rest-auth-layer.md)).
Schemes are added to the card, not a redesign, if that decision is ever
reversed.

## Consequences

- A fifth binary (`/app/a2a`) in the same image and an `a2a.*` chart block
  (`<release>-a2a`, default **off**), exposed through Kong at
  `/api/a2a/labor-performance` rewritten to `/`. `A2A_AGENT_URL` is mandatory
  (an absolute http(s) URL; the binary refuses to boot otherwise) and the chart
  defaults it to the in-cluster Service DNS, overridable through
  `a2a.agentUrl`.
- Like `cmd/mcp`, `cmd/a2a` runs migrations on start (idempotent) and needs
  only `DATABASE_URL` (plus the optional direct `MIGRATIONS_DATABASE_URL`).
- Port 8093 is the fleet's A2A port (8090 is MCP, 8091 the projector admin
  port, 8092 the reports API).
- Giving the agent a write later (there is no candidate today) needs a new ADR
  that first settles it on the MCP charter, then adds the skill through the
  same use case and transactional outbox the REST/MCP adapters use.

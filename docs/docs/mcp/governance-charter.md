---
id: governance-charter
title: MCP Governance Charter
sidebar_label: MCP Governance Charter
description: "The estate-wide rules every warehouse-systems MCP server follows — tool curation, naming, annotations, auth scopes, audit, and the review gate. Federated: global standards, domain-owned servers."
---

# MCP Governance Charter

This charter is the **federated computational governance** for MCP across
warehouse-systems: one set of global standards, enforced the same way in every
repository, while each bounded context owns its own server. It is the MCP
counterpart to the platform's existing 5-stage quality gate and its ADR
discipline. `fulfillment-execution` is the reference implementation
(see [its ADR-0008](https://github.com/claudioed/fulfillment-execution)); the
other five contexts — `inventory-storage`, `wes-work-planning`,
`workforce-management`, `facility-layout`, `labor-performance` — copy it. In
`labor-performance` the adopting decision is
[ADR-0009](../adr/0009-mcp-inbound-adapter.md).

Keywords **MUST**, **SHOULD**, **MAY** are used per RFC 2119.

## 1. Architecture rules (non-negotiable)

1. An MCP server **MUST** be an inbound adapter at
   `internal/adapters/inbound/mcp/`, depending inward on `application` only.
2. Tool handlers **MUST** call existing application-layer **use cases**. They
   **MUST NOT** touch the domain layer directly, run SQL, or duplicate use-case
   logic. No MCP type may appear in `internal/domain/**` or
   `internal/application/**` — the arch-go fitness tests enforce the
   dependency rule and **MUST** be extended to cover the mcp adapter.
3. Each server **MUST** ship as a separate `cmd/mcp` binary reusing the service's
   existing composition wiring.
4. Transport **MUST** be Streamable HTTP. stdio builds **MUST NOT** be shipped.
5. The official Go SDK (`github.com/modelcontextprotocol/go-sdk`) **MUST** be
   used and **MUST** be version-pinned in `go.mod`. Deprecated MCP features
   (`roots`, `sampling`, `logging`; SEP-2577) **MUST NOT** be used — prefer tool
   parameters, resource URIs, and configuration.

## 2. Tool curation — the surface is a product

The single most important rule: **expose intent-level tools, not one tool per
REST endpoint.** Tools are designed around decisions an agent makes.

1. A tool **MUST** map to an outcome an agent wants (`diagnose_stuck_tasks`),
   not to a transport route (`post_tasks_id_complete`).
2. A server **SHOULD** expose **no more than 8 tools**. A PR that pushes a server
   over that count **MUST** carry an explicit justification in its description
   and be approved by a second reviewer. This is the "curated surface" review
   rule; the Phase-6 CI lint enforces the count mechanically.
3. Bulk read access **MUST NOT** be exposed as a tool. Large read models are
   **resources**, scoped to a decision (see §5).

## 3. Naming conventions

| Element | Convention | Example |
| --- | --- | --- |
| Tool name | `snake_case`, `verb_noun`, intent-level | `get_queue_status` |
| Resource URI | `<kind>://<context>/<scope>` | `queue://fulfillment/PICK/status` |
| Auth scope | `mcp:<context>:<read\|write>` | `mcp:fulfillment:write` |
| Prompt name | `snake_case`, names the SOP | `triage_backlog` |

`<context>` is the bounded-context short name (`fulfillment`, `inventory`,
`work-planning`, `workforce`, `facility`, `labor`). This server's one
resource template is `scorecard://labor/{associateId}`.

## 4. Tool annotations (mandatory)

Every tool **MUST** declare annotations so a host can reason about risk before
letting a model call it:

1. A **read** tool **MUST** be annotated read-only (no state change).
2. A **write** tool **MUST** be annotated destructive and **MUST** require the
   `:write` scope.
3. Annotations and descriptions are treated as **untrusted** across servers; a
   host **MUST NOT** rely on another server's annotations for its own safety
   decisions. (Within our own trusted servers they are authoritative.)
4. Descriptions **MUST** state what the tool does and its side effects plainly —
   the description is read by the model and is part of the safety surface.

## 5. Resources — scoped context contracts

1. A resource **MUST** be scoped to a decision, backed by an existing read model
   / projection. It **MUST NOT** dump an entire table, config, or log.
2. Resources are read-only. Anything that changes state is a write **tool**, not
   a resource.

## 6. Prompts — operational SOPs

Prompts **SHOULD** encode operational discipline the model should follow: how to
interpret a tool result, when to stop and escalate, what "done" means. They are
user-initiated and carry the least risk, but they standardize agent behaviour
across clients and **SHOULD** be used rather than leaving procedure implicit.

## 7. Security & authorization (current posture: unauthenticated)

The static bearer-key layer this section originally mandated (ADR 0009,
adopted fleet-wide by
[ADR 0011](../adr/0011-rest-auth-static-bearer-scopes.md)) was **removed**
by [ADR 0012](../adr/0012-remove-rest-auth-layer.md). The MCP server
(`cmd/mcp`) currently serves every request without authentication, the same
as this context's REST surfaces. The rules that still apply:

1. No secret, token, or key may appear in any log line.
2. Servers **MUST** remain reachable only in-cluster. A server **MUST NOT**
   be exposed to public/end-user traffic until an identity layer is
   reintroduced by a future ADR.
3. This server exposes read tools only, so the read/write scope split has
   nothing to separate today. A future write tool needs a new ADR that
   settles its authorization first.
4. When a tool must call another service, the server **MUST** authenticate
   as its own client for that hop and **MUST NOT** pass a client token
   through (confused-deputy prevention). This applies the day any upstream
   hop exists; `labor-performance` has none.

## 8. Guardrails (regardless of auth)

1. Every tool handler **MUST** validate its inputs defensively — the caller is a
   model, arguments are untrusted.
2. Write tools **MUST** be rate-limited.
3. Domain errors **MUST** surface as clean structured tool errors, mapped from
   RFC 7807. The existing invariants are the safety net for model-invoked writes
   and **MUST NOT** be bypassed.

## 9. Auditability

Every tool call **MUST** emit an audit record with, at minimum:

- `client_id` and `scope` (once an identity layer exists — see §7),
- `tool` name,
- `outcome` (allowed / denied / error),
- timestamp and trace id.

Audit records **MUST** carry the OpenTelemetry trace id so a call links to its
span. The adapter **MUST** be instrumented with the platform's existing OTel
setup: a span per tool call plus invocation and denial counters, visible in
Jaeger and Grafana alongside HTTP.

## 10. Quality gate (same bar as the rest of the service)

1. Tool handlers **MUST** be unit-tested (table-driven, in-memory adapters) to
   the platform's ≥90% coverage bar, plus at least one transport-level test.
2. The MCP adapter **MUST** pass `make check` (fmt, vet, build, lint, test) and
   the arch-go fitness tests.
3. **Phase-6 governance gate:** `internal/adapters/inbound/mcp/governance_test.go`
   boots the real server over an in-process transport and fails the build if
   the tool count exceeds 8, a tool name breaks the `verb_noun` pattern, or a
   tool lacks annotations or a description. It is a plain `go test`, so it
   runs in the existing CI `test` job.
4. **Eval gate (E1–E3):** the tool surface **MUST** pass the eval suites in
   `internal/adapters/inbound/mcp/eval_*_test.go` and
   `evalsuite_test.go`, all plain `go test`s inside the CI `test` job:
    - **E1 — schema & metadata** (`eval_governance_test.go`): every
      advertised tool's input schema resolves as a JSON Schema, accepts a
      schema-shaped arguments object, and REJECTS wrong-typed values (it
      constrains model input, not just decorates it); every parameter
      carries a non-empty description; the advertised surface matches
      `testdata/tool_registry.golden`; and this repo's tools are present,
      correctly credited, and globally unique in
      `testdata/fleet_tool_snapshot.golden` (the federated registry kept
      identical across all fleet repos — a model host mounts several of
      these servers together, so tool names MUST NOT collide).
    - **E2 — wire conformance** (`eval_conformance_test.go`): over the real
      Streamable HTTP handler — initialize handshake carries server info
      and non-empty instructions; unknown tools, wrong-typed arguments,
      unknown extra arguments, unknown resources and prompts are rejected;
      resource templates and prompts are discoverable; a closed session
      fails loudly.
    - **E3 — behavioral evals** (`evalsuite_test.go` +
      `testdata/features/mcp_tools.feature`): Gherkin scenarios driving
      `tools/call` with model-realistic arguments (stray keys, wrong types,
      unknown associates and task types) against seeded state at fixed
      clock offsets, pinning structured results — including the nullable
      metrics, which MUST stay `null` rather than become a fabricated
      number.

### Pinned behavioral contracts the evals found

- Typed tool schemas are **strict** (`additionalProperties: false`, the
  SDK default): stray model-generated argument keys are rejected with a
  validation error, not silently ignored.
- `get_associate_scorecard` with two scored tasks reports trend
  `INSUFFICIENT_DATA` — `ClassifyTrend` needs 3 scored recent tasks — and
  a false coaching flag (likewise a 3-task rule). Both are real values,
  not absences.
- `get_task_type_utilization`'s `associates` count derives from the
  **idle-period** read, not from task rows: a task type with recorded
  tasks but no idle gaps reports `associates: 0` alongside non-zero
  `taskSeconds` (pinned in the 2h-window PACK scenario).
- The utilization window filter admits rows whose `CompletedAt` is after
  the (fixed) clock's now — the read is `since`-bounded only. The eval
  seed keeps every in-window completion in the past so the pins do not
  depend on this.
- `get_task_type_performance` for a valid-but-never-seen task type
  succeeds with zero counts and `null` means, while `get_labor_standard`
  for the same task type errors ("no active labor standard") — a closed
  enum with no rows is knowable, an unposted standard is not.

## 11. Changing this charter

This charter is versioned with the docs. A change to a global standard **MUST**
be proposed as a PR and, because it binds all five contexts, **SHOULD** be
recorded as an ADR when it changes an architecturally significant rule.

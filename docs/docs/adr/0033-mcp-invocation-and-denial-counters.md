---
id: 0033-mcp-invocation-and-denial-counters
slug: /adr/0033-mcp-invocation-and-denial-counters
title: "33. MCP tool invocation and denial counters"
sidebar_label: "33. MCP invocation & denial counters"
description: "ADR 0033 — cmd/mcp emits the two counters the MCP Governance Charter (§9) requires: labor_performance.mcp.tool.invocations (by mcp.tool.name and outcome ok/error/denied) and labor_performance.mcp.tool.denials (by mcp.tool.name and reason unknown_tool/invalid_arguments), recorded by a receiving middleware on tools/call."
---

# 33. MCP tool invocation and denial counters

## Status

Accepted. Decided 2026-10-12 (fleet gap-fix wave, decision D28). It
implements the counters §9 of the [MCP Governance
Charter](../mcp/governance-charter.md) requires; [ADR
0009](./0009-mcp-inbound-adapter.md) and [ADR
0030](./0030-mcp-eval-and-governance-gates.md) are not changed.

## Context

The charter says the MCP adapter "MUST be instrumented with the platform's
existing OTel setup: a span per tool call plus invocation and denial
counters, visible in Jaeger and Grafana alongside HTTP". The span existed
(`mcp.tool <name>`, `internal/adapters/inbound/mcp/tools.go`); the counters
did not. The charter names the two counters but not their instrument names
or attributes, so this record fixes them.

Two facts shape the design:

- The server has no identity layer ([ADR 0012](./0012-remove-rest-auth-layer.md)),
  so nothing is denied for lack of a scope. What the server does refuse is a
  call to a tool it does not register, and arguments that fail the tool's
  input schema (wrong type, unknown extra argument, missing required field).
- The Go MCP SDK refuses both **before** the tool handler runs: an unknown
  tool is a JSON-RPC `invalid params` error, schema-invalid arguments come
  back as a tool result with `isError: true`. The handler wrapper (and its
  span) never sees those calls, so counters recorded there would miss every
  denial.

## Decision

1. **Two Int64 counters**, unit `{call}`, meter scope
   `github.com/claudioed/labor-performance/internal/adapters/inbound/mcp`:

   | Instrument | Attributes | Prometheus series |
   | --- | --- | --- |
   | `labor_performance.mcp.tool.invocations` | `mcp.tool.name`, `outcome` = `ok` \| `error` \| `denied` | `labor_performance_mcp_tool_invocations_total` |
   | `labor_performance.mcp.tool.denials` | `mcp.tool.name`, `reason` = `unknown_tool` \| `invalid_arguments` | `labor_performance_mcp_tool_denials_total` |

   One counter with an `outcome` attribute follows the fleet metrics
   convention ([ADR 0008](./0008-standard-metrics-convention.md));
   `mcp.tool.name` is the key the span already carries.
2. **Outcomes.** `ok`: the handler ran and returned a result. `error`: the
   handler ran and returned an error; that includes domain rejections
   ("no active labor standard for this task type") and the handlers' own
   input checks (empty `associateId`, unknown task type), because the handler
   ran. `denied`: the SDK refused the call before the handler ran. Every
   denied call also adds one to the denials counter with its reason.
3. **Recorded by a receiving middleware** on `tools/call`
   (`internal/adapters/inbound/mcp/metrics.go`), installed by `NewServer`.
   The handler wrapper (`addTool`) marks the call's context when it runs;
   the middleware reads that mark after the SDK returns, which is how it
   tells a denial from a handled call. Other methods (`tools/list`,
   resources, prompts) are not counted.
4. **Bounded labels.** A tool name the server does not register is recorded
   as `mcp.tool.name="unknown"`, so a caller cannot create series by
   inventing names.
5. **Provider.** `inboundmcp.Deps.MeterProvider` (nil = the global provider
   `telemetry.Setup` installs before `cmd/mcp` builds the server). If an
   instrument cannot be created the middleware records into a no-op counter:
   an MCP call never fails because of metrics.

## Consequences

- Grafana can show tool usage per tool and outcome, and alert on a rising
  denial rate (a model calling tools that do not exist or sending
  ill-typed arguments usually means a prompt or client regression).
- The span still only covers handled calls; a denied call has a counter and
  the HTTP server span of `cmd/mcp`, not an `mcp.tool` span.
- The charter's audit record (§9: one record per call with tool, outcome,
  timestamp and trace id) is not part of this decision. The counters do not
  replace it.
- If an identity layer is ever reintroduced, a refusal for a missing scope
  becomes a third denial reason; the instruments do not change.

## Verification

`internal/adapters/inbound/mcp/metrics_test.go`:
`TestToolMetrics_CountInvocationsByOutcomeAndDenialsByReason` (real server
over the in-memory transport, SDK manual metric reader: ok, two handler
errors, two schema denials, one unknown tool),
`TestToolMetrics_OtherMethodsAreNotCounted`, `TestClassify`.

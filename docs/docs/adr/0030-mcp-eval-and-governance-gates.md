---
id: 0030-mcp-eval-and-governance-gates
slug: /adr/0030-mcp-eval-and-governance-gates
title: "30. MCP eval suite and governance gate, run as plain go test"
sidebar_label: "30. MCP eval & governance gates"
description: "ADR 0030 — the MCP adapter's tool surface is pinned by a governance gate (tool count, naming, annotations) and three eval suites (schema/metadata, wire conformance, behavioral Gherkin scenarios), all plain go test functions in the existing CI test job, per the fleet's MCP Governance Charter."
---

# 30. MCP eval suite and governance gate, run as plain go test

## Status

Accepted — implemented prior to this record, as the Phase-6 gate the [MCP
Governance Charter](../mcp/governance-charter.md) (§10) mandates; this
closes the documentation gap found by the 2026-10-04 ADR audit (the charter
and the test files existed and were referenced from [ADR
0009](./0009-mcp-inbound-adapter.md), but the gate itself had no ADR of its
own recording it as a decision).

## Context

[ADR 0009](./0009-mcp-inbound-adapter.md) registered this context's MCP
tool surface; the fleet's [MCP Governance
Charter](../mcp/governance-charter.md) sets the estate-wide rules every MCP
server must satisfy (curated tool count, naming conventions, mandatory
annotations, auditability). A charter with no automated check is a
convention a future PR can silently drift from; this context needs the
same enforcement every sibling MCP server has.

## Decision

**`internal/adapters/inbound/mcp/` carries two kinds of automated checks,
both plain `go test` functions inside the existing CI `test` job — no
separate pipeline, no separate binary:**

- **Governance gate** (`governance_test.go`): boots the real server over an
  in-process transport and fails the build if the tool count exceeds the
  charter's cap of 8, a tool name breaks the `verb_noun` naming convention,
  or a tool lacks annotations or a description.
- **Eval suites** (`eval_governance_test.go`, `eval_conformance_test.go`,
  `evalsuite_test.go` + `testdata/features/mcp_tools.feature`):
  - **E1 — schema & metadata**: every tool's input schema resolves as a
    JSON Schema, accepts schema-shaped input, and rejects wrong-typed
    values; every parameter has a non-empty description; the surface
    matches `testdata/tool_registry.golden` and this repo's tools are
    unique in the fleet-wide `testdata/fleet_tool_snapshot.golden`.
  - **E2 — wire conformance**: over the real Streamable HTTP handler —
    initialize handshake, unknown tools/resources/prompts rejected, wrong-
    and unknown-typed arguments rejected, a closed session fails loudly.
  - **E3 — behavioral evals**: Gherkin scenarios drive `tools/call` with
    model-realistic arguments against seeded state at fixed clock offsets,
    pinning structured results (including that nullable metrics stay
    `null`, never a fabricated number).

## Consequences

- The charter's rules are enforced the moment a PR is opened, not only at
  review time: exceeding the tool cap, a malformed schema, or a
  colliding tool name fails CI.
- The fleet-wide tool-name uniqueness check
  (`testdata/fleet_tool_snapshot.golden`) means a tool added here that
  collides with another context's tool name is caught locally, before a
  host that mounts several of these servers together would ever see the
  collision.
- These are ordinary Go tests, so no new CI job, image, or runner was
  needed to add this gate — the trade is a larger `internal/adapters/
  inbound/mcp` test surface to maintain alongside the adapter itself.

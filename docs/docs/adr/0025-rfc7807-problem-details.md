---
id: 0025-rfc7807-problem-details
slug: /adr/0025-rfc7807-problem-details
title: "25. RFC 7807 Problem Details for every REST error"
sidebar_label: "25. RFC 7807 problem details"
description: "ADR 0025 — every REST error response (OLTP and reports routers) is an RFC 7807 application/problem+json body with a stable type/title pair per failure category, never an ad hoc JSON error shape."
---

# 25. RFC 7807 Problem Details for every REST error

## Status

Accepted — adopted from the first REST handler and used consistently since;
this closes the documentation gap found by the 2026-10-04 ADR audit (the
convention was referenced piecemeal from several other ADRs but never
recorded on its own).

## Context

A REST client (and, before [ADR 0012](./0012-remove-rest-auth-layer.md)
removed it, an MCP tool handler mapping a domain error) needs a
machine-readable, consistently-shaped error to branch on — not a bespoke
`{"error": "..."}` string that differs handler to handler. Every sibling
context in this fleet already standardized on
[RFC 7807](https://www.rfc-editor.org/rfc/rfc7807) ("Problem Details for
HTTP APIs"); a bounded context shaped differently is a tax on any client
(including `warehouse-ops-agent`) that talks to more than one of them.

## Decision

**Every REST error response, on both the OLTP router
(`internal/adapters/inbound/http/server.go`) and the reports router
(`reports_handler.go`), is `application/problem+json` carrying `type`,
`title`, `status`, `detail`, and `instance` (the request path), via the
shared `writeProblem`/`problemInfo` helpers and the `ProblemDetails` schema
in `apis/openapi.yaml`/`apis/openapi-reports.yaml`.**

- `type` is a short, stable slug per failure category (e.g.
  `standard-conflict`, `concurrent-modification`, `idempotency-key-reused`,
  `report-store-error`) — a client can switch on it without parsing
  `detail`'s prose.
- Domain/application errors are mapped to one of these categories at the
  HTTP adapter boundary; the domain layer itself never constructs a
  `ProblemDetails` (that would leak an HTTP concern inward, violating [ADR
  0001](./0001-hexagonal-ports-and-adapters.md)'s dependency rule).
- Every OpenAPI error response (`400`, `409`, `422`, `503`) references the
  same `ProblemDetails` schema, so the generated API docs and a real
  response always agree.

## Consequences

- One error shape to parse, fleet-wide, for both REST surfaces this service
  exposes.
- Adding a new failure category is additive: a new `type` slug and a new
  OpenAPI response entry, never a shape change existing clients must
  re-parse for.
- `detail` is prose for a human/log, not a stable contract; only `type`
  should ever be pattern-matched by a caller.

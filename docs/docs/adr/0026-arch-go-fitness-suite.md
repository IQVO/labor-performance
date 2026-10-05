---
id: 0026-arch-go-fitness-suite
slug: /adr/0026-arch-go-fitness-suite
title: "26. arch-go architecture fitness tests, enforced in CI"
sidebar_label: "26. arch-go fitness suite"
description: "ADR 0026 — internal/architecture runs arch-go-backed fitness tests (dependency-rule, MCP-adapter-isolation) as plain go test, gated by the CI arch-test job, closing ADR-0001's originally-deferred 'the rule is upheld by review only' gap."
---

# 26. arch-go architecture fitness tests, enforced in CI

## Status

Accepted — implemented prior to this record; this closes the documentation
gap found by the 2026-10-04 ADR audit, and the stale "deferred" claim this
same audit found in [ADR 0001](./0001-hexagonal-ports-and-adapters.md) (now
corrected there).

## Context

[ADR 0001](./0001-hexagonal-ports-and-adapters.md) accepted, as a known
weakness, that nothing in the compiler stops a use case importing
`net/http` or `kafka-go` directly — the inward-only dependency rule was
upheld only by code review. Every sibling context in this fleet closed that
gap with [`arch-go`](https://github.com/arch-go/arch-go), a dependency-rule
checker driven by a Go API rather than a separate CLI config file.

## Decision

**`internal/architecture/fitness_test.go` encodes this context's
architecture rules as `arch-go` dependency assertions, run as a plain `go
test` and gated by the CI `arch-test` job:**

- The hexagonal rule from [ADR 0001](./0001-hexagonal-ports-and-adapters.md):
  `internal/domain` imports nothing from this module outside itself;
  `internal/application` imports only `internal/domain`; adapters may
  import inward (`application`, `domain`) but nothing may import an
  adapter package from outside `cmd/`.
- `TestMCPAdapterDependencyRule` ([ADR
  0009](./0009-mcp-inbound-adapter.md)): the MCP inbound adapter may depend
  only on `application`/`domain`, and — the direction that actually matters
  for keeping it additive — nothing else in this module may import
  `internal/adapters/inbound/mcp`.
- `TestNoSiblingContextOutboundCalls` ([ADR
  0017](./0017-kafka-dlq-and-graceful-shutdown.md)): this context never
  makes an outbound REST/gRPC call to another bounded context, preserving
  the choreography-only posture of [ADR
  0003](./0003-kafka-choreography-consumer-of-fulfillment-execution.md).

Because these are ordinary `go test` functions (not a separate binary or
config format), they run inside the existing `test`/`arch-test` CI jobs
with zero additional CI plumbing, and fail the build — not just a review
comment — the moment a rule is violated.

## Consequences

- The dependency rule is enforced mechanically, not only by reviewer
  attention; a PR that imports `kafka-go` into `internal/application` fails
  CI, full stop.
- New adapters (MCP, the future frontend's own API surface if one is ever
  added) get the same enforcement for free by adding one more assertion,
  not a parallel tool.
- `arch-go`'s own API surface is a dependency this repo now carries in
  `go.mod`; a breaking `arch-go` release is a real (if rare) maintenance
  cost, accepted in exchange for a compiler-adjacent guarantee instead of a
  purely social one.

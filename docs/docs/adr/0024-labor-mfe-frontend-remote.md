---
id: 0024-labor-mfe-frontend-remote
slug: /adr/0024-labor-mfe-frontend-remote
title: "24. labor-mfe: a chart-shipped, disabled-by-default frontend remote"
sidebar_label: "24. labor-mfe frontend remote"
description: "ADR 0024 — this service ships its console screen as its own micro-frontend remote (web/, an nginx-unprivileged SPA), rendered by the chart's frontend-deployment.yaml/frontend-service.yaml, gated behind frontend.enabled (default false)."
---

# 24. labor-mfe: a chart-shipped, disabled-by-default frontend remote

## Status

Accepted — implemented prior to this record; this closes the documentation
gap found by the 2026-10-04 ADR audit.

## Context

`web/` holds a Vite/TypeScript SPA (the `labor-mfe` remote) that renders
this context's console screen against the existing REST API — the same
scorecard/standard/utilization data [ADR 0009](./0009-mcp-inbound-adapter.md)
exposes over MCP, but for a human in the WES Dashboard console, not a model.
It needs its own deployable because it is a static-asset server with no
server-side session or per-request state, a different scaling profile than
every Go binary in this chart.

## Decision

**We will ship `web/` as its own chart-rendered Deployment, disabled by
default, independent of the four Go binaries' lifecycle.**

- `charts/labor-performance/templates/frontend-deployment.yaml` /
  `frontend-service.yaml` render only when `frontend.enabled=true` (default
  `false` — existing releases are unaffected; `warehouse-infra` sets it true
  and supplies the built image tag).
- The image runs `nginx-unprivileged` (uid/gid 101, `readOnlyRootFilesystem:
  true`, every Linux capability dropped) serving the built SPA bundle.
- No ingress/HTTPRoute/Gateway API block is rendered for the frontend
  Service (kept `ClusterIP`): host-facing path routing for the console is
  owned entirely by the WES Dashboard's own gateway, so this is not a second
  public entrypoint.
- It gets its own `HorizontalPodAutoscaler` entry
  ([ADR 0019](./0019-horizontal-autoscaling-and-pgxpool-tuning.md)) — the
  most trivially horizontally-scalable workload in this chart, no
  server-side session to worry about.

## Consequences

- The console screen can be deployed, scaled, and rolled back independently
  of `cmd/labor`/`cmd/mcp`/`cmd/labor-reports`/`cmd/labor-projector`.
- CORS on the OLTP and reports routers ([ADR
  0007](./0007-analytical-data-product.md)) already allows the origins this
  SPA calls from; no further backend change was needed to light it up.
- Further screen *content* work (which read models it surfaces, how it
  composes them) is explicitly out of scope for this record — this ADR
  covers only that the remote exists and how it ships.

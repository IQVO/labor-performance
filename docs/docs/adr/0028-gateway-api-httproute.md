---
id: 0028-gateway-api-httproute
slug: /adr/0028-gateway-api-httproute
title: "28. Gateway API HTTPRoute, disabled by default, alongside Ingress"
sidebar_label: "28. Gateway API HTTPRoute"
description: "ADR 0028 — the chart ships a Gateway API HTTPRoute template (gatewayApi.enabled, default false) as an alternative to the existing Ingress template, so a cluster that has migrated to Gateway API can adopt this service without a chart fork."
---

# 28. Gateway API HTTPRoute, disabled by default, alongside Ingress

## Status

Accepted — implemented prior to this record; this closes the documentation
gap found by the 2026-10-04 ADR audit.

## Context

This chart already ships a classic `networking.k8s.io/v1` `Ingress`
template for the OLTP API. The fleet's clusters are migrating toward the
[Gateway API](https://gateway-api.sigs.k8s.io/) (`gateway.networking.k8s.io`)
as the forward-looking standard for north-south routing: it is portable
across implementations (unlike Ingress's proliferation of
annotation-driven, implementation-specific behavior) and expresses routing
rules (path rewrite, header match) as first-class fields rather than
annotations a reader has to know to look for.

## Decision

**`charts/labor-performance/templates/httproute.yaml` renders an
`HTTPRoute` resource, gated behind `gatewayApi.enabled` (default `false`),
as an alternative to — not a replacement for — the existing `Ingress`
template.**

- `gatewayApi.parentRefs` names the `Gateway` resource(s) this `HTTPRoute`
  attaches to (name/namespace/`sectionName`), matching the Gateway API's
  own attachment model.
- `gatewayApi.hosts` lists path matches (`path`, `pathType`, default
  `PathPrefix`); `gatewayApi.stripPath` optionally adds a `URLRewrite`
  filter that strips the matched prefix before forwarding, for a cluster
  that fronts several services under one host with per-service path
  prefixes.
- Both templates can in principle render at once (nothing in the chart
  prevents `ingress.enabled` and `gatewayApi.enabled` both being `true`);
  operationally a cluster picks one or the other per its own migration
  state. Neither is default-enabled, so existing releases are unaffected.

## Consequences

- A cluster that has migrated to Gateway API can adopt this service by
  setting one values flag, with no chart fork and no hand-written
  `HTTPRoute` manifest to keep in sync with chart changes.
- Two routing templates to keep correct is more chart surface than one;
  accepted because the alternative (forcing every consuming cluster onto
  whichever one this chart picked first) is worse during a fleet-wide,
  multi-year migration window.
- The frontend Service deliberately has **no** `HTTPRoute`/`Ingress`/
  `gatewayApi` block of its own (see [ADR
  0024](./0024-labor-mfe-frontend-remote.md)) — only the OLTP API is
  host-routed by this chart.

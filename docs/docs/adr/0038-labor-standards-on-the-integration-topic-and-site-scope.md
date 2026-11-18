---
id: 0038-labor-standards-on-the-integration-topic-and-site-scope
slug: /adr/0038-labor-standards-on-the-integration-topic-and-site-scope
title: "38. Labor standards on the integration topic, with an optional site scope"
sidebar_label: "38. Standards on the integration topic + site scope"
description: "ADR 0038 — LaborStandardDefined and LaborStandardRevised are also published on warehouse.labor-performance.events with the analytics payload unchanged, so warehouse-planning can keep a local copy; a standard gains an optional site_code scope (site-specific wins for a consumer that knows the site); scoring is unchanged."
---

# 38. Labor standards on the integration topic, with an optional site scope

## Status

Accepted (2026-11-18). Extends [ADR 0013](./0013-labor-performance-integration-events.md)
(the integration topic, whose first cut carried `TaskPerformanceRecorded` only and named this
additive step) and [ADR 0035](./0035-handling-unit-standards-and-team-lift.md) (the standard's
identity). Consumer: warehouse-planning ADR 0022, which plans labour from these standards.
Additive: no existing type, payload field, route or response field changes.

## Context

warehouse-planning answers whether a warehouse can process the demand assigned to it. To say
how many labour hours a window's demand requires it needs the engineered standards this
context owns, not a second set of numbers declared somewhere else. Two things stop it today:

1. `LaborStandardDefined` and `LaborStandardRevised` are published on
   `warehouse.labor-performance.analytics` only. That topic feeds this context's own projector;
   the fleet rule is that no other context reads it. `integration_publisher.go` already says a
   consumer needing the standards "can be added to this publisher's marshalData in a later,
   additive change".
2. A standard has no site. Layouts differ between buildings, and engineered standards are
   normally set per facility. A planning context that plans per site needs to be able to tell
   a site-specific standard from the fleet-wide default.

## Decision

1. **Both standard events go on the integration topic too.** The integration publisher's
   `marshalData` maps `LaborStandardDefined` and `LaborStandardRevised` to
   `warehouse.labor-performance.events`, through the same transactional outbox, with:
   - the same `type` (`com.warehouse.wes.labor-performance.standard.<Event>`);
   - `dataschema` `urn:warehouse:labor-performance:events:<Event>:v1` (the stream segment names
     the integration stream);
   - the SAME `data` as the analytics copy (field names and types unchanged);
   - partition key `task_type`, so every revision of one task type stays in order; `subject`
     is the `standard_id`.
   The analytics copy is unchanged. The AsyncAPI integration channel lists both messages, and
   the CloudEvents type catalogue marks both `(events + analytics)`.
2. **Optional site scope.** A standard may carry `site_code` (a facility-layout site code;
   `""`, the default and the only state before this ADR, means every site).
   - `POST /standards` accepts an optional `siteCode`; the `Standard` response and both events
     carry it (`site_code` on the wire, omitted when empty, so today's golden bytes stay
     identical).
   - At most one open standard per `(task_type, handling_unit, site_code)`; a revision closes
     only the record of the same triple.
   - `GET /standards/{taskType}` keeps its meaning (the site-less standard); `?siteCode=` reads
     the standard of that site exactly, `404` when none.
   - Migration `0008` adds the column with default `''` and widens the open-standard
     uniqueness to the triple.
3. **Precedence is the consumer's rule, scoring is unchanged.** A consumer that knows the site
   prefers a site-specific standard over the site-less one (warehouse-planning ADR 0022).
   `RecordTaskPerformance` keeps scoring against site-less standards exactly as today: this
   context does not attribute a completed task to a site, and changing what a score means is a
   decision of its own.
4. **Non-negotiables hold.** The site code is declarative input on a standard; this context
   still calls no sibling and reads only `warehouse.fulfillment.events`.

## Consequences

- warehouse-planning (and any later context) can keep a local copy of the standards and plan
  with the same numbers this context scores against.
- One more outbox row per standard change; standards change rarely.
- A site-specific standard is not yet used for scoring; until a later ADR changes that, a
  site's efficiency is measured against the fleet-wide standard. The `Standard` response says
  which site a standard belongs to, so the difference is visible.
- Consumers must treat `site_code` as optional: messages produced before this ADR have none.

## Alternatives rejected

- **Let warehouse-planning read the analytics topic.** Analytics topics belong to their
  context's projector.
- **A `GET` for planning to call.** Live cross-context calls are banned in both contexts.
- **Make `site_code` required.** Every existing standard and message would become invalid; the
  site-less standard is a real business fact (one engineered time for every building).

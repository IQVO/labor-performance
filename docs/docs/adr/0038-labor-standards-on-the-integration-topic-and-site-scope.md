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

## Addendum (2026-11-20): the SiteCode schema must spell the constructor's rule, not `\S`

The decision above is unchanged — a site code is 1-32 characters with no whitespace, exactly as
`shared.NewSiteCode` validates it (`internal/domain/shared/shared.go`: rune count over
`MaxSiteCodeLength` or invalid UTF-8 refused, then any rune for which `unicode.IsSpace` is true
refused). This addendum records how that rule must be ENCODED in the API specs, and why.

The OpenAPI `SiteCode` schema first spelled "no whitespace" as `pattern: '^\S+$'`. `\S` is not a
portable way to say "no Unicode White_Space": its meaning depends on the regex engine —
ECMA-262 (JSON Schema's `pattern` language) excludes U+00A0 but admits U+0085 (NEL); Python's
`re` excludes both but also excludes U+001C-U+001F, which Go's `White_Space` does not; and
Schemathesis 4.28's value generation admitted U+000B, U+0085 and U+00A0 outright. The `contract`
CI job generates requests FROM the schema, so it periodically produced a schema-compliant
`siteCode` the constructor refused with `400 invalid-site-code`, and failed with
"API rejected schema-compliant request" — randomly, depending on generation. CI run 44738035745
(PR #282's first head run, before any production change) failed on
`GET /standards/PACK?siteCode=%5E%C2%A0%C2%9Aq%3F%12O` — the `%C2%A0` is U+00A0 NO-BREAK SPACE.
A local probe with the pinned Schemathesis 4.28.0 and CI's exact flags reproduced the flake on
7 of 10 seeds with `^\S+$` and on 0 of 10 with the fix below.

**The rule:** wherever a spec constrains a value the domain constructs, the spec must encode the
constructor's validation exactly, and spelling a Unicode property class by its shorthand (`\S`,
`\w`, `\d`) is not that — the shorthand is engine-dependent. `apis/openapi.yaml`'s `SiteCode`
pattern now lists the refused code points explicitly (Go's `White_Space` set, which is what
`unicode.IsSpace` implements):

```
^[^\t\n\v\f\r\x20\x85\xa0\u1680\u2000-\u200a\u2028\u2029\u202f\u205f\u3000]+$
(exactly as it appears in `apis/openapi.yaml`; every code point is Go's `White_Space` set)

Anything the constructor accepts that the old pattern refused (U+001C-U+001F, U+FEFF, currency
symbols, zero-width characters) is now schema-valid too — the schema matches the domain in BOTH
directions. `internal/adapters/inbound/http/site_code_spec_test.go` pins this: a table of every
`White_Space` code point (and the exact CI-generated value) must be refused by the pattern, a
table of constructor-accepted values must be admitted, and minLength/minLength must equal 1 and
`MaxSiteCodeLength`. The test failed on the pre-fix schema and passes on the fixed one.

**Known remainder, deliberately not fixed here:** `apis/asyncapi.yaml`'s two `site_code` payload
fields still carry `^\S+$`. The drift there is inert — every published event's `site_code` has
already passed `NewSiteCode`, so no consumer can ever receive a value the pattern wrongly admits
— and fixing it is not this repo's alone to do: `scripts/harness/asyncapi_breaking.py` classifies
any pattern change on an owned payload field as breaking (4 ERR findings on the proposed change,
verified locally), and warehouse-planning pins `apis/asyncapi.yaml` byte-for-byte
(`contracts/pinned.json@develop`). It needs either a fleet-blessed drift-correction carve-out in
the breaking gate or a coordinated pin refresh; the REST fix in this addendum is the template.

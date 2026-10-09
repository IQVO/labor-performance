---
paths:
  - "docs/**"
  - "apis/**"
---

# Docs & API drift: two OpenAPI specs, one AsyncAPI spec, one Docusaurus site

## Why there are TWO OpenAPI specs

`labor-performance` ships an analytical data product (ADR 0007): a
separate `cmd/labor-reports` binary, on its own port, against its own
read-only analytical database, with its own REST surface. That surface is
documented in a SEPARATE spec from the OLTP API:

- `apis/openapi.yaml` — the OLTP API (`cmd/labor`, port 8080): 6
  endpoints plus `/healthz` and `/readyz` — standards, performance and utilization reads. Spectral-linted in CI
  (`api-lint` job) against `.spectral.yaml`.
- `apis/openapi-reports.yaml` — the reports API (`cmd/labor-reports`, port
  8092): `GET /reports/performance`, `GET /reports/performance/freshness`,
  `GET /healthz`. ALSO Spectral-linted in CI, same ruleset.

Both are gated in the same `api-lint` CI job — check `.github/workflows/
ci.yml` for the current step list if a new spec is ever added.

## Docusaurus wiring for both specs

`docs/docusaurus.config.ts`'s `docusaurus-plugin-openapi-docs` plugin has
**two config entries** (verify this is still true — it's easy for a future
edit to add only one):

- `labor` → `specPath: '../apis/openapi.yaml'`, outputs to
  `docs/docs/api-reference/rest/`.
- `laborReports` → `specPath: '../apis/openapi-reports.yaml'`, outputs to
  `docs/docs/api-reference/rest-reports/`.

`docs/package.json` scripts:

```bash
npm run gen-api-docs           # regenerate ONLY the OLTP spec's pages
npm run gen-api-docs:reports   # regenerate ONLY the reports spec's pages
npm run gen-api-docs:all       # regenerate BOTH (docusaurus gen-api-docs all)
npm run clean-api-docs[:reports|:all]   # matching clean-api-docs variants
```

`docs/sidebars.ts` imports both generated sidebar files
(`./docs/api-reference/rest/sidebar` and
`./docs/api-reference/rest-reports/sidebar`) into two top-level "API
Reference" / "Reports API Reference" categories. If you regenerate one
spec's pages and NOT the other, `sidebars.ts` still needs both directories
to exist or the Docusaurus build fails at import time.

CI's `docs-api-drift` job runs `npm run clean-api-docs:all && npm run
gen-api-docs:all` in `docs/` and fails on any `git diff` under
`docs/docs/api-reference/rest` or `docs/docs/api-reference/rest-reports`. Reproduce
it locally before pushing a spec change.

**When either `apis/openapi.yaml` or `apis/openapi-reports.yaml` changes:**
run `npm run gen-api-docs:all` (not just the single-spec script for
whichever one you touched, unless you're confident the other is already
current) and commit the resulting diff under `docs/docs/api-reference/`
as its own docs-only change. `onBrokenLinks: 'throw'` /
`onBrokenAnchors: 'throw'` in `docusaurus.config.ts` mean `npm run build`
fails loudly on a broken cross-reference — always run a full `build`, not
just `gen-api-docs`, before committing.

## AsyncAPI (`apis/asyncapi.yaml`) — narrative docs, not generated

Unlike the OpenAPI specs, there is no `docusaurus-plugin-asyncapi-docs`
wiring in this repo's `docs/` site (the fleet-wide aggregator
`warehouse-docs` has that tooling; this per-service site does not). The
AsyncAPI contract is instead described narratively in:

- `docs/docs/overview/introduction.md` (the fleet diagram) and `docs/docs/ecosystem/integration.md` (every upstream/downstream edge)
- `docs/docs/ecosystem/context-map.md` (the inbound relationship with the
  full CloudEvents example, and the outbound integration topic)
- `docs/docs/ddd/subdomain-classification.md` (domain events and where
  they are published)
- the DDD artifact pack under `docs/docs/ddd/` — above all
  `domain-events.md` (every type, topic, key and payload field),
  `bounded-context-canvas.md` (message tables),
  `domain-message-flow.md`, `eventstorming.md` and `sequence-diagrams.md`
- `docs/docs/adr/0007-analytical-data-product.md`,
  `docs/docs/adr/0010-transactional-outbox.md`,
  `docs/docs/adr/0013-labor-performance-integration-events.md`,
  `docs/docs/adr/0021-cloudevents-mandatory-event-envelope.md` (the
  envelope itself) and
  `docs/docs/adr/0014-labor-utilization-idleness.md` (the outbound topics
  and the `idle_seconds_before` field)
- `docs/docs/api-reference/rest-reports/labor-performance-reports-api.info.mdx`
  (references the analytics topic feeding the report)

**When `apis/asyncapi.yaml` changes** (a new channel, a changed payload
field, a new event type), these narrative pages must be hand-updated —
there is no generator to catch drift automatically. Grep for the topic
name (`warehouse.fulfillment.events`, `warehouse.labor-performance.
analytics`, `warehouse.labor-performance.events`) and the event type names (`TaskCompleted`,
`LaborStandardDefined`, etc.) across `docs/docs/**/*.md*` to find every
page that would need a matching update.

## Known pitfall: `@faker-js/faker` override breaks `gen-api-docs`

`docusaurus-plugin-openapi-docs` depends on `postman-collection@5.3.1`,
which calls faker's legacy `faker.address.city()` API — removed in
faker's v6 major (restructured into per-locale modules). If
`docs/package.json`'s `overrides` block pins `@faker-js/faker` to
`^10.5.0` (or any v6+), `npm run gen-api-docs*` fails with:

```
TypeError: Cannot read properties of undefined (reading 'city')
    at .../postman-collection/lib/superstring/dynamic-variables.js
```

`npm install`/`npm ci` succeeds silently either way — this only surfaces
when you actually run `gen-api-docs` or `build`. The fix is to NOT
override `@faker-js/faker` in this `docs/` package (most sibling repos'
`docs/package.json` — `order-management`, `workforce-management`,
`facility-layout`, `wes-work-planning`, `process-path-management` — have
no such override and resolve the compatible faker `5.5.3` transitively;
`inventory-storage` DOES carry the override and is consequently unable to
regenerate its API docs locally without the same fix). There is no faker
version that is simultaneously CVE-patched (`GHSA-qxc2-j82w-r537`, fixed
at `>=10.5.0`) and API-compatible with `postman-collection@5.3.1` — this
is a documented, accepted gap, not something to keep re-attempting.

## `docs.yml` trigger branch vs. the Pages environment's allowed branches

`gh api repos/IQVO/labor-performance/environments/github-pages/
deployment-branch-policies` shows **both `develop` and `main`** are
allowed to deploy to the `github-pages` environment. But
`.github/workflows/docs.yml`'s `on.push.branches` is `[main]` only — a
docs-only PR merged into `develop` does NOT trigger a live Pages deploy
until that content reaches `main` via the next GitFlow release. This is
consistent with the fleet's release cadence (docs ship with releases,
same as code) but worth knowing explicitly: don't expect a `develop`-only
docs change to appear on the live site immediately.

## Hand-written operational pages track the code

Env vars, Kafka topics, metric instruments and HTTP routes are documented by
hand in `docs/docs/operations/{configuration,runbook,observability,troubleshooting}.md`,
`docs/docs/development/testing.md` and `docs/docs/mcp/tools.md`. Nothing
regenerates them: update the matching page in the same PR as the code change
(a new `getenv`, topic, instrument or route).

---
id: 0037-adopt-fleet-rest-api-guideline
slug: /adr/0037-adopt-fleet-rest-api-guideline
title: "37. Adopt the fleet REST API guideline"
sidebar_label: "37. Fleet REST API guideline"
description: "ADR 0037 — labor-performance adopts the fleet REST API guideline: noun routes (no route of this service needed a rename), every POST and PATCH idempotent through the Idempotency-Key middleware with a global key, 409 idempotency-key-reused, the Idempotency-Replayed response header and 400 idempotency-key-invalid, no collection GET to paginate, enforced by the rest-guideline Spectral ruleset (R1-R8) in the api-lint job."
---

# 37. Adopt the fleet REST API guideline

## Status

Accepted (2026-11-01). Amends [ADR 0016](./0016-idempotency-key-middleware.md)
(the idempotency middleware: reuse is now `409`, not `422`) and follows
[ADR 0025](./0025-rfc7807-problem-details.md) (RFC 9457 problems) and
[ADR 0034](./0034-postgres-and-kafka-are-the-only-runtime.md) (Postgres is the
only runtime, so the middleware is always on). Part of the fleet-wide REST
cutover: it ships and is deployed together with the other services' cutover
PRs, because the key header contract and the list envelope are breaking for
every client.

## Context

The 2026-10-30 portfolio assessment measured two spellings of the replay
header, key reuse answered `422` in most services and `409` in one, three
command-URL styles and unpaginated lists. The fleet standard (the "REST API
guideline") removes each divergence and is enforced by a Spectral ruleset
(`.spectral.yaml` + `.spectral/functions/restGuideline.js`, rules R1-R8) wired
into the `api-lint` job with `--fail-severity=warn`.

This service has a very small write surface: `POST /standards`
(`DefineStandard`) is its only write over REST (everything else is a read, and
`TaskPerformance` rows come from the Kafka consumer). Its routes were already
nouns, it was already keyed, and it has no collection `GET`.

## Decision

Adopt the guideline as written. What it means for labor-performance:

1. **Routes (R1, R2).** URLs name things: a `:custom-method` suffix and a verb
   path segment are forbidden. **No route of this service had to be renamed**
   (`/standards`, `/standards/{taskType}`, `/associates/{associateId}/scorecard`,
   `/task-types/{taskType}/performance`, `/task-types/{taskType}/utilization`,
   `/associates/{associateId}/utilization`, `/reports/performance`,
   `/reports/performance/freshness`). `TestRoutesAreNounsNotVerbs` walks the
   real router and fails on a `:` suffix or a verb segment, so a future route
   cannot slip in.
2. **Idempotency (R3-R6, R8).** Every `POST` and `PATCH` sits behind the
   route-scoped `RequireIdempotencyKey` middleware (today only
   `POST /standards`); `TestEveryPostAndPatchRouteRequiresAnIdempotencyKey`
   walks the router and fails if a new `POST`/`PATCH` route answers anything
   but `400 idempotency-key-required` without a key. `PUT` and `DELETE` take
   no middleware. The contract is:
   - `Idempotency-Key`: 1-128 printable ASCII characters, a client-generated
     UUID per logical submit. Missing is `400 idempotency-key-required`;
     malformed (over 128 characters, a control character, non-ASCII) is
     `400 idempotency-key-invalid`.
   - The key is **global**: the stored record is found by the key alone
     (`idempotency_keys.key` is the primary key) and compared by the request
     fingerprint, method + path + SHA-256(body).
   - Same key, same fingerprint: the stored answer (status, body, headers) is
     replayed byte for byte with `Idempotency-Replayed: true`; the handler
     does not run again. The first execution never carries the header.
     `Idempotent-Replayed` is retired.
   - Same key, different method, path or body: **`409 Conflict`**, problem
     `idempotency-key-reused` (was `422`). It never executes anything and
     never carries `Idempotency-Replayed`. A business `409`
     (`concurrent-modification`, `standard-conflict`) is told apart from it by
     the problem type.
   - Persistence (unchanged from ADR 0016): the key row, the aggregate change
     and the outbox rows commit in one transaction. `2xx` and `4xx` answers are
     stored and replayed; a `5xx` or a panic rolls the whole transaction back,
     key row included, and is **never stored**, so the retry executes again
     (new in this ADR). Concurrent requests with one key are serialized by the
     unique index; the loser replays the winner's answer.
   - OpenAPI (STANDARD 2.7): the `IdempotencyKey` parameter
     (`required: true`, the middleware is on in every runtime mode, ADR 0034),
     `400` and `409` on the operation, and the `Idempotency-Replayed` header
     on the `201`. The `422` of `POST /standards` now describes only business
     validation.
3. **Pagination (R7).** There is no collection `GET`: every read addresses one
   thing (a standard, a scorecard, a task type's performance or utilization)
   or is a time-windowed report whose `rows` and `byTaskType` arrays are
   members of one report object (`/reports/performance`, bounded by the
   mandatory `from`/`to` window). The ruleset flags nothing, so **no
   `x-pagination-exempt` is declared and `page` / `cursor` are not
   introduced**. If a collection `GET` is ever added it must paginate
   (`limit` + `cursor`, keyset, `{items,nextCursor}`).
4. **Contract job.** `scripts/contract-test.sh` is unchanged in shape: with no
   paged operation no `schemathesis.toml` is needed. `scripts/contract_hooks.py`
   keeps minting a fresh UUID per request, and its comment now names `409`.

## Exemptions

None. No `x-pagination-exempt`, and no `PUT` with a keyed exception (this
service has no `PUT`).

## Consumers

- `web/src/api.ts` (the `labor_mfe` remote) already sends a fresh
  `crypto.randomUUID()` `Idempotency-Key` on its only write
  (`POST /standards`) and reads no list; it needs no change.
- No other service calls this service's REST API (none was found in the fleet
  sweep); the MCP and A2A adapters call use cases, not HTTP routes.
- Wire contract for any external client: send `Idempotency-Key` on
  `POST /standards`, treat `409 idempotency-key-reused` as a client bug (do not
  retry blindly), and read `Idempotency-Replayed: true` as "this is the stored
  answer".

## Consequences

- A client that relied on `422 idempotency-key-reused` must now handle `409`;
  a client that checked `Idempotent-Replayed` (none in the fleet) must check
  `Idempotency-Replayed`.
- A key longer than 128 characters, or with a non-printable or non-ASCII
  character, is `400 idempotency-key-invalid` instead of being stored.
- A transient `5xx` no longer poisons a key: the retry with the same key
  re-executes.
- The breaking change merges and deploys with the rest of the REST cutover
  set; `develop` is half-migrated until the last PR of the set lands.

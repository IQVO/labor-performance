---
id: 0020-migrations-direct-postgres-connection
slug: /adr/0020-migrations-direct-postgres-connection
title: 20. Run golang-migrate against a direct Postgres connection, not PgBouncer
sidebar_label: 20. Migrations bypass PgBouncer
description: "ADR 0020 -- Phase 4 fleet-wide finding (originating in order-management ADR-0029): golang-migrate's postgres driver takes a session-scoped pg_advisory_lock to serialize concurrent migration runs, which is incompatible with PgBouncer's transaction-pooling mode (warehouse-infra PR #43). Two or more labor-performance replicas (cmd/labor or cmd/mcp) starting concurrently -- an HPA scale-out (ADR-0019) or an ordinary rolling deploy -- crash-loop until one wins the advisory-lock race. Fix: a second env var, MIGRATIONS_DATABASE_URL, carries a direct (non-pooled) connection string used ONLY for the migration step; DATABASE_URL/the runtime pgxpool is untouched and keeps going through PgBouncer."
---

# 20. Run golang-migrate against a direct Postgres connection, not PgBouncer

## Status

Accepted — implemented in the same change that introduces this record.
This is labor-performance's instance of the Phase 4 fleet-wide bug fix
first found and fixed in
[order-management PR #115](https://github.com/claudioed/order-management/pull/115)
([ADR-0029](https://github.com/claudioed/order-management/blob/develop/docs/docs/adr/0029-migrations-direct-postgres-connection.md)),
the reference implementation every other OLTP service in the fleet ports
verbatim. The companion infrastructure change,
[warehouse-infra PR #44](https://github.com/claudioed/warehouse-infra/pull/44),
already provisions the `MIGRATIONS_DATABASE_URL` secret key for all 9 OLTP
services (including this one) in one pass — no further warehouse-infra
work is needed for this port.

## Context

warehouse-infra's PgBouncer rollout
([PR #43](https://github.com/claudioed/warehouse-infra/pull/43), Phase 3)
repointed every one of the fleet's 9 OLTP services' `DATABASE_URL` secret
at PgBouncer (`terraform/pgbouncer.tf`), in **transaction-pooling** mode
(`pool_mode = "transaction"`). That's the correct mode for this fleet's
steady-state traffic — application code never holds session state across
statements — and PR #43 already carved out one deliberate exception:
analytics DSNs (`ANALYTICS_DATABASE_URL`, consumed by this service's own
`cmd/labor-projector` and `cmd/labor-reports`) were left pointed directly
at Postgres, because a single low-QPS analytics consumer gets no pooling
benefit. Those two binaries are therefore **out of scope for this ADR** —
they never went through PgBouncer in the first place, so they were never
exposed to the bug described below.

What PR #43 did not carve out: **migrations**. This service's OLTP
binaries, `cmd/labor` and `cmd/mcp`, both run golang-migrate's postgres
driver (`github.com/golang-migrate/migrate/v4/database/postgres`) against
`DATABASE_URL` at process startup, before serving any traffic
(`buildPersistence` in `cmd/labor/main.go`, `buildAdapters` in
`cmd/mcp/main.go`). golang-migrate's postgres driver calls `SELECT
pg_advisory_lock($1)` to serialize concurrent migration runs — this is by
design: if two processes start at once and both try to run the same
migration, whichever loses the lock should block, not race.

`pg_advisory_lock` is **session-scoped**: the lock is held by whichever
physical backend connection issued it, and is expected to be released by
that same connection (or the session ending). PgBouncer's
transaction-pooling mode does not preserve that mapping — each statement
in a client's logical session can be routed to a different physical
backend connection, because the client's backend connection is returned
to the pool the instant its transaction commits. So:

- Pod A dials PgBouncer, gets backend connection #1, takes the advisory
  lock, runs migrations.
- Pod B dials PgBouncer *concurrently*, gets a **different** backend
  connection, and PgBouncer may freely reuse/rotate backend connections
  for either pod's subsequent statements mid-"session" from the
  application's point of view.
- The advisory lock never behaves as a real mutex across the two pods.
  Whichever pod's statements land on a backend connection with
  unexpected transaction/prepared-statement state gets errors like
  `pq: unnamed prepared statement does not exist` or `pq: canceling
  statement due to statement timeout`, and crash-loops for roughly 1-2
  minutes until the race resolves.

This is a **latent, fleet-wide, production-blocking bug**, not a
load-test artifact: it fires on any ordinary rolling ArgoCD deploy with
more than 1 replica of `cmd/labor` or `cmd/mcp`, and on every HPA
scale-out event for the `api` workload (ADR-0019 enables an HPA for `api`
specifically; `mcp` was deliberately excluded from that ADR for an
unrelated in-memory-session-state reason, but `cmd/mcp` still runs
migrations on every pod start and is still exposed to this bug on any
ordinary rolling deploy with `replicas > 1`). It was found fleet-wide
during Phase 4 (k6/HPA load-test validation) cleanup and fixed first in
order-management, reproduced live there exactly as this ADR's Context
section describes. It blocks safely running any of this service's own
Phase-3 HPA (ADR-0019) at more than 1 replica.

## Decision

Give this service a **second** connection string,
`MIGRATIONS_DATABASE_URL` — a direct (non-pooled, session-mode) Postgres
connection string, same user/password/dbname as `DATABASE_URL`, pointed
at Postgres itself rather than PgBouncer — used **only** for the
golang-migrate startup step in `cmd/labor` and `cmd/mcp`. `DATABASE_URL`
and the pgxpool built from it are completely unchanged: every request
this service serves still goes through PgBouncer in transaction-pooling
mode, exactly as PR #43 set up.

This is architecturally identical to the analytics-DSN carve-out PR #43
already made, and to universal Postgres/PgBouncer operational guidance:
**migrations need a direct/session connection; steady-state application
traffic goes through the pooler.** We are not weakening or changing
PgBouncer's `pool_mode` (still `transaction`, still correct for this
fleet's traffic) — this fix is entirely about routing one specific,
short-lived, startup-only operation around the pooler, not about
changing how the pooler behaves for anyone else.

`warehouse-infra` PR #44 already provisions `MIGRATIONS_DATABASE_URL` as
a new key alongside the existing `DATABASE_URL` key in this service's
`kubernetes_secret.service_db` (and the same for all 9 OLTP services).
This repo's `cmd/labor/main.go` and `cmd/mcp/main.go` (both run
migrations) now read `MIGRATIONS_DATABASE_URL` for the migration step:

```go
migrationsDatabaseURL := getenv("MIGRATIONS_DATABASE_URL", databaseURL)
...
buildPersistence(ctx, databaseURL, migrationsDatabaseURL, migrationsPath, logger)
...
postgres.RunMigrations(migrationsDatabaseURL, migrationsPath)  // migrations only
// the pgxpool opened right after this still uses databaseURL, unchanged
```

The fallback to `databaseURL` when `MIGRATIONS_DATABASE_URL` is unset
keeps every environment that doesn't provision the split — local dev,
CI integration tests, or any cluster whose Terraform predates this fix —
working exactly as before, byte-for-byte. Nothing about local dev or CI
changes as a result of this PR.

`charts/labor-performance`: new `database.migrationsExistingSecretKey`
value (default `"MIGRATIONS_DATABASE_URL"`) renders the env var in both
the `api` (`templates/deployment.yaml`) and `mcp`
(`templates/mcp-deployment.yaml`) Deployments, sourced from the same
`existingSecret`, with `optional: true` on the `secretKeyRef` so a secret
that predates this key still starts the pod. The analytics Deployments
(`projector-deployment.yaml`, `reports-deployment.yaml`) are untouched —
they were never on `DATABASE_URL`/PgBouncer and were never exposed to
this bug, per the Context section above.

### Why not just make PgBouncer's pool_mode session for this fleet?

Rejected, for the same reason order-management's ADR-0029 rejected it:
session pooling would fix the advisory-lock problem but throws away the
entire point of PgBouncer for this fleet — transaction pooling is what
lets many short-lived HTTP-request-scoped OLTP connections share a small
number of physical Postgres backends. Switching to session mode
fleet-wide to accommodate a ~1-2 second startup-time lock call is the
tail wagging the dog.

### Why not just remove the advisory lock / skip migrations on non-leader replicas?

Rejected, for the same reason as order-management's ADR-0029: golang-
migrate's advisory lock is exactly the right mechanism *given a
session-scoped connection* — the bug is the mismatch between that
mechanism and the pooling mode we run migrations through, not the
mechanism itself. An init-container/Job-based alternative was considered
and rejected there for being a bigger architectural change (a new
Kubernetes resource type, coordination with rollout strategy) for the
same outcome this two-line env-var fallback already achieves, and it
would still need its own direct-vs-pooled connection decision. Same
conclusion applies here; no new information changes it for this repo.

## Consequences

- **Fixes** the crash-loop bug for `labor-performance`'s two OLTP
  binaries (`cmd/labor`, `cmd/mcp`), matching the fix already live for
  order-management and being ported across the rest of the fleet.
- **No runtime behavior change**: `DATABASE_URL` is untouched, so
  request-serving connection pooling, `pool_mode`, and PgBouncer's own
  configuration are all unaffected by this PR.
- **No behavior change for environments without the split**: the
  `getenv("MIGRATIONS_DATABASE_URL", databaseURL)` fallback means local
  dev and CI integration tests keep using `DATABASE_URL` for everything,
  exactly as before.
- **Unblocks running this service's ADR-0019 HPA at maxReplicas > 1**:
  this was a real, latent blocker for that ADR's own goal — an HPA
  scale-out is exactly the "2+ replicas start concurrently" trigger for
  this bug.
- No new secret key to provision from this repo's side — `warehouse-
  infra` PR #44 already carries `MIGRATIONS_DATABASE_URL` for this
  service (and all 9 OLTP services) mechanically from the same
  `local.services` map `DATABASE_URL` already comes from.
- `cmd/labor-projector` and `cmd/labor-reports` (the analytics binaries)
  are unaffected: they were never on PgBouncer, so there is nothing for
  them to opt into here.

## Verification

Reproduced and fixed first in order-management (see that repo's
ADR-0029 for the full live-cluster verification evidence: forced 3
concurrent replicas twice, 0 restarts, no `pq:` errors, versus reliable
crash-looping before the fix). This repo's own local verification for
this port:

- `go build ./...` — clean.
- `go test ./cmd/... -race -v` — all new and existing tests pass,
  including `TestMigrationsDatabaseURLFallback`,
  `TestMigrationsDatabaseURLFallback_MCP`,
  `TestBuildPersistence_UsesMigrationsDatabaseURLNotDatabaseURLForMigrations`,
  and `TestBuildAdapters_UsesMigrationsDatabaseURLNotDatabaseURLForMigrations`
  (the last two prove the migration step actually receives
  `migrationsDatabaseURL`, not `databaseURL`, by giving `DATABASE_URL` an
  unreachable address and `MIGRATIONS_DATABASE_URL` a malformed one and
  asserting on which error surfaces).
- `helm lint charts/labor-performance` and `helm template
  charts/labor-performance` — both Deployments render `DATABASE_URL` and
  `MIGRATIONS_DATABASE_URL` from the same secret with `optional: true` on
  the new key, and rendering with `database.migrationsExistingSecretKey`
  unset (simulating a pre-this-PR secret) omits the new env var entirely
  rather than failing to render.
- Full local quality gate (`make check`, `make check-all`) run green
  before opening the PR — see the PR description for the exact command
  output.

Live-cluster verification (forcing concurrent replicas against this
service specifically, the way order-management's ADR-0029 did) is left to
a follow-up deploy step once this PR is reviewed and merged, mirroring
how the other fleet repos in this fan-out are being ported; the fix
itself is identical code+chart shape to the already-live-verified
order-management reference.

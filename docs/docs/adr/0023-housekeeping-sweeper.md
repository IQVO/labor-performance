---
id: 0023-housekeeping-sweeper
slug: /adr/0023-housekeeping-sweeper
title: "0023. Housekeeping sweeper for idempotency keys and published outbox rows"
sidebar_label: "0023. Housekeeping sweeper"
description: "ADR 0023 — a small background sweeper in cmd/labor that deletes idempotency_keys older than a TTL (default 24h) and PUBLISHED outbox_events older than a retention (default 7d), closing the unbounded-growth gaps left open by ADR-0016 and ADR-0010."
---

# 0023. Housekeeping sweeper for idempotency keys and published outbox rows

## Status

Accepted — implemented in the same change that introduced this record.
Mirrors inventory-storage's ADR-0026 reference implementation verbatim in
shape. Closes the "known gap" each of ADR-0010 (`outbox_events`) and
ADR-0016 (`idempotency_keys`) recorded when those tables were introduced.

## Context

Two tables in the OLTP database only ever grow:

- `idempotency_keys` (ADR-0016) gets one row per `Idempotency-Key` seen on
  `POST /standards`, including the stored response body. Nothing deleted
  them; `idx_idempotency_keys_created_at` was created specifically for a
  future cleanup job, and that job's absence is the "Known follow-up" the
  migration's own comment records.
- `outbox_events` (ADR-0010) keeps every row after the relay marks it
  `published_at`. Published rows are only useful for short-term forensics.

Both were accepted as known gaps at introduction time. At production
traffic they are an unbounded storage and vacuum cost.

## Decision

`internal/adapters/outbound/postgres.Sweeper` — **one** small type with
one loop (both jobs are "delete old rows in batches" and share an
interval) — started by the `cmd/labor` composition root whenever the
Postgres backing exists (idempotency keys are written regardless of
`EVENT_PUBLISHER`, so the sweeper must not be gated on it):

| Env var | Default | Meaning |
|---|---|---|
| `HOUSEKEEPING_INTERVAL` | `1h` | Sweep period. `0` disables the sweeper. |
| `IDEMPOTENCY_KEY_TTL` | `24h` | Delete `idempotency_keys` rows with `created_at` older than this. `0` keeps them forever. |
| `OUTBOX_RETENTION` | `168h` (7d) | Delete **published** `outbox_events` rows with `published_at` older than this. `0` keeps them forever. |

(Chart values: `config.housekeepingInterval`, `config.idempotencyKeyTtl`,
`config.outboxRetention` — alongside `config.eventPublisher` and
`config.outboxRelayInterval`, which previously had to be injected by hand
via `extraEnv`.)

Rules the implementation guarantees:

1. **Unpublished outbox rows are never deleted**, however old — an event
   still waiting for the relay (broker outage) is not garbage.
2. Deletes are **batched** (1000 rows per statement, repeated until a
   batch comes back short) so a large backlog is removed in short
   transactions, not one long lock. Each statement targets an explicit
   key/id set chosen by a subquery, so it is safe to run in several
   replicas at once.
3. It runs one pass **immediately on start** and then every interval,
   never returns an error (a failed pass is logged and retried), and is
   stopped — with a bounded wait for its in-flight pass — **before** the
   pgx pool closes during graceful shutdown.
4. The sweeper stays in `cmd/labor`; `cmd/mcp`, `cmd/labor-projector` and
   `cmd/labor-reports` do not sweep (none of them writes either table).

## Consequences

- The 24h TTL is now the **replay window** of the idempotency contract: a
  retry of a `POST /standards` whose key is older than the TTL is treated
  as a brand-new request, not replayed. Clients must retry within that
  window (the intended use is retrying a dropped response, seconds to
  minutes).
- Published outbox rows older than 7 days are gone; forensics beyond that
  must come from Kafka itself (topic retention) rather than the outbox
  table.
- Proven against a real testcontainers Postgres:
  `TestSweeper_DeletesOnlyExpiredIdempotencyKeysAndPublishedOutboxRows`,
  `TestSweeper_ZeroTTLAndRetentionKeepEverything`,
  `TestSweeper_RunSweepsOnStartAndStopsOnCancel`
  (`internal/adapters/outbound/postgres/sweeper_integration_test.go`).

## Alternatives considered

- **A Kubernetes CronJob / pg_cron.** Rejected: another deployable (or
  extension) to operate, and the retention knobs would live outside the
  service's own config.
- **Partitioned tables with drop-partition.** Rejected as over-engineered
  for the current row volume.
- **Two independent jobs.** Rejected: one loop with two statements is
  simpler and shares the interval, shutdown and logging.

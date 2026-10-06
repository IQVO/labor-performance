---
id: 0031-analytics-consumer-atomic-claim-and-retry
slug: /adr/0031-analytics-consumer-atomic-claim-and-retry
title: "31. Analytics consumer: atomic claim + apply, commit after success, retry transient failures"
sidebar_label: "31. Analytics consumer atomicity"
description: "ADR 0031 — the analytics projector claims the CloudEvents id and applies the projection in ONE Postgres transaction (analyticsstore.UnitOfWork), reads with FetchMessage and commits the offset only after the transaction succeeds, and retries transient failures with capped exponential backoff instead of skipping the message."
---

# 31. Analytics consumer: atomic claim + apply, commit after success, retry transient failures

## Status

Accepted. Refines [ADR 0007](./0007-analytical-data-product.md), which
accepted the consumer-level dedupe gate and the projection's own claim as
two idempotency layers but did not discuss their ordering relative to the
offset commit. No wire contract changes.

## Context

`AnalyticsConsumer` (cmd/labor-projector) read the analytics topic with
`ReadMessage` under a consumer group, which commits the offset before the
handler runs. `HandleMessage` then recorded the CloudEvents `id` in
`analytics_consumed_events` in its own autocommit statement and only then
applied the projection in a second transaction. When the apply failed (a
transient database error), `Run` logged the error and read the next
message:

- the offset was already committed, so the message was never redelivered;
- the `analytics_consumed_events` claim had survived, so even a manual
  replay of the same id was short-circuited as an already-consumed
  duplicate.

The rollup silently lost that event for good. A malformed `data` payload
was returned as an error too, which is only safe while errors are
swallowed.

## Decision

1. **One transaction.** A new `analyticsstore.UnitOfWork` (the same
   ctx-carried `pgtx` mechanism the OLTP side uses) wraps the consumer's
   `MarkProcessed` claim and the projection's `Apply*`. `ConsumedEventsRepo`
   and `PostgresProjection` join the transaction in ctx when present; the
   projection still opens its own transaction when called without one
   (existing callers and tests). A failed apply rolls the claim back.
2. **Commit after success.** `Run` uses `FetchMessage` and calls
   `CommitMessages` only after the handler returns nil.
3. **Retry, never skip, on transient errors.** A non-nil handler error is
   retried on the same message with capped exponential backoff (200 ms to
   5 s, cancellable by ctx). A cancelled ctx leaves the offset uncommitted.
4. **Error classification.** The handler returns non-nil only for
   infrastructure failures (claim or apply). Deterministic bad input — not
   a CloudEvent, an unprojected type, or an undecodable `data` payload — is
   logged at WARN and skipped with a nil return, without being claimed, so
   it is committed past rather than retried forever. This consumer has no
   DLQ.

No migration and no event/REST contract change: both idempotency tables are
unchanged.

## Consequences

**Positive**

- A projection failure can no longer lose an event: either the claim, the
  idempotency row and the rollup upsert all commit, or none do and the same
  message is retried until it applies.
- Proved by testcontainers Postgres (rollback of all three writes) and an
  end-to-end Kafka + Postgres test in which the first apply fails after
  writing and the retry converges to exactly one counted task.

**Negative / accepted**

- A persistent database outage now stalls the projector on that message
  (with backoff and an ERROR log per attempt) instead of skipping ahead —
  correct for a replayable read model, but a poison-pill that fails
  *transiently-shaped* (a DB error caused by the payload itself) would block
  its partition; there is still no DLQ for this topic.
- The two idempotency tables remain redundant with each other; collapsing
  them would need a migration and is left out of scope.

## Alternatives considered

- **Drop `analytics_consumed_events` and rely only on
  `analytics_processed_events`.** Would also fix the loss, but removes a
  table (the migration policy is additive only) and changes ADR 0007's
  accepted shape.
- **Keep `ReadMessage` and re-publish failures to a retry topic.** More
  moving parts than committing after success, for a single-writer projector.

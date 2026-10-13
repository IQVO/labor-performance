---
id: 0032-outbox-relay-attempt-cap
slug: /adr/0032-outbox-relay-attempt-cap
title: "32. Outbox relay attempt cap (OUTBOX_MAX_ATTEMPTS)"
sidebar_label: "32. Outbox relay attempt cap"
description: "ADR 0032 — the transactional-outbox relay stops claiming a row after OUTBOX_MAX_ATTEMPTS failed sends (default 20, 0 = unlimited), logs it once at ERROR and leaves it in outbox_events for an operator to re-arm. A capped row is overtaken by later rows with the same key."
---

# 32. Outbox relay attempt cap (OUTBOX_MAX_ATTEMPTS)

## Status

Accepted. Amends [ADR 0010](./0010-transactional-outbox.md) (the relay's
retry rule). Fleet gap fix D12 of 2026-10-12; the semantics are copied
from the fleet's reference relay in inventory-storage so every context's
relay behaves the same way.

## Context

The relay (`internal/adapters/outbound/postgres/outbox_relay.go`) claims
unpublished `outbox_events` rows in id order and sends them one at a
time. On the first failed send it records `attempts + 1` and
`last_error` on that row, commits what was already sent and stops the
pass, so a later event for the same key never overtakes a failed earlier
one (ADR 0010).

That rule has no exit. A row the broker will never accept (an oversized
message, a topic that cannot be created, a value the broker rejects) is
claimed first on every pass, fails again and stops the pass again. Every
event behind it, for every key, waits forever: `workforce-management`
and the analytics projector stop receiving anything, and the only signal
is a climbing `labor_performance.outbox.lag_seconds`.

## Decision

1. `postgres.WithMaxAttempts(n)` is a relay option. With `n > 0` the
   claim query adds `AND attempts < n`, so a row that has failed `n`
   times is no longer claimed. `n = 0` (the option's zero value) keeps
   the old unbounded behaviour.
2. `cmd/labor` always passes it, from `OUTBOX_MAX_ATTEMPTS` (default
   `20`, `0` = unlimited; a negative or non-integer value logs a warning
   and uses the default). The chart exposes it as
   `config.outboxMaxAttempts`.
3. The failed send that brings a row to the cap is logged once at ERROR,
   after the attempt is committed, with `outbox_id`, `topic`,
   `event_type`, `key`, `attempts` and `max_attempts`.
4. The row is never deleted or moved. It stays in `outbox_events` with
   `attempts` and `last_error` for inspection. An operator re-arms it
   after fixing the cause:
   `UPDATE outbox_events SET attempts = 0 WHERE id = ...`. The
   housekeeping sweeper (ADR 0023) only deletes published rows, so it
   never removes an excluded row.
5. A fitness test (`TestOutboxRelaysPassAttemptCap` in
   `internal/architecture`) fails when a non-test file under `cmd/` calls
   `NewOutboxRelay` without `WithMaxAttempts`, so the cap is always a
   deliberate setting and never the unlimited default by omission.

The fleet's existing dead-letter convention for consumers (a `.dlq`
topic, ADR 0017) is not used here: a row that cannot be sent to Kafka
usually cannot be sent to a DLQ topic on the same broker either, and the
outbox table already is a durable place to keep it.

## Consequences

- **Per-key ordering is given up for a capped row.** Once a row is
  excluded, later rows with the same key are claimed and published. A
  consumer can therefore see that key's newer events before the capped
  one, if it is ever re-armed and sent. This is the deliberate trade: one
  poison row must not stop the whole relay forever. The re-armed row
  keeps its original CloudEvents `id`, so a consumer that dedupes on it
  (the analytics projector does, ADR 0031) treats a resend as a no-op.
- A transient broker outage longer than 20 relay passes (about 20 s at
  the default `OUTBOX_RELAY_INTERVAL` of 1 s) also excludes the head
  row. The ERROR line and the outbox lag gauge make that visible, and
  re-arming is one `UPDATE`. Raise `OUTBOX_MAX_ATTEMPTS` (or set `0`) in
  environments where long broker outages are normal.
- `labor_performance.outbox.lag_seconds` still counts an excluded row
  (it is unpublished), so the existing "Outbox stuck" alert keeps firing
  until the row is re-armed or removed.
- `markPublished` also increments `attempts`, so a published row's
  `attempts` counts its successful send too. Only unpublished rows are
  claimed, so the cap only ever compares failed sends.

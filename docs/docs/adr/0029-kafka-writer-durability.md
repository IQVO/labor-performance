---
id: 0029-kafka-writer-durability
slug: /adr/0029-kafka-writer-durability
title: "29. Kafka writer durability: RequireAll acks + 10ms BatchTimeout"
sidebar_label: "29. Kafka writer durability"
description: "ADR 0029 — every synchronous kafka-go Writer in this service (the two outbound publishers, the outbox relay sink, and the inbound consumer's DLQ writer) sets RequiredAcks=RequireAll and BatchTimeout=10ms, replacing kafka-go's RequireNone/1s defaults that silently risked at-most-once delivery and capped throughput at ~1 msg/s."
---

# 29. Kafka writer durability: RequireAll acks + 10ms BatchTimeout

## Status

Accepted — implemented prior to this record (see
`internal/adapters/outbound/kafka/writer_config.go`'s
`syncWriterRequiredAcks`/`syncWriterBatchTimeout` and the matching DLQ
writer fields in `internal/adapters/inbound/kafka/consumer.go`); this
closes the documentation gap found by the 2026-10-04 ADR audit (the
decision existed only as code comments, never as its own ADR, and the DLQ
writer had not yet adopted it — closed in the same change that added this
record).

## Context

`kafka-go`'s `Writer` defaults are tuned for throughput over durability and
latency: `RequiredAcks` defaults to `RequireNone` (`WriteMessages` returns
success as soon as the client has sent the bytes, without waiting for the
broker to actually store them), and `BatchTimeout` defaults to `1s` (a
synchronous, one-message-per-call writer — every writer in this service —
sits idle for up to a full second waiting to fill a batch that will never
fill, because there is only ever one message).

Both defaults caused real, observed problems in this fleet:

- **`RequireNone` is silently at-most-once.** A probe against a fresh
  multi-partition topic lost whole batches in 3 of 6 runs; the
  transactional outbox ([ADR 0010](./0010-transactional-outbox.md)) marked
  rows `published_at` that the broker never durably stored.
- **The 1s `BatchTimeout` capped throughput at ~1 event/s per writer.** A
  backlog of queued events took hours to drain while each write waited out
  the batch window for no reason — there was never a second message to
  batch with.

## Decision

**Every synchronous `kafkago.Writer` this service constructs sets
`RequiredAcks: kafkago.RequireAll` and `BatchTimeout: 10 * time.Millisecond`:**

- `internal/adapters/outbound/kafka`'s `IntegrationPublisher`,
  `AnalyticsPublisher`, and `RelaySink` (`syncWriterRequiredAcks` /
  `syncWriterBatchTimeout`).
- `internal/adapters/inbound/kafka`'s dead-letter writer
  (`dlqRequiredAcks` / `dlqBatchTimeout`) — a DLQ write is the last copy of
  a message that already failed every in-process retry; `RequireNone`
  there would silently drop it rather than land it durably on the DLQ
  topic.

`RequireAll` waits for every in-sync replica to acknowledge, not just the
leader; on this fleet's single-broker kind clusters it is equal to the
leader's own ack, so there is no latency surprise in that environment, and
it is the durable choice the moment a cluster runs with real replication.
10ms keeps a real burst of messages batched under load while flushing a
lone message almost immediately — not "no batching," just "don't wait a
full second for a batch that will never fill."

A source-level fitness test
(`internal/adapters/outbound/kafka/writer_batchtimeout_test.go`'s
`TestEverySyncWriterSetsBatchTimeout`, and the inbound package's own
`TestNewConsumerForTopic_DLQWriterAutoCreatesTopic`) parses every
`kafkago.Writer{...}` literal in each package and fails the build if either
field is left unset, so a new writer added later cannot silently regress to
the library defaults.

## Consequences

- No message this service produces (analytics, integration, or
  dead-lettered) can be marked "sent" without the broker actually durably
  storing it.
- A large backlog drains at real broker throughput, not capped at ~1
  msg/s/writer.
- `RequireAll` is marginally slower per write than `RequireNone` on a
  multi-replica cluster (it waits for every in-sync replica, not just the
  leader) — accepted as the correct trade for a service whose entire job
  is "report this fact accurately," not high-frequency low-stakes
  telemetry.

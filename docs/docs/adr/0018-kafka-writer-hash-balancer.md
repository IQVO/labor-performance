---
id: 0018-kafka-writer-hash-balancer
slug: /adr/0018-kafka-writer-hash-balancer
title: 18. Key-aware Hash balancer on every outbound Kafka writer
sidebar_label: 18. Kafka writer Hash balancer
description: "ADR 0018 -- every kafkago.Writer in internal/adapters/outbound/kafka (IntegrationPublisher, AnalyticsPublisher, RelaySink) was configured with &kafkago.LeastBytes{}, a balancer that ignores Message.Key entirely and routes purely by cumulative byte volume. Message.Key has always been set correctly (AssociateId / TaskType), but LeastBytes silently discarded it for partition routing, so same-key ordering was never actually guaranteed once warehouse-infra PR #42 scaled every business topic from 1 to 8 partitions. Fixed by switching every writer's Balancer to &kafkago.Hash{} (FNV-1a over Key), the same fix applied fleet-wide in order-management PR #111, inventory-storage PR #102, and workforce-management PR #106. Proven with a real-broker Testcontainers test against an 8-partition topic."
---

# 18. Key-aware Hash balancer on every outbound Kafka writer

## Status

**Accepted** -- implemented in the same change that introduced this
record. Mirrors the fleet-wide fix already merged in order-management
(PR #111), inventory-storage (PR #102), and workforce-management
(PR #106).

## Context

Every `kafkago.Writer` this service constructs in
`internal/adapters/outbound/kafka/` --
`IntegrationPublisher.Writer` (`NewIntegrationPublisher`),
`AnalyticsPublisher.Writer` (`NewAnalyticsPublisher`), and
`RelaySink.Writer` (`NewRelaySink`) -- was configured with
`Balancer: &kafkago.LeastBytes{}`.

`kafkago.LeastBytes` balances purely by the cumulative number of bytes
written to each partition so far (`len(msg.Key) + len(msg.Value)`); it
never hashes or otherwise inspects the CONTENT of `Message.Key` to pick
a partition. This is the opposite of real Kafka's own default
partitioner (a hash of the key) and of `kafka-go`'s own
`Hash`/`ReferenceHash`/`CRC32Balancer` types, which DO route on the
key's bytes -- but `kafka-go`'s `Writer` does not switch to one of
those automatically just because a message happens to carry a non-nil
key. `Balancer` is an independent configuration knob, and it must
ITSELF be key-aware for "same key -> same partition" to hold.

This service's `Message.Key` has, in every one of these three writers,
always been set correctly:

- `IntegrationPublisher.Encode` (via `integrationData`) keys every
  `TaskPerformanceRecorded` message by `AssociateId`, so a downstream
  per-associate consumer (workforce-management, ADR 0013) can rely on
  in-order delivery.
- `AnalyticsPublisher.Encode` (via `marshalData`) keys every message by
  `TaskType`, so the analytics projector applies all events for one
  report dimension in publish order.
- `RelaySink.Send` forwards already-`Key`-populated `Encoded` messages
  from the transactional outbox (ADR 0010) unchanged.

None of that mattered for partition placement, because `LeastBytes`
discarded the key at the one place that actually decides which
partition a message lands on. This was invisible for as long as every
business topic ran at 1 partition (there is nowhere else for a message
to go, so "same partition" trivially held) -- see warehouse-infra
PR #42, which took every business topic, including
`warehouse.labor-performance.events` and
`warehouse.labor-performance.analytics`, from 1 to 8 partitions. From
that point on, a consumer could observe two events for the SAME
associate (or the same task type) out of order, because they were free
to land on different partitions and be consumed by different partition
readers at different rates.

This was found by a fleet-wide code audit following the identical fix
already merged in three sibling repos: order-management (PR #111),
inventory-storage (PR #102), and workforce-management (PR #106) --
see `references/kafka-go-key-balancer-mismatch.md` in this fleet's
ops skill for the general shape of the bug. A fake-writer unit test
(this repo already had one per publisher, asserting `msg.Key` is set
correctly) is necessary but NOT sufficient to catch this: it proves
the key is set, not that the `Writer`'s `Balancer` actually uses it to
route. Only a real-broker, multi-partition integration test can prove
that.

## Decision

Switch every writer's `Balancer` in
`internal/adapters/outbound/kafka/{integration_publisher.go,analytics_publisher.go,relay_sink.go}`
from `&kafkago.LeastBytes{}` to `&kafkago.Hash{}` (FNV-1a over
`Message.Key`, the balancer kafka-go documents as
Sarama-hash-partitioner-compatible). No `Message.Key`-setting logic
changes -- it was already correct; only the balancer that decides what
to DO with that key changes.

Proven end to end by extending the existing real-Kafka Testcontainers
integration test file
(`integration_publisher_integration_test.go`) with a new test,
`TestIntegrationPublisher_KeysMessagesForSameAssociateOntoTheSamePartition`,
mirroring order-management PR #111's reference shape: create an
8-partition topic (matching warehouse-infra PR #42's real scaleup),
publish 3 `TaskPerformanceRecorded` events for one `AssociateId` and 1
for a different associate, read every partition back explicitly, and
assert all 3 same-associate messages land on exactly one partition
while the other associate's message is free to land elsewhere. Also
updated the existing single-partition integration test's inline
`Writer` literal (which pinned `LeastBytes` locally) to `Hash`, so it
stays representative of production wiring.

## Consequences

- `warehouse.labor-performance.events` and
  `warehouse.labor-performance.analytics` now actually deliver
  per-associate and per-task-type ordering guarantees at their real,
  current partition count (8), not only at the 1-partition count they
  were implicitly (and invisibly) relying on before.
- No behavior change to which key is chosen, no wire-format change, no
  consumer-side change required -- this is purely a producer-side
  routing-correctness fix.
- `RelaySink` (the transactional outbox's send path) inherits the same
  fix automatically, since it shares the one `Writer` construction
  site with the other two publishers.
- Matches the balancer choice already accepted fleet-wide in
  order-management (ADR 0027), inventory-storage, and
  workforce-management -- a new writer added to this package should
  default to `&kafkago.Hash{}`, not copy `&kafkago.LeastBytes{}` from
  an older example.

## Alternatives considered

- **`kafkago.ReferenceHash` or `kafkago.CRC32Balancer`.** Both are also
  key-aware. Rejected in favor of `Hash` only for consistency with the
  fleet-wide precedent (order-management PR #111) -- no external
  consumer in this fleet uses Sarama or librdkafka's own partitioner,
  so exact hash-function compatibility with those clients is not a
  requirement here, and picking the same balancer as every sibling
  repo keeps this class of bug trivially greppable fleet-wide.
- **Leaving `LeastBytes` and instead re-deriving ordering some other
  way (e.g. a single-partition topic, or an application-level sequence
  number).** Rejected: warehouse-infra PR #42 deliberately scaled every
  business topic to 8 partitions for throughput/parallelism; reverting
  that or bolting on an application-level ordering scheme would both
  undo real, wanted infrastructure work to work around a producer bug
  that has a direct, minimal fix.

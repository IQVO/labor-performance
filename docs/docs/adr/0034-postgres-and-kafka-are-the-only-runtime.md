---
id: 0034-postgres-and-kafka-are-the-only-runtime
slug: /adr/0034-postgres-and-kafka-are-the-only-runtime
title: "34. Postgres and Kafka are the only runtime"
sidebar_label: "34. Postgres and Kafka only"
description: "ADR 0034 — every cmd binary of labor-performance runs on real Postgres and real Kafka. No in-memory repositories, no log publisher, no direct fan-out, no optional relay or consumer. Mandatory configuration is a boot error naming the variable; /readyz checks Postgres and a Kafka broker; sensors, the contract job and the chart keep it true."
---

# 34. Postgres and Kafka are the only runtime

## Status

**Accepted** (2026-10-19). Supersedes the in-memory fallback, the log
publisher, the direct (non-outbox) Kafka fan-out and the localhost broker
default that [ADR 0001](./0001-hexagonal-ports-and-adapters.md),
[ADR 0007](./0007-analytical-data-product.md),
[ADR 0009](./0009-mcp-inbound-adapter.md),
[ADR 0010](./0010-transactional-outbox.md),
[ADR 0013](./0013-labor-performance-integration-events.md) and
[ADR 0016](./0016-idempotency-key-middleware.md) allowed; each carries a
one-line pointer here. Fleet decision of 2026-10-17, "everything must use real
database and broker", following product-master (their ADR 0010),
workforce-management (their ADR 0038) and facility-layout (their ADR 0039). No
data migration; the CloudEvents envelope
([ADR 0021](./0021-cloudevents-mandatory-event-envelope.md)) and the MCP
surface are unchanged. One contract detail follows from it: `Idempotency-Key`
is now `required: true` in `apis/openapi.yaml`, because the Postgres-backed
check can no longer be absent.

## Context

The binaries could start in states that cannot serve the cluster, and did so
silently:

- `cmd/labor` and `cmd/mcp` without `DATABASE_URL` logged
  `database url not configured; using in-memory adapters` and served from
  their own empty in-memory repositories: data vanished on restart, the MCP
  server saw none of the API's data, there was no transactional outbox, no
  sweeper, no outbox-lag gauge and the Idempotency-Key check was skipped.
- `EVENT_PUBLISHER` defaulted to the log publisher (and the chart default was
  `config.eventPublisher: "log"`): nothing reached
  `warehouse.labor-performance.events` or `.analytics`, while `/readyz` said
  ready. With Kafka but without a database the service published directly,
  outside any transaction.
- `KAFKA_BROKERS` defaulted to `localhost:9092` in `cmd/labor` and
  `cmd/labor-projector`, `KAFKA_CONSUMER_GROUP` defaulted to
  `labor-performance`, and the chart's `kafka.enabled: false` default left
  `KAFKA_BROKERS` unset, so the one consumer that is this service's only
  inbound data path dialled `localhost:9092` inside the pod.
- `/readyz` was only a drain gate: ready while Postgres or Kafka was
  unreachable.
- The Schemathesis `contract` job booted the service in memory, so it tested a
  mode that is not deployed.

A fallback that works makes a mis-set environment look like a healthy
deployment.

## Decision

1. **One runtime.** `cmd/labor` and `cmd/mcp` run on real Postgres;
   `cmd/labor` and `cmd/labor-projector` run on real Kafka; the projector and
   `cmd/labor-reports` run on the analytical Postgres. There is no in-memory
   mode, no log sink and no direct publisher: events are written through the
   transactional outbox in the use case's transaction, the relay always runs and
   the `warehouse.fulfillment.events` consumer always runs.
2. **Missing configuration is a boot error that names the variable**, raised
   before telemetry, any connection and the listener, every problem reported at
   once: `DATABASE_URL is required: this context runs on real Postgres`,
   `KAFKA_BROKERS is required: this context runs on real Kafka`,
   `KAFKA_CONSUMER_GROUP is required: this context runs on real Kafka`;
   `cmd/mcp` requires `DATABASE_URL`; `cmd/labor-projector` requires
   `ANALYTICS_DATABASE_URL` and `KAFKA_BROKERS` (no `localhost:9092`).
   `EVENT_PUBLISHER` stays only because the chart and warehouse-infra set it:
   unset and `kafka` are accepted, `log` and anything else stop the boot.
3. **The composition roots build real adapters only.** The `memory` adapters
   stay in `internal/adapters/outbound/memory` as test doubles for unit and BDD
   tests and are never imported by a binary. The wall clock the binaries need
   moved to `internal/adapters/outbound/system`.
4. **`/readyz` tells the truth.** `cmd/labor` answers `200` only while the drain
   gate is open AND Postgres answers a ping AND a Kafka broker answers a
   metadata request. A failed dependency is `503 dependency-unavailable`
   (problem+json) naming it. The verdict is cached 3 s and bounded to 2 s, and
   `/healthz` stays a pure liveness check.
5. **Readiness checks the broker, not the topics.** The metadata request names
   no topic (an empty, non-nil list), on a fresh connection each time; the
   broker auto-creates topics on the first write (fleet convention, the writers
   set `AllowAutoTopicCreation`) and this service never creates topics through
   an admin API. A not-ready pod receives no traffic and so could never make
   the first write: requiring the topics would deadlock a cold cluster.
6. **Sensors keep it true.** `TestRuntimeBinaryHasNoInMemoryAdapters`
   (`internal/architecture`, with `TestRuntimeBinarySensorBites`) fails when a
   non-test file under `cmd/**` imports `internal/adapters/outbound/memory`.
7. **The deployment mirrors it.** The chart fails the render, naming the value,
   without `database.url`/`database.existingSecret`, `kafka.brokers` or
   `config.kafkaConsumerGroup`, with `config.eventPublisher` other than `kafka`,
   with `kafka.enabled=false`, with an `EVENT_PUBLISHER` or `*_MODE` entry in
   `extraEnv`, and with a grace period under 20 s. `KAFKA_BROKERS`,
   `KAFKA_CONSUMER_GROUP` and `DATABASE_URL` are always rendered (api, mcp and
   projector). `kafka.enabled` is still accepted (warehouse-infra sets it to
   `true`) but is no longer a switch.
   `charts/labor-performance/tests/warehouse-infra-values.yaml` mirrors what
   warehouse-infra sets and `render_test.sh` renders it, so infra needs no
   change.
8. **The contract job runs the real service.** `docker-compose.yml` now carries
   `confluentinc/confluent-local:7.6.1` next to `postgres:16`; the CI `contract`
   job uses the same two images as service containers (the broker advertises
   `localhost:9092`, the service-container port) and `scripts/contract-test.sh`
   requires `DATABASE_URL` and `KAFKA_BROKERS`. The old in-memory boot seeded
   nothing and the new boot starts on an empty database equally. A Schemathesis
   hook (`scripts/contract_hooks.py`) gives every request that carries the
   header a fresh `Idempotency-Key`, since a reused key with a different body is
   by design a `422`.
9. **A NUL is a client error.** A fuzzed U+0000 in a string reached a Postgres
   text parameter, which aborts the transaction (SQLSTATE 22021, then 25P02 for
   the next statement) and surfaced as `500` on the real runtime; the in-memory
   adapters never noticed (found by facility-layout's contract run on real
   Postgres, ADR 0039 there). `rejectNUL`
   (`internal/adapters/inbound/http/nul.go`) answers a NUL in a body with
   `400 malformed-request-body` before the Idempotency-Key middleware records
   anything, and a NUL in the path with `404`. The contract script passes
   `--generation-allow-x00 false`, because OpenAPI 3.0 cannot express "no
   U+0000" and the rejection would otherwise read as "API rejected
   schema-compliant request"; the behaviour is pinned by `TestRejectNUL` and
   `TestNUL_IsAClientErrorAndDoesNotConsumeTheKey` (real Postgres).

## Consequences

- A mis-set environment fails at boot with a message that names the missing
  variable; it no longer degrades into a pod that is up, empty and silent.
- Local `go run ./cmd/labor` needs a Postgres and a Kafka broker:
  `docker compose up -d --wait` provides both. Fast feedback stays in unit and
  BDD tests over the memory doubles.
- A broker outage takes the api pods out of the Service (readiness `503`) where
  the probe is `/readyz`; liveness is unaffected, so nothing restarts. Writes
  that were accepted stay in the transactional outbox and are relayed when the
  broker returns ([ADR 0010](./0010-transactional-outbox.md),
  [ADR 0032](./0032-outbox-relay-attempt-cap.md)).
- Tests: `TestRunOnRealPostgresAndKafka` (`cmd/labor/e2e_integration_test.go`,
  testcontainers) boots the real `run()` with nothing but the deployment's
  environment, drives REST and Kafka only (a standard over REST, a
  `TaskCompleted` CloudEvent into `warehouse.fulfillment.events`), asserts the
  Postgres rows, the CloudEvents envelope on both output topics,
  `Idempotency-Key` replay, a NUL body, a redelivered `TaskCompleted` (recorded
  once), an in-process restart (no loss, no duplicate) and `/readyz` following
  the dependencies. Every boot error is a table test
  (`cmd/labor/bootconfig_test.go`, `cmd/labor-projector/config_test.go`,
  `cmd/mcp/wiring_test.go`).

## Out of scope

labor-performance makes no REST or MCP call to a sibling context (ADR 0015), so
no `*_MODE=permissive|http` switch exists here; the one that points at this
service (`LABOR_PERFORMANCE_MODE` in workforce-management) is theirs to retire.
Not changed here and left as follow-ups: warehouse-infra's readiness probe
still points at `/healthz` (switching it to `/readyz` is an infra change); the
projector's admin listener has `/healthz` only (no `/readyz`); the projector's
analytics consumer group is the `AnalyticsConsumerGroup` constant, not an
environment variable; `cmd/labor-reports` is unchanged (it already requires
`ANALYTICS_DATABASE_URL` and has no Kafka dependency).

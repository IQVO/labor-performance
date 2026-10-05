---
id: 0027-bootretry-and-startup-probe
slug: /adr/0027-bootretry-and-startup-probe
title: "27. Boot-time dial retry (bootretry) + Kubernetes startupProbe"
sidebar_label: "27. bootretry + startupProbe"
description: "ADR 0027 — every binary that dials Postgres/Kafka at boot uses internal/adapters/outbound/bootretry's jittered exponential backoff instead of crash-looping on a cold-start race, paired with a Kubernetes startupProbe that holds liveness/readiness off until the binary is actually up."
---

# 27. Boot-time dial retry (bootretry) + Kubernetes startupProbe

## Status

Accepted — implemented as part of [ADR
0017](./0017-kafka-dlq-and-graceful-shutdown.md)'s Phase 2 resilience work;
this is its own short record because the audit found the mechanism
undocumented as a decision in its own right (only mentioned in passing in
ADR 0017's consequences).

## Context

On a fresh cluster rollout (or any pod restart racing a Postgres/Kafka
restart), this service's dependencies are not guaranteed to be dial-able
the instant the binary starts. Without a retry, `cmd/labor` (and every
other composition root that dials Postgres) would exit non-zero on the
first failed connection attempt, and Kubernetes would crash-loop it with
exponential backoff *between pod restarts* — slower and noisier than
retrying *inside* one running process.

## Decision

**Every composition root dials its Postgres/Kafka dependencies through
`internal/adapters/outbound/bootretry`'s jittered exponential backoff,
bounded by a maximum elapsed time, instead of failing fast on the first
error — paired with a Kubernetes `startupProbe` so the orchestrator does
not mark the pod live/ready until the retry loop actually succeeds.**

- `bootretry.Dial` (or the equivalent wrapper used by each root) retries a
  dial function with jittered exponential backoff; a permanent
  configuration error (e.g. an unparseable DSN) is not retried, only a
  transient dial failure is.
- `charts/labor-performance/templates/deployment.yaml`'s `startupProbe`
  (templated from `.Values.startupProbe`) gives the retry loop room to
  succeed before `livenessProbe`/`readinessProbe` start counting failures
  — a pod that is still inside its boot-retry window is neither killed nor
  routed traffic.

## Consequences

- A cold-start race (Postgres/Kafka not yet accepting connections when this
  service's pod starts) self-heals inside one process restart instead of
  requiring several crash-loop cycles.
- The retry window is bounded: a genuinely broken configuration still fails
  the pod eventually, rather than retrying forever and masking a real
  outage from alerting.
- `startupProbe`'s own timeout must stay longer than `bootretry`'s maximum
  elapsed time, or Kubernetes kills the pod mid-retry — the two are coupled
  configuration, not independent knobs.

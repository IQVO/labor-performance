---
id: 0017-kafka-dlq-and-graceful-shutdown
slug: /adr/0017-kafka-dlq-and-graceful-shutdown
title: 17. Kafka consumer dead-letter queue and graceful shutdown hardening
sidebar_label: 17. Kafka DLQ and graceful shutdown
description: "ADR 0017 -- Phase 2 resilience for labor-performance, ported from order-management's ADR 0025 for the DLQ and graceful-shutdown pieces only: this service has no sync outbound HTTP calls to any sibling bounded context (ADR 0003/0015's blanket boundary), so there is no circuit breaker to add. Consumer.handleMessage now retries handleFulfillmentEvent in-process (cenkalti/backoff/v4, up to 3 attempts) before dead-lettering an exhausted/poisoned message to warehouse.fulfillment.events.dlq and committing its offset; graceful shutdown gains a readiness-flip-first sequence backing a new GET /readyz, distinct from the liveness-only GET /healthz."
---

# 17. Kafka consumer dead-letter queue and graceful shutdown hardening

## Status

**Accepted** -- implemented in the same change that introduced this
record. Ported from order-management's `RepromiseConsumer`
dead-letter-queue design and graceful-shutdown sequencing (PR #107,
ADR 0025), for the DLQ and shutdown-hardening pieces only. This
service has no per-dependency circuit breaker work to do -- see
"Why no circuit breaker" below.

## Context

Before this change, this service's inbound Kafka consumer
(`internal/adapters/inbound/kafka/consumer.go`, consuming
`warehouse.fulfillment.events` for `TaskCompleted`, ADR 0003) had no
dead-letter handling at all: `handleMessage` logged and unconditionally
committed past ANY error from `handleFulfillmentEvent` -- including a
genuine infrastructure failure (a Postgres hiccup, a lost connection
mid-`RecordTaskPerformance.Execute`), which meant a single transient
blip silently dropped a real, otherwise-valid `TaskCompleted` event
with no record it ever happened, no retry, and no operator-visible
signal. There was also no bounded in-process retry to let a transient
failure heal itself before falling back to any such handling.

Graceful shutdown (`cmd/labor/main.go`) already had a
`signal.NotifyContext` + `httpServer.Shutdown` + relay-stop sequence,
but no readiness-flip step (a Kubernetes `readinessProbe` pointed at
the same `/healthz` liveness endpoint the whole time, so there was no
way to signal "stop routing new traffic" separately from "the process
is alive"), no bounded wait for the Kafka consumer's own in-flight
message to actually finish and commit before the process could exit,
and no `terminationGracePeriodSeconds` in the Helm chart to give any of
that room to run before Kubernetes SIGKILLs the pod.

### Why no circuit breaker

order-management's ADR 0025 also added per-dependency circuit breakers
around its sync outbound HTTP clients (`inventorystorage.Client`,
`productclassification.Client`). This service has **no sync outbound
HTTP calls to any sibling bounded context at all** -- ADR 0002/0003
scope it as a pure Kafka choreography consumer of
`fulfillment-execution`'s `TaskCompleted`, and ADR 0015 restates the
same blanket boundary explicitly when the travel-component feature
made a live `facility-layout` lookup look tempting: "No REST dependency
in either direction with any sibling context." `internal/architecture`'s
`TestNoSiblingContextOutboundCalls` fitness test enforces this at build
time. There is therefore no dependency edge to wrap in a breaker --
this ADR covers only the DLQ and graceful-shutdown pieces of the fleet
Phase 2 resilience plan.

## Decision

### 1. Dead-letter queue for the inbound Kafka consumer

`Consumer.handleMessage` now retries `handleFulfillmentEvent`
in-process, with jittered exponential backoff (`cenkalti/backoff/v4`,
100ms-2s), up to `maxHandlerAttempts` (3) total attempts, via the new
`handleWithRetry`. A transient infrastructure error (a momentary
Postgres hiccup, a lost connection) heals itself within this budget
without ever reaching the DLQ.

Once all 3 attempts are exhausted, the message is published -- raw
payload byte-for-byte, plus `x-dlq-source-topic`/`x-dlq-error`/
`x-dlq-failed-at` headers carrying replay/debugging context -- to
`<source-topic>.dlq` via a `*kafkago.Writer` the consumer now owns
(`Consumer.dlqWriter`, closed alongside the reader in `Close`), and
**the offset is committed anyway**: one poison message must never
permanently block every other associate's task behind it on the same
partition. This is logged at WARN level
(`"exhausted retries, sending to dead-letter topic"`) with enough
context (topic, dlq_topic, event_id, event_type, attempts, error) to
be an alert-worthy signal and support a manual replay tool, not a
silent drop.

`NewConsumerForTopic` derives the DLQ topic as
`<its own source topic>+".dlq"` (never a fixed constant) -- an isolated
integration-test topic automatically gets its own isolated DLQ topic
for free, exactly mirroring the existing constructor's convention of
letting tests isolate the source topic/group without touching
production names. `dlqPublish` guards against a nil `dlqWriter`, so the
several existing unit tests that construct a bare `Consumer{...}`
directly (calling `handleFulfillmentEvent` without ever routing through
`handleMessage`) keep compiling and passing unchanged.

Unlike order-management's `RepromiseConsumer`, this service's
idempotency gate (`ports.ProcessedEvents.MarkProcessed`, called from
inside `RecordTaskPerformance.Execute`) has no
`ErrConcurrentModification`-shaped sentinel that needs its own
leave-uncommitted-for-redelivery branch -- every non-nil error from
`handleFulfillmentEvent` is a candidate for the same retry-then-DLQ
path.

Proven end to end with a real Testcontainers Kafka
(`consumer_dlq_integration_test.go`,
`TestConsumer_PoisonMessage_GoesToDeadLetterTopicWithoutBlockingPartition`):
a message whose handler is made to always fail (via a
`ports.ProcessedEvents` wrapper that unconditionally errors for one
specific `event_id` -- injected at `MarkProcessed`, the very first call
`RecordTaskPerformance.Execute` makes, so it never has a side effect to
undo across the 3 retry attempts) lands on the `.dlq` topic after
exactly 3 attempts, with the raw original JSON payload and the
error-context headers intact, and -- published right after the poison
message on the SAME topic -- a well-formed `TaskCompleted` for a real
associate is processed without delay, proving the partition was never
blocked.

### 2. Graceful shutdown hardening

`cmd/labor/main.go`'s pre-existing `signal.NotifyContext` +
`httpServer.Shutdown(shutdownCtx)` + relay-stop sequence is extended,
not rewritten, into this order:

1. **Flip readiness to not-ready FIRST**
   (`inboundhttp.Readiness.SetNotReady`, backing a new `GET /readyz`,
   distinct from the pre-existing `GET /healthz` which stays a pure
   liveness signal and is never flipped by shutdown) -- before anything
   else stops, so a Kubernetes `readinessProbe` polling `/readyz` has a
   window to observe the flip and stop routing NEW traffic to this pod
   before step 2 ever closes the listener.
2. **Stop accepting new HTTP connections and drain in-flight requests**
   -- `httpServer.Shutdown(shutdownCtx)`, unchanged from before.
3. **Stop the outbox relay and the Kafka consumer's loop cleanly** --
   cancel each one's own context (no NEW work is picked up after this)
   and WAIT, bounded by the same `shutdownCtx`, for each goroutine to
   actually finish in-flight work -- for the Kafka consumer, this means
   a message already being handled runs to completion INCLUDING its
   offset commit (`handleMessage`'s commit-before-return shape) before
   `Run` returns -- rather than merely firing the cancel and moving on.
   This is the "final offset commit" guarantee: no message is left
   processed-but-uncommitted by an abrupt stop.
4. **Close the pgx pool LAST** -- `persistence.close()`/
   `closePublisher()`/`consumer.Close()` are `defer`red near the TOP of
   `run()`, so by `defer`'s LIFO order they run AFTER every
   consumer/relay goroutine has already stopped touching the pool, not
   before. `consumer.Close()` closes both the Kafka reader and the new
   DLQ writer.

`Readiness`'s zero value (and a `nil *Readiness`) is always ready --
every existing test and any caller that predates this type behaves
exactly as before.

`charts/labor-performance/values.yaml`'s `readinessProbe` now points at
`/readyz` (was `/healthz`) -- `startupProbe`/`livenessProbe` are
UNCHANGED, still `/healthz`, because liveness must never be flipped by
a graceful drain or Kubernetes would SIGKILL the pod mid-drain instead
of letting it finish. A new `terminationGracePeriodSeconds: 30` was
added to `charts/labor-performance/templates/deployment.yaml` (via a
new `.Values.terminationGracePeriodSeconds`, previously absent -- a
confirmed gap) -- the HTTP `Shutdown` budget (10s) plus the
relay/consumer stop budget (the same 10s, reused) plus margin for the
`readinessProbe`'s `periodSeconds: 5` to have actually observed the
not-ready flip before traffic fully stops arriving.

## Consequences

- A transient Postgres/infrastructure blip while recording a
  `TaskCompleted` now self-heals within ~2.2s (3 attempts, 100ms-2s
  jittered backoff) instead of silently dropping the event on the
  first failure.
- A genuinely poisoned message (a handler that always fails -- a
  malformed payload shape a future producer never should have sent, or
  a permanently broken downstream dependency) no longer wedges the
  partition forever; it is dead-lettered with full replay context after
  a bounded number of attempts, and every task behind it keeps flowing.
- Operators gain a genuine `/readyz` distinct from `/healthz`: a pod
  mid-graceful-drain now correctly stops receiving new traffic instead
  of racing a closing listener.
- A `SIGTERM` no longer risks losing an in-flight Kafka message's
  offset commit or an in-flight outbox relay pass -- both are now
  waited on (bounded) before the process exits.
- `cenkalti/backoff/v4` and its transitive `x/sys`/`x/exp` (whatever
  `go mod tidy` pulls in) become a direct dependency of this module
  (previously indirect, already present via a dependency of a
  dependency at the SAME version, `v4.3.0`, order-management also
  uses).
- No new failure mode: `dlqPublish`'s own failure (the DLQ broker
  unreachable) still aborts the consume loop rather than silently
  dropping the poison message's context, exactly like a commit failure
  already did before this change.
- This service still makes zero sync outbound HTTP calls to any sibling
  bounded context -- `TestNoSiblingContextOutboundCalls` continues to
  pass unchanged, and nothing in this ADR touches that boundary.

## Alternatives considered

- **A circuit breaker around the Postgres/Kafka adapters themselves**
  (rather than around a sibling-context HTTP call, since there is
  none). Rejected: `gobreaker`-style breakers exist to stop a healthy
  caller from hammering a REMOTE, independently-failing dependency
  during an outage; Postgres and Kafka are this process's OWN
  persistence and messaging infrastructure, already covered by
  `internal/adapters/outbound/bootretry`'s boot-time retry and this
  ADR's own in-process handler retry. Wrapping them in a breaker too
  would add OPEN-state fallback complexity (what does "fall back" even
  mean for "I cannot write my own database row?") with no real
  dependency-isolation benefit order-management's design was solving
  for.
- **Leaving the message uncommitted for redelivery instead of a DLQ**
  (mirroring order-management's `ErrConcurrentModification` branch).
  Rejected as the DEFAULT for every error: this service's idempotency
  gate has no equivalent "someone else already advanced this" sentinel
  -- every failure here is either transient (handled by the in-process
  retry) or a genuine poison message, and leaving a genuine poison
  message uncommitted would re-deliver and re-fail it on every restart
  forever, exactly the partition-blocking failure mode this ADR fixes.

---
name: how-to-add-an-integration-event
description: Publish or consume a cross-service Kafka event: CloudEvents 1.0 type naming, AsyncAPI, transactional outbox, consumer-group rules. Use when touching internal/adapters kafka or outbox code, a publisher/consumer, or apis/asyncapi*.yaml.
---

# How to add an integration event (publish and consume)

Use when asked to publish a new cross-context integration event, or
adjust how this service consumes one from a sibling bounded context.
This fleet's Kafka is ONE broker platform-wide — every design decision
below exists because that shared-broker reality has already caused a
real incident once (wes-work-planning#67).

## This repo's own non-negotiable is stricter than the fleet norm — read this first

Most fleet how-to guides for this topic have a "consuming a sibling
event" section and leave publishing/consuming symmetric. This repo does
not: AGENTS.md's non-negotiable #1 states **"Pure Kafka consumer of
`fulfillment-execution`, nothing else"** — "no Go import from, and no
write access to, `fulfillment-execution` or `workforce-management`. It
never calls either sibling synchronously — choreography, not
orchestration." That is not merely "prefer Kafka over REST for this
integration" (the fleet-wide default); it is an absolute ban on any
outbound REST or MCP call to those sibling contexts, full stop, encoded
so strictly that `internal/architecture/fitness_test.go`'s
`TestNoSiblingContextOutboundCalls` statically fails CI if an outbound
HTTP client package ever appears under `internal/adapters/outbound/`
here. If a future feature seems to need "ask fulfillment-execution for
X at scoring time," that is a signal the feature belongs on the Kafka
event payload instead (get fulfillment-execution to publish X), not a case
for special-casing a synchronous call in this repo. See ADR 0002 and ADR
0003 for the full reasoning.

## Publishing a new integration event

### 1. Is it actually cross-service?

This service publishes to TWO different Kafka topics with different
audiences (see `cmd/labor/main.go`'s `buildEventPublisher` doc comment):
`warehouse.labor-performance.analytics` feeds this service's OWN
analytics projector (ADR 0007) — not cross-service — while
`warehouse.labor-performance.events` (ADR 0013) is the actual
integration surface for sibling contexts. Before adding a new event to
the integration publisher, confirm a sibling context genuinely needs to
react to it; check `docs/docs/adr/0013-labor-performance-integration-events.md`
for who is actually downstream today.

### 2. Envelope: CloudEvents 1.0, structured mode (MANDATORY, ADR 0021)

Every message is a CloudEvents 1.0 event built ONLY through
`internal/adapters/kafka/cloudevents` (`cloudevents.New(Spec{...})`,
which uses `github.com/cloudevents/sdk-go/v2/event` and validates). Never
hand-roll an envelope struct, never add a flat/dual mode. Required
attributes: `specversion=1.0`, `id` (UUID minted once at `Encode`, so the
outbox republishes the same id), `source=/warehouse/labor-performance`,
`type=com.warehouse.wes.labor-performance.<entity>.<EventName>`, `subject`
(aggregate id, never empty), `time` (domain occurred-at), 
`datacontenttype=application/json`,
`dataschema=urn:warehouse:labor-performance:<events|analytics>:<EventName>:v1`.
Every produced message carries `cloudevents.ContentTypeHeader()`
alongside the trace headers. Add the full `Type...` constant to the
`cloudevents` package.

### 3. Implementation

Add the event struct to `internal/domain/<aggregate>/` first if it
doesn't already exist as a domain event — publishing wires an EXISTING
domain event onto Kafka, it doesn't invent a new payload shape at the
adapter layer. In `internal/adapters/outbound/kafka/`:

- Add the case to the publisher's `integrationData`/`marshalData`
  (event name, entity, key, subject, payload map)
- Give the message a partition key that keeps ordering where it matters
  (usually the aggregate id — `AssociateId` or `StandardId` here)
- Use this service's own topic constants from `cloudevents`
  (`cloudevents.TopicLaborPerformanceEvents`/`...Analytics`), never a
  sibling's

### 4. Contract + docs

- Add the message to `apis/asyncapi.yaml` under this service's published
  channel.
- **This repo has no AsyncAPI doc generator, unlike its two OpenAPI
  specs.** Unlike `apis/openapi.yaml`/`apis/openapi-reports.yaml` (which
  DO have `docusaurus-plugin-openapi-docs` wiring and a `gen-api-docs*`
  npm script), `apis/asyncapi.yaml` is described narratively, by hand, in
  `docs/docs/overview.md`, `docs/docs/ecosystem/context-map.md`, and the
  relevant ADR (`0007-analytical-data-product.md` for the analytics
  topic, `0013-labor-performance-integration-events.md` for the
  integration topic) — see `.claude/rules/docs-and-api-drift.md` for the
  full list. There is no generator to catch drift automatically here: a
  new channel or changed payload field means grepping for the topic
  name (`warehouse.labor-performance.events`,
  `warehouse.labor-performance.analytics`) and the event type names
  across `docs/docs/**/*.md*` and hand-updating every page that
  mentions the old shape.

### 5. Test

Add a golden exact-JSON test (see
`internal/adapters/outbound/kafka/golden_cloudevents_test.go`) pinning
every attribute, the full type and the content-type header. Unit test the
marshal shape against a fake `Writer`/`kafkago.Writer` —
never a real broker in a unit test (see the pattern in
`internal/adapters/outbound/kafka/`'s existing publisher tests). If this
event needs an integration test asserting real delivery, it MUST use
testcontainers — see `internal/adapters/inbound/kafka/consumer_integration_test.go`
and the `make integration-kafka-testcontainers` target for the working
recipe (unique topic per test, one shared container per package,
explicit `CreateTopics` + poll for the partition leader before the
first read/write). This fleet's CI `integration` job provisions Postgres
ONLY, never Kafka, so a skip-gated `KAFKA_BROKERS` test or a hardcoded
`localhost:9092` silently proves nothing there.

## Consuming an integration event from a sibling context (this service's actual, live example)

This service's real production consumer is exactly this shape:
`internal/adapters/inbound/kafka/consumer.go` subscribes to
`warehouse.fulfillment.events` (a topic shared/fan-out with
`wes-work-planning`), decodes with `cloudevents.Decode` (anything that is
not a valid CloudEvent goes to the DLQ, never parsed), and reacts only to
the FULL type `com.warehouse.wes.fulfillment-execution.task.TaskCompleted`,
feeding it into `RecordTaskPerformance`. Read that
file's own doc comment and ADR 0003 end to end before writing a new
consumer — they are the canonical worked example for this whole
section, not just a fleet-wide abstraction.

### 1. Never import the sibling's Go packages

`consumer.go`'s `taskCompletedData` struct is this context's own private
mirror of fulfillment-execution's wire shape — hand-verified against
fulfillment-execution's actual `internal/adapters/outbound/kafka/publisher.go`
`TaskCompletedData` struct, not assumed. This service never adds a Go
module dependency on `fulfillment-execution` or `workforce-management`;
`internal/architecture/fitness_test.go`'s `TestNoSiblingContextOutboundCalls`
enforces the outbound half of this statically, and the hexagonal
dependency-rule fitness test (`architecture_test.go`) would catch a
stray sibling import anywhere in this module.

### 2. Choose the right consumer-group pattern — this is the part that bites

Two DIFFERENT correct patterns exist. Picking the wrong one is THE most
common integration-event mistake in this fleet, learned from a real
incident (wes-work-planning#67: a local dev/test harness process joined
the SAME broker's SAME group as a live in-cluster Deployment, and
Kafka's rebalance protocol starved one of the two members silently).

**Pattern A — long-lived, single-instance consumer group (a named,
configurable default).** This is what this service actually uses.
`cmd/labor/main.go` resolves the group id via
`getenv("KAFKA_CONSUMER_GROUP", "labor-performance")` — never a bare
inline string literal. `internal/architecture/fitness_test.go`'s
`TestKafkaConsumerGroupNeverHardcodedInline` statically greps for an
inline `GroupID: "literal"` pattern and fails CI if one appears; the
env-var indirection through `getenv` is what satisfies it.

**Pattern B — per-process-unique consumer group (a generated id).** Use
only when a consumer rebuilds a complete read model from a topic's FULL
history on every start (an event-sourced local cache, not a work
queue). This service does not currently have one of these — if you add
one, the group id must be unique per process instance
(hostname+PID+timestamp), never a fixed shared string, because Kafka's
committed-offset resume semantics would otherwise hand a brand-new
process an EARLIER instance's already-consumed offset, marking it
"ready" having replayed nothing.

**Never do this** (the actual incident, applied to this repo): running
`go run ./cmd/labor` locally against the same broker as a deployed
`labor-performance` pod, both with the default `KAFKA_CONSUMER_GROUP`
unset, silently starves one of the two `TaskCompleted` consumers. Always
pass a distinct `KAFKA_CONSUMER_GROUP` for any ad hoc local run against
a shared cluster broker.

### 3. Readiness gate, if this consumer backs a local cache

This service's actual consumer does NOT back a local cache — it writes
straight through `RecordTaskPerformance` into Postgres/in-memory repos
per message, so there is no replay-then-become-ready gate here. If a
future consumer in this repo ever does rebuild a cache from full topic
history, expose a `Ready()` gate the health check consults and use the
per-process-unique group pattern above, which sidesteps the whole class
of readiness-deadlock bug that a shared, already-caught-up group would
otherwise hit.

### 4. Idempotency: gate on the CloudEvents id, not the payload's natural key

`RecordTaskPerformance`'s idempotency (`ports.ProcessedEvents`) is keyed
on the CloudEvents `id`, not `TaskId` — a `TaskId` could in
principle be reused after a very long time. Unlike
`workforce-management`'s use of the identically-shaped pattern (which
gates only an additive analytics side-projection), here it gates the
ENTIRE OLTP write path, since consuming `TaskCompleted` IS this
service's whole job (ADR 0003). Mirror this exactly for any new
consumer-driven use case in this repo — check `ProcessedEvents` before
doing anything else, inside the same `UnitOfWork.Execute` scope as the
Save.

## Verify before opening the PR

```bash
make check-all    # includes arch-test — will catch a sibling-package
                   # import or a hardcoded consumer-group literal
```

Prove any new fitness-test-adjacent behavior actually matters by running
`make integration-kafka-testcontainers` against a real (containerized)
broker, not just the fake-reader unit test.

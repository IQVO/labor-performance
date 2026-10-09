# Labor Performance

> **⚠️ Study project.** This repository is an educational exercise in
> Domain-Driven Design applied to warehouse management/execution systems. It
> follows real industry-standard patterns and terminology (WMS/WES/WCS,
> CloudEvents 1.0 envelopes, RFC 7807, hexagonal architecture) but is
> **not a production system** and is **not affiliated with, endorsed by, or
> representative of any real-world
> company**.

Engineered labor standards ("a PICK should take 45s") and
actual-vs-standard performance scoring ("this associate's last PICK took
52s — 87% of standard") for the `warehouse-systems` fleet — the eighth
bounded-context Go service in this fleet, after `order-management`,
`inventory-storage`, `wes-work-planning`, `workforce-management`,
`fulfillment-execution`, `facility-layout`, and `warehouse-ops-agent`.

📚 **Full documentation site:** https://iqvo.github.io/labor-performance/
(includes the ddd-crew DDD artifact pack — canvases, context map,
EventStorming, class/ER/sequence diagrams — under *DDD artifacts*).

## Documentation

Published at https://iqvo.github.io/labor-performance/ (sources under
[`docs/docs/`](docs/docs/)):

| Area | Pages |
| --- | --- |
| Overview | [Introduction](docs/docs/overview.md) · [Architecture](docs/docs/overview/architecture.md) · [Quickstart](docs/docs/overview/quickstart.md) |
| Operations | [Configuration](docs/docs/operations/configuration.md) · [Runbook](docs/docs/operations/runbook.md) · [Observability](docs/docs/operations/observability.md) · [Troubleshooting](docs/docs/operations/troubleshooting.md) |
| Development | [Testing](docs/docs/development/testing.md) |
| Domain | [Domain vision](docs/docs/business-context/domain-vision.md) · [Subdomain classification](docs/docs/ddd/subdomain-classification.md) · [Use cases](docs/docs/ddd/use-cases.md) · [DDD artifacts](docs/docs/ddd/ddd-artifacts.md) |
| Ecosystem | [Integration](docs/docs/ecosystem/integration.md) · [Context map](docs/docs/ecosystem/context-map.md) |
| MCP | [MCP tools](docs/docs/mcp/tools.md) · [Governance charter](docs/docs/mcp/governance-charter.md) |
| Decisions | [ADR index](docs/docs/adr/about.md) (31 records) |
| APIs | [`apis/openapi.yaml`](apis/openapi.yaml) · [`apis/openapi-reports.yaml`](apis/openapi-reports.yaml) · [`apis/asyncapi.yaml`](apis/asyncapi.yaml) |

## Why this context exists

Competitor research (Manhattan Active Labor Management, Blue Yonder
Workforce & Labor Management) converges on this as a distinct product
capability, not a sub-feature of shift/schedule planning. Neither
`workforce-management` (which explicitly "stops at the path boundary" and
never links an associate to a task) nor `fulfillment-execution` (which
follows a strict "Task/Station only" design discipline) is the right home
for it. See
[ADR 0002](docs/docs/adr/0002-new-bounded-context-not-extension-of-workforce-or-fulfillment.md)
for the full reasoning.

## Bounded-context boundary (read this first)

This service is a **pure Kafka consumer** of `fulfillment-execution`'s
already-published `TaskCompleted` integration event (topic
`warehouse.fulfillment.events`). It is a **separate Go module in a
separate repository** — it imports no Go code from `fulfillment-execution`
or `workforce-management` and has no write access to either service's
aggregates. It never calls either sibling synchronously: this is
choreography, not orchestration, matching the proven
`wes-work-planning` ← `fulfillment-execution`/`order-management` consumer
pattern already in this fleet. See
[ADR 0003](docs/docs/adr/0003-kafka-choreography-consumer-of-fulfillment-execution.md).

This build is **100% additive** from `fulfillment-execution`'s point of
view — the sibling `feature/labor-performance-hooks` PR in that repo
already carries `associate_id` and `duration_seconds` on the existing
`TaskCompleted` event, and fulfillment-execution ADR-0023 later added
`task_type`; nothing else in that repository is touched.

This context DOES expose its own REST API (to configure standards and
read performance scores) — that is this context's OWN Open Host Service,
symmetric with every other context in the fleet. It also publishes its own
integration event, `TaskPerformanceRecorded`, on
`warehouse.labor-performance.events`
([ADR 0013](docs/docs/adr/0013-labor-performance-integration-events.md)),
which `workforce-management` consumes as its measured-rate cache. Every
integration is inbound or published: this service makes **no** outbound
REST or MCP call to any sibling context
([ADR 0015](docs/docs/adr/0015-optional-travel-component-on-labor-standard.md)
restates that boundary).

## Architecture

Hexagonal (ports & adapters), with a strict inward-only dependency rule —
**domain depends on nothing; application depends on domain; adapters
depend on application/domain** — identical in shape to the other
services in the fleet. See
[ADR 0001](docs/docs/adr/0001-hexagonal-ports-and-adapters.md).

```
cmd/labor/                        main.go — the OLTP composition root (REST +
                                   Kafka consumer + outbox relay)
cmd/labor-projector/              main.go — analytics WRITER (the only writer of
                                   the analytical DB); consumes the analytics
                                   topic from FirstOffset, runs its migrations
cmd/labor-reports/                main.go — analytics READER (read-only pool);
                                   serves GET /reports/performance[/freshness]
cmd/mcp/                          main.go — MCP server (Streamable HTTP, read-only
                                   tools; ADR 0009)
internal/
  domain/
    standard/                     LaborStandard aggregate: TaskType -> expected duration
                                   (+ optional TravelComponentSeconds, ADR 0015)
    performance/                  TaskPerformance aggregate: one scored completed task
    idleness/                     IdlePeriod aggregate: between-task idle gap (ADR 0014)
    shared/                       TaskType, AssociateId, domain events, errors
  application/
    ports/                        OUT: StandardRepo, PerformanceRepo, IdlePeriodRepo,
                                   ProcessedEvents, EventPublisher, UnitOfWork, Clock
    usecases/                     DefineStandard, GetStandard, RecordTaskPerformance
                                   (Kafka-consumer-driven), GetAssociateScorecard,
                                   GetTaskTypePerformance, GetUtilization
  analytics/
    report/                       ANALYTICAL read model (ADR 0007) — depends on
                                   NOTHING; the OLTP layers must not import it
                                   and it must not import them (arch-go enforced)
  adapters/
    inbound/http/                 chi handlers, DTOs, RFC 7807 error mapping;
                                   reports_handler.go serves the read model;
                                   idempotency.go — Idempotency-Key on POST
                                   /standards (ADR 0016); readiness.go — /readyz
                                   (ADR 0017)
    inbound/kafka/                consumer.go        — warehouse.fulfillment.events
                                                       (TaskCompleted only, OLTP)
                                   analytics_consumer.go — warehouse.labor-performance
                                                       .analytics (projector)
    inbound/mcp/                  MCP tools, resource template, prompt (cmd/mcp)
    kafka/                        cloudevents (CloudEvents 1.0 New/Decode, topics,
                                   full type constants — ADR 0021), otelkafka
    outbound/postgres/            pgxpool repos + golang-migrate runner (OLTP DB);
                                   unit_of_work.go, outbox_publisher.go, outbox_relay.go
                                   — the transactional outbox (ADR 0010);
                                   sweeper.go — housekeeping (ADR 0023)
    outbound/bootretry/           boot-time dial retry for migrations/DB ping (ADR 0027)
    outbound/analyticsstore/      analytical DB: writer projection, read-only
                                   reader, in-memory store for tests
    outbound/memory/              in-memory repos for tests/local
    outbound/events/              log publisher (the default)
    outbound/kafka/               analytics + integration publishers, relay sink,
                                   fan-out (EVENT_PUBLISHER=kafka)
    outbound/telemetry/           OTel traces/metrics/logs (copied from workforce-management)
  pgtx/                           the one tx-in-context mechanism shared by the
                                   idempotency middleware and the Postgres UnitOfWork
migrations/                       golang-migrate SQL files (OLTP schema, 0001–0006)
migrations/analytics/             golang-migrate SQL files (analytical schema, 0001)
apis/openapi.yaml                 This service's OWN OLTP REST API (6 endpoints + /healthz + /readyz)
apis/openapi-reports.yaml         The read-only reports API (ADR 0007)
apis/asyncapi.yaml                What this service SUBSCRIBES TO and PUBLISHES
docker-compose.yml                Local Postgres 16 (OLTP :5435, analytics :5436)
docs/docs/adr/                    Architecture Decision Records
```

**Four processes, one writer per database.** The analytical data product
added in [ADR 0007](docs/docs/adr/0007-analytical-data-product.md) splits
into `cmd/labor` (OLTP), `cmd/labor-projector` (the only writer of the
analytical database) and `cmd/labor-reports` (read-only). They share no
database connection: report query load can never contend with the
transactional path that ingests `TaskCompleted`. `cmd/mcp`
([ADR 0009](docs/docs/adr/0009-mcp-inbound-adapter.md)) is a fourth,
read-only deployable over the OLTP database.

The domain layer is pure Go: no `chi`, no `pgx`, no `kafka-go`. No JSON
struct tags in the domain packages.

## Business rules worth knowing before you read the code

- **LaborStandard is append-only history.** `DefineStandard` for a
  `TaskType` that already has an active standard closes the prior
  standard's effective range and starts a new one — it never overwrites
  `ExpectedSeconds` in place. See
  [ADR 0004](docs/docs/adr/0004-standard-frozen-at-completion-time-not-recomputed.md).
- **A `TaskPerformance`'s `StandardSecondsAtCompletion` is resolved
  once, "active AS OF `CompletedAt`", and frozen forever.** A revision
  never rewrites a past row's efficiency, and a possibly out-of-order or
  replayed Kafka message is scored against the standard genuinely in
  force when the task completed, not whatever is active right now. See
  [ADR 0004](docs/docs/adr/0004-standard-frozen-at-completion-time-not-recomputed.md).
- **`EfficiencyPct` never divides by zero.** `ActualSeconds<=0` (an
  unmeasurable completion) or `StandardSecondsAtCompletion<=0` (no
  standard was active) both yield `null`, not an error and not a
  fabricated number.
- **An empty `AssociateId` is legitimate, not an error.** A `TaskCompleted`
  from a station with no checked-in occupant (e.g. a robot station) is
  still recorded and counted in `GetTaskTypePerformance`, just excluded
  from any per-associate scorecard.
- **Idempotent on the CloudEvents `id`.** Recording the same
  event `id` twice is a no-op, never a double-count. Keyed on the event
  `id`, not `TaskId`, since a task id could in principle be reused after a long
  time.
- **Every write and its event commit together.** With Postgres and
  `EVENT_PUBLISHER=kafka`, a use case's Saves, the `processed_events`
  marker and the analytics event are one transaction: the event goes into
  an `outbox_events` row, and an in-process relay ships it to
  `warehouse.labor-performance.analytics` and
  `warehouse.labor-performance.events` within `OUTBOX_RELAY_INTERVAL`.
  The OLTP store and the topics can no longer diverge, and a
  consumer redelivery after a partial failure is scored rather than
  dropped as a duplicate. See
  [ADR 0010](docs/docs/adr/0010-transactional-outbox.md).
- **Idle gaps are derived, never invented.** Each consumed `TaskCompleted`
  derives the associate's idle gap since their previous completion
  (`claimedAt = time − duration_seconds`, `time` being the TaskCompleted CloudEvent attribute), capped at
  `IDLE_GAP_CAP_SECONDS`, in the same unit of work as the performance row.
  A still-running "open gap" is computed at read time only. See
  [ADR 0014](docs/docs/adr/0014-labor-utilization-idleness.md).

## Running locally

### 1. Without a database or a broker (fastest)

With no `DATABASE_URL`, the service starts on the in-memory adapters and is
fully functional over REST (the Kafka consumer still needs a broker to
actually receive messages, but the process starts and serves regardless):

```bash
go run ./cmd/labor
# {"level":"INFO","msg":"database url not configured; using in-memory adapters"}
# {"level":"INFO","msg":"http server listening","addr":":8080"}
# {"level":"INFO","msg":"kafka consumer starting", ...}
```

### 2. With Postgres

```bash
docker compose up -d postgres          # Postgres 16 on localhost:5435

export DATABASE_URL='postgres://labor:***@localhost:5435/labor?sslmode=disable'
go run ./cmd/labor                     # migrations run automatically at startup
```

### 3. With Docker

A working `Dockerfile` builds a scratch-minimal, non-root image:

```bash
docker build -t labor-performance:local .
docker run --rm -p 8080:8080 \
  -e DATABASE_URL='postgres://labor:***@host.docker.internal:5435/labor?sslmode=disable' \
  labor-performance:local
curl -s localhost:8080/healthz
```

### 4. With Helm (kind / any Kubernetes cluster)

```bash
helm install labor-performance charts/labor-performance \
  --set database.url='postgres://labor:***@postgres:5432/labor?sslmode=disable'
kubectl port-forward svc/labor-performance 8080:80
curl -s localhost:8080/healthz
```

See `charts/labor-performance/values.yaml` for the full configuration
surface (`kafka.enabled`, `otel.enabled`, ingress, autoscaling, an
`existingSecret` pattern for `DATABASE_URL`).

### 5. Kafka consumer smoke test against the shared broker

```bash
# The fleet runs ONE Kafka broker: the in-cluster broker deployed by
# warehouse-infra, whose external listener is reachable from the host at
# localhost:9092 (this repo's docker-compose.yml has no Kafka service).
export KAFKA_BROKERS=localhost:9092
export KAFKA_CONSUMER_GROUP=labor-performance
go run ./cmd/labor

# In another terminal, publish a TaskCompleted-shaped message using any
# Kafka producer CLI, e.g. kcat:
# Every message must be a CloudEvents 1.0 event (ADR 0021); anything else
# is dead-lettered to warehouse.fulfillment.events.dlq.
echo '{"specversion":"1.0","id":"'$(uuidgen)'","source":"/warehouse/fulfillment-execution","type":"com.warehouse.wes.fulfillment-execution.task.TaskCompleted","subject":"task-1","time":"2026-08-29T22:00:00Z","datacontenttype":"application/json","dataschema":"urn:warehouse:fulfillment-execution:events:TaskCompleted:v1","data":{"task_id":"task-1","station_id":"station-1","work_unit_id":"wu-1","associate_id":"assoc-1","duration_seconds":52,"task_type":"PICK"}}' \
  | kcat -P -b localhost:9092 -t warehouse.fulfillment.events \
      -H 'content-type=application/cloudevents+json; charset=UTF-8'

# Then verify it was recorded:
curl -s localhost:8080/associates/assoc-1/scorecard
```

### Configuration

`cmd/labor` (OLTP):

| Variable | Default | Purpose |
| --- | --- | --- |
| `HTTP_ADDR` | `:8080` | Listen address. |
| `DATABASE_URL` | *(unset)* | Postgres DSN. Unset ⇒ in-memory adapters. |
| `MIGRATIONS_DATABASE_URL` | *(falls back to `DATABASE_URL`)* | Direct (non-PgBouncer) DSN used only for the golang-migrate step ([ADR 0020](docs/docs/adr/0020-migrations-direct-postgres-connection.md)). |
| `MIGRATIONS_PATH` | `migrations` | golang-migrate source directory. |
| `KAFKA_BROKERS` | `localhost:9092` | Comma-separated broker addresses. |
| `KAFKA_CONSUMER_GROUP` | `labor-performance` | Consumer group id on `warehouse.fulfillment.events`. |
| `EVENT_PUBLISHER` | `log` | `log` or `kafka`. With `kafka`, domain events are fanned onto `warehouse.labor-performance.analytics` and `warehouse.labor-performance.events` (ADR 0013); when `DATABASE_URL` is also set they go through the transactional outbox (`outbox_events` + in-process relay, [ADR 0010](docs/docs/adr/0010-transactional-outbox.md)), otherwise straight to the broker. |
| `OUTBOX_RELAY_INTERVAL` | `1s` | How long the outbox relay sleeps between passes that found nothing to publish (Go duration; only used in outbox mode). |
| `HOUSEKEEPING_INTERVAL` | `1h` | How often the housekeeping sweeper runs ([ADR 0023](docs/docs/adr/0023-housekeeping-sweeper.md)); only with `DATABASE_URL`. |
| `IDEMPOTENCY_KEY_TTL` | `24h` | Age after which `idempotency_keys` rows are swept (ADR 0016/0023). |
| `OUTBOX_RETENTION` | `168h` (7d) | Age after which PUBLISHED `outbox_events` rows are swept; unpublished rows are never swept. |
| `IDLE_GAP_CAP_SECONDS` | `3600` | Upper bound on a recorded idle gap (a capped gap is stored with `capped: true`); malformed/non-positive values fall back to the default ([ADR 0014](docs/docs/adr/0014-labor-utilization-idleness.md)). |
| `CORS_ALLOWED_ORIGINS` | `http://localhost:5173,http://localhost:5187` | Comma-separated allowed origins (also read by `cmd/labor-reports`). |
| `OTEL_SERVICE_NAME` | `labor-performance` | OTel `service.name` resource attribute. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `localhost:4317` | OTLP/gRPC Collector endpoint. |
| `SERVICE_VERSION` | `dev` | OTel `service.version` (overridden by `-ldflags -X main.version`). |
| `ENVIRONMENT` | `local` | OTel `deployment.environment.name` resource attribute. |
| `LOG_LEVEL` | `info` | `debug` \| `info` \| `warn` \| `error`. |

The other binaries read the same `LOG_LEVEL`/`OTEL_EXPORTER_OTLP_ENDPOINT`/`SERVICE_VERSION` set
(their `OTEL_SERVICE_NAME` defaults are `labor-performance-projector`,
`labor-performance-reports` and `labor-performance-mcp`), plus:

| Binary | Variable | Default | Purpose |
| --- | --- | --- | --- |
| `cmd/labor-projector` | `ANALYTICS_DATABASE_URL` | *(required)* | Analytical Postgres DSN (the only writer). |
| `cmd/labor-projector` | `ANALYTICS_MIGRATIONS_PATH` | `migrations/analytics` | Analytical schema migrations. |
| `cmd/labor-projector` | `KAFKA_BROKERS` | `localhost:9092` | Broker for `warehouse.labor-performance.analytics`. |
| `cmd/labor-projector` | `ADMIN_ADDR` | `:8091` | `/healthz` listener. |
| `cmd/labor-reports` | `ANALYTICS_DATABASE_URL` | *(required)* | Analytical Postgres DSN (read-only pool). |
| `cmd/labor-reports` | `HTTP_ADDR` | `:8092` | Reports API listener. |
| `cmd/mcp` | `MCP_ADDR` | `:8090` | Streamable HTTP listener. |
| `cmd/mcp` | `DATABASE_URL` / `MIGRATIONS_DATABASE_URL` / `MIGRATIONS_PATH` | *(unset)* / *(falls back to `DATABASE_URL`)* / `migrations` | Same OLTP store as `cmd/labor`; unset ⇒ in-memory. |

None of the binaries exposes a Prometheus `/metrics` endpoint: metrics are
pushed over OTLP to `OTEL_EXPORTER_OTLP_ENDPOINT` (ADR 0008).

## API

Six endpoints plus liveness and readiness probes. The full contract, including the RFC
7807 error schema, is in [`apis/openapi.yaml`](apis/openapi.yaml).

| Method | Path | Use case |
| --- | --- | --- |
| `POST` | `/standards` | DefineStandard (optional `travelComponentSeconds`, ADR 0015). Requires an `Idempotency-Key` header — this is the service's one true resource-creation endpoint; see ADR 0016. |
| `GET` | `/standards/{taskType}` | GetStandard |
| `GET` | `/associates/{associateId}/scorecard` | GetAssociateScorecard |
| `GET` | `/task-types/{taskType}/performance` | GetTaskTypePerformance |
| `GET` | `/task-types/{taskType}/utilization?window=1h` | GetUtilization (ADR 0014) |
| `GET` | `/associates/{associateId}/utilization?window=1h` | GetUtilization (ADR 0014) |
| `GET` | `/healthz` | Liveness probe |
| `GET` | `/readyz` | Readiness probe — flips to 503 as the first step of graceful shutdown ([ADR 0017](docs/docs/adr/0017-kafka-dlq-and-graceful-shutdown.md)) |

The read-only reports API (`cmd/labor-reports`,
[`apis/openapi-reports.yaml`](apis/openapi-reports.yaml)) serves
`GET /reports/performance`, `GET /reports/performance/freshness` and
`GET /healthz`.

The MCP server (`cmd/mcp`) exposes four read-only tools —
`get_associate_scorecard`, `get_task_type_performance`,
`get_labor_standard`, `get_task_type_utilization` — plus the
`scorecard://labor/{associateId}` resource template and the
`review_associate_performance` prompt. No write tool is registered.

None of these surfaces is authenticated: the bearer-key layer from
[ADR 0011](docs/docs/adr/0011-rest-auth-static-bearer-scopes.md) was removed by
[ADR 0012](docs/docs/adr/0012-remove-rest-auth-layer.md).

There is deliberately **no** REST endpoint for `RecordTaskPerformance` —
it is exclusively Kafka-consumer-driven (see
[ADR 0003](docs/docs/adr/0003-kafka-choreography-consumer-of-fulfillment-execution.md)).

Every error response is `application/problem+json` (RFC 7807), the same
shape every other service in this fleet emits.

### Curl walkthrough

**Health:**

```bash
curl -s localhost:8080/healthz
# {"status":"ok"}
```

**DefineStandard:**

```bash
curl -s -X POST localhost:8080/standards \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: 3f9c1e4a-...' \
  -d '{"taskType":"PICK","expectedSeconds":45}'
# 201 Created
# {"taskType":"PICK","expectedSeconds":45,"effectiveFrom":"2026-08-29T12:00:00Z"}
```

Revising it closes the prior record and starts a new one (a fresh
`Idempotency-Key` is required — reusing the one above would replay the
first call's cached 201, not define a revision):

```bash
curl -s -X POST localhost:8080/standards \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: 7a2d5b8c-...' \
  -d '{"taskType":"PICK","expectedSeconds":40}'
# 201 Created
# {"taskType":"PICK","expectedSeconds":40,"effectiveFrom":"2026-08-30T09:00:00Z"}
```

**GetStandard:**

```bash
curl -s localhost:8080/standards/PICK
# 200 OK, or 404 application/problem+json if none is active
```

**GetAssociateScorecard** (populated only by the Kafka consumer):

```bash
curl -s localhost:8080/associates/assoc-1/scorecard
# 200 OK:
# {"associateId":"assoc-1","taskCount":3,"meanEfficiencyPct":91.2,
#  "byTaskType":{"PICK":{"taskCount":3,"meanEfficiencyPct":91.2}},
#  "trend":"STABLE","coachingFlag":false}
# 404 application/problem+json if this service has never seen assoc-1
```

`trend` (`IMPROVING`/`DECLINING`/`STABLE`/`INSUFFICIENT_DATA`) and
`coachingFlag` are computed from the associate's most recent (up to 10)
scored tasks — see [ADR 0005](docs/docs/adr/0005-associate-trend-and-coaching-flag.md)
for the full design. Visibility only: this service never automates
coaching, pay, or task-claim decisions from either field.

**GetTaskTypePerformance** (fleet-wide, all associates):

```bash
curl -s localhost:8080/task-types/PICK/performance
# 200 OK — always 200, including a zero-count result for a never-seen type:
# {"taskType":"PICK","taskCount":42,"meanEfficiencyPct":88.7,"meanActualSeconds":39.4}
```

`meanActualSeconds` is a real measured rate, independent of
`meanEfficiencyPct` — populated even when no `LaborStandard` has ever
been defined for this TaskType. See
[ADR 0006](docs/docs/adr/0006-mean-actual-seconds-independent-of-standard.md).

## Quality gate

Every `make` target mirrors a step in `.github/workflows/ci.yml`, so the
same feedback CI gives you post-push is available locally, pre-commit:

```bash
make check       # fmt-check + vet + build + lint + test -race
make check-all   # check + coverage (gate: 90% on domain + application + analytics) + arch-test + bdd
```

| Target | What it runs |
| --- | --- |
| `build` | `go build ./...` |
| `vet` | `go vet ./...` |
| `fmt` / `fmt-check` | `gofmt -w .` / fail if `gofmt -l .` is non-empty |
| `lint` | `golangci-lint run ./...` (CI pins `v2.13.1`) |
| `test` | `go test ./... -race` |
| `coverage` | coverage profile + the 90% gate |
| `bdd` / `arch-test` | godog acceptance tests / arch-go fitness tests |
| `mutation-fast` / `mutation` | gremlins on `./internal/domain/performance` / `./internal/domain` |
| `vuln` / `api-lint` | govulncheck / Spectral on all three specs |
| `integration-kafka-testcontainers` | Build-tagged consumer test against a Testcontainers Kafka broker (needs Docker only) |
| `contract` | Schemathesis contract tests over `apis/openapi.yaml` (`scripts/contract-test.sh`) |
| `check-fast` | fmt-check + vet + arch-test — the quick pre-"done" gate |
| `guide-lint` / `harness-test` | Lint the agent guides (skills, references, context budget) / unit-test the agent hooks |

Additional verification surfaces, each with its own CI job:

```bash
go test ./... -run TestFeatures -v                  # BDD (godog/Gherkin)
go test ./internal/architecture/... -v               # arch-fitness (arch-go)
go test -tags=integration ./... -race -count=1       # Postgres + Kafka integration (testcontainers)
go test -tags=integration ./internal/adapters/outbound/postgres/ -run Outbox -race -count=1
                                                     # outbox: testcontainers Postgres, needs Docker only
gremlins unleash ./internal/domain                    # mutation testing (see .gremlins.yaml)
ct lint --charts charts/labor-performance \
  --validate-maintainers=false --check-version-increment=false
spectral lint apis/openapi.yaml --ruleset .spectral.yaml --fail-severity=warn
spectral lint apis/openapi-reports.yaml --ruleset .spectral.yaml --fail-severity=warn
spectral lint apis/asyncapi.yaml --ruleset .spectral.asyncapi.yaml --fail-severity=warn
```

Git hooks are wired through [lefthook](https://github.com/evilmartians/lefthook)
— `pre-commit` runs fmt-check/vet/lint, `pre-push` runs `make check`. Hooks
are not tracked by git, so activate them once per clone:

```bash
brew install lefthook   # or: go install github.com/evilmartians/lefthook@latest
lefthook install
```

CI (`.github/workflows/ci.yml`) runs the full fleet-standard matrix:
**`lint`**, **`guide-lint`** (agent-guide and harness lint), **`complexity`**
(gocyclo/gocognit/cyclop/funlen/nestif), **`test`**, **`bdd`**,
**`contract`** (Schemathesis over `apis/openapi.yaml`), **`evals-tests`**
(MCP evals E1–E3, ADR 0030), **`integration`** (testcontainers boots its own
Postgres/Kafka; a fitness test fails CI if a Postgres integration test
reintroduces a `DATABASE_URL` skip gate), **`mutation-fast`** (blocking,
`./internal/domain/performance`) and **`mutation`** (exhaustive,
scheduled/manual, `./internal/domain`), **`api-lint`** (Spectral against
`apis/openapi.yaml`, `apis/openapi-reports.yaml` and `apis/asyncapi.yaml`),
**`vuln`** (govulncheck), **`arch-test`** (arch-go fitness tests),
**`web`** (lint/typecheck/test/build of `web/`), **`docs-api-drift`**
(regenerates both generated API references and fails on any diff),
**`drift`** (scheduled/manual dead-code and coverage-quality report),
**`helm-lint`** (`ct lint`) and **`trivy-scan`** (container CVE gate) on
pull requests into `main`, **`docker-publish`** (main-only, cosign
keyless signing + SPDX SBOM attestation), and **`release`** (main-only,
auto-tagged GitHub release + published Helm chart). Plus
`.github/workflows/codeql.yml` (security-extended CodeQL analysis) and
`.github/workflows/scorecard.yml` (OpenSSF Scorecard).

## Known gaps

Found while refreshing these docs against the code on `develop`; there
are currently none open.

Closed entries:

- The `labor_mfe` remote did not send `Idempotency-Key` on `POST
  /standards`; `web/src/api.ts` now sends a fresh UUID per submission.
- The analytics consumer's dedupe gate committed before the projection and
  `ReadMessage` auto-committed the offset, so a failed apply was never
  retried; the claim and the apply now share one `analyticsstore.UnitOfWork`
  transaction and the offset is committed (`FetchMessage` +
  `CommitMessages`) only after it succeeds (ADR 0031).
- `fulfillment-execution`'s
`TaskCompleted` payload not carrying a `task_type` field, leaving every
consumed event bucketed as `""` (unclassified) — was closed by
fulfillment-execution ADR-0023 (`task_type` added to the wire payload)
together with this service's own consumer update (`ParseTaskTypeLenient`
now receives the real value instead of a hardcoded `""`).

## Deferred (v1)

The following are **deliberately out of scope**. They are listed so an
absence is never mistaken for an oversight — each is a decision, not a gap
someone forgot about.

- **Automatic pay-for-performance / bonus calculation.** A real Manhattan
  competitor feature, explicitly out of scope — this context surfaces the
  number, a human/other system decides what to do with it.
- **Gamification, automated coaching workflows, real-time digital
  communication to associates.** Real Manhattan/Blue Yonder features,
  explicitly out of scope for v1 — v1 (this PR, ADR 0005) ships the
  passive visibility signal only (`Trend`/`CoachingFlag` on the
  Scorecard read model, a human reads it); an active workflow that
  automatically messages, schedules, or nudges an associate based on
  that signal remains deferred.
- **Any change to `fulfillment-execution` beyond what
  `feature/labor-performance-hooks` already does.** This build is 100%
  additive from that repo's point of view.

### Now built (fleet-parity pass) — no longer deferred

The following were listed as deferred in this service's original v1 build
and have since been added, bringing this service to full fleet parity with
`inventory-storage` / `wes-work-planning`:

- **Helm chart** (`charts/labor-performance/`) — `helm install
  labor-performance charts/labor-performance`.
- **godog/BDD acceptance tests** (`features/*.feature` +
  `features_test.go`) — covers the REST-visible contract (define/revise/get
  a standard, get-scorecard 404, get-task-type-performance always-200).
- **Postgres integration tests**
  (`internal/adapters/outbound/postgres/*_integration_test.go`) — real
  round-trips for `StandardRepo`, `PerformanceRepo`, `ProcessedEventRepo`.
- **Gremlins mutation-testing gate** (`.gremlins.yaml`) — see the CI section
  above for the measured baseline.
- **arch-go fitness tests** (`internal/architecture/architecture_test.go`)
  — enforces the hexagonal dependency rule.
- **Spectral API linting** (`.spectral.yaml`, `.spectral.asyncapi.yaml`) —
  against both `apis/openapi.yaml` and `apis/asyncapi.yaml`.
- **CodeQL, OpenSSF Scorecard, Trivy container scanning.**
- **Docker image publishing + release automation** (`docker-publish`,
  `release` CI jobs) — cosign keyless signing, SPDX SBOM attestation,
  auto-tagged GitHub releases, Helm chart published to
  `oci://ghcr.io/iqvo`.
- **Full Docusaurus documentation site**, live at
  https://iqvo.github.io/labor-performance/.
- **`labor-mfe` micro-frontend remote** (`web/`) — see
  [Operator micro-frontend](#operator-micro-frontend-web) below.
- **Per-service analytical data product** — analytics topic, analytical
  Postgres, `cmd/labor-projector` + `cmd/labor-reports`
  ([ADR 0007](docs/docs/adr/0007-analytical-data-product.md)).
- **MCP inbound adapter** — `cmd/mcp`
  ([ADR 0009](docs/docs/adr/0009-mcp-inbound-adapter.md)).
- **Integration event** — `TaskPerformanceRecorded` on
  `warehouse.labor-performance.events`, consumed by
  `workforce-management`
  ([ADR 0013](docs/docs/adr/0013-labor-performance-integration-events.md)).

## Architecture Decision Records

1. [0001 — Hexagonal (ports & adapters) architecture](docs/docs/adr/0001-hexagonal-ports-and-adapters.md)
2. [0002 — A new bounded context, not an extension of workforce-management or fulfillment-execution](docs/docs/adr/0002-new-bounded-context-not-extension-of-workforce-or-fulfillment.md)
3. [0003 — Kafka choreography consumer of fulfillment-execution, no REST dependency](docs/docs/adr/0003-kafka-choreography-consumer-of-fulfillment-execution.md)
4. [0004 — StandardSecondsAtCompletion is frozen at ingestion time, never recomputed](docs/docs/adr/0004-standard-frozen-at-completion-time-not-recomputed.md)
5. [0005 — Associate Trend and CoachingFlag on the Scorecard read model](docs/docs/adr/0005-associate-trend-and-coaching-flag.md)
6. [0006 — MeanActualSeconds on TaskTypePerformance, independent of any standard](docs/docs/adr/0006-mean-actual-seconds-independent-of-standard.md)
7. [0007 — Per-service analytical data product (the Labor Performance Report)](docs/docs/adr/0007-analytical-data-product.md)
8. [0008 — Standard metrics convention across the fleet](docs/docs/adr/0008-standard-metrics-convention.md)
9. [0009 — Model Context Protocol as an inbound adapter, not a new service](docs/docs/adr/0009-mcp-inbound-adapter.md)
10. [0010 — Transactional outbox for the analytics topic](docs/docs/adr/0010-transactional-outbox.md)
11. [0011 — REST identity: fleet-standard static bearer keys with read/read-write scopes (superseded by 0012)](docs/docs/adr/0011-rest-auth-static-bearer-scopes.md)
12. [0012 — Remove the REST/MCP identity layer](docs/docs/adr/0012-remove-rest-auth-layer.md)
13. [0013 — Labor performance publishes an integration event](docs/docs/adr/0013-labor-performance-integration-events.md)
14. [0014 — Measuring idleness and utilization](docs/docs/adr/0014-labor-utilization-idleness.md)
15. [0015 — Optional travel-time component on a LaborStandard](docs/docs/adr/0015-optional-travel-component-on-labor-standard.md)
16. [0016 — Transactional Idempotency-Key middleware for POST /standards](docs/docs/adr/0016-idempotency-key-middleware.md)
17. [0017 — Kafka consumer dead-letter queue and graceful shutdown hardening](docs/docs/adr/0017-kafka-dlq-and-graceful-shutdown.md)
18. [0018 — Key-aware Hash balancer on every outbound Kafka writer](docs/docs/adr/0018-kafka-writer-hash-balancer.md)
19. [0019 — Per-workload HorizontalPodAutoscaler and pgxpool MaxConns/statement_timeout tuning](docs/docs/adr/0019-horizontal-autoscaling-and-pgxpool-tuning.md)
20. [0020 — Run golang-migrate against a direct Postgres connection, not PgBouncer](docs/docs/adr/0020-migrations-direct-postgres-connection.md)
21. [0021 — CloudEvents 1.0 as the mandatory event envelope](docs/docs/adr/0021-cloudevents-mandatory-event-envelope.md)
22. [0022 — Optimistic concurrency and one open standard per task type on labor_standards](docs/docs/adr/0022-optimistic-concurrency-one-open-standard.md)
23. [0023 — Housekeeping sweeper for idempotency keys and published outbox rows](docs/docs/adr/0023-housekeeping-sweeper.md)
24. [0024 — labor-mfe: a chart-shipped, disabled-by-default frontend remote](docs/docs/adr/0024-labor-mfe-frontend-remote.md)
25. [0025 — RFC 7807 Problem Details for every REST error](docs/docs/adr/0025-rfc7807-problem-details.md)
26. [0026 — arch-go architecture fitness tests, enforced in CI](docs/docs/adr/0026-arch-go-fitness-suite.md)
27. [0027 — Boot-time dial retry (bootretry) + Kubernetes startupProbe](docs/docs/adr/0027-bootretry-and-startup-probe.md)
28. [0028 — Gateway API HTTPRoute, disabled by default, alongside Ingress](docs/docs/adr/0028-gateway-api-httproute.md)
29. [0029 — Kafka writer durability: RequireAll acks + 10ms BatchTimeout](docs/docs/adr/0029-kafka-writer-durability.md)
30. [0030 — MCP eval suite and governance gate, run as plain go test](docs/docs/adr/0030-mcp-eval-and-governance-gates.md)
31. [0031 — Analytics consumer: atomic claim + apply, commit after success, retry transient failures](docs/docs/adr/0031-analytics-consumer-atomic-claim-and-retry.md)

The same list, with statuses, is in [docs/docs/adr/about.md](docs/docs/adr/about.md).

## License

MIT — see [LICENSE](LICENSE).

## Operator micro-frontend (`web/`)

`web/` is `labor_mfe`, this context's Module Federation remote. It talks only to
this service's own REST API and is never part of `make check`.

**Standalone development** is unchanged:

```bash
cd web && npm install && npm run dev     # http://localhost:5187
```

**Deployed to the kind cluster**, it is built into a static bundle and served
by its own `nginx-unprivileged` pod:

```bash
cd web
docker build --build-context uikit=../../warehouse-ui-kit \
  -t warehouse/labor-performance-frontend:local .
```

The cluster's localhost topology separates the two kinds of traffic onto two
independent entrypoints, and neither proxies to the other:

| URL | Served by | Carries |
|---|---|---|
| `http://localhost/mfes/labor-performance/` | Nginx web gateway → this remote's nginx pod | HTML, JS, CSS, fonts, `remoteEntry.js` |
| `http://localhost:8000/api/labor-performance/` | Kong | this service's REST API |

Kong never serves frontend assets, and the Nginx gateway never proxies an API.
Enable the workload with `frontend.enabled=true` in the Helm chart; the Service
is deliberately `ClusterIP` with no Ingress/HTTPRoute, because frontend path
routing belongs to the Nginx web gateway in `warehouse-infra`.

Because one image must work in more than one environment, the remote reads its
API origin at runtime from `window.__WAREHOUSE_CONFIG__.apiOrigin` (published
by the console shell) rather than baking a hostname in at build time. A
production build with no runtime config **fails loudly** instead of silently
falling back to a developer port; standalone `npm run dev` still uses
`http://localhost:8088`. See `web/src/config.ts`.

Chart invariants are asserted by:

```bash
python3 charts/labor-performance/tests/test_service_selectors.py
```

which proves every Service selects exactly one Deployment — the OLTP Service
must never select the frontend, analytics or MCP pods.

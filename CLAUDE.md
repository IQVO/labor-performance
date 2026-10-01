# Project: Labor Performance (Supporting Bounded Context)

Engineered labor standards ("a PICK should take 45s") and actual-vs-standard
performance scoring ("this associate's last PICK took 52s — 87% of
standard") for the `warehouse-systems` fleet. The eighth bounded-context Go
service in the fleet (after `order-management`, `inventory-storage`,
`wes-work-planning`, `workforce-management`, `fulfillment-execution`,
`facility-layout`, `warehouse-ops-agent`). Full docs site:
https://claudioed.github.io/labor-performance/

> **Study project.** Educational DDD exercise using real industry patterns
> and terminology (WMS/WES, CloudEvents 1.0 envelopes, RFC 7807, hexagonal
> architecture). Not a production system; not affiliated with a major e-commerce retailer,
> Manhattan Associates, Blue Yonder, or any other company.

## Non-negotiables (read before writing any code)

1. **Pure Kafka consumer of `fulfillment-execution`, nothing else.** This
   service subscribes to `warehouse.fulfillment.events` (topic, shared/
   fan-out with `wes-work-planning`) and reacts only to
   `com.warehouse.wes.fulfillment-execution.task.TaskCompleted`. It
   is a separate Go module/repo: **no Go import from, and no write access
   to**, `fulfillment-execution` or `workforce-management`. It never calls
   ANY sibling context over REST or MCP — choreography, not orchestration
   (ADR 0015 restates this for facility-layout). Siblings may read this
   context's own surfaces and consume its integration topic. See
   `.claude/rules/domain-model.md` and ADR 0002/0003/0013.
2. **Hexagonal / ports & adapters, strict inward dependency rule:** domain
   depends on nothing; application depends on domain; adapters depend on
   application/domain. No framework, HTTP, Kafka, or SQL types in the domain
   layer (arch-go enforced, `internal/architecture/architecture_test.go`).
3. **Never fabricate a number.** `EfficiencyPct`, `meanEfficiencyPct`, and
   `meanActualSeconds` are nullable everywhere (domain, REST, analytics
   report) and MUST be `null`, never `0`, when nothing was scorable/
   measurable. See `.claude/rules/domain-model.md`.
4. **This service NEVER accepts a TaskPerformance write over REST.**
   Recording a performance row is exclusively Kafka-consumer-driven
   (`RecordTaskPerformance`, idempotent on the CloudEvents `id`).

## Events: CloudEvents 1.0 is MANDATORY

Every Kafka message this service produces or consumes (integration
`warehouse.<ctx>.events` AND analytics `warehouse.<ctx>.analytics`) is a
CloudEvents 1.0 event in structured content mode. This is a hard fleet rule,
not a preference:

- No flat envelope (`event_id`/`event_type`/`occurred_at`), no dual-write,
  no dual-read, no envelope toggle env var (`EVENT_ENVELOPE_MODE` is gone).
- Build/validate/(un)marshal with `github.com/cloudevents/sdk-go/v2/event`
  via `internal/adapters/kafka/cloudevents/`; transport stays kafka-go.
- Kafka header `content-type: application/cloudevents+json; charset=UTF-8`.
- Required attributes: `specversion=1.0`, `id` (UUID, stable across outbox
  redelivery), `source=/warehouse/labor-performance`, `type`, `subject` (aggregate id), `time`
  (occurred-at, UTC), `datacontenttype=application/json`,
  `dataschema=urn:warehouse:labor-performance:<events|analytics>:<EventName>:v<N>`.
- `type` = `com.warehouse.<subdomain>.<bounded-context>.<entity>.<EventName>`;
  for this service: `com.warehouse.wes.labor-performance.<entity>.<EventName>`. Breaking payload
  change => new `.v2` type + new dataschema version, never mutate.
- Consumers dispatch on the FULL `type`, ignore unknown types, dedupe on
  `id`, and DLQ/skip (never crash, never parse a legacy shape) anything that
  fails CloudEvents validation.

Full standard and the fleet's cross-service type catalogue: ADR-0021
(`docs/docs/adr/`).

This service's types (exact): publishes
`com.warehouse.wes.labor-performance.standard.LaborStandardDefined`,
`com.warehouse.wes.labor-performance.standard.LaborStandardRevised` (analytics
only) and `com.warehouse.wes.labor-performance.performance.TaskPerformanceRecorded`
(analytics + integration; consumed by workforce-management); consumes
`com.warehouse.wes.fulfillment-execution.task.TaskCompleted` plus its own
three analytics types (projector).

## Project overview

- Go 1.26, module `github.com/claudioed/labor-performance`.
- Four processes, one writer per database (ADR 0007, ADR 0009, ADR 0010):
  - `cmd/labor` — OLTP: REST API + Kafka consumer of `TaskCompleted` +
    outbox relay (publishes to the analytics and integration topics).
  - `cmd/labor-projector` — analytics writer; consumes
    `warehouse.labor-performance.analytics`, the ONLY writer of the
    analytical DB.
  - `cmd/labor-reports` — analytics reader; read-only pool; serves
    `GET /reports/performance[/freshness]`.
  - `cmd/mcp` — MCP server (Streamable HTTP, `:8090`), four read-only
    tools over the OLTP store.
- Two OpenAPI specs (`apis/openapi.yaml` OLTP, `apis/openapi-reports.yaml`
  reports) and one AsyncAPI spec (`apis/asyncapi.yaml`, documents what this
  service consumes and what it publishes to its analytics topic and its
  integration topic `warehouse.labor-performance.events`, ADR 0013).
- No REST or MCP surface is authenticated (ADR 0012 removed ADR 0011's
  bearer-key layer).
- `charts/labor-performance/` — Helm chart, fleet parity.
- `web/` — the `labor_mfe` micro-frontend remote (module federation, React,
  Vite) consuming `@warehouse/ui-kit`; mounted by the console at `/labor`.
- `docs/` — Docusaurus site (see `.claude/rules/docs-and-api-drift.md`).

## Architecture

```
cmd/labor/                    main.go — OLTP composition root
cmd/labor-projector/          main.go — analytics WRITER (only writer of the analytical DB)
cmd/labor-reports/            main.go — analytics READER (read-only pool)
cmd/mcp/                      main.go — MCP server (read-only tools, ADR 0009)
internal/
  domain/
    standard/                 LaborStandard aggregate: TaskType -> expected duration
                                (+ optional TravelComponentSeconds, ADR 0015)
    performance/               TaskPerformance aggregate: one scored completed task
    idleness/                  IdlePeriod aggregate: between-task idle gap (ADR 0014)
    shared/                    value objects: TaskType, AssociateId, events, errors
  application/
    ports/                     OUT: StandardRepo, PerformanceRepo, IdlePeriodRepo, ProcessedEvents,
                                EventPublisher, UnitOfWork, Clock
    usecases/                  DefineStandard, GetStandard, RecordTaskPerformance
                                (Kafka-consumer-driven), GetAssociateScorecard,
                                GetTaskTypePerformance, GetUtilization
  analytics/
    report/                    Analytical read model (ADR 0007) — depends on NOTHING;
                                OLTP layers must not import it and vice versa (arch-go enforced)
  adapters/
    inbound/http/               chi handlers, DTOs, RFC7807 error mapping;
                                 reports_handler.go serves the analytics read model
    inbound/kafka/               consumer.go (warehouse.fulfillment.events, TaskCompleted only)
                                  analytics_consumer.go (warehouse.labor-performance.analytics)
    inbound/mcp/                 MCP tools, resource template, prompt, governance test
    outbound/postgres/          pgxpool repos + golang-migrate; unit_of_work.go,
                                 outbox_publisher.go, outbox_relay.go (transactional outbox, ADR 0010)
    outbound/analyticsstore/    analytical DB: writer projection, read-only reader, in-memory store
    outbound/memory/            in-memory repos for tests/local
    outbound/events/            log publisher (default)
    outbound/kafka/              analytics + integration publishers, relay sink, fan-out
                                 (EVENT_PUBLISHER=kafka)
    kafka/cloudevents/           the ONLY CloudEvents 1.0 New/Decode helper, topics, type consts (ADR 0021)
    outbound/telemetry/         OTel traces/metrics/logs
migrations/                    golang-migrate SQL (OLTP schema)
migrations/analytics/          golang-migrate SQL (analytical schema)
apis/openapi.yaml               OLTP REST API (6 endpoints + /healthz)
apis/openapi-reports.yaml       Reports REST API (ADR 0007)
apis/asyncapi.yaml               Consumed + published event contracts
docs/docs/adr/                  ADRs (Nygard format), Docusaurus-rendered
```

Domain layer is pure Go: no `chi`, no `pgx`, no `kafka-go`, no JSON tags.

See `.claude/rules/domain-model.md` for the full ubiquitous language,
aggregates, invariants, and use cases.

## Key commands

```bash
# Run locally (in-memory adapters, no DB/broker needed)
go run ./cmd/labor

# With Postgres
docker compose up -d postgres                 # :5435
export DATABASE_URL='postgres://labor:labor@localhost:5435/labor?sslmode=disable'
go run ./cmd/labor                            # migrations run automatically

# Quality gate (mirrors .github/workflows/ci.yml)
make check       # fmt-check + vet + build + lint + test -race
make check-all   # check + coverage (gate: 90%) + arch-test + bdd

# Individual verification surfaces
go test ./... -run TestFeatures -v                    # BDD (godog/Gherkin)
go test ./internal/architecture/... -v                 # arch-fitness (arch-go)
go test -tags=integration ./... -race -count=1         # Postgres + Kafka (testcontainers) integration
make integration-kafka-testcontainers                  # just the Kafka consumer test
gremlins unleash ./internal/domain                      # mutation testing
spectral lint apis/openapi.yaml --ruleset .spectral.yaml --fail-severity=warn
spectral lint apis/openapi-reports.yaml --ruleset .spectral.yaml --fail-severity=warn
spectral lint apis/asyncapi.yaml --ruleset .spectral.asyncapi.yaml --fail-severity=warn
ct lint --charts charts/labor-performance --validate-maintainers=false --check-version-increment=false

# Docs site (see .claude/rules/docs-and-api-drift.md before regenerating)
cd docs && npm ci && npm run typecheck && npm run build
npm run clean-api-docs:all && npm run gen-api-docs:all   # what CI's docs-api-drift job runs
```

`lefthook install` wires `pre-commit` (fmt-check/vet/lint) and `pre-push`
(`make check`) — activate once per clone (hooks are not tracked by git).

## Code standards & testing

- gofmt/go vet clean; every package has a doc comment.
- golangci-lint `v2.13.1` (pinned, matches CI) — copy `.golangci.yml` from
  `inventory-storage` when bootstrapping a new package.
- Table-driven tests: domain + application (in-memory adapters, fake Kafka
  reader — never hit a real broker in unit tests). One httptest per REST
  endpoint (success + error path). Build-tagged Kafka integration test
  using Testcontainers (`internal/adapters/inbound/kafka/consumer_integration_test.go`).
- Coverage gate: 90% on `./internal/domain/...,./internal/application/...,
  ./internal/analytics/...` (`make coverage`).
- RFC 7807 `application/problem+json` for every error response, identical
  shape across both OLTP and reports APIs.
- CI (`.github/workflows/ci.yml`) runs the full fleet-standard matrix: lint,
  test, bdd, integration, mutation-fast (blocking) / mutation (scheduled
  exhaustive), api-lint (Spectral, all 3 specs), vuln (govulncheck),
  arch-test, web, docs-api-drift, drift (scheduled/manual report),
  helm-lint + trivy-scan (PRs into `main`), docker-publish + release
  (main-only). Plus `codeql.yml`, `scorecard.yml` and `docs.yml` (Pages).

## Git workflow

- GitFlow: `develop` is the default/working branch; `main` is release-only.
- Branch off `develop`, PR back into `develop` (`gh pr create --base
  develop`). Never force-push over pushed history. Do not self-merge —
  leave PRs open for review unless told otherwise.
- **This repo's `github-pages` environment allows deploys from BOTH
  `develop` and `main`**, but `.github/workflows/docs.yml` currently
  triggers only on push to `main` — see `.claude/rules/docs-and-api-drift.md`.

## Reference rules (load when relevant)

- `.claude/rules/domain-model.md` — why this context exists, ubiquitous
  language, aggregates & invariants, domain events, use cases, REST/Kafka
  contracts, v1 scope decisions.
- `.claude/rules/docs-and-api-drift.md` — the two OpenAPI specs, the
  Docusaurus `docusaurus-plugin-openapi-docs` wiring for both, the
  `@faker-js/faker` / `postman-collection` pitfall, and the docs.yml
  trigger-branch gap.
- `.claude/rules/adrs-and-decisions.md` — index of ADRs and what each one
  settles (ADR 0021 = the mandatory CloudEvents envelope).

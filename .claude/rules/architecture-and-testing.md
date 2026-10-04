---
paths:
  - "cmd/**"
  - "internal/**"
  - "migrations/**"
  - "charts/**"
  - "features/**"
  - ".golangci.yml"
  - "Makefile"
---

# Architecture details, processes and testing standards

(Moved out of CLAUDE.md. The hard rules stay in CLAUDE.md; this is the detail.
Use `ls`/`grep` for the directory layout; it is not duplicated here.)

## Processes: four binaries, one writer per database (ADR 0007, 0009, 0010)

- `cmd/labor` — OLTP: REST API + Kafka consumer of `TaskCompleted` +
  outbox relay (publishes to the analytics and integration topics).
- `cmd/labor-projector` — analytics writer; consumes
  `warehouse.labor-performance.analytics`, the ONLY writer of the
  analytical DB.
- `cmd/labor-reports` — analytics reader; read-only pool; serves
  `GET /reports/performance[/freshness]`.
- `cmd/mcp` — MCP server (Streamable HTTP, `:8090`), four read-only
  tools over the OLTP store.
- Specs: `apis/openapi.yaml` (OLTP), `apis/openapi-reports.yaml` (reports),
  `apis/asyncapi.yaml` (consumed events + the analytics topic + the
  integration topic `warehouse.labor-performance.events`, ADR 0013).
- No REST or MCP surface is authenticated (ADR 0012 removed ADR 0011's
  bearer-key layer).
- `web/` is the `labor_mfe` micro-frontend remote (module federation, React,
  Vite) consuming `@warehouse/ui-kit`; mounted by the console at `/labor`.
  `charts/labor-performance/` is the Helm chart (fleet parity).

## Layering notes

- Domain layer is pure Go: no `chi`, no `pgx`, no `kafka-go`, no JSON tags.
- `internal/analytics/report` is the analytical read model (ADR 0007): it
  depends on NOTHING; OLTP layers must not import it and vice versa
  (arch-go enforced by `internal/architecture/architecture_test.go`).
- Domain aggregates: `standard` (LaborStandard, optional
  `TravelComponentSeconds`, ADR 0015), `performance` (TaskPerformance),
  `idleness` (IdlePeriod, ADR 0014), `shared` (value objects, events, errors).
- Outbound ports (`internal/application/ports`): StandardRepo, PerformanceRepo,
  IdlePeriodRepo, ProcessedEvents, EventPublisher, UnitOfWork, Clock.
- Transactional outbox lives in `internal/adapters/outbound/postgres`
  (`unit_of_work.go`, `outbox_publisher.go`, `outbox_relay.go`, ADR 0010).
- Inbound Kafka: `consumer.go` (`warehouse.fulfillment.events`,
  TaskCompleted only) and `analytics_consumer.go`
  (`warehouse.labor-performance.analytics`) under
  `internal/adapters/inbound/kafka`.

## Code standards and testing

- gofmt/go vet clean; every package has a doc comment.
- golangci-lint `v2.13.1` (pinned, matches CI). Copy `.golangci.yml` from
  `inventory-storage` when bootstrapping a new package.
- Table-driven tests: domain + application (in-memory adapters, fake Kafka
  reader — never hit a real broker in unit tests). One httptest per REST
  endpoint (success + error path). Build-tagged Kafka integration test
  using Testcontainers
  (`internal/adapters/inbound/kafka/consumer_integration_test.go`).
- Coverage gate: 90% on `./internal/domain/...,./internal/application/...,
  ./internal/analytics/...` (`make coverage`).
- RFC 7807 `application/problem+json` for every error response, identical
  shape across both OLTP and reports APIs.
- Other verification surfaces:

```bash
go test ./... -run TestFeatures -v                    # BDD (godog/Gherkin)
go test ./internal/architecture/... -v                # arch-fitness (arch-go)
go test -tags=integration ./... -race -count=1        # Postgres + Kafka (testcontainers)
make integration-kafka-testcontainers                 # just the Kafka consumer test
gremlins unleash ./internal/domain                    # mutation testing
spectral lint apis/openapi.yaml --ruleset .spectral.yaml --fail-severity=warn
spectral lint apis/openapi-reports.yaml --ruleset .spectral.yaml --fail-severity=warn
spectral lint apis/asyncapi.yaml --ruleset .spectral.asyncapi.yaml --fail-severity=warn
ct lint --charts charts/labor-performance --validate-maintainers=false --check-version-increment=false
```

CI (`.github/workflows/ci.yml`) runs the fleet-standard matrix; read the
workflow itself for the current job list.

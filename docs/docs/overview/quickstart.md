---
id: quickstart
title: Quickstart
sidebar_label: Quickstart
description: Build, test and run labor-performance locally — in memory, with Postgres, with the shared Kafka broker, the analytics pair and the MCP server — and the first calls to make.
---

# Quickstart

## Prerequisites

| Tool | Version | Needed for |
| --- | --- | --- |
| Go | `go 1.26.9` (from `go.mod`) | build, run, test |
| make | any | the quality gate (`Makefile`) |
| golangci-lint | v2.13.1 (pinned in CI) | `make lint`, `make check` |
| Docker + Compose | any recent | local Postgres (`docker-compose.yml`) and the `-tags=integration` tests (testcontainers) |
| gremlins v0.6.0, govulncheck, Spectral 6.16.3, Schemathesis 4.28.0 | optional | `make mutation*`, `make vuln`, `make api-lint`, `make contract` |
| Node 20+ | optional | the docs site (`docs/`). Node 22 for `web/`. |
| `kcat` | optional | publishing a test `TaskCompleted` |
| lefthook | optional | git hooks (`lefthook install`) |

The fleet runs **one** Kafka broker: the in-cluster broker that
`warehouse-infra` deploys into the kind cluster, reachable from the host
at `localhost:9092`. This repo's `docker-compose.yml` has no Kafka service.

## Build and test

```bash
make build        # go build ./...
make test         # go test ./... -race (no DB, no broker)
make check        # fmt-check vet build lint test
make check-all    # check + coverage gate + arch-test + bdd
make help         # every target
```

See [Testing](../development/testing.md) for the full list.

## 1. Run in memory (no database, no broker)

```bash
go run ./cmd/labor
# {"level":"INFO","msg":"database url not configured; using in-memory adapters"}
# {"level":"INFO","msg":"event publisher configured","publisher":"log","mode":"direct"}
# {"level":"INFO","msg":"http server listening","addr":":8080"}
```

The REST API works fully. The Kafka consumer keeps retrying
`localhost:9092` in the background without blocking HTTP. If the shared
broker **is** reachable, the consumer joins the consumer group
`labor-performance`, which is the in-cluster Deployment's group. To keep
a local process from stealing partitions, set
`KAFKA_CONSUMER_GROUP=labor-performance-local` (or any unique name), or
point `KAFKA_BROKERS` at nothing.

In memory there is no `Idempotency-Key` enforcement, no outbox and no
sweeper, because all three need Postgres.

## 2. First calls

These responses were captured from an in-memory `cmd/labor` built from
this branch. Timestamps will differ.

```bash
curl -s localhost:8080/healthz
# {"status":"ok"}
curl -s localhost:8080/readyz
# {"status":"ready"}

curl -s -X POST localhost:8080/standards \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: 6f1c2a9e-0000-4000-8000-000000000001' \
  -d '{"taskType":"PICK","expectedSeconds":45}'
# 201 {"taskType":"PICK","expectedSeconds":45,"effectiveFrom":"2026-10-09T12:46:48Z"}

# revise it (closes the previous record) with a travel component
curl -s -X POST localhost:8080/standards \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: 6f1c2a9e-0000-4000-8000-000000000002' \
  -d '{"taskType":"PICK","expectedSeconds":40,"travelComponentSeconds":12}'
# 201 {"taskType":"PICK","expectedSeconds":40,"travelComponentSeconds":12,"effectiveFrom":"..."}

curl -s localhost:8080/standards/PICK
# 200 {"taskType":"PICK","expectedSeconds":40,"travelComponentSeconds":12,"effectiveFrom":"..."}

curl -s localhost:8080/standards/pick
# 400 application/problem+json  .../unknown-task-type  (task types are case-sensitive)

curl -s localhost:8080/task-types/PICK/performance
# 200 {"taskType":"PICK","taskCount":0,"meanEfficiencyPct":null,"meanActualSeconds":null}

curl -s 'localhost:8080/task-types/PICK/utilization?window=30m'
# 200 {"taskType":"PICK","associates":0,"windowSeconds":1800,"taskSeconds":0,"idleSeconds":0,"openGapSeconds":0,"utilizationPct":null}

curl -s localhost:8080/associates/assoc-1/scorecard
# 404 application/problem+json  .../associate-not-found  (nothing ingested yet)
```

Always send a fresh `Idempotency-Key` on `POST /standards`. It is
required as soon as `DATABASE_URL` is set, and reusing a key replays the
first response ([ADR 0016](../adr/0016-idempotency-key-middleware.md)).
The full contract is in the [API Reference](/docs/api-reference/rest/labor-performance-api).

## 3. Run with Postgres

```bash
docker compose up -d postgres      # Postgres 16 on localhost:5435, user/password/db: labor
export DATABASE_URL='postgres://labor:labor@localhost:5435/labor?sslmode=disable'
go run ./cmd/labor                 # applies migrations/0001..0006 at startup
```

Migrations run automatically, and the sweeper and outbox-lag gauge
start. `POST /standards` now returns `400 idempotency-key-required`
when the header is missing.

## 4. Ingest a `TaskCompleted` from the shared broker

There is no seed script. Data comes in only through `POST /standards`
and `TaskCompleted` events. To score a task, publish a CloudEvents 1.0
structured-mode message:

```bash
export KAFKA_BROKERS=localhost:9092
export KAFKA_CONSUMER_GROUP=labor-performance-local
go run ./cmd/labor

# another terminal
echo '{"specversion":"1.0","id":"'"$(uuidgen)"'","source":"/warehouse/fulfillment-execution","type":"com.warehouse.wes.fulfillment-execution.task.TaskCompleted","subject":"task-1","time":"2026-10-09T12:00:00Z","datacontenttype":"application/json","data":{"task_id":"task-1","station_id":"station-1","work_unit_id":"wu-1","associate_id":"assoc-1","duration_seconds":52,"task_type":"PICK"}}' \
  | kcat -P -b localhost:9092 -t warehouse.fulfillment.events \
      -H 'content-type=application/cloudevents+json; charset=UTF-8'

curl -s localhost:8080/associates/assoc-1/scorecard
```

The task is scored against the standard active **at `time`**. With
`expectedSeconds` 40 and `duration_seconds` 52, `efficiencyPct` is
`100 × 40 / 52 ≈ 76.9`. A message that is not a valid CloudEvent goes to
`warehouse.fulfillment.events.dlq`.

:::caution
`warehouse.fulfillment.events` is the **shared** topic. Every in-cluster
consumer (labor-performance, wes-work-planning) also sees what you
publish. Use ids and subjects you can recognise and clean up after.
:::

## 5. Publish events and run the analytics pair

```bash
export EVENT_PUBLISHER=kafka       # with DATABASE_URL: through the transactional outbox
go run ./cmd/labor

docker compose up -d analytics-postgres   # localhost:5436, user/password/db: labor_analytics
export ANALYTICS_DATABASE_URL='postgres://labor_analytics:labor_analytics@localhost:5436/labor_analytics?sslmode=disable'
go run ./cmd/labor-projector       # ADMIN_ADDR :8091, runs migrations/analytics
go run ./cmd/labor-reports         # HTTP_ADDR :8092

curl -s 'localhost:8092/reports/performance?from=2026-10-09T00:00:00Z&to=2026-10-10T00:00:00Z'
curl -s localhost:8092/reports/performance/freshness
```

:::caution
With `EVENT_PUBLISHER=kafka`, a local `cmd/labor` publishes onto the
shared `warehouse.labor-performance.*` topics, which in-cluster
`workforce-management` consumes. The projector's consumer group
`labor-performance-analytics` is a constant. Against the shared broker,
a local projector joins the in-cluster projector's group and takes some
of its partitions. Prefer running the analytics pair in the cluster, or
against an isolated broker.
:::

## 6. Run the MCP server

```bash
MCP_ADDR=:8090 go run ./cmd/mcp    # add DATABASE_URL to read the same store as cmd/labor

curl -s localhost:8090/healthz
# {"status":"ok"}

curl -si localhost:8090/ \
  -H 'Content-Type: application/json' -H 'Accept: application/json, text/event-stream' \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"curl","version":"0"}}}'
# 200, header Mcp-Session-Id: <id>, SSE body with serverInfo {"name":"labor-performance-mcp","version":"1.0.0"}
```

Send `notifications/initialized`, then `tools/list` or `tools/call` with
the `Mcp-Session-Id` header. Tool inputs are in [MCP tools](../mcp/tools.md).
Without `DATABASE_URL`, `cmd/mcp` has its **own** empty in-memory store,
so tools return no data even when `cmd/labor` has some.

## 7. Docs site

```bash
cd docs
npm ci
npm run build          # onBrokenLinks/onBrokenAnchors: 'throw'
npm run start          # http://localhost:3000/labor-performance/
```

If you changed `apis/openapi.yaml` or `apis/openapi-reports.yaml`,
regenerate the API reference with
`npm run clean-api-docs:all && npm run gen-api-docs:all`. The CI job
`docs-api-drift` fails on any diff.

## 8. Operator remote (`web/`)

```bash
cd web && npm install && npm run dev     # http://localhost:5187
```

In standalone dev the remote calls `http://localhost:8088`
(`DEV_API_BASE` in `web/src/config.ts`), not `cmd/labor`'s default
`:8080`. Start the API with `HTTP_ADDR=:8088` for that. Inside the
console it reads `window.__WAREHOUSE_CONFIG__.apiOrigin`.

Next: [Configuration](../operations/configuration.md) for every variable,
and the [Runbook](../operations/runbook.md) for the cluster.

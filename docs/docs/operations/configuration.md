---
id: configuration
title: Configuration reference
sidebar_label: Configuration
description: Every environment variable each of the four labor-performance binaries reads, with its default, whether it is required, what it does and the file that reads it.
---

# Configuration reference

All four binaries are configured only through environment variables. There
is no config file and no flag parsing. This page lists every variable each
binary reads, taken from its composition root (`cmd/<binary>/main.go`) and
the adapters it wires. Test-only variables (the integration suites start
their own containers) are not listed.

How values are parsed:

- **Strings** use a `getenv(key, fallback)` helper: an unset **or empty**
  variable falls back to the default.
- **Durations** (`cmd/labor` only) use `durationEnv` in `cmd/labor/main.go`.
  The value is a Go duration (`500ms`, `30s`, `1h`, `168h`). A value that
  does not parse, **or that is `0` or negative**, silently falls back to
  the default. You cannot disable the housekeeping sweeper or keep rows
  forever by setting `0`. See [the note below](#zero-does-not-disable-housekeeping).
- **`IDLE_GAP_CAP_SECONDS`** is an integer. A malformed, zero or negative
  value falls back to 3600.
- **Lists** (`KAFKA_BROKERS`, `CORS_ALLOWED_ORIGINS`) are split on `,`
  without trimming spaces, so write them without spaces.

The Helm chart column shows the `charts/labor-performance/values.yaml` key
that sets the variable. "—" means the chart never sets it, so the binary
default applies unless you add it through `extraEnv` (api) or
`mcp.extraEnv` (mcp).

## `cmd/labor` (OLTP service: REST API, Kafka consumer, outbox relay, sweeper)

| Variable | Default | Required? | Meaning | Read in | Chart value |
| --- | --- | --- | --- | --- | --- |
| `HTTP_ADDR` | `:8080` | no | Listen address of the REST API (`/standards`, scorecard, performance, utilization, `/healthz`, `/readyz`). | `cmd/labor/main.go` | `config.httpAddr` |
| `DATABASE_URL` | *(unset)* | no | OLTP Postgres DSN for the runtime `pgxpool` (`MaxConns` 10, `statement_timeout` 5s). **Unset means in-memory adapters**: no persistence, no transactional outbox, no `Idempotency-Key` middleware, no sweeper, no outbox-lag gauge. | `cmd/labor/main.go` | `database.url` / `database.existingSecret` + `database.existingSecretKey` |
| `MIGRATIONS_DATABASE_URL` | value of `DATABASE_URL` | no | Direct (non-PgBouncer) DSN used **only** by the golang-migrate step at startup. golang-migrate takes a session-scoped advisory lock, which PgBouncer transaction pooling cannot hold ([ADR 0020](../adr/0020-migrations-direct-postgres-connection.md)). | `cmd/labor/main.go` | `database.migrationsExistingSecretKey` (secret key, `optional: true`) |
| `MIGRATIONS_PATH` | `migrations` | no | Directory of the OLTP `*.up.sql` files (`migrations/0001`–`0006`). | `cmd/labor/main.go` | `config.migrationsPath` |
| `KAFKA_BROKERS` | `localhost:9092` | no | Comma-separated brokers. Used by the `warehouse.fulfillment.events` consumer, its DLQ writer and, with `EVENT_PUBLISHER=kafka`, the publishers and relay sink. | `cmd/labor/main.go` | `kafka.brokers` (only when `kafka.enabled=true`) |
| `KAFKA_CONSUMER_GROUP` | `labor-performance` | no | Consumer group id on `warehouse.fulfillment.events`. | `cmd/labor/main.go` | `config.kafkaConsumerGroup` |
| `EVENT_PUBLISHER` | `log` | no | `log` writes domain events to the structured log only. `kafka` (case-insensitive) also publishes them: to `warehouse.labor-performance.analytics` (all three events) and `warehouse.labor-performance.events` (`TaskPerformanceRecorded` only). With `DATABASE_URL` set, `kafka` goes through the transactional outbox (`outbox_events` + in-process relay, [ADR 0010](../adr/0010-transactional-outbox.md)). Without a database it writes straight to the broker. | `cmd/labor/main.go` | `config.eventPublisher` |
| `OUTBOX_RELAY_INTERVAL` | `1s` | no | How long the relay sleeps after a pass that published fewer than 100 rows. Used only in outbox mode. | `cmd/labor/main.go` | `config.outboxRelayInterval` |
| `HOUSEKEEPING_INTERVAL` | `1h` | no | How often the housekeeping sweeper runs ([ADR 0023](../adr/0023-housekeeping-sweeper.md)). Only runs with `DATABASE_URL`. `0` falls back to `1h`. | `cmd/labor/main.go` | `config.housekeepingInterval` |
| `IDEMPOTENCY_KEY_TTL` | `24h` | no | Age after which `idempotency_keys` rows are deleted. A retry older than this is treated as a new request. `0` falls back to `24h`. | `cmd/labor/main.go` | `config.idempotencyKeyTtl` |
| `OUTBOX_RETENTION` | `168h` (7 days) | no | Age after which **published** `outbox_events` rows are deleted. Unpublished rows are never deleted. `0` falls back to `168h`. | `cmd/labor/main.go` | `config.outboxRetention` |
| `IDLE_GAP_CAP_SECONDS` | `3600` | no | Upper bound on a recorded idle gap. A longer gap is stored as the cap with `capped = true` ([ADR 0014](../adr/0014-labor-utilization-idleness.md)). | `cmd/labor/main.go` (default in `internal/application/usecases/record_task_performance.go`) | — |
| `CORS_ALLOWED_ORIGINS` | `http://localhost:5173,http://localhost:5187` | no | Browser origins allowed to call the REST API. Allowed methods are `GET` and `POST`. Allowed headers are `Content-Type`, `Authorization` and `Idempotency-Key`. | `internal/adapters/inbound/http/server.go` | `config.corsAllowedOrigins` |
| `LOG_LEVEL` | `info` | no | `debug`, `info`, `warn` or `error` (case-insensitive). Anything else means `info`. | `cmd/labor/main.go` | `config.logLevel` |
| `OTEL_SERVICE_NAME` | `labor-performance` | no | OTel `service.name`, also used as the HTTP metrics/span service label. | `cmd/labor/main.go` | `otel.serviceName` (only when `otel.enabled=true`) |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `localhost:4317` | no | OTLP/gRPC Collector endpoint (insecure) for traces and metrics. Export does not block startup: if no Collector is listening, telemetry is dropped and the service still runs. | `cmd/labor/main.go` | `otel.endpoint` (only when `otel.enabled=true`) |
| `SERVICE_VERSION` | `dev` | no | OTel `service.version`. A build-time `-ldflags "-X main.version=..."` wins if set. The `Dockerfile` does not set one. | `cmd/labor/main.go` | image tag or `Chart.AppVersion` |
| `ENVIRONMENT` | `local` | no | OTel `deployment.environment.name` resource attribute. | `internal/adapters/outbound/telemetry/telemetry.go` | `environment` |

## `cmd/labor-projector` (analytics writer)

The projector is the only writer of the analytical database. It never
opens `DATABASE_URL`. Its consumer group is the constant
`labor-performance-analytics` (`internal/adapters/inbound/kafka/analytics_consumer.go`)
and cannot be configured.

| Variable | Default | Required? | Meaning | Read in | Chart value |
| --- | --- | --- | --- | --- | --- |
| `ANALYTICS_DATABASE_URL` | — | **yes** | Read-write DSN of the analytical Postgres (`MaxConns` 5, `statement_timeout` 10s). The process exits with `ANALYTICS_DATABASE_URL is required` when it is unset. | `cmd/labor-projector/main.go` | secret key `ANALYTICS_DATABASE_URL` (`analytics.database.projectorUrl` or `analytics.database.existingSecret`) |
| `ANALYTICS_MIGRATIONS_PATH` | `migrations/analytics` | no | Directory of the analytical `*.up.sql` files. The projector runs them at startup. | `cmd/labor-projector/main.go` | `analytics.migrationsPath` |
| `KAFKA_BROKERS` | `localhost:9092` | no | Brokers for `warehouse.labor-performance.analytics`. | `cmd/labor-projector/main.go` | `kafka.brokers` (always set, even when `kafka.enabled=false`) |
| `ADMIN_ADDR` | `:8091` | no | Listen address of the admin server, which serves only `GET /healthz`. | `cmd/labor-projector/main.go` | `analytics.projector.adminAddr` |
| `LOG_LEVEL` | `info` | no | `debug`, `info`, `warn`/`warning` or `error` (case-insensitive). | `cmd/labor-projector/main.go` | — |
| `OTEL_SERVICE_NAME` | `labor-performance-projector` | no | OTel `service.name`. | `cmd/labor-projector/main.go` | hard-coded `labor-performance-projector` when `otel.enabled=true` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `localhost:4317` | no | OTLP/gRPC Collector endpoint. | `cmd/labor-projector/main.go` | `otel.endpoint` |
| `SERVICE_VERSION` | `dev` | no | OTel `service.version` (`-ldflags -X main.version` wins). | `cmd/labor-projector/main.go` | image tag (only when `otel.enabled=true`) |
| `ENVIRONMENT` | `local` | no | OTel `deployment.environment.name`. | `internal/adapters/outbound/telemetry/telemetry.go` | `environment` (only when `otel.enabled=true`) |

## `cmd/labor-reports` (analytics reader)

The reader opens the analytical database over a read-only pool: every
connection sets `default_transaction_read_only=on`, `MaxConns` 5 and
`statement_timeout` 15s. It never migrates and never writes.

| Variable | Default | Required? | Meaning | Read in | Chart value |
| --- | --- | --- | --- | --- | --- |
| `ANALYTICS_DATABASE_URL` | — | **yes** | DSN of the analytical Postgres. Use a read-only role. The process exits with `ANALYTICS_DATABASE_URL is required` when it is unset. | `cmd/labor-reports/main.go` | secret key `ANALYTICS_READER_DATABASE_URL` (from `analytics.database.reportsUrl`, falling back to `projectorUrl`) |
| `HTTP_ADDR` | `:8092` | no | Listen address of the reports API (`/reports/performance`, `/reports/performance/freshness`, `/healthz`). | `cmd/labor-reports/main.go` | `analytics.reports.httpAddr` |
| `CORS_ALLOWED_ORIGINS` | `http://localhost:5173,http://localhost:5187` | no | Browser origins allowed to call the reports API (`GET` only). | `internal/adapters/inbound/http/server.go` | — |
| `LOG_LEVEL` | `info` | no | `debug`, `info`, `warn`/`warning` or `error` (case-insensitive). | `cmd/labor-reports/main.go` | — |
| `OTEL_SERVICE_NAME` | `labor-performance-reports` | no | OTel `service.name`. | `cmd/labor-reports/main.go` | hard-coded `labor-performance-reports` when `otel.enabled=true` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `localhost:4317` | no | OTLP/gRPC Collector endpoint. | `cmd/labor-reports/main.go` | `otel.endpoint` |
| `SERVICE_VERSION` | `dev` | no | OTel `service.version` (`-ldflags -X main.version` wins). | `cmd/labor-reports/main.go` | image tag (only when `otel.enabled=true`) |
| `ENVIRONMENT` | `local` | no | OTel `deployment.environment.name`. | `internal/adapters/outbound/telemetry/telemetry.go` | `environment` (only when `otel.enabled=true`) |

## `cmd/mcp` (MCP server, Streamable HTTP)

`cmd/mcp` reads the same OLTP store as `cmd/labor` and, like it, runs the
OLTP migrations at startup. Migrations are idempotent, so the two
binaries can share one directory.

| Variable | Default | Required? | Meaning | Read in | Chart value |
| --- | --- | --- | --- | --- | --- |
| `MCP_ADDR` | `:8090` | no | Listen address. The MCP Streamable HTTP endpoint is mounted at `/`, and `GET /healthz` returns `{"status":"ok"}`. | `cmd/mcp/main.go` | `mcp.httpAddr` |
| `DATABASE_URL` | *(unset)* | no | OLTP Postgres DSN. Unset means in-memory repositories, which are empty and never populated, because only `cmd/labor` writes. | `cmd/mcp/main.go` | same secret as the api Deployment |
| `MIGRATIONS_DATABASE_URL` | value of `DATABASE_URL` | no | Direct DSN for the migration step only ([ADR 0020](../adr/0020-migrations-direct-postgres-connection.md)). | `cmd/mcp/main.go` | `database.migrationsExistingSecretKey` |
| `MIGRATIONS_PATH` | `migrations` | no | OLTP migrations directory. | `cmd/mcp/main.go` | `config.migrationsPath` |
| `LOG_LEVEL` | `info` | no | `debug`, `warn` or `error`. **Case-sensitive**: `DEBUG` is read as `info`, unlike the other binaries. | `cmd/mcp/main.go` | `config.logLevel` |
| `OTEL_SERVICE_NAME` | `labor-performance-mcp` | no | OTel `service.name`. | `cmd/mcp/main.go` | hard-coded `labor-performance-mcp` when `otel.enabled=true` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `localhost:4317` | no | OTLP/gRPC Collector endpoint. | `cmd/mcp/main.go` | `otel.endpoint` |
| `SERVICE_VERSION` | `dev` | no | OTel `service.version`. This binary has no `main.version` ldflags hook. | `cmd/mcp/main.go` | image tag |
| `ENVIRONMENT` | `local` | no | OTel `deployment.environment.name`. | `internal/adapters/outbound/telemetry/telemetry.go` | `environment` |

## Zero does not disable housekeeping

`charts/labor-performance/values.yaml` and the comment on `startHousekeeping`
in `cmd/labor/main.go` both say that `0` disables the sweeper, or keeps
rows forever when set as a TTL or retention. The `Sweeper` type does
support that (`<=0` disables). But the composition root parses all three
variables with `durationEnv`, which replaces any value `<= 0` with the
default before the sweeper ever sees it. With the code as it is today:

- `HOUSEKEEPING_INTERVAL=0` runs the sweeper every hour.
- `IDEMPOTENCY_KEY_TTL=0` deletes keys after 24 hours.
- `OUTBOX_RETENTION=0` deletes published outbox rows after 7 days.

To make the sweep effectively a no-op, set a very large duration such as
`87600h`.

## Fixed values that are not configurable

| Setting | Value | Where |
| --- | --- | --- |
| OLTP pool | `MaxConns` 10, `statement_timeout` 5s | `internal/adapters/outbound/postgres/pool.go` |
| Analytics writer pool | `MaxConns` 5, `statement_timeout` 10s | `internal/adapters/outbound/analyticsstore/pool.go` |
| Analytics reader pool | `MaxConns` 5, `statement_timeout` 15s, read-only | `internal/adapters/outbound/analyticsstore/pool.go` |
| Boot retry | 5 attempts, 1s base delay doubling (about 31s total) for migrations and DB ping | `internal/adapters/outbound/bootretry/retry.go` |
| Fulfillment consumer retry | 3 attempts, backoff from 100ms up to 2s, then DLQ | `internal/adapters/inbound/kafka/consumer.go` |
| Analytics consumer retry | unbounded, backoff from 200ms doubling up to 5s | `internal/adapters/inbound/kafka/analytics_consumer.go` |
| Kafka writers | `RequiredAcks=RequireAll`, `BatchTimeout` 10ms, `Hash` balancer, auto topic creation | `internal/adapters/outbound/kafka/writer_config.go` |
| Outbox relay batch | 100 rows per pass | `internal/adapters/outbound/postgres/outbox_relay.go` |
| Sweeper DELETE batch | 1000 rows | `internal/adapters/outbound/postgres/sweeper.go` |
| Metric export interval | 30s | `internal/adapters/outbound/telemetry/telemetry.go` |
| Graceful shutdown budget | 10s (HTTP drain, relay, consumer), 5s for the telemetry flush | `cmd/*/main.go` |
| Utilization default window | 1h (REST `?window=` absent or unparsable, MCP `windowSeconds` <= 0) | `internal/application/usecases/get_utilization.go` |

See [Runbook](./runbook.md) for how these values behave in a deployment.

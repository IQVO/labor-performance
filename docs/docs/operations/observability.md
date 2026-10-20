---
id: observability
title: Observability
sidebar_label: Observability
description: Every OpenTelemetry metric, span and log field labor-performance emits, how telemetry is exported, the Grafana dashboard that exists for it, and suggested alerts.
---

# Observability

All four binaries call `telemetry.Setup`
(`internal/adapters/outbound/telemetry/telemetry.go`) once at boot. It
installs:

- an OTLP/gRPC **trace** exporter and **metric** exporter, both insecure,
  both pointed at `OTEL_EXPORTER_OTLP_ENDPOINT` (default `localhost:4317`);
- a periodic metric reader with a **30s** export interval;
- the W3C `traceparent` + `baggage` propagator;
- Go runtime metrics (`go.opentelemetry.io/contrib/instrumentation/runtime` v0.71.0);
- resource attributes `service.name` (`OTEL_SERVICE_NAME`), `service.version`
  (`SERVICE_VERSION`) and `deployment.environment.name` (`ENVIRONMENT`).

Export never blocks startup. If no Collector is listening, telemetry is
dropped and the service runs normally. **No binary exposes a Prometheus
`/metrics` endpoint.** Every metric is pushed over OTLP, and the Collector
in `warehouse-infra` turns it into Prometheus series. Dots become
underscores, and counters get a `_total` suffix and histograms a unit
suffix. For example, `labor_performance.standards.defined` becomes
`labor_performance_standards_defined_total`.

## Metrics

### Instruments defined in this repo

| Instrument | Type | Unit | Attributes | Emitted by | Meaning |
| --- | --- | --- | --- | --- | --- |
| `labor_performance.standards.defined` | Int64Counter | `{standard}` | `outcome` = `accepted` \| `rejected` | `cmd/labor` (`internal/adapters/outbound/telemetry/metrics.go`, recorded in `DefineStandard`) | One per `POST /standards` that reaches the use case. `rejected` is recorded only when the `LaborStandard` aggregate refuses the values: `expectedSeconds <= 0`, a negative travel component, or a travel component larger than `expectedSeconds`. An unknown task type (400), malformed JSON, a missing `Idempotency-Key` or a 409 conflict is **not** counted. |
| `labor_performance.mcp.tool.invocations` | Int64Counter | `{call}` | `mcp.tool.name` (a registered tool, or `unknown`), `outcome` = `ok` \| `error` \| `denied` | `cmd/mcp` (`internal/adapters/inbound/mcp/metrics.go`, a receiving middleware on `tools/call`) | One per `tools/call` request ([ADR 0033](../adr/0033-mcp-invocation-and-denial-counters.md), charter §9). `ok`: the handler returned a result. `error`: the handler ran and returned an error, including domain rejections and the handlers' own input checks (empty `associateId`, unknown task type). `denied`: the SDK refused the call before the handler ran. `tools/list`, resources and prompts are not counted. |
| `labor_performance.mcp.tool.denials` | Int64Counter | `{call}` | `mcp.tool.name` (a registered tool, or `unknown`), `reason` = `unknown_tool` \| `invalid_arguments` | `cmd/mcp` (same middleware) | One per call the SDK refused before the handler ran: an unregistered tool (`unknown_tool`, label `unknown` so invented names cannot create series) or arguments that failed the input schema (`invalid_arguments`: wrong type, unknown extra argument, missing required field). There is no identity layer, so nothing is denied for a missing scope. |
| `labor_performance.outbox.lag_seconds` | Float64ObservableGauge | `s` | none | `cmd/labor` (`internal/adapters/outbound/postgres/outbox_metrics.go`) | Age of the oldest `outbox_events` row with `published_at IS NULL`, sampled at each collection. `0` when the outbox is drained. Always registered: the outbox is the only publish path. |

Meter scopes: `github.com/claudioed/labor-performance` for the standards
counter,
`github.com/claudioed/labor-performance/internal/adapters/inbound/mcp`
for the two MCP counters, and
`github.com/claudioed/labor-performance/internal/adapters/outbound/postgres`
for the lag gauge. After Collector translation the MCP counters are
`labor_performance_mcp_tool_invocations_total{mcp_tool_name, outcome}` and
`labor_performance_mcp_tool_denials_total{mcp_tool_name, reason}`.

### Instruments from libraries

| Instrument | Type | Unit | Attributes | Emitted by |
| --- | --- | --- | --- | --- |
| `http.server.request.duration` | Float64Histogram | `s` | `http.method`, `http.scheme`, `http.route` (when a chi route matched). The scope also carries `service.name`. **There is no status-code attribute** in otelchi v0.12.3. | All four binaries: `cmd/labor` and `cmd/labor-reports` through their chi routers, `cmd/mcp` through its chi wrapper, `cmd/labor-projector` around its admin `ServeMux` (no `http.route` there). |
| `go.memory.used`, `go.memory.limit`, `go.memory.allocated`, `go.memory.allocations`, `go.memory.gc.goal`, `go.goroutine.count`, `go.processor.limit`, `go.config.gogc` | runtime observables | per OTel Go semconv | none | All four binaries (`runtime.Start`). |

kafka-go, pgx and the MCP SDK emit no metrics here. Consumer lag is
not instrumented in-process. Read it from the broker for the groups
`labor-performance` and `labor-performance-analytics`.

## Traces

| Span name | Kind | Where | Key attributes |
| --- | --- | --- | --- |
| Route pattern, for example `/standards/{taskType}` or `/reports/performance` | server | otelchi middleware on the `cmd/labor` and `cmd/labor-reports` routers | standard otelchi HTTP attributes |
| `GET /healthz` or `POST /` (method plus route) | server | `cmd/mcp` (`otelchi.WithRequestMethodInSpanName`) | standard otelchi HTTP attributes |
| `mcp.tool <tool name>`, for example `mcp.tool get_associate_scorecard` | internal | `internal/adapters/inbound/mcp/tools.go` | `mcp.tool.name`, `mcp.tool.outcome` = `ok` \| `error`, status `Error` on failure. Only handled calls get this span: a call the SDK denies (unknown tool, schema-invalid arguments) is visible in `labor_performance.mcp.tool.denials` and the HTTP server span only. |
| `kafka.consume warehouse.fulfillment.events` | consumer | `internal/adapters/inbound/kafka/consumer.go` | `messaging.system=kafka`, `messaging.operation.name=consume`, `messaging.destination.name`, `messaging.kafka.offset`, `messaging.destination.partition.id`, `messaging.message.id`, `cloudevents.event_type`, `cloudevents.event_source`, `cloudevents.event_subject` |
| `kafka.consume warehouse.labor-performance.analytics` | consumer | `internal/adapters/inbound/kafka/analytics_consumer.go` (projector) | messaging attributes plus the four CloudEvents attributes above |
| `kafka.publish warehouse.labor-performance.analytics` / `kafka.publish warehouse.labor-performance.events` | producer | direct-publish path (`EVENT_PUBLISHER=kafka` without a database) and the outbox `RelaySink` (named after the first message's topic) | `messaging.system=kafka`, `messaging.operation.name=publish`, `messaging.destination.name` |

Trace context crosses Kafka in the message headers. In outbox mode the
`traceparent` is captured when the event is **encoded**, inside the
request or consume span, and stored in `outbox_events.headers`. The
projector's consume span is therefore a child of the original request,
not of the relay pass. A consume span is a child of the producer span
found in the message headers, so a `TaskCompleted` from
`fulfillment-execution` keeps its upstream trace.

Two integration tests (real Postgres and Kafka through testcontainers,
`internal/adapters/outbound/postgres/outbox_trace_kafka_integration_test.go`,
gap fix D27) pin this: `TestOutboxTrace_TraceparentCrossesTheOutboxToKafka`
proves a request span's `traceparent` is stored on the outbox row, sent as
a Kafka header by a relay pass running on a background context, and
extracted by a consumer as the request span; and
`TestOutboxTrace_RowWithoutTraceHeadersRelaysUnchanged` proves a row with
no stored trace context relays unchanged and without any
`traceparent`/`tracestate` header.

## Logs

Every binary logs JSON to stdout through `slog.NewJSONHandler`, wrapped
in `telemetry.TraceHandler`. Each line has `time`, `level` and `msg`.
`trace_id` and `span_id` are added whenever the log call uses a context
that carries a span (`*Context` variants). `LOG_LEVEL` sets the threshold.

Request log line (`internal/adapters/inbound/http/logging.go`, `cmd/labor`
and `cmd/labor-reports`): `msg="http request"` with `method`, `route`
(the route pattern, or the CR/LF-stripped raw path for a 404), `status`,
`bytes`, `duration_ms` and `request_id` (chi `RequestID`).

Operationally useful messages:

| Message | Level | Binary | Fields | Meaning |
| --- | --- | --- | --- | --- |
| `event publisher configured` | INFO | labor | `publisher` (`kafka`), `mode` (`outbox`), `topics`, `brokers`, `outbox_max_attempts` | The publishing path, always the Kafka outbox ([ADR 0034](../adr/0034-postgres-and-kafka-are-the-only-runtime.md)). |
| `housekeeping sweeper configured` | INFO | labor | `interval`, `enabled`, `idempotency_key_ttl`, `outbox_retention` | Sweeper settings in effect at boot (`enabled=false` when `HOUSEKEEPING_INTERVAL` is `0` or negative). |
| `retrying` / `succeeded after retry` | WARN / INFO | all | `op`, `attempt`, `in`, `err` | Boot-time dial retry for migrations or DB ping. |
| `invalid cloudevent, sending to dead-letter topic` | WARN | labor | `topic`, `partition`, `offset`, `dlq_topic`, `error` | Poison message dead-lettered. |
| `exhausted retries, sending to dead-letter topic` | WARN | labor | `topic`, `dlq_topic`, `id`, `type`, `attempts`, `error` | `RecordTaskPerformance` failed 3 times. |
| `skipping idle gap: out-of-order Kafka delivery` | WARN | labor | `associate_id`, `task_id`, `error` | Expected occasionally. No idle period is recorded for that task. |
| `outbox relay pass failed` | ERROR | labor | `error` | A send or DB error. The relay retries after `OUTBOX_RELAY_INTERVAL`. |
| `outbox relay: row reached the attempt cap and is excluded from future claims; ...` | ERROR | labor | `outbox_id`, `topic`, `event_type`, `key`, `attempts`, `max_attempts`, `error` | Logged once, when a row's failed sends reach `OUTBOX_MAX_ATTEMPTS` ([ADR 0032](../adr/0032-outbox-relay-attempt-cap.md)). The row is no longer claimed until re-armed. |
| `housekeeping sweep` / `housekeeping sweep failed` | INFO / ERROR | labor | `idempotency_keys_deleted`, `outbox_events_deleted`, `error` | Sweeper result. |
| `outbox lag gauge unavailable` | WARN | labor | `error` | The gauge could not be registered at boot. |
| `analytics message handling failed; retrying` | ERROR | projector | `topic`, `offset`, `attempt`, `retry_in`, `error` | Projection hit a transient DB error. The same message is retried. |
| `skipping invalid cloudevent on analytics topic` / `skipping analytics event with undecodable data` | WARN | projector | `topic`, `id`, `type`, `error` | Bad input, skipped and committed. |
| `kafka consumer did not stop before the shutdown deadline` / `outbox relay did not stop before the shutdown deadline` | WARN | labor | — | Shutdown drain exceeded 10s. |

## Dashboards

`warehouse-infra` (on its `develop` branch) provisions a Grafana dashboard
for this context, **Warehouse — Labor Performance**, at
`terraform/dashboards/contexts/labor-performance.json`, generated by
`scripts/gen-context-dashboards.py`. Its panels:

- Kong request rate, 5xx rate and p95 latency for the context's HTTPRoute;
- `http_server_request_duration_seconds` rate by `service_name` and
  `http_route`, and p95 by process. Every `service_name` matching
  `labor-performance.*` is included, which covers OLTP, projector,
  reports and MCP;
- a 5xx-by-process panel that filters on `http_response_status_code` or
  `http_status_code`. otelchi v0.12.3 sets neither attribute, so this
  panel stays empty. Use the Kong 5xx panel instead;
- `sum by (outcome) (rate(labor_performance_standards_defined_total[5m]))`;
- goroutines and Go memory used;
- a Loki log stream, log volume by level, and errors/warnings only.

There is no panel yet for `labor_performance_outbox_lag_seconds` or for
Kafka consumer-group lag.

## Suggested alerts

| Alert | Expression (Prometheus, after Collector translation) | Why |
| --- | --- | --- |
| Outbox stuck | `max(labor_performance_outbox_lag_seconds) > 60` for 5m | The relay cannot reach Kafka or keeps failing on one row. Downstream (`workforce-management`, projector) stops receiving events. A row excluded by `OUTBOX_MAX_ATTEMPTS` also keeps this gauge high while newer events flow: look for the ERROR `outbox relay: row reached the attempt cap`. |
| Reports stale | `GET /reports/performance/freshness` → `lagSeconds` > 900. This is a blackbox or ops-agent check, not a metric. | The projector has stopped applying events. |
| Fulfillment consumer lag | broker lag of group `labor-performance` on `warehouse.fulfillment.events` keeps growing | Scorecards fall behind real completions. |
| DLQ growth | any new message on `warehouse.fulfillment.events.dlq` | A poison message, or a persistent DB failure during scoring. |
| MCP denials | `sum(rate(labor_performance_mcp_tool_denials_total[15m])) / sum(rate(labor_performance_mcp_tool_invocations_total[15m])) > 0.2` | An agent keeps calling tools that do not exist or sending ill-typed arguments: usually a prompt or client regression in `warehouse-ops-agent`. |
| Standard rejections | `sum(rate(labor_performance_standards_defined_total{outcome="rejected"}[15m])) / sum(rate(labor_performance_standards_defined_total[15m])) > 0.5` | A broken input form or feed is submitting invalid standards. |
| Crash loop | `kube_pod_container_status_restarts_total` rising for the release's pods | Migrations, DB or broker unreachable for longer than the boot retry budget. |

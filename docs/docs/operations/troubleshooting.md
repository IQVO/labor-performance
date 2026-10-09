---
id: troubleshooting
title: Troubleshooting
sidebar_label: Troubleshooting
description: Symptom, cause, check and fix for the failure modes visible in labor-performance's code — boot failures, readiness, Kafka lag and DLQ, outbox lag, RFC 7807 problem types, idempotency-key conflicts and reports freshness.
---

# Troubleshooting

Each row starts from what you observe, names the cause in the code,
gives the check that confirms it, and the fix. Environment variables are
described in [Configuration](./configuration.md). Procedures such as DLQ
re-drive and read-model rebuild are in the [Runbook](./runbook.md).

All REST errors are RFC 7807 `application/problem+json` bodies with
`type` = `https://errors.labor-performance.warehouse-systems.dev/<slug>`
(`internal/adapters/inbound/http/errors.go`). The slugs are listed in
[Problem types](#problem-types).

## Boot and readiness

| Symptom | Cause | Check | Fix |
| --- | --- | --- | --- |
| Pod in `CrashLoopBackOff`. The last log line is `service exited with error` with `run migrations (after 5 attempts): …` | The OLTP database is unreachable, the DSN is wrong, or a migration failed. Boot retry gives up after about 31s. | `kubectl logs --previous` and look for `retrying op="run migrations"` lines. Try the DSN with `psql`. | Fix the Secret (`DATABASE_URL`, `MIGRATIONS_DATABASE_URL`) or the network, then `kubectl rollout restart`. |
| Migrations fail intermittently on rollout with `unnamed prepared statement does not exist` or `canceling statement due to statement timeout` | `MIGRATIONS_DATABASE_URL` is unset, so migrations go through PgBouncer transaction pooling and golang-migrate's advisory lock does not hold ([ADR 0020](../adr/0020-migrations-direct-postgres-connection.md)). | `kubectl exec … env \| grep MIGRATIONS_DATABASE_URL` | Add the `MIGRATIONS_DATABASE_URL` key (direct DSN) to the database Secret. |
| Boot fails with `Dirty database version N. Fix and force version.` | An earlier migration failed halfway and golang-migrate marked `schema_migrations` dirty. | `SELECT version, dirty FROM schema_migrations;` | Repair the schema by hand, then `migrate -path migrations -database "$MIGRATIONS_DATABASE_URL" force <N>`. |
| `labor-projector` or `labor-reports` exits immediately with `ANALYTICS_DATABASE_URL is required` | `analytics.enabled=true` but neither `analytics.database.projectorUrl` nor `analytics.database.existingSecret` is set, so the chart rendered no Secret. | `kubectl get secret` for the analytics Secret. | Set the analytics DSN values or the existing Secret. |
| Pod never becomes Ready, and the startup probe fails for 60s | Boot is still inside the retry budget, or the process exits before it listens (see the rows above). `/healthz` and `/readyz` are only served **after** migrations and the DB ping succeed. | `kubectl describe pod` (probe events) plus the logs. | Fix the dependency. Note that `/readyz` never checks Postgres or Kafka once the process is up. |
| `/readyz` returns 503 `{"status":"not_ready"}` | The pod received `SIGTERM` and is draining ([ADR 0017](../adr/0017-kafka-dlq-and-graceful-shutdown.md)). This is the only way readiness flips. | Pod is `Terminating`. | Expected. The pod exits within the 10s drain and the 30s grace period. |
| Service starts but nothing persists across restarts | `DATABASE_URL` is empty, so the in-memory adapters are used. | Log line `database url not configured; using in-memory adapters`. | Set `database.url` or `database.existingSecret`. |

## Kafka ingestion (`warehouse.fulfillment.events`)

| Symptom | Cause | Check | Fix |
| --- | --- | --- | --- |
| Scorecards stay 404 and task-type performance stays at `taskCount: 0` in the cluster | `kafka.enabled=false` (the chart default) leaves `KAFKA_BROKERS` unset, so the consumer dials `localhost:9092` inside the pod and never connects. | `kubectl exec … env \| grep KAFKA_BROKERS` | Set `kafka.enabled=true` and `kafka.brokers`. |
| Consumer group `labor-performance` lag grows | Slow DB (5s `statement_timeout`), every message taking all 3 attempts, or too few replicas for the partitions. | `kafka-consumer-groups.sh --describe --group labor-performance`. Look for `exhausted retries` log lines. | Fix the DB, or scale the api (HPA `autoscaling.api`, up to 4). Each extra replica takes partitions only while there are idle partitions. |
| Messages land on `warehouse.fulfillment.events.dlq` | Either the message is not a valid CloudEvents 1.0 event (dead-lettered immediately), or `RecordTaskPerformance` failed 3 times. | Read the DLQ headers `x-dlq-error` and `x-dlq-failed-at`. | Poison message: fix the producer. Transient: fix the cause, then re-drive ([Runbook](./runbook.md#re-drive-dead-lettered-taskcompleted-messages)). |
| Consumer process exits with `kafka: publish to dead-letter topic: …` | The DLQ write itself failed after about 10s of topic-not-ready retries, or the broker refused it. The offset is **not** committed, so nothing is lost. | Broker health, and topic auto-creation on the broker. | Fix the broker. The pod restarts and the message is redelivered. |
| A `TaskCompleted` is recorded but `efficiencyPct` is `null` | No standard was active for that task type **at the event's `time`**, `duration_seconds <= 0`, or `task_type` was absent or unrecognised. Values other than PICK, PACK and SLAM are stored as unclassified `""`. | `GET /standards/{taskType}`. Compare `effectiveFrom` with the event time. | Expected behaviour ([ADR 0004](../adr/0004-standard-frozen-at-completion-time-not-recomputed.md)). Define the standard **before** the work happens. Past rows are never re-scored. |
| `skipping idle gap: out-of-order Kafka delivery` warnings | A completion arrived whose claim instant is at or before the associate's previous completion. | Log fields `associate_id` and `task_id`. | Expected occasionally. That task has no idle period (`idle_seconds_before` is `null`). |
| Same `TaskCompleted` delivered twice, counted once | Idempotency on the CloudEvents `id` (`processed_events`). | — | Working as designed. Duplicates with a **new** id are counted again. |

## Publishing, outbox and analytics

| Symptom | Cause | Check | Fix |
| --- | --- | --- | --- |
| Projector, reports and `workforce-management` see no events at all | `EVENT_PUBLISHER=log`, which is the chart default (`config.eventPublisher`). | Log line `event publisher configured publisher=log`. | Set `config.eventPublisher=kafka`. |
| `labor_performance.outbox.lag_seconds` keeps climbing, with `outbox relay pass failed` errors | The broker is unreachable, or one row keeps failing. The relay stops at the failed row to keep per-key order. | `SELECT id, topic, attempts, last_error FROM outbox_events WHERE published_at IS NULL ORDER BY id LIMIT 5;` | Fix the broker. Rows publish by themselves on the next pass. |
| `POST /standards` returns 500 with `kafka: publish …` while running without a database | `EVENT_PUBLISHER=kafka` with no `DATABASE_URL` publishes directly, and a broker error fails the request. | Logs. | Start the broker, or use `EVENT_PUBLISHER=log` for local in-memory runs. |
| `GET /reports/performance/freshness` → `lagSeconds` keeps growing | The projector is down or stuck retrying a transient DB error (`analytics message handling failed; retrying`). It retries forever and never skips. | Projector logs, and group `labor-performance-analytics` lag. | Fix the analytical DB. If the read model is corrupt, rebuild it ([Runbook](./runbook.md#rebuild-the-analytical-read-model)). |
| Report shows `meanEfficiencyPct: null` beside `tasksRecorded > 0` | Every task in the bucket was unscorable. | `tasksScored` is 0. | Expected. Null is never shown as 0%. |
| Report with `taskType=pick` returns empty rows | The rollup key is normalised to upper case and the reader compares exactly. | Repeat with `taskType=PICK`. | Send the task type in upper case. |
| Old `outbox_events` or `idempotency_keys` rows never go away after setting the TTL or retention to `0` | `durationEnv` turns `0` back into the default. A `0` never disables the sweep. | — | See [Configuration](./configuration.md#zero-does-not-disable-housekeeping). |

## REST callers

| Symptom | Cause | Fix |
| --- | --- | --- |
| 400 `idempotency-key-required` on `POST /standards` | The `Idempotency-Key` header is missing. It is required whenever `DATABASE_URL` is set ([ADR 0016](../adr/0016-idempotency-key-middleware.md)). | Send a fresh UUID per logical request. |
| 422 `idempotency-key-reused` | The same key was sent with a **different** body. The SHA-256 hash of the body did not match the stored `request_hash`. | Use a new key for a genuinely new request. |
| A retry returns exactly the first response again, even a 4xx | Same key and same body: the stored outcome is replayed. Every non-panic outcome is cached for `IDEMPOTENCY_KEY_TTL`. | Expected. To try again with different values, use a new key. |
| 409 `concurrent-modification` | Two `DefineStandard` calls raced on the same prior standard, and the optimistic `version` check lost ([ADR 0022](../adr/0022-optimistic-concurrency-one-open-standard.md)). | Re-read with `GET /standards/{taskType}` and retry with a new key. |
| 409 `standard-conflict` | The unique "one open standard per task type" index (23505) rejected a second open standard. | Same as above. |
| 422 `non-positive-expected-seconds`, `negative-travel-component-seconds`, `travel-component-exceeds-expected-seconds` | The `LaborStandard` invariants rejected the values. These also count as `outcome=rejected` on the metric. | Fix the payload. |
| 400 `unknown-task-type` | The task type is not PICK, PACK or SLAM (path or body). Matching is case-sensitive. | Use the upper-case enum. |
| 400 `malformed-request-body` | The body is not valid JSON. | Fix the client. |
| 404 `associate-not-found` | The service has never recorded a task for that associate, or the id is empty. | Check that ingestion works (see Kafka above). |
| 404 `standard-not-found` | No standard is currently open for that task type. | `POST /standards`. |
| `?window=90` returns a 1h window | `window` is a Go duration (`90m`, `2h`). A bad value silently falls back to the 1h default instead of returning 400. | Send a unit. |
| Browser call blocked by CORS | The origin is not in `CORS_ALLOWED_ORIGINS`. The OLTP API allows only `GET`/`POST`, and the reports API only `GET`. | Add the origin (comma-separated, no spaces). |

There is no 412 in this service: it uses no `If-Match` or ETag. There
is no circuit breaker either: labor-performance makes no outbound HTTP or
MCP calls, so nothing can trip one.

## MCP

| Symptom | Cause | Fix |
| --- | --- | --- |
| Clients get "session not found" or are re-initialised intermittently | `mcp.replicaCount > 1`. The go-sdk Streamable HTTP sessions live in memory and the Service has no session affinity. | Keep one replica. |
| A tool returns an error such as `associateId is required` or `unknown task type…` | Input validation in `internal/adapters/inbound/mcp/tools.go`. The error is recorded on the `mcp.tool <name>` span. | Fix the arguments. See [MCP tools](../mcp/tools.md). |
| Every tool returns not-found or empty data | `DATABASE_URL` is unset on the MCP Deployment, so it uses its own empty in-memory repositories. | Point it at the same OLTP Secret as the api. |
| `LOG_LEVEL=DEBUG` has no effect on `cmd/mcp` | Its level parser is case-sensitive. | Use `debug`. |

## Problem types

| Slug | HTTP | Raised by |
| --- | --- | --- |
| `standard-not-found` | 404 | `GetStandard` |
| `associate-not-found` | 404 | `GetAssociateScorecard` |
| `concurrent-modification` | 409 | `StandardRepo.Save` version check |
| `standard-conflict` | 409 | One-open-standard unique index |
| `unknown-task-type` | 400 | `shared.NewTaskType` |
| `empty-event-id`, `empty-task-id` | 400 | `performance.New`. Mapped for completeness, but only reachable from the Kafka path, which never writes HTTP. |
| `non-positive-expected-seconds`, `negative-travel-component-seconds`, `travel-component-exceeds-expected-seconds` | 422 | `standard.New` |
| `malformed-request-body` | 400 | JSON decode, or body read in the idempotency middleware |
| `idempotency-key-required` | 400 | `RequireIdempotencyKey` |
| `idempotency-key-reused` | 422 | `RequireIdempotencyKey` |
| `invalid-report-query` | 400 | Reports API |
| `report-store-error` | 500 | Reports API |
| `internal-error` | 500 | Any unmapped error |

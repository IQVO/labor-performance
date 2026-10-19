#!/usr/bin/env bash
# Contract-test the REST API with Schemathesis (property-based testing
# against apis/openapi.yaml): builds the service, boots it against the
# Postgres named by DATABASE_URL and the Kafka broker named by KAFKA_BROKERS
# on a loopback port, waits for /readyz, generates valid AND invalid requests
# for every operation, and asserts the responses conform to the spec (status
# codes, content types, response schemas; positive data accepted, negative
# data rejected).
#
# Mirrors the `contract` job in .github/workflows/ci.yml — same pinned
# Schemathesis version, same flags — so a local pass means a CI pass.
#
# This service has no in-memory mode (ADR 0034): cmd/labor requires
# DATABASE_URL (it migrates and serves from Postgres), KAFKA_BROKERS (the
# outbox relay, the consumer and /readyz) and KAFKA_CONSUMER_GROUP. CI
# provides a postgres:16 and a Kafka service container; for a local run,
# start disposable ones first (the broker must advertise the address you pass
# in KAFKA_BROKERS):
#
#   docker run -d --rm --name lp-contract-pg -p 18099:5432 \
#     -e POSTGRES_USER=labor -e POSTGRES_PASSWORD=labor \
#     -e POSTGRES_DB=labor postgres:16
#   docker run -d --rm --name lp-contract-kafka -p 19099:9092 \
#     -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:29092,PLAINTEXT_HOST://localhost:19099 \
#     confluentinc/confluent-local:7.6.1
#   export DATABASE_URL='postgres://labor:labor@localhost:18099/labor?sslmode=disable'
#   export KAFKA_BROKERS='localhost:19099'
#
# (credentials/URL shape mirror docker-compose.yml and the CI `contract`
# job's postgres service).
#
# Requires `st` on PATH:
#   python3 -m pip install --user 'schemathesis==4.28.0'
set -euo pipefail

SCHEMATHESIS_VERSION="4.28.0"
PORT="${CONTRACT_PORT:-18083}"
BASE_URL="http://127.0.0.1:${PORT}"
MAX_EXAMPLES="${CONTRACT_MAX_EXAMPLES:-100}"

if ! command -v st >/dev/null 2>&1; then
  echo "schemathesis (st) is not installed (or not on PATH)."
  echo "Install the exact version CI pins:"
  echo "  python3 -m pip install --user 'schemathesis==${SCHEMATHESIS_VERSION}'"
  exit 1
fi

if [ -z "${DATABASE_URL:-}" ]; then
  echo "DATABASE_URL is not set — this service has no in-memory mode"
  echo "(cmd/labor requires Postgres and applies migrations on boot)."
  echo "Start a disposable one and export DATABASE_URL, e.g.:"
  echo "  docker run -d --rm --name lp-contract-pg -p 18099:5432 \\"
  echo "    -e POSTGRES_USER=labor -e POSTGRES_PASSWORD=labor \\"
  echo "    -e POSTGRES_DB=labor postgres:16"
  echo "  export DATABASE_URL='postgres://labor:labor@localhost:18099/labor?sslmode=disable'"
  exit 1
fi

if [ -z "${KAFKA_BROKERS:-}" ]; then
  echo "KAFKA_BROKERS is not set — this service has no log sink and no optional relay"
  echo "(cmd/labor requires a real Kafka broker; /readyz depends on it)."
  echo "Start a disposable one and export KAFKA_BROKERS, e.g.:"
  echo "  docker run -d --rm --name lp-contract-kafka -p 19099:9092 \\"
  echo "    -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:29092,PLAINTEXT_HOST://localhost:19099 \\"
  echo "    confluentinc/confluent-local:7.6.1"
  echo "  export KAFKA_BROKERS='localhost:19099'"
  exit 1
fi

cd "$(dirname "$0")/.."
BIN="$(mktemp -d)/labor"
go build -o "$BIN" ./cmd/labor

# The old in-memory boot started EMPTY (no seed data), and so does this one:
# the migrations create the schema and insert no rows, so Schemathesis still
# starts from an empty store and creates what it needs itself. The consumer
# group is mandatory (ADR 0034); the fulfillment topic is auto-created by
# the broker, so the consumer simply idles.
KAFKA_CONSUMER_GROUP="${KAFKA_CONSUMER_GROUP:-labor-performance-contract}" \
  LOG_LEVEL="${LOG_LEVEL:-warn}" HTTP_ADDR="127.0.0.1:${PORT}" "$BIN" &
SERVER_PID=$!
trap 'kill "$SERVER_PID" 2>/dev/null || true' EXIT

# Wait for /readyz: it is 200 only while Postgres answers a ping AND the
# broker answers a metadata request (up to ~60s: migrations + broker).
for _ in $(seq 1 300); do
  if curl -sf "${BASE_URL}/readyz" >/dev/null 2>&1; then
    break
  fi
  sleep 0.2
done
curl -sf "${BASE_URL}/readyz" >/dev/null # fail loudly if it never became ready

# defineStandard is excluded: travelComponentSeconds must not exceed
# expectedSeconds when present — a cross-field conditional OpenAPI 3.0.3
# cannot express in a schema. The conditional IS tested — by the HTTP
# layer (travel_component_seconds_test.go's
# TestPostStandards_TravelComponentSecondsExceedsExpectedSeconds_Returns422),
# the use case (TestDefineStandard_RejectsInvalidTravelComponentSeconds)
# and the domain aggregate — this exclusion only stops Schemathesis
# generating the unexpressible-but-invalid combinations.
#
# getTaskTypeUtilization and getAssociateUtilization are excluded:
# Schemathesis merges an operation's declared query parameters into a
# closed container (additionalProperties: false) and sends unknown query
# parameters expecting a 4xx — a constraint OpenAPI 3.0.3 cannot express
# for query containers. This service deliberately ignores unknown query
# parameters (pinned by the "unknown query parameters are ignored"
# subtests of TestGetTaskTypeUtilization/TestGetAssociateUtilization in
# server_test.go), so this exclusion only stops Schemathesis asserting
# the unexpressible closed-container assumption.
#
# Idempotency-Key (ADR 0016, enforced in every deployment now): the hooks
# file gives every request that carries the header a fresh UUID, like a real
# client sending one per logical submit (see scripts/contract_hooks.py).
#
# --generation-allow-x00 false: the schemas cannot say "no U+0000", and
# PostgreSQL text cannot hold one. The service answers a NUL in a body with
# 400 and in a path with 404 (internal/adapters/inbound/http/nul.go, pinned by
# TestRejectNUL and TestNUL_IsAClientErrorAndDoesNotConsumeTheKey), which
# Schemathesis would report as "API rejected schema-compliant request".
export SCHEMATHESIS_HOOKS="scripts/contract_hooks.py"
st run apis/openapi.yaml \
  --url "${BASE_URL}" \
  --max-examples "${MAX_EXAMPLES}" \
  --workers 4 \
  --generation-allow-x00 false \
  --exclude-operation-id defineStandard \
  --exclude-operation-id getTaskTypeUtilization \
  --exclude-operation-id getAssociateUtilization

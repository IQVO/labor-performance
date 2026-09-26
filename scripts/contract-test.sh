#!/usr/bin/env bash
# Contract-test the REST API with Schemathesis (property-based testing
# against apis/openapi.yaml): builds the service, boots it with its
# in-memory adapters on a loopback port, waits for /healthz, generates
# valid AND invalid requests for every operation, and asserts the
# responses conform to the spec (status codes, content types, response
# schemas; positive data accepted, negative data rejected).
#
# Mirrors the `contract` job in .github/workflows/ci.yml — same pinned
# Schemathesis version, same flags — so a local pass means a CI pass.
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

cd "$(dirname "$0")/.."
BIN="$(mktemp -d)/labor"
go build -o "$BIN" ./cmd/labor

# DATABASE_URL unset => in-memory adapters; the Kafka consumer retries its
# (absent) broker silently in the background and never blocks HTTP.
HTTP_ADDR="127.0.0.1:${PORT}" "$BIN" &
SERVER_PID=$!
trap 'kill "$SERVER_PID" 2>/dev/null || true' EXIT

# Wait for the server to report healthy (up to ~10s).
for _ in $(seq 1 50); do
  if curl -sf "${BASE_URL}/healthz" >/dev/null 2>&1; then
    break
  fi
  sleep 0.2
done
curl -sf "${BASE_URL}/healthz" >/dev/null # fail loudly if it never came up

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
st run apis/openapi.yaml \
  --url "${BASE_URL}" \
  --max-examples "${MAX_EXAMPLES}" \
  --workers 4 \
  --exclude-operation-id defineStandard \
  --exclude-operation-id getTaskTypeUtilization \
  --exclude-operation-id getAssociateUtilization

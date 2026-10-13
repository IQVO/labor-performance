#!/usr/bin/env bash
# Chart render assertions for labor-performance (run by the `chart-tests`
# CI job and `make chart-test`). Needs only helm.
#
#   bash charts/labor-performance/tests/render_test.sh
set -euo pipefail

chart_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
release=labor-performance

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

api_deployment="$(helm template "$release" "$chart_dir" --show-only templates/deployment.yaml)"
configmap="$(helm template "$release" "$chart_dir" --show-only templates/configmap.yaml)"
configmap_name="$(awk '/^  name:/ {print $2; exit}' <<<"$configmap")"

# D14 (2026-10-12): the api Deployment (cmd/labor, the only binary that
# reads every key of the chart ConfigMap) mounts it with envFrom, or a
# config.* value that only reaches the ConfigMap silently does nothing.
grep -A3 'envFrom:' <<<"$api_deployment" | grep -q 'configMapRef:' \
  || fail "api Deployment does not mount the ConfigMap via envFrom"
grep -A3 'envFrom:' <<<"$api_deployment" | grep -q "name: $configmap_name" \
  || fail "api Deployment envFrom does not reference ConfigMap $configmap_name"

# A tuned config value reaches the ConfigMap.
tuned="$(helm template "$release" "$chart_dir" --set config.corsAllowedOrigins=https://ops.example --show-only templates/configmap.yaml)"
grep -q 'CORS_ALLOWED_ORIGINS: "https://ops.example"' <<<"$tuned" \
  || fail "config.corsAllowedOrigins is not rendered into the ConfigMap"

# D12 / ADR 0032: the outbox relay attempt cap defaults to 20 and is
# tunable (0 = unlimited) through config.outboxMaxAttempts.
grep -A1 'name: OUTBOX_MAX_ATTEMPTS' <<<"$api_deployment" | grep -q 'value: "20"' \
  || fail "api Deployment does not set OUTBOX_MAX_ATTEMPTS=20 by default"
unlimited="$(helm template "$release" "$chart_dir" --set config.outboxMaxAttempts=0 --show-only templates/deployment.yaml)"
grep -A1 'name: OUTBOX_MAX_ATTEMPTS' <<<"$unlimited" | grep -q 'value: "0"' \
  || fail "config.outboxMaxAttempts=0 is not rendered"

# D15: a zero housekeeping interval reaches the api pod as is (0 disables
# the sweeper, ADR 0023).
no_sweep="$(helm template "$release" "$chart_dir" --set config.housekeepingInterval=0s --show-only templates/deployment.yaml)"
grep -A1 'name: HOUSEKEEPING_INTERVAL' <<<"$no_sweep" | grep -q 'value: "0s"' \
  || fail "config.housekeepingInterval=0s is not rendered"

analytics_args=(--set analytics.enabled=true --set mcp.enabled=true --set analytics.database.projectorUrl=postgres://projector)

# D24: cmd/mcp serves GET /healthz, so all three MCP probes are httpGet
# /healthz on the http port (they used to be tcpSocket).
mcp="$(helm template "$release" "$chart_dir" "${analytics_args[@]}" --show-only templates/mcp-deployment.yaml)"
for probe in startupProbe livenessProbe readinessProbe; do
  grep -A3 "$probe:" <<<"$mcp" | grep -q 'httpGet:' || fail "mcp $probe is not httpGet"
  grep -A3 "$probe:" <<<"$mcp" | grep -q 'path: /healthz' || fail "mcp $probe does not probe /healthz"
done
if grep -q 'tcpSocket:' <<<"$mcp"; then
  fail "mcp Deployment still has a tcpSocket probe"
fi

# The other workloads read their own explicit env and must NOT inherit the
# api's HTTP_ADDR / OTEL_SERVICE_NAME from the ConfigMap.
for template in reports-deployment.yaml projector-deployment.yaml mcp-deployment.yaml; do
  if helm template "$release" "$chart_dir" "${analytics_args[@]}" --show-only "templates/$template" | grep -q 'configMapRef:'; then
    fail "$template must not mount the api ConfigMap"
  fi
done

echo "labor-performance chart render assertions passed"

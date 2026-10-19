#!/usr/bin/env bash
# Chart render assertions for labor-performance (run by the `chart-tests`
# CI job and `make chart-test`). Needs only helm.
#
# The chart deploys the REAL runtime of this context (ADR 0034: Postgres and
# Kafka are the only runtime, no in-memory mode, no log sink), so the mandatory
# configuration has no silent default, no permissive/off mode is ever rendered
# and the env the binaries read is the env the chart renders.
#
#   bash charts/labor-performance/tests/render_test.sh
set -euo pipefail

chart_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
repo_dir="$(cd "$chart_dir/../.." && pwd)"
release=labor-performance
# Dummy values, no password: the chart refuses to render without a database
# source, brokers and a consumer group; none of them is ever dialled.
required=(--set "database.url=postgres://u@example.invalid:5432/db" --set "kafka.brokers=kafka.example.invalid:9092" --set "config.kafkaConsumerGroup=labor-performance")

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

render() {
  helm template "$release" "$chart_dir" "${required[@]}" "$@"
}

env_value() { # env_value <rendered> <NAME>
  grep -A1 "name: $2\$" <<<"$1" | grep -m1 'value:' | sed -E 's/.*value: "?//; s/"$//'
}

api_deployment="$(render --show-only templates/deployment.yaml)"
configmap="$(render --show-only templates/configmap.yaml)"
configmap_name="$(awk '/^  name:/ {print $2; exit}' <<<"$configmap")"

# D14 (2026-10-12): the api Deployment (cmd/labor, the only binary that
# reads every key of the chart ConfigMap) mounts it with envFrom, or a
# config.* value that only reaches the ConfigMap silently does nothing.
grep -A3 'envFrom:' <<<"$api_deployment" | grep -q 'configMapRef:' \
  || fail "api Deployment does not mount the ConfigMap via envFrom"
grep -A3 'envFrom:' <<<"$api_deployment" | grep -q "name: $configmap_name" \
  || fail "api Deployment envFrom does not reference ConfigMap $configmap_name"

# A tuned config value reaches the ConfigMap.
tuned="$(render --set config.corsAllowedOrigins=https://ops.example --show-only templates/configmap.yaml)"
grep -q 'CORS_ALLOWED_ORIGINS: "https://ops.example"' <<<"$tuned" \
  || fail "config.corsAllowedOrigins is not rendered into the ConfigMap"

# D12 / ADR 0032: the outbox relay attempt cap defaults to 20 and is
# tunable (0 = unlimited) through config.outboxMaxAttempts.
grep -A1 'name: OUTBOX_MAX_ATTEMPTS' <<<"$api_deployment" | grep -q 'value: "20"' \
  || fail "api Deployment does not set OUTBOX_MAX_ATTEMPTS=20 by default"
unlimited="$(render --set config.outboxMaxAttempts=0 --show-only templates/deployment.yaml)"
grep -A1 'name: OUTBOX_MAX_ATTEMPTS' <<<"$unlimited" | grep -q 'value: "0"' \
  || fail "config.outboxMaxAttempts=0 is not rendered"

# D15: a zero housekeeping interval reaches the api pod as is (0 disables
# the sweeper, ADR 0023).
no_sweep="$(render --set config.housekeepingInterval=0s --show-only templates/deployment.yaml)"
grep -A1 'name: HOUSEKEEPING_INTERVAL' <<<"$no_sweep" | grep -q 'value: "0s"' \
  || fail "config.housekeepingInterval=0s is not rendered"

analytics_args=(--set analytics.enabled=true --set mcp.enabled=true --set analytics.database.projectorUrl=postgres://projector@example.invalid:5432/a)

# D24: cmd/mcp serves GET /healthz, so all three MCP probes are httpGet
# /healthz on the http port (they used to be tcpSocket).
mcp="$(render "${analytics_args[@]}" --show-only templates/mcp-deployment.yaml)"
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
  if render "${analytics_args[@]}" --show-only "templates/$template" | grep -q 'configMapRef:'; then
    fail "$template must not mount the api ConfigMap"
  fi
done

# --- ADR 0034: the mandatory configuration is rendered from values ---------------------
[[ "$(env_value "$api_deployment" KAFKA_BROKERS)" == "kafka.example.invalid:9092" ]] || fail "KAFKA_BROKERS is not rendered from kafka.brokers"
[[ "$(env_value "$api_deployment" KAFKA_CONSUMER_GROUP)" == "labor-performance" ]] || fail "KAFKA_CONSUMER_GROUP is not rendered from config.kafkaConsumerGroup"
[[ "$(env_value "$api_deployment" EVENT_PUBLISHER)" == "kafka" ]] || fail "EVENT_PUBLISHER must default to kafka"
grep -q '^  EVENT_PUBLISHER: "kafka"' <<<"$configmap" || fail "the ConfigMap must carry EVENT_PUBLISHER=kafka"
grep -q '^  KAFKA_BROKERS: "kafka.example.invalid:9092"' <<<"$configmap" || fail "the ConfigMap must carry KAFKA_BROKERS"
grep -A4 'name: DATABASE_URL$' <<<"$api_deployment" | grep -q 'secretKeyRef:' || fail "DATABASE_URL must come from a Secret (secretKeyRef)"
if grep -qE '^  (MIGRATIONS_)?DATABASE_URL:' <<<"$configmap"; then fail "a DSN must never land in the ConfigMap"; fi
# warehouse-infra still passes the pre-ADR-0034 switch kafka.enabled: it must keep rendering.
render --set kafka.enabled=true --show-only templates/deployment.yaml >/dev/null || fail "kafka.enabled=true (set by warehouse-infra) must keep rendering"

# --- no in-memory / permissive / log mode, ever --------------------------------------
all_docs="$(render "${analytics_args[@]}" --set frontend.enabled=true)"
notes="$(render --show-only templates/NOTES.txt 2>/dev/null || true)"
if grep -qiE 'value: "?(permissive|log)"?$' <<<"$all_docs"; then fail "the render carries permissive/log, which the binaries reject at boot"; fi
if grep -q 'DATABASE_URL: ""' <<<"$all_docs"; then fail "an empty DATABASE_URL must never be rendered"; fi
if grep -qi 'in-memory' <<<"$all_docs"$'\n'"$notes"; then fail "the render mentions an in-memory mode"; fi
if grep -E '(name: |^  )[A-Z_]+_MODE' <<<"$all_docs" | grep -q .; then
  fail "the render carries a *_MODE variable: labor-performance has none"
fi

# --- probes: /healthz liveness (and startup), /readyz readiness ----------------------
probe_block() { # probe_block <probeName> <rendered deployment>
  awk -v p="$1:" '$1 == p {f=1; next} f && /^          [a-zA-Z]/ {exit} f {print}' <<<"$2"
}
grep -q 'path: /healthz' <<<"$(probe_block livenessProbe "$api_deployment")" || fail "livenessProbe must hit /healthz"
grep -q 'path: /healthz' <<<"$(probe_block startupProbe "$api_deployment")" || fail "startupProbe must hit /healthz"
grep -q 'path: /readyz' <<<"$(probe_block readinessProbe "$api_deployment")" || fail "readinessProbe must hit /readyz"

# --- graceful shutdown room ----------------------------------------------------------
grace="$(awk '/terminationGracePeriodSeconds:/ {print $2; exit}' <<<"$api_deployment")"
(( grace >= 20 )) || fail "terminationGracePeriodSeconds is $grace; want at least 20 (10 s shared drain budget + 5 s telemetry flush)"

# --- the mandatory env of every binary is rendered -----------------------------------
mcp_dep="$(render --set mcp.enabled=true --show-only templates/mcp-deployment.yaml)"
grep -A4 'name: DATABASE_URL$' <<<"$mcp_dep" | grep -q 'secretKeyRef:' || fail "the mcp DATABASE_URL must come from a Secret"
proj="$(render "${analytics_args[@]}" --show-only templates/projector-deployment.yaml)"
reports="$(render "${analytics_args[@]}" --show-only templates/reports-deployment.yaml)"
[[ "$(env_value "$proj" KAFKA_BROKERS)" == "kafka.example.invalid:9092" ]] || fail "the projector KAFKA_BROKERS is not rendered from kafka.brokers"

# --- chart vs binary ---------------------------------------------------------------------
# Every mandatory variable a binary's boot config demands (ADR 0034) is
# rendered by its Deployment: a new mandatory variable fails until the chart
# carries it.
for var in DATABASE_URL KAFKA_BROKERS KAFKA_CONSUMER_GROUP EVENT_PUBLISHER; do
  grep -q "name: $var\$" <<<"$api_deployment" || fail "the api Deployment does not render $var"
done
grep -q 'name: DATABASE_URL$' <<<"$mcp_dep" || fail "the mcp Deployment does not render DATABASE_URL"
grep -q 'name: ANALYTICS_DATABASE_URL$' <<<"$proj" || fail "the projector Deployment does not render ANALYTICS_DATABASE_URL"
grep -q 'name: ANALYTICS_DATABASE_URL$' <<<"$reports" || fail "the reports Deployment does not render ANALYTICS_DATABASE_URL"
for bin in labor-projector labor-reports; do
  case "$bin" in labor-projector) rendered="$proj" ;; *) rendered="$reports" ;; esac
  vars="$(grep -ho 'errors.New("[A-Z_]* is required' "$repo_dir"/cmd/"$bin"/*.go | sed -E 's/errors.New\("//; s/ is required//' | sort -u)"
  [[ -n "$vars" ]] || fail "found no mandatory variables in cmd/$bin; the scan is looking in the wrong place"
  for var in $vars; do
    grep -q "name: $var\$" <<<"$rendered" || fail "the $bin Deployment does not render $var, which cmd/$bin requires"
  done
done

# --- refusals: the mandatory values name themselves ----------------------------------
expect_refusal() { # expect_refusal <why> <needle> <helm args...>
  local why="$1" needle="$2" err; shift 2
  if err="$(helm template "$release" "$chart_dir" "$@" 2>&1)"; then
    fail "the chart must refuse to render: $why"
  fi
  grep -qF -- "$needle" <<<"$err" || fail "the refusal for '$why' does not name '$needle':
$err"
}
base=("${required[@]}")
expect_refusal "nothing set" "database.url or database.existingSecret"
expect_refusal "no database source" "database.url or database.existingSecret" --set kafka.brokers=k:9092 --set config.kafkaConsumerGroup=g
expect_refusal "no brokers" "kafka.brokers" --set database.url=postgres://u@h/db --set config.kafkaConsumerGroup=g
expect_refusal "no consumer group" "config.kafkaConsumerGroup" --set database.url=postgres://u@h/db --set kafka.brokers=k:9092
expect_refusal "the log publisher" "config.eventPublisher" "${base[@]}" --set config.eventPublisher=log
expect_refusal "an unknown publisher" "the only publisher" "${base[@]}" --set config.eventPublisher=rabbit
expect_refusal "kafka switched off" "Kafka cannot be switched off" "${base[@]}" --set kafka.enabled=false
expect_refusal "a short grace period" "terminationGracePeriodSeconds" "${base[@]}" --set terminationGracePeriodSeconds=10
expect_refusal "the log publisher through extraEnv" "EVENT_PUBLISHER" "${base[@]}" --set 'extraEnv[0].name=EVENT_PUBLISHER' --set 'extraEnv[0].value=log'
expect_refusal "a *_MODE through extraEnv" "*_MODE" "${base[@]}" --set 'extraEnv[0].name=FULFILLMENT_MODE' --set 'extraEnv[0].value=permissive'
# The MCP server needs the database on its own too.
expect_refusal "mcp without a database" "database.url or database.existingSecret" --set mcp.enabled=true --set kafka.brokers=k:9092 --set config.kafkaConsumerGroup=g

# --- what warehouse-infra sets keeps working -----------------------------------------
# tests/warehouse-infra-values.yaml mirrors helm-values/labor-performance.yaml +
# _platform.yaml + kind-resources/labor-performance.yaml of warehouse-infra:
# kafka.enabled=true, eventPublisher=kafka, the existing database secret, the
# analytics secret, mcp and frontend on, empty extraEnv, probes that point at
# /healthz and the platform brokers. Infra needs NO values change.
infra=(-f "$chart_dir/tests/warehouse-infra-values.yaml")
infra_all="$(helm template "$release" "$chart_dir" "${infra[@]}")" || fail "the warehouse-infra values no longer render"
infra_api="$(helm template "$release" "$chart_dir" "${infra[@]}" --show-only templates/deployment.yaml)"
[[ "$(env_value "$infra_api" KAFKA_BROKERS)" == "kafka.warehouse-systems.svc.cluster.local:9092" ]] || fail "infra values: KAFKA_BROKERS"
[[ "$(env_value "$infra_api" KAFKA_CONSUMER_GROUP)" == "labor-performance" ]] || fail "infra values: KAFKA_CONSUMER_GROUP"
[[ "$(env_value "$infra_api" EVENT_PUBLISHER)" == "kafka" ]] || fail "infra values: EVENT_PUBLISHER"
grep -q 'labor-performance-db' <<<"$infra_api" || fail "infra values: DATABASE_URL must come from the labor-performance-db secret"
infra_proj="$(helm template "$release" "$chart_dir" "${infra[@]}" --show-only templates/projector-deployment.yaml)"
[[ "$(env_value "$infra_proj" KAFKA_BROKERS)" == "kafka.warehouse-systems.svc.cluster.local:9092" ]] || fail "infra values: projector KAFKA_BROKERS"
for component in api analytics-projector analytics-reports mcp frontend; do
  grep -q "app.kubernetes.io/component: $component" <<<"$infra_all" || fail "infra values: component $component not rendered"
done

echo "labor-performance chart render assertions passed"

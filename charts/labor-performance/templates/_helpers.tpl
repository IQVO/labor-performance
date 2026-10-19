{{/*
Expand the name of the chart.
*/}}
{{- define "labor-performance.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "labor-performance.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Chart name and version as used by the chart label.
*/}}
{{- define "labor-performance.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "labor-performance.labels" -}}
helm.sh/chart: {{ include "labor-performance.chart" . }}
{{ include "labor-performance.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "labor-performance.selectorLabels" -}}
app.kubernetes.io/name: {{ include "labor-performance.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "labor-performance.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "labor-performance.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Name of the Secret holding DATABASE_URL, when the chart creates its own.
*/}}
{{- define "labor-performance.databaseSecretName" -}}
{{- if .Values.database.existingSecret }}
{{- .Values.database.existingSecret }}
{{- else }}
{{- include "labor-performance.fullname" . }}-database
{{- end }}
{{- end }}

{{/*
MANDATORY (ADR 0034: Postgres is the only runtime). cmd/labor and cmd/mcp exit
at boot without DATABASE_URL; there is no in-memory mode.
*/}}
{{- define "labor-performance.requireDatabase" -}}
{{- if not (or .Values.database.url .Values.database.existingSecret) -}}
{{- fail "labor-performance requires database.url or database.existingSecret to be set (DATABASE_URL): the service runs on real Postgres only and exits at boot without it (ADR 0034)." -}}
{{- end -}}
{{- end -}}

{{/*
MANDATORY (ADR 0034: Kafka is the only runtime). cmd/labor and cmd/labor-projector
exit at boot without KAFKA_BROKERS, cmd/labor also without KAFKA_CONSUMER_GROUP,
and cmd/labor rejects EVENT_PUBLISHER=log; there is no default broker, no log
sink and no way to switch Kafka off. kafka.enabled is kept only so values
written before ADR 0034 still render (warehouse-infra sets it to true): it may
be true, never false.
*/}}
{{- define "labor-performance.requireKafka" -}}
{{- $_ := required "labor-performance requires kafka.brokers to be set (KAFKA_BROKERS, comma-separated): the service runs on real Kafka only and exits at boot without it (ADR 0034)." .Values.kafka.brokers -}}
{{- $_ := required "labor-performance requires config.kafkaConsumerGroup to be set (KAFKA_CONSUMER_GROUP): the fulfillment-events consumer is always on and exits at boot without a stable group id (ADR 0034)." .Values.config.kafkaConsumerGroup -}}
{{- if and (hasKey .Values.kafka "enabled") (not .Values.kafka.enabled) -}}
{{- fail "kafka.enabled is false — Kafka cannot be switched off: the service runs on real Kafka only (ADR 0034). Drop the key or set it to true and set kafka.brokers." -}}
{{- end -}}
{{- if ne (toString .Values.config.eventPublisher) "kafka" -}}
{{- fail (printf "config.eventPublisher is %q — the only publisher is \"kafka\": the log sink was removed (ADR 0034); the binary rejects EVENT_PUBLISHER=log at boot." (toString .Values.config.eventPublisher)) -}}
{{- end -}}
{{- end -}}

{{/*
Shutdown is sequential: readiness flips, then ONE 10s budget drains HTTP and
lets the outbox relay and the consumer finish, then a 5s telemetry flush. A
grace period under 20s gets the pod SIGKILLed mid-drain (the default is 30s).
*/}}
{{- define "labor-performance.requireGracePeriod" -}}
{{- if lt (int .Values.terminationGracePeriodSeconds) 20 -}}
{{- fail (printf "terminationGracePeriodSeconds is %v — want at least 20: 10s HTTP/relay/consumer drain (one shared budget) + 5s telemetry flush." .Values.terminationGracePeriodSeconds) -}}
{{- end -}}
{{- end -}}

{{/*
extraEnv may not bring back an in-memory or log mode (ADR 0034): no
EVENT_PUBLISHER other than kafka and no *_MODE variable at all.
labor-performance reads no *_MODE switch (it consumes no sibling context over
REST and its one Kafka consumer cannot be switched off), so any one in extraEnv
is a leftover of a permissive/off mode that no longer exists.
*/}}
{{- define "labor-performance.requireNoInMemoryEnv" -}}
{{- range .Values.extraEnv -}}
{{- $name := toString .name -}}
{{- $value := toString (default "" .value) -}}
{{- if and (eq $name "EVENT_PUBLISHER") (ne $value "kafka") -}}
{{- fail (printf "extraEnv sets EVENT_PUBLISHER=%s — the only publisher is \"kafka\" (ADR 0034)." $value) -}}
{{- end -}}
{{- if hasSuffix "_MODE" $name -}}
{{- fail (printf "extraEnv sets %s — *_MODE variables are not configurable: labor-performance runs on real Postgres and real Kafka only, with no permissive or off mode (ADR 0034)." $name) -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Every render-time requirement of the real runtime, in one include. The api
Deployment (always rendered) includes it, so a mis-set release fails
`helm template` / `helm install` whichever component is enabled.
*/}}
{{- define "labor-performance.requireRealRuntime" -}}
{{- include "labor-performance.requireDatabase" . -}}
{{- include "labor-performance.requireKafka" . -}}
{{- include "labor-performance.requireGracePeriod" . -}}
{{- include "labor-performance.requireNoInMemoryEnv" . -}}
{{- end -}}

{{/*
Fully qualified name of the analytics projector deployment (ADR-0007).
*/}}
{{- define "labor-performance.projectorFullname" -}}
{{- include "labor-performance.fullname" . }}-projector
{{- end }}

{{/*
Fully qualified name of the analytics reports deployment/service (ADR-0007).
*/}}
{{- define "labor-performance.reportsFullname" -}}
{{- include "labor-performance.fullname" . }}-reports
{{- end }}

{{/*
Name of the Secret holding the analytics DSNs, when the chart creates its own.
*/}}
{{- define "labor-performance.analyticsSecretName" -}}
{{- if .Values.analytics.database.existingSecret }}
{{- .Values.analytics.database.existingSecret }}
{{- else }}
{{- include "labor-performance.fullname" . }}-analytics
{{- end }}
{{- end }}

{{/*
Fully qualified name of the MCP server deployment/service (ADR-0009).
*/}}
{{- define "labor-performance.mcpFullname" -}}
{{- include "labor-performance.fullname" . }}-mcp
{{- end }}

{{/*
Fully qualified name of the frontend Module Federation remote deployment/service.

The remote is served by its own nginx pod and reached through warehouse-infra's
Nginx web gateway at /mfes/labor-performance/. It is deliberately a separate
workload from the API: Kong never routes to it, and the OLTP Service must never
select it.
*/}}
{{- define "labor-performance.frontendFullname" -}}
{{- include "labor-performance.fullname" . }}-frontend
{{- end }}


{{/*
Comma-joined names of this chart's enabled KongPlugins, for the
konghq.com/plugins annotation on every HTTPRoute this chart renders.
Empty string (annotation omitted) when none are enabled — standalone
installs without Kong are unaffected.
*/}}
{{- define "labor-performance.kongPluginNames" -}}
{{- $names := list -}}
{{- range $name, $cfg := .Values.kongPlugins -}}
{{- if $cfg.enabled -}}
{{- $names = append $names (printf "%s-%s" (include "labor-performance.fullname" $) $name) -}}
{{- end -}}
{{- end -}}
{{- join "," $names -}}
{{- end -}}

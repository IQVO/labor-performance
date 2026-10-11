# syntax=docker/dockerfile:1.7

# --- build stage ---
FROM golang:1.26-alpine@sha256:c95332c2af86b6d89b91bd0500f4b9529ccbd090a0d1855c6d1ceaa142ae8615 AS build
WORKDIR /src

# Cache go.mod/go.sum download separately from source so editing source
# code doesn't bust the module-download layer.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .

# BuildKit cache mounts for the module and build caches speed up repeat
# builds in CI without baking the cache into the image layers.
# Four binaries ship in one image: the OLTP service, the analytics data
# product's writer and read-only reader (ADR 0007), and the MCP inbound
# adapter (ADR 0009). They are separate PROCESSES, selected at run time via
# the entrypoint/command — the projector is the only writer of the
# analytical database, the reports reader never writes or migrates, and the
# mcp server runs the same read use cases as ./labor over the same OLTP
# database.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/labor ./cmd/labor && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/labor-projector ./cmd/labor-projector && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/labor-reports ./cmd/labor-reports && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/mcp ./cmd/mcp

# --- runtime stage ---
FROM alpine:3.24@sha256:28bd5fe8b56d1bd048e5babf5b10710ebe0bae67db86916198a6eec434943f8b
RUN apk upgrade --no-cache && \
    apk add --no-cache ca-certificates=20260909-r0 && \
    addgroup -g 1000 -S app && adduser -u 1000 -S app -G app
WORKDIR /app
COPY --from=build --chown=app:app /out/labor ./labor
COPY --from=build --chown=app:app /out/labor-projector ./labor-projector
COPY --from=build --chown=app:app /out/labor-reports ./labor-reports
COPY --from=build --chown=app:app /out/mcp ./mcp
# Both migration sets: /migrations is the OLTP schema (run by ./labor and,
# idempotently, by ./mcp), /migrations/analytics is the analytical schema
# (run by ./labor-projector, and by nothing else).
COPY --from=build --chown=app:app /src/migrations ./migrations
USER 1000
EXPOSE 8080
ENTRYPOINT ["./labor"]

---
name: how-to-add-a-rest-endpoint
description: Add or change a REST endpoint in this service in the fleet's hexagonal order (domain invariant, use case, port, HTTP adapter, apis/openapi.yaml, generated docs, godog scenario). Use when touching internal/adapters/inbound/http, apis/openapi.yaml, or exposing a use case over HTTP.
---

# How to add a REST endpoint

Use when asked to add a new REST use case/endpoint to this service. Follow
this order — domain first, adapter last — never the reverse; writing the
HTTP handler before the domain invariant it enforces produces handlers
that validate nothing and use cases that get bypassed.

This walks the exact path `GET /associates/{associateId}/scorecard` took
(`internal/application/usecases/get_associate_scorecard.go` +
`internal/adapters/inbound/http/server.go`'s `handleGetAssociateScorecard`)
as the concrete worked example — read those two files alongside this
guide. A second worked example lives in the same two files:
`GetUtilization`/`handleGetTaskTypeUtilization`/`handleGetAssociateUtilization`,
which is the shape to copy when an endpoint needs a query parameter
(`?window=`) rather than only a path parameter.

## 1. Domain first: does an invariant already exist, or do you need one?

Check `internal/domain/<aggregate>/` for the rule this endpoint enforces.
A REST endpoint should almost never contain business logic itself — it
decodes a request, calls a use case, encodes the result.
`GetAssociateScorecard` is a good example of this discipline even though
it is read-only: the "is this a real associate" 404 rule
(`associateId == ""` is fulfillment-execution's "no checked-in occupant"
marker, never a real identity, and always 404s) lives in the use case,
not the handler — the handler only extracts the path param and calls
`Execute`. If the operation needs a new domain rule (e.g. a new
aggregate invariant), add it to `internal/domain/`, with its own
table-driven unit test, BEFORE touching the application or adapter
layers.

## 2. Application: define the use case

Add a new file in `internal/application/usecases/` (one file per use
case, this repo's convention — not one giant `usecases.go`). Shape,
mirroring `get_associate_scorecard.go`:

```go
package usecases

type <Verb><Noun> struct {
    Performances ports.PerformanceRepo   // driven ports only — never a concrete adapter
    // ... other ports this use case needs (Standards, IdlePeriods, Clock)
}

func (uc *<Verb><Noun>) Execute(ctx context.Context, /* domain-typed args */) (<ResultType>, error) {
    // 1. load aggregate(s)/read-model rows via the port(s)
    // 2. apply any domain rule via the aggregate's own method or a pure
    //    domain function (never inline the invariant here — see
    //    GetAssociateScorecard delegating Trend/CoachingFlag computation
    //    to performance.ClassifyTrend/DetectCoachingFlag)
    // 3. return the result — a read model struct, not a stored aggregate
}
```

Add the port to `internal/application/ports/ports.go` if the query
doesn't exist yet on `PerformanceRepo`/`StandardRepo`/`IdlePeriodRepo` —
ports are interfaces ONLY (arch-go fitness tests in
`internal/architecture/` enforce the hexagonal dependency rule; a struct
or function in a ports file would violate it). Never fabricate a number:
this repo's non-negotiable #3 means any mean/percentage field (see
`ports.Scorecard.MeanEfficiencyPct`, `TaskTypePerformance.MeanActualSeconds`,
`UtilizationResult.UtilizationPct`) is a `*float64`, nil when nothing was
scorable — never `0`.

Write the use case's unit test against the in-memory adapter
(`internal/adapters/outbound/memory/`) — never a real Postgres/Kafka
call in a unit test. Cover the success path AND the "not found"/error
path (`get_associate_scorecard.go`'s own test covers both the empty-id
case and the never-recorded-associate case as two separate assertions,
not one).

## 3. Adapter: wire the HTTP handler

In `internal/adapters/inbound/http/`:

1. `dto.go` — add the response DTO struct (JSON tags). DTOs live ONLY in
   the adapter layer — domain/ports types never carry JSON tags directly
   (see `toScorecardResponse` in `server.go` converting `ports.Scorecard`
   to `scorecardResponse`).
2. `server.go` — add the route (`r.Get("/path/{param}", s.handle<Name>)`
   inside `NewRouter`) and the handler function:
   - decode + validate the request, converting to domain value objects
     immediately (`shared.NewTaskType`, `shared.AssociateId(...)`) — a
     bad `TaskType` fails here as an RFC 7807 validation error, never
     reaches the use case
   - call the use case's `Execute`
   - map use-case errors to HTTP status via `writeError` (check
     `errors.go` for the existing error→status mapping before adding a
     new error type)
   - encode the result back to the response DTO and `writeJSON`
3. Add the new use case field to the `Server` struct and wire it in the
   composition root (`cmd/labor/main.go`'s `server := &inboundhttp.Server{...}`
   block).

**This repo's own non-negotiable #4 is the sharpest version of "domain
first, adapter last" in this fleet**: `RecordTaskPerformance` has NO REST
handler at all, by design — it is exclusively Kafka-consumer-driven (see
the how-to-add-an-integration-event guide). Don't add a
`POST /performances`-style endpoint "for completeness"; it would
contradict AGENTS.md and the architecture this service was built around.

Write at least one httptest per endpoint: one success path, one error
path — see `server_test.go`'s `newTestEnv`/`recordViaBackdoor` pattern,
which seeds `TaskPerformance` rows directly through the use case (bypassing
HTTP, since there is deliberately no REST write path) to set up GET-path
fixtures.

## 4. Contract: update OpenAPI, then regenerate docs

Add the path to `apis/openapi.yaml` (OLTP surface, `cmd/labor`) or
`apis/openapi-reports.yaml` (the read-only `cmd/labor-reports` surface,
ADR 0007) — pick whichever process actually serves the route. Include
the RFC 7807 problem-detail response for each error case; see the
existing `/associates/{associateId}/scorecard` entry for the shape.

Regenerate the Docusaurus REST reference — this repo's `docs-api-drift`
CI job fails the PR if you skip this:

```bash
cd docs
npm run clean-api-docs        # OLTP spec (or clean-api-docs:reports / :all)
npm run gen-api-docs
```

## 5. Behaviour: add a godog scenario

If this endpoint is user-facing behaviour (not purely internal
plumbing), add a `.feature` file under `features/` exercising it
end-to-end against the real HTTP server (Given/When/Then over real
HTTP, not mocked) — this repo's `bdd` CI job (`go test ./... -run
TestFeatures -v`) runs these.

## 6. Verify before opening the PR

```bash
make check       # fmt-check vet build lint test
make check-all   # + coverage (90% gate) + arch-test + bdd
```

`make coverage` gates `./internal/domain/...,./internal/application/...,
./internal/analytics/...` at 90% — a new use case with no test on its
error/not-found path is the most common way to miss this gate.

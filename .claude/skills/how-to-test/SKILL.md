---
name: how-to-test
description: Write or review tests and diagnose a failing coverage, mutation, bdd or integration CI job: the four test layers, the 90% gate, gremlins threshold semantics, the testcontainers rule. Use when adding tests, killing a surviving mutant, or fixing a red check.
---

# How to test

Use when writing or reviewing tests in this repo, or diagnosing a
failing `coverage`/`mutation-fast`/`bdd`/`integration` CI job. This
fleet's quality bar is layered — passing `go test` is necessary but is
the WEAKEST signal of the four; mutation testing exists specifically
because green tests can assert nothing.

## The four layers, in order of what they actually prove

1. **Unit tests** (`make test` / `go test ./... -race`) — prove the code
   runs without panicking and returns SOMETHING. Table-driven, in-memory
   adapters only (`internal/adapters/outbound/memory/`) or a fake Kafka
   reader (see `internal/adapters/inbound/kafka/consumer_test.go`),
   never a real network/DB call.
2. **Coverage** (`make coverage`, 90% gate on
   `./internal/domain/...,./internal/application/...,./internal/analytics/...`
   — note this repo's gate covers `internal/analytics` too, not just
   domain+application, because that package holds the analytical read
   model's own aggregation logic, ADR 0007) — proves lines executed.
   Proves nothing about whether the test asserted the right thing.
3. **Mutation testing** (`make mutation-fast`, gremlins) — proves the
   tests actually ASSERT, not merely execute. A mutant is a deliberately
   broken version of the code (`<` -> `<=`, `+` -> `-`, etc.); if the
   test suite still passes against the mutant, it "survived" — meaning
   no test would catch that exact bug in production.
4. **BDD / behaviour** (`make bdd`, godog) — proves the use case works
   end-to-end through the real HTTP surface, not through a mocked port.

## Mutation testing: this repo's own thresholds and the switch-statement gap they document

`.gremlins.yaml` sets `efficacy: 99` / `mutant-coverage: 99` — gremlins
fails when the MEASURED score is `<=` the threshold, so 99 locks in
today's actual 100% (measured 2026-08-29 on `internal/domain/performance`
and the exhaustive `internal/domain` run: 29 mutants total, all killed,
zero survivors, zero not-covered). The file's own comment documents a
real, already-found gap worth remembering before writing a new
conditional in this codebase: an expression-less `switch { case
...: ... }` form showed 100% normal Go test coverage but gremlins
reported 2 of its 3 branches NOT COVERED — the mutation tool does not
reliably instrument that switch form. It was rewritten as
if/else-if/return (the pattern every other conditional in this domain
package already used), and every mutant on those lines is now KILLED.
Prefer if/else-if/return over an expression-less switch for any new
domain-layer branching logic here.

Also note `.gremlins.yaml` pins `workers: 1` / `timeout-coefficient: 30`
— the default worker count produced spurious TIMED OUT results on this
module (mutants finish in tens of milliseconds; the "timeouts" were
parallel workers contending over the build cache, not slow tests). Don't
"fix" a slow mutation run by cranking workers back up; it will
reintroduce the false timeout.

## Three real pitfalls that have each cost a real CI failure in this fleet

### 1. Zero/origin-value fixtures hide arithmetic mutants

A test built around zero-valued operands makes `a - b` and `a + b`
produce the same result, so a mutant flipping `-` to `+` survives even
though coverage looks complete. `get_utilization.go`'s
`openGapSeconds`/`associateCountFor` arithmetic is exactly this kind of
code — any new value object or read-model computation with real
arithmetic needs fixture values where EVERY operand and every
per-field delta is distinct and non-zero, and the test must assert the
exact expected value, not just "no error".

### 2. Boundary guards need the boundary value itself

A test for a `<= 0`/`< 0` guard that only tries a clearly-invalid and a
clearly-valid value never exercises the boundary itself, so a
`CONDITIONALS_BOUNDARY` mutant survives silently. `standard`'s
`ExpectedSeconds must be > 0` invariant (see ADR references to
`standard.ErrNonPositiveExpectedSeconds`) and `openGapSeconds`'s
`lastActivity.Before(since)` clamp are both boundary guards in this
repo's real domain — any test for one of these needs an explicit
assertion at the exact threshold value, not just comfortably on either
side of it.

### 3. Tie-break / near-equivalent mutants: know when NOT to chase them

A comparison whose mutation only diverges on an exact tie (two equal
timestamps, two equal durations) is sometimes undetectable by ANY test
whose fixture values are all distinct. Do NOT force an artificial tied
fixture just to kill it; that pins an arbitrary, currently-unspecified
tie-break order as if it were a real invariant, which is worse than an
accepted near-equivalent survivor. This repo does not yet have a
`MUTATION.md` triage file — if you hit one of these while adding a new
mutation-tested package, create `MUTATION.md` at the repo root
documenting the specific survivor and why it's accepted, mirroring how
`.gremlins.yaml`'s own header comment already documents the
switch-statement gap above. Don't silently lower the threshold to make
an accepted survivor disappear from the report.

## Diagnosing a `mutation-fast`/`mutation` CI failure: diff against develop, don't chase every LIVED line

```bash
gremlins unleash ./internal/domain/performance          # on your branch (mirrors make mutation-fast)
git stash && git checkout origin/develop -- . && gremlins unleash ./internal/domain/performance   # baseline
```

Only entries NEW on your branch are your regression. `make mutation`
(the slower, exhaustive `./internal/domain` run with `--workers 1
--timeout-coefficient 30`) is what the scheduled CI `mutation` job runs;
`make mutation-fast` (just `./internal/domain/performance`) is the fast
blocking subset on every PR — check which one a red CI run actually was
before assuming your local `mutation-fast` pass means the exhaustive job
will also pass.

## Kafka/Postgres integration tests: testcontainers, never a skip-gate

A `-tags=integration` test touching Kafka or Postgres MUST start its own
container via `testcontainers-go`. Never gate on
`os.Getenv("KAFKA_BROKERS")` + `t.Skip(...)`, and never hardcode
`localhost:9092`. This fleet's CI `integration` job provisions Postgres
ONLY (no Kafka) — a skip-gated Kafka test silently skips in CI and
proves nothing there, while testcontainers actually exercises the
assertions on the runner. This repo's own working recipe is
`internal/adapters/inbound/kafka/consumer_integration_test.go`, runnable
directly via `make integration-kafka-testcontainers` (unique topic per
test, one shared container per package, explicit `CreateTopics` + poll
for the partition leader before the first read/write).

## Verify before opening the PR

```bash
make check-all   # check + coverage + arch-test + bdd (the full local gate)
```

`make check-all` deliberately does NOT include `mutation-fast`/`vuln`/
`api-lint` — run them explicitly too before pushing:

```bash
make mutation-fast
make vuln
make api-lint
```

CI runs all of these as real blocking/scheduled jobs
(`.github/workflows/ci.yml`'s `mutation-fast`, `mutation`, `vuln`,
`api-lint`, `docs-api-drift`, `helm-lint`, `web` jobs) even though
`make check-all` doesn't invoke them locally — a PR can pass your local
gate and still go red in CI otherwise.

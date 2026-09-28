---
id: 0016-idempotency-key-middleware
title: 16. Transactional Idempotency-Key middleware for POST /standards
sidebar_label: 16. Idempotency-Key middleware
sidebar_position: 16
description: "A route-scoped, transactional Idempotency-Key HTTP middleware for POST /standards -- this service's one true resource-creation endpoint -- that reuses the transactional-outbox's tx-in-context mechanism (extracted into internal/pgtx) so idempotency bookkeeping, the LaborStandard Save(s) and the outbox insert commit or roll back together, with Postgres' own unique-index lock serializing concurrent duplicate submissions."
---

# 16. Transactional Idempotency-Key middleware for POST /standards

## Status

**Accepted** — implemented in the same change that introduced this
record. Ported from order-management's reference implementation
(PR #105, ADR 0023), the fleet's canonical design for this capability.

## Context

`POST /standards` (`DefineStandard`) is this service's **one true
resource-creation endpoint**: it mints a fresh `StandardId`
server-side (`StandardRepo.NextID`) and, when a standard is already
active for the TaskType, additionally closes that prior record —
neither action is idempotent by construction. A client that times out
waiting for the 201, or whose response is lost on the wire, and retries
the exact same `POST /standards` body today gets a **second** standard
defined (and, if one was already active, the FIRST retry's standard
closed a second time) — a silent double-write with no natural-key
collision to catch it, because `StandardId` is server-minted, not
caller-supplied.

Every other mutating surface on this service is Kafka-consumer-driven
(`RecordTaskPerformance`, gated by its own `ProcessedEvents` idempotency
port on the Kafka `event_id` — a different mechanism for a different
transport) or a pure read. There is exactly one endpoint this ADR needs
to cover.

This service already has the machinery an idempotency-key design needs,
from the transactional-outbox rollout (ADR 0010): `postgres.UnitOfWork`
begins a `pgx.Tx` and binds it into the request context so every
`Repo.Save` and `EventPublisher.Publish` inside one use case call
commits or rolls back together. The obstacle is that the tx-in-context
key `unit_of_work.go` used was **unexported and private to
`internal/adapters/outbound/postgres`** — but the idempotency middleware
must live in `internal/adapters/inbound/http` (it wraps the HTTP
handler, not a use case), and this service's own architecture fitness
tests (`internal/architecture/architecture_test.go`) forbid inbound
adapter code from importing the outbound postgres package.

## Decision

**Extract the transaction-in-context mechanism into a new,
dependency-free `internal/pgtx` package** (`WithTx`/`TxFrom`), imported
by BOTH `internal/adapters/outbound/postgres` (which now aliases its own
`withTx`/`txFrom` to `pgtx.WithTx`/`pgtx.TxFrom`) and
`internal/adapters/inbound/http` (the new middleware). Neither adapter
package imports the other; both import the small shared `pgtx` package
sitting beside them. This required **zero changes to any use case** —
`DefineStandard.Execute`'s existing `atomically()`/`UnitOfWork.Execute`
call already has an "already in a tx? just run fn(ctx) directly" branch
(`unit_of_work.go`'s `Execute`: `if _, ok := txFrom(ctx); ok { return
fn(ctx) }`), so once the middleware binds a transaction into the request
context via `pgtx.WithTx`, `DefineStandard`'s own `UnitOfWork.Execute`
call detects it and JOINS it rather than opening a second, nested one.
Verified concretely with a real testcontainers Postgres integration test
(`idempotency_integration_test.go`), not assumed from reading the code.

**Add a route-scoped (chi `r.With(...)`, never `r.Use` — this protects
ONLY `POST /standards`) `RequireIdempotencyKey` middleware**:

1. Missing `Idempotency-Key` header → 400
   (`idempotency-key-required`). Deliberate v1 choice: require it on
   this one true resource-creation endpoint rather than generating one
   server-side.
2. Hash the request body (`sha256`), restore `r.Body` via
   `io.NopCloser` so `handleDefineStandard`'s own JSON decode is
   unaffected.
3. Begin a `pgxpool.Pool` transaction directly (no import of the
   `postgres` package needed — `RequireIdempotencyKey` takes a
   `*pgxpool.Pool`), bind it into the request context via
   `pgtx.WithTx`.
4. `INSERT INTO idempotency_keys (key, method, path, request_hash)
   VALUES (...) ON CONFLICT (key) DO NOTHING` inside that transaction:
   - 1 row (a genuinely new key) → call `handleDefineStandard` with the
     tx-bound context, capturing its response in an `httptest.Recorder`
     rather than writing to the real `ResponseWriter` yet.
   - 0 rows → a row already exists. Postgres' own unique-index lock
     guarantees the ORIGINAL inserting transaction has already resolved
     (commit or rollback) by the time this call observes 0 rows — no
     polling, no lock-retry budget, no timeout needed. Roll back this
     call's own (empty) transaction, then read the existing row
     non-transactionally: hash mismatch → 422
     (`idempotency-key-reused`); hash match → the stored row is
     GUARANTEED to have `status_code`/`response_body` populated (see
     the invariant below) — write it back verbatim, never re-invoking
     `handleDefineStandard`.
5. Fresh-key path only, after the recorder captures the real outcome:
   within the SAME transaction, `UPDATE idempotency_keys SET
   status_code=..., response_body=..., response_headers=...,
   completed_at=now() WHERE key=...`, `COMMIT`, **then** copy the
   recorder's headers/status/body onto the real `ResponseWriter`. Every
   normal completion is cached this way — including a 4xx business
   error (e.g. `standard.ErrNonPositiveExpectedSeconds`), not only a
   201 — so a retry with the same key+body deterministically gets the
   same answer. A panic is recovered, rolls back (no idempotency row
   and no `LaborStandard`/outbox write is ever visible), and is
   re-panicked so the outer chi `Recoverer` still produces the normal
   500 — a panic's outcome is never cached, since it is not a "normal"
   response and a retry after one must re-attempt the real work.

**The core correctness invariant**: a COMMITTED `idempotency_keys` row
can never have a NULL `status_code`, because the outcome `UPDATE`
always runs, in the same transaction, immediately before the `COMMIT`
that makes the row visible at all — there is no third "in progress"
state and therefore no client-facing retry-after/409 needed, unlike
naive two-phase idempotency-key designs.

**Table** (migration `0005_idempotency_keys`, already drafted and
reviewed against the reference schema — no changes needed):

```sql
CREATE TABLE idempotency_keys (
    key              TEXT PRIMARY KEY,
    method           TEXT NOT NULL,
    path             TEXT NOT NULL,
    request_hash     TEXT NOT NULL,
    status_code      INTEGER,
    response_body    BYTEA,
    response_headers JSONB,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at     TIMESTAMPTZ
);
CREATE INDEX idx_idempotency_keys_created_at ON idempotency_keys (created_at);
```

**Composition root** (`cmd/labor/main.go`): `inboundhttp.Server` gained
an `IdempotencyPool *pgxpool.Pool` field, wired from
`persistence.pool` — nil in the in-memory dev configuration
(`DATABASE_URL` unset), exactly this service's existing convention for
every other optional Postgres-backed capability (`UnitOfWork`, the
outbox relay). `NewRouter` applies `RequireIdempotencyKey` to `POST
/standards` only when `IdempotencyPool != nil`; otherwise the route is
wired directly, unprotected — matching the in-memory tests'
already-established expectation that `POST /standards` succeeds with no
`Idempotency-Key` header when no Postgres backing exists.

## Consequences

### Easier

- A client retry after a lost `201` (timeout, connection reset, load
  balancer hiccup) is now provably safe: it gets back the exact
  original response and creates no duplicate `labor_standards` row, no
  duplicate prior-standard closure, and no duplicate outbox event.
- No new client-facing state machine, timeout, or 409-retry-later
  contract — the database's own MVCC/unique-index locking is the entire
  synchronization primitive, proven with real concurrent goroutines
  against real Postgres, not asserted from reading the code.
- `internal/pgtx`'s extraction cost nothing to the use case layer: zero
  changes to `DefineStandard`, zero changes to `RecordTaskPerformance`,
  zero changes to `internal/architecture`'s fitness-test expectations
  (inbound still never imports outbound, outbound still never imports
  inbound — both import the new small shared package instead).

### Harder

- One more table and one more piece of middleware state to reason
  about on this one route. No TTL/cleanup job exists yet for old
  `idempotency_keys` rows — `idx_idempotency_keys_created_at` exists
  purely so a future cleanup job can scan without a full table scan;
  building that job is an explicit, deliberate follow-up, not part of
  this change.
- A caller retrying with the same key but a genuinely different body
  (a real bug on the caller's side, or a key reused across two
  different logical requests) gets a 422 rather than either request
  succeeding — by design, but it means a caller must mint a fresh key
  per logical request, not reuse one casually.
- Every NORMAL response is cached, including a business validation
  error. A caller who wants to correct a mistake and retry with a
  FIXED body must also mint a new `Idempotency-Key` — reusing the old
  one simply replays the old error forever. This is the same
  deliberate v1 simplification order-management's reference
  implementation made, not something this port introduced.

## Alternatives considered

- **Generate the idempotency key server-side (e.g. from a
  content hash of the body alone).** Rejected: two textually identical
  `POST /standards` calls made deliberately (e.g. re-defining PICK at
  45s a week after it was already defined and later reverted) are
  legitimate, distinct business events, and a body-hash key would
  incorrectly treat the second as a replay of the first.
- **A separate "idempotency status" 409/425 response while the
  original request is still in flight.** Rejected: Postgres' own
  blocking behavior on the conflicting `INSERT` already means no
  concurrent request ever observes an in-progress row — by the time any
  request sees `rowsAffected()==0`, the original has unconditionally
  resolved. Adding a client-facing in-progress state would be strictly
  more complexity for zero additional correctness.
- **Apply the middleware globally (`r.Use`) rather than route-scoped.**
  Rejected: every other route on this service is either idempotent by
  HTTP semantics (`GET`) or has no server-generated id to protect
  against duplication. Scoping narrowly avoids paying the
  transaction-per-request cost, and the extra bookkeeping table, on
  routes that do not need it.

## Verification

- Unit/build: `go build ./...`, `go build -tags=integration ./...`,
  `go vet ./...`, `go vet -tags=integration ./...`, `gofmt -l .`
  (clean), `make check` (fmt-check, vet, build, lint 0 issues, `go test
  ./... -race`) all green.
- Architecture fitness (`make arch-test`): the existing
  inbound-does-not-depend-on-outbound /
  outbound-does-not-depend-on-inbound rules still pass unmodified —
  `internal/pgtx` sits outside both, imported by both.
- Coverage (`make coverage`, gated at 90% over
  `./internal/domain/...,./internal/application/...,./internal/analytics/...`):
  unchanged at 99.4% before and after this change — the middleware
  itself lives in the HTTP adapter package, outside the gated packages,
  and is verified by the integration suite below instead, mirroring
  this service's existing convention for adapter-layer coverage.
- Integration (`-tags=integration`, **testcontainers** Postgres
  `postgres:16-alpine` through the REAL chi router
  (`inboundhttp.NewRouter`) over real `net/http` requests — the test
  owns its own disposable database, never reads `DATABASE_URL`, never
  hardcodes `localhost`, never `t.Skip`s):
  `internal/adapters/inbound/http/idempotency_integration_test.go`,
  six scenarios, all passing with `-race -count=1`:
  - a fresh key + valid body creates the standard and the
    `idempotency_keys` row records the exact 201 outcome byte-for-byte;
  - replaying the same key + identical body returns the byte-identical
    response and creates no second `labor_standards` row;
  - the same key with a different body returns 422
    (`idempotency-key-reused`) and creates no second row;
  - no header returns 400 (`idempotency-key-required`) and creates no
    row at all;
  - 5 real concurrent goroutines with the same key+body all receive
    the identical 201 and exactly one `labor_standards` row is
    created — the real Postgres unique-index-lock proof, not a
    sequential simulation;
  - a business-validation error (`ExpectedSeconds<=0`, 422) is cached
    and replayed verbatim on retry with the same key+body, and no
    `labor_standards` row is ever created either time.
- The full existing integration suite (`go test -tags=integration
  ./... -race -count=1`) — including the outbox, Kafka consumer and
  Postgres repo integration tests already in this repo — passes
  unmodified after the `internal/pgtx` extraction, confirming the
  refactor of `unit_of_work.go` did not change its observable behavior.

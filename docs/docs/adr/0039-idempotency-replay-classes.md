---
id: 0039-idempotency-replay-classes
slug: /adr/0039-idempotency-replay-classes
title: "39. Every problem type has an idempotency replay class; a retryable refusal is not stored"
sidebar_label: "39. Idempotency replay classes"
description: "ADR 0039 — a request retried under the same Idempotency-Key is answered from the store only when its first answer was final for that request. Each problem type declares FINAL, OUTCOME or RETRYABLE on its catalogue entry; a RETRYABLE refusal (concurrent-modification, standard-conflict, a 404 on a fact not known yet, any 5xx) rolls the transaction back, key row included, and a retry executes again. Supersedes the 'every 4xx is stored and replayed' sentence of ADR 0016 and 0037."
---

# 39. Every problem type has an idempotency replay class; a retryable refusal is not stored

## Status

Accepted (2026-11-20). Supersedes the sentence of
[ADR 0016](./0016-idempotency-key-middleware.md) and
[ADR 0037](./0037-adopt-fleet-rest-api-guideline.md) that says every `4xx`
answer is stored and replayed (their Decision text is not rewritten; each
carries a dated pointer here). Implements the fleet rule in
`.claude/rules/fleet/idempotency-and-outbox.md` (owner decision 2026-11-18).
Follows [ADR 0022](./0022-optimistic-concurrency-one-open-standard.md) (the two
`409`s) and [ADR 0025](./0025-rfc7807-problem-details.md) (the problem catalogue).

## Context

`POST /standards` is this service's only write over REST and the only route
behind the `Idempotency-Key` middleware. The middleware stored every `2xx` and
every `4xx`. A `4xx` that a retry could pass was therefore pinned for the whole
`IDEMPOTENCY_KEY_TTL` (24 h): a client that got `409 concurrent-modification`
or `409 standard-conflict`, did exactly what the problem says ("reload and
retry") with the same key, and was handed the same `409` back from the store
without the request ever executing again. ADR 0022 even called that "the
correct answer for that logical request". It is not: the answer depended on
state (which standard was open at that instant) that had moved on.

### The store design (verified in code)

The key row lives in the SAME pgx transaction as the business unit of work
(`idempotency.go`): `pool.Begin`, `INSERT ... ON CONFLICT (key) DO NOTHING`, the
handler runs behind `SAVEPOINT handler_run` with the transaction bound into the
context (`pgtx.WithTx`), so `postgres.UnitOfWork.Execute` joins it
(`unit_of_work.go`: a context that already carries a transaction runs `fn`
directly, no inner transaction). Then one `UPDATE` of status, body and headers,
`COMMIT`, and only then is the client answered. In-flight duplicates block on
the primary key of the `INSERT` until the first transaction ends. A `5xx` or a
panic rolls the whole transaction back (the key row and every write vanish).
Replay sets `Idempotency-Replayed: true`. Expired rows are purged by the
housekeeping sweeper (`postgres/sweeper.go`, `IDEMPOTENCY_KEY_TTL`, default
24 h). A `4xx` was kept with `RELEASE SAVEPOINT` (the handler's writes survive);
`ROLLBACK TO SAVEPOINT` ran only when the failed statement had aborted the
transaction.

## Decision

Each problem type this service can answer has exactly one class, declared ON its
catalogue entry (the `class` field of `problemInfo`, `errors.go` for the typed
domain errors and `problems.go` for the rest):

| Class | Meaning | Stored and replayed |
|---|---|---|
| `FINAL` | the answer is a function of the request alone | yes |
| `OUTCOME` | the command COMMITTED state, then answered an error that must be remembered | yes, and its writes commit with the stored answer |
| `RETRYABLE` | nothing committed; the answer depends on state that can change | no: the key is released and a retry executes |

- A `2xx` is stored. A `5xx` and a panic are never stored. The middleware reads
  the class (the shared error writer `writeProblem` marks the capturing writer,
  `replay_class.go`) and holds **no list of slugs** and no status range beyond
  `2xx` and `5xx`.
- A refusal that declares no class (a handler writing a bare status) is
  released like `RETRYABLE`: the safe fallback.
- A problem type without a class fails a unit test
  (`TestEveryProblemTypeDeclaresAReplayClass`), and a `problemInfo` literal
  outside the catalogue files fails another. There is no runtime default.
- **RETRYABLE is released exactly like the `5xx` branch**: `tx.Rollback`, key row
  and handler writes with it (`idempotency.go`, `if !rec.storable()`), checked
  BEFORE the savepoint is released, so an aborted transaction needs no healing.
  The refusal is still answered to the caller.
- **A concurrent duplicate** that was blocked on the key's `INSERT` while the
  first request ended `RETRYABLE` proceeds as a fresh request the moment the
  rollback lands (its `INSERT` succeeds): it executes and may itself succeed. It
  does not see the refusal. A request that arrives after the release is the same.
- **Releasing means a different body under the same key afterwards is a new
  request**, not `409 idempotency-key-reused`: no row is left to compare against.
- RELEASED means only that the same key may be tried again by whoever sent it.
  This ADR adds no retry behaviour to any client or consumer; a person-driven
  caller must surface the refusal, and only automation retries (a separate change).

### Class table (held equal to the catalogue by `TestTheADRClassTableMatchesTheCatalogue`)

| Slug | Status | Class | Why |
|---|---|---|---|
| `standard-not-found` | 404 | RETRYABLE | no ACTIVE standard for the task type yet; a `POST /standards` or a revision makes it exist. Answered by `GET` only. |
| `associate-not-found` | 404 | RETRYABLE | associate performance is fed by `TaskCompleted` events; the next one makes it exist. Answered by `GET` only. |
| `concurrent-modification` | 409 | RETRYABLE | `StandardRepo.Save` found the row's version moved on (`standard_repo.go` `RowsAffected() == 0`); nothing was written; a retry reloads the current standard. |
| `standard-conflict` | 409 | RETRYABLE | a lost race on the one-open-standard index (`23505`, `standard_repo.go`); the winner's standard is now open, so the retry supersedes it. |
| `unknown-task-type` | 400 | FINAL | `PICK`, `PACK`, `SLAM` is a fixed set. |
| `unknown-handling-unit` | 400 | FINAL | `EACH`, `CASE`, `PALLET` is a fixed set. |
| `invalid-site-code` | 400 | FINAL | 1-32 characters, no whitespace: a function of the field. |
| `empty-event-id` | 400 | FINAL | an input check on the Kafka path; never on a keyed route. |
| `empty-task-id` | 400 | FINAL | as above. |
| `non-positive-expected-seconds` | 422 | FINAL | function of the body. |
| `negative-travel-component-seconds` | 422 | FINAL | function of the body. |
| `travel-component-exceeds-expected-seconds` | 422 | FINAL | function of the body. |
| `team-lift-requires-case-unit` | 422 | FINAL | function of the body (`handlingUnit` with `teamLift`). |
| `non-positive-team-lift-threshold` | 422 | FINAL | function of the body. |
| `non-positive-team-lift-expected-seconds` | 422 | FINAL | function of the body. |
| `malformed-request-body` | 400 | FINAL | invalid JSON, an unreadable body or a NUL character: all answered before any write (see judgement call 3). |
| `idempotency-key-required` | 400 | FINAL | answered before a key row exists; never stored. |
| `idempotency-key-invalid` | 400 | FINAL | as above. |
| `idempotency-key-reused` | 409 | FINAL | the store's own answer to a key seen with another request; no handler ran, no row is written for the request that gets it. |
| `resource-not-found` | 404 | FINAL | no route matches the path (a fixed route table), or a NUL in the path. |
| `method-not-allowed` | 405 | FINAL | the same, for the method. |
| `invalid-report-query` | 400 | FINAL | reports API only (no keyed route): a function of the query string. |
| `internal-error` | 500 | RETRYABLE | a `5xx` is never stored. |
| `dependency-unavailable` | 503 | RETRYABLE | `/readyz` only; a `5xx` is never stored. |
| `report-store-error` | 500 | RETRYABLE | reports API only; a `5xx` is never stored. |

### No OUTCOME

An `OUTCOME` would be a refusal that provably committed state. None does.
`DefineStandard.ExecuteSpec` (`define_standard.go`) refuses either before its
first write (the lookup at line 75, `buildTeamLift` at line 85, `NewForScope` at
line 90: all pure or read-only; `NextID` mints a UUID in memory, line 80) or from
inside the atomic block (lines 99-113: `Save(prior)`, `Save(next)`, `publish`),
and a failed block answers a refusal with nothing the caller needs remembered.
On the keyed route the unit of work JOINS the middleware's transaction (no inner
transaction to roll back), so the rollback of a `RETRYABLE` refusal is the
middleware's, and it takes a half-done `Save(prior)` of a `Save(next)` that
failed with it (a case the row locks make practically unreachable, but one that
a stored `409` would have committed with the closed prior standard; RETRYABLE
removes it by construction). The store would allow
an `OUTCOME` (a `4xx` keeps the handler's writes via `RELEASE SAVEPOINT`), and the
class and its handling exist in the code (`classOutcome`, an `OUTCOME` refusal
behind an aborted transaction fails the request rather than store an answer whose
writes were discarded), but there is no entry of that class and no proof to write.
The harness test `TestOutcomeRefusalCommitsItsWritesOnceAndIsReplayed` skips on an
empty class, by design; it starts to run the day the first `OUTCOME` entry lands.

### Judgement calls (most consequential first)

1. **`standard-conflict` is RETRYABLE although its slug reads like a business rule.**
   In `DefineStandard` a prior open standard is superseded, never refused, so the
   conflict can only be the loss of a race (`standard_repo.go`, `23505` on
   `idx_labor_standards_one_open_per_task_type`). By the time the loser retries, the
   winner's standard is open and the retry supersedes it. FINAL would pin a false
   "another standard is open" for 24 h on a request that now succeeds.
2. **`standard-not-found` and `associate-not-found` are RETRYABLE.** They are not
   the target of a keyed command (only `GET` answers them), so they never reach the
   store today; they are classified because the catalogue must be complete and
   because the fleet default says a 404 on a fact that an event or another request
   may deliver later is RETRYABLE. This service mints the ids (`NextID`) but a
   standard is defined by a later request and a performance row by a later event.
3. **`malformed-request-body` is FINAL for all three of its variants, including the
   unreadable body.** One slug has one class. A body that cannot be read is answered
   by the middleware before a transaction is opened (and by `rejectNUL` before the
   middleware), so the class has no runtime effect; FINAL records that the content
   variants are functions of the body.
4. **The three `idempotency-key-*` problems are FINAL although none is ever stored**:
   each is answered before or instead of the handler.
5. **Splitting is a follow-up, not part of this change**: no slug here mixes request-only
   and state-dependent causes, so none had to be forced to RETRYABLE.

## Consequences

**Positive**

- A client that gets `409 concurrent-modification` or `409 standard-conflict` and
  retries with the same key now has the request executed again; if it passes, the
  `201` is stored and replayed from then on.
- The class is declared where the problem is declared; a new problem type cannot
  ship without one, and the ADR table cannot drift from the catalogue.
- `2xx`, `5xx` and every FINAL answer are byte-identical to before; slugs, titles and
  statuses are unchanged on the wire.

**Negative / accepted**

- A refused request that is retried with the same key re-executes (one more
  execution) instead of replaying; that is the intent.
- A different body under a key whose first answer was RETRYABLE is a new request.
- `Idempotency-Replayed: true` is no longer set on a retry of a RETRYABLE refusal
  (it is a fresh execution): the header means "stored answer".
- Retention: expired rows are purged hourly by the sweeper, but a replay does not
  itself check the row's age, so a row can outlive `IDEMPOTENCY_KEY_TTL` by up to
  one sweep interval. Recorded, not changed here.
- MCP and A2A expose no write use case (`mcp/tools.go`, `a2a/server.go`), so there is
  no second store to classify, and the service calls no sibling over REST or MCP, so
  there is no outbound caller with a deterministic key.

## Verification

- `replay_class_test.go` (unit, runs without Docker): every entry declares a class; the
  unclassified-entry check bites; no `problemInfo` literal outside the catalogue; the
  ADR table equals the catalogue; the storable decision for every status and class;
  `writeProblem` and `writeError` mark the capture.
- `idempotency_replay_classes_integration_test.go` (the harness template, over a real
  Postgres through the real middleware): per entry, the same key twice replays (FINAL)
  or executes twice (RETRYABLE, every `5xx`); RETRYABLE releases the key, the retry runs
  and is then stored, another body under the released key is a new request; `2xx` stored;
  `5xx` and a panic not.
- `idempotency_retryable_integration_test.go` (real router, real Postgres):
  `concurrent-modification` and `standard-conflict` leave no key row, the retry under the
  same key executes and is then stored and replayed.

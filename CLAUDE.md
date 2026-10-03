# Project: Labor Performance (Supporting Bounded Context)
Engineered labor standards ("a PICK should take 45s") and actual-vs-standard
performance scoring for the `warehouse-systems` fleet (Go 1.26, module
`github.com/claudioed/labor-performance`). Docs: https://claudioed.github.io/labor-performance/

> **Study project.** Educational DDD exercise using real industry patterns
> (WMS/WES, CloudEvents 1.0, RFC 7807, hexagonal). Not a production system;
> not affiliated with any retailer or WMS vendor.

## Non-negotiables (read before writing any code)

1. **Pure Kafka consumer of `fulfillment-execution`, nothing else.** This
   service subscribes to `warehouse.fulfillment.events` (shared/fan-out with
   `wes-work-planning`) and reacts only to
   `com.warehouse.wes.fulfillment-execution.task.TaskCompleted`. It is a
   separate Go module/repo: **no Go import from, and no write access to**,
   `fulfillment-execution` or `workforce-management`. It **NEVER calls ANY
   sibling context over REST or MCP** — no live lookups, ever; cross-context
   data must be local/declarative. Choreography, not orchestration (ADR 0015
   restates this for facility-layout). Siblings may read this context's own
   surfaces and consume its integration topic. See
   `.claude/rules/domain-model.md` and ADR 0002/0003/0013.
2. **Hexagonal / ports & adapters, strict inward dependency rule:** domain
   depends on nothing; application depends on domain; adapters depend on
   application/domain. No framework, HTTP, Kafka, or SQL types in the domain
   layer (arch-go enforced, `internal/architecture/architecture_test.go`).
3. **Never fabricate a number.** `EfficiencyPct`, `meanEfficiencyPct`, and
   `meanActualSeconds` are nullable everywhere (domain, REST, analytics
   report) and MUST be `null`, never `0`, when nothing was scorable/
   measurable. See `.claude/rules/domain-model.md`.
4. **This service NEVER accepts a TaskPerformance write over REST.**
   Recording a performance row is exclusively Kafka-consumer-driven
   (`RecordTaskPerformance`, idempotent on the CloudEvents `id`).
5. **One writer per database (ADR 0007/0010).** `cmd/labor-projector` is the
   ONLY writer of the analytical DB; `cmd/labor-reports` uses a read-only
   pool. `internal/analytics/report` depends on NOTHING and OLTP layers must
   not import it (arch-go enforced).
6. **No REST or MCP surface is authenticated** (ADR 0012 removed ADR 0011's
   bearer layer). Do not re-implement it.

## Events: CloudEvents 1.0 is MANDATORY

Every Kafka message produced or consumed (integration
`warehouse.<ctx>.events` AND analytics `warehouse.<ctx>.analytics`) is a
CloudEvents 1.0 event in structured content mode — a hard fleet rule, not a
preference (ADR-0021):

- No flat envelope, no dual-write, no dual-read, no envelope toggle env var.
- Build/decode ONLY via `internal/adapters/kafka/cloudevents/`
  (sdk-go v2 `event`); transport stays kafka-go.
- Consumers dispatch on the FULL `type`, ignore unknown types, dedupe on
  `id`, and DLQ/skip (never crash, never parse a legacy shape) anything that
  fails CloudEvents validation.
- Breaking payload change => new `.v2` type + new dataschema version, never
  mutate.
- Required attributes, type naming and this service's exact type list:
  `.claude/rules/cloudevents-envelope.md`.

## Key commands

```bash
go run ./cmd/labor            # in-memory adapters, no DB/broker needed
docker compose up -d postgres # :5435; set DATABASE_URL, migrations run automatically

make check-fast  # fmt-check + vet + arch-test — run before saying "done"
make check       # fmt-check + vet + build + lint + test -race (mirrors CI)
make check-all   # check + coverage (gate: 90%) + arch-test + bdd

cd docs && npm ci && npm run typecheck && npm run build   # docs site
npm run clean-api-docs:all && npm run gen-api-docs:all    # what CI's docs-api-drift job runs
```

`lefthook install` wires `pre-commit` (fmt-check/vet/lint) and `pre-push`
(`make check`) — activate once per clone (hooks are not tracked by git).
Other verification surfaces (BDD, integration, mutation, spectral, ct lint): `.claude/rules/architecture-and-testing.md`.

## Testing standards (hard rules)

- Never hit a real broker in unit tests (fake Kafka reader, in-memory
  adapters); one httptest per REST endpoint (success + error path).
- Coverage gate 90% on domain, application and analytics (`make coverage`).
- RFC 7807 `application/problem+json` for every error response, same shape
  on the OLTP and reports APIs.

## Git workflow

- GitFlow: `develop` is the default/working branch; `main` is release-only.
- Branch off `develop`, PR back into `develop` (`gh pr create --base
  develop`). Never force-push over pushed history. Do not self-merge —
  leave PRs open for review unless told otherwise.
- **This repo's `github-pages` environment allows deploys from BOTH
  `develop` and `main`**, but `.github/workflows/docs.yml` currently
  triggers only on push to `main` — see `.claude/rules/docs-and-api-drift.md`.

## Reference rules (load when relevant)

- `.claude/rules/domain-model.md` — why this context exists, ubiquitous
  language, aggregates & invariants, domain events, use cases, REST/Kafka
  contracts, v1 scope decisions.
- `.claude/rules/architecture-and-testing.md` — processes, layering notes,
  ports, code standards, extra verification commands.
- `.claude/rules/cloudevents-envelope.md` — full CloudEvents attribute list
  and this service's event types.
- `.claude/rules/docs-and-api-drift.md` — two OpenAPI specs, Docusaurus
  wiring, `@faker-js/faker` / `postman-collection` pitfall, docs.yml gap.
- `.claude/rules/adrs-and-decisions.md` — index of ADRs and what each one
  settles (ADR 0021 = the mandatory CloudEvents envelope). Fleet-wide rules: `.claude/rules/fleet/*.md` (canonical in IQVO/warehouse-docs `agents/fleet/`; never hand-edit).

<!-- harness:scoped-rules:start (generated by tools/migrate_v3.py in warehouse-harness-template; do not hand-edit) -->
## Scoped rules and harness

Claude Code loads each rule below automatically when you touch the matching paths. OpenCode and Codex do NOT: read the rule BEFORE editing matching files.

| When touching | Read |
|---|---|
| `docs/**`, `apis/**` | `.claude/rules/docs-and-api-drift.md` |

Hooks (`scripts/harness/hook.py`, wired for Claude Code, Codex and OpenCode) block pushes to develop/main, `--no-verify`, bare `rm -rf`, and edits to generated files, and feed gofmt/vet findings back after each edit. Before saying "done" run `make check-fast`; the full gate is `make check-all`. `HARNESS_OFF=1` disables the hooks when debugging the harness itself.
<!-- harness:scoped-rules:end -->

---
name: how-to-write-an-adr
description: Write an Architecture Decision Record in this repo's numbering and format, including companion ADRs for cross-repo changes. Use when a design decision should be recorded or a change contradicts an existing ADR.
---

# How to write an ADR

Use when a change is architecturally significant — a new bounded-context
integration, a reversal of a prior decision, a cross-repo contract
change, or anything a future reader would otherwise have to
reverse-engineer from the diff. Not every change needs one: a bug fix or
a routine feature addition inside an already-decided architecture
doesn't.

## Numbering and location

`docs/docs/adr/NNNN-kebab-case-title.md`, four-digit zero-padded,
sequential — check the highest existing number
(`git ls-tree --name-only origin/develop -- docs/docs/adr/` and pick the
next integer, never reuse or guess). As of this writing the highest is
`0015-optional-travel-component-on-labor-standard.md`, so the next ADR
in this repo is `0016-...`. `docs/docs/adr/about.md` explains the format
to readers; you don't need to touch it when adding a new ADR.

## Frontmatter (Docusaurus needs all five fields)

```yaml
---
id: NNNN-kebab-case-title
slug: /adr/NNNN-kebab-case-title
title: "NN. Title (a short noun phrase, matching the heading)"
sidebar_label: "NN. Short label for the nav sidebar"
sidebar_position: NN
description: "One or two sentences — this shows up in search and link
  previews, so make it stand alone without the rest of the doc."
---
```

`id`/`slug` are the full kebab-case filename (minus `.md`); `title`/
`sidebar_label` repeat the number as plain text (`"12. ..."`, not
`#12`); `sidebar_position` is the bare integer. Getting these
inconsistent is the most common cause of a broken sidebar entry or 404
after merge — verify by running the docs build (see below) before
opening the PR. See `0012-remove-rest-auth-layer.md` for a
real, currently-merged example of this exact frontmatter shape.

## Format: Michael Nygard's template

```markdown
# NNNN. Title (a short noun phrase)

## Status
Accepted | Proposed | Deprecated | Superseded by ADR-XXXX

## Context
The forces at play — technical, business, constraints — that make this
decision necessary. Write in the past tense, as if explaining to someone
who wasn't there. State the alternatives seriously considered, not just
the one chosen; a reader six months from now needs to know a simpler
option was weighed and rejected, not assume nobody thought of it.

## Decision
What was actually decided, stated as an active, present-tense
declaration ("we will...", not "we might..."). Be specific about the
mechanism, not just the intent — this section should let a reader
implement the same decision from scratch without asking follow-up
questions.

## Consequences
What becomes easier, what becomes harder, and what future work this
creates or forecloses. Be honest about the downsides — an ADR that only
lists benefits reads as marketing, not a decision record.
```

The `## Decision` section is the part worth the most editing effort: see
ADR-0003 (`docs/docs/adr/0003-kafka-choreography-consumer-of-fulfillment-execution.md`)
for a model example in this repo — it states the exact mechanism (a pure
Kafka consumer under a named consumer group, idempotency keyed on the
event id rather than `TaskId`), names the wire-contract gap
it accepts (`TaskType` resolving to unclassified when absent), and is
specific enough that this skill set's own
how-to-add-an-integration-event guide can point straight at it for the
worked example.

## Superseding an earlier ADR

Don't edit the old ADR's Decision section. Add a `## Status` line noting
`Superseded by ADR-XXXX` on the OLD one (a one-line patch), and open the
new ADR referencing it. This repo has a real, merged example of exactly
this pattern: ADR-0012 (`0012-remove-rest-auth-layer.md`) supersedes
ADR-0011 (`0011-rest-auth-static-bearer-scopes.md`) — its Status section
reads **"Accepted. Supersedes [ADR 0011 — REST identity: fleet-standard
static bearer keys with read/read-write scopes](./0011-rest-auth-static-bearer-scopes.md)."**
Copy that exact wording pattern (accepted-and-supersedes in one
sentence, with a relative markdown link) rather than inventing new
phrasing.

## Cross-repo decisions: use a companion ADR, not one repo's private opinion

When a decision genuinely spans two bounded-context repos, write ONE ADR
per repo, each referencing the other explicitly as "the companion ADR"
with a one-line description of the split of responsibility. This
repo already has the reverse of that pattern documented in ADR-0002 and
ADR-0003: ADR-0002 explains why this context exists as separate from
`workforce-management`/`fulfillment-execution` (citing each sibling's
own CLAUDE.md boundary promise), and ADR-0003 covers the mechanics of
how it observes `fulfillment-execution`'s `TaskCompleted` event without
touching either sibling. If a future decision spans this repo and, say,
`fulfillment-execution`'s own publisher changes, write this repo's half
here and reference fulfillment-execution's companion ADR by name — don't
assume a reader of this repo's docs site will discover the other
repo's record on their own.

## After writing: regenerate and verify the docs build

```bash
cd docs
npm ci
npm run build   # onBrokenLinks / onBrokenAnchors are both 'throw' — this
                 # WILL fail if the frontmatter/slug is wrong or a
                 # cross-reference link is broken
```

A broken ADR link or malformed frontmatter fails the build with a clear
Docusaurus error, not a silent 404 — always run this locally before
opening the PR. This repo's `docs.yml` workflow runs the docs build in
CI too, but don't rely on CI alone to catch it — a red `docs.yml` run
after merge is a much more expensive fix than catching it locally first.

---
title: The duplication scanners do not look where agents actually duplicate
date: 2026-08-12
category: workflow-issues
module: tooling
problem_type: workflow_issue
component: development_workflow
severity: medium
root_cause: inadequate_tooling_scope
resolution_type: workflow_improvement
applies_when:
  - A rule, contract or invariant is described in more than one file
  - Reviewing a change that alters a documented contract
  - Trusting a green duplication scan as evidence that a change introduced no duplication
  - An agent has written the same explanation into several docs, headers or instruction files
symptoms:
  - The same stale claim is found and fixed several times, in different files, by different reviews
  - Reviews report each stale sentence as its own finding rather than naming the duplication
  - "dups: clones" and "dups: similarity" pass while a contract is restated in nine places
tags:
  - dry
  - duplication
  - documentation-drift
  - review-limits
  - tooling-scope
---

# The duplication scanners do not look where agents actually duplicate

## What happened

`astubbs/parallel-consumer#287` changed what the `claude-review` gate asserts. The contract was
stated in **nine files, in nine different sentences**. The change updated the places that *argued*
for the contract and missed the places that merely *restated it in passing*.

Four separate review rounds then found four stale restatements - `AGENTS.md`, the `docs/ci.md`
workflow overview, the dispatch workflow header, and the in-flight tracker - **one at a time**, over
four pushes. Each was reported, and fixed, as its own P2.

Meanwhile both duplication jobs were green on every one of those pushes.

## Why the scanners were silent

Not bad luck. They cannot see it, for two independent reasons.

**1. Scope: they only scan Java source.** From `.github/workflows/maven.yml`:

- `dups: clones` - `directories: 'parallel-consumer-core/src parallel-consumer-vertx/src …'`
- `dups: similarity` - the same directories, **plus** `file_extensions: 'java'`

`docs/`, `.github/`, `bin/` and `AGENTS.md` are not scanned by either. The scanners did not miss the
duplication; they never looked at the files containing it.

**2. Capability: this is semantic duplication, not lexical.** Even pointed at `docs/`, a token-based
clone detector (CPD, jscpd) finds repeated *token sequences*. Nine paraphrases of one rule share
almost no token runs. The whole-file similarity tool compares files overall, and an `AGENTS.md`
against a workflow YAML is overwhelmingly dissimilar apart from one sentence - far below its
`ignore_below: 30` floor.

So the class the scanners catch is **copy-paste**, and the class that bit here is **restatement**.

## Why the reviews did not name it either

Both automated reviewers reported symptoms. That is a property of reviewing a *diff*: each round's
diff contained one or two stale sentences, so each round produced one or two findings about
sentences. Counting instances across the whole repo means stepping outside the diff, which none did.

The fourth review came closest and is worth reading as the near-miss: it explicitly said *"this is
the exact class of drift caught three other times on this PR"* - naming the class - and then still
prescribed the symptom fix, changing the sentence. Naming a class is not the same as proposing a
structural fix.

## The rule

**A contract gets one canonical statement. Everything else links to it.**

When you find yourself fixing the same stale claim in a second file, stop fixing and start counting:
`grep -rn` the claim across `docs/`, `.github/`, `bin/` and the instruction files. If it is stated
in more than one place, the duplication is the defect and the stale sentence is a symptom.

In `#287` this collapsed nine statements to one canonical paragraph in `docs/ci.md`, marked as
canonical in an HTML comment, with every other mention reduced to a pointer.

## What this means for the scanners

The scanners exist **because** agents struggle to stay DRY - that is why there are several. This
finding says the defence has a hole exactly where that failure concentrates: agents duplicate prose
and configuration at least as readily as code, and neither scanner is pointed at either.

Two follow-ups, tracked in [`docs/inflight/ci-review-agent.md`](../../inflight/ci-review-agent.md):

- Point a clone engine at `docs/` and `.github/` (jscpd handles markdown). It will catch verbatim
  copy-paste between docs, which is a real and frequent agent behaviour. It will **not** catch
  paraphrase - do not let a green scan there be read as "no duplication".
- For contracts specifically, the mechanical guard is narrower and more reliable than a scanner:
  assert that the phrasing appears in its canonical file and nowhere else.

## See also

- [`negative-results-need-an-instrument-that-could-have-said-yes.md`](negative-results-need-an-instrument-that-could-have-said-yes.md) -
  the same shape one level up. A green duplication scan is a negative result, and it was worthless
  here because the instrument could never have said yes.

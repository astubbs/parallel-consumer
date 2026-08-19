---
title: "The write-up existed, was accurate, and was still not reached for - reach for it at the symptom, not after"
date: 2026-08-19
category: workflow-issues
module: CI/GitHub Actions
problem_type: workflow_issue
component: development_workflow
severity: medium
applies_when:
  - A CI log read returns less than expected, or nothing
  - Diagnosing a chaos-suite or other long-log job failure
  - A docs/solutions/ write-up exists for a failure mode that keeps recurring
---

# The write-up existed, was accurate, and was still not reached for

## What happened

Diagnosing a `Chaos Pain Suite` failure, `gh run view --job <id> --log` returned nothing usable.
Two further attempts (`gh api .../jobs/<id>/logs`, a filtered re-read) also came back empty. Only
then was `docs/solutions/workflow-issues/gh-run-view-log-truncation.md` opened - which describes
this exact failure, names the working route, and records that it had **already caught two people**,
the second of whom *"had this document available and did not read it"*.

Pulling the run-logs archive endpoint gave the full 12,015-line log immediately, and the diagnosis
was straightforward from there. The truncating route had silently returned a fraction of it.

## The part that matters

This is now the third recorded instance, and the pattern across all three is identical: the
document is found **after** the fruitless attempts, not before. Each person had it available. Each
reached for the obvious command first, then a variant, then a third, and only searched the solutions
directory once out of ideas.

So the failure is not knowledge. The corpus is right, discoverable by an obvious grep, and says so
plainly. The failure is *timing*: a `docs/solutions/` search is treated as an escalation step rather
than a first move, because the first command feels too simple to have a known trap.

That framing is backwards for exactly this class. A tool returning **less** than expected - empty
output, a suspiciously short log, a clean result where a failure was expected - is a much stronger
signal of a known trap than a loud error, because a loud error tends to explain itself.

## What to do

- **Search `docs/solutions/` at the symptom, not after the third attempt.** One `grep -rl` is
  cheaper than the second failed command, let alone the wrong diagnosis at the end of them.
- Treat "returned nothing / returned less than expected" as the trigger. Silence is the signature of
  a swallowed limit.
- For CI logs specifically, prefer the run-logs archive endpoints over `gh run view --log`; the
  existing write-up carries the commands and the completeness check.
- When a write-up records its own repeat offences, that count is data. A third instance means the
  document is not the intervention - reaching for it earlier is.

## Related

- [`gh-run-view-log-truncation.md`](gh-run-view-log-truncation.md) - the underlying trap, its
  commands, and the first two instances
- [`bug-857-family.md`](../../inflight/bug-857-family.md) - the family this was diagnosed for; a
  truncated read had already misattributed one of its sightings

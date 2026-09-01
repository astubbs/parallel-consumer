---
title: A lane nothing runs cannot catch its own guard drifting
date: 2026-09-01
category: workflow-issues
module: build-system
problem_type: workflow_issue
component: development_workflow
severity: high
root_cause: config_error
resolution_type: workflow_improvement
applies_when:
  - "Adding a non-gating, opt-in test lane or CI check that is meant to be wired in later"
  - "A lane carries a hand-pinned roster or count guard specifically because a derived count is blind to a dropped item"
  - "Reviewing a PR where a detector exists and passes by hand but no workflow invokes it"
  - "Wiring an existing script into a CI workflow for the first time"
  - "Judging whether a lane is done - the script exists and passes locally is a different claim from some workflow invokes it on every PR"
symptoms:
  - "A code review flags that a regression detector never runs in CI, though its script exists and is documented"
  - "The first CI run of a newly-wired lane fails immediately on its own roster guard, not on the thing the lane exists to catch"
  - "A hand-pinned EXPECTED count in a guard script is wrong on the base branch, unrelated to the current branch's changes"
  - "A class was added to the tagged set without anyone bumping the guard's expected count, and nothing caught it"
tags: [ci, silent-failure, false-negative, guard-design, lincheck, roster-guard, definition-of-done, non-gating]
---

# A lane nothing runs cannot catch its own guard drifting

## Context

`bin/lincheck-test.sh` runs the repo's Lincheck concurrency lane - scheduler-controlled
interleaving tests over `parallel-consumer-core`'s state classes. The script carries a hand-pinned
roster guard, `EXPECTED_LINCHECK_CLASSES`, and its own header explains why the number is typed in
rather than derived:

> WHY THE NUMBER IS PINNED BY HAND rather than derived from `grep -rl '@Tag("lincheck")'` at run
> time [...] deriving it makes the guard BLIND to a dropped tag, because the same missing tag that
> drops the class from the run also drops it from the expectation, and the two cancel to a green.

That reasoning is sound, and the count was **correct at birth**: astubbs#347 created exactly five
tagged classes and pinned the constant at 5. The guard went stale at the very next roster addition
- the RetryQueue off-lock fix landed a sixth harness (`RetryQueueLincheckTest`) with no bump, and
prior-session records show the class was noticed, manually run, and verified at the time, while no
one touched the count (session history). That is precisely the event the guard was designed to
catch. It caught nothing, because the lane was opt-in: `lincheck` sat in the pom's default
`excluded.groups`, and no CI workflow invoked `bin/lincheck-test.sh` - a shape the adoption plan
(`docs/plans/2026-08-25-001-test-lincheck-poc-plan.md`) had recorded as deliberate at the time,
and which later tooling lanes then reused as precedent without re-litigating (session history).

A multi-agent code review on astubbs/parallel-consumer#392 flagged the gap directly: dropping
`synchronized` from an allocator's spend path - the lane's own control-arm red - would merge with
every other check green, because the one check built to catch it never ran. That branch had also
just added a seventh harness, again without bumping the constant. When the lane was wired into
`maven.yml`, its **first execution failed**: `selected=7` against `EXPECTED_LINCHECK_CLASSES=5` -
two generations of silent roster growth surfaced by the first run anything ever gave the guard.

A smaller instance of the same rot sat in the same file: the header claimed a 26-29s runtime;
re-measured at wiring time it was 42-53s. Unexecuted claims drift exactly like unexecuted guards.

## Guidance

**"Runs somewhere on every PR" is part of a verification lane's definition of done, not a
follow-up task.** A lane shipped as "opt-in for now, wire it in later" starts rotting the moment
it ships, and the parts that rot first are the ones with no other reason to be touched: guards,
counts, timing claims in comments.

**Treat an unwired lane as equivalent to no lane, not as partial coverage.** "The harnesses exist
and the roster guard would catch drift" was true and irrelevant - drift shipped through the gap
twice, because "would catch" and "does catch" differ by exactly the CI wiring step.

**When wiring a lane in, pick the shape by cost - and keep "own job" and "own JVM" as separate
questions.** This lane rides an existing job as a tail step
(`cmd: "bin/ci-unit-test.sh && bin/lincheck-test.sh"` in `.github/workflows/maven.yml`'s Unit
Tests row) because at under a minute it does not earn a separate check name - but it stays a
second `./mvnw` invocation because Lincheck installs a JVM-wide instrumentation agent and needs
serial execution. Conflating the two questions either wastes a check slot on a fast lane or
corrupts its results by sharing a JVM it cannot share.

**A hand-pinned constant nothing has re-derived since its last edit may have been wrong since
then.** The constant must stay hand-typed (the header's dropped-tag reasoning stands), but
spot-checking it costs one grep - `grep -rl '@Tag("lincheck")' | wc -l` against the pinned value.
Deriving the guard at run time is the mistake; occasionally re-deriving it by hand is the hygiene.

## Why This Matters

This is the same failure shape as
`docs/solutions/workflow-issues/ci-retries-hid-flakes-from-the-ledger-2026-08-07.md`: a mechanism
built to surface signal was in practice surfacing nothing, while green builds were read as the
proof it should have provided. There it was retries deleting failure evidence after the fact; here
a lane never invoked at all - deletion-after and omission-before are the same blind spot from
opposite directions. Both compound silently until an unrelated event (a review, a wiring pass)
forces the mechanism to actually run.

The sharper point is second-order: the guard's *design* was correctly reasoned against its named
failure mode (a silently dropped tag), and that reasoning did nothing, because design reasoning
cannot substitute for execution. A guard is only as live as its most recent evaluation - and this
one's first evaluation came a week and two roster changes after it was written.

## When to Apply

- Before merging any new test lane, check script, or guard: ask where it runs on every PR, not
  whether it exists and passes by hand.
- When a review flags "this control would catch X but nothing invokes it": treat it as blocking
  and wire the lane in the same PR.
- When wiring a lane in for the first time: expect its first real run to fail on stale internal
  state (counts, thresholds, timing comments). That failure is the wiring working, not a defect in
  the wiring.
- When choosing the wiring shape for a fast lane: default to a tail step on an existing job; give
  it a separate process only when it genuinely cannot share one; give it a separate job only when
  it is too slow or too noisy to ride along.
- Periodically re-derive any hand-pinned roster or count guard by hand as a spot check, without
  converting the guard itself to a derived value.

## Examples

**Before** (astubbs#347 until astubbs/parallel-consumer#392): the guard existed and was
well-reasoned, but nothing ran it - so two roster additions passed it silently.

```bash
# bin/lincheck-test.sh - runnable by hand only, invoked by no workflow
EXPECTED_LINCHECK_CLASSES=5   # correct when astubbs#347 pinned it; stale from the
                              # sixth harness onward, and again at the seventh
```

```yaml
# .github/workflows/maven.yml - Unit Tests row, before wiring
- suite: unit
  name: "Unit Tests"
  cmd: "bin/ci-unit-test.sh"
```

**After** (astubbs/parallel-consumer#392, commit "ci(tests): the Lincheck lane rides the Unit
Tests row - and its roster guard was already miscounted"):

```yaml
# .github/workflows/maven.yml
- suite: unit
  name: "Unit Tests"
  # The Lincheck lane rides this row as a ~40s tail step rather than owning a matrix row: it
  # must be its own maven invocation (JVM-wide instrumentation agent, serial execution - the
  # script header owns the why), but it is far too cheap to earn a separate check name.
  cmd: "bin/ci-unit-test.sh && bin/lincheck-test.sh"
```

```bash
# bin/lincheck-test.sh
EXPECTED_LINCHECK_CLASSES=7   # corrected the moment the lane finally ran
```

`docs/testing.md`'s Lincheck section now states the wiring: "Since astubbs#392 the lane gates: it
runs as a tail step of the `Unit Tests` row in `maven.yml`."

## Related

- `docs/solutions/workflow-issues/a-check-that-reports-success-without-having-run.md` - the direct
  ancestor: this same script's *original* guard defect (counting stale reports from a previous
  run), fixed in astubbs#347 by cleaning first and asserting the exact roster count. This doc is
  the sequel: that fix was necessary but not sufficient, because nothing ever ran the assertion.
- `docs/solutions/workflow-issues/ci-retries-hid-flakes-from-the-ledger-2026-08-07.md` - the same
  silent-signal-loss family from the opposite direction (evidence deleted after the fact rather
  than never produced).
- `bin/lincheck-test.sh` - the guard, the five-flags header, and the "WHY THE NUMBER IS PINNED BY
  HAND" reasoning this doc builds on.
- `docs/testing.md`, "Lincheck lane" section - the lane's current wiring and operating detail.
- `docs/plans/2026-08-25-001-test-lincheck-poc-plan.md` - the dated adoption plan that recorded
  the original non-gating shape; astubbs/parallel-consumer#392 revisited that decision (the plan
  itself stands as a record of its time).
- PR astubbs/parallel-consumer#392 - the review finding that triggered the wiring, and the commit
  that wired the lane and corrected the count.

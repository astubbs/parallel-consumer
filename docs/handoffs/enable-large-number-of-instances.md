---
artifact_contract: "ce-handoff/v1"
created_at: "2026-09-01T06:10:00Z"
title: "Re-enable largeNumberOfInstances properly, starting by measuring its failure rate"
summary: "A test @Disabled on master with no reason was re-enabled inside an unrelated PR, failed on first exposure, and degraded a neighbouring throughput measurement; this branch re-enables it deliberately so the work can be done on its own terms."
keywords: ["largeNumberOfInstances", "MultiInstanceRebalanceTest", "performance-lane", "dark-test", "failure-rate", "confluentinc-857"]
cwd: "/Users/astubbs/github/parallel-consumer/.claude/worktrees/enable-lnoi"
resume_focus: "Measure largeNumberOfInstances' failure rate with bin/exp-measure-large-instances-failure-rate.sh BEFORE diagnosing anything, then decide whether it can live in a gating lane."
repository: "astubbs/parallel-consumer"
repo_root_sha: "713c7468d5ecda55a2190d38e698cda017e91259"
branch: "handoff/enable-large-number-of-instances"
head: "cut from bugs/857-paused-consumption-multi-consumers-bug at eaaa875c4"
worktree_path: "/Users/astubbs/github/parallel-consumer/.claude/worktrees/enable-lnoi"
---

# Re-enable `largeNumberOfInstances`, properly this time

## What this branch is

One change: the `@Disabled` on
`MultiInstanceRebalanceTest.largeNumberOfInstances` is removed, so the test runs again.

**Expect this branch to be RED from its first push.** That is the starting state, not a fault. The
test failed the last time it ran, and making CI green by re-disabling it is the move that produced
this situation in the first place - it was switched off years ago and nobody recorded why.

## The one instruction the user attached to this handoff

**Measure the failure rate before diagnosing anything.** `bin/exp-measure-large-instances-failure-rate.sh`
exists for exactly this question, defaults to 10 iterations, and has never been run. Read its header
first - it carries the method and a warning about an earlier attempt that measured overload rather
than the effect. `bin/exp-sweep-large-instances-scale.sh` asks the adjacent question of whether the
rate moves with scale, which discriminates a coordinator problem from a PC one.

Everything below is status and evidence, not instructions.

## Why the rate is the first question, and why CI cannot answer it

The test was **`@Disabled` on master carrying no reason at all**. Its javadoc says only that it
"takes some time, but seems required in order to expose some race conditions without synthetically
creating them", citing confluentinc#188 and confluentinc#189. So it was switched off for **cost**,
and has never been shown to fail.

It has therefore **never run in CI** - not in the integration lane, not in chaos, nowhere. There is
no historical failure rate to look up. A sub-agent spent roughly 173k tokens mining CI history for
one before that was understood; the search could not have succeeded, and repeating it is the most
likely wrong path here.

The upstream context that makes the rate interesting: a 2026-01 report on confluentinc#857 said
"every other run of this test is failing" with "No progress beyond N records after M rounds". If
that is still true, ~50% is the number to expect - and a test at that rate cannot live in a gating
lane whatever else is true.

## What happened when it was last enabled

It was re-enabled inside astubbs/parallel-consumer#29 (a deadlock-fix PR) by removing `@Disabled` and
adding `@Tag("performance")`. Two things followed.

**It failed on first exposure.** A progress timeout at 111.2 s: 500,000 records,
`PERIODIC_CONSUMER_ASYNCHRONOUS`, `UNORDERED`, and critically
`PC reported exception states: []` with `{}` - **no crash and no recorded failure cause**. Whether
that is a real defect or an ordinary slow/flaky run is unknown.

**It made a neighbour look broken, and that part is settled.** The performance lane runs classes
sequentially in one reused JVM (`reuseForks=true`; `-Pci` sets `parallel-tests=false`). Order in the
failing run was `VeryLargeMessageVolumeTest`, `LargeVolumeInMemoryTests`,
**`MultiInstanceRebalanceTest`**, **`MultiInstanceHighVolumeTest`**, `LoadTest` - so 21 PC instances
churned for 111 s immediately before a throughput measurement in the same JVM. That measurement fell
from ~73,700 to 39,684 records/second across three CI runs and read as a 45% product regression on
astubbs/parallel-consumer#29.

**It is not a regression.** Control arm, same tree, same test, lane of one:
`processed=3000000 expected=3000000 recordsPerSecond=73722 outcome=PASSED`, against a master-based
baseline of 71,387. Do not go looking for a throughput bug; there isn't one.

## The finding that decides where this test can live

The class javadoc describes the capacity profiles as going in *"the performance lane, which never
gates a merge"*, and says their **pass rate over many runs is the measurement; a single run's outcome
is not a verdict on PC**.

**That belief is false.** `Performance Tests` is a required status check on master's ruleset
(`gh api repos/astubbs/parallel-consumer/rules/branches/master`). So a test whose own documentation
says a single run means nothing is sitting in a lane where a single run blocks merges.

The user has ruled out one resolution already: **do not make the lane non-gating.** Their reasoning,
recorded here because it constrains the design - GitHub runners perform reliably enough that a
baseline shift is real signal, so the answer is to improve the tests rather than stop watching. That
leaves options like isolating the fork, moving capacity profiles to a separate non-required lane, or
asserting a rate rather than an outcome - but the choice is open and is the interesting part of this
work.

## Current state

- **This branch**: `@Disabled` removed. Nothing else changed. Not pushed as a PR yet; whether it
  needs one is the operator's call, and a draft is reasonable given it starts red.
- **astubbs/parallel-consumer#29**: the test is `@Disabled` again there, now with a full reason on the
  annotation instead of master's bare one. That PR's own failure is resolved - see `eaaa875c4`.
- **Not done**: any measurement at all. No rate, no scale sweep, no local reproduction.

## Load-bearing references

- `parallel-consumer-core/src/test-integration/java/bz/stub/parallelconsumer/integrationTests/MultiInstanceRebalanceTest.java`
  - grep `largeNumberOfInstances` for the test, and the class javadoc's `Capacity profiles` bullet for
    the false non-gating claim and the pass-rate framing.
- `bin/exp-measure-large-instances-failure-rate.sh` and `bin/exp-sweep-large-instances-scale.sh` -
  the instruments, and their headers carry the method.
- `docs/inflight/test-largenumberofinstances-residual-failures-unmeasured.md` - the note that owns the
  open question.
- `bin/AGENTS.md`, "A script that answered its question is finished" - the lifecycle these
  experiment runners are under, if the question gets answered.
- `docs/solutions/workflow-issues/gh-run-view-log-truncation.md` - **read before reading any CI log.**
  `gh run view --log` silently truncated a 9,968-line job to 7,138 lines during this investigation
  and produced a confident wrong conclusion. Use the run-archive zip route it prescribes.

## Wrong paths already taken, so they are not retaken

- **Mining CI history for this test's failure rate.** It has none; it never ran. ~173k tokens.
- **Hunting a per-record cost in astubbs/parallel-consumer#29's main-code diff** to explain the 45%
  throughput drop. There is no product regression; the control arm settled it.
- **Reading a CI log through `gh run view --log`** and concluding `MultiInstanceHighVolumeTest` never
  ran. It ran; the log was truncated before the summary.

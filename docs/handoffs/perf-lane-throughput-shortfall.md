---
artifact_contract: "ce-handoff/v1"
created_at: "2026-09-01T07:05:00Z"
title: "MultiInstanceHighVolumeTest is 39% down on CI and nothing explains it yet"
summary: "Lane composition and runner speed are both ruled out by measurement, leaving the tree as the only remaining difference - and the like-for-like CI comparison that would settle it has never been run."
keywords: ["MultiInstanceHighVolumeTest", "throughput", "performance-lane", "GATING_CEILING", "runner-variance", "confluentinc-857"]
cwd: "/Users/astubbs/github/parallel-consumer/.claude/worktrees/enable-lnoi"
resume_focus: "Run MultiInstanceHighVolumeTest alone on CI against BOTH astubbs/parallel-consumer#29's tree and master, and compare. Until that pair exists, nobody knows whether there is a product regression."
repository: "astubbs/parallel-consumer"
repo_root_sha: "713c7468d5ecda55a2190d38e698cda017e91259"
branch: "handoff/enable-large-number-of-instances"
head: "d2673226b"
worktree_path: "/Users/astubbs/github/parallel-consumer/.claude/worktrees/enable-lnoi"
---

# The throughput shortfall nobody has explained

**Companion to `enable-large-number-of-instances.md` on this branch.** That one is about a dark test
being switched on. This one is about a *different* test in the same lane that is failing for reasons
still unknown, and it is the reason
astubbs/parallel-consumer#29's merge is **paused by operator decision**.

## The one instruction the operator attached

**Nobody merges astubbs/parallel-consumer#29 until this is understood.** That is a deliberate hold,
not an oversight, and it stands until this question is answered rather than until CI happens to go
green.

## What is failing

`MultiInstanceHighVolumeTest.multiInstance` on
astubbs/parallel-consumer#29, three consecutive CI runs. Most recent, at `b42ab61d7`:

```
processed=2638050 expected=3000000 elapsedMs=60572 recordsPerSecond=43552
commitMode=PERIODIC_CONSUMER_SYNC order=KEY maxPoll=500  FAILED
```

The test asserts 3,000,000 records within a 60-second `GATING_CEILING`. `elapsedMs=60572` is that
ceiling being struck, so the reported "rate" is records-reached over sixty seconds - **not a
throughput measurement**. Treat every failing number here as arithmetic on a deadline.

## Two explanations were tested and BOTH are ruled out

**Lane composition - ruled out by measurement.** astubbs/parallel-consumer#29 had added
`MultiInstanceRebalanceTest`'s capacity profiles to this lane, where they shared one reused JVM with
the throughput test (`reuseForks` defaults to true; `-Pci` sets `parallel-tests=false`, so this is
sequential carryover, not concurrency). That was a real effect: with a 21-instance profile running
111 s immediately before, the number was 39,684; disabling it moved it to 44,992.

But those profiles are now `@Disabled` and the class costs **0.020 s, 3 tests, all skipped** - and the
test still failed at 43,552. Composition is not the cause.

**Runner speed - ruled out by the neighbours.** Comparing the failing run against the passing
baseline run, class by class:

| | baseline run | failing run | delta |
|---|---|---|---|
| VeryLargeMessageVolumeTest | 51.25 s | 53.86 s | +5% |
| LargeVolumeInMemoryTests | 37.99 s | 39.45 s | +4% |
| **MultiInstanceHighVolumeTest** | **71,387 rec/s** | **43,552 rec/s** | **-39%** |

A uniformly slower machine slows everything proportionally. The neighbours are within 5%. Only the
throughput test is 39% down.

## What that leaves, and why it is not yet a conclusion

The remaining difference between those two runs is **the tree**:
astubbs/parallel-consumer#29's versus the baseline branch's. That is suggestive and it is **not
established**, for two reasons a careful reader should hold onto:

- **The instrument's own spread is 1.54x.** The same test, same code, same lane has been observed at
  27,298 ms / 109,898 rec/s, 36,361 ms / 82,505 rec/s and 42,024 ms / 71,387 rec/s across three CI
  runs. A shortfall concentrated in one test on one run is not impossible from variance alone.
- **It does not reproduce locally.** On a development machine astubbs/parallel-consumer#29's tree
  gives 73,722 rec/s alone and 72,498 in the full lane, both completing all 3,000,000. Local hardware
  has headroom a hosted runner does not, so local measurement cannot settle this either way.

An earlier claim in this investigation - "confident there is no product regression" - rested on that
local pair and was **withdrawn** once the neighbour timings showed the machines were comparable.
Do not treat "no regression" as established; treat it as unproven in both directions.

## The measurement that would settle it, and has never been run

**`MultiInstanceHighVolumeTest` alone, on CI, on both trees.** One CI run per side, the same lane
selection, nothing else in it:

- astubbs/parallel-consumer#29's tree (or this branch, which is cut from it)
- `master`

That is the like-for-like pair this investigation never had. Every comparison made so far mixed
machines (local versus CI) or mixed lane compositions. Given the 1.54x spread, **more than one run per
side is worth having** before believing a difference; the spread is the reason a single pair can
mislead.

If the two trees come back within noise of each other, there is no regression and the failures are
the deadline meeting a slow draw - at which point the assertion is the whole problem. If
astubbs/parallel-consumer#29's tree is consistently down, there is a real effect that only appears
under CI's resource constraints, and its main-code diff is where to look.

## The assertion is unsound regardless of which way that lands

Filed as `docs/inflight/test-perf-lane-asserts-a-deadline-on-a-varying-machine.md`, which carries the
spread data and three costed-out routes. In short: a 60-second wall clock on a machine that varies
1.5x manufactures its own failures, and a bigger ceiling moves the line without removing its
load-bearing role.
<!-- file-refs: N/A - that note lives on astubbs/parallel-consumer#29's branch, which this one was cut
     before; naming it is how the next agent finds it -->

This repo already argues the fix in two places - the general rule in
`docs/solutions/best-practices/a-timing-bound-used-as-a-correctness-gate-manufactures-its-own-evidence.md`,
and, more pointedly, in the javadoc of the class next door: gate on **progress**, "never 'all N
records within T', which fails a slow run and a stalled run identically". `MultiInstanceHighVolumeTest`
does exactly what that sentence forbids.

The unsolved part is that reporting a number does not gate, and the threshold cannot be picked from
three samples. `docs/inflight/perf-throughput-regression-gate.md` already records that collection
landed and gating deliberately did not, for exactly this reason.

## Wrong paths already taken

- **Fork isolation** (`-DreuseForks=false` in `bin/performance-test.sh`) was added and then reverted.
  It buys headroom under the wall rather than removing the wall's load-bearing role, and composition
  turned out not to be the cause anyway. Worth knowing: the user property is `reuseForks`, **not**
  `failsafe.reuseForks` - the qualified guess is accepted and silently does nothing, which is the
  third instance of that trap in this investigation.
- **Reading CI logs with `gh run view --log`.** It silently returned 7,138 of 9,968 lines and produced
  a confident wrong conclusion. Use the run-archive zip route in
  `docs/solutions/workflow-issues/gh-run-view-log-truncation.md`. Note the archive 404s until the
  whole run completes, which is why the temptation recurs.
- **Believing a single-run CI comparison.** 109,898 and 71,387 are the same code.

## State of the two branches

- **astubbs/parallel-consumer#29**: capacity profiles `@Disabled` with reasons; the fork-isolation
  commit reverted; `docs/inflight/test-perf-lane-asserts-a-deadline-on-a-varying-machine.md` and the
  close-out in `test-857-branch-red-lanes-cause-unestablished.md` both landed. `Performance Tests` is
  still RED and is a **required** check. Merge paused.
  <!-- file-refs: N/A - both notes are on that PR's branch, deliberately not on this one -->
- **This branch**: `largeNumberOfInstances` re-enabled, expected red, plus the two handoffs. Nothing
  measured yet on either question.

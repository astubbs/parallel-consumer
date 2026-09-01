---
title: "Reverting half a fix is not a control: to ask whether a commit removed a violation, check out its parent and run"
date: 2026-09-01
category: best-practices
module: parallel-consumer-core
problem_type: best_practice
component: testing_framework
applies_when:
  - "A concurrency harness that used to find a violation has stopped finding one"
  - "About to hand-reconstruct a defect from a diff to test whether a fix is what removed it"
  - "Writing a control arm by editing today's code rather than by checking out an old tree"
  - "Concluding that a suspected fix is NOT the cause because reintroducing the defect changed nothing"
  - "An inverted test - one asserting a bug EXISTS - has gone quiet"
related_components:
  - development_workflow
  - documentation
tags:
  - concurrency
  - lincheck
  - control-arm
  - false-negative
  - bisect
  - investigation-method
---

# Reverting half a fix is not a control

## Context

`ShardManagerLincheckTest`'s stress arm is inverted: it asserts that Lincheck *finds* a violation, and
fails when nothing is found. It went quiet, and the obvious suspect was astubbs#336, which had
rewritten `ProcessingShard#addWorkContainer` and edited the lane's note in the same commit.

Three separate controls were written to test that suspicion, over two sessions. **All three came back
negative, and all three were wrong.** The conclusion drawn from them - "astubbs#336 is refuted, the
harness has lost its seam" - was recorded in a handoff note and pushed to a PR before a fourth
experiment overturned it.

What settled it took one command: check out astubbs#336's parent and run the unchanged harness.

## Guidance

### 1. A hand-rebuilt before-state is a new experiment, not the old one

Each control reverted one *part* of the fix onto today's tree:

| Control | What it restored | Result |
|---|---|---|
| First | moved the population accounting, kept the put atomic | no violation - and it never reintroduced a check-then-act at all |
| Second | `workMap.get` / branch / `workMap.put`, the map's check-then-act | no violation |
| Third | the counter's bare `incrementAndGet`, decided from the pre-put read | no violation |

The defect was neither half. It was **deciding the accounting from the pre-put read**, which
astubbs#336 removed wholesale by admitting to the population first and reading the outcome from the
map rather than from the earlier read. Every control kept that restructuring, because it is the part
that does not *look* like the defect - it looks like tidying.

**A control assembled by hand tests the shape you believed the defect had.** When it comes back
negative you have learned that your reconstruction is not the defect; you have learned nothing about
the commit.

### 2. Check out the parent - the whole tree, not the file

`git show <commit>^:<path>` shows you the method, and reading it is worth doing. It is not the
experiment. The commit's parent is a tree that once demonstrably behaved the way you are asking
about, and checking it out costs one `git worktree add --detach`:

```bash
git worktree add --detach /path/to/throwaway <commit>^
cd /path/to/throwaway && <run the harness>
```

Here that produced the counterexample immediately - `revokeSweep(0)` in the prefix, then
`addWork(0)` against `addWork(0)` in parallel - identical to the one the lane's note had recorded
months earlier, which is what confirmed the harness and the toolchain were both fine.

### 3. Then bisect over the commits that touched the class, and name the sole one

Fires at astubbs#345, at confluentinc#905's hot-shard metric and at astubbs#373's claim
compare-and-set; misses at astubbs#336. Attribution is only exact because astubbs#336 is the **sole**
commit touching the module's main sources between the last hit and the first miss - which is a
`git log <good>..<bad> -- <module>/src/main` away and worth stating in the write-up, because
"the bisect landed on X" is a weaker claim than "X is the only candidate in the interval".

### 4. Pin the constants before blaming the code, and prove the instrument still speaks

Two cheap checks come first, and both were skipped for a day:

- **Has the harness itself moved?** `git log -- <the test file>` answered in one line: unchanged since
  the fix it was written for.
- **Has the tool moved?** The Lincheck version was pinned at the same release in both trees.
- **Does the lane's own control arm still fire?** `LincheckToolchainProbeTest` exists precisely to say
  whether Lincheck is instrumenting anything at all, and it fired - which is what made "the product
  code changed" the only surviving explanation.
  See [`a-stress-probe-is-an-instrument-you-built-not-a-test.md`](a-stress-probe-is-an-instrument-you-built-not-a-test.md):
  read the positive control first, or nothing else in the run is interpretable.

### 5. A negative control deserves the scepticism a positive one gets

`docs/investigating.md` owns the forward direction - **a fix that works is not evidence of the
cause.** This is its mirror, and it is the one that reads as rigorous while being weaker:
**a reintroduced defect that changes nothing is not evidence the cause is absent.** A control arm
proves something only when it is known to be capable of producing the effect, and a hand-written one
never has that provenance. Say in the write-up which kind of control you ran, and if it was
hand-built, say that its negative is soft.

## Why This Matters

The false conclusion was not idle. It was written into a handoff note as a section headed
*"the hypothesis that looked right and is REFUTED - do not re-run it"*, which is precisely the
instruction that would have stopped the next reader running the experiment that settles it. A wrong
negative recorded confidently costs more than no record: it converts the cheapest remaining
experiment into the one nobody will try.

It also cost the arm its correct disposition. The lane's inversion contract says to flip a harness to
assert-no-violation once its fix lands; astubbs#336 *was* that fix, so the flip had been available
and green the whole time - 0 violations in 250,000 invocations at the bound the counterexample was
originally found at, and 0 in 2,500,000 at ten times it.

## When to Apply

- **When an inverted test goes quiet** - before writing any control, check out the parent of every
  commit that touched the class under it.
- **Before recording a "refuted" verdict** from a control you wrote by hand.
- **When a control comes back negative and the hypothesis still feels right** - that is the signal
  the control is wrong, not the hypothesis.
- **Before repricing a bound or adding a retry** because a stress arm "stopped finding" something.

## Related

- [`a-stress-probe-is-an-instrument-you-built-not-a-test.md`](a-stress-probe-is-an-instrument-you-built-not-a-test.md)
  - the positive-control discipline this leans on, and why a zero needs its denominator.
- [`a-stress-probes-calibration-is-a-claim-about-one-machine.md`](a-stress-probes-calibration-is-a-claim-about-one-machine.md)
  - why "reprice the bound" is the wrong first move when a stress arm stops hitting.
- [`../../investigating.md`](../../investigating.md) - **owns the general method**; a fix that works is
  not evidence of the cause. This doc is the negative-control mirror of that rule.
- [`../../inflight/test-lincheck-lane-open-items.md`](../../inflight/test-lincheck-lane-open-items.md)
  - the lane's owning note, including the inversion contract's running record and the artefact-or-defect
  question this arm's counterexample is still parked under.

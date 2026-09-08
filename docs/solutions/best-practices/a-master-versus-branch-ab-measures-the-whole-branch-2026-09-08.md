---
title: "A master-versus-branch A/B measures the whole branch - diff the arms in the subsystem you are measuring, not just the change you mean to test"
date: 2026-09-08
category: best-practices
module: parallel-consumer-core
problem_type: best_practice
component: testing_framework
severity: high
applies_when:
  - "Comparing plain master against a feature branch to decide whether that branch's fix closes a symptom"
  - "A branch arm comes out WORSE than the master arm and the write-up reads it as 'the fix does not close this'"
  - "Attributing a throughput, latency or timing-bound result to one named commit on a branch that carries several"
  - "Reading a replay grid whose arms were built months ago, from a branch that has since been reviewed and changed"
related_components:
  - ConsumerManager
  - development_workflow
tags:
  - control-arm
  - confluentinc-857
  - benchmarking
  - chaos-testing
---

# The mechanism

A control arm is only a control arm if the two trees differ in ONE place. `master` versus
`<feature-branch>` looks like that and is not: the branch carries every commit its author has pushed,
including the ones written to fix review findings on itself. When the measurement is a *rate* -
throughput, recovery time, a timing bound - any of those other commits that touches intake, commits,
back-pressure or locking is a second term, and it does not announce itself.

The failure has a signature worth learning, because it reads as a finding rather than a fault: **the
branch arm comes out worse than master, and the write-up concludes the fix does not close the
symptom.** That conclusion is available whether the branch's own fix is inert or the branch carries a
regression, and the two are indistinguishable from the arms alone.

# The incident

The confluentinc#857 replay grid (`docs/inflight/test-857-revoke-under-work-sightings.md`,
"The first replays") compared plain master `438b09d9b` against astubbs#29's branch `b8a335b05` on two
recorded chaos seeds, to decide whether that branch's revoke-path lock change closed an eager-assignor
lag-stagnation stall. The branch arm reproduced on every cell and reproduced *harder* than master, and
the grid recorded exactly that: the fix does not close this stall. A "fourth open item" was opened in
`docs/inflight/bug-857-family.md` on the strength of it.

The two arms also differed in `ConsumerManager.poll`. Master refreshed the pause cache at poll ENTRY
and again at exit; the branch had dropped the entry call while fixing a `ConcurrentModificationException`,
leaving exit-only. That is the shape
[`paused-poll-wakeup-lost-to-stale-pause-cache-2026-09-01.md`](../performance-issues/paused-poll-wakeup-lost-to-stale-pause-cache-2026-09-01.md)
owns and measures at a 4-10x collapse under back-pressure - found, and fixed, two weeks after the grid
ran. Nobody checked the arms' diff in the subsystem the grid was measuring, because the arms had names
("DEFECT" and "FIXED") that described the one term the experiment was about.

# What the honest follow-up found, including the refuted half

Reverting the pause-cache fix on today's tree and replaying the same seed did **not** reproduce the
symptom, and did not even slow the scenario - so the second term is not the *explanation*, only proof
that the grid was never a one-term experiment. Recording that refutation matters as much as the
finding: a confound that is real does not have to be the cause, and stopping at "found the confound,
therefore that was it" would have replaced one unsupported attribution with another.

What did settle it was a control arm on the machine rather than the code: with the tree and the seed
held constant, the bound was crossed at `-XX:ActiveProcessorCount=8` and not at the box's own twelve,
two runs each. `docs/inflight/bug-857-family.md`'s `## A fourth open item` section owns those numbers.

# The reusable lessons

- **Before reading a master-versus-branch result, diff the arms in the subsystem the measurement
  touches** - `git diff <master-arm> <branch-arm> -- <that package>` - and say in the write-up what
  came back. It costs one command and it is not recoverable later: by the time the branch merges, the
  second term has usually been fixed and the grid's arms no longer exist to be diffed.
- **A branch arm that is WORSE than master is a prompt to look for a second term, not a finding about
  the fix.** A fix that does not work predicts parity with master; a branch regression predicts the
  branch being worse. Those are different shapes, and the grid had the second one.
- **When the symptom is a timing bound, run the resource control arm first.** It is cheaper than
  building two trees, it needs no branch archaeology, and if the bound crosses on a processor knob
  alone then no amount of code comparison was ever going to be informative.
- **Name the arms for the trees they are, not for the verdict you expect.** "DEFECT" and "FIXED" are
  what stopped anyone asking what else was in them.

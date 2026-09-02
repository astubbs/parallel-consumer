---
title: "An A/B whose arms run in time order is confounded with time - interleave, and read the confound's own signature"
date: 2026-09-01
category: best-practices
module: parallel-consumer-core
problem_type: best_practice
component: testing_framework
severity: high
applies_when:
  - "Running all of arm A then all of arm B to compare two trees, two configs, or two commits"
  - "A benchmark arm declines or improves monotonically within itself"
  - "Attributing a throughput difference to a code change on a machine that also gets hot, caches, or accumulates state"
  - "A local measurement is about to corroborate a CI result and the two must not share a confound"
related_components:
  - MultiInstanceHighVolumeTest
tags:
  - benchmarking
  - control-arm
  - confounds
  - interleaving
  - measurement
related:
  - "ablate-your-own-change-not-only-the-baseline.md - which arms to run at all; this doc is about the ORDER you run them in"
  - "a-stress-probes-calibration-is-a-claim-about-one-machine.md - the sibling rule about which machine a rate describes"
  - "../performance-issues/slf4j-defers-formatting-not-argument-evaluation-2026-09-01.md - the defect this method was built to measure"
---

# An A/B whose arms run in time order is confounded with time

## The trap

The natural way to run a two-arm comparison is all of A, then all of B. It is easy to script, and the
tree only has to be switched once. It also makes **arm perfectly correlated with time**, so anything
that changes over the course of the session - thermal throttling, a filling disk, page cache, Docker
accumulating containers, another process starting - is indistinguishable from the effect you are
measuring.

This matters most when the result is the one you wanted. A clean separation between arms is exactly
as consistent with "the fix works" as with "the machine got slower", and nothing in the numbers
themselves tells the two apart.

## The signature that should stop you

**Look at the trend WITHIN each arm, not just the gap between them.** A real code effect has no
reason to drift inside an arm; a machine effect does. In the run that produced this note, arm B's
three values declined monotonically:

```
A-fixed   109,894   147,579   131,590      (no trend)
B-eager    80,398    69,772    66,815      (monotonic decline)
```

That decline is the tell. It is not proof of a confound - three samples can decline by chance - but
it is the point at which the result stops being reportable as it stands.

## The cheap fix

**Interleave, and put the arm you expect to lose FIRST in each cycle.** Under a drift explanation the
arm running second is the slower one, so if your expected-loser still loses while running first, the
drift reading is dead:

```
cycle 1:   B-eager  75,441   ->   A-fixed  122,374
cycle 2:   B-eager  73,733   ->   A-fixed  101,228
```

Arm A ran second in both cycles - the disadvantaged position - and won by a wide margin. Combined
with the first experiment the arms did not overlap at all, and the confound had been *tested* rather
than argued away.

Two runs per arm of an interleaved cycle is usually enough to kill an order confound, because you are
no longer estimating the effect size from it - you are asking one yes/no question about sign.

## Why not just argue it away

It is tempting to reason that thermal drift "would be too small to explain a 30% gap". That argument
requires knowing the machine's drift magnitude, which nobody measured, and it is the same shape of
reasoning that this repo has repeatedly found to be wrong - a mechanism story standing in for a
measurement. Interleaving costs a few minutes and removes the need for the argument entirely.

## What an interleaved local result can and cannot buy

In the case that produced this note, every run **passed** in both arms - the local box, even with the
JVM pinned to two processors, had real cores serving the OS, the broker and Docker, so the deadline
under test was never in danger. The local experiment measured a *throughput difference* and could not
reproduce the *failure*.

So state which claim the corroboration supports. "The mechanism is real and its magnitude reproduces
on other hardware" was established; "this fixed the failing gate" rested on the CI run, and saying so
kept a strong result from being quoted as a stronger one.

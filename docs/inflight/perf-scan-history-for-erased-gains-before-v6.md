# Before v6 ships: scan the history since 0.5.3.3 for gains we made and then lost

<!-- inflight-type: task -->
<!-- inflight-impact: release-gate -->
<!-- inflight-state: deferred - release-gated on v6, and blocked until the measurement instrument has a known noise floor -->

**A release gate, not a nice-to-have.** Before 0.6.0.0 ships, walk the history from upstream's last
release (0.5.3.3, 2025-08-28) to the release commit and look at performance as a *trajectory* rather
than as two endpoints.

## The question a before/after comparison cannot answer

"Are we faster than 0.5.3.3?" is answerable by measuring two commits, and the answer is already
believed to be yes. That is not the interesting question.

**The interesting one is whether we were faster still at some point in between.** A change that wins
20% followed by one that loses 20% nets to zero against the old baseline, so an endpoint comparison
reports "no change" and everybody moves on. The gain is real, it was paid for, and it has been
silently handed back. Nothing in this repo would notice: the throughput gate compares against a
committed baseline that only ever moves forward when somebody re-baselines, and a lost gain looks
exactly like never having had it.

So the scan is looking for **positive outliers followed by a return to trend**, which is a shape only
visible across a series.

## Why this is deferred rather than started

**The instrument's noise floor is unknown and may be larger than the effects being hunted.** Measured
2026-09-01: the same commit, measured twice on an idle machine, gave 24.893s and 33.881s for
`MultiInstanceHighVolumeTest#multiInstance` - a 36% swing on identical code. A trajectory drawn
through points that noisy shows peaks and troughs that are not there, and a scan built on it would
manufacture exactly the false "we lost a gain here" findings it exists to catch.

**Characterising the instrument comes first**: run one commit N times, report the spread, and only
then decide how many repeats per point a trajectory needs. If the floor stays that wide, the honest
outcome is that this test cannot support the scan and a different measurement must carry it.

## Method, once the instrument is known

- **Sample, then densify.** Hundreds of commits and minutes per measurement means a sparse pass first,
  then more points around anything that looks like a step - in either direction.
- **Full lane, per method.** The neighbour classes are the machine-speed control and `<testcase>`
  carries per-method times, which exclude container startup. A subject-only run has no control, and
  that mistake already destroyed one bisect on this project.
- **Look both ways.** A regression scan that only looks for slowdowns cannot see an erased gain,
  because the erasure IS the slowdown and it lands back at a number that reads as normal. Flag steps
  in both directions, then check whether each improvement is still present at the tip.
- **Report the peak, not just the endpoints.** The deliverable is "best observed, where, and whether
  we still have it", not a single before/after ratio.

## What it feeds

The release notes should be able to say what got faster and by how much, sourced from measurement
rather than from what people remember doing. That is also the cheapest way to find out that a
correctness fix quietly cost a fifth of throughput - which is the open question in
`perf-multiinstancehighvolume-20-percent-slower-since-2026-08-26.md`, and the reason this note exists
rather than being an idea somebody had once.

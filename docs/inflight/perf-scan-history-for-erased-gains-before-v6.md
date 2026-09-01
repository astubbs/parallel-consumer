# Before v6 ships: scan the history since 0.5.3.3 for gains we made and then lost

<!-- inflight-type: task -->
<!-- inflight-impact: release-gate -->
<!-- inflight-state: deferred - release-gated on v6; the noise floor that blocked it is now MEASURED (below), and the finding is that this instrument cannot carry the scan unaided -->

**A release gate, not a nice-to-have.** Before 0.6.0.0 ships, walk the history from upstream's last
release (0.5.3.3, 2025-08-28) to the release commit and look at performance as a *trajectory* rather
than as two endpoints.

## The question a before/after comparison cannot answer

"Are we faster than 0.5.3.3?" is answerable by measuring two commits, and the answer is already
believed to be yes. That is not the interesting question.

**The interesting one is whether we were faster still at some point in between.** A change that wins
20% followed by one that loses 20% nets to zero against the old baseline, so an endpoint comparison
reports "no change" and everybody moves on. The gain is real, it was paid for, and it has been
silently handed back. Nothing in this repo would notice: the throughput check compares against a
reference built from *recent* master runs, so its window slides forward and a gain that has already
been handed back is outside it. A lost gain looks exactly like never having had it.

So the scan is looking for **positive outliers followed by a return to trend**, which is a shape only
visible across a series.

## Why this is deferred rather than started

**The noise floor is now measured, and it is wider than most of the effects being hunted.** Eight
runs of one unchanged commit on an idle machine, 2026-09-01, robust spread across the lane:

| Series | Spread |
|---|---|
| `LoadTest` | 0.9% |
| `LargeVolumeInMemoryTests` | 0.9% |
| `VeryLargeMessageVolumeTest` | 6.1% |
| Subject, raw | 13.4% |
| Subject / CPU-bound control | 17.2% |

Two things follow, and the second was a surprise. The **raw subject time is the quietest signal
available** - normalising against a control adds about four points of noise on a single machine,
because the control carries its own spread and dividing by it compounds rather than cancels. And a
floor of 13.4% means a trajectory drawn through single measurements shows peaks and troughs that are
not there, which is exactly the false "we lost a gain here" finding this scan exists to catch.

**So the honest outcome is the one the earlier version of this note named as possible: this test
cannot support the scan unaided.** It is not blocked on characterising the instrument any more - that
is done. It is blocked on a design that survives a 13.4% floor: repeats per point with a median, a
quieter subject, or a different measurement entirely. Sizing that is the first task when this is
picked up, not the scan itself.

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

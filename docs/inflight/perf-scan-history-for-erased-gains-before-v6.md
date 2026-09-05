# Before v6 ships: scan the history since 0.5.3.3 for gains we made and then lost

<!-- inflight-type: task -->
<!-- inflight-impact: release-gate -->

**A release gate, not a nice-to-have.** Before 0.6.0.0 ships, walk the history from upstream's last
release (0.5.3.3, 2025-08-28) to the release commit and look at performance as a *trajectory* rather
than as two endpoints.

**No longer deferred.** It carried `inflight-state: deferred` while the instrument was believed
unable to support it. The block was real and it has been removed by measurement, not by decision:
the 13.4% floor is a property of the *subject test that was chosen*, not of the lane, and a subject
four times quieter has been sitting in the same run all along. The design below is sized and ready
to run; what it needs is machine-hours, not another investigation.

## The question a before/after comparison cannot answer

"Are we faster than 0.5.3.3?" is answerable by measuring two commits, and the answer is already
believed to be yes. That is not the interesting question.

**The interesting one is whether we were faster still at some point in between.** A change that wins
20% followed by one that loses 20% nets to zero against the old baseline, so an endpoint comparison
reports "no change" and everybody moves on. The gain is real, it was paid for, and it has been
silently handed back. Nothing in this repo would notice: the throughput check compares against a
reference built from *recent* master runs, so its window slides forward and a gain that has already
been handed back is outside it - and the artifacts holding those runs expire, which is
[`perf-a-queryable-history-instead-of-a-single-committed-baseline.md`](perf-a-queryable-history-instead-of-a-single-committed-baseline.md)'s
subject, not this note's.

So the scan is looking for **positive outliers followed by a return to trend**, which is a shape only
visible across a series.

## The noise is in the SUBJECT, not in the lane

Two measurements, and the second was taken to size this note.

**2026-09-01, eight runs of one unchanged commit on one idle box.** Robust spread per series, the
table `bin/lib/throughput-verdict.mjs` carries beside `NOISE_FLOOR` and that this note is not going
to restate a second time: the two sleeping controls came out inert, `VeryLargeMessageVolumeTest`
mid, the subject `MultiInstanceHighVolumeTest` widest, and every normalisation *added* noise rather
than cancelling it. That file owns those figures.

**2026-09-05, twelve consecutive `perf baseline (master)` runs**, per-method times taken from the
failsafe XML those runs upload, stratified by the runner's CPU model - which
[`bin/performance-test.sh`](../../bin/performance-test.sh) writes into the summary as a
`# machine cpu=` line for exactly this purpose. Relative IQR of the per-method time:

| Series | Across all runners | Within one CPU model | Idle box, 2026-09-01 |
|---|---|---|---|
| `LoadTest.asyncConsumeAndProcessAtVolume` | 3.5% | 0.5-0.6% | 0.9% |
| `VeryLargeMessageVolumeTest.shouldNotThrowBitSetTooLongException` | 29.0% | 2.8-5.7% | 6.1% |
| `MultiInstanceHighVolumeTest.multiInstance` | 22.0% | 2.5-13.0% | 13.4% |

Reproduce it, do not quote it: `gh run list -R astubbs/parallel-consumer --workflow
perf-baseline.yml`, then `gh run download <id> -p 'performance-*'` and read `<testcase time=>`
against the summary's `cpu=` line. These twelve runs are twelve *different* commits, so each figure
mixes noise with whatever the code actually did - every one of them is an **upper bound** on the
noise, which is the safe direction for sizing.

Three things follow, and the last one corrects a conclusion already written down.

- **Most of the apparent CI noise is which runner you drew.** `VeryLargeMessageVolumeTest` is
  bimodal by CPU model: on the older EPYC it lands near 33 s in every run, on all three newer models
  near 25 s. Stratified, its spread collapses to 2.8-5.7% and agrees with the idle-box 6.1%.
  **Pin the machine and the instrument is already good enough**; that costs a `runs-on` label, not a
  redesign.
- **The subject's noise is intrinsic and does not collapse.** It stays near 13% within one CPU model,
  matching the idle-box figure. The mechanism is in the test: it starts three PC instances against
  one group, so its wall time contains a group-join settling period quantised by broker-side timers.
  `VeryLargeMessageVolumeTest` is single-instance and has no such term.
- **"Normalising costs noise" was measured on one box and does not generalise to hosted runners.**
  On this twelve-run series the share `subject / VeryLarge` came out at 13.5%, quieter than either
  component raw. That is the cross-runner case `bin/lib/throughput-verdict.mjs` says outright its
  data could not test, and it is not a contradiction of it. It does not change the recommendation:
  pinning the machine beats normalising, because it needs no control, no model list and no second
  measurement's variance.

## How many repeats a point needs

**Distribution assumed: log-normal in the body, contaminated by a heavy right tail.** Defensible for
run times specifically, because a run is bounded below by the work it must do and unbounded above -
a GC pause, a noisy neighbour or CPU steal can only ever make it slower - and the perturbations are
multiplicative (a throttling factor, a contention factor), so the logs are roughly normal. The tail
is why the estimator is a **median of n** rather than a mean, and why the spreads above are robust
ones. Below about 20% relative spread the log-scale and relative-scale arithmetic agree to within a
point, so the sums are done in relative units.

The arithmetic also assumes the `n` repeats at a point are **independent**, which they are not if
they run back to back in one session - that is the sampling-order constraint below, and it is a
design requirement rather than a caveat. Under those assumptions the median of `n` has standard error `σ·√(π/2n)`, so the difference of two
independent points has `σ·√(π/n)` = `1.7725·σ/√n`, and detecting a step `δ` with power `1-β` at
significance `α` needs

```
n ≥ ( (z[α/2] + z[β]) · 1.7725 · σ / δ )²
```

**`α` must be corrected for how many comparisons the scan makes.** A trajectory over `P` sampled
points tests `P-1` adjacent pairs, and the failure this scan exists to avoid is exactly a peak that
is not there. At `P = 43` that is 42 comparisons, Bonferroni `α = 0.05/42`, and `z` rises from 1.96
to 3.24. Both columns are given because the per-pair one is the right test *after* the fact, when
you are checking one candidate step somebody has already pointed at.

Repeats per point, 80% power, rounded up:

| Series and setup (σ) | 20% step, per pair | 20% step, family-wise | 10% step, per pair | 10% step, family-wise |
|---|---|---|---|---|
| `VeryLargeMessageVolumeTest`, machine pinned (6.1%) | 3 | **5** | 10 | **20** |
| `MultiInstanceHighVolumeTest`, machine pinned (13.4%) | 12 | **24** | 45 | **95** |
| `MultiInstanceHighVolumeTest`, runner unpinned (22.0%) | 30 | 64 | 120 | 254 |
| `VeryLargeMessageVolumeTest`, runner unpinned (29.0%) | 52 | 111 | 208 | 441 |

Read the same arithmetic the other way, as the smallest step a family-wise honest scan may flag:

| Repeats per point | `VeryLarge` pinned | subject pinned |
|---|---|---|
| 1 | 44% | 97% |
| 3 | 26% | 56% |
| 5 | 20% | 43% |
| 8 | 16% | 34% |
| 20 | 10% | 22% |

**That last table is the whole argument.** A trajectory drawn through single measurements of the
current subject cannot honestly flag anything smaller than a doubling, which is why the earlier
version of this note concluded the instrument could not carry the scan. Change the subject and pin
the machine and five repeats resolve a 20% step - the size of the effect this scan is hunting.

## The subject changes to `VeryLargeMessageVolumeTest`

`shouldNotThrowBitSetTooLongException` is the series to draw. Three properties earn it:

- **It is CPU-bound and it is `KEY`-ordered**, over a million records with a distinct key each, so
  the shard map is at its widest. That is the shape the one throughput defect actually found in this
  window turned on - see
  [`slf4j-defers-formatting-not-argument-evaluation-2026-09-01.md`](../solutions/performance-issues/slf4j-defers-formatting-not-argument-evaluation-2026-09-01.md).
  A quiet instrument blind to the class of regression you are hunting would be no use, and this one
  is not blind to it.
- **Its workload has not changed across the whole range.** A million records,
  `PERIODIC_CONSUMER_ASYNCHRONOUS`, `ProcessingOrder.KEY`, `HIGH_MAX_POLL_RECORDS_CONFIG` of 10,000,
  identical at the 0.5.3.3 tag and at master; the handful of commits touching the file changed
  deadlines, reporting and the package name, never the volume or the options. Check before trusting
  it: `git log -p 0.5.3.3..origin/master -- '*VeryLargeMessageVolumeTest.java'`. **A series whose
  workload moves under it measures the test, not the code**, and nothing in the harness can detect
  that for you.
- **The measured window is most of the method.** The `ThroughputReport` line covers about 19 s of a
  23 s method time on a recent runner; the rest is producing the records. Per-method `<testcase
  time=>` is what to draw, because it excludes container startup - the argument
  `bin/lib/throughput-verdict.mjs` already makes for the verdict.

What it will miss, stated rather than discovered later: it is **single-instance**, so a regression
that only appears with several PC instances in one group - rebalance handling, cross-instance commit
contention, the confluentinc#857 family's territory - does not reach it. That is not an argument for
using the multi-instance test as the subject; it is an argument for running the whole lane at each
point, which costs nothing extra, and treating the subject series as the one with the resolution.

### `LoadTest` cannot carry the scan, and its 0.9% is not what it looks like

Tempting, at the quietest spread in the lane, and wrong twice over.

- **It is inert.** Every record sleeps a uniform 0-5 ms, so the wall time is the sleep. A change in
  per-record engine cost is a small fraction of a 2.5 ms mean and disappears into it. Its 0.5%
  within-model spread is not a precise measurement of PC, it is a precise measurement of
  `Thread.sleep`. The same reasoning `bin/lib/throughput-verdict.mjs` gives for refusing it as a
  machine-speed control applies to refusing it as a subject.
- **Its own workload changed inside the scan window.** `asyncConsumeAndProcessAtVolume` and its
  40,000 records arrived with astubbs#264; before that the class ran `asyncConsumeAndProcess` over
  `total = 4_000`. A `LoadTest` series across this range therefore has a discontinuity in it that is
  the test changing, not the code, and would read as exactly the step this scan is looking for.

**Keep it as a tripwire, not a series.** It is the flattest thing in the lane across every runner
model, so a `LoadTest` point that moves says the harness or the infrastructure moved, and every
other series that day is suspect.

## The sampling plan, and what it costs

Sparse pass first, then densify around anything that looks like a step in **either** direction. The
inputs, each with the command that yields it rather than a number to go stale:

| Input | Value when this was written | Command |
|---|---|---|
| Commits in range | 343 (337 first-parent) | `git rev-list --count 0.5.3.3..origin/master` |
| Sparse density | every 8th first-parent commit | - |
| Sparse points `P` | 43 | `git rev-list --first-parent 0.5.3.3..origin/master \| sed -n '1~8p' \| wc -l` |
| Repeats `n` | 5 | the sizing table above |
| Lane wall time per run | ~6.6 min median, 5.1-7.7 min | job durations from `gh run list --workflow perf-baseline.yml` |

At `P = 43`, `n = 5`, and allowing five candidate steps each bisected three levels deep at the same
`n`: **215 sparse runs plus 75 densifying runs, about 32 machine-hours** of the whole lane. Running
only the subject class saves roughly a third of each run - most of a run is the reactor build, not
the tests - and loses the tripwire, so it is not recommended.

For contrast, and this is the number that decides the scope: the same plan resolving a **10% step**
needs `n = 20` and about **128 machine-hours**, and keeping the current subject at a 20% step needs
`n = 24` and about **154**. Resolve 20%, and say in the write-up that a 10% step was out of budget.

Practical constraints the plan has to survive, none of them sized here:

- **Measure the points in RANDOMISED order, never in commit order.** A trajectory scan run oldest to
  newest makes commit index perfectly correlated with wall-clock time, so thermal drift, a filling
  disk, accumulating Docker state or a change in runner-fleet composition renders as a trend in the
  history - and a trend is precisely what this scan reports.
  [`an-a-b-whose-arms-run-in-time-order-is-confounded-with-time.md`](../solutions/best-practices/an-a-b-whose-arms-run-in-time-order-is-confounded-with-time.md)
  owns the rule and its signature; the version of it that binds here is that a point's `n` repeats
  are **spread across the session** rather than run back to back, so a point measured during a slow
  hour is not slow in all five of its samples. Re-measuring the tip commit at intervals through the
  pass is the cheap check: its series should be flat, and if it is not, the drift is real and the
  whole trajectory is suspect.
- **Pin `runs-on` to one runner image and check the `cpu=` line of every run**, discarding or
  re-running points that drew a different model. Unpinned, the repeats column quadruples.
- **Old commits must still build.** The range crosses the `io.confluent` to `bz.stub` package rename,
  so class names in the failsafe XML differ across it (`-Dit.test=VeryLargeMessageVolumeTest` selects
  by simple name and is unaffected). Whether a commit near the tag still resolves its dependencies
  and starts its broker container is unknown - **run one pilot point at the tag before committing to
  the pass**, because a build failure there costs the whole design.
- **Store the results as they are produced.** Perf artifacts expire, and a scan whose output lives
  only in run artifacts has to be re-run to be re-read.

## Method

- **Look both ways.** A regression scan that only looks for slowdowns cannot see an erased gain,
  because the erasure IS the slowdown and it lands back at a number that reads as normal. Flag steps
  in both directions, then check whether each improvement is still present at the tip.
- **Report the peak, not just the endpoints.** The deliverable is "best observed, where, and whether
  we still have it", not a single before/after ratio.
- **Confirm a step before believing it.** A flagged step is a hypothesis; settling it is the one-term
  comparison [`docs/investigating.md`](../investigating.md) owns, run at the two adjacent commits
  with more repeats, not more sampled points.

## If the answer is to drop it from the v6 gate instead

The alternative rewrite, kept here so a maintainer can flip this note into it without re-deriving
anything. **Nothing outside this file has to change**: the gate lives in this note's own
`inflight-impact: release-gate` and its first line, not in
[`release-0.6.0.0.md`](release-0.6.0.0.md), which has never mentioned the scan - so dropping it is
retagging this file `<!-- inflight-state: deferred - after v6 -->`, softening the opening line, and
nothing else.

The case for it, honestly: 32 machine-hours buys a 20% resolution over a year of history in which
the only throughput defect anybody has actually found was caught by the ordinary lane within days,
diagnosed, and written up. The scan's value is the gain nobody noticed handing back, and there is no
evidence yet that one exists. The case against is that this is unfalsifiable in exactly the way the
note opens with - an endpoint comparison cannot see one, so "no evidence" is what the absence of the
scan predicts either way.

## What it feeds

The release notes should be able to say what got faster and by how much, sourced from measurement
rather than from what people remember doing. That is also the cheapest way to find out that a
correctness fix quietly cost a fifth of throughput. That last is not hypothetical: it happened in
this window, as `MultiInstanceHighVolumeTest` dropping to 43,552 rec/s, and the cause turned out to
be an eagerly evaluated log argument scanning every shard on every control-loop pass
([`slf4j-defers-formatting-not-argument-evaluation-2026-09-01.md`](../solutions/performance-issues/slf4j-defers-formatting-not-argument-evaluation-2026-09-01.md)).
It was found because a required check went red, which is the case the existing lane already covers.
This scan is for the one that does not go red.

# Scanning the history since 0.5.3.3 for gains we made and then lost

<!-- inflight-type: task -->
<!-- inflight-impact: ci -->
<!-- inflight-state: deferred - needs a perf lane that can be run at a chosen commit, with repeats and randomised order, and somewhere durable for the results -->

**Decided 2026-09-05: this is not a v6 release gate, and the reason is the instrument, not the
question.** The scan is worth running and the statistics for it are worked out below - but the perf
lane cannot be pointed at an arbitrary commit, cannot be asked for repeats in a randomised order, and
does not exist at all across most of the range the scan would cover. Building that is the work item;
the design is the appendix that is waiting on it.

Dropping it touches no other file. The gate was this note's own `inflight-impact: release-gate` and
its opening line - [`release-0.6.0.0.md`](release-0.6.0.0.md) has never mentioned the scan
(`grep -niE 'erased|trajectory|scan' docs/inflight/release-0.6.0.0.md`).

## The question, kept because it does not go away

"Are we faster than 0.5.3.3?" is answerable by measuring two commits. **The interesting question is
whether we were faster still at some point in between.** A change that wins 20% followed by one that
loses 20% nets to zero against the old baseline, so an endpoint comparison reports "no change" and
everybody moves on. The gain is real, it was paid for, and it has been silently handed back.

Nothing in this repo would notice: the throughput check compares against a reference built from
*recent* master runs, so its window slides forward and a gain already handed back is outside it - and
the artifacts holding those runs expire, which is
[`perf-a-queryable-history-instead-of-a-single-committed-baseline.md`](perf-a-queryable-history-instead-of-a-single-committed-baseline.md)'s
subject, not this note's. So the scan is looking for **positive outliers followed by a return to
trend**, a shape only visible across a series.

## Why it is dropped from the v6 gate

Each of these was checked against the tree rather than recalled, and each carries the command that
re-checks it. Together they say the same thing: **the instrument the design assumes does not exist,
and cannot be pointed backwards.**

- **The lane runs on whatever hosted machine GitHub hands out.** `.github/workflows/perf-baseline.yml`
  is `runs-on: ubuntu-latest` with no CPU-model pin, which is why twelve consecutive runs drew four
  different models. `grep -n 'runs-on' .github/workflows/perf-baseline.yml`. Pinning is the design's
  single largest lever on the repeat count, and there is nothing in the workflow to pin.
- **The per-method harness the design measures with is younger than most of the range.** The failsafe
  XML upload, and the `# machine cpu=` line the stratification reads, both arrived in one commit:
  `git log --oneline -S 'machine cpu=' -- bin/performance-test.sh` and
  `git log --oneline -S 'performance-failsafe-xml' -- .github/workflows/maven.yml` both name
  `54301ebdf`, which sits about a tenth of the way back from the tip -
  `git rev-list --first-parent --count 54301ebdf..origin/master` against
  `git rev-list --first-parent --count 0.5.3.3..origin/master`. Every perf-baseline run that exists
  is younger than it (`gh run list -R astubbs/parallel-consumer --workflow perf-baseline.yml`).
- **At the tag there is no lane to run.** `bin/performance-test.sh` is absent
  (`git cat-file -e 0.5.3.3:bin/performance-test.sh` fails), and `VeryLargeMessageVolumeTest` carries
  no `@Tag` at all there - `git grep -c '@Tag' 0.5.3.3 -- '*VeryLargeMessageVolumeTest.java'` finds
  nothing, against a hit on `origin/master`. The script arrives a dozen or so first-parent commits into the range
  (`git log --oneline --diff-filter=A -- bin/performance-test.sh`, then
  `git rev-list --first-parent --count 0.5.3.3..b810e8730`), so the pilot point the design calls for -
  at the tag - has nothing to execute.
- **Nothing can run the lane at a chosen commit.** `perf-baseline.yml`'s `workflow_dispatch` is bare,
  with no inputs (`sed -n '/^on:/,/^permissions:/p' .github/workflows/perf-baseline.yml`). The only
  on-demand route is `maven.yml`'s `suite` dispatch, and `workflow_dispatch` takes a **ref** - a
  branch or tag - not a SHA, and resolves the workflow file *from that ref*, where on an old commit
  it does not exist. Measuring a point therefore means pushing a ref carrying today's harness onto an
  old tree, which is a thing somebody has to build and nobody has.
- **There is no scan driver.** Nothing in `bin/` or `.github/workflows/` dispatches runs, repeats a
  point, or randomises order: `grep -rliE 'randomis|randomiz|trajectory|bisect' bin/ .github/workflows/`
  returns nothing perf-related, and `bin/perf-backfill.mjs` mines runs that already happened rather
  than causing new ones (`grep -nE 'dispatch|shuffle|random' bin/perf-backfill.mjs`).
- **There is nowhere durable to put the results.** A durable series - a data branch or a benchmark
  action - was considered and **declined** on 2026-09-01, recorded in the workflow's own header:
  `grep -n 'DECLINED' .github/workflows/perf-baseline.yml`. A scan whose output lives only in run
  artifacts has to be re-run to be re-read, and the artifacts expire.

**And a confounder the design did not price.** The subject is not isolated from its dependencies:
`pc.pollAndProduce` returns a `ProducerRecord` for **every input record**, so the measured window
contains the Kafka producer client and broker-side produce throughput as well as PC. Those moved
across the range - TestContainers, Kafka and JUnit all stepped
(`diff <(git show 0.5.3.3:pom.xml | grep -oE '<[a-z0-9.-]*\.version>[^<]*<') <(git show origin/master:pom.xml | grep -oE '<[a-z0-9.-]*\.version>[^<]*<')`)
- so **a dependency bump reads as a PC step** in exactly the shape the scan is hunting, and the scan
has no way to tell the two apart. Attributing a flagged step would mean re-running it with the
dependency held fixed, which the design does not currently allow for.

The honest cost/benefit, stated rather than implied: tens of machine-hours buys a 20% resolution over
a year of history in which the only throughput defect anybody has actually found was caught by the
ordinary lane within days, diagnosed and written up
([`slf4j-defers-formatting-not-argument-evaluation-2026-09-01.md`](../solutions/performance-issues/slf4j-defers-formatting-not-argument-evaluation-2026-09-01.md)).
The counter-argument is real and is why this is deferred rather than closed: the scan's value is the
gain nobody noticed handing back, and an endpoint comparison cannot see one, so "no evidence such a
gain exists" is what the *absence* of the scan predicts either way.

---

# Appendix: if it is ever picked up

The sizing below is kept so nobody re-derives it. It is not runnable as written until the tooling
gaps are closed.

## The precondition - the tooling that would have to exist first

This is the real work item. One line each, and nothing here is started:

- A way to run the perf lane **at an arbitrary commit** - a `workflow_dispatch` input taking a SHA,
  with the harness supplied by the dispatching ref rather than by the tree being measured.
- A **pinned runner**, so a point is not silently measured on a different CPU model; plus the
  `cpu=` line checked per run and mismatches re-run.
- A **driver** that takes a point list, repeats each point `n` times in **randomised order** with each
  point's repeats spread across the session, and re-measures the tip periodically as a flatness check.
- A **durable results store**, since the 2026-09-01 decision leaves only expiring run artifacts.
- A **dependency-holding arm**, so a flagged step can be re-run with TestContainers/Kafka/JUnit fixed
  and the confounder above ruled in or out.
- A **backport shim or a documented floor**, because the lane does not exist at the older end of the
  range at all.

## The subject would be `VeryLargeMessageVolumeTest`

`shouldNotThrowBitSetTooLongException`, not the current `MultiInstanceHighVolumeTest`. Three
properties earn it:

- **It is CPU-bound and `KEY`-ordered** over a million records with a distinct key each, so the shard
  map is at its widest - the shape the one throughput defect in this window actually turned on. A
  quiet instrument blind to the class of regression being hunted would be no use, and this one is not
  blind to it.
- **Its workload has not moved across the range.** A million records,
  `PERIODIC_CONSUMER_ASYNCHRONOUS`, `ProcessingOrder.KEY`, `HIGH_MAX_POLL_RECORDS_CONFIG` of 10,000,
  identical at the tag and at master; the commits touching the file changed deadlines, reporting and
  the package name, never the volume or the options. Check before trusting it:
  `git log -p 0.5.3.3..origin/master -- '*VeryLargeMessageVolumeTest.java'`. **A series whose workload
  moves under it measures the test, not the code**, and nothing in the harness detects that for you.
- **The measured window is most of the method** - the `ThroughputReport` line covers most, not all, of
  the method time; the rest is producing the fixture. Reproduce the ratio for a given run:
  `gh run download <id> -R astubbs/parallel-consumer -p 'performance-*'`, then compare `elapsedMs=`
  on the `test=VeryLargeMessageVolumeTest` line of `performance-throughput.txt` against
  `<testcase name="shouldNotThrowBitSetTooLongException" ... time=>` in the failsafe XML. Draw the
  per-method `time=`, because it excludes container startup - the argument
  `bin/lib/throughput-verdict.mjs` already makes for the verdict.

What it would miss, stated rather than discovered later: it is **single-instance**, so a regression
appearing only with several PC instances in one group - rebalance handling, cross-instance commit
contention, the confluentinc#857 family's territory - does not reach it. That is not an argument for
the multi-instance test as the subject; it is an argument for running the whole lane at each point,
which costs nothing extra, and treating the subject series as the one with the resolution.

### `LoadTest` cannot carry it either

Tempting, at the quietest spread in the lane, and wrong twice over.

- **It is inert.** Every record sleeps `nextInt(0, 5)` ms - `RandomUtils.nextInt` is **exclusive at
  the top**, so the values are 0-4 and the mean is **2.0 ms**, not 2.5. The wall time is the sleep,
  and a change in per-record engine cost is a small fraction of that and disappears into it. Its
  within-model spread is not a precise measurement of PC, it is a precise measurement of
  `Thread.sleep`. The reasoning `bin/lib/throughput-verdict.mjs` gives for refusing it as a
  machine-speed control applies to refusing it as a subject.
- **Its own workload changed inside the window.** `asyncConsumeAndProcessAtVolume` and its 40,000
  records arrived with astubbs#264; before that the class ran `asyncConsumeAndProcess` over
  `total = 4_000`. A `LoadTest` series across this range therefore carries a discontinuity that is the
  test changing, not the code, and would read as exactly the step being hunted.

**Keep it as a tripwire, not a series.** It is the flattest thing in the lane across every runner
model, so a `LoadTest` point that moves says the harness or the infrastructure moved, and every other
series that day is suspect.

## The noise is in the subject, not in the lane

Two measurements. `bin/lib/throughput-verdict.mjs` **owns** the eight-run, one-idle-box table beside
`NOISE_FLOOR` - the per-series figures, the finding that the sleeping controls are inert, and the
finding that every normalisation *added* noise rather than cancelling it. Read them there; they are
deliberately not restated here.

The second, taken to size this note: **2026-09-05, twelve consecutive `perf baseline (master)` runs**,
per-method times from the failsafe XML those runs upload, stratified by the `# machine cpu=` line
[`bin/performance-test.sh`](../../bin/performance-test.sh) writes into the summary for exactly this
purpose. Relative IQR of the per-method time:

| Series | Across all runners | Within one CPU model |
|---|---|---|
| `LoadTest.asyncConsumeAndProcessAtVolume` | 3.5% | 0.5-0.6% |
| `VeryLargeMessageVolumeTest.shouldNotThrowBitSetTooLongException` | 29.0% | 2.8-5.7% |
| `MultiInstanceHighVolumeTest.multiInstance` | 22.0% | 2.5-13.0% |

Reproduce it, do not quote it: `gh run list -R astubbs/parallel-consumer --workflow
perf-baseline.yml`, then `gh run download <id> -p 'performance-*'` and read `<testcase time=>`
against the summary's `cpu=` line. Those runs are twelve *different* commits, so each figure mixes
noise with whatever the code did - every one is an **upper bound** on the noise, the safe direction
for sizing.

Two things follow:

- **Most of the apparent CI noise is which runner you drew.** `VeryLargeMessageVolumeTest` is bimodal
  by CPU model: near 33 s on the older EPYC, near 25 s on all three newer models. Stratified, its
  spread collapses and agrees with the idle-box figure. **Pin the machine and the instrument improves
  more than any redesign would**; that costs a `runs-on` label.
- **The subject's noise is intrinsic and does not collapse.** It stays near its idle-box figure within
  one CPU model. The mechanism is in the test: it starts three PC instances against one group, so its
  wall time contains a group-join settling period quantised by broker-side timers.
  `VeryLargeMessageVolumeTest` is single-instance and has no such term.

On this twelve-run series the share `subject / VeryLarge` came out quieter than either component raw -
which is the cross-runner case `bin/lib/throughput-verdict.mjs` says outright its own single-box data
could not test, and so is not a contradiction of it. It does not change the recommendation: pinning
beats normalising, because it needs no control, no model list and no second measurement's variance.

### The quantisation floor nobody priced

The subject's completion is detected by an Awaitility wait with `.pollInterval(1, SECONDS)`
(`grep -n 'pollInterval' parallel-consumer-core/src/test-integration/java/bz/stub/parallelconsumer/integrationTests/VeryLargeMessageVolumeTest.java`).
The measured window can therefore only end on a one-second boundary, so up to about a second of it is
quantisation rather than work. Against a method time in the low tens of seconds that is **a few
percent - the same order as the pinned spread the sizing treats as noise**. Dropping the poll interval
is a cheap precondition and would tighten σ before any of the arithmetic below is redone.

## How many repeats a point would need

**Distribution assumed: log-normal in the body, contaminated by a heavy right tail.** Defensible for
run times specifically - a run is bounded below by the work it must do and unbounded above (a GC
pause, a noisy neighbour or CPU steal can only make it slower), and the perturbations are
multiplicative, so the logs are roughly normal. The tail is why the estimator is a **median of n**
rather than a mean. Below about 20% relative spread the log-scale and relative-scale arithmetic agree
to within a point, so the sums are in relative units.

The arithmetic assumes the `n` repeats at a point are **independent**, which they are not if they run
back to back in one session - hence the randomised-order requirement, a design constraint rather than
a caveat. Under those assumptions the median of `n` has standard error `σ·√(π/2n)`, the difference of
two independent points has `σ·√(π/n)` = `1.7725·σ/√n`, and detecting a step `δ` with power `1-β` at
significance `α` needs

```
n ≥ ( (z[α/2] + z[β]) · 1.7725 · σ / δ )²
```

**`α` is corrected for how many comparisons the scan makes.** A trajectory over `P` sampled points
tests `P-1` adjacent pairs, and the failure this scan exists to avoid is exactly a peak that is not
there, so Bonferroni applies. Both columns are given because the per-pair one is the right test
*after* the fact, when checking one candidate step somebody has already pointed at.

Repeats per point, 80% power, rounded up, family-wise column computed at the sparse `P` the plan
below yields:

| Series and setup (σ) | 20% step, per pair | 20% step, family-wise | 10% step, per pair | 10% step, family-wise |
|---|---|---|---|---|
| `VeryLargeMessageVolumeTest`, machine pinned (6.1%) | 3 | **5** | 10 | **20** |
| `MultiInstanceHighVolumeTest`, machine pinned (13.4%) | 12 | **24** | 45 | **95** |
| `MultiInstanceHighVolumeTest`, runner unpinned (22.0%) | 30 | 64 | 120 | 254 |
| `VeryLargeMessageVolumeTest`, runner unpinned (29.0%) | 52 | 111 | 208 | 441 |

The same arithmetic read backwards, as the smallest step a family-wise honest scan may flag:

| Repeats per point | `VeryLarge` pinned | current subject pinned |
|---|---|---|
| 1 | 44% | 97% |
| 3 | 26% | 56% |
| 5 | 20% | 43% |
| 8 | 16% | 34% |
| 20 | 10% | 22% |

**That last table is the whole argument.** A trajectory drawn through single measurements of the
current subject cannot honestly flag anything smaller than a doubling. Change the subject and pin the
machine and five repeats resolve a 20% step - the size of effect this scan hunts.

### Four things wrong with the arithmetic above, none fatal, all load-bearing

Anyone picking this up fixes these before running anything.

- **The σ column's unit is unverified.** The pinned figures are the eight-run "robust
  (outlier-insensitive) spread" from `bin/lib/throughput-verdict.mjs`, and **that file defines the
  estimator nowhere** - IQR, MAD and a trimmed SD would all fit the phrase
  (`grep -niE 'robust|IQR|deviation' bin/lib/throughput-verdict.mjs`). The 2026-09-05 figures beside
  them *are* relative IQR, so the two sources are not known to be on one scale. It matters
  quantitatively: for a near-normal body σ = IQR/1.349, and because `n ∝ σ²`, feeding an IQR in where
  a σ belongs **overstates every entry in the repeats column by about a factor of 1.8** - the
  recommended 5 would be 3, and the current subject's 24 would be 14. Settle the estimator first; the
  table is conservative until you do, which is the safe direction but not a free one.
- **The median's standard error is normal theory, and the stated distribution is not normal.**
  `σ·√(π/2n)` comes from `1/(2·f(median)·√n)` evaluated for a Gaussian. The note then assumes a
  log-normal body with a heavy right tail and does *not* carry that assumption through to `f(median)`.
  A heavier tail concentrates mass in the body, raising `f(median)` and so **lowering** the true SE -
  probably conservative again, but that is an argument, not a computation. Either evaluate the SE for
  the assumed density or bootstrap it from the run series directly.
- **The family-wise column is pinned to one sparse density and sits on a rounding boundary.** `z[α/2]`
  rises with `P`, so those entries are only valid at the `P` the plan below yields. The recommended
  entry computes to about 4.87 before rounding: it crosses into **6** once the adjacent-pair count
  passes roughly 51, i.e. a `P` in the low fifties - which an every-sixth-commit density would give.
  Recompute the column for whatever density is actually chosen; do not carry the 5 over.
- **The subject comparison is a spread ratio, not a "four times quieter".** Pinned, the current
  subject's spread is about **2.2×** the proposed one (13.4% against 6.1%). It is the **variance**
  ratio, about **4.8×**, that sets the repeats - which is why the family-wise column goes 24 against 5.
  Say both; "four times quieter" states neither and reads as the spread.

## The sampling plan, and what it would cost

Sparse pass first, then densify around anything that looks like a step in **either** direction. Each
input with the command that yields it, never a number to go stale:

| Input | Command |
|---|---|
| Commits in range | `git rev-list --count 0.5.3.3..origin/master`, and `--first-parent` for the walk |
| Sparse points `P`, at every 8th first-parent commit | `git rev-list --first-parent 0.5.3.3..origin/master \| awk 'NR%8==1' \| wc -l` |
| Repeats `n` | the sizing table above, recomputed for that `P` |
| Lane wall time per run | job durations from `gh run list -R astubbs/parallel-consumer --workflow perf-baseline.yml --json databaseId,createdAt,updatedAt` |

**Use `awk 'NR%8==1'`, not `sed -n '1~8p'`.** The step address is a GNU extension; BSD `sed` rejects it
with `invalid command code ~` on stderr, and in the pipeline above `wc -l` still prints - as **0** -
so the whole thing reads as a completed count of zero points on macOS.

Cost is `P × n × lane-minutes`, plus the densifying runs (budget a handful of candidate steps, each
bisected a few levels at the same `n`). At the recommended subject and repeat count that lands in the
**tens of machine-hours** of the whole lane for a 20% resolution. A 10% resolution multiplies it by
`n` going 5 → 20, i.e. **about four times as much**; keeping the current subject at 20% costs `n`
going 5 → 24. Resolve 20%, and say in the write-up that 10% was out of budget. Running only the
subject class saves roughly a third of each run - most of a run is the reactor build, not the tests -
and loses the tripwire, so it is not recommended.

Practical constraints the plan must survive:

- **Measure the points in RANDOMISED order, never in commit order.** A scan run oldest to newest makes
  commit index perfectly correlated with wall-clock time, so thermal drift, a filling disk,
  accumulating Docker state or a change in runner-fleet composition renders as a trend in the history -
  and a trend is precisely what this scan reports.
  [`an-a-b-whose-arms-run-in-time-order-is-confounded-with-time.md`](../solutions/best-practices/an-a-b-whose-arms-run-in-time-order-is-confounded-with-time.md)
  owns the rule and its signature; the version binding here is that a point's `n` repeats are **spread
  across the session** rather than run back to back, so a point measured during a slow hour is not slow
  in all its samples. Re-measuring the tip commit at intervals through the pass is the cheap check: its
  series should be flat, and if it is not, the whole trajectory is suspect.
- **Pin `runs-on` and check the `cpu=` line of every run**, discarding or re-running points that drew a
  different model. Unpinned, the repeats column does not scale by one factor: for the proposed subject
  it rises more than twenty-fold (5 → 111 family-wise at 20%, because its cross-model spread is 29%),
  and for the current subject under three-fold (24 → 64). Pinning is worth most to exactly the subject
  the design recommends.
- **Old commits must still build.** The range crosses the `io.confluent` → `bz.stub` package rename, so
  class names in the failsafe XML differ across it (`-Dit.test=VeryLargeMessageVolumeTest` selects by
  simple name and is unaffected). Whether a commit near the tag resolves its dependencies and starts
  its broker container is unknown - **run one pilot point at the tag before committing to the pass**,
  noting it needs the harness backport listed in the preconditions to run at all.
- **Store results as they are produced.** Perf artifacts expire, and a scan whose output lives only in
  run artifacts has to be re-run to be re-read.

## Method, if it runs

- **Look both ways.** A regression scan that only looks for slowdowns cannot see an erased gain,
  because the erasure IS the slowdown and it lands back at a number that reads as normal. Flag steps in
  both directions, then check whether each improvement is still present at the tip.
- **Report the peak, not just the endpoints.** The deliverable is "best observed, where, and whether we
  still have it", not a single before/after ratio.
- **Confirm a step before believing it.** A flagged step is a hypothesis; settling it is the one-term
  comparison [`docs/investigating.md`](../investigating.md) owns, run at the two adjacent commits with
  more repeats - and with the dependency versions held fixed, per the confounder above - not with more
  sampled points.

## What it would feed

Release notes able to say what got faster and by how much, sourced from measurement rather than from
what people remember doing. That is also the cheapest way to find out that a correctness fix quietly
cost a fifth of throughput - not hypothetical: it happened in this window, as
`MultiInstanceHighVolumeTest` dropping sharply, and the cause was an eagerly evaluated log argument
scanning every shard on every control-loop pass
([`slf4j-defers-formatting-not-argument-evaluation-2026-09-01.md`](../solutions/performance-issues/slf4j-defers-formatting-not-argument-evaluation-2026-09-01.md)).
It was found because a required check went red, which the existing lane already covers. This scan is
for the one that does not go red - which is why it is deferred rather than dropped.

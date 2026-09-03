# Investigation: where the CI Integration Tests wall time goes, and why more forks make it worse

Date: 2026-09-03. Lane: `.github/workflows/maven.yml`, the `Integration Tests` matrix entry
(`bin/ci-integration-test.sh`, failsafe, `ubuntu-latest`). It is the critical path of the PR build.

Measured on CI, by dispatching maven.yml's own `workflow_dispatch` harness with `suite=integration`
on one branch per sample. There is no faithful local proxy: the lane is TestContainers brokers on a
hosted runner, and the `unit-gate` run's 2-CPU Docker replica does not extend to Docker-in-Docker.

> **Read the postscript at the end before acting on anything here.** Two later batches overturned
> parts of this document: the baseline's "16s spread" understated the noise by nearly an order of
> magnitude, the 4% comparison threshold derived from it was never justified, and the compaction
> poll listed below as open work was tried and is a proven no-op. The measurements and the
> reasoning above are left as they were taken; the postscript says which conclusions survived.

## Headline

**Raising `forkCount` from 4 to 6 makes this lane slower, not faster** - 469s of failsafe against a
420s baseline - and it destabilises a test that has never failed before. The plan that led here
assumed the opposite. The three-arm control design that caught it is the reusable part of this
document.

## Baseline, and a correction to the premise

Three samples of identical trees at `c30aaee15`, dispatched in parallel:

| metric | samples | median |
|---|---|---:|
| `job_seconds` | 620 / 622 / 606 | **620** |
| `maven_seconds` | 607 / 604 / 595 | 604 |
| `core_failsafe_seconds` | 428 / 420 / 417 | 420 |
| `slowest_class_seconds` | 348 / 339 / 333 | 339 |
| `class_time_total` | 1561 / 1528 / 1520 | 1528 |

**The lane is 10m20s on master, not the ~11m that prompted the work.** The 11m readings were three
*different* feature branches running 207 tests against master's 204; their spread was mostly
content, not variance. Identical trees agree to within 16s (2.6%), which is far more reproducible
than the PR history suggested and let the comparison threshold be set at 4% rather than 6%.

## Where the 604s of Maven actually goes

Attributed by differencing the timestamp of every `[INFO] --- plugin:goal @ module ---` line;
682 of 683s accounted for, so nothing material hides between goals.

| goal | seconds | share |
|---|---:|---:|
| `failsafe:integration-test` | 546 | 80.1% |
| `compiler:testCompile` | 60 | 8.8% |
| `compiler:compile` | 42 | 6.2% |
| `javadoc:jar` | 14 | 2.1% |
| `lombok:delombok` | 8 | 1.2% |
| `truth:generate` | 7 | 1.0% |
| everything else | ~5 | 0.7% |

**A correction worth having**, because the intuitive target is wrong: the always-active lifecycle
extras the root pom binds outside any profile - `delombok`, `javadoc:jar`, `source:jar`, jacoco's
report goals, `forbiddenapis`, `dependency:tree` - total **29s**, not the minute-plus their number
suggests. The non-test cost that is actually worth a hypothesis is **compilation at 102s**, and
specifically `compiler:testCompile` at 60s: this lane runs `-DskipUTs=true`, which stops the unit
tests running but not being *compiled*, so it builds all of `src/test/java` and executes none of it.

`ossindex:audit` costs nothing and was never a candidate - it binds to `validate` but activates only
under `-Dossindex.skip=false`, which only `dependency-audit.yml` sets
(`docs/inflight/ci-ossindex-lane-reassessment.md`).

## The experiment: a 2x2 factorial, and what it refuted

The model going in: failsafe's forks pull whole *classes* from one queue, so a class is never split
across forks, and the wall is bounded by `max(slowest_class, class_time_total / forkCount)`. At
forkCount=4 those two floors nearly coincided - 339s of `Rebalance857CommitSyncDeadlockProbeIT`
against 1528/4 = 382s - which predicted that raising forks and splitting the probe would each be
worthless alone and large together. Each arm's prediction was written into its commit message
before it ran.

| arm | forks | probe | `core_failsafe` | `class_time_total` | tail | build |
|---|---|---|---:|---:|---:|---|
| baseline | 4 | whole | **420** | 1528 | 339 | pass |
| H1 | 6 | whole | **469** | 1691 | 332 | FAIL |
| H2 | 4 | split | **424** | 1533 | 161 | pass |
| H3 | 6 | split | **379** | 1710 | 160 | FAIL |

- **H1 predicted 340-380, measured 469.** Refuted, in the opposite direction.
- **H2 predicted "nearly nothing", measured 424 against 420.** Confirmed exactly.
- **H3 predicted 260-300, measured 379.** Direction right, magnitude wrong by ~100s.

**The single false assumption was that `class_time_total` is invariant to `forkCount`.** It is not:
the identical tests cost 1528s at four forks and ~1700s at six, +11%, because the forks contend.
More forks manufacture work. Recomputing the bound at the work actually observed gives
`max(160, 1710/6) = 285s` for H3 against 379s measured, so contention degrades packing efficiency
*on top of* inflating the work - two compounding penalties the model had neither of.

### Read the comparison on `core_failsafe_seconds`, never `job_seconds`

H1 and H3 failed at `verify`, so Maven never built the other ten modules and their `job_seconds`
omit roughly 120s that the passing runs paid. **H1's `job_seconds` of 548 looks better than the
620s baseline while its failsafe phase was 49s worse.** Ranked on `job_seconds`, the most harmful
arm of the four is the winner. Any future harness for this lane should refuse to rank a sample
whose build did not succeed, whatever its timings say.

### The two reds are different kinds of event

- H3's `RegistrationRaceStaleResidentIT`, on `control thread must reach the mid-loop pause point
  (offset 25)`, is a **known flake** with that exact signature already in
  [`docs/inflight/test-untracked-ci-flakes.md`](../inflight/test-untracked-ci-flakes.md). Not
  attributable to the change; recorded there as a further sighting.
- H1's `ManagedPCInstanceLifecycleTest.rapidToggleShouldNotCreateDuplicateInstances` is **not
  known**. `bin/inflight.mjs codecov test` shows it passing on every recorded run, normally in
  11-21s. It failed at 31s, timeout-shaped, while its class ran 124s against a 103.5s baseline.
  First failure ever, under measurable contention. **That one is attributable to fork pressure**,
  and it is the starvation signature
  [`parallel-integration-tests-flaky-under-concurrency-2026-07-28.md`](../solutions/test-flakiness/parallel-integration-tests-flaky-under-concurrency-2026-07-28.md)
  describes and that `docs/ci.md` predicted for failsafe specifically.

The distinction is the reason both arms ran. `docs/solutions/best-practices/attribute-a-red-only-after-a-control-arm-on-the-gates-own-configuration.md`
is about not blaming a change for a pre-existing rate; the converse applies here too - a *known*
flake alongside a *first-ever* failure are not the same evidence, and averaging them would have
hidden the real one.

## What this closes, and what it opens

**Both parallelism directions for this lane are now measured and closed.** Thread-parallelism was
closed in 2026-07 (real races, plus starvation on a hosted runner). Fork oversubscription is closed
here. `forkCount=4` is not a number to tune upward; the remaining lever is **work reduction**.

Still open, in rough order of measured size:

1. `PartitionStateCommittedOffsetIT.triggerCompactionProcessing()` sleeps a flat 20s with the
   author's own `// or wait?` beside it, from two call sites in a seven-test class - 60s+ of the
   class's 159s. Waiting on the log start offset advancing instead makes the 20s a deadline rather
   than a duration, and is strictly more informative than sleeping, which asserted nothing.
2. `Rebalance857CommitSyncDeadlockProbeIT`'s `@RepeatedTest(20)` is ~340s. Splitting it (H2) is
   proven green and free but buys nothing at forkCount=4; its value is as a precondition. Cutting
   the per-PR repetition count is the larger lever and a detection-power trade-off for a human.
3. `compiler:testCompile` at 60s, compiling unit tests this lane never runs.
4. The 29s of always-active javadoc/source/delombok work.

## Two stale claims found while reading, corrected in this PR

**`parallel-consumer-core/pom.xml`** justified withholding a per-fork log directory from failsafe
with "It declares no forkCount, so it runs at the default of 1". maven.yml passes `-DforkCount=4`,
and failsafe has no pinned `<forkCount>`, so it takes that bare user property - the very
`-DforkCount` vs `-Dsurefire.forkCount` trap the same comment warns about a few lines earlier. Four
JVMs are already writing to one flat `pc.log.dir`, into appenders that comment says lose writes
under exactly that condition. Keeping the path flat may still be right, because `bin/`'s experiment
runners classify from a flat `probes.log`; but the stated reason was not true.

**`bin/ci-integration-test.sh`**'s header said "the ci profile runs sequentially on GitHub-hosted
2-core runners", describing the unparameterised script rather than what CI invokes.

## An unverified premise worth settling cheaply

The runner reports `Image: ubuntu-24.04`. GitHub moved standard public-repo runners to 4 vCPU/16GB
in early 2024, while `docs/self-hosted-runner.md` and `docs/ci.md` still reason from "2-core" - and
the `unit-gate` run built a **2-CPU Docker replica** to imitate this runner, concluding from it that
"the 2-core box is already saturated" and that `-T 1C` oversubscribes "2 CPUs ~4:1".

**Nothing here read `nproc`, and an image name is not a core count**, so this is flagged rather than
asserted. One `nproc` line in the CI step would settle it permanently, and it would change how those
older conclusions should be read. This investigation did not need the answer: H1 and H3 measured the
oversubscription directly.

---

## Postscript: this lane's wall time cannot resolve a 20-second change

Two further batches were run after the section above was written. They overturn part of it, and the
correction is more useful than the original claim.

### The noise floor, measured properly

**Three CONCURRENT samples of identical code spread 119 seconds: 563 / 575 / 682.** That is roughly
eleven times the largest effect the remaining hypotheses were worth.

The baseline at the top of this document reported a 16s spread and a comparison threshold was set
from it. **That number measured almost nothing.** Three samples dispatched together share fleet
conditions and are correlated; their agreement describes within-minute reproducibility, not the
run-to-run variance that matters when arms are compared. A later batch of three concurrent samples
of *near-identical work* disagreed by 82s, and the batch after that put identical code 119s apart.

Decomposed, the variance is not in the work. It is in **fork packing efficiency** - how well four
forks fill - which none of the changes tried here touch:

| arm | work | failsafe | efficiency |
|---|---:|---:|---:|
| control (master) | 1491 | 413 | **3.61x** |
| build-skip | 1451 | 478 | 3.04x |
| compaction poll | 1455 | 485 | 3.00x |

Same base, same fork count, same 42 classes, dispatched together. The residual is which VM each job
drew.

### A retraction

An earlier reading of this run reported `prebuild_seconds` as cleanly separating a ~10s effect (43
against a control of 52). It did not. The control has since been measured at 41, with its own
samples spanning 41-57. **That separation was drift with a convenient sign.** No metric this
harness collects - `job_seconds`, `core_failsafe_seconds`, `prebuild_seconds` or
`class_time_total` - resolves an effect of ~10-40s on this lane.

### What the logs settled that the timings could not

With wall-clock useless at this scale, both remaining changes were judged by **counting artefacts
and log lines** instead:

- **Skipping javadoc, sources and delombok WORKS**: a run with the flags builds **0 javadoc jars
  against 9** without, and delombok reports skipping **11 times against 2**. It provably does less
  work and therefore cannot be slower, which is the whole basis on which it was taken. The first cut
  of it also carried a real bug - `-Dsource.skip=true` is not a property maven-source-plugin reads
  (it wants `maven.source.skip`), so sources jars were still built in both arms and nothing in the
  timings could have revealed it.
- **The compaction poll is a NO-OP**: it logged "Compaction did NOT advance" four times out of four,
  so it never fired and paid the full 20s deadline every call. Dropped rather than merged. The
  detector was wrong - log compaction need not advance the log start offset - and the open question
  it exposed is recorded in
  [`docs/inflight/test-compaction-wait-has-no-observable.md`](../inflight/test-compaction-wait-has-no-observable.md).

### The conclusion that follows

**Incremental optimisation of this job is not measurable.** Effects of 10-40s are real but invisible,
and no affordable sample count changes that: the variance is in the fleet, not in the sampling.

That is an argument for a structural change rather than more tuning. Sharding is the only lever
whose effect would exceed 119s, and
[`docs/inflight/ci-shard-the-integration-gate.md`](../inflight/ci-shard-the-integration-gate.md)
carries it with the ordering argument - the serial build work is what each shard re-pays, so cutting
it first is what makes sharding pay.

**And the method generalises past this lane.** When an effect sits below the noise floor, the way
forward is not more samples; it is to stop measuring the aggregate and start counting the thing the
change actually does. Nine javadoc jars became zero. Four compaction waits fired zero times. Neither
fact needed a stopwatch, and neither could have been read off one.

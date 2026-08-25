# Direct pull's collapse is the scan, not the claim - and the scan is now fixed for UNORDERED

<!-- inflight-type: perf -->
<!-- inflight-impact: performance -->
<!-- inflight-labels: needs-measurement -->

Measured 2026-08-22. Two mechanisms were on the table for the direct-pull engine's collapse above a
hundred workers, and they call for completely different fixes: **workers losing the claim CAS to each
other**, or **the shard scan being paid once per record on every worker**. The obvious word is
contention. **It is not contention.**

`DirectPullScanCostMeasurementTest` settles it, and `ShardOccupancy` removes what it found.

## Why the benchmark could not have settled this, and what does

In the engine the number of scanning workers and the number of records in flight **are the same
number** - `maxConcurrency` sets both - so no observation of the engine can tell a per-scanner cost
from a per-in-flight-record cost. Every one of the six benchmark cells in
[`perf-direct-pull-measured.md`](perf-direct-pull-measured.md) moves both terms together.

The test decouples them. Workers only *select*; a single controller thread holds exactly `inFlight`
records out at any instant and completes the oldest to make room. So `scanners` and `inFlight` move
independently, and **`scanners = 1` is a control arm in which claim contention cannot exist at any
depth**. Cost is read off `DispatchScanMeter`, which counts - the reason it exists is that this
machine's load moves a stopwatch by more than the effect being measured.

## The numbers

Ten shards, 20,000 records, one-minute load 5-8 on twelve cores throughout. Entries examined per
record dispatched:

**One scanner. No claim contention is possible.**

| in flight | before | after |
|---:|---:|---:|
| 10 | 1.00 | 1.00 |
| 100 | 9.99 | 1.00 |
| 1,000 | 98.02 | 1.01 |
| 5,000 | **440.13** | **1.00** |

**Scanners and in-flight together, as the engine has them:**

| arm | before | after |
|---|---:|---:|
| 10 x 10 | 1.54 | 1.20 |
| 100 x 100 | 11.01 | 1.29 |
| 1,000 x 1,000 | 91.57 | 1.27 |
| 5,000 x 5,000 | **1,621.89** | **1.60** |

**Scanners alone, at a fixed depth of 1,000 in flight** - the arm that isolates contention:

| scanners | before |
|---:|---:|
| 1 | 97.71 |
| 10 | 102.87 |
| 100 | 106.56 |
| 1,000 | 72.94 |
| 5,000 | 1,142.89 |

## What that says, including the part that is not a clean win for either hypothesis

- **The walk alone accounts for a 440x rise between concurrency 10 and 5,000, with a single
  scanner.** No lock-tuning, no backoff, no striping of the resume point and no reduction in claim
  collisions could have touched it. Hypothesis 2 dominates, and it is not close.
- **Adding scanners at a fixed depth is nearly free until the very top.** 1 to 100 scanners moves the
  count by under 10%. Claim contention is real but secondary.
- **At 5,000 it is not negligible: 1,621.89 against the 440.13 the walk alone explains.** The extra is
  the walk's own consequence - `UNORDERED` has no `break`, so a worker that loses a claim carries on
  walking the contested region, and a burst of K simultaneous walkers over the same head costs about
  K²/2 examinations there. **Contention that only exists because there is a walk to contend over.**
  With the index the same arm costs 1.60, so removing the walk removed the contention with it.
- **The 1,000-scanner cell being *lower* than the 100-scanner one was not predicted and is not
  explained.** It is reported rather than smoothed over. The likely reading is that at 1,000 scanners
  against 1,000 slots the harness's reservation gate throttles simultaneous walkers, which is a
  property of the harness and not of the engine; nobody has confirmed it.

## The fix

`ShardOccupancy` keeps an index of the offsets no worker is holding, and `UNORDERED` dispatch walks
that instead of the shard's entry map. Maintained by the two halves of one state transition on the
record - claimed and landed - which is the arrangement already keeping the per-shard in-flight count
from drifting, so there is no removal site whose condition can be got wrong.

**The ordered modes deliberately still walk the entry map.** In `KEY` and `PARTITION` the in-flight
record at the head is what makes a shard refuse a second taker; taking it out of that walk's view is
what cost `perf/split-shard-inflight` ten failing tests
([`parked-resume-shard-dispatch-scan.md`](parked-resume-shard-dispatch-scan.md)). They also have
nothing to gain - the break fires after one entry either way.

This is the third design in that note - *an index over the single collection* - which it called the
best of three and did not build because the answer was already known **for the shipped engine**. It
was: three mechanisms all returned 0% end to end, because in-flight is bounded well below where the
walk hurts when the control loop is the only selector. **Direct pull is the case where that bound does
not apply**, and there the same walk is the whole story.

## What is NOT measured, and it is the number that matters most

**No end-to-end throughput run has been taken since the change.** The counting instrument says
dispatch work per record is now flat in concurrency; it does not say what the engine does at
5,000 with a real broker. That run is the point of this work and it has not happened - the machine
was carrying a benchmark sweep in another worktree for most of the session, and a throughput number
taken at load 800 is worthless.

**The experiment to run**: `bench/run-direct-pull.sh` at 0ms and 2ms, concurrencies 10, 100, 1,000 and
5,000, on a quiet machine. Two things have to hold for direct pull to be worth putting back into the
comparison matrix: **it must complete a 5,000 run at all** (it currently hits the 60s cap), and it must
not have regressed at 10 and 100, where it is 3.2x faster than the shipped engine and must stay so.

## A test that this change makes fail more often, and it is not a defect

`CoreBatchTest.simpleBatchTest` under `-Dpc.directPull=true`: five records at batch size 2, expecting
three batches, receiving four - `[0,2] [4] [3] [1]`. **Every record is delivered exactly once and no
batch exceeds the batch size**; what fails is the assumption that a batch is filled by one selector.
Direct pull has never satisfied that, and the same class of assumption already accounts for two of the
failures recorded in [`perf-direct-pull-measured.md`](perf-direct-pull-measured.md). Making selection
genuinely concurrent surfaces it more often.

Rates, because "it flakes" is not a measurement. Full core unit suite, twelve cores, one-minute load
5-9:

| Arm | Runs | Result |
|---|---:|---|
| Direct pull, before | 5 | Exactly one failure every time, always `SubmitWorkToPoolShutdownRaceTest.drainModeCloseStillTerminatesOverADeadPool` - deterministic and pre-existing |
| Direct pull, after | 3 | That same failure, **plus one more in every run**: `CoreBatchTest.simpleBatchTest` twice, `inFlightMessagesCommittedIfProcessedDuringShutdown` once |
| Shipped engine, after | 3 | Clean twice; once `ParallelEoSStreamProcessorTest.processInKeyOrder:783`, which [`test-untracked-ci-flakes.md`](test-untracked-ci-flakes.md) already records as a point check that should be an await |

**0 of 5 before against 3 of 3 after is a real difference and is not dismissed as noise.** The
attribution is the failure text itself: all five records present, batch sizes legal, four selectors
instead of three.

## Prior art this settles, contradicts, or leaves alone

- [`perf-unordered-dispatch-rescans-the-inflight-prefix.md`](perf-unordered-dispatch-rescans-the-inflight-prefix.md)
  asked for exactly this measurement at high concurrency and near-zero handler delay, and predicted the
  cost scales with `maxConcurrency`. **It holds, and the coefficient is one examination per in-flight
  record per shard.**
- [`next-direct-pull-unordered-selection.md`](next-direct-pull-unordered-selection.md) offered three
  shapes: a shard lock, O(1) selection, and per-worker shard affinity. **Option 2 is what landed.**
  Option 1 would have serialised something that is not the cost, and option 3 cannot help ten shards
  against thousands of workers.
- [`parked-2022-central-queue-rework.md`](parked-2022-central-queue-rework.md)'s "1/3 as fast" figure
  was checked and, as its own retraction says, measures `centralQueue.take()` on a different design.
  **It settles nothing here and was not used.**
- [`next-selectable-shard-queue.md`](next-selectable-shard-queue.md) states plainly that it does
  nothing for `UNORDERED`, which is the mode this collapse was measured in. **That remains true and is
  not a criticism of it**: it targets the cost of *visiting* shards that cannot yield, which is a
  different quantity, and `DispatchScanMeter` cannot see it at all - the busy-shard guard skips such a
  shard without examining an entry. Deciding it needs a shards-visited counter, which does not exist.
- [`next-pre-rendered-work-order-list.md`](next-pre-rendered-work-order-list.md) is a superset of what
  landed here - a list of eligible *records* rather than a per-shard index of unheld offsets. Its own
  arithmetic put incremental maintenance at ~2 writes per delivery; the index costs exactly that
  (one add, one remove) for `UNORDERED` while leaving the ordering enforcement completely untouched.
  **Whether the remaining idea still earns a third representation of the data is now a question about
  the ordered modes only.**

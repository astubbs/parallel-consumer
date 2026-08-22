# Direct pull AND virtual threads together is the fastest configuration measured

<!-- inflight-type: next -->
<!-- inflight-impact: performance -->
<!-- inflight-labels: needs-measurement, release-note -->

**Antony asked, 2026-08-22: "does `core-vt` use direct pull underneath now? If not, why haven't you
tested vt on dp?"** It does not, they compose, and nobody had - a gap I had named that morning as
worth testing and then dropped.

## They compose because the pullers run on the worker pool

`DirectPullWorkerPool#start(Executor, int)` runs its pullers on whatever executor it is handed, and
`AbstractParallelEoSStreamProcessor` hands it `workerThreadPool.get()`. So `#setupWorkerPool` decides
what a puller *is*: with `useVirtualThreads` on, **direct pull's workers are virtual threads.** The two
options are independent booleans; nothing prevents both.

## The measurement

100,000 records, one partition, `UNORDERED`, concurrency 5,000, non-blocking callee, JDK 21, two
repeats, load 3-13 at the start and rising to ~150 by the end (the figures below are stable across
repeats, and the in-flight column is the load-robust one):

| Arm | 2ms msg/s | peak in flight | 100ms msg/s | peak in flight |
|---|---:|---:|---:|---:|
| **`core-dpvt`** both | **25,874** | **5000** | **18,376** | **5000** |
| `core-vt` | 24,219 | 4,702-4,782 | 17,270 | 5000 |
| `core-dp` | 20,778 | 1,071-3,979 | 11,408 | 4,713-5000 |
| `core` shipped | 19,149 | **543-768** | 12,422 | 2,841-3,953 |

**1.35x the shipped engine at 2ms, 1.48x at 100ms**, and the only arm holding a flat 5000/5000 at
both delays.

## It explains the anomaly it was run to investigate

Direct pull alone was the **worst** arm at 100ms - 11,408 against the shipped engine's 12,422 -
**while holding 5,000 records in flight to the shipped engine's 2,841.** More concurrency, less
throughput, which had no explanation.

**Put the pullers on virtual threads and it goes to 18,376: +61%.** So the deficit was the cost of
5,000 *platform* threads doing the pulling. Direct pull was paying the platform-thread ceiling to
reach its concurrency, and the throughput that concurrency bought did not cover what it cost.

**The framing that produced the puzzle was wrong**, and worth recording as such: "more concurrency,
less throughput, so something other than concurrency is the bound" treated the concurrency as free.
The bound was the threads *achieving* it - which is the same ceiling
[`perf-platform-threads-are-the-ceiling.md`](perf-platform-threads-are-the-ceiling.md) documents,
arriving in a place nobody looked for it because the arm appeared to have already won that fight.

## What this changes

**Direct pull's case is now conditional on virtual threads**, and stated that way it is much stronger:
on JDK 21 it is the fastest thing this project has; below JDK 21 it is roughly the shipped engine at
2ms and worse at 100ms. That is a far clearer recommendation than "3.2x at ten workers, unusable at
five thousand".

**It also sharpens the load-factor case.** The shipped engine reaches **543-768 of a configured 5,000**
in flight at 2ms. Direct pull with virtual threads holds 5,000. The gap is not the user function or
the broker - it is the buffer machinery failing to keep workers fed. See
[`next-starvation-is-the-signal-not-queue-depth.md`](next-starvation-is-the-signal-not-queue-depth.md).

## Not settled

- **Why every arm is far below the theoretical ceiling at 100ms.** 5,000 in flight at 100ms per record
  is 50,000 msg/s; the best arm reaches 18,376. Something upstream of the engine - fetch rate, poll
  cadence, the commit path - bounds all of them, and no arm has been profiled at that operating point.
- **One partition only.** Every figure here is single-partition; the poll path is per-partition and may
  well be the ceiling above.
- **`core-dpvt` is not an option users can set by name.** It is two independent booleans, and nothing
  documents that they are better together or validates the combination.


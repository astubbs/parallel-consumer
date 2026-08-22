# Latency is measured now; below saturation is what is still missing

<!-- inflight-type: task -->
<!-- inflight-impact: test-debt -->
<!-- inflight-labels: needs-measurement -->

**Supersedes `next-the-harness-cannot-measure-latency-yet.md`, whose central premise was wrong and
whose remaining half is the only thing still open.** That note said latency could not be measured
because the dataset is produced before the run, so every record shares one arrival instant. Antony's
correction on 2026-08-22: measure from **when the record comes out of the consumer** to **when
Parallel Consumer finishes with it**. Produce time and broker wait are the environment, not the
engine. No producer change is needed, and that measure now exists as `pc.record.residence.time` and as
the `residence_*` columns in `bench/run-bisect.sh`.

**What is still true, and it is the harder half: every run this harness does is a saturated backlog
drain, and at 100% utilisation residence measures the backlog rather than the engine.**

## The measurement that proved it, rather than argued it

The head-of-line experiment - `PARTITION` ordering against `KEY` ordering, same engine, same broker,
same records, same handler, same seed, both arms at `maxConcurrency` 24 over 24 partitions, a tailed
handler with one record in a hundred at 1,000ms against a 20ms median:

| ordering | handler | msg/s | peak in flight | residence p99 |
|---|---|---:|---:|---:|
| `PARTITION` | flat | 694.4 / 684.9 | 24 | 11,273 / 11,273 |
| `KEY` | flat | 712.9 / 720.2 | 24 | 10,733 / 10,733 |
| `PARTITION` | tailed | 531.6 | 24 | 15,568 |
| `KEY` | tailed | 535.9 / 539.4 | 24 | 15,565 / 15,565 |

**The two orderings are indistinguishable - 0.02% apart under the tail.** The head-of-line prediction
is refuted at this operating point, and the reason is arithmetic rather than a fact about ordering:
under a saturated backlog, residence is buffered depth over throughput. Both arms hold the same
buffer and have the same throughput ceiling (24 over the mean work), so their residence is equal *by
construction*. A 1,000ms slow record is 6% of a 15,568ms p99 and cannot be seen underneath it.

## And the buffer cannot simply be made shallower

`PARTITION` ordering only reaches its 24-way parallelism when the buffer is deep enough to hold a
record for every partition - about `max.poll.records` x partitions, 12,000 here. Below that it starves:
at `messageBufferSize` 240 it holds **2** records in flight and manages 37-39 msg/s.
See [`perf-partition-ordering-starves-on-a-narrow-buffer.md`](perf-partition-ordering-starves-on-a-narrow-buffer.md).

**So the two requirements are in direct conflict: deep enough to run is too deep to measure.** There is
no buffer setting at which `PARTITION` both achieves its parallelism and has a queue shallow enough for
a slow record to be visible. This is not a tuning problem to be swept; it is why the experiment cannot
be done as the harness stands.

## What would settle it

**`BENCH_ARRIVAL_RATE` - produce during the measured window at a configured rate, and sweep it.** At
60-70% of measured throughput the queue is short, so residence is dominated by service time and
queueing *caused by the engine*, which is exactly where head-of-line blocking lives. That axis was
already proposed in the note this replaces; what is new is that it is now **necessary rather than
nicer**, and that there is a measurement showing why.

Note the one thing it does NOT need: the arrival timestamp still comes from the `WorkContainer`, not
from `ConsumerRecord#timestamp()`. Producing during the run is needed to control **utilisation**, not
to define arrival.

## The corroboration that already exists, from the one arm whose queue is shallow

`vanilla` (a plain consumer, strictly serial) against `pool` (a plain consumer and a thread pool), same
records, same delay, both measuring residence as poll-return to completion: **p50 900ms against 18ms,
p99 1,848ms against 32ms.** A serial consumer's residence is fifty times a concurrent one's on
identical work, and that gap is visible precisely because neither arm buffers deeply. It is the same
phenomenon the `PARTITION` experiment was built to isolate, seen where the backlog does not drown it -
though it confounds ordering with concurrency (`vanilla` runs one record at a time in total, not one
per partition) and so cannot stand in for the experiment.

## Still open, and deliberately not claimed

- **The head-of-line argument is neither confirmed nor refuted.** What was refuted is the specific
  prediction at a saturated operating point, where the measurement cannot see the effect either way.
  Anything written about Streams' latency cost must not cite these rows as support *or* as a
  counter-example.
- The percentile axis for `residence` comes through Micrometer's histogram and is good to about 3%;
  `drain` is exact. Immaterial next to the effects above, material if the effect being chased is small.

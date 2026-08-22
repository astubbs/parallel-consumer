# THE TAIL EXPERIMENT: one run that settles Share Groups, Kafka Streams, and PC's actual value

<!-- inflight-type: next -->
<!-- inflight-impact: performance -->
<!-- inflight-labels: needs-measurement -->

> **IT RAN, 2026-08-22 - results and predictions in
> [`perf-the-tail-experiment-ran-2026-08-22.md`](perf-the-tail-experiment-ran-2026-08-22.md).** The
> blocker below is removed: `BENCH_ARRIVAL_RATE` feeds records during the measured window, and
> `BENCH_KEY_DISTRIBUTION` gives the run a workload in which `KEY` ordering constrains anything at
> all. **What separated the arms was not the tail.** A 101x handler tail moves throughput by a few
> percent for every arm, as prediction 1 said it would; the key distribution moves it by 3.1x and a
> 1% failure rate by 40%. This note is kept for its reasoning and its predictions, which the results
> note grades one by one. Delete it when the follow-on work it names is either done or has its own
> note.

**Antony's instruction, 2026-08-22: make sure we do this.** It is one sweep, and it answers three
separate open questions that have each been argued rather than measured.

## Why one experiment covers all three

Every engine in this comparison has a **blocking unit** - the amount of work that one slow record
holds up. That is the whole argument, and nothing here has ever measured it, because **every figure
this project has taken uses a CONSTANT per-record delay**, and constant work is the best possible case
for a serial design: with no variance there is no head of line to block.

| Arm | Blocking unit | Consequence for a tailed handler |
|---|---|---|
| `PARTITION` ordering, N partitions | **a partition** - this is Kafka Streams' execution model, one thread per task | Everything behind a slow record waits, until the backlog drains |
| `share` | **a batch** - neither acknowledgement mode may poll with records unacknowledged | Poll cadence set by each batch's MAXIMUM, not its mean |
| `KEY` / `UNORDERED` on PC | **one record** - other keys proceed | Only records for the same key wait |

**So the same sweep produces the Kafka Streams argument, the Share Groups qualifier, and PC's actual
differentiator.** Three notes currently carry these as reasoning; this replaces all three with one
table.

## What it needs, and what is already built

| Piece | State |
|---|---|
| Tailed work model - `BENCH_DELAY_P99`, `BENCH_DELAY_STDDEV`, `BENCH_FAILURE_RATE`, seeded | **Built** 2026-08-22, off by default |
| The `share` arm and a Kafka 4.3.1 broker | **Built** 2026-08-22, merged |
| Latency percentiles from record residence time | **Built** - `pc.record.residence.time`, and the `residence_*` and `drain_*` columns in `bench/run-bisect.sh`. It was named the blocker here; it is not, see below |

It could not have run before residence time landed, because throughput barely moves - the point of a
tailed handler is what it does to the *distribution*, and a mean hides it completely. Reporting msg/s
here would produce a null result from an experiment that had not been run. **That reasoning was right
and the blocker it named was wrong**, which the next section is about.

## BLOCKED, and not on what this note first said - measured 2026-08-22

**The blocker is not residence time. That landed. The blocker is that a saturated backlog drain cannot
show a tail at all**, and this note as first written would have produced a confident null result.

Measured on `perf/record-residence-time`: `PARTITION` and `KEY`, both held at 24 records in flight
over 24 partitions, handler p99 at 50x its median. **Residence p99 15,568ms against 15,565ms** -
indistinguishable.

**The reason is arithmetic, not a fact about ordering.** Every run this harness does drains a backlog
produced before the run began, so residence is *buffered depth over throughput*. Both arms have the
same buffer and the same throughput ceiling, so their residence is equal **by construction**. A
1,000ms slow record is 6% of a 15,568ms p99 and cannot surface.

**And the buffer cannot simply be made shallower**: `PARTITION` only reaches its parallelism when the
buffer covers every partition - about `max.poll.records x partitions`. **Deep enough to run is too
deep to measure.**

### What it actually needs: arrival below saturation

Records fed in **during** the measured window at a controlled rate, swept - 50%, 70%, 90% of the
throughput already measured. At 100% utilisation a queueing system measures its backlog; below it, the
engine.

This was raised on 2026-08-22, dropped when it was correctly pointed out that residence time is
well-defined without producing during the test, and is now **re-established for a different reason**.
Both are true and they are not the same claim:

- **Defining** latency needs no producer change - residence is poll-to-completion, which works
  against a pre-produced dataset.
- **Discriminating** between engines on tail latency needs the system unsaturated, which does need
  controlled arrival.

**So the ordering is: arrival-rate control first, then this experiment.** Running it before then
produces a null result from an experiment that was never performed - the exact failure this note
exists to prevent, arrived at from an angle it did not anticipate.

### One arm DID show the effect, because its queue is shallow

The measurement is not entirely empty-handed. `vanilla` (a plain consumer, strictly serial) against
`pool` (the same consumer with a thread pool), same records, same delay, both reporting residence
measured the same way - poll-return to completion, in `Bench` itself: **p50 900ms against 18ms, p99
1,848ms against 32ms.** A serial consumer's residence is fifty times a concurrent one's on identical
work, and it is visible precisely because neither arm buffers deeply.

**It is not a substitute for the experiment** - `vanilla` runs one record at a time in *total*, not one
per partition, so it confounds ordering with concurrency, and its handler is flat. What it does show is
that the residence measure resolves this class of effect perfectly well when the backlog is not
drowning it, which is the strongest available evidence that the null above is the operating point's
fault rather than the instrument's.

### A second finding, which bounds the `PARTITION` arm of this experiment

`PARTITION` ordering does not reach its own parallelism at default settings - 2 to 6 records in flight
over 24 partitions, with a **flat** handler. See
[`bug-partition-ordering-starves-on-a-narrow-buffer.md`](bug-partition-ordering-starves-on-a-narrow-buffer.md).
Until that is fixed, **the `PARTITION` arm of this run measures that defect and would report it as the
cost of serial execution.** Set `messageBufferSize` well above `max.poll.records x partitions`, and
check `peak_in_flight` against the partition count before believing any `PARTITION` row.

### The predictions below are NOT refuted

They were tested under a condition that cannot express them. **Prediction 4's failure would have meant
the argument this project is building is wrong - it did fail, and it does not mean that**, because the
measurement could not have shown a difference had one existed. Leave them stated; re-run when the
harness can answer.

## The run

- **Arms**: `share`, `core-vt` in `UNORDERED`, `core-vt` in `KEY`, `core-vt` in `PARTITION`.
- **Partitions**: several, not one. `PARTITION` mode on a single partition is strictly serial and
  simply times out - it has already done so once.
- **Handler**: `BENCH_DELAY_P99` at 50-100x the median. One record in a hundred that is slow is the
  honest model; a normal curve understates it.
- **Also run the flat control** - same arms, `BENCH_DELAY_P99` unset. **The comparison is between the
  two runs**, not within either. A tail that costs an engine nothing shows up only as the difference.
- **Report percentiles** - p50, p99, p99.9, max - for both residence and backlog-drain latency, per
  the two-measures warning in the residence work.
- One partition count, one concurrency, both handlers. Small.

## Predictions, stated in advance

Written before the run so they can be wrong:

1. **Throughput barely moves for any arm.** The tail is 1% of records; a mean absorbs it. If throughput
   moves a lot, something other than the tail is being measured.
2. **`PARTITION` p99 degrades worst** - a slow record delays every record behind it in its partition,
   so the cost is inherited by the whole queue rather than paid once.
3. **`share` degrades second**, and by roughly the batch size relative to `PARTITION`'s partition
   depth - its blocking unit is smaller but still far larger than one record.
4. **`KEY` and `UNORDERED` stay closest to the handler's own distribution** - only same-key records
   wait, and under `UNORDERED` nothing does.
5. **The ordering between 2 and 3 could invert** if PC's batch-level behaviour matters more than
   expected. That would be the interesting result, and it is why `share` and `PARTITION` are both in
   the same run rather than compared across sweeps.

**If prediction 4 fails, the argument this project has been building all day is wrong**, and that
matters more than any of the throughput numbers taken today.

## What it settles

- [`next-what-kafka-streams-on-pc-is-worth.md`](next-what-kafka-streams-on-pc-is-worth.md) - head-of-line
  blocking is currently its central argument and is unmeasured.
- [`next-what-survives-share-groups.md`](next-what-survives-share-groups.md) - "Share Groups have
  head-of-line blocking too, at batch granularity" is currently reasoning from the API contract.
- [`perf-share-groups-versus-pc-2026-08-22.md`](perf-share-groups-versus-pc-2026-08-22.md) - its 2.5x
  is a flat-handler result and says so; this is the other half.

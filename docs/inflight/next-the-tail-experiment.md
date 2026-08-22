# THE TAIL EXPERIMENT: one run that settles Share Groups, Kafka Streams, and PC's actual value

<!-- inflight-type: next -->
<!-- inflight-impact: performance -->
<!-- inflight-labels: needs-measurement -->

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
| Latency percentiles from record residence time | **In progress** - `perf/record-residence-time`. **This is the blocker** |

**It cannot run until residence time lands**, because throughput will barely move - the point of a
tailed handler is what it does to the *distribution*, and a mean hides it completely. Reporting msg/s
here would produce a null result from an experiment that had not been run.

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

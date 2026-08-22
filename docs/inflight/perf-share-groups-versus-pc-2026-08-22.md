# Share groups beat Parallel Consumer on this workload by 2.5x, and cost the broker 5x for it - measured 2026-08-22

<!-- inflight-type: next -->
<!-- inflight-impact: strategy -->
<!-- inflight-labels: needs-decision -->

**Say it first, because burying it would make every other number in this repository less
trustworthy.** On the workload this harness measures, a bare `KafkaShareConsumer` loop with **no
Parallel Consumer in it at all** processes 100,000 records at **~66,000 msg/s** where PC's best arm
manages **~27,000** and the shipped default manages **~17,500**. Same broker, same topic, same bytes,
same simulated work per record, same machine, same minute.

This replaces the *position* `STRATEGY.md` holds against share groups with a *number*, which is worth
more - and the number goes against us.

**And it inverts at a longer delay: at 100ms PC wins by 14.5%.** The 2.5x is a per-record-overhead
result, not a general one - it appears where framework cost dominates the work and vanishes where the
work dominates the framework. **Neither row may be quoted without the other.** Read
[What it does NOT show](#what-it-does-not-show) before quoting any of this.

## The comparison

100,000 records, one partition, `UNORDERED`, `maxConcurrency` 5,000, 2ms simulated work,
`kafka-clients` 4.3.1 for every arm, JDK 21, LOCAL build, timer callee, one-minute load 5-8
throughout. Round-robin: every arm ran once per round, so a load excursion hits all of them in the
same round rather than landing on one.

| Arm | What it is | msg/s (median of 4) | peak in flight | broker CPU us/record |
|---|---|---:|---:|---:|
| **`share`** implicit | `KafkaShareConsumer`, **no PC** | **67,898** | 2,520 | **14.88** |
| **`share-explicit`** | `KafkaShareConsumer`, **no PC** | **66,524** | 2,520 | **15.82** |
| `core-vt` | PC core on virtual threads | 27,289 | 4,604 | 3.22 |
| `core-dpvt` | PC direct pull + virtual threads | 26,396 | **5,000** | - |
| `core` | PC shipped default | 17,780 | 420 | 3.14 |
| `pool` | no engine - the Java client floor | 17,152 | 256 | 2.89 |

**`share` is 2.5x `core-vt`, PC's best arm, and 3.8x the shipped default.** Load 6.8-10.0 for every
row; each arm ran once per round, four rounds, medians reported.

**`share` against `core-vt` is the like-for-like row Antony asked for**: both deliver records to
whichever worker is free, and neither promises per-key order.

**There is no table putting share groups beside `KEY` mode, deliberately.** Share groups have no
ordering guarantee at all, so such a row would compare a system that orders against one that does
not, and flatter share groups for doing strictly less work. `KEY` ordering is a **capability share
groups lack**, not a race they win.

## Why - and it is not the callee, and not concurrency

The arm reports where its wall clock goes. Ten consecutive runs on a quiet machine, all processing
the full dataset:

```
polls=43  emptyPolls=1  maxBatch=2606  records=100000
pollMs~1290   joinMs~166   ackMs=0     totalMs~1480
```

**87% of the time is inside `poll()`** and almost none of it is the simulated work. So the share arm
is bounded by the fetch-plus-acknowledge round trip - and at 2ms per record that round trip is still
cheaper than what PC spends per record on its work manager, shard map and offset encoding.

**PC holds MORE concurrency and gets LESS throughput.** `core-vt` reaches 5,000 records in flight
against the share arm's 2,520, and still finishes later. That is the cleanest statement of the
result: this is a per-record overhead gap, not a parallelism gap.

**The one confound runs in share groups' favour, which is why the loss is robust.** `core` and `pool`
do their simulated work as a `Thread.sleep` that holds a worker thread; the share arm's callee is a
timer that holds none. The advantage was share groups', and PC lost anyway.

## The two acknowledgement modes cost the same

| Mode | `share.acknowledgement.mode` | msg/s (median) | client-side ack cost |
|---|---|---:|---|
| `share` | `implicit` (default) | 67,898 | `ackMs=0` - it rides the next fetch |
| `share-explicit` | `explicit` | 66,524 | `ackMs` ~0 too - `acknowledge()` only records intent locally |

**Both were measured, because the choice looked like it might be the whole result. It is not.**

**Neither mode can poll ahead of completion**, and that is the structural difference from PC.
Explicit forbids it outright - the client throws `IllegalStateException` if you poll with records
unacknowledged. Implicit permits it and acknowledges records that have not been processed, which is
at-most-once delivery wearing an at-least-once label. So an honest share processor is
batch-synchronous: poll, run the batch, finish it, poll again. **PC keeps records from many polls
outstanding at once, and that is what its offset encoding buys.**

## At 100ms the result INVERTS, and PC wins - the prediction held

Stated before the run, from the in-flight ceilings alone: at a long delay throughput is bounded by
records-in-flight over delay, the share arm's ceiling is its batch (~2,606) and PC's is its
configured 5,000 - so PC should win at 100ms even though it loses at 2ms.

| Arm | msg/s (median of 3) | peak in flight | ceiling implied by peak |
|---|---:|---:|---:|
| `core-dpvt` | **18,328** | **5,000** | 50,000 |
| `core-vt` | **18,275** | **5,000** | 50,000 |
| `share-explicit` | 16,098 | 2,606 | 26,060 |
| `share` | 15,957 | 2,606 | 26,060 |
| `core` shipped default | 12,943 | 3,053 | 30,530 |
| `pool` | 11,950 | 2,877 | 28,770 |

**PC's best arm beats share groups by 14.5% at 100ms.** So the 2.5x at 2ms is not a general result,
it is a *per-record-overhead* result: it appears where the framework cost dominates the work, and it
disappears where the work dominates the framework. **Which operating point a reader cares about
decides which of these two rows is the answer**, and neither one may be quoted alone.

**And it is the harness's own concurrency limit that decides it.** Share groups cannot be asked for
more than `share.partition.max.record.locks` per share-partition, so on one partition the arm cannot
reach 5,000 in flight however it is configured. On a topic with more partitions that ceiling rises
and this row would move - **untested, and the most obvious next measurement.**

### The `vertx` row taken alongside these is void, and the harness now refuses it

`vertx` was added to this campaign to get a fully callee-matched comparison - it and the share arms
were the two arms sharing `Bench#callCallee`. It cannot be: the Vert.x engine issues its own HTTP
request through `vertxHttpReqInfo`, so under `BENCH_TIMER_CALLEE` there is no server, every request
fails, and the arm STILL prints a plausible 17,221 msg/s because the engine's `onResponse` fires on
failures. The only tell was `peak_in_flight` = 0.

It also drove the machine's load from 12 to 44 while spinning at 190% CPU, contaminating the other
arms in the same round. `run-bisect.sh` now refuses the combination. **A callee-matched share-versus-
engine comparison still has not been taken** - it needs `BENCH_ASYNC_STUB=1` for both sides.

## The throughput win is bought with broker CPU - about 5x of it

**No results file in this repository has ever recorded what the broker was doing**, and this is the
arm that makes that gap matter: share groups acknowledge per record into the share coordinator's
`__share_group_state` topic, where PC batches the same information into one encoded offset commit.
None of that appears in a consumer-side msg/s figure.

`bench/broker-load.sh` differences the broker container's cumulative cgroup CPU across one run.
**`pool` is the control**: a plain consumer with `enable.auto.commit=false` that never commits
anything, so its cost is pure fetch and whatever each other arm spends ABOVE it is its
acknowledgement design and nothing else.

| Arm | msg/s | broker CPU us/record | **above the fetch floor** | durable write |
|---|---:|---:|---:|---|
| `pool` - fetch only, commits nothing | 17,734 | 2.89 | *(the floor)* | - |
| `core` - PC batched encoded commit | 17,918 | 3.14 | **0.25** | below `du` granularity |
| `core-vt` | 26,983 | 3.22 | **0.33** | below `du` granularity |
| `share` implicit | 69,204 | 14.88 | **11.99** | +16 KB `__share_group_state` |
| `share-explicit` | 66,225 | 15.82 | **12.93** | +16 KB across two topics |

**Share-group acknowledgement costs the broker roughly 48x what PC's batched commit costs**
(11.99 vs 0.25 us per record), and about 5x the total per-record broker CPU. **This is the honest
counterweight to the headline**, and it is exactly the cost that a throughput table cannot show: the
consumer got 2.5x faster and the broker got 5x more expensive per record, so the win is real but it
is not free, and it is paid by shared infrastructure rather than by the application that gained it.

**Bounded**: one broker, one partition, one client, and `du` resolves to 4 KB so the log-growth
column is coarse. The CPU column is cumulative microseconds and is not coarse.

## The broker upgrade did NOT move PC's numbers - and the first answer said it did

**This is the finding that had to be checked before anything else could be reported**, because every
figure in this repository was taken on `apache/kafka:3.9.0` and a share-groups number from a 4.3.1
broker compared against those is exactly the like-for-like error this repository has published once
already.

**First answer, wrong**: `core-vt` read 22,341 msg/s against the 3.9.0 broker and 26,954 against
4.3.1 - a 20% gain attributable to nothing but the broker version, repeatable across five rounds.

**The control that killed it**: `ps -Ao pcpu,etime,args -r | grep '[B]ench '` showed **another
session's sweep running against the same 3.9.0 container**, which is shared. The 4.3.1 broker was
private to this campaign; the 3.9.0 one was not. So the comparison was not two Kafka versions, it was
a contended broker against an uncontended one.

**Re-run with a private 3.9.0 broker on its own port, everything else identical**, the gap
disappears:

| Arm | 3.9.0, private broker | 4.3.1, private broker | difference |
|---|---:|---:|---:|
| `core` | 18,215 | 17,780 | -2.4% |
| `core-vt` | 26,948 | 27,289 | +1.3% |
| `core-dpvt` | 26,632 | 26,396 | -0.9% |
| `pool` | 16,844 | 17,152 | +1.8% |

**Every arm is within 2.4%, and the sign is not consistent** - two arms up, two down. That is noise,
not a broker effect. **The 100ms campaign says the same**: `core-vt` 18,258 against 18,275 (+0.1%),
`core-dpvt` 18,152 against 18,328 (+1.0%), `core` 12,614 against 12,943 (+2.6%), `pool` 12,015 against
11,950 (-0.5%). Two operating points, eight comparisons, no consistent direction.

**So no existing figure in this repository needs re-stating for the broker version.** They need
re-stating for who else was using the broker, which is a different and much older problem -
`bench/README.md` already documents it and this is another instance.

## Re-taken with a key distribution and a failure rate, 2026-08-23

**The 2.5x was measured on `UNORDERED`, all-distinct keys, one partition, a 2ms constant handler and a
zero failure rate - share groups' best case and Parallel Consumer's worst**, which is the audit in
[`next-benchmark-a-model-of-work-not-work.md`](next-benchmark-a-model-of-work-not-work.md). This
section re-takes it with the two axes it never had.

**`UNORDERED` stays, and that is the fair reading rather than a convenience.** Share groups have no
ordering guarantee at all, so a `KEY` row beside them would compare a system that orders against one
that does not, and flatter share groups for doing strictly less work. What changes is the key
distribution, the failure rate and the partition count - and all three apply to both sides.

### One method note before any number, because it cost a whole pass

**The share arms need `kafka-clients` 4.3.1 pinned explicitly, and under `CLIENT_PINS=NATIVE` they do
not compile.** This fork's transitive client is 3.9.2, which has no `KafkaShareConsumer` at all, so a
sweep that forgets the pin records `COMPILE_FAILED` for every share row - correctly, loudly, and only
after the whole matrix has run. The published campaign pinned 4.3.1 for **every** arm precisely so
that the client is not a free variable between the two sides, and the re-take does the same.

**Its PC rows are therefore NOT comparable with the ones in
[`perf-engine-comparison-2026-08-22.md`](perf-engine-comparison-2026-08-22.md)'s re-take**, which run
at `NATIVE`. Two rows that disagree about the client are two experiments; they live in
[`bench/results/realistic-share-groups-matrix.csv`](../../bench/results/realistic-share-groups-matrix.csv)
rather than in the throughput matrix for that reason.

### The structural finding, which is not a number and does not depend on one

**In its default implicit acknowledgement mode a share consumer cannot model a failure at all.**
`poll()` acknowledges the whole previous batch, so by the time processing has failed the record has
already been acknowledged and can never be redelivered. An implicit-mode processor that wants
at-least-once has to retry **in process**, holding the batch open - and a share consumer cannot poll
while a batch is outstanding, so retrying holds the entire fetch pipeline.

The harness refuses the combination rather than reporting a number for it, which is why the tables
below carry `share-explicit` in every failure cell and `share` in none. **That refusal is the
finding**: of the two acknowledgement modes, only one can be compared against PC on a workload where
records fail, and it is not the default.

**And the comparison that remains still runs in share groups' favour.** `share-explicit` answers a
failure with `RELEASE`, and a released record is immediately re-acquirable, where PC waits out
`defaultMessageRetryDelay` - one second - before re-offering it. So at the same configured failure
rate the two arms are not paying the same retry cost, and **no row putting them side by side may be
quoted without saying so.**

## What it does NOT show

- **One partition, all-distinct keys, one broker, one machine, one delay.** The 2ms operating point
  is where per-record overhead dominates, which is the point that flatters the cheaper client. At
  100ms both converge on `records x delay / concurrency` and the gap should shrink or invert - and
  share groups' in-flight ceiling is the batch, so the arm holding 2,520 against PC's 5,000 should
  cost it there.
- **No ordering, no retries, no failure injection, no rebalance.** Share groups' delivery-count
  limit, acquisition-lock expiry and redelivery behaviour are untested here, and they are where the
  per-record acknowledgement design has to earn its keep.
- **Share groups have no `KEY` ordering and no equivalent of it.** That is the differentiator this
  measurement leaves entirely intact.
- **The in-flight ceiling is the broker's, not the caller's.** `max.poll.records` does nothing for a
  share consumer - set to 100 it left the batch at 2,606 - so a share consumer's concurrency is
  `share.partition.max.record.locks` (group config, default 2,000) per share-partition. On one
  partition the arm cannot be asked for more.
- **`msg_per_sec` is not load-robust on this machine and `peak_in_flight` is.** The share arm read
  15,446 and 68,400 for the identical row during the first campaign; the slow mode never appeared at
  load below about 8. Every figure above carries the load it was taken under.

## What this obliges

**`STRATEGY.md` currently claims** that batching acknowledgement locally should give *lower*
per-record overhead than acknowledging per message to the broker. On this workload the measurement
contradicts it consumer-side. The claim should be restated as what the evidence supports, or
withdrawn.

The honest positioning that survives: **PC's case against share groups is `KEY` ordering, retries and
offset semantics - not throughput.** That is the same shape as the already-recorded finding that
`core` is within 1% of a hand-rolled consumer-plus-pool.

## Related

- [`perf-engine-comparison-2026-08-22.md`](perf-engine-comparison-2026-08-22.md) - the arms, and the
  figures this campaign re-took.
- `bench/README.md`, "The share-groups arms" - how to run it, and the three non-obvious things about
  a `KafkaShareConsumer`.
- [`next-franz-go-as-a-client-option.md`](next-franz-go-as-a-client-option.md) - `franz-go` v1.21.0 is
  the only Go client with share-group support, if a Go-side share arm is ever wanted. This supersedes
  `next-share-groups-as-a-bench-arm.md`, which proposed the arm now built.

## Still open

- **More than one partition.** Share groups' in-flight ceiling is per share-partition, so the 100ms
  row - the one PC wins - is set by a limit that rises with partition count. This is the single most
  obvious next measurement and it could move the only operating point PC currently wins.
- **A callee-matched engine row.** `core`/`core-vt`/`pool` do their simulated work as an inline
  blocking sleep, while `share` uses `Bench#callCallee`. The obvious matched arm, `vertx`, turned out
  to have no timer form at all (see above), so this comparison still needs taking with
  `BENCH_ASYNC_STUB=1` on both sides.
- **Nothing about retries, redelivery, lock expiry or rebalance**, which is where per-record
  acknowledgement should cost share groups something and where PC's offset encoding is hardest to beat.

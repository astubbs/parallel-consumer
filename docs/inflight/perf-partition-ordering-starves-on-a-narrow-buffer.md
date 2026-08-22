# PARTITION ordering runs at a fraction of its partitions, and the buffer is why

<!-- inflight-type: bug -->
<!-- inflight-impact: throughput -->

**`PARTITION` ordering over 24 partitions holds 2 to 6 records in flight, not 24.** Measured
2026-08-22 with the residence timer, which is what made it legible: the records are fetched, they are
sitting in the shards, and the engine is not running them.

Conditions, identical for every row: `core`, 10,000 records, 24 partitions, `maxConcurrency` 24, base
delay 20ms, `max.poll.records` 500, seed 42, LOCAL build, two repeats, one-minute load 3.5-6.0.

| ordering | messageBufferSize | msg/s | peak in flight | residence p50 | residence p99 |
|---|---:|---:|---:|---:|---:|
| `PARTITION` | default | 46.9 (one repeat timed out) | **3** | 9,663 | 36,506 |
| `KEY` | default | 707.0 / 706.0 | **24** | 770 / 1,206 | 2,682 / 3,353 |
| `PARTITION` | **20,000** | **694.4 / 684.9** | **24** | 5,636 / 5,905 | 11,273 / 11,273 |
| `KEY` | 20,000 | 712.9 / 720.2 | 24 | 5,365 / 5,365 | 10,733 / 10,733 |

**A flat handler throughout - there is no tail here, and therefore no head-of-line blocking to blame.
This is dispatch, and it costs 15x.**

## The control arm, and the prediction it tested

Stated before running: `PARTITION` ordering can only run a partition it is **holding a record for**.
PC's prefetch target is counted in **records** (`maxConcurrency * loadFactor`), not in shards, and the
Java client answers a poll largely from one partition at a time - so the buffer is deep but **narrow**,
covering a few of the 24 partitions, and one record per covered partition is all the engine can offer.
Predicted: force a 20,000-record buffer and coverage widens, taking in-flight from ~3 to ~24 with
throughput rising in proportion.

**It held, on the one term changed: 46.9 msg/s at 3 in flight becomes 694.4 at 24 - 14.8x - and lands
within 3% of `KEY` on the same settings.** Nothing else moved.

## Why this is a defect rather than a tuning note

An operator choosing `PARTITION` ordering is choosing Kafka Streams' concurrency model deliberately -
one record at a time per partition, N partitions in parallel. What they get instead is one record at a
time across **the two or three partitions the fetch buffer happens to hold**, which is nearer serial
than parallel, and no setting they would think to reach for says so. `messageBufferSize` is documented
as a buffering knob, not as the thing that decides whether ordered parallelism happens at all.

**The buffer needed scales as `max.poll.records` x partitions** - roughly 12,000 records here - because
that is what it takes for the poll history to have touched every partition. A hundred-partition
assignment would need five times that. The fix is not a bigger default: it is that the prefetch target
should be expressed in terms the ordering mode actually consumes - **shard coverage** - rather than a
flat record count that means completely different things under `KEY` and under `PARTITION`.

## What this costs the comparison it was found in

**Every PC-versus-Streams argument this project wants to make runs through `PARTITION` ordering**, on
the reasoning in `docs/inflight/next-what-kafka-streams-on-pc-is-worth.md` that `PARTITION` *is* the Streams
execution model. That equivalence does not hold today: Streams gives each task its own consumer and
its own fetch, so it has no coverage problem, while PC's `PARTITION` arm is starved by one. A
comparison drawn now measures this defect and reports it as the cost of serial execution.

**Anything measured with `PARTITION` ordering before this is fixed needs `peak_in_flight` read first.**
A row with in-flight far below the partition count is a starved run, whatever else it says.

## Not yet done

- No fix. The diagnosis is the buffer's shape; the design question - whether the load factor should
  target shard coverage, and what it should do when shards outnumber the concurrency budget - is open.
- Not established whether the same starvation reaches `KEY` ordering on a **skewed** key distribution,
  where a few shards hold most records. The measurements here use all-distinct keys, which is the best
  possible case for `KEY` and says nothing about the worst.
- The direct-pull engine takes work from the shards itself and may not share this; unmeasured.

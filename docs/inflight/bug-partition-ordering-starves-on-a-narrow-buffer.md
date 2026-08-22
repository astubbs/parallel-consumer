# PARTITION ordering runs at a fraction of its partitions, and the buffer is why

<!-- inflight-type: bug -->
<!-- inflight-impact: throughput -->

**You configure `maxConcurrency` 24 over 24 partitions and get 2 to 6.** Throughput is 37-68 msg/s
where the same settings under `KEY` ordering give 532-723 - **an order of magnitude, with a uniform
handler and nothing in the user function to blame.** No option a user would think to reach for says
so, and the only outward sign is that the machine is idle.

Measured 2026-08-22 with the residence timer, which is what made it legible: the records are fetched,
they are sitting in the shards, and the engine is not running them. That distinction - not fetched
versus fetched and queued - is the one thing no other meter could draw.

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

## `messageBufferSize` is a WORKAROUND, and the fix is not built

**Setting `messageBufferSize` high enough recovers the throughput - it is not the fix, because a user
cannot be expected to derive the number.** The buffer needed scales as `max.poll.records` x
partitions, roughly 12,000 here; a hundred-partition assignment would need five times that, and
neither figure is written down anywhere or checked by anything. Set it too low and the symptom is
silent idleness, which is exactly where this started. It is also not free: that buffer is records held
in memory and, per [`next-the-tail-experiment.md`](next-the-tail-experiment.md),
seconds of added residence time.

**The fix is that the prefetch target should be expressed in terms the ordering mode actually
consumes - shard coverage - rather than a flat record count that means completely different things
under `KEY` and under `PARTITION`.** Under `KEY` with distinct keys, N records is N runnable shards;
under `PARTITION`, N records may be one. **None of that is built**, and the design question it turns
on is open: what the target should do when shards outnumber the concurrency budget, and whether
coverage should drive the fetch or only the selection.

## What this costs the comparison it was found in

**Every PC-versus-Streams argument this project wants to make runs through `PARTITION` ordering**, on
the reasoning in `docs/inflight/next-what-kafka-streams-on-pc-is-worth.md` that `PARTITION` *is* the Streams
execution model. That equivalence does not hold today: Streams gives each task its own consumer and
its own fetch, so it has no coverage problem, while PC's `PARTITION` arm is starved by one. A
comparison drawn now measures this defect and reports it as the cost of serial execution.

**Anything measured with `PARTITION` ordering before this is fixed needs `peak_in_flight` read first.**
A row with in-flight far below the partition count is a starved run, whatever else it says.

## Not yet done

- ~~Not established whether the same starvation reaches `KEY` ordering on a **skewed** key
  distribution.~~ **ESTABLISHED 2026-08-22, and it is worse than this note guessed** - see
  [`perf-the-tail-experiment-ran-2026-08-22.md`](perf-the-tail-experiment-ran-2026-08-22.md). On a
  Zipf distribution over 200 keys, `KEY` ordering sustains **1 record in flight of a configured 24**
  and runs at a third of `UNORDERED` on the identical records, with a flat handler and no failures.
  It is that a hot key is a serial queue whatever the buffer holds, so the ceiling is set by the
  busiest shard rather than by the fetch. `peak_in_flight` reads 24 throughout and cannot see it,
  which is why the harness gained an `inflight_p50` column.
- ~~**It is not the buffer**: `messageBufferSize` was already 20,000, the fix this note
  prescribes.~~ **Half right, and the half that was wrong is this note's own subject.** The tail
  experiment was run at 20,000, so the hot-key floor it found is real and is not a buffer effect.
  But **PC's DEFAULT buffer costs another 2.3x on top of it**, on the ordered arm only. One term
  changed, `core`, 12,000 records, 24 partitions, `maxConcurrency` 24, Zipf over 200 keys, flat
  handler, no failures:

  | `messageBufferSize` | `KEY` msg/s | `UNORDERED` msg/s |
  |---|---:|---:|
  | 20,000 | **367.8** | 1,227.0 |
  | PC's default | **162.2** | 1,221.2 |

  `UNORDERED` does not move - 1,227.0 against 1,221.2 - which is what makes the ordered figure
  attributable. **So the starvation this note describes reaches `KEY` on a skewed distribution after
  all**, and the note's own prescription is what the tail experiment had already applied. A user who
  sets nothing gets the 162.2 row.
- ~~The direct-pull engine takes work from the shards itself and may not share this; unmeasured.~~
  **MEASURED 2026-08-23: it shares it exactly.** On the same Zipf workload at `messageBufferSize`
  20,000, `core` reads 370.9 msg/s at 2 sustained in flight, `core-vt` 362.6 at 2, and `core-dpvt`
  **362.9 at 2**. Taking work from the shards directly does not help, because the constraint is not
  how work is fetched or selected - it is that the busiest shard may only run one record at a time.

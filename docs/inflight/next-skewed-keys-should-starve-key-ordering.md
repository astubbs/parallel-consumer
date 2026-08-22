# A skewed key distribution should starve `KEY` ordering the way a narrow buffer starves `PARTITION`

<!-- inflight-type: next -->
<!-- inflight-impact: performance -->
<!-- inflight-labels: needs-measurement -->

**Written 2026-08-22, BEFORE the run that tests it**, so that it can be wrong. It is the sixth
prediction attached to [`next-the-tail-experiment.md`](next-the-tail-experiment.md), and the only one
about the axis that experiment did not previously have: `BENCH_KEY_DISTRIBUTION`.

## The claim

**`KEY` ordering's effective parallelism is the number of distinct keys with runnable records in the
buffer, not the number of records in it.** Every measurement this project has ever taken used
all-distinct keys, where those two quantities are equal - so the distinction has never been visible,
and `KEY` has behaved indistinguishably from `UNORDERED` in every table.

Under a skewed distribution they come apart. A shard under `KEY` ordering runs **one record at a
time**; a hot key is therefore a serial queue however many of its records are buffered. Under
Zipf(s=1) over N keys the top key takes roughly `1 / ln(N)` of the traffic - about 19% at N=200 - and
the top ten take over half. A buffer holding 500 records of that stream does not hold 500 runnable
shards; it holds a few hundred keys of which a handful carry most of the depth.

**This is the same defect as
[`bug-partition-ordering-starves-on-a-narrow-buffer.md`](bug-partition-ordering-starves-on-a-narrow-buffer.md),
reached from the other side.** That note found `PARTITION` running 3 records in flight against a
configured 24, because PC's prefetch target is counted in RECORDS and a partition can only contribute
one runnable record at a time. It states in its own "Not yet done" section that whether the same
starvation reaches `KEY` under a skewed distribution is **unestablished**. The mechanism is identical:
a record count is a poor proxy for a shard count whenever the ordering mode consumes shards.

## What should be observed, in order of how much it would cost to be wrong

1. **`KEY` peak in flight falls below `maxConcurrency`, and falls further as the skew rises.** This is
   the load-bearing one. With 24 configured and a hot key carrying a fifth of the traffic, expect the
   engine to be unable to find 24 runnable records at some points in the run even while the buffer is
   deep. All-distinct keys at the same operating point should reach 24.
2. **`KEY`'s advantage over `PARTITION` shrinks, and the shrinkage is the interesting number.** Both
   remain ordered, both remain sharded, and `KEY` still has far more shards than `PARTITION` has
   partitions - so the rank should hold. What should not survive is the MAGNITUDE: under distinct
   keys `KEY` has effectively no ordering constraint at all, so the distinct-key gap is an upper
   bound on `KEY`'s advantage rather than an estimate of it.
3. **`KEY`'s residence and end-to-end p99 rise relative to `UNORDERED` on the same records.** Under
   distinct keys the two are the same workload wearing two names; under skew, `UNORDERED` is
   unconstrained and `KEY` is not, so this is the first measurement in which the two should separate
   at all. **If they do not separate, `BENCH_KEY_DISTRIBUTION` did not do what it claims** and the
   `KEYDIST` receipt line is the first thing to read.
4. **A tail and a skew should compound rather than add.** A slow record on a hot key blocks that key's
   whole queue, and that queue is a fifth of the stream. Under distinct keys a slow record blocks
   exactly one record - itself. So `KEY`'s tail sensitivity should be far higher under skew than the
   distinct-key runs suggest.

## What would refute it

- `KEY` reaching 24 in flight under skew, with residence indistinguishable from `UNORDERED`. That
  would mean PC's shard selection finds runnable work well enough that key concentration does not
  reach the dispatch path, and the prefetch-target criticism in the `PARTITION` bug note is narrower
  than it is currently written.
- `KEY` degrading as badly as `PARTITION`. 200 keys over 24 partitions is an order of magnitude more
  shards, so a `KEY` arm that matched `PARTITION` would mean shard COUNT is not what decides, and the
  cost is somewhere else - most likely the scan.

## Why it matters beyond this run

**`KEY` ordering is the reason to adopt Parallel Consumer.** `UNORDERED` is where a bare
`KafkaShareConsumer` beats PC's best arm 2.5x
([`perf-share-groups-versus-pc-2026-08-22.md`](perf-share-groups-versus-pc-2026-08-22.md)) and a plain
thread pool is within 1%. If `KEY` costs materially more than the all-distinct numbers imply, then
every published figure understates the price of the one feature that is not available elsewhere - and
the fix named in the `PARTITION` bug note (express the prefetch target in shard coverage rather than
in records) stops being a `PARTITION` fix and becomes a core one.

# The UNORDERED dispatch scan re-walks the whole in-flight prefix on every pass

<!-- inflight-type: parked -->
<!-- inflight-impact: performance -->
<!-- inflight-labels: needs-measurement -->

Measured 2026-08-22 with `DispatchScanMeter`, which counts entries
`ProcessingShard#getWorkIfAvailable` examines. **Counts, not timings** - the numbers below are the same
on a loaded machine and an idle one, which is the whole reason the meter exists (see
`OrderingModeDispatchParityTest` for the day of wrong conclusions that preceded it).

## The numbers

20,000 records, taken 500 at a time, **never completed** - so the in-flight set only grows:

| Mode | Entries examined | Per record |
|---|---:|---:|
| `KEY` | 20,000 | **1.0** |
| `UNORDERED` | 410,000 | **20.5** |

`KEY` gives each distinct key its own shard, and the scan leaves after the head, so it is exactly one
examination per record - flat, regardless of how many records there are.

`UNORDERED` shards by topic-partition, so one shard holds every in-flight record, and **each pass
restarts from the beginning of it**: pass *k* walks past the `(k-1) x BATCH` records already taken
before reaching new ones. That is `BATCH x (1 + 2 + ... + PASSES)`, which is exactly 410,000 here.

## The ratio is not a constant, and quoting "20.5x" without its workload is wrong

```
ratio = (PASSES + 1) / 2      where PASSES = RECORDS / BATCH
```

So it is **half the number of passes**, and it grows without bound as the record count rises against a
fixed batch size. At 20,000/500 it is 20.5; at 200,000/500 it would be 200.5. Any statement of the form
"UNORDERED dispatch costs N times KEY" is meaningless without both constants.

## What this does and does not mean in production - the important qualifier

**The test's in-flight set grows monotonically because it never completes a record. Production does
not do that.** In a running consumer the in-flight prefix is bounded by `maxConcurrency`, not by the
size of the topic, so the rescan cost per pass is bounded by concurrency and does not grow with how
much data has been consumed.

That reframes it rather than dismissing it. **The cost scales with `maxConcurrency`** - which is
precisely the regime this performance session cares about. At `maxConcurrency` 5,000 every dispatch
pass walks up to 5,000 entries to find selectable ones, so the examinations per record dispatched are
roughly `in-flight / batch`. That is a real per-record CPU cost that rises as concurrency rises, on the
control thread, which is the thread that also polls and commits.

**Whether it is material has been measured twice, and both times it was not:**

- `perf/split-shard-inflight` - split shard state into selectable and in-flight so the scan stops
  seeing in-flight records. **10x cheaper dispatch, 0% end to end.**
- `perf/resume-shard-scan` - resume the scan instead of restarting it. **+0.2%.**

Both branches carry their measurement. The reason the end-to-end number does not move is that at any
realistic per-record handler delay the dispatch scan is a rounding error next to the handler, and the
end-to-end benchmark puts the two ordering modes **0.9% apart**.

## So why keep this open at all

Because the two measurements above were taken at operating points that do not include the one the
thread-ceiling work has since made interesting: **a near-zero handler delay with very high
concurrency**, where there is no handler time for the scan to hide behind. The virtual-threads result
(3.0x at 2ms/5,000) puts the engine in exactly that regime for the first time. Nobody has re-measured
dispatch cost there.

**The experiment**: `bench/run-bisect.sh` at 0ms and 2ms handler delay, concurrency 5,000, `UNORDERED`
against `KEY`, with `perf/split-shard-inflight` as the treatment arm and its base as the control. If
the split still buys 0% at that operating point, this is closed for good and the branch can be
deleted. If it buys something, it becomes a candidate rather than a curiosity.

**Do not re-derive the counts by timing.** Read them off `DispatchScanMeter`; the whole point is that
they are deterministic.

## Not a defect - and this is the part that gets forgotten

In-flight records remaining in the shard is **how ordering is enforced**. The scan has to see them to
know the shard is blocked. `perf/split-shard-inflight` found this by construction: removing them from
the scan's view breaks ordering unless the blocked-shard state is tracked separately, which is the
cost that made a 10x cheaper dispatch worth 0%. Anyone approaching this as "the scan is quadratic,
obviously fix it" will rediscover that.

See also: [`perf-platform-threads-are-the-ceiling.md`](perf-platform-threads-are-the-ceiling.md),
[`perf-virtual-threads-measured.md`](perf-virtual-threads-measured.md),
[`next-direct-pull-unordered-selection.md`](next-direct-pull-unordered-selection.md).

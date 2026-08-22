# UNORDERED: a record leaves the shard when TAKEN - built, measured, and it buys no throughput

<!-- inflight-type: feature -->
<!-- inflight-impact: refactor -->

> **SCOPE: `UNORDERED` ONLY.** The ordered modes examine **exactly one entry per record dispatched**
> (measured, `OrderingModeDispatchParityTest`), because the break fires after the head. Their blocked
> state is implied by a record's presence in the shard, so changing it trades a working guarantee for
> nothing. The ordered path is untouched and must stay that way.

**Built on `perf/unordered-available-queue`, off `research/market-analysis-recut`, 2026-08-22.** This
note used to be the design; it is now the result. The question it asked was Antony's: *is the shard
required under `UNORDERED`, and is indexing around it the wrong fix?* Both halves are answered.

## What was built

Under `UNORDERED`, `ProcessingShard`'s entry map now holds only records the shard **can offer**. A
record leaves when it is claimed and re-enters when its delivery lands. There is therefore no
in-flight prefix for a scan to cross, and **`ShardOccupancy` - the index of unheld offsets that the
scan used to walk instead - is deleted.** The two per-mode walks collapse back into one that differs
only in whether the ordered `break` fires.

## The result, stated first: identical throughput, one structure fewer

**This was a maintainability change and it behaved like one.** Both arms, same machine, same broker,
same dataset, 100,000 records, one partition, `UNORDERED`, concurrency 5,000, `BENCH_ASYNC_STUB=1`,
two repeats x two client pins, `bench/run-bisect.sh`:

| Arm | 2ms base | 2ms after | 100ms base | 100ms after |
|---|---:|---:|---:|---:|
| **`core-dpvt`** | **25,757** | **25,689** (-0.3%) | **18,512** | **18,582** (+0.4%) |
| `core-vt` | 22,451 | 23,351 | 16,462 | 15,578 |
| `core-dp` | 19,928 | 18,757 | 11,792 | 11,997 |
| `core` shipped | 16,407 | 17,747 | 12,514 | 12,573 |

**Only the `core-dpvt` row is worth reading.** It is the only arm that holds a flat 5,000 in flight
at both delays, so it is the only one whose figure is not partly a report on how much concurrency it
managed to reach that minute - and it reproduces
[`perf-direct-pull-plus-virtual-threads.md`](perf-direct-pull-plus-virtual-threads.md)'s recorded
25,874 / 18,376 to within 0.7% in **both** arms, which is the evidence that the instrument was sound
on the day. The other three scatter by 5-25% *within* a single sweep (`core-dp` run 1 against run 2:
14,489 and 21,730), and the machine was shared: load 12 rising to 137 across the base sweep, 14
rising to 246 across the treatment sweep, with a spot reading of 622 mid-run. **Do not read a
difference out of those rows.** In particular `core`'s apparent +8% at 2ms is not a finding.

**The counting instrument, which load cannot move**, `DirectPullScanCostMeasurementTest`,
examinations per shard entry per record dispatched:

| scanners x in flight | before either change | `ShardOccupancy` | departure-on-take |
|---|---:|---:|---:|
| 1 x 5,000 | 440.13 | 1.03 | 1.04 |
| 5,000 x 5,000 | 1,621.89 | 1.60 - 5.54 | 1.68 |

The single-scanner arm is deterministic and the two designs are indistinguishable on it. The
many-scanner arm is not deterministic - it counts re-examinations caused by lost claims, so it moves
with machine load: the same base build measured 1.60 when `ShardOccupancy` landed and **5.54** on a
re-run here. Quote it as a range or not at all.

`OrderingModeDispatchParityTest`'s `EXPECTED_UNORDERED_EXAMINATIONS` is **unchanged**, which is the
whole claim in one number: same cost, one fewer structure holding the same facts.

## The constraint that would have sunk a naive version, and the measurement of it

`UNORDERED` needs only "give me any available record", so the container looks free to be any bag. It
is not. **PC commits the lowest incomplete offset and encodes the incompletes above it, so the commit
payload is sized by the SPREAD of outstanding offsets** - against a hard broker metadata ceiling -
and a wider spread also means more replay after a crash. A plain FIFO keeps the happy path, because
records enter in poll order, and **breaks the retry path**: a failed record appended to the tail is
re-offered behind everything that arrived while it was out.

So re-entry is by offset - a keyed insert into the same sorted map - not an append. **Measured with a
throwaway tail-append control arm**, on a 2,000-record run at concurrency 50 with every seventh record
failing once, the widest gap between the highest succeeded offset and the committable frontier - which
is exactly the range the encoder pays for:

| re-entry | widest encoded offset range |
|---|---:|
| by offset (base, and after) | **49** - one less than the concurrency, the floor |
| tail append (control arm) | **2,000** - the entire batch |

`UnorderedRetryOffsetOrderTest` pins it, and **passes unchanged on the branch point**, which is the
point: this design was required to preserve a property, not to add one. Under the control arm its two
retry tests went red and its no-failures test stayed green - the split that says a throughput
benchmark would never have caught this.

## What it cost, which is the part to weigh

**A record out at a worker is no longer in the shard, so the epoch sweep at revocation cannot reach
it.** Two consequences, both now asserted by `WorkManagerTest`'s conservation walk:

- The conservation figure keeps counting a revoked-but-live delivery until it lands, and **the landing
  retires it**. That is later than before. It is the safe direction - the figure gates broker intake,
  so counting a live delivery fetches less rather than more - but it is a real change to when a number
  moves, and the note it moves for is `confluentinc#857`-shaped.
- **A stale landing is now a departure in its own right.** Without that, a revoked record would be put
  back into the offerable set after the sweep that should have taken it, where the scan could meet it -
  the `confluentinc#909` hazard.

**And the honest accounting of "one structure fewer":** `ShardOccupancy` held a `ConcurrentSkipListSet`
of offsets that duplicated a subset of the entry map, plus the in-flight `LongAdder`. What replaces it
is the `LongAdder` alone, restored where it was before. So the duplicated collection is gone and
nothing takes its place - but `RecordPopulation`'s retirement rule gained a second site (the stale
landing above), and `getCountOfWorkTracked()` now has a mode branch. **It is a smaller net win than
"delete a class" suggests.**

## The verdict, and what is still open

**Take it or leave it on maintainability grounds; there is no performance argument either way, and
that was the prediction.** The tree is smaller, the unordered scan has no index to keep in step, and
the offset-spread property is now pinned by a test that did not exist before. Against that, the
revocation path acquired a timing change that needed two test rewrites to state.

Still open, and deliberately not built here - one change, measured:

- The **selectable-shard queue** ([`next-selectable-shard-queue.md`](next-selectable-shard-queue.md))
  and **retry selection from `RetryQueue`**
  ([`next-select-retries-from-the-retry-queue.md`](next-select-retries-from-the-retry-queue.md)).
- **The per-shard first-claimable cursor is now moot**, not merely unbuilt: it indexed around a prefix
  that no longer exists. Do not build it.
- **Whether the ordered modes want anything like this. They do not** - see the scope box.

See also: [`perf-unordered-dispatch-rescans-the-inflight-prefix.md`](perf-unordered-dispatch-rescans-the-inflight-prefix.md),
[`perf-direct-pull-collapse-is-the-scan.md`](perf-direct-pull-collapse-is-the-scan.md),
[`next-open-items-from-the-perf-session.md`](next-open-items-from-the-perf-session.md).

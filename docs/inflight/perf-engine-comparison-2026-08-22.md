# The engine comparison, measured 2026-08-22

<!-- inflight-type: next -->
<!-- inflight-impact: performance -->
<!-- inflight-labels: needs-measurement -->

**The first cross-engine measurement this project has taken.** Every previous claim about the engine
family rested on Vert.x plus the assumption that a shared `ExternalEngine` superclass made the others
behave alike. It does not entirely, and the differences are not where they were expected.

**Conditions**, identical for every row: 100,000 records, one partition, `UNORDERED`, concurrency
5,000, `max.poll.records` 500, LOCAL build, native kafka-clients, JDK 21, twelve cores, two repeats.
Figures are the mean of the two.

## With a NON-BLOCKING callee - the only fair reading for the async engines

| Arm | 2ms msg/s | peak in flight | 100ms msg/s | peak in flight |
|---|---:|---:|---:|---:|
| **`core-vt`** virtual threads | **25,934** | **5000** | 17,830 | **5000** |
| **`proxy`** | **25,615** | 4,831 | 15,615 | **5000** |
| `core` shipped default | 18,546 | 446 | 12,960 | 2,824 |
| `reactor` | 17,840 | 4,444 | 15,945 | **5000** |
| `vertx` | 17,364 | 1,492 | 15,925 | **5000** |
| `mutiny` | 17,049 | 2,025 | 12,472 | **5000** |
| `core-dp` direct pull | **could not complete a run** - see below | | | |

## With a BLOCKING callee - correct for core, MEANINGLESS for the async engines

Kept because it is the comparison that was nearly published, and because the difference is the point.

| Arm | 2ms msg/s | peak | 100ms msg/s | peak |
|---|---:|---:|---:|---:|
| `llingr` (not PC) | 30,308 | 1,100-1,645 | 19,571 | 5000 |
| `core-vt` | 26,582 | 5000 | 18,034 | 5000 |
| `pool` hand-rolled | 18,103 | 214-282 | 11,586 | 3,025 |
| `core` | 17,918 | 357-380 | 12,572 | 3,526 |
| `proxy` | 16,897 | 390-422 | 12,231 | 2,943 |
| `vertx` | 15,533 | 357 | 11,440 | 3,042 |
| `reactor` | 12,194 | 317-319 | 9,023 | 2,460 |
| `mutiny` | **5,745** | **177-180** | 6,686 | 1,825 |
| `core-dp` | 1,357, one run never finished | 3,131 | neither run finished | |
| `vanilla`, `franz` | skipped - serial, projected 200s and 10,000s | | | |

## What the numbers say

**1. Virtual threads are the only PC arm that reaches `maxConcurrency` on a blocking callee** -
5000/5000, where the shipped engine tops out at 2,824-3,526. That is the platform-thread ceiling
measured directly rather than argued, and it is worth **1.4x** at both delays.

**2. A blocking callee makes the async engines meaningless, and by a factor of three.** `mutiny` reads
5,745 msg/s at 180 in flight blocking, and 17,049 at 2,025 with a timer callee. The blocking rows were
nearly published as an engine comparison. The harness now **refuses** that combination
(`BENCH_ALLOW_BLOCKING_ENGINE=1` overrides).

**3. At 100ms every async engine holds 5,000 in flight and converges on ~16,000 msg/s**, beating the
shipped default's 12,960 by about 23% - which is the shape `ExternalEngine` predicts, finally observed.

**4. `proxy` is the surprise: within 1% of virtual threads at 2ms.** That is the path **every non-JVM
client takes**, so its ceiling is theirs. **Bounded**: the arm drives the engine in-process across the
`DispatchSink`/`report` seam, where production funnels every report through one serialised inbound
callback per session. Upper bound, not a wire number.

**5. `core` is within 1% of a hand-rolled consumer-plus-pool** (18,546 vs 18,103 at 2ms). PC's value on
this workload is offset management and ordering, not throughput. Worth saying plainly.

**6. llingr leads, and the honest gap is smaller than it looks.** 30,308 against `core-vt`'s 26,582 at
2ms - **14%** - but it holds 1,100-1,645 in flight against 5,000. More throughput from a third of the
concurrency is a real result. At 100ms it is 19,571 against 18,034, **8%**.

**The llingr number is UNCONTROLLED and must not be quoted.** llingr reaches Kafka through franz-go,
PC through the Java client, and the `franz` control arm was skipped as serial at these record counts.
Without that floor there is no way to attribute the 14% between engine and client. **Run `franz` at a
small record count before this figure is used for anything.**

## Direct pull, re-measured after the scan fix - it works

`ShardOccupancy` (branch `perf/direct-pull-scan-collapse`) only. **None of the structural designs
proposed on 2026-08-22 are built** - not the `UNORDERED` available-queue, not the selectable-shard
queue, not retry selection, not the manager thread. This measures the scan fix alone, deliberately, so
the result is attributable.

100,000 records, one partition, `UNORDERED`, two repeats, load 3-5 throughout:

| delay | conc | `core` | `core-dp` | |
|---:|---:|---:|---:|---|
| 0ms | 10 | 12,398 / 12,435 | **27,655 / 27,770** | **2.23x** |
| 0ms | 100 | 23,010 / 23,052 | **27,747 / 27,556** | **1.20x** |
| 0ms | 1,000 | 25,113 / 24,931 | 23,702 / 26,853 | parity |
| 0ms | 5,000 | 24,272 / 24,771 | 25,641 / 23,535 | parity |
| 2ms | 100 | 15,533 / 15,470 | 16,367 / 16,324 | 1.05x |
| 2ms | 1,000 | 22,129 / 22,031 | 22,548 / 23,272 | 1.03x |
| 2ms | 5,000 | 16,725 / 16,556 | 16,281 / 22,051 | noisy |

**Both acceptance criteria met.** At concurrency 5,000 the arm went from 1,357 msg/s with runs that
never finished, to parity with the shipped engine; and it kept its low-concurrency win rather than
trading it away.

**The in-flight column is the more interesting result.** At 5,000 / 0ms `core` peaks at **173-239
records in flight** against a configured 5,000, while `core-dp` reaches **2,268-5,000**; at
concurrency 1,000 direct pull holds a flat 1,000 where `core` manages 299-713. **Direct pull is now
the arm that achieves the configured concurrency, and the shipped engine's buffer machinery is what
fails to keep up** - which is `DynamicLoadFactor` showing up as a measurement rather than an argument.
See [`next-starvation-is-the-signal-not-queue-depth.md`](next-starvation-is-the-signal-not-queue-depth.md).

**A harness limitation, not a product one**: the `2ms / concurrency 10` cell timed out at 60s for
**both** arms. 100,000 records at 2ms with 10-way concurrency is ~20s theoretical and nearer 60s with
per-record overhead, so the flat `RUN_TIMEOUT` is too tight there. It should scale with projected
runtime the way the serial-arm guard already does. That cell is missing, not failed.

**Still outstanding on that branch**: `CoreBatchTest.simpleBatchTest` fails 3-of-3 under direct pull
against a 0-of-5 baseline - all records delivered once, no batch over the limit, but four selectors
where the test assumes one.

## The released-upstream arm, and what "last public release" actually means

**0.5.3.3 is genuinely public** - the jar resolves from Maven Central (HTTP 200) and Central's own
metadata lists it as both `<latest>` and `<release>`. 0.5.3.2 and 0.5.3.1 are there too. So a
comparison against the last thing a user could actually depend on is a real comparison, not a
comparison against a tag.

**Worth noting against how upstream is described elsewhere**: 0.5.3.3 shipped in **August 2025**, a
year after 0.5.3.2. The README states upstream is unmaintained, and that remains true of the project's
direction, but a release did land later than most summaries assume. Anything written about upstream's
last activity should use this date.

**Which modes may be swept at a released version, and which must not.** `core-dp` and `core-vt` select
themselves with `-Dpc.directPull=true` and `-Dpc.virtualThreads=true`. **A released version does not
have those options and will silently ignore the property**, producing a row labelled `core-dp` that is
a plain `core` run. That is the same silent-duplicate failure the `prepare()` cache defect already
produced once in this harness. **Sweep only `core` and `vertx` at released versions.**

## Re-taken on a Kafka 4.3.1 broker - the figures hold

The share-groups campaign re-took `core`, `core-vt`, `core-dpvt` and `pool` on **both** a 3.9.0 and a
4.3.1 broker, each private to that campaign, with `kafka-clients` 4.3.1 for every arm. **Every arm
came back within 2.4% between the two brokers, with the sign inconsistent** - so nothing in this
document needs re-stating for the broker version, and a share-groups number taken on 4.2+ may be
compared against these.

**One correction it did produce**: the first attempt read `core-vt` 20% faster on 4.3.1, which was
another session's sweep sharing the 3.9.0 container rather than a Kafka version effect. Details, and
the 2.5x result the campaign was actually for:
[`perf-share-groups-versus-pc-2026-08-22.md`](perf-share-groups-versus-pc-2026-08-22.md).

## Re-taken on a realistic workload, 2026-08-23 - the ranking above is a ranking of `UNORDERED`

**Read this before quoting anything above it.** Every row in this document was taken on one
partition, all-distinct keys, `UNORDERED`, a constant handler and a zero failure rate. On all-distinct
keys `KEY` ordering constrains nothing - every record is its own shard - so the table ranks engines on
the one workload in which Parallel Consumer has no differentiator. That is the audit in
[`next-benchmark-a-model-of-work-not-work.md`](next-benchmark-a-model-of-work-not-work.md), and this
section is the re-take it asked for. **The figures above are not wrong. They are answers to a
narrower question than they read as.**

### Conditions

**Matrix A, the ordering matrix**: 12,000 records, **24 partitions**, 10ms handler, `maxConcurrency`
24, `messageBufferSize` 20,000, `max.poll.records` 500, non-blocking (timer) callee, Kafka 4.3.1
broker, JDK 21, two repeats, seed 42. Keys swept `distinct` and `zipf` (200 keys, exponent 1.0, top
key 16.6%); failures swept 0 and 1%; ordering swept `KEY` and `UNORDERED` inside one invocation so the
arms alternate. Data:
[`bench/results/realistic-ordering-matrix.csv`](../../bench/results/realistic-ordering-matrix.csv).

**`maxConcurrency` 24 rather than 5,000, and the reason is arithmetic.** `KEY` on a skewed
distribution is bounded by the hot key, which runs serially however wide the engine is, so its
throughput barely moves with concurrency while `UNORDERED`'s rises with it. At `maxConcurrency` 200
the span between the two ends of the matrix is 9x and no single record count gives both a usable
`UNORDERED` window and a finishable `KEY` one. **This is therefore not a re-take of the concurrency
question**; it is a measurement of what ordering costs. The concurrency question is answered at its
own operating point below.

### Every engine pays the same 3.1-3.3x, and none of them escapes it

Zipf keys, flat handler, no failures. `msg/s` (sustained records in flight, of a configured 24):

| arm | `UNORDERED` | `KEY` | cost of `KEY` | `KEY` in flight |
|---|---:|---:|---:|---:|
| `mutiny` | 1,289.3 | **410.2** | 3.14x | 2 |
| `reactor` | 1,271.6 | **407.9** | 3.12x | 2 |
| `vertx` | 1,267.8 | **407.9** | 3.11x | 2 |
| `core` | 1,232.3 | **370.9** | 3.32x | 2 |
| `core` **@ 0.5.3.3** | 1,227.8 | **369.7** | 3.32x | 2 |
| `core-vt` | 1,160.7 | **362.6** | 3.20x | 2 |
| `core-dpvt` | 1,155.3 | **361.6** | 3.19x | 2 |
| **`proxy`** | 796.9 | **78.0** | **10.2x** | **0** |

**Virtual threads do not help, direct pull does not help, and neither does moving to an
`ExternalEngine`.** Point 1 above - virtual threads are worth 1.4x - is a statement about the
platform-thread ceiling, and at `maxConcurrency` 24 there is no ceiling to lift: `core-vt` is 6%
*slower* than `core` here. Point 3 - the async engines converge ~23% above the shipped default at
100ms - survives in sign but not in magnitude on this workload.

**The distinct-key control is the row that ties this table to the one above it.** Same arms, same
everything, keys distinct: `core` 1,217.2 `KEY` against 1,224.3 `UNORDERED`, 0.5.3.3 1,210.8 against
1,223.4, and every engine within 1.5% of itself across the two modes. **On that workload the ordering
axis has no result to find**, which is exactly why nobody found one.

### `proxy` is the arm that breaks, and it is the one every non-JVM client uses

**78.0 msg/s at a residence p99 of 139,586ms, sustaining ZERO records in flight** - 4.8x worse than
`core` on the identical workload, where on distinct keys it is only 1.5x worse (803.5 against 1,217.2).
Point 4 above reads *"`proxy` is the surprise: within 1% of virtual threads at 2ms"*, and that holds
at its own operating point. **On a skewed keyed workload the same path is the worst arm measured by a
factor of five.**

That matters more than a benchmark row usually does, because
[`astubbs#242`](https://github.com/astubbs/parallel-consumer/issues/242)'s language proxies reach PC
through `ProxyProcessor` and through nothing else - so this is the ceiling for **every** non-JVM
client on the workload those clients are being offered for. The mechanism is not established here;
the arm drives the engine in-process across the `DispatchSink`/`report` seam, where production
funnels every report through one serialised inbound callback per session, and a workload that
produces one runnable record at a time is the worst possible case for a serialised report path.
**Unattributed, and the single most important thing this re-take found that was not being looked
for.**

### Matrix B - the published operating point, reproduced and then given the two missing axes

**First the reproduction, because without it nothing below is attributable.** Same dataset the table
at the top of this note used - 100,000 records, **one partition**, 2ms, `maxConcurrency` 5,000,
`UNORDERED`, all-distinct keys, no failures, non-blocking callee - re-run 2026-08-23:

| Arm | published 2026-08-22 | re-taken 2026-08-23 | drift |
|---|---:|---:|---:|
| `proxy` | 25,615 | **26,721** | +4.3% |
| `core-vt` | 25,934 | **26,368** | +1.7% |
| `core-dpvt` | 26,396 * | **26,089** | -1.2% |
| `core` | 18,546 | **18,634** | +0.5% |
| `vertx` | 17,364 | **18,497** | +6.5% |
| `reactor` | 17,840 | 13,045 ** | - |
| `mutiny` | 17,049 | **17,291** | +1.4% |

\* from the share-groups campaign. \*\* `reactor`'s two repeats disagreed by 75%; not a comparison.

**Five of six arms are within 7% and the ranking is unchanged, so the machine has not moved.** Any
difference in the tables below is the workload.

### The two axes, at 24 partitions

`UNORDERED` throughout, `msg/s` (sustained in flight), two repeats, load 6-14:

| Arm | distinct, no failures | **Zipf**, no failures | distinct, **1% failures** | **Zipf, 1% failures** |
|---|---:|---:|---:|---:|
| `proxy` | 26,582 | 26,075 | 16,526 | 16,529 |
| `core-vt` | 26,490 | 26,393 | 17,064 | 16,942 |
| `core-dpvt` | **25,861** | 25,850 | **12,616** | 13,596 |
| `vertx` | 18,135 | 18,465 | *void* | *void* |
| `reactor` | 17,953 | 14,123 ! | 10,211 ! | 12,508 |
| `core` | 17,588 | 16,967 | 13,010 | 13,476 |
| `core` **@ 0.5.3.3** | 17,685 | 17,649 | 13,593 | 12,925 |
| `mutiny` | 16,882 | 12,456 ! | 12,520 | 7,888 ! |

`!` = the two repeats disagreed by more than 15%; those cells are noise, not results. `vertx`'s
failure cells are **void** - the arm counts a failed record as a completed one, see the harness NOTE
it now prints.

**The key-distribution columns are a control and they behave like one.** Under `UNORDERED` there are
no shards to contend for, so a skewed key distribution should cost nothing - and it costs nothing:
`core` @0.5.3.3 reads 17,649 against 17,685, `core-vt` 26,393 against 26,490, `core-dpvt` 25,850
against 25,861, `proxy` 26,075 against 26,582. **That is what makes the 3.3x in matrix A attributable
to ordering rather than to the dataset.**

### The failure rate is what reorders this table, and it takes the winner out

**`core-dpvt` loses half its throughput to a 1% failure rate** - 25,861 to 12,616 - which drops the
fastest arm below the shipped default. The others lose 26-38%:

| Arm | flat | 1% failures | cost |
|---|---:|---:|---:|
| `core` | 17,588 | 13,010 | -26% |
| `core-vt` | 26,490 | 17,064 | -36% |
| `proxy` | 26,582 | 16,526 | -38% |
| **`core-dpvt`** | **25,861** | **12,616** | **-51%** |

**That bears directly on P4 of the v6 announcement** - *"a direct-pull engine, opt-in - fastest
configuration measured when paired with virtual threads"*. It is the fastest configuration measured
**on a workload where nothing fails**, and it is the slowest of the four when 1% of records do.
Unattributed; the obvious suspect is how the direct-pull worker pool interacts with the retry queue,
and nothing here isolates it.

### What survives from the original table, point by point

| # | Claim | Verdict on a realistic workload |
|---|---|---|
| 1 | Virtual threads are the only PC arm that reaches `maxConcurrency` on a blocking callee, worth **1.4x** | **Holds at its own operating point, worth nothing at this one.** It removes a platform-thread ceiling; a hot shard is not that ceiling |
| 2 | A blocking callee makes the async engines meaningless, by 3x | **Untouched** - a harness property, not a workload one, and the guard that enforces it still stands |
| 3 | At 100ms every async engine holds 5,000 in flight and beats the default by ~23% | **Untested here** and untouched; this matrix runs at `maxConcurrency` 24 |
| 4 | `proxy` is within 1% of virtual threads | **Holds on distinct keys, fails badly on skewed ones** - see above |
| 5 | `core` is within 1% of a hand-rolled consumer-plus-pool | **Narrow, and the audit already said so**: a thread pool cannot do `KEY` ordering at all, so on this workload the comparison has no counterpart |
| 6 | llingr leads by 14%, uncontrolled | **Unchanged and still must not be quoted** |

## Caveats that bound all of it

- **One partition, all-distinct keys, one broker, one machine.** A best case for any key-sharded
  design, and `next-performance-regression-testing.md` records the key-distribution axis as missing.
- **`msg_per_sec` is not load-robust and `peak_in_flight` is** - see `bench/README.md`. Load was 4-13
  for the async sweep and rose above 800 during the direct-pull rows, which is why those are reported
  as "did not complete" rather than as a number.
- **`core-dp` predates the scan fix.** `ShardOccupancy` on `perf/direct-pull-scan-collapse` takes
  dispatch from 440 examinations per record to 1.00; the end-to-end re-measure has not been taken.


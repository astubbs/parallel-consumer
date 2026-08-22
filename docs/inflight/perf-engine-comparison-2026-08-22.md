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

## Caveats that bound all of it

- **One partition, all-distinct keys, one broker, one machine.** A best case for any key-sharded
  design, and `next-performance-regression-testing.md` records the key-distribution axis as missing.
- **`msg_per_sec` is not load-robust and `peak_in_flight` is** - see `bench/README.md`. Load was 4-13
  for the async sweep and rose above 800 during the direct-pull rows, which is why those are reported
  as "did not complete" rather than as a number.
- **`core-dp` predates the scan fix.** `ShardOccupancy` on `perf/direct-pull-scan-collapse` takes
  dispatch from 440 examinations per record to 1.00; the end-to-end re-measure has not been taken.


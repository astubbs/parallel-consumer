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

## Caveats that bound all of it

- **One partition, all-distinct keys, one broker, one machine.** A best case for any key-sharded
  design, and `next-performance-regression-testing.md` records the key-distribution axis as missing.
- **`msg_per_sec` is not load-robust and `peak_in_flight` is** - see `bench/README.md`. Load was 4-13
  for the async sweep and rose above 800 during the direct-pull rows, which is why those are reported
  as "did not complete" rather than as a number.
- **`core-dp` predates the scan fix.** `ShardOccupancy` on `perf/direct-pull-scan-collapse` takes
  dispatch from 440 examinations per record to 1.00; the end-to-end re-measure has not been taken.


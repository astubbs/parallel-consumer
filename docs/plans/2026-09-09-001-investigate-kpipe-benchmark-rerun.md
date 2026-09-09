# KPipe's benchmark, rerun with Parallel Consumer's concurrency turned up

**Question:** KPipe's README claims 6.6x Parallel Consumer's throughput at 10 ms of work per
record and 41x at 100 ms. Its harness pins Parallel Consumer 0.5.3.3 at `maxConcurrency(100)`.
Is the gap the setting or the engine?

**Answer, measured 2026-09-09:** the setting. At `maxConcurrency(2000)` the same artifact beats
KPipe at 10 ms in both orderings, and at 100 ms sits at 91 percent of its own configured ceiling.
The published 100-worker numbers reproduced on this box to within 15 percent, so the environment
is comparable to the author's.

## Method

KPipe's own JMH harness, unmodified except for the `CONFLUENT_MAX_CONCURRENCY` constant in
`ParallelProcessingBenchmarkInfrastructure.java`, run twice from the same clone at KPipe commit
`baf19ac` (2026-09-08): once at 2000, once at 100 as the control. `run.sh` beside this document is
the script; the two JSON files are JMH's raw output.

| Setting | Value |
|---|---|
| Arms | `kpipe` (PARALLEL), `kpipeKeyOrdered`, `confluent` (UNORDERED), `confluentKey` (KEY) |
| Work per record | 10 ms and 100 ms, `LockSupport.parkNanos` |
| Records per invocation | 25,000, the harness default |
| JMH | 2 forks, 2 warmup, 3 measurement iterations, throughput mode |
| Parallel Consumer | 0.5.3.3, the abandoned upstream artifact KPipe pins, not this fork |
| Broker | `apache/kafka:4.3.0` via Testcontainers, per trial |
| JVM | GraalVM CE 25.0.2 from mise; Graal JIT, not C2, for every arm |
| Processors | **8**, pinned by the box's `JAVA_TOOL_OPTIONS -XX:ActiveProcessorCount=8`, for every arm |
| Wall clock | 2000 pass 16 min, 100 pass 21 min |

## Results, records per second, JMH 99.9 percent error in brackets

| Arm | 10 ms | 100 ms |
|---|---:|---:|
| PC UNORDERED, 2000 workers | 101,349 (3.3%) | 18,269 (0.3%) |
| PC UNORDERED, 100 workers | 9,875 (0.1%) | 998 (0.02%) |
| KPipe PARALLEL | 58,389 (0.7%) | 47,881 (2.7%) |
| PC KEY, 2000 workers | 61,088 (1.2%) | 9,585 (0.2%) |
| PC KEY, 100 workers | 9,871 (0.2%) | 997 (0.9%) |
| KPipe KEY_ORDERED | 37,438 (0.3%) | 5,403 (0.1%) |

KPipe's rows are from the 2000 pass; the 100 pass reproduced them within 1 percent.

## Ratios

| Comparison | 10 ms | 100 ms |
|---|---:|---:|
| KPipe PARALLEL over PC UNORDERED at 100 workers, KPipe's published cell | 5.9x | 48x |
| KPipe PARALLEL over PC UNORDERED at 2000 workers | **0.58x** | 2.6x |
| KPipe KEY_ORDERED over PC KEY at 100 workers | 3.8x | 5.4x |
| KPipe KEY_ORDERED over PC KEY at 2000 workers | **0.61x** | **0.56x** |

KPipe's published 2026-07-21 capture reports 6.6x and 41.3x for the first row and 4.0x and 5.4x for
the third; both reproduce here, which is the control that makes the other two rows believable.

## What it establishes

- **The 100-worker gap is `workers / work-time`, exactly as KPipe's own README predicts.** 100
  workers over 100 ms is 1,000 records per second; measured 998. Over 10 ms it is 10,000; measured
  9,875. The dial is the result.
- **At 10 ms, Parallel Consumer wins both orderings once the dial is turned.** 1.7x unordered, 1.6x
  key-ordered.
- **At 100 ms, Parallel Consumer is still on its ceiling.** 2000 workers over 100 ms is 20,000;
  measured 18,269, 91 percent. KPipe's remaining 2.6x lead in the unordered cell is the distance to
  wherever the next turn of the dial stops, and nothing here measured where that is. The in-flight
  ceiling near 2,750 recorded in the llingr work is the first candidate.
- **Key-ordered, Parallel Consumer wins at both work sizes.** KPipe's `KEY_ORDERED` at 100 ms is
  5,403, below PC KEY's 9,585, and the published 5.4x the other way inverts to 0.56x. KPipe's
  key-ordered mode costs it 89 percent of its unordered throughput at 100 ms; PC's costs 48 percent.
- **KPipe is dial-limited too.** Its log shows its backpressure controller pausing the consumer over
  six hundred times per pass at its 10,000 in-flight watermark, so its 100 ms cell is a watermark
  reading, not an engine reading, in the same way PC's is a worker-count reading.

## Addendum, same day: 4000 workers, the 100 ms unordered cell only

Asked for after the table above. Same harness, same box, `CONFLUENT_MAX_CONCURRENCY = 4000`,
`confluent` arm at 100 ms, two forks, three iterations; raw output in
`pc-maxconcurrency-4000-100ms-unordered.json`.

| PC UNORDERED, workers | Ceiling `workers / 0.1 s` | Measured | Of ceiling |
|---:|---:|---:|---:|
| 100 | 1,000 | 998 | 100% |
| 2000 | 20,000 | 18,269 | 91% |
| 4000 | 40,000 | **29,964** (0.6%) | **75%** |

Doubling the dial from 2000 to 4000 bought 1.6x, not 2x, and 29,964 a second at 100 ms is about
3,000 records in flight. That is the plateau the fork's own campaign found and attributed, with a
control arm, to the Java Kafka stack under platform threads rather than to the engine: a bare
`KafkaConsumer` with a thread pool stops at about 2,850 in flight on that harness, and PC at about
2,750 (`docs/inflight/bug-in-flight-ceiling-above-2000-concurrency.md` on
astubbs/parallel-consumer#363). KPipe's 47,881 at this cell is about 4,800 in flight on virtual
threads, which is the same stack without the platform-thread plateau. So the remaining gap at
100 ms is the thread type, and the fork's `useVirtualThreads` arm, which held 5,000 in flight at
100 ms where platform threads held 2,850 (`docs/inflight/perf-virtual-threads-measured.md`, same
PR), is the arm this cell should be rerun with once it ships.
<!-- file-refs: N/A - both inflight notes live on the perf/engine-concurrency branch, astubbs/parallel-consumer#363, not on this one -->

## What it does not establish

- Nothing about this fork's engine: the artifact is upstream 0.5.3.3, and the fork has not released.
- Nothing above 4000 workers; the ceiling on this box is bracketed at about 3,000 in flight, not located.
- Nothing about sub-millisecond work, allocation, or latency percentiles, which KPipe's capture
  also reports and which were not rerun.
- 8 processors and the Graal JIT are the same for every arm, so the ratios stand, but the absolutes
  are this box's and no other's.

## Where this goes

[`../inflight/core-hasten-adjacent-systems-register.md`](../inflight/core-hasten-adjacent-systems-register.md)
carries the KPipe entry and now cites this; the rerun item in
[`../inflight/process-prior-art-research-targets.md`](../inflight/process-prior-art-research-targets.md)
is closed by it. The fork-artifact arm and the ceiling sweep are the two follow-ups, and neither is
queued until somebody wants the public comparison.

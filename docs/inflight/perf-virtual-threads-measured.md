# Virtual threads, measured against Parallel Consumer itself

<!-- inflight-type: perf -->
<!-- inflight-impact: performance -->
<!-- inflight-labels: release-note, needs-measurement -->

Measured 2026-08-22, on the branch that implements `useVirtualThreads`. This is the re-take that
[`perf-platform-threads-are-the-ceiling.md`](perf-platform-threads-are-the-ceiling.md) asked for and
said must happen *"against PC itself before any of them are published"* - the numbers there came from a
40-line control with no Kafka and no Parallel Consumer in it.

**The prediction held.** At `maxConcurrency` 5,000 with a 100ms handler, platform threads plateau in
the high 2,000s and virtual threads reach the cap exactly.

## Conditions

Both arms in one invocation so they alternate, which is this harness's only defence against machine
drift between arms. Same broker, same topic, same 500,000 records, same JVM.

| | |
|---|---|
| Command | `MODES="core core-vt" PC_VERSIONS=LOCAL CLIENT_PINS=NATIVE bench/run-bisect.sh 500000 …` |
| Broker | `pc-bench-broker`, `bench-500000-p10`, 10 partitions, `BENCH_SKIP_PRODUCE=1` |
| JVM | **Temurin 21.0.9, for both arms.** Running the platform arm on 17 and the virtual arm on 21 would confound JDK version with thread type, which is the one variable being compared. |
| Machine | 12-core laptop, shared with other work. Load average recorded either side of each batch. |
| Repeats | 3 |

## Results

**100ms handler - the case virtual threads exist for.** Load 29 -> 45 across the batch.

| maxConcurrency 5,000 | Peak in flight | msg/s |
|---|---:|---:|
| Platform | 2,849 · 2,829 · 3,770 | 19,524 · 19,342 · 19,080 |
| **Virtual** | **5,000 · 5,000 · 5,000** | **35,115 · 34,490 · 34,032** |

**1.8x throughput, and the cap is reached instead of missed by 43%.** The platform plateau -
2,829-3,770 - sits where the standalone control put it (2,438-2,756 across a 10x load range) and where
the earlier PC measurement put it (2,751).

**100ms handler, below the knee.** Load 38 -> 15.

| maxConcurrency 1,000 | Peak in flight | msg/s |
|---|---:|---:|
| Platform | 1,000 · 1,000 · 1,000 | 8,928 · 8,875 · 8,868 |
| Virtual | 1,000 · 1,000 · 1,000 | 8,708 · 8,745 · 8,745 |

**No difference, correctly.** `min(maxConcurrency, r x handler_latency)` is `maxConcurrency` here, so
the thread type cannot matter - and the arms agree to 2%. **A mode that changed this number would be
a bug**, which is what makes this row worth keeping.

**2ms handler - where the gap is widest.** Load 24 -> 29.

| maxConcurrency 5,000 | Peak in flight | msg/s |
|---|---:|---:|
| Platform | 391 · 432 · 355 | 32,644 · 31,496 · 30,090 |
| **Virtual** | **5,000 · 5,000 · 5,000** | **80,775 · 97,752 · 106,496** |

**3.0x**, the largest gain measured, and the one most likely to surprise: a 2ms handler looks too cheap
for thread cost to matter, and it is the case where platform threads hold the *fewest* records in
flight of any row here.

**0ms handler - the control.** Load 24 -> 29.

| maxConcurrency 5,000 | Peak in flight | msg/s |
|---|---:|---:|
| Platform | 1,803 · 1,410 · 1,759 | 101,092 · 101,461 · 102,250 |
| Virtual | 1,879 · 1,673 · 2,115 | 111,359 · 103,135 · 106,564 |

**Within noise, correctly.** Nothing blocks, so no thread is held, so there is nothing for virtual
threads to fix. Both arms sit at the pipeline's own ceiling of roughly 105,000/s. This row is the
control that rules out "virtual threads are just faster at everything".

## What is NOT explained, and is recorded as open

**At 5,000 in flight and a 100ms handler, theoretical throughput is 50,000/s and the virtual arm
reaches 34,000.** It holds the full 5,000 records - that part of the prediction is exact - but does not
convert them into the arithmetic rate. The 0ms rows put the pipeline's own ceiling near 105,000/s, so
supply is not the constraint. Candidates not separated: the control loop's submit rate, commit
overhead, or the mailbox return path. **Not investigated here**, and it does not affect the
conclusion, which is about the thread ceiling.

## The false negative that came first, because it is the reusable part

**The first run of this comparison showed no effect at all** - 19,215 msg/s and 4,251 in flight for
platform, 19,641 and 2,764 for virtual. Written up as-is, that would have refuted the change.

**It was measuring a build without the option in it.** `bench/run-bisect.sh` resolves
`bz.stub.parallelconsumer` from the local Maven repository, not from `target/`, and the branch had not
been installed. So `-Dpc.virtualThreads=true` reached a Parallel Consumer that had never heard of the
property and ignored it silently, and both arms ran the same engine.

**Nothing in the output said so.** The harness printed a normal row; the flag is a system property, and
an unrecognised system property is not an error in any JVM. This is the trap
[`docs/investigating.md`](../investigating.md) states as *verify your instrumentation actually reached
the run*, and it is worth a specific rule for this harness:

> **`./mvnw install -DskipTests -Dcopyright.skip=true` before benchmarking a local change, and prove
> the option engaged before trusting a number.** A three-line program that constructs
> `ParallelConsumerOptions` off the installed artifact and prints `isUseVirtualThreads()` is enough,
> and it is the difference between a refutation and an artefact.

## What this does not cover

- **`Thread.sleep` is not I/O.** A real handler blocks on a socket, which parks differently. The
  mechanism - blocked work holding an OS thread - is the same, and the 0ms control shows the effect
  disappears when nothing blocks, but the numbers are for a sleeping handler.
- **A laptop is not a server.** Twelve cores, and shared with other work while these ran.
- **Ordering is UNORDERED throughout.** Key-ordered runs are a separate question.
- **No ablation of the counter rework.** The pressure system's executor reads were replaced in the
  same change, and these arms differ by thread type only - both carry the new counters. Whether the
  counters themselves cost anything is unmeasured; the 0ms and concurrency-1,000 rows, where the arms
  agree with each other and with the historic figures, are weak evidence that they do not.

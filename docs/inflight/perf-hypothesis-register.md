# Performance hypotheses: what was tested, and what it turned out to be

<!-- inflight-type: perf -->
<!-- inflight-impact: performance -->
<!-- inflight-labels: needs-measurement -->

Opened 2026-08-21. **Every explanation raised during the performance investigation, and its fate.**

The reason this exists: **nine hypotheses were tested and eight were wrong**, several of them read
convincingly off the source or off a profile before being demolished by a control arm. Without this
register the next session re-derives them, because each one is genuinely the obvious next thought.

**The rule the register earned:** *an explanation that has not been removed and re-measured is a
hypothesis, however well it reads.* Inspection generates candidates. Only a control arm settles one.

## Refuted

| # | Hypothesis | How it was tested | Result |
|---|---|---|---|
| 1 | The 0.3 -> 0.5 regression is the pressure system's stepping | Patched out the load factor | **The buffer, not the stepping.** Partly held - see the perf note |
| 2 | The in-flight ceiling is a short measurement window | 100k vs 500k records | Throughput rose 56%, plateau moved 1.6%. **Refuted** |
| 3 | The ceiling is single-partition supply | 1 vs 10 partitions | Within 8%, plateau moved the wrong way. **Refuted** |
| 4 | The ceiling is `max.poll.records` | 500 vs 5,000 | Within 0.3%. **Refuted** |
| 5 | The ceiling is the loading-factor buffer | Dynamic vs static 25,000 | Within 1%. **Refuted** |
| 6 | Summing work across shards each poll is O(shards) and costly | `KEY` mode: ~500,000 shards vs 10 | **Faster.** Refuted outright |
| 7 | The in-shard rescan past in-flight records costs throughput | Resume the scan; then split the state entirely | Dispatch 10x cheaper, **end-to-end 0%** at 0ms, 2ms and 100ms. Refuted twice |
| 8 | The worker pool's queue lock costs throughput | Replaced it with a lock-free `LinkedTransferQueue` | **69% WORSE.** Refuted, and inverted |
| 9 | That loss was `LinkedTransferQueue.size()` being O(n) | Wrapped it with an O(1) counter | 31,832 vs 33,743 - no difference. **Refuted** |

| 10 | The mailbox lock costs throughput | Replaced it with a lock-free counted queue | **+3.3% at 100ms, -2.7% at 0ms.** Real, tiny, a trade - and the wrong target, see below |

## The shortcut that would have saved all of it

**Classify parks by intent before choosing a lock to attack.** Of ~39,000 parks: **19,722 were workers
waiting for work** on an empty queue, and 17,785 were workers blocked returning it.

**Workers starved as often as blocked means the limit is upstream of them**, and no improvement to the
return path can fix a supply problem - the worker just reaches an empty queue sooner. That one
observation predicts hypotheses 6 through 10 all returning nothing, and it needs no experiment.

**A lock that appears in a profile while the threads hitting it are starved anyway is not on the
critical path.**

## Confirmed, and load-bearing

- **The gap is the client, not the engine.** With no engine on either side, the Java floor reaches
  31-67% of the Go floor. PC sits within a few percent of the Java floor and beats it at two points.
- **In-flight plateaus near 2,750 for PC and 2,848 for a bare `KafkaConsumer` with a thread pool** -
  the same wall, so it is not PC's.
- **Ordering is enforced by in-flight records remaining in the shard.** Discovered by removing them and
  watching ten tests fail. The "wasteful walk" is the mechanism.
- **`RetryQueue`'s fair lock is not a problem** - 5 parks out of 39,000, despite being a fair
  `ReentrantReadWriteLock` on the broker-poll path, which is a strong suspect on inspection.

## Two inversions worth remembering

**Contention in a profile is not contention costing throughput.** A park is where a thread *waits*. A
worker waiting for its next record is the pool working correctly. 31,000 parks looked like a smoking
gun and were normal operation.

**A lock can be the cheap option.** `LinkedTransferQueue` spins before parking; `LinkedBlockingQueue`
takes a lock and parks immediately. At maxConcurrency 1,000 on twelve cores - **about eighty threads per
core** - spinning burns exactly the cores the working threads need. Removing the lock cost 69%.

## Verdicts withdrawn on inspection

| Claim | Why it fell |
|---|---|
| "The 2022 direct-pull rework was 1/3 as fast, so the design is slow" | Its worker idle path is a **busy-spin** - the blocking wait is commented out with an unresolved race. The branch was unfinished; the design was never measured |
| "Every Vert.x measurement in the repo was harness-capped" | Too broad. The bisect ran at concurrency 100, nowhere near the stub's ceiling. **The 0.4.0.0 cliff, the buffer finding and the 35% recovery all stand** - only the high-concurrency Vert.x cells were capped |
| "The async stub shows the Vert.x engine is 65% faster than core" | Conflates ceiling removal with **machine relief** - the WireMock stub's 2,600 sleeping Jetty threads were competing for the same 12 cores on localhost. It is a threading-model comparison, not an engine comparison |

## Still open

- **Too many platform threads for the machine.** Raised by the owner early, argued down by me on
  evidence that was weaker than I presented it, and now the strongest surviving candidate. See below.
- **The mailbox** - [`parked-mailbox-is-not-the-bottleneck.md`](parked-mailbox-is-not-the-bottleneck.md), the largest single park
  site and PC's own code. Currently a hypothesis, not a finding.
- The two ~20% cells against the Java floor, and whether they are even real given run-to-run spread.

## The one I dismissed too quickly

**The owner proposed "is it the overhead of running 5,000 Java threads?" and I argued against it.** My
counter was that the pool does create the threads, and that in-flight rose to 3,889 when the delay
doubled - so "a limit that scales with latency is a rate limit, not a capacity limit".

**That argument is still correct and it was never sufficient.** It rules out a hard cap on thread
count. It says nothing about threads being *expensive*, and the `LinkedTransferQueue` result is
direct evidence that thread-to-core ratio governs behaviour here: the same change that would help a
well-provisioned pool cost 69% on an oversubscribed one.

**It remains unproven** - nothing has yet shown platform threads cost throughput in the baseline, where
they park cheaply rather than spin. But it is now the leading candidate rather than a dismissed one,
and **the decisive test is virtual threads**, which has still not been run.

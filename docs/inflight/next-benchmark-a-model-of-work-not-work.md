# Next: every benchmark handler is a sleep, and every number is from one operating system

<!-- inflight-type: next -->
<!-- inflight-impact: performance -->
<!-- inflight-labels: needs-measurement -->

Opened 2026-08-21. **Two limits that sit underneath every performance number this project has**, and
neither is a caveat on a detail - both are caveats on headlines.

## Every handler is `Thread.sleep`

Three call sites in `bench/Bench.java.template`, no exceptions. **We have measured a model of work,
never work.**

**What the model gets right, with one condition:** **blocking** I/O parks a thread exactly as a sleep
does, so the platform-thread mechanism carries over intact and the reachable-concurrency law
`min(maxConcurrency, r x handler_latency)` holds -
[`perf-platform-threads-are-the-ceiling.md`](perf-platform-threads-are-the-ceiling.md).

**The condition is that the handler blocks at all.** A user on a non-blocking client - an async HTTP
library, R2DBC, a reactive driver - does not park a thread while waiting, and none of this applies to
them.

**Except that in the core engine they cannot avoid parking one, because the API will not let them.**

```java
void poll(Consumer<PollContext<K, V>> usersVoidConsumptionFunction);
```

**The callback returns `void` and is synchronous.** Completion is signalled by returning, so a user
holding a `CompletableFuture` from a non-blocking client has exactly one option: `.get()` or `.join()`
it - **which parks the thread they were trying not to park.** Their non-blocking library buys them
nothing, and the sleep model describes them accurately after all.

**So the thread is pinned by the API's shape, not by the user's I/O choice**, and that is the sharpest
version of the argument for the async engines: they are not merely faster, they are **the only way to
use a non-blocking client without wasting it.** `ExternalEngine` exists precisely so the user can hand
back something unfinished.

**The one case the sleep model genuinely does not describe is CPU-bound work**, which parks nothing,
is limited by cores rather than by activations, and inverts the analysis - more concurrency makes it
worse.

**What it leaves out, and any of these could move a number:**

- **Allocation.** Real handlers allocate per record; a sleep allocates nothing. GC pressure at 20,000
  records/second is a term this harness cannot see at all.
- **The network stack.** Sockets, TLS, syscalls, and the kernel buffers underneath them.
- **Connection pools.** A real HTTP or database client has a bounded pool, and **that bound often sits
  well below the concurrency being configured** - so the user's effective ceiling may be their pool,
  not ours. This is already suspected in one measured number: PC + Vert.x reached 32,332 msg/s where
  the pure async control reached 47,022, and **the Vert.x WebClient's unconfigured connection pool is
  the leading candidate for that gap.**
- **Deserialization**, which the harness skips almost entirely - the values are `"value-<i>"`.
- **CPU-bound work**, which does not park a thread at all and therefore inverts the entire
  thread-ceiling analysis: a CPU-bound handler is limited by cores, and more concurrency makes it
  worse.

**What to do:** add a handler axis alongside delay and concurrency - sleep, a real HTTP call to a
remote host, a database round trip, and a CPU burn. **The sleep arm stays**, because it is the clean
control; the point is to know how far the others diverge from it.

## Every number is macOS

`r ~ 20,000-27,000 thread activations/second` is **a macOS constant**, measured on one twelve-core
laptop.

**Linux parking is futex-based and materially cheaper.** If `r` is several times higher there, the
ceiling moves with it - `min(maxConcurrency, r x latency)` could put 5,000 in flight comfortably
within reach at a 100ms handler on a Linux server, which would make the whole ceiling a
developer-laptop phenomenon rather than a production one.

**That is not a small caveat.** It is the difference between "core caps at ~25,000 records/second per
instance" and "core caps at ~25,000 records/second per instance *on a Mac*". **The advice we would
give a user changes depending on which is true, and nobody knows.**

**What to do:** run `bench/threads/ThreadCeiling.java` on Linux. It has no dependencies beyond the JDK
and needs no broker - it is forty lines and answers the question on its own. A CI runner would do.
**Until that is run, every ceiling number in these notes should be read as one operating system's.**

## Why this is filed rather than fixed

Both are cheap. Neither was done because the session's questions were comparative - *is it the engine
or the client, the scan or the threads* - and comparisons survive a shared model of work, since both
arms pay the same simplification. **The moment a number is quoted as an absolute rather than a ratio,
both of these bite.** The landing page's benchmark section is exactly that moment; see
the landing-page work (dropped 2026-08-22; see git history for `next-landing-page.md`), where the rule is already written down as *never
publish a figure without the conditions that produced it*.

## 2026-08-22: the audit Antony asked for - every comparison models a workload nobody would use PC for

**"Comparing key ordering to partition ordering when you have a zero-millisecond or ten-millisecond
function that always succeeds, never fails, never retries, never gets stuck - it's not playing to the
use case people would use Parallel Consumer for."**

Correct, and the scale of it is worse than it reads. **Four choices are shared by every performance
number this project has ever published, and each one removes a reason to adopt PC.**

| Dimension | What every run used | What a real workload does | Knob |
|---|---|---|---|
| **Ordering mode** | `UNORDERED` | `KEY` - it is why people adopt PC | already there |
| **Handler duration** | constant | long-tailed: GC, a slow dependency, an outsized payload | `BENCH_DELAY_P99` / `BENCH_DELAY_STDDEV` |
| **Failure rate** | zero | records fail and retry, and retries reorder work | `BENCH_FAILURE_RATE` |
| **Key distribution** | all keys distinct | skewed - a few hot keys carry most traffic | `BENCH_KEY_DISTRIBUTION` |

**All four knobs exist and all four have now been used.** The audit below was written when the last
three read "built and never used" and "does not exist"; the key-distribution axis was built the same
day and swept on 2026-08-22
([`perf-the-tail-experiment-ran-2026-08-22.md`](perf-the-tail-experiment-ran-2026-08-22.md)), and the
failure axis was swept across the engine, version and share-group comparisons on 2026-08-23. **The
paragraphs that follow are kept as written, because the conclusions they draw are what got tested** -
see each one's outcome recorded at the end of this section.

### The ordering-mode count, which is the one that should be uncomfortable

Across every results file from 2026-08-22:

| ordering | rows |
|---|---:|
| `UNORDERED` | **369** |
| `KEY` | **7** |
| `PARTITION` | 4 |

**`UNORDERED` is the mode in which Parallel Consumer has no differentiator.** It is where a bare
`KafkaShareConsumer` beats PC's best arm 2.5x
([`perf-share-groups-versus-pc-2026-08-22.md`](perf-share-groups-versus-pc-2026-08-22.md)), and where
a plain thread pool is within 1% of it. **We measured the mode we lose in, fifty times more than the
mode we exist for.**

Not because anyone decided to. `UNORDERED` is the harness default, all-distinct keys make it
maximally parallel, and a constant handler makes runs short and repeatable. **Every one of those is a
choice that makes measurement easy and makes the result mean less.**

### Key distribution is the gap with no knob at all

All-distinct keys is the **best case for any key-sharded design**: every record is its own shard, so
`KEY` ordering imposes no constraint whatsoever and behaves like `UNORDERED`. That is why the two
modes have looked so similar in every number here - **we have never actually tested key ordering, we
have tested `UNORDERED` wearing its name.**

A realistic distribution - Zipf, or a handful of hot keys - is where `KEY` ordering costs something
and where PC's shard machinery is doing real work. It is also where a competitor without per-key
ordering cannot follow. **`BENCH_KEY_DISTRIBUTION` does not exist and is the single most valuable
axis missing**, because it is the one that makes the differentiator visible instead of free.

### What this means for the numbers already taken

**They are not wrong; they are narrow, and they were read as general.** Every one is a valid
measurement of: distinct keys, constant work, no failures, no ordering constraint. State that
alongside them rather than retracting them.

**But two conclusions drawn from them need re-testing before they are repeated:**

- **"Share Groups beat PC 2.5x"** - measured in `UNORDERED`, at a 2ms constant handler, all keys
  distinct. That is Share Groups' best case and PC's worst.
- **"`core` is within 1% of a hand-rolled thread pool"** - same. A thread pool cannot do `KEY`
  ordering at all, so at a realistic key distribution the comparison stops existing.

**And it reframes the tail experiment** ([`next-the-tail-experiment.md`](next-the-tail-experiment.md)):
its arms should be `KEY` against `PARTITION` and `share` **with a skewed key distribution and a
failure rate**, not the flat all-distinct workload it currently specifies. Otherwise it measures the
same narrow thing more precisely.

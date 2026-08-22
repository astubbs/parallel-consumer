# PC's async engines already break the platform-thread ceiling - today, on Java 8

<!-- inflight-type: perf -->
<!-- inflight-impact: performance -->
<!-- inflight-labels: release-note, needs-measurement -->

Measured 2026-08-21. **The `ExternalEngine` family - Vert.x, Reactor, Mutiny - is not subject to the
ceiling that caps the core engine, and needs no virtual threads to escape it.**

## The measurement

Same engine, same broker, same dataset, same concurrency setting. **The only variable is whether the
thing being called holds a thread while it works.**

| Vert.x engine, 100ms, 5,000 concurrent | msg/s | Peak in flight |
|---|---:|---:|
| **Async stub** - responds on a timer, no thread held | **32,332** | **5,000** of 5,000 |
| WireMock stub - thread per request, sleeps on it | 13,975 | 2,651 of 5,000 |

**2.3x, and the configured concurrency is reached exactly.** At 1,000 concurrent the async arm also
reaches 1,000 exactly, twice.

**Against the core engine at the same operating point - 19,577 msg/s, peak 2,751 - the Vert.x engine
with an async callee is 1.65x.** Core is not slow; core is thread-bound.

## Which Vert.x numbers were capped - and, importantly, which were not

**Scope this carefully.** An earlier draft of this note said "every previous Vert.x number in this
repository was capped by the harness". **That is too broad and would discard five valid findings.**

The WireMock stub's ceiling binds only where the configured concurrency approaches its own
`r x delay` - about 2,600 at a 100ms handler. **The version bisect ran at maxConcurrency 100 with a
2ms delay and measured peak in flight of exactly 100.** Nowhere near the stub's ceiling. So **the
0.4.0.0 cliff, the load-factor buffer finding, the `ExternalEngine` override attribution and the 35%
recovery all stand.**

**What was capped: the high-concurrency, long-delay Vert.x cells only** - which is to say, the ones
this note is about, and no others.

## How the stub capped them

`VertxHttpStub` is WireMock configured with `containerThreads(maxConcurrency)`, and the caller's
listener **sleeps on the serving thread**. Its javadoc sizes that pool to the caller's concurrency
precisely so *"the stub is never what limits observed concurrency"* - which was the right instinct
against a thread-per-request server and is the wrong fix, because platform threads stop scaling under
~3,000 regardless of how many you ask for
([`perf-platform-threads-are-the-ceiling.md`](perf-platform-threads-are-the-ceiling.md)).

**So the stub reproduced the ceiling server-side, and the result read as Parallel Consumer failing to
reach its ceiling** - the exact failure mode that javadoc was written to prevent.

The replacement lives in `bench/Bench.java.template` behind `BENCH_ASYNC_STUB=1`: a Vert.x HTTP server
that registers the response on `setTimer` and returns the event-loop thread immediately.

## What this changes

1. **PC has a path past the thread ceiling *today*.** No virtual threads, no JDK 21, no PR astubbs#51,
   no package rename, no CI lane. **On Java 8.** For a user whose work is a network call, the Vert.x
   engine already decouples concurrency from threads.
2. **The `ExternalEngine` throughput regression becomes the most valuable open defect in the project.**
   Two overrides in `ExternalEngine` cost ~35% and are documented in
   [`perf-throughput-regression-since-0-3.md`](perf-throughput-regression-since-0-3.md). That is a tax
   on **the one engine that is not thread-bound** - which reframes it from a historical curiosity into
   the thing standing between users and the ceiling being lifted.
3. **The core engine's ceiling is confirmed from the other direction.** Everything else was measured by
   *removing* things from core and finding nothing changed. This changes the callee's threading model
   and the ceiling moves, which is the same conclusion by a different experiment.
4. **It is an argument for the async engines in the documentation**, which currently present Vert.x and
   Reactor as integration conveniences rather than as the concurrency answer they turn out to be.

## What it does not show

- **Nothing about a blocking user function.** If the handler blocks a thread, the Vert.x engine is back
  in the same regime. The gain belongs to workloads whose work is genuinely async - which is most
  network I/O, and none of the CPU-bound cases.
- **Nothing at 0ms or 2ms**, which were not run here.
- **Not a quiet-machine number.** Taken at load average ~100 on twelve cores. The ratio is the result;
  treat 32,332 as approximate.
- **The 2.3x is not pure ceiling-removal.** Everything is on localhost, so the WireMock stub's ~2,600
  sleeping Jetty threads were competing for the same twelve cores as the system under test. **Part of
  that delta is the machine being relieved, not concurrency being unlocked.** Splitting the two needs a
  sharded or remote stub. The honest claim is "the async stub removed a server-side ceiling *and* a
  co-located thread burden".
- **32,332 is 64% of the 50,000 theoretical, and the remaining 36% is unattributed.** The pure async
  control reached 47,000 on four threads, so there is a real ~15,000/s gap owned by something in the
  PC + Vert.x + client stack - possibly the Vert.x WebClient's connection pool, which nobody configured
  deliberately. **Check that before quoting 32k as the engine's capability.**
- **It also weakens what remained of "the client cannot supply".** The same `kafka-clients` fed 32,000
  records/second at 100ms and 5,000 concurrent through Vert.x. **The client floor at this operating
  point is at least 32k, not 20k.**
- **Reactor and Mutiny are assumed, not measured.** They share `ExternalEngine`, so the argument
  carries, but only Vert.x was run.

## Next

- **Re-measure the `ExternalEngine` regression against the async stub.** Every previous measurement of
  it was taken through the capped stub, so its size is unknown at the concurrencies that matter.
- **Run Reactor and Mutiny** the same way, to confirm the family behaves as one.
- **Say this in the docs.** The choice between core and an async engine is currently framed as a
  dependency preference; it is a concurrency-model decision worth far more than that.

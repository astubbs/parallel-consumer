# HIGH PRIORITY: do Java virtual threads work in a GraalVM native image, and do ours?

<!-- inflight-type: next -->
<!-- inflight-impact: performance -->
<!-- inflight-labels: needs-measurement, release-note -->

**The question is one experiment wide and the answer decides how the proxy sidecar is positioned.**
Build the sidecar as a GraalVM native image, turn on `useVirtualThreads`, and see whether the pool is
created and whether the throughput matches the JVM figures.

## Why it is worth more than a compatibility checkbox

Virtual threads are measured at **1.8x at 100ms/5,000 and 3.0x at 2ms/5,000** on the JVM (see
`perf-virtual-threads-measured.md`). If that survives native compilation, then in the proxy
architecture the engine is a native binary and the caller is a thin client, so:

- **Every foreign-language client gets virtual-thread concurrency, whatever its own runtime does.**
  The concurrency lives in the engine, not in the caller. That is the answer to the obvious objection
  to a Java-engine sidecar from a Go shop - goroutines are the thing Go users would be giving up, and
  this is how they do not give it up.
- **A caller stuck on an old JDK gets it too**, through the Java client talking to the native engine,
  without that JDK ever needing to support virtual threads. The counter-argument is real and should be
  said out loud rather than discovered later: **an organisation frozen on an old JDK is often frozen
  by a policy that would also forbid GraalVM**. So this is a route, not a general answer. It is worth
  having anyway, because the *first* bullet does not depend on it at all.

## What is known, and what is only asserted

**Known**: Native Image supports virtual threads from GraalVM for JDK 21. Earlier native-image
releases were JDK 17-based, where the feature does not exist. The ordinary JDK caveats carry over
rather than being Graal-specific - on JDK 21 to 23 a virtual thread pins its carrier inside
`synchronized`, which JEP 491 removed in 24.

**Asserted, not measured - this is the actual risk.** `AbstractParallelEoSStreamProcessor`
`#setupVirtualThreadWorkerPool` reaches every JDK 21 symbol **reflectively**, because this module
compiles to Java 8 bytecode through Jabel and `Thread.ofVirtual()` is not on its compile-time API
surface at all. Native Image does closed-world static analysis, and **this repo has no
`META-INF/native-image` reflection configuration anywhere**. Constant-folded `Class.forName` and
`getMethod` are often auto-registered by current native-image, but "often" is not a guarantee, and
nothing here has been run.

The failure mode is at least loud: the `ReflectiveOperationException` is caught and rethrown as an
`IllegalStateException` saying validation passed but the JVM cannot create the executor. It fails at
pool construction rather than silently falling back to platform threads.

## 2026-08-22: the proxy measurement makes this considerably stronger

`proxy` measured **25,615 msg/s at 2ms/5,000**, within **1%** of `core-vt`'s 25,934 - and it is the
path **every non-JVM client takes**. That was not known when this note was written.

**Antony's framing, and it is the right one:** Java becomes a *foreign language*. A shop that cannot
run JDK 21 - or does not want a JVM at all - runs the engine as a GraalVM native binary with virtual
threads inside it, and talks to it over the proxy. They get virtual-thread concurrency **without
having virtual threads**, because the concurrency lives in the binary rather than in their runtime.

That composes two results that were measured independently today:

| | |
|---|---|
| Virtual threads are the only thing that reaches `maxConcurrency` on a blocking function | 5,000/5,000 against core's 2,824 |
| The proxy path costs almost nothing next to the best in-process engine | within 1% at 2ms |

**If both hold under native compilation, a Python or Ruby consumer gets the fastest configuration
this project has**, which no client library in those languages can offer. That is the pitch, and it
rests entirely on the untested question at the top of this note.

**Bounded, and the bound matters**: the proxy arm drives the engine in-process across the
`DispatchSink`/`report` seam. Production funnels every report through one serialised inbound callback
per session, which this does not. The 25,615 is an upper bound and says nothing about the wire - so
**the end-to-end gRPC arm has to exist before this pitch is made**, not after.

## The experiment

1. Native-build the proxy sidecar, run it with `useVirtualThreads` on, and confirm the pool is created
   at all. If it throws, add `reflect-config.json` entries for `Thread.ofVirtual`, `Thread$Builder` and
   `Executors.newThreadPerTaskExecutor` and repeat - that is the expected first result, not a failure
   of the idea.
2. Re-run the two operating points virtual threads were measured at on the JVM. **Same operating
   points, so the numbers are comparable** - a native run at a different concurrency or delay answers
   nothing.
3. Control arm: the same native binary with `useVirtualThreads` off. If native compilation changes
   throughput on its own, a virtual-threads-only measurement cannot distinguish that from the feature.

## What it unblocks

`next-work-server-pitch-and-buyer.md` and the polyglot client story. If it holds, the sentence is
"the engine gives your Python, Ruby or Node consumer the concurrency profile of virtual threads",
which no client-library competitor in those languages can say. If it does not, the sidecar's
concurrency ceiling is platform threads and `perf-platform-threads-are-the-ceiling.md` applies to it
in full - which is a materially weaker pitch and should be known before it is made, not after.

See also: [`perf-virtual-threads-measured.md`](perf-virtual-threads-measured.md),
[`perf-platform-threads-are-the-ceiling.md`](perf-platform-threads-are-the-ceiling.md),
[`branch-language-proxy.md`](branch-language-proxy.md).

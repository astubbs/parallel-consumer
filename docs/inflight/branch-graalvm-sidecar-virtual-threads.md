# The sidecar on virtual threads: every language gets Loom without upgrading its own runtime

Branch `feats/graalvm-sidecar-virtual-threads`, based on `research/market-analysis-recut` (which
holds the virtual-threads plan and the two ceiling benches). **Design and rationale only - the
experiment is not run yet.**

## The idea, which is an architecture claim rather than a performance one

The sidecar is a **separate process**. So the JVM the *engine* runs on is decoupled from the runtime
the *application* runs on. That produces two things the library cannot offer on its own:

1. **A Go, Python, Ruby or C++ application gets virtual-thread throughput** without a JVM anywhere in
   its own stack. It talks gRPC to a sidecar that happens to be running JDK 21+ or GraalVM.
2. **A Java application stuck below JDK 21 gets it too**, through the Java binding, talking to a
   sidecar on a newer JVM. The one thing that usually blocks Loom adoption - "we cannot upgrade the
   application's JDK" - stops being a blocker, because the thing that needs 21 is not the
   application.

The performance framing the owner gave: virtual threads bring PC to roughly the throughput of Go
libraries whose concurrency model is goroutines. The sidecar is what lets non-JVM languages have that
without adopting the JVM.

## The evidence that already exists, and what it does NOT settle

From [`perf-platform-threads-are-the-ceiling.md`](perf-platform-threads-are-the-ceiling.md), measured
with no Kafka and no Parallel Consumer at all:

| concurrency 5,000, 100ms sleep | peak in flight | msg/s |
|---|---:|---:|
| platform threads | 2,756 of 5,000 | 6,481 |
| **virtual threads** | **5,000 of 5,000** | **46,083** |

**7.1x, thread type the only variable.** That is the reason to expect a win, and it is why this was
not run today: the owner's position is that we already know what the result looks like. The branch
exists to prove it rather than to discover it.

**But the same note has a second result that constrains this one.**
[`bench/threads/AsyncCeiling.java`](../../bench/threads/AsyncCeiling.java) removes thread-holding
instead of changing thread type, and on **JDK 17 with four scheduler threads** reaches 46,802 msg/s at
concurrency 5,000 - matching virtual threads almost exactly - and 50,000 records in flight at higher
dials. **Concurrency decoupled from threading**, and no Loom involved.

So the honest question for the sidecar is not "are virtual threads faster than platform threads" -
that is settled - but:

> **Does the proxy's engine hold a thread per in-flight record while it waits for a foreign worker's
> verdict?**

If it does, virtual threads lift the ceiling exactly as the bench predicts. If it is already
non-blocking, the AsyncCeiling result says the ceiling is already lifted and virtual threads buy
little - and the interesting claim becomes about *simplicity*, not throughput. **Measure that before
building anything.** It is one question and it decides whether this branch is a performance change or
a no-op with better ergonomics.

## The plan says the proxy is excluded, and that is out of date

`docs/plans/2026-08-22-001-feat-virtual-threads-plan.md` R5:

> `ExternalEngine` and its subclasses (Vert.x, Reactor, Mutiny, proxy) do not use virtual threads for
> the worker pool.

and KTD9 gives `ExternalEngine` a `supportsVirtualThreads()` opt-out hook, mirroring
`supportsDirectPull()`.

**Per the owner (2026-08-22) that exclusion is out of date - the external engines do use virtual
threads now, or will very soon.** So this branch either amends R5 or supersedes it, and whichever it
does must be written down where R5 is, not only here. The opt-out hook is the seam that makes the
change small: the proxy would answer `true` where the plan currently assumes `false`.

Also relevant from that plan, and inherited rather than re-litigated:

- **R4: core still compiles to Java 8 bytecode; JDK 21 APIs are reached reflectively.** Nothing here
  changes that. Core compiled to 8 runs perfectly well *on* a 21 JVM - which is exactly what makes
  the sidecar trick work.
- **KTD1: fix the JDK 21 build first**, and treat it as a discovered blocker rather than an
  assumption.
- **R14: when a mode is selected and the runtime cannot provide it, the run fails** - it does not
  skip and does not report green.

## The GraalVM half, and what is already decided about it

GraalVM matters here for the sidecar's *packaging*, not its concurrency: a native image starts fast
and carries no JVM for the user to install, which is what makes an invisible spawned sidecar (KTD41)
palatable to a Go or Python team.

Existing decisions to respect rather than rediscover:

- [`parked-a-c-client-and-the-ffi-question.md`](parked-a-c-client-and-the-ffi-question.md): "The
  Graal sidecar is not a hypothetical either" - KTD13 dual-ships and makes the native image a
  first-class artifact.
- [`2026-08-17-001-feat-native-core-rewrite-deferral-plan.md`](../plans/2026-08-17-001-feat-native-core-rewrite-deferral-plan.md):
  the `native-image --shared` **embedding** path is closed and is not to be reopened. A standalone
  native sidecar is a different thing from embedding PC into a foreign process, and only the latter
  was rejected.

**SETTLED 2026-08-22, by building one.** Oracle GraalVM 23 (`23-graal`, already installed), 20,000
tasks each sleeping 100ms, counting how many ran on a virtual thread:

| | tasks | peak in flight | ran on virtual |
|---|---:|---:|---:|
| GraalVM, JVM mode | 20,000 | 20,000 | 20,000 |
| **GraalVM, native image** | 20,000 | **20,000** | **20,000** |

A `Mach-O 64-bit arm64` executable, built in 28.3s with `--no-fallback`, holding twenty thousand
virtual threads - identical to JVM mode, and the same shape as ThreadCeiling's virtual arm. **The
language feature and the AOT compiler do not fight.** Native image support for virtual threads
arrived in GraalVM for JDK 21 (23.1); the JDK 17-era builds do not have it, which is worth knowing
because the repo's own toolchain pins are below that.

**What this does NOT prove, and the gap is the whole job:** the test was forty lines with no
dependencies. The sidecar carries gRPC/Netty and the Kafka client, both of which are hostile to
native image's closed-world analysis - reflection, dynamic proxies and resource loading it cannot
see. Expect a reachability-configuration exercise, not a recompile.

**One collision to decide early, because the two choices are not independent:** the virtual-threads
plan keeps core on Java 8 bytecode and reaches JDK 21 APIs *reflectively* (its requirement R4).
Reflection is exactly what native image must be told about in advance. "Core is Java 8 plus
reflection" and "the sidecar ships as a native image" therefore have to be decided together.

## What to prove, in order

1. **Does the proxy hold a thread per in-flight record?** Read `ProxyProcessor`'s dispatch and await
   path. One reading, and it decides everything below.
2. **Sidecar on JDK 21 with a virtual-thread pool, serving a foreign client end to end.** The claim
   is that a non-JVM application gets the lift; a Java-only benchmark does not demonstrate it. Use one
   of the eleven existing clients.
3. **The same on GraalVM**, native image, to establish that Loom and native image co-operate at the
   versions we would ship.
4. **A JDK-17 Java application against a JDK-21 sidecar** - the "stuck below 21" story, which is the
   one most likely to be quoted and the one most likely to have a sharp edge.

## Traps, all of them already paid for elsewhere in this repo

- **Control arms, one term at a time.** Thread type is one term; the counter rework the plan
  describes is another; the sidecar hop is a third. R17 already insists the counter rework is ablated
  separately from the thread-type change - the same discipline applies here or the number means
  nothing.
- **Do not measure on a loaded box.** An entire fan-out's throughput figures were discarded this
  month for exactly that, at load 20-113.
- **A mode that cannot run must fail, not skip** (R14). "Virtual threads unavailable, continuing on
  platform threads" is the shape of a green run that proved nothing.

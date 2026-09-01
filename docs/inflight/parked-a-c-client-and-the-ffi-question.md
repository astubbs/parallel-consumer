# Parked: a C client, and the FFI question underneath it

Asked 2026-08-15: librdkafka is written in C rather than C++, so should there be a C client too?
Considered and parked, because the reasoning generalises to any future "why not language X" and is
worth not re-deriving.

## Why librdkafka is C, and why that reasoning does not transfer

C is the FFI lingua franca. One C core with a stable ABI is callable from Python, Ruby, Node, Go,
PHP, Perl and R, so a single implementation reaches every ecosystem through thin bindings. That is
librdkafka's whole architecture, and C is the only language that makes it work.

**This project deliberately does not need that model.** The shared core is the *sidecar*, on the JVM,
where the mature Kafka client already lives; each language gets a native client generated from the
frozen protocol. Protobuf codegen is our "one core, many bindings" — with the boundary at the
**process** edge rather than the **FFI** edge. It is the same distinction that makes this fork's
Kafka currency a version bump where librdkafka's is a reimplementation.

## Three practical objections

- **There is no pure-C gRPC.** gRPC Core is C++ behind a thin C surface almost nobody writes clients
  against. A C client would either bind the C++ stack — at which point it is the C++ client with
  worse ergonomics — or hand-roll HTTP/2 framing. Protobuf is the same: no official C implementation,
  only third-party (`protobuf-c`, `nanopb`).
- **Every ecosystem a C shim would reach, we already reach natively**, with better ergonomics than an
  FFI wrapper and no ABI to keep stable.
- **The population is thin**: someone writing pure C who wants key-ordered concurrency, and who would
  probably reach for librdkafka directly.

## Where it would genuinely make sense

If the target were **languages whose native gRPC support is poor or absent** — embedded targets, Lua,
Perl, R, older runtimes — then a small C shim over the protocol becomes the reach mechanism and the
FFI argument is the right one. That is a decision about serving a long tail, and it should be taken
on evidence that the tail exists rather than on symmetry with librdkafka.

## PARTLY ANSWERED BY MEASUREMENT, 2026-08-22

Go **and Python** processes have now run the engine in-process over a GraalVM `--shared` library -
see [`perf-embedding-the-engine-over-ffi.md`](perf-embedding-the-engine-over-ffi.md).

**The callback table below, introduced as the breakdown that "decides the whole proposal", ranks a
design that turned out not to be necessary.** It assumes the engine calls *out* into the host on
many threads. The surface built instead is a pull model - the host pulls work frames and pushes
verdict frames on its own threads - so nothing re-enters Java from a foreign thread, and Go's
"medium" ranking was never exercised. Whether that reopens Python, Ruby and Node is now an open
question - and for Python it is now answered. **The GIL objection is measured dead**: `ctypes`
releases the GIL around a blocking pull, 1142x against a `PyDLL` control that differs in one flag.
The table's "hard mechanically" ranking for Python is false under the pull model.

It does not follow that Python should embed. "Their work is slow, so the hop is noise" is untouched
and was always the stronger half - **but it is a different argument, and this file ran the two
together.** What fell is the difficulty claim, not the value claim.

**What did NOT change**: the release-matrix objection, which this file calls decisive. An embedded
engine still rebuilds on every Kafka bump. The measurement proves feasibility, not that it ships.

## SUPERSEDED BY KTD41, 2026-08-18 - read this before the rest of the file

**The rejection recorded below no longer stands, and the section headed "PRIOR ART FOUND LATE" is the
part that went stale.** KTD41 (`96131dd6d`, session-settled, user-directed) supersedes the
shared-library dead end **on direction** in both the plan and the branch ledger: the owner proposed
the direction, so the rejection half dissolves. What survives is the untested caveats, now recast as a
**qualification probe's checklist** rather than as reasons not to look.

**Two things KTD41 settles that this file predates:**

- **The FFI track is re-scoped to Rust, C++ and edge targets.** Go, Python, Ruby, Node and .NET stay
  on the sidecar, for reasons a better FFI design does not remove. That is the same place the
  per-language callback breakdown below arrived at independently - difficulty and demand rank together
  - with one owner's correction: .NET moves to the sidecar side.
- **The sidecar becomes invisible rather than operated.** Every language package vendors the
  platform-matched binary and spawns it on first use; attaching to a supplied address is the escape
  hatch for preforking process models and platforms that forbid executing a vendored binary. That
  changes the comparison this file was making, because the thing FFI was competing against - "a second
  process the user must operate" - largely stops existing.

**The probe is gated: kill criterion first, measured against KTD41's invisible-sidecar control arm.**
So the experiment framing recorded further down survives; only the verdict it was arguing against has
changed.

**The plan owns this now.** Read `docs/plans/2026-08-14-001-feat-language-proxy-plan.md` - KTD41, its
Scope Boundaries entry and its Dead ends section - before acting on anything below. What is kept here
is the per-language callback evidence, which nothing else records.

## PRIOR ART FOUND LATE: the plan already rejected this, and already measures the hop

Recorded 2026-08-15 as a correction. The section below was written and discussed **without checking
the plan first**, which is the prior-art rule this repo puts before forming any hypothesis. Two things
were already settled and had to be found afterwards:

- **`native-image --shared` was explicitly rejected**, in
  `docs/plans/2026-08-14-001-feat-language-proxy-plan.md`, under *Compiling PC to a native shared
  library*. Its grounds are stronger than anything reasoned out below, and one is decisive: the
  cleared native-image gate produced an **executable, not a `--shared` export**, which differs in
  entry-point surface, isolate and thread-attach semantics, **two garbage collectors sharing a
  process**, and callbacks re-entering from foreign threads — none of it tested. And **it is the worst
  available shape for agentic development**, because its failures are segfaults and memory corruption
  with no useful stack trace, where gRPC and protobuf fail legibly and fast. This project is built
  agentically, so debuggability is a selection criterion rather than a preference.
- **The hop is already measured, by design.** R31 requires v1 to report median and p99
  poll-to-completion latency through the proxy *and* for in-process PC on the same workload, and
  **Java ships twice — `java-direct` and `java-grpc` — precisely as the experimental control that
  isolates the hop from the language.** So the controlled comparison the section below proposes
  building in Rust **already exists, with no FFI, no second garbage collector and no segfaults.**

**What survives of the section below**: the per-language callback table, which is useful for its own
sake, and the observation that difficulty and demand rank together. **What does not survive**: the
proposal itself, and the "two Rust backends as a controlled experiment" framing — the experiment is
already built, and the cheaper arm is the one already shipping.

**The Graal sidecar is not a hypothetical either.** KTD13 dual-ships and makes the **native image the
default**, R51 requires both artifacts from the first release, the native-image feasibility gate (R25)
cleared at a **45MB binary building in 33–52s**, and `parallel-consumer-proxy/pom.xml` carries the
native profile. So the confound named below — comparing an AOT-compiled embedded engine against a JIT
sidecar — is already controlled for by what ships.

## The other C proposal: embed the engine, do not wrap the protocol

Raised 2026-08-15, and it is **not the client this note parked** - it inverts it. Rather than a C
client speaking gRPC to a sidecar, compile Parallel Consumer itself with GraalVM `native-image
--shared` into `libparallelconsumer.so`, expose `@CEntryPoint` functions, and let every FFI-capable
language link it directly.

**Same programming model as the existing clients** (owner's framing, and it is the load-bearing
part): register a worker, receive dispatched work, report verdicts. Nothing about the semantics
changes. So this is **a third binding rather than a new product** - and in the transport-seam terms of
[`parked-http-dialect-and-generated-clients.md`](parked-http-dialect-and-generated-clients.md) it is
not a third transport either. It *collapses* the transport: the wire becomes a function call. It is
what `java-direct` already is for the JVM, offered to everyone else.

**It contradicts this document's central claim, deliberately.** Above, the boundary is argued to
belong at the **process** edge rather than the **FFI** edge. This proposal moves it to the FFI edge.
That is the disagreement to settle, not a detail.

**Where it genuinely wins:**

- **Fast records.** `STRATEGY.md` already makes this argument in the other direction - for a base
  client the per-record hop is proportionally large. Slow work makes the hop noise; fast work makes it
  the cost. In-process is the right answer for the fast end, and no amount of protocol tuning gets a
  sidecar there.
- **No process to operate**, which is the single biggest adoption objection to the sidecar. It
  restores the property `STRATEGY.md` names as the reason this project works at all: invisible to the
  cluster, needing nobody's permission to deploy.
- Edge and embedded targets, where a second process is not merely inconvenient but unavailable.
- Fast startup and low memory against a JVM sidecar.

**Four costs, and the first two are the ones that decide it:**

- **It couples the platform release matrix to Kafka versions.** Stated too absolutely in the first
  draft, and corrected by the owner: *any* C-based channel needs per-platform binaries, so a matrix
  exists either way. **The difference is what re-triggers it.** A C client speaking the protocol
  rebuilds when the client changes; an embedded engine rebuilds on **every Kafka bump**, because the
  Kafka client is inside the binary. So a version bump goes from a dependency change to a rebuild and
  re-release of every platform binary and every language package that vendors one - and it is
  precisely this fork's currency argument that pays the price.
- **The callback concurrency model collides, differently in every language** - broken down below,
  because that breakdown turns out to decide the whole proposal.
- **Native-image reachability metadata for `kafka-clients`**, which uses reflection for serialisers and
  config. Solved territory (Quarkus does it) but real work.
- **Crash isolation is lost** - a segfault or OOM in the native image takes the host process with it,
  where the sidecar's process boundary contains it.

### The callback breakdown, and the finding that falls out of it

The question per language is the same: the engine must call *out* of native-image Java, on many
threads at once, into the host runtime. Ordered easiest to hardest. **All of it is from reasoning
about each runtime's published threading model, none of it measured** - treat it as the hypothesis to
test, not a result.

| Language | Mechanism | Real concurrency? | Difficulty |
|---|---|---|---|
| **Rust** | `extern "C"` function pointer | **yes, true parallelism** - no runtime, no GC pause, no interpreter lock | trivial |
| **C++** | plain C callback | **yes** | trivial |
| **Swift** | `@convention(c)` closure | **yes** | easy |
| **C#/.NET** | reverse P/Invoke, `[UnmanagedCallersOnly]`; the runtime attaches foreign threads itself | **yes** | easy |
| **Go** | cgo `//export`; the thread is attached to a goroutine | **yes**, but with real per-call overhead and cgo's pointer rules to respect | medium |
| **Python** | `PyGILState_Ensure` per call | **only while the lock is released** - I/O-bound handlers do release it, CPU-bound ones do not | hard mechanically |
| **Ruby** | `rb_thread_call_with_gvl`, thread must be registered | same shape as Python, via the global VM lock | hard |
| **Node/TypeScript** | N-API thread-safe function, marshalled onto the event loop | **no** - callbacks are serialised onto one thread; concurrency only from async I/O inside the handler | hardest |

**The finding: the difficulty ranking and the demand ranking align, and that is not a coincidence.**

- The **easy** languages - Rust, C++, Swift, C# - are also **where embedding is worth having**: the
  edge and embedded targets where a second process is unavailable, and the latency-sensitive
  fast-record workloads where the per-record hop is the cost rather than noise.
- The **hard** ones - Python, Ruby, Node - are where **the sidecar is already the right answer**.
  Their typical work is I/O-bound and slow, which makes the hop noise; and for I/O-bound handlers
  their in-process concurrency profile would be *the same as the threading they already have*, so
  embedding buys them very little at the highest cost.

So the owner's instinct that the easiest one is a good win is right, and for a stronger reason than
being easiest: **easiest and most valuable are the same set.** If this is ever attempted, **Rust
first** - trivial callbacks, true parallelism, and a population that actually wants an in-process
engine.

That inverts the earlier advice in this section to prove it where the callback question is hardest.
That advice suits a proposal that must eventually serve everyone; this one does not, because the
languages where it is hardest are the ones it need not serve at all.

**What makes it cheap to evaluate**: the conformance suite keys bindings on *(language, dialect)*, so
a native binding is more rows and no new scenarios. The Rust binding already exists over gRPC, so the
experiment is a second dialect for a language already covered - and the suite decides whether it
behaves identically rather than merely appearing to.

### Having both is the point: it is the first controlled measurement of the hop

Owner's call, 2026-08-15, and it converts this from "does embedding work" into something better
posed. **Two Rust backends behind one controller is a genuinely controlled experiment** - same
language, same controller, same conformance scenarios, one variable - which is rare enough here to be
worth having for its own sake.

**What it would settle.** `STRATEGY.md` asserts that for a base client the per-record hop is
proportionally large, and that assertion has never been measured. The output should not be a winner
but a **crossover point**: the per-record processing time at which the hop stops mattering. That
number is directly useful in documentation whichever side wins, because it tells a user which
transport their workload calls for instead of leaving them to guess.

**The confound that would invalidate it if left alone**: the embedded engine is
**native-image-compiled** while the sidecar is a **JVM**, so a naive comparison measures ahead-of-time
versus just-in-time compilation as much as it measures the transport - and native image typically
trades peak throughput for startup and memory, which would be charged to the hop by mistake. **Run the
sidecar as a native image too**, so the engine is identical on both arms and the transport is the only
difference. Report warm and cold separately.

**Measure across a sweep of processing durations** - nothing, a millisecond, ten, a hundred, a second
- since the whole claim is that the hop is noise for slow work and the cost for fast work. A single
duration cannot show a crossover.

**It still needs a stated exit**, because an experiment that ships accidentally becomes a product
maintained forever - the accretion failure `AGENTS.md` describes. Write the kill criterion before
building: what result deletes the embedded backend rather than fixing it. Behind an opt-in Cargo
feature it is one crate with two backends rather than two clients, which keeps the cost of being wrong
small.

**And if it succeeds, the end state is still not "both everywhere"** - it is *different defaults per
language*, embedded where the callback table says trivial and the demand is edge or latency, sidecar
where it says hard and the work is slow.

## What to do instead, for now

**Let demand decide.** The clients' root README should say that if someone wants a language that is
not here, they should open an issue and we will give it a go. That costs nothing, and one person
asking is better evidence than any amount of reasoning about who might want it — the same standard
applied to every other claim in this work.
